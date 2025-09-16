from __future__ import annotations
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator

# === НАСТРОЙКИ ===
# где лежит Excel в MinIO
S3_URL = "s3://raw/rest/clients.xlsx"

# доступ к MinIO берём из env (как у тебя принято)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "adminadmin")

# доступ к Postgres (хост и порт серверные, БД создадим сами)
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "analytics")
PG_PASS = os.getenv("PG_PASS", "analytics")

DB_NAME = "rest"            # создадим, если нет
TABLE_NAME = "clients"      # целевая таблица

# маппинг колонок Excel -> английские имена
COLUMN_MAP = {
    "ID": "id",
    "Пол": "gender",
    "дата рождения": "birth_date",
    "есть приложение": "has_app",
    "есть телефон": "has_phone",
    "есть имейл": "has_email",
    "дата первой авторизации": "first_auth_date",
}

def _pg_url(db: str) -> str:
    # SQLAlchemy URL для psycopg2
    return f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{db}"

def ensure_database():
    """Создаёт БД rest, если её ещё нет."""
    engine = create_engine(_pg_url("postgres"), future=True, pool_pre_ping=True)
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": DB_NAME},
        ).scalar()
        if not exists:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f'CREATE DATABASE "{DB_NAME}"')
            )

def create_table():
    """Создаёт таблицу clients (id PK, типы нормализованы)."""
    engine = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id                BIGINT PRIMARY KEY,
            gender            VARCHAR(1),
            birth_date        DATE,
            has_app           BOOLEAN,
            has_phone         BOOLEAN,
            has_email         BOOLEAN,
            first_auth_date   TIMESTAMP
        );
        """))
        conn.commit()

def load_excel_to_postgres():
    """Читает Excel из MinIO, переименовывает колонки, приводит типы и загружает в Postgres (upsert по id)."""
    # читаем из MinIO через s3fs
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    }
    df = pd.read_excel(S3_URL, storage_options=storage_options)

    # переименовать колонки
    df = df.rename(columns=COLUMN_MAP)

    # привести типы
    # дата рождения / дата первой авторизации → даты/таймстемпы
    if "birth_date" in df.columns:
        df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce").dt.date
    if "first_auth_date" in df.columns:
        df["first_auth_date"] = pd.to_datetime(df["first_auth_date"], errors="coerce")

    # булевые поля: считать истинным все варианты «Да/True/1/есть», остальное False
    def to_bool(s):
        if pd.isna(s): return False
        v = str(s).strip().lower()
        return v in {"1","true","t","yes","y","да","есть","aga","ok"}
    for col in ["has_app","has_phone","has_email"]:
        if col in df.columns:
            df[col] = df[col].map(to_bool)

    # gender → 'M'/'F' (оставим первую букву, рус/лат поддержим)
    if "gender" in df.columns:
        def norm_g(x):
            if pd.isna(x): return None
            v = str(x).strip().lower()
            if v.startswith(("m","м")): return "M"
            if v.startswith(("f","ж")): return "F"
            return None
        df["gender"] = df["gender"].map(norm_g)

    # убедимся, что id — целое
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    # загрузка: используем временную staging-таблицу, затем upsert в целевую
    engine = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    tmp = f"_{TABLE_NAME}_stg"
    
    # 1) Почистим временную таблицу в явной транзакции SQLAlchemy
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp};"))
    # 2) ВАЖНО: передаём ИМЕННО engine (или SQLAlchemy Connection), а не conn.connection
    #    И добавим chunksize/method для стабильности
    df.to_sql(
        tmp,
        con=engine,              
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    # 3) UPSERT в целевую таблицу и удаление staging
    cols = ["id","gender","birth_date","has_app","has_phone","has_email","first_auth_date"]
    cols = [c for c in cols if c in df.columns]
    col_list = ", ".join(cols)
    set_list = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "id"])

    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {TABLE_NAME} ({col_list})
            SELECT {col_list} FROM {tmp}
            ON CONFLICT (id) DO UPDATE SET {set_list};
        """))
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp};"))

def rowcount_check():
    """Простейший QC — в таблице есть строки."""
    engine = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    with engine.connect() as conn:
        cnt = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};")).scalar()
        if not cnt or cnt == 0:
            raise ValueError("QC failed: clients is empty")

with DAG(
    dag_id="load_clients_from_excel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rest","excel","minio","postgres"],
) as dag:

    t1 = PythonOperator(task_id="ensure_database", python_callable=ensure_database)
    t2 = PythonOperator(task_id="create_table", python_callable=create_table)
    t3 = PythonOperator(task_id="load_excel", python_callable=load_excel_to_postgres)
    t4 = PythonOperator(task_id="quality_check", python_callable=rowcount_check)

    t1 >> t2 >> t3 >> t4
