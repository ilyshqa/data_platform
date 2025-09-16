from __future__ import annotations
from datetime import datetime
import os
import re
import pandas as pd
from sqlalchemy import create_engine, text

# --- настройки источника ---
S3_URL = "s3://raw/rest/fact.xlsx"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "adminadmin")

# --- настройки Postgres ---
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "analytics")
PG_PASS = os.getenv("PG_PASS", "analytics")

DB_NAME = "rest"
TABLE_NAME = "fact"

# --- Airflow ---
from airflow import DAG
from airflow.operators.python import PythonOperator

def _pg_url(db: str) -> str:
    return f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{db}"

def ensure_database():
    eng = create_engine(_pg_url("postgres"), future=True, pool_pre_ping=True)
    with eng.connect() as conn:
        ok = conn.execute(text("select 1 from pg_database where datname=:n"), {"n": DB_NAME}).scalar()
        if not ok:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(f'create database "{DB_NAME}"'))

def read_excel() -> pd.DataFrame:
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    }
    df = pd.read_excel(S3_URL, storage_options=storage_options, engine="openpyxl")
    # нормализуем названия колонок в snake_case латиницей
    def norm_col(c):
        c = str(c).strip()
        c = c.replace("№", "no").replace("%", "pct")
        c = re.sub(r"[^\w\s]", "_", c, flags=re.U)
        c = re.sub(r"\s+", "_", c, flags=re.U)
        return c.lower().strip("_")
    df.columns = [norm_col(c) for c in df.columns]

    # привести даты/числа/булевы где это очевидно
    for col in df.columns:
        if df[col].dtype == object:
            # попытка распарсить даты
            if re.search(r"(date|dt|_at|дата|время)$", col):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            # булево «да/нет»
            vals = df[col].astype(str).str.lower().str.strip()
            if vals.isin(["да","нет","true","false","1","0","y","n","yes","no"]).mean() > 0.8:
                df[col] = vals.isin(["да","true","1","y","yes"])
    return df

def create_table(df: pd.DataFrame):
    # маппинг pandas → pg
    def pg_type(s: pd.Series) -> str:
        if pd.api.types.is_bool_dtype(s): return "BOOLEAN"
        if pd.api.types.is_integer_dtype(s): return "BIGINT"
        if pd.api.types.is_float_dtype(s): return "DOUBLE PRECISION"
        if pd.api.types.is_datetime64_any_dtype(s): return "TIMESTAMP"
        return "TEXT"

    cols_sql = []
    for c in df.columns:
        cols_sql.append(f'"{c}" {pg_type(df[c])}')
    # если есть столбец id — делаем его PK
    has_id = any(c.lower() == "id" for c in df.columns)
    if has_id:
        # гарантируем BIGINT для id
        cols_sql = [s.replace('"id" TEXT', '"id" BIGINT') for s in cols_sql]
        cols_sql = [s.replace('"id" DOUBLE PRECISION', '"id" BIGINT') for s in cols_sql]
        pk_sql = ', PRIMARY KEY ("id")'
    else:
        pk_sql = ""

    eng = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    with eng.begin() as conn:
        conn.execute(text(f'create table if not exists "{TABLE_NAME}" ({", ".join(cols_sql)}{pk_sql});'))

def load_data():
    df = read_excel()
    if df.empty:
        raise ValueError("Excel is empty after parsing")

    # привести id к int, если есть
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df = df[df["id"].notna()]

    eng = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    tmp = f"_{TABLE_NAME}_stg"

    # staging (важно: передаём engine, а не raw-connection)
    with eng.begin() as conn:
        conn.execute(text(f'drop table if exists "{tmp}";'))
    df.to_sql(tmp, eng, if_exists="replace", index=False, chunksize=1000, method="multi")

    with eng.begin() as conn:
        if "id" in df.columns:
            cols = list(df.columns)
            col_list = ", ".join([f'"{c}"' for c in cols])
            set_list = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in cols if c != "id"])
            conn.execute(text(f'''
                insert into "{TABLE_NAME}" ({col_list})
                select {col_list} from "{tmp}"
                on conflict ("id") do update set {set_list};
            '''))
        else:
            # нет id — делаем truncate-load
            conn.execute(text(f'truncate table "{TABLE_NAME}";'))
            cols = list(df.columns)
            col_list = ", ".join([f'"{c}"' for c in cols])
            conn.execute(text(f'insert into "{TABLE_NAME}" ({col_list}) select {col_list} from "{tmp}";'))
        conn.execute(text(f'drop table if exists "{tmp}";'))

def qc():
    eng = create_engine(_pg_url(DB_NAME), future=True, pool_pre_ping=True)
    with eng.connect() as conn:
        cnt = conn.execute(text(f'select count(*) from "{TABLE_NAME}"')).scalar()
        if not cnt or cnt == 0:
            raise ValueError(f'QC failed: table "{TABLE_NAME}" is empty')

with DAG(
    dag_id="load_fact_from_excel",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rest","excel","minio","postgres"],
) as dag:
    t0 = PythonOperator(task_id="ensure_database", python_callable=ensure_database)
    t1 = PythonOperator(task_id="create_table", python_callable=lambda: create_table(read_excel()))
    t2 = PythonOperator(task_id="load_data", python_callable=load_data)
    t3 = PythonOperator(task_id="quality_check", python_callable=qc)
    t0 >> t1 >> t2 >> t3
