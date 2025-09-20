# app/db.py
from typing import List, Tuple, Optional, Dict
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, Integer, Float, Numeric, Text, String, DateTime, Boolean, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from contextlib import contextmanager
from io import StringIO

# ---- Типы и создание таблицы ----

TYPE_MAP = {
    "NUMERIC": Numeric,
    "TEXT": Text,
    "TIMESTAMP": DateTime,
    "BOOLEAN": Boolean,
}

def get_engine(db_url: str) -> Engine:
    return create_engine(db_url, pool_pre_ping=True)

def list_tables(engine: Engine, schema: Optional[str]) -> List[str]:
    insp = inspect(engine)
    return insp.get_table_names(schema=schema)


def set_column_comments(engine, schema_name: str | None, table_name: str, schema: list) -> tuple[bool, str]:
    """
    Проставляет комментарии к колонкам на основе поля `description` в схеме.
    """
    try:
        full_table = (
            f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}'
            if schema_name else _quote_ident(table_name)
        )
        with engine.begin() as conn:
            for col in schema:
                col_name = col["column"]
                desc = (col.get("description") or "").strip()
                if desc:
                    # экранируем одинарные кавычки
                    desc_sql = desc.replace("'", "''")
                    col_ref = f"{full_table}.{_quote_ident(col_name)}"
                    conn.execute(text(f"COMMENT ON COLUMN {col_ref} IS '{desc_sql}'"))
        return True, "Комментарии к колонкам обновлены."
    except Exception as e:
        return False, f"Не удалось проставить комментарии: {e}"


def make_columns(schema: list) -> list:
    cols = []
    for col in schema:
        name = col["column"]
        sql_type = col.get("sql_type_suggested", "TEXT")
        sa_type = TYPE_MAP.get(sql_type, Text)
        if sql_type == "NUMERIC":
            column = Column(name, sa_type)
        elif sql_type == "TEXT":
            column = Column(name, String(length=2048))
        else:
            column = Column(name, sa_type)
        cols.append(column)
    return cols

def create_table(engine: Engine, schema_name: Optional[str], table_name: str, columns: list) -> Tuple[bool, str]:
    meta = MetaData(schema=schema_name)
    try:
        Table(table_name, meta, *columns)
        meta.create_all(engine, checkfirst=True)
        return True, "Таблица создана (или уже существовала)."
    except SQLAlchemyError as e:
        return False, f"Ошибка создания таблицы: {e}"

# ---- Утилиты для загрузки ----

def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'

@contextmanager
def _pg_cursor(engine: Engine):
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        yield raw, cur
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        try:
            cur.close()
        finally:
            raw.close()

def _table_columns(engine: Engine, schema_name: Optional[str], table_name: str) -> List[str]:
    insp = inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table_name, schema=schema_name)]
    return cols

def _align_df_to_table(df: pd.DataFrame, table_cols: List[str]) -> pd.DataFrame:
    # берём пересечение и располагаем в порядке таблицы
    common = [c for c in table_cols if c in df.columns]
    return df[common]

def _prep_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    # COPY ... NULL '' — пустая строка трактуется как NULL
    df2 = df.copy()
    for c in df2.columns:
        if pd.api.types.is_datetime64_any_dtype(df2[c]):
            s = pd.to_datetime(df2[c], errors="coerce")
            df2[c] = s.dt.strftime("%Y-%m-%d %H:%M:%S").where(s.notna(), "")
        elif pd.api.types.is_bool_dtype(df2[c]):
            df2[c] = df2[c].map({True: "true", False: "false"}).fillna("")
        else:
            df2[c] = df2[c].where(pd.notnull(df2[c]), "")
    return df2

def _copy_append(engine: Engine, schema_name: Optional[str], table_name: str, df: pd.DataFrame, chunksize: int = 50000):
    full = f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}' if schema_name else _quote_ident(table_name)
    from psycopg2 import sql  # просто чтобы psycopg2 точно подтянулся (через raw_connection)
    from io import StringIO
    with _pg_cursor(engine) as (raw, cur):
        n = len(df)
        if n == 0:
            return 0
        step = max(1, int(chunksize))
        inserted = 0
        for i in range(0, n, step):
            part = df.iloc[i:i+step]
            buf = StringIO()
            part.to_csv(buf, index=False, header=False)
            buf.seek(0)
            cols_sql = ", ".join(_quote_ident(c) for c in part.columns)
            sql_copy = f"COPY {full} ({cols_sql}) FROM STDIN WITH CSV NULL ''"
            cur.copy_expert(sql_copy, buf)
            inserted += len(part)
        return inserted

# ---- Публичная функция загрузки ----

def load_dataframe(
    engine: Engine,
    schema_name: Optional[str],
    table_name: str,
    df_ok: pd.DataFrame,
    mode: str = "append",
    chunksize: int = 5000
) -> tuple[bool, str]:
    """
    Быстрая загрузка:
      - append: COPY FROM STDIN
      - replace: загрузка во временную таблицу + атомарная замена.
    Возвращает: (ok, message)
    """
    if df_ok is None or df_ok.empty:
        return True, "Нет валидных строк для загрузки."

    mode = mode if mode in {"append", "replace"} else "append"

    try:
        # Выравниваем порядок колонок под таблицу
        table_cols = _table_columns(engine, schema_name or None, table_name)
        if not table_cols:
            table_cols = list(df_ok.columns)  # если инспектор не вернул (нестандартные права), работаем по df

        df_aligned = _align_df_to_table(df_ok, table_cols)
        if df_aligned.empty:
            return False, "Ни одна колонка из данных не совпала с колонками таблицы."
        df_csv = _prep_for_csv(df_aligned)

        full = f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}' if schema_name else _quote_ident(table_name)

        # До/после для отчёта
        with engine.connect() as conn:
            before = conn.execute(text(f"SELECT COUNT(*) FROM {full}")).scalar()

        if mode == "append":
            inserted = _copy_append(engine, schema_name, table_name, df_csv, chunksize=max(1000, int(chunksize)))
        else:
            tmp_name = f"tmp_{table_name}_copy"
            full_tmp = f'{_quote_ident(schema_name)}.{_quote_ident(tmp_name)}' if schema_name else _quote_ident(tmp_name)
            # создать tmp структуру как у target (или по df, если target не существует)
            with engine.begin() as conn:
                try:
                    conn.execute(text(f'CREATE TABLE {full_tmp} AS TABLE {full} WITH NO DATA'))
                except Exception:
                    df_csv.head(0).to_sql(tmp_name, engine, schema=schema_name, if_exists="replace", index=False)

            # COPY в tmp
            _copy_append(engine, schema_name, tmp_name, df_csv, chunksize=max(10000, int(chunksize)))

            # атомарная замена
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {full}"))
                conn.execute(text(f'ALTER TABLE {full_tmp} RENAME TO {_quote_ident(table_name)}'))
                if schema_name:
                    conn.execute(text(f'ALTER TABLE {_quote_ident(table_name)} SET SCHEMA {_quote_ident(schema_name)}'))
            inserted = len(df_csv)

        with engine.connect() as conn:
            after = conn.execute(text(f"SELECT COUNT(*) FROM {full}")).scalar()

        return True, f"Вставлено: {inserted}. Было: {before}, стало: {after}."
    except Exception as e:
        # fallback: execute_values (даст подробную диагностическую ошибку)
        try:
            from psycopg2.extras import execute_values
            cols = list(df_ok.columns)
            cols_sql = ", ".join(_quote_ident(c) for c in cols)
            df2 = df_ok.copy()
            for c in df2.columns:
                if pd.api.types.is_datetime64_any_dtype(df2[c]):
                    df2[c] = pd.to_datetime(df2[c], errors="coerce").dt.to_pydatetime()
                else:
                    df2[c] = df2[c].where(pd.notnull(df2[c]), None)
            rows = [tuple(r) for r in df2.itertuples(index=False, name=None)]
            full = f'{_quote_ident(schema_name)}.{_quote_ident(table_name)}' if schema_name else _quote_ident(table_name)
            with _pg_cursor(engine) as (raw, cur):
                sql_ins = f"INSERT INTO {full} ({cols_sql}) VALUES %s"
                execute_values(cur, sql_ins, rows, page_size=int(chunksize or 1000))
            return False, f"COPY не удался, но execute_values прошёл. Проверьте данные/типы. Детали: {e}"
        except Exception as e2:
            return False, f"Ошибка загрузки: {e2} (первичная: {e})"
