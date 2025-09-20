import streamlit as st
import pandas as pd
import os, io, tempfile, time
from dotenv import load_dotenv
from parser import parse_special_excel
from db import get_engine, list_tables, make_columns, create_table, load_dataframe, set_column_comments
from validator import coerce_df
import copy
from profiles import list_profiles, save_profile, delete_profile
from history import add_record, list_records, get_record, save_dataframe_csv, update_record

st.set_page_config(page_title="Импорт данных → БД", page_icon="🧩", layout="wide")
st.title("🧩 Импорт данных: Excel → БД")

load_dotenv()

# --- Profiles section ---
with st.sidebar:
    st.header("Профили подключений")
    profs = list_profiles()
    names = ["<не выбран>"] + [p["name"] for p in profs]
    selected_name = st.selectbox("Профиль", options=names, index=0)
    if selected_name != "<не выбран>":
        prof = next(p for p in profs if p["name"] == selected_name)
        default_url = prof["db_url"]
        default_schema = prof["schema"]
    else:
        default_url = os.getenv("DB_URL", "postgresql+psycopg2://analytics:analytics@158.160.43.121:15432/load_excel?sslmode=prefer")
        default_schema = os.getenv("DB_SCHEMA", "public")

    db_url = st.text_input("DB URL", value=default_url)
    db_schema = st.text_input("Schema", value=default_schema)

    colp1, colp2 = st.columns(2)
    with colp1:
        new_name = st.text_input("Сохранить как профиль (имя)", placeholder="prod-load-excel")
    with colp2:
        if st.button("Сохранить профиль", use_container_width=True, disabled=not new_name or not db_url):
            save_profile(new_name, db_url, db_schema or "")
            st.success(f"Профиль '{new_name}' сохранён")

    if selected_name != "<не выбран>":
        if st.button("Удалить выбранный профиль", use_container_width=True):
            delete_profile(selected_name)
            st.success(f"Профиль '{selected_name}' удалён. Обновите список (перезапустите страницу).")

    st.divider()
    connect = st.button("Проверить подключение / Обновить список таблиц")

tables = []
engine = None
if connect:
    try:
        engine = get_engine(db_url)
        tables = list_tables(engine, db_schema or None)
        st.session_state["db_url"] = db_url
        st.session_state["db_schema"] = db_schema
        st.session_state["tables"] = tables
        st.success(f"Ок! Найдено таблиц: {len(tables)}")
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")

# --- File upload & parse ---
st.subheader("1) Загрузка файла")
file_format = st.selectbox("Тип файла", ["Excel со спец-шапкой (3 строки)", "Обычный Excel/CSV (первая строка — заголовки)"], index=0)
f = st.file_uploader("Загрузите Excel (.xlsx) или CSV", type=["xlsx","csv"])

schema = None
df_raw = None
if f:
    try:
        if file_format == "Excel со спец-шапкой (3 строки)" and f.name.endswith(".xlsx"):
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
            tmp.write(f.read()); tmp.close()
            schema, df_raw = parse_special_excel(tmp.name)
            os.unlink(tmp.name)
            st.success("Файл распознан по шаблону со спец-шапкой.")
        else:
            if f.name.endswith(".csv"):
                df_raw = pd.read_csv(f)
            else:
                df_raw = pd.read_excel(f)
            schema = [{"column": c, "description": "", "source_type": "", "sql_type_suggested": "TEXT"} for c in df_raw.columns]
            st.info("Обычный режим (первая строка — заголовки).")
        st.write("**Схема — редактирование типов:**")
        schema_edit = copy.deepcopy(schema)
        type_options = ["TEXT","NUMERIC","TIMESTAMP","BOOLEAN"]
        for col in schema_edit:
            col["sql_type_suggested"] = st.selectbox(
                f"Тип для '{col['column']}'",
                type_options,
                index=type_options.index(col.get("sql_type_suggested","TEXT")) if col.get("sql_type_suggested","TEXT") in type_options else 0,
                key=f"type_{col['column']}"
            )
        schema = schema_edit
        st.dataframe(pd.DataFrame(schema))
        st.write("**Превью данных:**")
        st.dataframe(df_raw.head(50))
    except Exception as e:
        st.error(f"Ошибка разбора файла: {e}")

st.divider()
st.subheader("2) Таблица назначения")

db_url = st.session_state.get("db_url", db_url)
db_schema = st.session_state.get("db_schema", db_schema)
tables = st.session_state.get("tables", tables)

col1, col2 = st.columns(2)
with col1:
    choice = st.radio("Выбор таблицы:", ["Выбрать из существующих", "Создать новую"], horizontal=True)
    if choice == "Выбрать из существующих":
        table_name = st.selectbox("Таблица", options=tables) if tables else st.text_input("Имя таблицы")
    else:
        table_name = st.text_input("Имя новой таблицы", placeholder="например: dim_items")
with col2:
    load_mode = st.selectbox("Режим загрузки", ["append", "replace"], index=0)
    chunk = st.number_input("Размер пачки (chunksize)", min_value=100, max_value=50000, value=5000, step=100)

from sql_preview import ddl_create, dml_insert_preview, replace_plan

if schema and table_name:
    st.markdown("**SQL-превью:**")
    st.code(ddl_create(db_schema or None, table_name, schema), language="sql")
    st.code(dml_insert_preview(db_schema or None, table_name, [c["column"] for c in schema]), language="sql")
    if load_mode == "replace":
        st.code(replace_plan(db_schema or None, table_name), language="sql")

if st.button("Загрузить", disabled=not (schema and table_name and db_url)):
    try:
        engine = get_engine(db_url)
        columns = make_columns(schema)
        ok, msg = create_table(engine, db_schema or None, table_name, columns)
        if ok:
            st.success(msg)
            okc, msgc = set_column_comments(engine, db_schema or None, table_name, schema)
            if okc:
                st.info(msgc)
            else:
                st.warning(msgc)
        else:
            st.error(msg)
        # if ok:
        #     try:
        #         # st.success(msg)
        #         st.session_state["tables"] = list_tables(engine, db_schema or None)
        #     except Exception:
        #         st.error(msg)
        #         pass
    except Exception as e:
        st.error(f"Ошибка: {e}")

st.divider()
st.subheader("3) Правила качества данных")
req_cols = st.multiselect("Обязательные колонки (NOT NULL)", options=[c["column"] for c in (schema or [])])
uniq_cols = st.multiselect("Уникальность по колонкам (составной ключ)", options=[c["column"] for c in (schema or [])])

st.divider()
st.subheader("4) Валидация, загрузка и история")

colA, colB = st.columns(2)
with colA:
    do_load = st.button("Проверить и загрузить", disabled=not (schema and df_raw is not None and table_name and db_url))
with colB:
    st.caption("История загрузок (последние 50 записей)")

if do_load:
    try:
        engine = get_engine(db_url)
        df_ok, df_err = coerce_df(df_raw, schema, req_cols, uniq_cols)

        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Всего строк", len(df_raw) if df_raw is not None else 0)
        with c2: st.metric("Валидно", len(df_ok))
        with c3: st.metric("Ошибки", len(df_err))

        # create history record
        rec_id = add_record({
            "ts": int(time.time()),
            "db_url": db_url,
            "schema": db_schema,
            "table": table_name,
            "mode": load_mode,
            "rules": {"required": req_cols, "unique": uniq_cols},
            "filename": getattr(f, "name", ""),
        })

        if not df_err.empty:
            st.warning("Найдены ошибки. Эти строки НЕ будут загружены.")
            st.dataframe(df_err.head(200))
            err_path = save_dataframe_csv(rec_id, df_err, "err")
            st.info(f"Отчёт об ошибках сохранён: {err_path}")
            st.download_button("Скачать отчёт об ошибках (CSV)", data=df_err.to_csv(index=False).encode("utf-8"), file_name=f"{rec_id}_errors.csv", mime="text/csv")
            update_record(rec_id, {"errors_csv": err_path, "errors_count": int(len(df_err))})

        if not df_ok.empty:
            ok, msg = load_dataframe(engine, db_schema or None, table_name, df_ok, mode=load_mode, chunksize=int(chunk))
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            if ok:
                ok_path = save_dataframe_csv(rec_id, df_ok, "ok")
                update_record(rec_id, {"ok_csv": ok_path, "ok_count": int(len(df_ok)), "status": "loaded"})
            else:
                update_record(rec_id, {"status": "failed", "error": msg})
        else:
            st.info("Валидных строк нет — загрузка пропущена.")
            update_record(rec_id, {"status": "no_valid_rows"})

    except Exception as e:
        st.error(f"Ошибка: {e}")

# History panel
st.subheader("История загрузок")
records = list_records(limit=50)
if not records:
    st.info("История пока пуста.")
else:
    import datetime
    rows = []
    for r in records:
        ts = datetime.datetime.fromtimestamp(r.get("ts", 0)).strftime("%Y-%m-%d %H:%M:%S")
        rows.append({
            "id": r.get("id"),
            "time": ts,
            "table": f"{r.get('schema')}.{r.get('table')}" if r.get("schema") else r.get("table"),
            "mode": r.get("mode"),
            "ok": r.get("ok_count", 0),
            "errors": r.get("errors_count", 0),
            "file": r.get("filename", ""),
            "status": r.get("status", ""),
        })
    st.dataframe(pd.DataFrame(rows))

    st.markdown("**Повторить загрузку по записи истории**:")
    rec_id_sel = st.selectbox("ID записи", options=[r["id"] for r in records])
    if st.button("Повторить загрузку этой записи", disabled=not rec_id_sel):
        rec = get_record(rec_id_sel)
        if rec and rec.get("ok_csv"):
            try:
                engine = get_engine(st.session_state.get("db_url", db_url))
                df_ok = pd.read_csv(rec["ok_csv"])
                ok, msg = load_dataframe(engine, rec.get("schema") or None, rec.get("table"), df_ok, mode=rec.get("mode","append"), chunksize=5000)
                st.success(f"[Re-run] {msg}")
                if ok:
                    st.success(f"[Re-run] {msg}")
                else:
                    st.error(f"[Re-run] {msg}")
            except Exception as e:
                st.error(f"[Re-run] Ошибка: {e}")
        else:
            st.warning("У записи нет файла валидных строк.")
