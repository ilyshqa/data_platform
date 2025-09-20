from typing import List, Dict, Optional

def ddl_create(schema_name: Optional[str], table_name: str, schema: List[Dict[str,str]]) -> str:
    def map_type(t: str):
        t = (t or "TEXT").upper()
        if t == "NUMERIC": return "NUMERIC"
        if t == "TIMESTAMP": return "TIMESTAMP"
        if t == "BOOLEAN": return "BOOLEAN"
        return "TEXT"
    cols = []
    for col in schema:
        name = col["column"]
        typ = map_type(col.get("sql_type_suggested","TEXT"))
        cols.append(f'"{name}" {typ}')
    full = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    return f'CREATE TABLE IF NOT EXISTS {full} (\n  ' + ",\n  ".join(cols) + "\n);"

def dml_insert_preview(schema_name: Optional[str], table_name: str, columns: List[str]) -> str:
    full = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    cols = ", ".join(f'"{c}"' for c in columns)
    vals = ", ".join(["?"]*len(columns))
    return f'INSERT INTO {full} ({cols}) VALUES ({vals}); -- preview'

def replace_plan(schema_name: Optional[str], table_name: str) -> str:
    full = f'"{schema_name}"."{table_name}"' if schema_name else f'"{table_name}"'
    return (
        "-- REPLACE plan (transactional):\n"
        "BEGIN;\n"
        f"  CREATE TABLE tmp_{table_name} AS TABLE {full} WITH NO DATA;\n"
        f"  -- COPY/INSERT данных в tmp_{table_name}\n"
        f"  DROP TABLE IF EXISTS {full};\n"
        f"  ALTER TABLE tmp_{table_name} RENAME TO \"{table_name}\";\n"
        "COMMIT;"
    )
