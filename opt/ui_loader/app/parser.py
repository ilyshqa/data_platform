import pandas as pd

def parse_special_excel(path: str):
    raw = pd.read_excel(path, header=None, engine="openpyxl")
    if len(raw) < 4:
        raise ValueError("Ожидались минимум 3 строки шапки и одна строка данных.")
    colnames = raw.iloc[0].fillna("").astype(str).tolist()
    descriptions = raw.iloc[1].fillna("").astype(str).tolist()
    source_types = raw.iloc[2].fillna("").astype(str).tolist()
    data = raw.iloc[3:].reset_index(drop=True)
    data.columns = colnames

    def map_type(t: str):
        t = (t or "").strip().lower()
        if "чис" in t or "int" in t or "float" in t or "дроб" in t:
            return "NUMERIC"
        if "дата" in t or "time" in t:
            return "TIMESTAMP"
        if "bool" in t or "логич" in t:
            return "BOOLEAN"
        return "TEXT"

    schema = []
    for i, name in enumerate(colnames):
        if not name or name.lower() == "nan":
            continue
        schema.append({
            "column": name,
            "description": descriptions[i] if i < len(descriptions) else "",
            "source_type": source_types[i] if i < len(source_types) else "",
            "sql_type_suggested": map_type(source_types[i] if i < len(source_types) else ""),
        })
    return schema, data
