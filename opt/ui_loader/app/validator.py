import pandas as pd
from typing import List, Tuple, Dict, Any

import pandas as pd
from typing import List, Tuple, Dict, Any

def coerce_df(df: pd.DataFrame, schema: List[Dict[str, Any]], required: List[str] | None = None, unique: List[str] | None = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    errors = []

    required = required or []
    unique = unique or []

    required_cols = [c['column'] for c in schema]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        out = df.copy()
        out['error_reason'] = f"Отсутствуют колонки: {missing}"
        return df.head(0), out

    # Приведение типов
    for colspec in schema:
        col = colspec["column"]
        typ = colspec.get("sql_type_suggested", "TEXT")

        if typ == "NUMERIC":
            coerced = pd.to_numeric(df[col], errors="coerce")
            bad_mask = coerced.isna() & df[col].notna() & (df[col].astype(str).str.len() > 0)
            for idx in df.index[bad_mask]:
                errors.append((idx, f"'{col}' не число: {df.loc[idx, col]!r}"))
            df[col] = coerced

        elif typ == "TIMESTAMP":
            coerced = pd.to_datetime(df[col], errors="coerce")
            bad_mask = coerced.isna() & df[col].notna() & (df[col].astype(str).str.len() > 0)
            for idx in df.index[bad_mask]:
                errors.append((idx, f"'{col}' не дата/время: {df.loc[idx, col]!r}"))
            df[col] = coerced

        elif typ == "BOOLEAN":
            map_true = {'true','1','yes','y','да','истина','t'}
            map_false = {'false','0','no','n','нет','ложь','f'}
            vals = df[col].astype(str).str.strip().str.lower()
            coerced = pd.Series([None]*len(df), dtype="boolean")
            mask_true = vals.isin(map_true)
            mask_false = vals.isin(map_false)
            coerced[mask_true] = True
            coerced[mask_false] = False
            bad_mask = ~(mask_true | mask_false) & df[col].notna() & (df[col].astype(str).str.len() > 0)
            for idx in df.index[bad_mask]:
                errors.append((idx, f"'{col}' не булево: {df.loc[idx, col]!r}"))
            df[col] = coerced

        else:
            df[col] = df[col].astype("string")

    # Правило: обязательные колонки не должны быть пустыми
    for col in required:
        if col in df.columns:
            bad_mask = df[col].isna() | (df[col].astype(str).str.strip() == "")
            for idx in df.index[bad_mask]:
                errors.append((idx, f"'{col}' обязателен"))

    # Правило: уникальность по набору колонок
    if unique:
        dup_mask = df.duplicated(subset=unique, keep=False)
        for idx in df.index[dup_mask]:
            errors.append((idx, f"Нарушена уникальность по {unique}"))

    if errors:
        by_row = {}
        for idx, reason in errors:
            by_row.setdefault(idx, []).append(reason)
        df_err = df.loc[list(by_row.keys())].copy()
        df_err["error_reason"] = df_err.index.map(lambda i: "; ".join(by_row[i]))
        df_ok = df.drop(index=list(by_row.keys())).copy()
        return df_ok, df_err
    else:
        return df, df.head(0)
