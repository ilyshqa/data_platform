from tinydb import TinyDB, Query
from pathlib import Path
from typing import Dict, Any, List, Optional
import time, json

DB_PATH = Path(__file__).resolve().parent.parent / "storage" / "history_db.json"
DATA_DIR = Path(__file__).resolve().parent.parent / "storage" / "history"

def _db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return TinyDB(DB_PATH)

def add_record(meta: Dict[str, Any]) -> str:
    ts = int(time.time())
    meta = dict(meta)
    meta["id"] = f"h{ts}"
    with _db() as db:
        db.table("records").insert(meta)
    return meta["id"]

def list_records(limit: int = 50) -> List[Dict[str, Any]]:
    with _db() as db:
        rows = db.table("records").all()
    rows.sort(key=lambda r: r.get("ts", 0), reverse=True)
    return rows[:limit]

def get_record(rec_id: str) -> Optional[Dict[str, Any]]:
    with _db() as db:
        T = Query()
        arr = db.table("records").search(T.id == rec_id)
    return arr[0] if arr else None

def save_dataframe_csv(rec_id: str, df, kind: str) -> str:
    # kind: 'ok' or 'err'
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / f"{rec_id}_{kind}.csv"
    df.to_csv(path, index=False)
    return str(path)

def update_record(rec_id: str, fields: Dict[str, Any]):
    with _db() as db:
        T = Query()
        db.table("records").update(fields, T.id == rec_id)
