from tinydb import TinyDB, Query
from pathlib import Path
from typing import List, Dict, Any

DB_PATH = Path(__file__).resolve().parent.parent / "storage" / "profiles.json"

def _db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return TinyDB(DB_PATH)

def list_profiles() -> List[Dict[str, Any]]:
    with _db() as db:
        return db.table("profiles").all()

def save_profile(name: str, db_url: str, schema: str, allowed_modes=None):
    with _db() as db:
        t = db.table("profiles")
        # upsert by name
        Profile = Query()
        if t.search(Profile.name == name):
            t.update({"db_url": db_url, "schema": schema, "allowed_modes": allowed_modes or ["append","replace"]}, Profile.name == name)
        else:
            t.insert({"name": name, "db_url": db_url, "schema": schema, "allowed_modes": allowed_modes or ["append","replace"]})

def delete_profile(name: str):
    with _db() as db:
        t = db.table("profiles")
        Profile = Query()
        t.remove(Profile.name == name)
