import sqlite3, json, time
from ..settings import settings


def init_db():
    with sqlite3.connect(settings.DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS runs(
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            config_json TEXT NOT NULL,
            output_json TEXT NOT NULL,
            metric REAL NOT NULL,
            status TEXT NOT NULL
        )
        """)


def record_run(config: dict, output: dict, metric: float, status: str = "success") -> int:
    with sqlite3.connect(settings.DB_PATH) as con:
        cur = con.execute(
            "INSERT INTO runs(ts, config_json, output_json, metric, status) VALUES (?,?,?,?,?)",
            (time.time(), json.dumps(config), json.dumps(output), metric, status)
        )
        return cur.lastrowid
