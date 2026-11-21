from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .contracts import canonical, utcnow

ROOT = Path(__file__).resolve().parents[2]


class Lab:
    def __init__(self, root: Path | str | None = None):
        self.root = Path(
            root or os.environ.get("QUANTOS_CONTEST_DIR", ROOT / ".quantos-contest")
        ).resolve()
        if (self.root / "private").exists():
            raise ValueError("runtime directory contains incompatible private configuration")
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("raw", "lake", "models", "reports"):
            (self.root / name).mkdir(exist_ok=True)
        with self.db() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS datasets(id TEXT PRIMARY KEY, manifest TEXT NOT NULL, status TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS series(dataset_id TEXT, id TEXT, meta TEXT NOT NULL, PRIMARY KEY(dataset_id,id));
            CREATE TABLE IF NOT EXISTS sources(dataset_id TEXT, path TEXT, status TEXT, count INTEGER, detail TEXT,
                PRIMARY KEY(dataset_id,path));
            CREATE TABLE IF NOT EXISTS documents(id TEXT PRIMARY KEY,dataset_id TEXT,kind TEXT,day TEXT,
                title TEXT,body TEXT,source TEXT,source_row INTEGER,raw_json TEXT,trust TEXT);
            CREATE INDEX IF NOT EXISTS documents_day ON documents(dataset_id,day);
            CREATE INDEX IF NOT EXISTS documents_kind ON documents(dataset_id,kind);
            CREATE INDEX IF NOT EXISTS documents_query ON documents(dataset_id,kind,day DESC);
            CREATE TABLE IF NOT EXISTS models(id TEXT PRIMARY KEY,card TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS runs(id TEXT PRIMARY KEY,spec TEXT,status TEXT,cursor INTEGER DEFAULT 0,
                total INTEGER DEFAULT 0,error TEXT,created_at TEXT,updated_at TEXT);
            CREATE TABLE IF NOT EXISTS results(run_id TEXT,day TEXT,payload TEXT,PRIMARY KEY(run_id,day));
            CREATE TABLE IF NOT EXISTS alerts(id TEXT PRIMARY KEY,run_id TEXT,day TEXT,payload TEXT,
                disposition TEXT DEFAULT 'open',note TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS tasks(id TEXT PRIMARY KEY,kind TEXT,status TEXT,detail TEXT,updated_at TEXT);
            CREATE TABLE IF NOT EXISTS analyses(id TEXT PRIMARY KEY,kind TEXT,payload TEXT);
            CREATE TABLE IF NOT EXISTS model_aliases(alias TEXT PRIMARY KEY,model_id TEXT,updated_at TEXT);
            CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY AUTOINCREMENT,at TEXT,actor TEXT,action TEXT,subject TEXT,detail TEXT);
            CREATE TABLE IF NOT EXISTS research(id TEXT PRIMARY KEY,owner TEXT,status TEXT,spec TEXT,result TEXT,created_at TEXT,updated_at TEXT);
            CREATE TABLE IF NOT EXISTS pdf_pages(document_id TEXT,page INTEGER,text TEXT,PRIMARY KEY(document_id,page));
            """)

    @contextmanager
    def db(self):
        c = sqlite3.connect(self.root / "lab.sqlite", timeout=30)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=30000")
        try:
            yield c
            c.commit()
        except BaseException:
            c.rollback()
            raise
        finally:
            c.close()

    def rows(self, sql, params=()):
        with self.db() as c:
            return [dict(r) for r in c.execute(sql, params).fetchall()]

    def execute(self, sql, params=()):
        with self.db() as c:
            return c.execute(sql, params).rowcount

    def dataset(self, dataset_id=None):
        rows = (
            self.rows("SELECT * FROM datasets WHERE id=?", (dataset_id,))
            if dataset_id
            else self.rows(
                "SELECT * FROM datasets WHERE status='ready' ORDER BY rowid DESC LIMIT 1"
            )
        )
        if not rows:
            raise ValueError("尚无完成导入的数据版本")
        return {**json.loads(rows[0]["manifest"]), "status": rows[0]["status"]}

    def series(self, dataset_id=None):
        dataset_id = dataset_id or self.dataset()["id"]
        return [
            json.loads(r["meta"])
            for r in self.rows(
                "SELECT meta FROM series WHERE dataset_id=? ORDER BY id", (dataset_id,)
            )
        ]

    def task(self, key, kind, status, detail):
        self.execute(
            "INSERT OR REPLACE INTO tasks VALUES(?,?,?,?,?)",
            (key, kind, status, canonical(detail), utcnow()),
        )
