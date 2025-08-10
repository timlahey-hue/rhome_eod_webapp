import sqlite3, pathlib, datetime, json
DB_PATH = pathlib.Path("eod.db")

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_date TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS job_rows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id INTEGER NOT NULL,
  job_code TEXT,
  job_name TEXT,
  pm TEXT,
  hours_today REAL,
  labour_cost_today REAL,
  materials_cost_today REAL,
  cost_today REAL,
  actual_cost_to_date REAL,
  estimated_cost REAL,
  burn_pct REAL,
  gm_to_date REAL,
  invoiced_today REAL,
  mtd_hours REAL,
  days_since_update INTEGER,
  at_risk INTEGER,
  FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
);
"""

def get_conn():
  conn = sqlite3.connect(DB_PATH)
  conn.row_factory = sqlite3.Row
  return conn

def init_db():
  with get_conn() as c:
    c.executescript(SCHEMA)

def create_snapshot(date_str: str):
  now = datetime.datetime.utcnow().isoformat()
  with get_conn() as c:
    cur = c.execute("INSERT INTO snapshots (snapshot_date, created_at) VALUES (?, ?)", (date_str, now))
    return cur.lastrowid

def insert_job_rows(snapshot_id: int, rows: list[dict]):
  keys = ["snapshot_id","job_code","job_name","pm","hours_today","labour_cost_today","materials_cost_today",
          "cost_today","actual_cost_to_date","estimated_cost","burn_pct","gm_to_date","invoiced_today","mtd_hours",
          "days_since_update","at_risk"]
  with get_conn() as c:
    for r in rows:
      vals = [snapshot_id,
              r.get("job_code"), r.get("job_name"), r.get("pm"),
              r.get("hours_today",0), r.get("labour_cost_today",0), r.get("materials_cost_today",0),
              r.get("cost_today",0), r.get("actual_cost_to_date",0), r.get("estimated_cost",0),
              r.get("burn_pct"), r.get("gm_to_date"), r.get("invoiced_today",0), r.get("mtd_hours",0),
              r.get("days_since_update",0), int(bool(r.get("at_risk", False)))]
      c.execute(f"INSERT INTO job_rows ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", vals)

def list_snapshots():
  with get_conn() as c:
    return c.execute("SELECT * FROM snapshots ORDER BY snapshot_date DESC, id DESC").fetchall()

def get_snapshot_rows(snapshot_id: int):
  with get_conn() as c:
    return c.execute("SELECT * FROM job_rows WHERE snapshot_id=? ORDER BY job_name", (snapshot_id,)).fetchall()

def get_latest_snapshot():
  snaps = list_snapshots()
  return snaps[0] if snaps else None
