import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from fastapi import FastAPI, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")


DB_PATH = os.getenv("DB_PATH", "eod.db")


def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    _ensure_tables(conn)
    return conn


def _ensure_tables(conn: sqlite3.Connection) -> None:
    # Tiny metadata table for showing last ingest result on the homepage
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            ok INTEGER NOT NULL DEFAULT 0,
            note TEXT,
            jobs_tried INTEGER NOT NULL DEFAULT 0,
            jobs_inserted INTEGER NOT NULL DEFAULT 0
        );
    """)
    conn.commit()


def _last_ingest(conn: sqlite3.Connection) -> Dict[str, Any]:
    row = conn.execute("""
        SELECT id, started_at, finished_at, ok, note, jobs_tried, jobs_inserted
        FROM ingest_run
        ORDER BY id DESC LIMIT 1
    """).fetchone()
    if not row:
        return {}
    return dict(row)


def _recent_jobs(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    # If you have a jobs table, this will show a few recent rows; otherwise it’s just empty.
    try:
        rows = conn.execute("""
            SELECT * FROM jobs
            ORDER BY rowid DESC
            LIMIT 12
        """).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        # jobs table not created yet; that’s fine
        return []


def _totals(conn: sqlite3.Connection) -> Dict[str, Any]:
    # Keep it safe: if your schema differs, we just return an empty dict and the template uses defaults.
    totals: Dict[str, Any] = {}
    try:
        # Example: if you store hours on a jobs or timesheets table, compute something here.
        # We’ll be defensive and do nothing if tables/columns don’t exist yet.
        pass
    except sqlite3.OperationalError:
        pass
    return totals


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    with _conn() as conn:
        totals = _totals(conn)  # always pass a dict so Jinja never breaks
        rows = _recent_jobs(conn)
        ingest = _last_ingest(conn)

    # IMPORTANT: pass totals (dict), rows (list), ingest (dict) – template uses .get() safely
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "totals": totals,
            "rows": rows,
            "ingest": ingest,
        },
    )


# ---- Ingest endpoints --------------------------------------------------------

from .ingest import ingest_live  # noqa: E402  (import after app init so templates mount earlier)


@app.post("/ingest/live")
def run_live_ingest():
    """
    Run ingest and always redirect home with the latest result stored in ingest_run.
    This avoids leaking a 500 to the browser if the upstream API 404s, etc.
    """
    ok, note, jobs_inserted, jobs_tried = ingest_live()

    # Store a result row so the home page can surface the outcome
    with _conn() as conn:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO ingest_run (started_at, finished_at, ok, note, jobs_tried, jobs_inserted)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (now, now, 1 if ok else 0, note, jobs_tried, jobs_inserted),
        )
        conn.commit()

    # Always bounce back to the home page
    return RedirectResponse(url="/", status_code=status.HTTP_303_SEE_OTHER)
