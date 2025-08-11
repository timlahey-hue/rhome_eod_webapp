# app/main.py
from __future__ import annotations

from pathlib import Path
import sqlite3

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool

from .ingest import ingest_live, ingest_demo

app = FastAPI(title="RHome EOD Dashboard")

# Mount /static if present (keeps your existing styles.css working)
_static_dir = Path(__file__).resolve().parent / "static"
if _static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(_static_dir)), name="static")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index():
    # Show last few ingest runs if DB is reachable; otherwise just render buttons.
    rows_html = ""
    try:
        conn = sqlite3.connect("eod.db")
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT id, started_at, ended_at, ok, jobs_tried, jobs_inserted, COALESCE(note,'') as note "
            "FROM ingest_runs ORDER BY id DESC LIMIT 10"
        )
        rows = cur.fetchall()
        if rows:
            items = []
            for r in rows:
                items.append(
                    f"<tr>"
                    f"<td>{r['id']}</td>"
                    f"<td>{r['started_at']}</td>"
                    f"<td>{r['ended_at'] or ''}</td>"
                    f"<td>{'✅' if r['ok'] else '❌'}</td>"
                    f"<td>{r['jobs_tried']}</td>"
                    f"<td>{r['jobs_inserted']}</td>"
                    f"<td>{r['note']}</td>"
                    f"</tr>"
                )
            rows_html = (
                "<h2>Recent ingest runs</h2>"
                "<table class='runs'><thead><tr>"
                "<th>ID</th><th>Started</th><th>Ended</th><th>OK</th><th>Tried</th><th>Inserted</th><th>Note</th>"
                "</tr></thead><tbody>" + "".join(items) + "</tbody></table>"
            )
        conn.close()
    except Exception:
        # DB might not exist yet — that's fine.
        rows_html = "<p class='muted'>No ingest history yet.</p>"

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>RHome EOD Dashboard</title>
  <link rel="stylesheet" href="/static/styles.css" />
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; padding: 2rem; }}
    .bar {{ display:flex; gap: 1rem; margin-bottom: 1.25rem; }}
    form button {{ padding: .6rem 1rem; font-weight: 600; cursor: pointer; }}
    table.runs {{ border-collapse: collapse; margin-top: 1rem; width: 100%; max-width: 1000px; }}
    table.runs th, table.runs td {{ border: 1px solid #ddd; padding: .4rem .6rem; text-align: left; }}
    .muted {{ color: #777; }}
  </style>
</head>
<body>
  <h1>RHome EOD Dashboard</h1>
  <div class="bar">
    <form method="post" action="/ingest/live">
      <button type="submit">Run Live Ingest</button>
    </form>
    <form method="post" action="/ingest/demo">
      <button type="submit">Load Demo Data</button>
    </form>
  </div>
  {rows_html}
</body>
</html>
    """.strip()
    return HTMLResponse(content=html, status_code=200)


@app.post("/ingest/live")
async def run_live_ingest():
    try:
        data = await run_in_threadpool(ingest_live)
    except Exception as e:
        # Show the error in logs and return a 500
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    if data.get("ok"):
        # Keep your previous behavior (303 -> "/")
        return RedirectResponse(url="/", status_code=303)
    return JSONResponse(data, status_code=500)


@app.post("/ingest/demo")
async def run_demo_ingest():
    try:
        data = await run_in_threadpool(ingest_demo)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    if data.get("ok"):
        return RedirectResponse(url="/", status_code=303)
    return JSONResponse(data, status_code=500)


@app.get("/favicon.ico")
def favicon():
    # Avoid 404 noise
    return PlainTextResponse("", status_code=204)
    
