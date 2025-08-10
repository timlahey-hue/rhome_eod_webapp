# r:home — Web EOD Dashboard (FastAPI)

A web dashboard that shows your end-of-day (EOD) project snapshot and lets you **push to Slack** on demand.
Start with **demo data**, then flip to **live** with your Simpro OAuth2 Client Credentials.

**Security note:** Your uploaded doc included client credentials; please **rotate your Simpro client secret** in Simpro before going live. Do not paste secrets into chat; fill `.env` only. (Details referenced from your file.)

## Quick start (demo mode)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
# visit http://127.0.0.1:8000
```
Click **Run Demo Ingest** to seed a snapshot.

## Switch to live (Simpro)
Edit `.env` with your values:
```
SIMPRO_BASE_URL=https://rhome.simprosuite.com
SIMPRO_CLIENT_ID=...
SIMPRO_CLIENT_SECRET=...
SIMPRO_COMPANY_ID=   # optional
SLACK_WEBHOOK_URL=   # optional (for Share to Slack button)
TZ=America/Chicago
```
Then in the web UI, click **Run Live Ingest**. If credentials are good, the app will fetch jobs and create a new snapshot (we’ll expand to cost centers, invoices, POs/receipts, schedules next).

## Schedule daily at 5:00 pm CT (optional)
Two options are provided in `ops/`:
- **systemd timer** (`eod.service` + `eod.timer`)
- **cron** (`cron.txt` uses America/Chicago TZ)

## Structure
- `app/main.py`         — FastAPI app + routes
- `app/db.py`           — SQLite schema & helpers
- `app/metrics.py`      — business logic (burn %, GM%, at-risk, exceptions)
- `app/ingest.py`       — demo + live ingest
- `app/simpro.py`       — OAuth2 Client Credentials + API helpers
- `app/slack.py`        — Share to Slack (Incoming Webhook)
- `app/templates/`      — Jinja pages
- `app/static/`         — CSS
- `ops/`                — systemd + cron examples

---

## Notes
- Live ingest starts with **Companies + Jobs** to validate credentials/connectivity. We’ll add **Job Cost Centers, Invoices, Vendor Receipts, Schedules** next so “today” costs are exact.
- We save a **daily snapshot** so EOD numbers stay stable even if late edits occur later.
- Slack is **on-demand** only (button).
