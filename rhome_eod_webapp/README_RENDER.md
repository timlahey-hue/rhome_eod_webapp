# Deploy to Render — r:home EOD Web Dashboard

This blueprint creates:
1) A **web service** for the dashboard (FastAPI + Uvicorn) with a **persistent disk** for snapshots.
2) A **cron job** that runs the live ingest every day at **5:00 pm America/Chicago** (set as **22:00 UTC** here).

## 1) Put your code in GitHub (private)
```bash
cd ~/Downloads/rhome_eod_webapp
git init
git add .
git commit -m "r:home EOD web app"
# using GitHub CLI:
gh repo create rhome-eod-webapp --private --source=. --push
# or create a private repo manually and push
```

## 2) Add this file to the repo root
Save `render.yaml` in the **root** of the repo (same level as `requirements.txt`). Commit and push it:
```bash
git add render.yaml
git commit -m "Add Render blueprint"
git push
```

## 3) Deploy on Render
- In Render, click **New → Blueprint** and choose this repo.
- Render will detect `render.yaml` and create **two resources**:
  - Web service: `rhome-eod-dashboard`
  - Cron job: `eod-live-ingest`

### Set environment variables (don’t commit secrets)
In the Render dashboard, open each resource → **Environment** and set:
- `SIMPRO_BASE_URL = https://rhome.simprosuite.com`
- `SIMPRO_CLIENT_ID = <your client id>`
- `SIMPRO_CLIENT_SECRET = <your rotated client secret>`
- `SIMPRO_COMPANY_ID =` *(optional)*
- `SLACK_WEBHOOK_URL =` *(optional; for the Share to Slack button)*
- `TZ = America/Chicago`

> The blueprint marks secret vars with `sync: false`, so Render will prompt you to fill them in the UI.

### Persistent disk for snapshots
The web service mounts a 1 GB disk at `/data` and symlinks `eod.db` to it so your history survives restarts.

### About the schedule
The cron job runs at **22:00 UTC**, which is **5:00 pm** in **America/Chicago** during daylight time. If you want to keep 5 pm year‑round automatically, change the schedule seasonally or set the cron to **23:00 UTC** during standard time.

## 4) Share the link
Once deployed, Render gives you a URL like `https://rhome-eod-dashboard.onrender.com`. Share it with your team. Use the **Share to Slack** button only if `SLACK_WEBHOOK_URL` is set.

## 5) Security
If you want to keep the dashboard private, add simple HTTP Basic auth or SSO later. Keep Simpro credentials **only** in Render environment settings.
