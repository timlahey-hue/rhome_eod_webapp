from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import logging

from .ingest import ingest_live, ingest_demo, check_api

log = logging.getLogger("uvicorn")

app = FastAPI()

# Static + templates (keep your existing templates/static folders)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # Keep your existing index.html; this route just renders it
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest/check")
def ingest_check():
    data = check_api()
    # Always return 200 with diagnostic info to avoid noisy 500s
    return JSONResponse(data)

@app.post("/ingest/live")
def ingest_live_route():
    data = ingest_live()
    # If it’s OK, bounce back to home so your UI refreshes; else return JSON with details
    if data.get("ok"):
        return RedirectResponse(url="/", status_code=303)
    return JSONResponse(data, status_code=500)

@app.post("/ingest/demo")
def ingest_demo_route():
    data = ingest_demo()
    if data.get("ok"):
        return RedirectResponse(url="/", status_code=303)
    return JSONResponse(data, status_code=500)
    
