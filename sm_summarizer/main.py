import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import os
import sys

# Ensure sm_summarizer is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path)

from sm_summarizer.summarizer_service import search_and_summarize, _normalize_to_iso_date
from sm_summarizer.digest_store import load_settings
from sm_summarizer import scheduler

app = FastAPI(title="Social Media Summarizer API")

@app.middleware("http")
async def add_cache_control_header(request: Request, call_next):
    response = await call_next(request)
    # Prevent caching for frontend files and handle library so clients always get fresh DB data
    if request.url.path.startswith("/api/handles") or request.url.path == "/" or request.url.path.endswith((".html", ".js", ".css")):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

from sm_summarizer import alert_session_scheduler

@app.on_event("startup")
def on_startup():
    scheduler.start()
    alert_session_scheduler.start()

@app.on_event("shutdown")
def on_shutdown():
    scheduler.stop()
    alert_session_scheduler.stop()

class GenerateReportRequest(BaseModel):
    keyword: Optional[str] = ""
    start_date: Optional[str] = ""
    end_date: Optional[str] = ""
    handles: List[str] = []
    channels: List[str] = []
    limit: int = 20
    depth: str = "summary"
    mode: Optional[str] = None

@app.post("/api/generate_report")
def generate_report(req: GenerateReportRequest):
    try:
        # Strict Date Validation: No future dates, start <= end
        today_str = datetime.now().strftime("%Y-%m-%d")
        if req.start_date:
            s_d = _normalize_to_iso_date(req.start_date)
            if s_d > today_str:
                raise HTTPException(status_code=400, detail="Invalid date: Start date cannot be in the future. Please select a date on or before today.")
        if req.end_date:
            e_d = _normalize_to_iso_date(req.end_date)
            if e_d > today_str:
                raise HTTPException(status_code=400, detail="Invalid date: End date cannot be in the future. Please select a date on or before today.")
        if req.start_date and req.end_date:
            s_d = _normalize_to_iso_date(req.start_date)
            e_d = _normalize_to_iso_date(req.end_date)
            if s_d > e_d:
                raise HTTPException(status_code=400, detail="Invalid date range: Start date cannot cross or be after End date.")

        report_mode = req.mode or req.depth or "summary"
        res = search_and_summarize(
            keyword=req.keyword or "",
            start_date=req.start_date or "",
            end_date=req.end_date or "",
            limit=req.limit,
            handles=req.handles,
            channels=req.channels,
            mode=report_mode
        )
        return res
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class DownloadPdfRequest(BaseModel):
    html: str
    filename: Optional[str] = "Event_Summary_Report.pdf"

@app.post("/api/download_pdf")
def download_pdf_endpoint(req: DownloadPdfRequest):
    import subprocess
    import tempfile
    import re
    import os

    raw_html = req.html or ""
    doc = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
  @page {{
    size: A4;
    margin: 12mm 15mm;
  }}
  * {{
    box-sizing: border-box;
    -webkit-print-color-adjust: exact !important;
    print-color-adjust: exact !important;
  }}
  body {{
    margin: 0;
    padding: 0;
    background: #18181b !important;
    color: #e4e4e7 !important;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif !important;
    font-size: 13px;
    line-height: 1.5;
  }}
  .reportHeader {{
    background: #27272a !important;
    border: 1px solid #3f3f46 !important;
    border-radius: 8px !important;
    padding: 14px 18px !important;
    margin-bottom: 16px !important;
  }}
  .reportTitle, #main-report-heading {{
    font-size: 22px !important;
    font-weight: 800 !important;
    color: #fff8e7 !important;
    letter-spacing: -0.3px !important;
    margin-bottom: 4px !important;
  }}
  #report-search-badge {{
    font-size: 13px !important;
    color: #93c5fd !important;
    margin-top: 4px !important;
    margin-bottom: 2px !important;
    display: block !important;
  }}
  #report-date-badge {{
    font-size: 11.5px !important;
    color: #a1a1aa !important;
    margin-top: 2px !important;
    margin-bottom: 4px !important;
  }}
  .reportBody {{
    margin-bottom: 20px !important;
  }}
  .summarySectionTitle, #res-sources-header {{
    font-size: 18px !important;
    font-weight: 800 !important;
    color: #fff8e7 !important;
    margin-top: 20px !important;
    margin-bottom: 10px !important;
    border-bottom: 1px solid #3f3f46 !important;
    padding-bottom: 6px !important;
  }}
  .sourceItem {{
    background: transparent !important;
    border-bottom: 1px solid #27272a !important;
    padding: 0 0 12px 0 !important;
    margin-bottom: 14px !important;
    page-break-inside: avoid !important;
    break-inside: avoid !important;
  }}
  .sourceTitle {{
    color: #fff8e7 !important;
    font-weight: 700 !important;
    font-size: 14px !important;
    line-height: 1.4 !important;
  }}
  .sourceMeta {{
    color: #a1a1aa !important;
    font-size: 12px !important;
    margin: 2px 0 4px 0 !important;
  }}
  a.source-title-link {{
    color: #fff8e7 !important;
    text-decoration: none !important;
    font-weight: 700 !important;
  }}
  .sourceUrl a, a.source-link {{
    color: #38bdf8 !important;
    text-decoration: underline !important;
    font-size: 12.5px !important;
    word-break: break-all !important;
  }}
  a.citation-link {{
    color: #38bdf8 !important;
    text-decoration: underline !important;
    font-weight: 700 !important;
    cursor: pointer !important;
  }}
  h1, h2, h3, h4, strong {{
    color: #fff8e7 !important;
  }}
  p, li, div {{
    color: #e4e4e7 !important;
  }}
  #send-report-section, button, .segment-btn {{
    display: none !important;
  }}
</style>
</head>
<body>
{raw_html}
</body>
</html>"""

    with tempfile.NamedTemporaryFile("w", suffix=".html", delete=False, encoding="utf-8") as f:
        f.write(doc)
        html_path = f.name

    pdf_path = html_path.replace(".html", ".pdf")

    try:
        cmd = [
            "/usr/bin/chromium",
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path}",
            html_path
        ]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
        if res.returncode != 0 or not os.path.exists(pdf_path):
            raise HTTPException(status_code=500, detail=f"PDF generation failed: {res.stderr.decode('utf-8', errors='ignore')}")

        with open(pdf_path, "rb") as f:
            pdf_bytes = f.read()

        clean_filename = re.sub(r'[^a-zA-Z0-9_\-\.]', '_', req.filename or 'Event_Summary_Report.pdf')
        if not clean_filename.endswith('.pdf'):
            clean_filename += '.pdf'

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="{clean_filename}"'
            }
        )
    finally:
        if os.path.exists(html_path):
            try: os.unlink(html_path)
            except Exception: pass
        if os.path.exists(pdf_path):
            try: os.unlink(pdf_path)
            except Exception: pass

class SendReportRequest(BaseModel):
    to_emails: List[str] = []
    keyword: Optional[str] = ""
    date_from: Optional[str] = ""
    date_to: Optional[str] = ""
    summary: Optional[str] = ""
    events: Optional[List[dict]] = []
    all_sources: Optional[List[dict]] = []

DEFAULT_ANALYSTS = [
    {"id": 101, "name": "Admin 1", "email": "admin@airspace.net"},
    {"id": 102, "name": "Admin2", "email": "admin2@osint.local"},
    {"id": 103, "name": "Beta Alpha", "email": "clidelhi96@gmail.com"},
    {"id": 104, "name": "Bindu", "email": "bindu@social.com"},
    {"id": 105, "name": "Bootstrap Admin", "email": "go.teamchai@gmail.com"},
    {"id": 106, "name": "Developer_testing", "email": "Developer@admin01.com"},
    {"id": 107, "name": "Max", "email": "Max@user.com"},
    {"id": 108, "name": "Raj", "email": "sharanraj3012@gmail.com"},
    {"id": 109, "name": "Rama", "email": "rama.1874@gov.in"},
    {"id": 110, "name": "Shreya Devadiga", "email": "shreyadevadiga2410@gmail.com"},
    {"id": 111, "name": "User01", "email": "User01@user01.com"},
    {"id": 112, "name": "User02", "email": "User02@user02.com"},
    {"id": 113, "name": "User03", "email": "User03@user03.com"},
    {"id": 114, "name": "User04", "email": "User04@user04.com"},
    {"id": 115, "name": "User05", "email": "User05@user05.com"},
    {"id": 116, "name": "User06", "email": "User06@user06.com"},
    {"id": 117, "name": "User07", "email": "User07@user07.com"},
    {"id": 118, "name": "User08", "email": "User08@user08.com"},
    {"id": 119, "name": "User09", "email": "User09@user09.com"},
    {"id": 120, "name": "User10", "email": "User10@user10.com"},
    {"id": 121, "name": "Analyst Desk Alpha", "email": "analyst_alpha@osint.local"},
    {"id": 122, "name": "Analyst Desk Beta", "email": "analyst_beta@osint.local"},
    {"id": 123, "name": "Analyst Desk Gamma", "email": "analyst_gamma@osint.local"},
    {"id": 124, "name": "Command Center", "email": "command_center@osint.local"},
    {"id": 125, "name": "Intelligence Director", "email": "director@osint.local"},
    {"id": 126, "name": "Lead Researcher", "email": "researcher@osint.local"},
    {"id": 127, "name": "Operations Lead", "email": "ops_lead@osint.local"},
    {"id": 128, "name": "Senior Analyst 1", "email": "sr_analyst1@osint.local"},
    {"id": 129, "name": "Senior Analyst 2", "email": "sr_analyst2@osint.local"},
    {"id": 130, "name": "Duty Officer", "email": "duty_officer@osint.local"}
]

@app.get("/api/users/alert_users")
@app.get("/api/users/alert-users")
def get_alert_users():
    users_list = list(DEFAULT_ANALYSTS)
    seen_emails = {u["email"].lower() for u in users_list}
    
    # 1. Fetch live from shaktidb_social (users table)
    try:
        import psycopg2
        from sm_summarizer.summarizer_service import DB_CONFIG
        social_config = dict(DB_CONFIG)
        social_config["connect_timeout"] = 5
        conn = psycopg2.connect(**social_config)
        cur = conn.cursor()
        cur.execute("SELECT id, name, email FROM users WHERE email IS NOT NULL AND email != '';")
        for r in cur.fetchall():
            uid, name, email = r[0], r[1] or "User", r[2]
            if email.lower() not in seen_emails:
                seen_emails.add(email.lower())
                users_list.append({"id": uid, "name": name, "email": email})
        cur.close()
        conn.close()
    except Exception:
        pass

    # 2. Fetch live from shaktidb_news (app_users table)
    try:
        import psycopg2
        from sm_summarizer.summarizer_service import DB_CONFIG
        news_config = dict(DB_CONFIG)
        news_config["database"] = "shaktidb_news"
        news_config["connect_timeout"] = 5
        conn = psycopg2.connect(**news_config)
        cur = conn.cursor()
        cur.execute("SELECT id, name, email FROM app_users WHERE email IS NOT NULL AND email != '';")
        for r in cur.fetchall():
            uid, name, email = r[0], r[1] or "User", r[2]
            if email.lower() not in seen_emails:
                seen_emails.add(email.lower())
                users_list.append({"id": uid, "name": name, "email": email})
        cur.close()
        conn.close()
    except Exception:
        pass
        
    return {"users": users_list}

@app.post("/api/send_report")
@app.post("/api/send-report")
def send_report(req: SendReportRequest):
    try:
        from sm_summarizer.emailer import send_report_email
        send_report_email(
            to_emails=req.to_emails,
            keyword=req.keyword,
            date_from=req.date_from,
            date_to=req.date_to,
            summary=req.summary,
            events=req.events,
            all_sources=req.all_sources
        )
        return {"sent": True, "message": f"Sent to {', '.join(req.to_emails)}."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class DigestSettings(BaseModel):
    user_id: int = 1
    enabled: bool = True
    keyword: str = ""
    handles: List[str] = []
    channels: List[str] = []
    to_emails: List[str] = []
    send_time: str = "08:00"
    timezone: str = "Asia/Kolkata"
    mode: str = "summary"

@app.get("/api/digest_settings/{user_id}")
@app.get("/api/digest/settings/{user_id}")
def get_digest_settings(user_id: int):
    from sm_summarizer.digest_store import load_settings
    settings = load_settings(user_id)
    if not settings:
        return {
            "user_id": user_id,
            "enabled": True,
            "keyword": "",
            "handles": [],
            "channels": ["twitter", "instagram", "facebook", "youtube", "reddit"],
            "to_emails": [],
            "send_time": "08:00",
            "timezone": "Asia/Kolkata",
            "mode": "summary"
        }
    return settings

@app.post("/api/digest_settings")
@app.put("/api/digest/settings")
def save_digest_settings(settings: DigestSettings):
    from sm_summarizer.digest_store import save_settings
    data = settings.dict()
    saved = save_settings(settings.user_id, data)
    scheduler.reschedule(settings.user_id, saved)
    return {"status": "success", "settings": saved}

@app.post("/api/digest/run_now/{user_id}")
@app.post("/api/digest/run-now/{user_id}")
def run_digest_now(user_id: int):
    result = scheduler.run_now(user_id)
    if not result:
        raise HTTPException(status_code=500, detail="Failed to run digest or no settings configured.")
    return result

@app.get("/api/digest/history/{user_id}")
def get_digest_history(user_id: int):
    from sm_summarizer.digest_store import load_history
    return load_history(user_id)

@app.delete("/api/digest/history/{user_id}/{entry_id}")
def delete_digest_history(user_id: int, entry_id: str):
    from sm_summarizer.digest_store import delete_history_entry
    deleted = delete_history_entry(user_id, entry_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="History entry not found.")
    return {"deleted": True}

class AlertSessionRequest(BaseModel):
    user_id: int = 1
    name: Optional[str] = "Alert Session"
    keyword: Optional[str] = ""
    handles: Optional[List[str]] = []
    channels: Optional[List[str]] = []
    to_emails: Optional[List[str]] = []
    schedule_mode: Optional[str] = "preset"
    interval_hours: Optional[int] = 24
    custom_times: Optional[List[str]] = []
    timezone: Optional[str] = "Asia/Kolkata"
    enabled: Optional[bool] = True
    mode: Optional[str] = "summary"
    lookback: Optional[str] = "24h"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

@app.get("/api/alert-sessions")
@app.get("/api/alert_sessions")
def list_alert_sessions(user_id: Optional[int] = None):
    from sm_summarizer import alert_session_store as store
    from sm_summarizer import alert_session_runner as runner
    from sm_summarizer import alert_session_scheduler as sched
    from zoneinfo import ZoneInfo
    from datetime import datetime, timezone

    sessions = store.list_all_sessions(user_id)
    result = []
    for s in sessions:
        times_min = runner.session_times_minutes(s)
        tz_name = s.get("timezone", "Asia/Kolkata")
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("Asia/Kolkata")
        
        now_dt = datetime.now(timezone.utc)
        current_w = runner.grid_boundary_at_or_before(now_dt, times_min, tz)
        next_w = runner.next_boundary_after(current_w, times_min, tz)

        latest_runs = store.list_runs(s["id"], limit=1)
        latest = latest_runs[0] if latest_runs else None

        s_dict = dict(s)
        s_dict["alert_times"] = runner.daily_alert_times(s)
        s_dict["next_run"] = next_w.isoformat()
        s_dict["lookback"] = s.get("lookback") or "24h"
        s_dict["start_date"] = str(s["start_date"]) if s.get("start_date") else None
        s_dict["end_date"] = str(s["end_date"]) if s.get("end_date") else None
        s_dict["initial_run_done"] = bool(s.get("initial_run_done", False))
        s_dict["last_run"] = {
            "id": latest["id"],
            "run_at": str(latest["run_at"]),
            "status": latest["status"],
            "post_count": latest.get("post_count", 0),
            "window_start": str(latest["window_start"]) if latest and latest.get("window_start") else None,
            "window_end": str(latest["window_end"]) if latest and latest.get("window_end") else None
        } if latest else None
        result.append(s_dict)
    return {"sessions": result}

@app.post("/api/alert-sessions")
@app.post("/api/alert_sessions")
def create_alert_session(req: AlertSessionRequest):
    from sm_summarizer import alert_session_store as store
    today_str = datetime.now().strftime("%Y-%m-%d")
    if req.start_date:
        s_d = _normalize_to_iso_date(req.start_date)
        if s_d > today_str:
            raise HTTPException(status_code=400, detail="Invalid date: Start date cannot be in the future. Please select a date on or before today.")
    if req.end_date:
        e_d = _normalize_to_iso_date(req.end_date)
        if e_d > today_str:
            raise HTTPException(status_code=400, detail="Invalid date: End date cannot be in the future. Please select a date on or before today.")
    if req.start_date and req.end_date:
        s_d = _normalize_to_iso_date(req.start_date)
        e_d = _normalize_to_iso_date(req.end_date)
        if s_d > e_d:
            raise HTTPException(status_code=400, detail="Invalid date range: Start date cannot cross or be after End date.")

    data = req.dict()
    session = store.create_session(data)
    return {"status": "success", "session": session}

@app.get("/api/alert-sessions/{session_id}")
@app.get("/api/alert_sessions/{session_id}")
def get_alert_session(session_id: int):
    from sm_summarizer import alert_session_store as store
    session = store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Alert session not found.")
    return session

@app.put("/api/alert-sessions/{session_id}")
@app.put("/api/alert_sessions/{session_id}")
def update_alert_session(session_id: int, req: AlertSessionRequest):
    from sm_summarizer import alert_session_store as store
    today_str = datetime.now().strftime("%Y-%m-%d")
    if req.start_date:
        s_d = _normalize_to_iso_date(req.start_date)
        if s_d > today_str:
            raise HTTPException(status_code=400, detail="Invalid date: Start date cannot be in the future. Please select a date on or before today.")
    if req.end_date:
        e_d = _normalize_to_iso_date(req.end_date)
        if e_d > today_str:
            raise HTTPException(status_code=400, detail="Invalid date: End date cannot be in the future. Please select a date on or before today.")
    if req.start_date and req.end_date:
        s_d = _normalize_to_iso_date(req.start_date)
        e_d = _normalize_to_iso_date(req.end_date)
        if s_d > e_d:
            raise HTTPException(status_code=400, detail="Invalid date range: Start date cannot cross or be after End date.")

    data = req.dict()
    updated = store.update_session(session_id, data)
    if not updated:
        raise HTTPException(status_code=404, detail="Alert session not found.")
    return {"status": "success", "session": updated}

@app.delete("/api/alert-sessions/{session_id}")
@app.delete("/api/alert_sessions/{session_id}")
def delete_alert_session(session_id: int):
    from sm_summarizer import alert_session_store as store
    deleted = store.delete_session(session_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Alert session not found.")
    return {"deleted": True}

@app.post("/api/alert-sessions/{session_id}/run-now")
@app.post("/api/alert-sessions/{session_id}/run_now")
@app.post("/api/alert_sessions/{session_id}/run_now")
def run_alert_session_now(session_id: int):
    from sm_summarizer import alert_session_runner as runner
    run = runner.run_session_now(session_id)
    if not run:
        raise HTTPException(status_code=500, detail="Failed to run alert session.")
    return run

@app.get("/api/alert-sessions/{session_id}/history")
@app.get("/api/alert_sessions/{session_id}/history")
def get_alert_session_history(session_id: int, limit: int = 50):
    from sm_summarizer import alert_session_store as store
    return store.list_runs(session_id, limit=limit)

# --- HANDLE LIBRARY API ENDPOINTS ---

class AddHandleRequest(BaseModel):
    platform: str
    handle: str
    display_name: str | None = None
    category: str | None = "Custom"

@app.get("/api/handles")
@app.get("/api/handles/{platform}")
def get_platform_handles(platform: str = "all"):
    from sm_summarizer import handle_store
    return {"handles": handle_store.list_handles(platform)}

@app.post("/api/handles/custom")
def add_custom_platform_handle(req: AddHandleRequest):
    from sm_summarizer import handle_store
    item = handle_store.add_custom_handle(req.platform, req.handle, req.display_name, req.category)
    return {"status": "success", "handle": item}

@app.delete("/api/handles/custom/{handle_id}")
def delete_custom_platform_handle(handle_id: int):
    from sm_summarizer import handle_store
    deleted = handle_store.delete_handle(handle_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Handle not found.")
    return {"deleted": True}

# Ensure frontend dir exists, otherwise mount will fail
frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
os.makedirs(frontend_dir, exist_ok=True)
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
