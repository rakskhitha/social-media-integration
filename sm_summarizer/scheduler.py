"""Social Media Daily digest scheduler.

One APScheduler cron job per user with saved settings -- each fires once a day at that user's
configured time. It searches today's social media posts for their saved keywords, handles, 
and channels, summarizes the results, and emails the digest.
"""
import logging
from datetime import date, datetime, timezone, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from . import digest_store
from .emailer import EmailError, send_digest_email
from .summarizer_service import search_and_summarize

logger = logging.getLogger("sm_summarizer.scheduler")

_DEFAULT_HOUR = 7
_DEFAULT_MINUTE = 0
_DEFAULT_TZ = "Asia/Kolkata"

_scheduler = None

def _job_id(user_id: int) -> str:
    return f"sm_daily_digest_{user_id}"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_hhmm(value: str) -> tuple[int, int]:
    hour_str, minute_str = value.split(":")
    return int(hour_str), int(minute_str)

def _run_digest_job(user_id: int, *, force: bool = False) -> dict | None:
    settings = digest_store.load_settings(user_id)
    if not settings or (not force and not settings.get("enabled")):
        logger.info("Social media daily digest skipped for user_id=%s: not configured or disabled", user_id)
        return None

    keyword = settings.get("keyword", "")
    handles = settings.get("handles", [])
    channels = settings.get("channels", [])
    today = date.today().isoformat()
    mode = settings.get("mode", "summary")

    handles_str = ", ".join(handles) if isinstance(handles, list) else str(handles)
    kw_label_parts = [p for p in [handles_str, keyword] if p]
    display_keyword = " & ".join(kw_label_parts) if kw_label_parts else "Social Media Digest"

    logger.info(
        "running SM daily digest for user_id=%s: keyword=%r date=%s handles=%s channels=%s mode=%s force=%s",
        user_id, keyword, today, handles, channels, mode, force,
    )

    entry = {"date": today, "keyword": display_keyword, "sent_at": _now_iso(), "count": 0}
    
    if not keyword and not handles:
        entry.update(status="failed", error="Please specify a Keyword or Target Handle before running digest.")
        return digest_store.append_history(user_id, entry)

    if not settings.get("to_emails"):
        entry.update(status="failed", error="Please select an analyst or enter a custom email before running digest.")
        return digest_store.append_history(user_id, entry)

    try:
        fetch_limit = 100 if mode in ("detailed", "complete") else 20
        result = search_and_summarize(
            keyword=keyword, 
            start_date=today, 
            end_date=today, 
            limit=fetch_limit,
            handles=handles, 
            channels=channels,
            mode=mode,
        )
    except Exception as exc:
        logger.exception("SM daily digest search failed for user_id=%s keyword=%r", user_id, keyword)
        entry.update(status="failed", error=str(exc))
        return digest_store.append_history(user_id, entry)

    entry["count"] = result.get("count", 0)
    all_sources = result.get("all_sources") or []
    
    if entry["count"] == 0 and not all_sources:
        entry["status"] = "no_results"
        logger.info("SM daily digest: no matching posts today for user_id=%s %r", user_id, keyword)
        return digest_store.append_history(user_id, entry)

    try:
        send_digest_email(
            to_emails=settings["to_emails"],
            keyword=display_keyword,
            report_date=today,
            summary=result.get("summary", ""),
            events=result.get("events", []),
            all_sources=all_sources,
        )
        entry["status"] = "sent"
    except EmailError as exc:
        entry["status"] = "failed"
        entry["error"] = str(exc)

    return digest_store.append_history(user_id, entry)


def _create_trigger(settings: dict):
    schedule_type = settings.get("schedule_type", "daily")
    tz = settings.get("timezone", _DEFAULT_TZ)
    if schedule_type == "interval":
        hours = settings.get("interval_hours", 24)
        return IntervalTrigger(hours=hours, timezone=tz)
    else:
        hour, minute = _parse_hhmm(settings.get("send_time", f"{_DEFAULT_HOUR:02d}:{_DEFAULT_MINUTE:02d}"))
        return CronTrigger(hour=hour, minute=minute, timezone=tz)

def start() -> None:
    global _scheduler
    if _scheduler is not None:
        return
    _scheduler = BackgroundScheduler()
    for settings in digest_store.list_all_settings():
        user_id = settings["user_id"]
        trigger = _create_trigger(settings)
        
        _scheduler.add_job(
            _run_digest_job,
            args=[user_id],
            trigger=trigger,
            id=_job_id(user_id),
            replace_existing=True,
        )
        logger.info("SM digest scheduled for user_id=%s with type=%s", user_id, settings.get("schedule_type", "daily"))
    _scheduler.start()


def stop() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def reschedule(user_id: int, settings: dict) -> None:
    if _scheduler is None:
        return
    trigger = _create_trigger(settings)
    job_id = _job_id(user_id)
    if _scheduler.get_job(job_id) is None:
        _scheduler.add_job(_run_digest_job, args=[user_id], trigger=trigger, id=job_id)
    else:
        _scheduler.reschedule_job(job_id, trigger=trigger)
    logger.info("SM digest rescheduled for user_id=%s", user_id)


def run_now(user_id: int) -> dict | None:
    return _run_digest_job(user_id, force=True)
