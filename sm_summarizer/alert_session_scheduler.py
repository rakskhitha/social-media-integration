"""Background ticker for Social Media Alert Sessions.
Checks every 60 seconds for due session windows across all active sessions
and triggers process_due_windows() automatically.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sm_summarizer import alert_session_store as store
from sm_summarizer import alert_session_runner as runner

logger = logging.getLogger("sm_summarizer.alert_session_scheduler")

_scheduler: BackgroundScheduler | None = None
_JOB_ID = "sm_alert_session_ticker"

def _tick():
    """Background tick: iterate active sessions and process due windows."""
    try:
        sessions = store.list_all_sessions()
        for session in sessions:
            if not session.get("enabled", True):
                continue
            try:
                runner.process_due_windows(session["id"])
            except Exception as e:
                logger.exception("Error processing alert session %s during tick: %s", session.get("id"), e)
    except Exception as e:
        logger.exception("Error during alert session ticker tick: %s", e)

def start():
    """Start the 60s background alert session ticker."""
    global _scheduler
    if _scheduler is not None:
        return
    _scheduler = BackgroundScheduler()
    _scheduler.add_job(
        _tick,
        trigger=IntervalTrigger(seconds=60),
        id=_JOB_ID,
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("Social Media Alert Sessions ticker started (checking every 60s).")

def stop():
    """Stop the background alert session ticker."""
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Social Media Alert Sessions ticker stopped.")
