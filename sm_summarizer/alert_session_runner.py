"""Window-scoped processing pipeline for Social Media Alert Sessions.
Anchored to local midnight in the session's timezone with grid boundary math,
persistent last_window_end watermarking, LLM summary generation, and multi-recipient email delivery.
"""

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from sm_summarizer import alert_session_store as store
from sm_summarizer.summarizer_service import fetch_social_media_posts, _generate_llm_summary
from sm_summarizer.emailer import send_report_email, EmailError

logger = logging.getLogger("sm_summarizer.alert_session_runner")

_CANDIDATE_LIMIT = 200
_DISPLAY_LIMIT = 20

def _parse_hhmm(value: str) -> int:
    """'HH:MM' -> minutes since midnight."""
    hours, minutes = value.split(":")
    return int(hours) * 60 + int(minutes)

def session_times_minutes(session: dict) -> list[int]:
    """The session's alert times as a sorted, deduped list of minutes-since-midnight."""
    custom_times = session.get("custom_times")
    if custom_times:
        return sorted({0, *(_parse_hhmm(t) for t in custom_times)})
    interval_hours = int(session.get("interval_hours", 24))
    return list(range(0, 24 * 60, interval_hours * 60))

def grid_boundary_at_or_before(dt: datetime, times_minutes: list[int], tz: ZoneInfo) -> datetime:
    """The most recent grid boundary at or before dt anchored to local midnight."""
    local = dt.astimezone(tz)
    day_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_minutes = (local - day_start).total_seconds() / 60
    candidates = [m for m in times_minutes if m <= elapsed_minutes]
    if candidates:
        return day_start + timedelta(minutes=max(candidates))
    return day_start - timedelta(days=1) + timedelta(minutes=max(times_minutes))

def next_boundary_after(boundary: datetime, times_minutes: list[int], tz: ZoneInfo) -> datetime:
    """The next grid boundary strictly after boundary."""
    local = boundary.astimezone(tz)
    day_start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_minutes = (local - day_start).total_seconds() / 60
    later = [m for m in times_minutes if m > elapsed_minutes]
    if later:
        return day_start + timedelta(minutes=min(later))
    return day_start + timedelta(days=1) + timedelta(minutes=min(times_minutes))

def daily_alert_times(session: dict) -> list[str]:
    """['00:00', '06:00', '12:00', '18:00']-style list of alert times in a day for this session."""
    return [f"{m // 60:02d}:{m % 60:02d}" for m in session_times_minutes(session)]

def _filter_posts_to_window(posts: list[dict], window_start: datetime, window_end: datetime) -> list[dict]:
    """Filters fetched posts strictly to published date in [window_start, window_end)."""
    kept = []
    for p in posts:
        dt = p.get("date")
        if not dt:
            continue
        if isinstance(dt, str):
            try:
                dt = datetime.fromisoformat(dt)
            except Exception:
                continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        if window_start <= dt < window_end:
            kept.append(p)
    return kept

def _search_window(session: dict, window_start: datetime, window_end: datetime) -> dict:
    """Executes social media post fetch and filters to window."""
    keyword = session.get("keyword", "")
    handles = session.get("handles") or []
    channels = session.get("channels") or []
    mode = session.get("mode", "summary")

    if not keyword and not handles:
        return {
            "summary": "No keywords or handles specified for this alert session.",
            "count": 0,
            "posts": []
        }

    start_date = window_start.strftime("%Y-%m-%d")
    end_date = window_end.strftime("%Y-%m-%d")

    # Fetch candidate pool
    all_posts = fetch_social_media_posts(
        keyword=keyword,
        start_date=start_date,
        end_date=end_date,
        handles=handles,
        channels=channels,
        limit=_CANDIDATE_LIMIT
    )

    window_posts = _filter_posts_to_window(all_posts, window_start, window_end)
    display_posts = window_posts[:_DISPLAY_LIMIT]

    if not display_posts:
        return {
            "summary": "No matching social media posts were published in this time window.",
            "count": 0,
            "posts": []
        }

    summary_text = _generate_llm_summary(
        keyword=keyword,
        posts=display_posts,
        mode=mode,
        handles=handles,
        start_date=start_date,
        end_date=end_date
    )

    return {
        "summary": summary_text,
        "count": len(display_posts),
        "posts": display_posts
    }

def _process_one_window(session: dict, window_start: datetime, window_end: datetime) -> dict:
    """Processes a single window: searches DB, generates LLM report, sends emails, logs run & deliveries."""
    session_id = session["id"]

    existing = store.find_non_failed_run(session_id, window_start, window_end)
    if existing is not None:
        logger.info("Alert session %s window %s..%s already processed (run %s); skipping",
                    session_id, window_start, window_end, existing["id"])
        return existing

    config_snapshot = {
        "name": session.get("name"),
        "keyword": session.get("keyword"),
        "handles": session.get("handles"),
        "channels": session.get("channels"),
        "to_emails": session.get("to_emails"),
        "schedule_mode": session.get("schedule_mode"),
        "interval_hours": session.get("interval_hours"),
        "custom_times": session.get("custom_times"),
        "timezone": session.get("timezone"),
        "mode": session.get("mode")
    }

    try:
        search_res = _search_window(session, window_start, window_end)
    except Exception as exc:
        logger.exception("Alert session %s window %s..%s search failed", session_id, window_start, window_end)
        return store.create_run(
            session_id=session_id,
            window_start=window_start,
            window_end=window_end,
            status="failed",
            error=str(exc),
            config_snapshot=config_snapshot
        )

    posts = search_res.get("posts", [])
    post_count = len(posts)
    initial_status = "no_results" if post_count == 0 else "sent"
    summary_text = search_res.get("summary", "")

    run = store.create_run(
        session_id=session_id,
        window_start=window_start,
        window_end=window_end,
        status=initial_status,
        post_count=post_count,
        summary=summary_text,
        config_snapshot=config_snapshot
    )
    run_id = run["id"]

    if posts:
        store.create_run_posts(run_id, posts)

    to_emails = session.get("to_emails") or []
    deliveries = []

    if to_emails and post_count > 0:
        # Prepare sources list for emailer
        sources_list = []
        for i, p in enumerate(posts, start=1):
            if hasattr(p['date'], 'strftime'):
                d_str = p['date'].strftime('%d/%m/%Y')
            else:
                raw_d = str(p.get('date', ''))[:10]
                parts = raw_d.split('-')
                d_str = f"{parts[2]}/{parts[1]}/{parts[0]}" if len(parts) == 3 else raw_d
            sources_list.append({
                "num": i,
                "platform": p.get("platform", ""),
                "author": p.get("author", ""),
                "date": d_str,
                "url": p.get("url", ""),
                "snippet": str(p.get("content", ""))[:200].replace("\n", " ")
            })

        date_str = f"{window_start.strftime('%d/%m/%Y %H:%M')} .. {window_end.strftime('%d/%m/%Y %H:%M %Z')}"
        display_label = session.get("name", "Alert Session")

        for email in to_emails:
            try:
                send_report_email(
                    to_emails=[email],
                    keyword=display_label,
                    date_from=window_start.strftime('%d/%m/%Y %H:%M'),
                    date_to=window_end.strftime('%d/%m/%Y %H:%M'),
                    summary=summary_text,
                    events=[],
                    all_sources=sources_list
                )
                deliveries.append({"recipient_email": email, "status": "sent", "error": None})
            except Exception as e:
                logger.error("Failed to send session email to %s: %s", email, e)
                deliveries.append({"recipient_email": email, "status": "failed", "error": str(e)})

        store.create_run_deliveries(run_id, deliveries)

    return run

def _calculate_initial_window(session: dict, tz: ZoneInfo) -> tuple[datetime, datetime]:
    """Calculates the full initial lookback window for a brand new session."""
    now_local = datetime.now(tz)
    lookback = session.get("lookback") or "24h"
    start_date = session.get("start_date")
    end_date = session.get("end_date")

    if start_date:
        if isinstance(start_date, str):
            w_start = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(hour=0, minute=0, second=0, tzinfo=tz)
        else:
            w_start = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=tz)
        
        if end_date:
            if isinstance(end_date, str):
                w_end = datetime.strptime(end_date[:10], "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=tz)
            else:
                w_end = datetime.combine(end_date, datetime.max.time()).replace(tzinfo=tz)
            if w_end > now_local:
                w_end = now_local
        else:
            w_end = now_local
        return w_start, w_end

    if lookback == "7d":
        w_start = now_local - timedelta(days=7)
    elif lookback == "48h":
        w_start = now_local - timedelta(hours=48)
    elif lookback == "24h":
        w_start = now_local - timedelta(hours=24)
    else:
        interval_h = int(session.get("interval_hours", 24))
        w_start = now_local - timedelta(hours=interval_h)
    
    return w_start, now_local

def process_due_windows(session_id: int) -> list[dict]:
    """Self-healing catchup loop that advances last_window_end sequentially up to current grid boundary.
    If the session has not completed its initial lookback run, it executes the full initial lookback window
    (e.g. 7 days) as a single comprehensive report, then marks initial_run_done = True and sets watermark."""
    session = store.get_session(session_id)
    if not session or not session.get("enabled"):
        return []

    tz_name = session.get("timezone", "Asia/Kolkata")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Asia/Kolkata")

    times_min = session_times_minutes(session)
    now_dt = datetime.now(timezone.utc)
    current_grid_end = grid_boundary_at_or_before(now_dt, times_min, tz)

    # 1. INITIAL RUN: If not completed yet, run the entire lookback window as a single unified report
    if not session.get("initial_run_done"):
        w_start, w_end = _calculate_initial_window(session, tz)
        logger.info("Executing initial lookback report for alert session %s (%s): %s .. %s",
                    session_id, session.get("name"), w_start, w_end)
        run = _process_one_window(session, w_start, w_end)
        # Mark initial run done and anchor watermark to current grid end so recurring runs take over
        store.mark_initial_run_done(session_id, current_grid_end)
        return [run]

    # 2. RECURRING RUNS: Advance from watermark up to current grid boundary
    last_w_end = session.get("last_window_end")
    if not last_w_end:
        window_start = current_grid_end - timedelta(hours=int(session.get("interval_hours", 24)))
    else:
        if isinstance(last_w_end, str):
            try:
                window_start = datetime.fromisoformat(last_w_end)
            except Exception:
                window_start = current_grid_end - timedelta(hours=24)
        else:
            window_start = last_w_end

    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=timezone.utc)

    if window_start >= current_grid_end:
        return []

    processed_runs = []
    while window_start < current_grid_end:
        window_end = next_boundary_after(window_start, times_min, tz)
        if window_end > current_grid_end:
            break

        logger.info("Processing due recurring window for alert session %s (%s): %s .. %s",
                    session_id, session.get("name"), window_start, window_end)

        run = _process_one_window(session, window_start, window_end)
        processed_runs.append(run)

        # Advance watermark
        store.update_session_watermark(session_id, window_end)
        window_start = window_end

    return processed_runs

def run_session_now(session_id: int) -> dict | None:
    """Instant test run execution. If initial lookback has not run, runs the full lookback range."""
    session = store.get_session(session_id)
    if not session:
        return None

    tz_name = session.get("timezone", "Asia/Kolkata")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Asia/Kolkata")

    times_min = session_times_minutes(session)
    now_dt = datetime.now(timezone.utc)
    current_grid_end = grid_boundary_at_or_before(now_dt, times_min, tz)

    if not session.get("initial_run_done"):
        w_start, w_end = _calculate_initial_window(session, tz)
        logger.info("Triggering initial run for alert session %s (%s): %s .. %s",
                    session_id, session.get("name"), w_start, w_end)
        run = _process_one_window(session, w_start, w_end)
        store.mark_initial_run_done(session_id, current_grid_end)
        return run

    # Already initialized: run for current window from watermark or interval
    last_w_end = session.get("last_window_end")
    if last_w_end:
        if isinstance(last_w_end, str):
            try:
                window_start = datetime.fromisoformat(last_w_end)
            except Exception:
                interval_h = int(session.get("interval_hours", 24))
                window_start = current_grid_end - timedelta(hours=interval_h)
        else:
            window_start = last_w_end
    else:
        interval_h = int(session.get("interval_hours", 24))
        window_start = current_grid_end - timedelta(hours=interval_h)

    window_end = current_grid_end if current_grid_end > window_start else datetime.now(tz)

    logger.info("Triggering run_session_now for alert session %s (%s): %s .. %s",
                session_id, session.get("name"), window_start, window_end)

    run = _process_one_window(session, window_start, window_end)
    store.update_session_watermark(session_id, window_end)
    return run
