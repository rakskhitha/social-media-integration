"""SMTP email delivery for social media reports and daily digests."""

import html
import logging
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import markdown

logger = logging.getLogger("sm_summarizer.emailer")

class EmailError(RuntimeError):
    """Raised when SMTP configuration is missing or the send fails."""

def _build_post_url(event: dict) -> str:
    url = str(event.get("url") or "").strip()
    if url and url != "#":
        return url
    plat = str(event.get("platform") or "").lower()
    auth = str(event.get("author") or "").lstrip("@").lstrip("u/").strip()
    if plat in ("twitter", "x"):
        return f"https://x.com/{auth}" if auth else "https://x.com"
    elif plat == "youtube":
        return f"https://www.youtube.com/@{auth}" if auth else "https://www.youtube.com"
    elif plat == "instagram":
        return f"https://www.instagram.com/{auth}" if auth else "https://www.instagram.com"
    elif plat == "facebook":
        return f"https://www.facebook.com/{auth}" if auth else "https://www.facebook.com"
    elif plat == "reddit":
        return f"https://www.reddit.com/user/{auth}" if auth else "https://www.reddit.com"
    elif plat == "linkedin":
        return f"https://www.linkedin.com/in/{auth}" if auth else "https://www.linkedin.com"
    return ""

def _summary_html(summary: str, events: list[dict] = None) -> str:
    if not summary:
        return "<p style='color:#64748b; font-style:italic;'>No summary content provided.</p>"

    url_map = {}
    if events and isinstance(events, list):
        for index, event in enumerate(events, start=1):
            num = str(event.get("num") or index)
            u = _build_post_url(event)
            if u:
                url_map[num] = u

    def get_citation_link(num: str, label: str = None) -> str:
        text = label or f"#{num}"
        u = url_map.get(str(num), "")
        if u:
            escaped = html.escape(u, quote=True)
            return f'<a href="{escaped}" target="_blank" rel="noopener noreferrer" style="color:#2563eb; text-decoration:underline; font-weight:700;">{text}</a>'
        return f'<span style="color:#2563eb; font-weight:700;">{text}</span>'

    # 1. Bracketed citations: [1], [#1], [1, 2]
    def repl_bracket(m):
        inner = m.group(1)
        subbed = re.sub(r'#?(\d+)', lambda nm: get_citation_link(nm.group(1), f"#{nm.group(1)}"), inner)
        return f'[{subbed}]'
    s = re.sub(r'\[([#\d\s,]+)\]', repl_bracket, summary)

    # 2. Parenthesized citations: (1, 5), (1), (see #1, #2)
    def repl_paren(m):
        inner = m.group(1)
        if '<a' in inner:
            return m.group(0)
        subbed = re.sub(r'#?(\d+)', lambda nm: get_citation_link(nm.group(1), nm.group(1)), inner)
        return f'({subbed})'
    s = re.sub(r'\(((?:see\s+|sources?:\s*)?(?:#?\d+\s*,\s*)*#?\d+\s*)\)', repl_paren, s, flags=re.I)

    # 3. Standalone #N (ensure not preceded by >, /, #, or alphanumeric, and not inside a tag)
    s = re.sub(r'(^|[^>\w#/])#(\d+)\b(?![^<]*>)(?!<\/a>)', lambda m: m.group(1) + get_citation_link(m.group(2), f"#{m.group(2)}"), s)

    return markdown.markdown(s, extensions=["sane_lists"])

def _format_date_ddmmyyyy(val: str, keep_time: bool = False) -> str:
    """Converts any date string (e.g. '2026-10-13', '2026-10-13 00:00:00') into DD/MM/YYYY."""
    if not val:
        return ""
    val_str = str(val).strip()
    if re.match(r"^\d{2}/\d{2}/\d{4}", val_str):
        return val_str
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{2}:\d{2}(?::\d{2})?))?", val_str)
    if m:
        year, month, day, time_part = m.groups()
        if keep_time and time_part and time_part not in ("00:00:00", "00:00", "23:59:59"):
            return f"{day}/{month}/{year} {time_part[:5]}"
        return f"{day}/{month}/{year}"
    m2 = re.match(r"^(\d{2})-(\d{2})-(\d{4})", val_str)
    if m2:
        return f"{m2.group(1)}/{m2.group(2)}/{m2.group(3)}"
    return val_str

def _format_date_range_ddmmyyyy(d_from: str, d_to: str) -> str:
    f = _format_date_ddmmyyyy(d_from)
    t = _format_date_ddmmyyyy(d_to)
    if f and t and f != t:
        return f"{f} to {t}"
    return f or t or ""

def _sources_html(events: list[dict]) -> str:
    if not events:
        return '<p style="color:#888;">No matching posts found.</p>'
    items = []
    for index, event in enumerate(events, start=1):
        num = event.get("num") or index
        author = html.escape(str(event.get("author") or "unknown"))
        platform = html.escape(str(event.get("platform") or "social").upper())
        url = _build_post_url(event)
        escaped_url = html.escape(url, quote=True)
        raw_date = str(event.get("date") or "")
        date_str = html.escape(_format_date_ddmmyyyy(raw_date))
        snippet = str(event.get("snippet") or event.get("content") or "").strip()
        clean_snippet = snippet.replace('\n', ' ')[:130]
        if len(snippet) > 130:
            clean_snippet += "..."
        headline = html.escape(clean_snippet) if clean_snippet else f"@{author}"
        title_line = f"{num}. {headline} - @{author} ({platform})"

        domain = ""
        if url and url != "#":
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.lower().replace("www.", "")
            except Exception:
                domain = platform.lower()
        if not domain:
            domain = platform.lower()

        if url and url != "#":
            title_html = f'<a href="{escaped_url}" target="_blank" rel="noopener noreferrer" style="color:#0f172a; text-decoration:none; font-weight:700; font-size:14.5px; line-height:1.4; display:inline-block;">{title_line} <span style="color:#2563eb; font-size:12px;">&#8599;</span></a>'
            domain_html = f'<a href="{escaped_url}" target="_blank" rel="noopener noreferrer" style="color:#64748b; font-size:12.5px; text-decoration:none; font-family:monospace;">{domain} — {date_str}</a>'
            url_html = f'<a href="{escaped_url}" target="_blank" rel="noopener noreferrer" style="color:#2563eb; text-decoration:underline; font-size:13px; word-break:break-all;">{escaped_url}</a>'
        else:
            title_html = f'<span style="font-weight:700; font-size:14.5px; color:#0f172a;">{title_line}</span>'
            domain_html = f'<span style="color:#64748b; font-size:12.5px; font-family:monospace;">{domain} — {date_str}</span>'
            url_html = '<span style="color:#94a3b8; font-size:12px;">No direct URL</span>'

        items.append(
            f'<div id="source-{num}" style="margin-bottom:18px; line-height:1.45; border-bottom:1px solid #e2e8f0; padding-bottom:12px;">'
            f'<div style="margin-bottom:3px;">{title_html}</div>'
            f'<div style="margin-bottom:4px;">{domain_html}</div>'
            f'<div>{url_html}</div>'
            f'</div>'
        )
    return "".join(items)

def _report_html(title: str, search_query: str, date_label: str, summary: str, events: list[dict]) -> str:
    from datetime import datetime
    generated_now = datetime.now().strftime("%d/%m/%Y %H:%M") + " IST"
    search_line = f'<div style="color:#475569; font-size:13.5px; margin-bottom:3px;">Search: <strong>{html.escape(search_query)}</strong></div>' if search_query else ''
    return f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 720px; margin: 0 auto; color: #0f172a; background: #ffffff; padding: 28px; border: 1px solid #e2e8f0; border-radius: 8px;">
  <div style="border-bottom: 2px solid #2563eb; padding-bottom: 12px; margin-bottom: 18px;">
    <h1 style="margin: 0 0 6px 0; font-size: 24px; font-weight: 800; color: #0f172a;">{html.escape(title)}</h1>
    {search_line}
    <div style="color: #64748b; font-size: 13px;">Date range: {html.escape(date_label)} &nbsp;|&nbsp; Generated: {generated_now}</div>
  </div>
  <h2 style="font-size: 18px; font-weight: 700; color: #0f172a; margin-top: 18px; margin-bottom: 10px;">Summary</h2>
  <div style="line-height: 1.6; font-size: 14.5px; color: #334155;">{_summary_html(summary, events)}</div>
  <h2 style="margin-top: 26px; margin-bottom: 14px; font-size: 18px; font-weight: 700; border-top: 1px solid #e2e8f0; padding-top: 16px; color: #0f172a;">
    Sources ({len(events)})
  </h2>
  <div>{_sources_html(events)}</div>
  <div style="margin-top: 28px; padding-top: 14px; border-top: 1px solid #e2e8f0; color: #94a3b8; font-size: 11px;">
    Auto-generated by Social Media Intelligence Summarizer.
  </div>
</div>
"""

def _send_smtp(to_emails: list[str], subject: str, plain_text: str, html_content: str) -> None:
    from dotenv import load_dotenv
    env_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_file):
        load_dotenv(env_file, override=True)

    user = os.environ.get("SMTP_USER", "").strip()
    password = os.environ.get("SMTP_PASSWORD", "").strip()
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com").strip()
    port = int(os.environ.get("SMTP_PORT", "465"))

    if not user or not password:
        logger.error("SMTP credentials (SMTP_USER/SMTP_PASSWORD) are missing in sm_summarizer/.env")
        raise EmailError("SMTP credentials not configured in sm_summarizer/.env")

    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = user
    message["To"] = ", ".join(to_emails)
    message.attach(MIMEText(plain_text, "plain"))
    message.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP_SSL(host, port, timeout=30) as server:
            server.login(user, password)
            server.sendmail(user, to_emails, message.as_string())
        logger.info(f"Email successfully sent to {to_emails}")
    except Exception as exc:
        logger.error(f"Failed to send email to {to_emails}: {exc}")
        raise EmailError(str(exc)) from exc

def send_digest_email(to_emails: list[str], keyword: str, report_date: str, summary: str, events: list[dict] = None, all_sources: list[dict] = None):
    sources = all_sources if (all_sources is not None and len(all_sources) > 0) else (events or [])
    formatted_date = _format_date_ddmmyyyy(report_date)
    html_body = _report_html("Daily Digest", keyword, formatted_date, summary, sources)
    _send_smtp(to_emails, f"Daily Digest: {keyword} — {formatted_date}", summary, html_body)

def send_report_email(to_emails: list[str], keyword: str, date_from: str, date_to: str, summary: str, events: list[dict] = None, all_sources: list[dict] = None):
    sources = all_sources if (all_sources is not None and len(all_sources) > 0) else (events or [])
    date_range = _format_date_range_ddmmyyyy(date_from, date_to)
    html_body = _report_html("Event Summary Report", keyword, date_range, summary, sources)
    _send_smtp(to_emails, f"Event Summary Report: {keyword} — {date_range}", summary, html_body)
