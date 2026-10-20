import logging
import re
from datetime import datetime, timedelta
import psycopg2
from openai import OpenAI

logger = logging.getLogger("sm_summarizer.summarizer_service")

LLM_BASE_URL = "http://10.10.116.215:30004/v1"
LLM_MODEL = "gemma-4-26b-a4b-it"

try:
    client = OpenAI(
        api_key="sk-no-key-required", 
        base_url=LLM_BASE_URL
    )
except Exception as e:
    logger.error(f"Failed to initialize OpenAI client: {e}")
    client = None

# Config for connecting to the social media DB.
DB_CONFIG = {
    "host": "10.10.116.215",
    "database": "shaktidb_social",
    "user": "social-media",
    "password": "social-media",
    "port": "13366",
    "connect_timeout": 10
}

def _escape_like(s: str) -> str:
    """Escapes SQL LIKE wildcards % and _ so user input matches literally."""
    return str(s).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

def _clean_alpha(s: str) -> str:
    """Extracts lowercase alphanumeric characters for fuzzy author matching."""
    return re.sub(r'[^a-zA-Z0-9]', '', str(s)).lower()

def _normalize_to_iso_date(d_str: str) -> str:
    """Normalizes any date string (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY) to YYYY-MM-DD."""
    if not d_str:
        return ""
    s = str(d_str).strip().split()[0]
    # Check if DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', s)
    if m:
        day, month, year = m.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    # Check if YYYY-MM-DD or YYYY/MM/DD
    m2 = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', s)
    if m2:
        year, month, day = m2.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return s

def _build_word_regex_sql(w: str) -> str:
    """Builds a Postgres regex pattern for word matching with word boundaries and inflections."""
    w_clean = w.strip().lower()
    if not w_clean:
        return ""
    if w_clean.endswith('ies') and len(w_clean) > 4:
        stem = w_clean[:-3]
        return r'\y' + re.escape(stem) + r'(y|ies)\y'
    elif w_clean.endswith('es') and len(w_clean) > 4:
        stem = w_clean[:-2]
        return r'\y' + re.escape(stem) + r'(e|es|ed|ing)?\y'
    elif w_clean.endswith('s') and len(w_clean) > 3 and not w_clean.endswith('ss'):
        stem = w_clean[:-1]
        return r'\y' + re.escape(stem) + r'(s|ed|ing)?\y'
    elif w_clean.endswith('ing') and len(w_clean) > 5:
        stem = w_clean[:-3]
        return r'\y' + re.escape(stem) + r'(s|ed|ing)?\y'
    elif w_clean.endswith('ed') and len(w_clean) > 4:
        stem = w_clean[:-2]
        return r'\y' + re.escape(stem) + r'(s|ed|ing)?\y'
    else:
        return r'\y' + re.escape(w_clean) + r'(s|ed|ing)?\y'

def _matches_keyword_phrase(text: str, phrase: str) -> bool:
    """Checks if text contains all words in the keyword phrase with word-boundary precision and inflection support."""
    if not text or not phrase:
        return False
    words = [w for w in re.split(r'\s+', phrase) if w]
    if not words:
        return False
    for w in words:
        w_clean = w.strip().lower()
        if not w_clean:
            continue
        if w_clean.endswith('ies') and len(w_clean) > 4:
            stem = w_clean[:-3]
            pat = r'\b' + re.escape(stem) + r'(y|ies)\b'
        elif w_clean.endswith('es') and len(w_clean) > 4:
            stem = w_clean[:-2]
            pat = r'\b' + re.escape(stem) + r'(e|es|ed|ing)?\b'
        elif w_clean.endswith('s') and len(w_clean) > 3 and not w_clean.endswith('ss'):
            stem = w_clean[:-1]
            pat = r'\b' + re.escape(stem) + r'(s|ed|ing)?\b'
        elif w_clean.endswith('ing') and len(w_clean) > 5:
            stem = w_clean[:-3]
            pat = r'\b' + re.escape(stem) + r'(s|ed|ing)?\b'
        elif w_clean.endswith('ed') and len(w_clean) > 4:
            stem = w_clean[:-2]
            pat = r'\b' + re.escape(stem) + r'(s|ed|ing)?\b'
        else:
            pat = r'\b' + re.escape(w_clean) + r'(s|ed|ing)?\b'
        if not re.search(pat, text, re.IGNORECASE):
            return False
    return True

def _build_platform_query(channel: str, handles: list, keyword, start_date: str, end_date: str, limit: int):
    """Helper to build a platform-specific SQL query and parameters.
    - If keywords are specified: every retrieved post MUST be relevant to at least one keyword.
    - Matching posts from specified handles are prioritized.
    - If only handles are specified (no keywords): posts from those handles are retrieved.
    """
    
    # Mapping of channel to its table and relevant columns.
    platform_map = {
        "twitter":     {"table": "twitter_posts",     "handle_col": "author_username",   "text_col": "text",              "kw_cols": ["keyword"],                  "date_col": "posted_at",          "url_col": "post_url"},
        "instagram":   {"table": "instagram_posts",   "handle_col": "user_name",         "text_col": "caption",           "kw_cols": ["keyword"],                  "date_col": "posted_at",          "url_col": "post_url"},
        "linkedin":    {"table": "linkedin_posts",    "handle_col": "author_page",       "text_col": "main_text",         "kw_cols": ["keyword"],                  "date_col": "posted_at",          "url_col": "post_url"},
        "reddit":      {"table": "reddit_posts",      "handle_col": "author",            "text_col": "body",              "kw_cols": ["keyword"],                  "date_col": "posted_at",          "url_col": "url"},
        "youtube":     {"table": "youtube_videos",    "handle_col": "channel_name",      "text_col": "COALESCE(video_title || E': ' || video_description, video_description, video_title)", "kw_cols": [], "date_col": "video_published_at", "url_col": "video_url"},
        "facebook":    {"table": "facebook_posts",    "handle_col": "author_name",       "text_col": "post_text",         "kw_cols": ["keywords"],                 "date_col": "posted_time",        "url_col": "url"},
        "mastodon":    {"table": "mastodon_posts",    "handle_col": "account_acct",      "text_col": "content_text",      "kw_cols": ["matched_hashtag"],          "date_col": "created_at",         "url_col": "url"},
        "whatsapp":    {"table": "whatsapp_posts",    "handle_col": "channel_id",        "text_col": "text",              "kw_cols": [],                           "date_col": "scraped_at",         "url_col": "external_link"},
        "telegram":    {"table": "telegram_posts",    "handle_col": "channel_name",      "text_col": "text",              "kw_cols": [],                           "date_col": "posted_at",          "url_col": "post_url"},
        "pinterest":   {"table": "pinterest_posts",   "handle_col": "creator_username",  "text_col": "description",       "kw_cols": ["keyword"],                  "date_col": "scraped_at",         "url_col": "pin_url"},
        "snapchat":    {"table": "snapchat_posts",    "handle_col": "creator",           "text_col": "caption",           "kw_cols": ["keyword", "search_term"],   "date_col": "posted_at",          "url_col": "spotlight_url"},
        "truthsocial": {"table": "truthsocial_posts", "handle_col": "author_handle",     "text_col": "text_plain",        "kw_cols": ["topic", "matched_variant"], "date_col": "created_at",         "url_col": "url"},
        "tiktok":      {"table": "tiktok_posts",      "handle_col": "author_username",   "text_col": "\"desc\"",          "kw_cols": ["keyword"],                  "date_col": "create_time_local",  "url_col": "video_url"},
        "baidu":       {"table": "baidu_tieba_posts", "handle_col": "author_name",       "text_col": "content",           "kw_cols": ["keyword_used", "title"],    "date_col": "posted_time",        "url_col": "post_url"}
    }
    
    if channel.lower() not in platform_map:
        return "", []
        
    p_info = platform_map[channel.lower()]
    table = p_info["table"]
    handle_col = p_info["handle_col"]
    text_col = p_info["text_col"]
    kw_cols = p_info["kw_cols"]
    date_col = p_info["date_col"]
    url_col = p_info["url_col"]

    all_search_parts = [f"COALESCE({text_col}::text, '')"] + [f"COALESCE({c}::text, '')" for c in kw_cols]
    search_expr = " || ' ' || ".join(all_search_parts)
    
    date_cond = ""
    date_params = []
    s_iso = _normalize_to_iso_date(start_date)
    e_iso = _normalize_to_iso_date(end_date)
    if s_iso and e_iso:
        date_cond = f"({date_col} >= %s::timestamp AND {date_col} < %s::timestamp + interval '1 day')"
        date_params = [s_iso, e_iso]
    elif s_iso:
        date_cond = f"({date_col} >= %s::timestamp)"
        date_params = [s_iso]
    elif e_iso:
        date_cond = f"({date_col} < %s::timestamp + interval '1 day')"
        date_params = [e_iso]
    
    # 1. Process Handles
    clean_handles_set = set()
    if handles:
        for h in handles:
            ch = str(h).lstrip('@').lstrip('u/').strip().lower()
            if ch:
                clean_handles_set.add(ch)

    kw_items = []
    if isinstance(keyword, list):
        kw_items = keyword
    elif isinstance(keyword, str) and keyword.strip():
        kw_items = [p.strip() for p in keyword.split(",") if p.strip()]

    pure_keywords = []
    for item in kw_items:
        s = str(item).strip()
        if not s:
            continue
        if s.startswith('@') or s.startswith('u/'):
            ch = s.lstrip('@').lstrip('u/').strip().lower()
            if ch:
                clean_handles_set.add(ch)
        else:
            pure_keywords.append(s)

    handle_clauses = []
    handle_params = []
    for clean_h in clean_handles_set:
        h_alpha = _clean_alpha(clean_h)
        if len(h_alpha) >= 3:
            c = f"(LOWER({handle_col}) = %s OR LTRIM(LOWER({handle_col}), '@') = %s OR REPLACE(REPLACE(REPLACE(LTRIM(LOWER({handle_col}), '@'), '_', ''), '.', ''), '-', '') = %s)"
            p = [clean_h, clean_h, h_alpha]
        else:
            c = f"(LOWER({handle_col}) = %s OR LTRIM(LOWER({handle_col}), '@') = %s)"
            p = [clean_h, clean_h]
        handle_clauses.append(c)
        handle_params.extend(p)

    # 2. Process Keywords with inflection-aware word boundaries
    kw_clauses = []
    kw_params = []
    for s in pure_keywords:
        words = [w for w in re.split(r'\s+', s) if w]
        if not words:
            continue
        word_conditions = []
        word_p = []
        for w in words:
            sql_reg = _build_word_regex_sql(w)
            word_conditions.append(f"({search_expr}) ~* %s")
            word_p.append(sql_reg)
        
        text_clause = f"({' AND '.join(word_conditions)})"
        kw_clauses.append(text_clause)
        kw_params.extend(word_p)

    # 3. Combine filter clauses:
    # When keywords are provided: EVERY retrieved post MUST match at least one keyword!
    where_parts = []
    where_params = []
    
    if date_cond:
        where_parts.append(date_cond)
        where_params.extend(date_params)
        
    if pure_keywords and kw_clauses:
        where_parts.append(f"({' OR '.join(kw_clauses)})")
        where_params.extend(kw_params)
    elif handle_clauses:
        # Handles only (no keywords entered)
        where_parts.append(f"({' OR '.join(handle_clauses)})")
        where_params.extend(handle_params)
    elif not date_cond:
        where_parts.append(f"{date_col} IS NOT NULL")

    where_clause = " AND ".join(where_parts)

    query = f"""
        WITH ranked AS (
            SELECT 
                '{channel}' as platform,
                {handle_col} as author,
                {text_col} as content,
                {date_col} as date,
                {url_col} as url,
                ROW_NUMBER() OVER (
                    PARTITION BY LOWER({handle_col})
                    ORDER BY {date_col} DESC
                ) as rn
            FROM {table}
            WHERE {where_clause}
        )
        SELECT platform, author, content, date, url
        FROM ranked
        WHERE rn <= {limit}
        ORDER BY date DESC
        LIMIT {limit * 3}
    """
        
    return query, where_params

FOOTER_PATTERNS = [
    # Publisher bios / channel boilerplate
    r'times of india \(toi\)',
    r'times of india videos',
    r"we track india's global rise",
    r'india today global is the',
    r'india today group',
    r"india's most trusted media",
    r'connects india to the world',
    r'about india today',
    r'crux is your daily video news guide',
    r'follow crux on',
    r'follow (us|the toi|the channel) on',
    r'follow .* on (instagram|telegram|twitter|x|facebook|whatsapp)',
    r'subscribe to (the |our )?(channel|newsletter|youtube|times of india|toi|itg)',
    r'subscribe for (more|updates|daily)',
    r'^\s*subscribe\b',
    r'press the bell icon',
    r'download (our|the|toi) app',
    r'download the .* app',
    r'visit our website',
    r'join millions reading',
    r'all rights reserved',
    r'copyright disclaimer',
    r'getclosertothenews',
    
    # Secondary roundups / chapter lists / side stories in long videos
    r'also in this episode:',
    r'in other news:',
    r'other stories:',
    r'timestamps:',
    r'chapters:',
    r'also watch:',
    r'watch more:',
    r'related videos:',
    r'^\s*0:00\b',
    r'^\s*00:00\b'
]

def _clean_content_for_matching(text: str, is_youtube: bool = False) -> str:
    """Strips publisher bio boilerplate footers, social links, and secondary roundups
    so keyword relevance is evaluated against actual editorial content rather than
    channel bios or passing footnotes."""
    if not text:
        return ""
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        lower = line.strip().lower()
        if any(re.search(pat, lower) for pat in FOOTER_PATTERNS):
            break
        clean_lines.append(line)
    return "\n".join(clean_lines).strip()

def _filter_relevant_posts(posts: list[dict], handles: list[str], keyword: str | list[str], start_date: str = "", end_date: str = "") -> list[dict]:
    """Strict post-filtering in Python:
    - Strictly enforces requested date boundaries.
    - If keywords are entered, EVERY retrieved post MUST be relevant to at least one keyword.
    - Promotional channel bios, boilerplates, and passing footnotes are stripped before relevance matching.
    - Posts not matching the entered keywords are dropped regardless of author.
    """
    if not posts:
        return []

    s_iso = _normalize_to_iso_date(start_date)
    e_iso = _normalize_to_iso_date(end_date)

    # Clean handles set
    clean_handles = set()
    clean_handles_alpha = set()
    if handles:
        for h in handles:
            ch = str(h).lstrip('@').lstrip('u/').strip().lower()
            if ch:
                clean_handles.add(ch)
                alpha = _clean_alpha(ch)
                if alpha:
                    clean_handles_alpha.add(alpha)

    # Clean keywords list and separate pure keywords from @handles
    kw_items = []
    if isinstance(keyword, list):
        kw_items = keyword
    elif isinstance(keyword, str) and keyword.strip():
        kw_items = [p.strip() for p in keyword.split(",") if p.strip()]

    pure_keywords = []
    for k in kw_items:
        k_str = str(k).strip()
        if not k_str:
            continue
        if k_str.startswith('@') or k_str.startswith('u/'):
            ch = k_str.lstrip('@').lstrip('u/').strip().lower()
            if ch:
                clean_handles.add(ch)
                alpha = _clean_alpha(ch)
                if alpha:
                    clean_handles_alpha.add(alpha)
        else:
            pure_keywords.append(k_str)

    # If neither handles nor keywords specified, return as-is
    if not clean_handles and not pure_keywords:
        return posts

    filtered = []
    for p in posts:
        # 1. Strict Date Filter: Post MUST be within [s_iso, e_iso]
        if s_iso or e_iso:
            p_date = p.get("date")
            p_date_str = ""
            if p_date:
                if hasattr(p_date, 'strftime'):
                    p_date_str = p_date.strftime('%Y-%m-%d')
                else:
                    p_date_str = _normalize_to_iso_date(str(p_date).strip()[:10])

            if s_iso and p_date_str and p_date_str < s_iso:
                continue
            if e_iso and p_date_str and p_date_str > e_iso:
                continue

        author = str(p.get("author", "")).strip().lower()
        author_clean = author.lstrip('@').lstrip('u/')
        content = str(p.get("content", ""))
        platform = str(p.get("platform", "")).lower()

        # Clean promotional boilerplates, channel bios, and secondary roundups
        clean_content = _clean_content_for_matching(content, is_youtube=(platform == "youtube"))

        # 2. Strict Keyword Relevance:
        # If the analyst entered keywords, EVERY retrieved post MUST be relevant to at least one keyword!
        if pure_keywords:
            is_keyword_match = any(_matches_keyword_phrase(clean_content, kw) for kw in pure_keywords)
            if not is_keyword_match:
                continue

        # 3. Handle filter: If ONLY handles were entered (no keywords entered)
        if not pure_keywords and clean_handles:
            is_handle_match = (
                author in clean_handles or 
                author_clean in clean_handles or 
                (_clean_alpha(author_clean) in clean_handles_alpha if author_clean else False)
            )
            if not is_handle_match:
                continue

        filtered.append(p)

    return filtered

def fetch_social_media_posts(keyword: str, start_date: str, end_date: str, handles: list[str], channels: list[str], limit: int) -> list[dict]:
    """Fetches posts from Postgres dynamically strictly within requested filters and date range. Never overrides user's dates."""
    if not channels:
        channels = ["twitter", "instagram", "linkedin", "reddit", "youtube"]
        
    posts = []
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        for ch in channels:
            try:
                q, p = _build_platform_query(ch, handles, keyword, start_date, end_date, limit)
                if not q:
                    continue
                cur.execute(q, p)
                rows = cur.fetchall()
                for r in rows:
                    posts.append({
                        "platform": r[0],
                        "author": r[1],
                        "content": r[2],
                        "date": r[3],
                        "url": r[4]
                    })
            except Exception as ch_err:
                logger.warning(f"Error querying channel {ch}: {ch_err}")
                conn.rollback()

        cur.close()
        conn.close()
    except Exception as db_err:
        logger.error(f"Database connection error: {db_err}")

    # Enforce strict Python-level relevance and date filtering
    filtered_posts = _filter_relevant_posts(posts, handles, keyword, start_date, end_date)
    return filtered_posts


def _generate_llm_summary(keyword: str, posts: list[dict], mode: str = "summary", handles: list[str] = None, start_date: str = "", end_date: str = "") -> str:
    if not posts:
        return "No relevant social media posts were found for this period."
        
    if not client:
        return f"Found {len(posts)} posts, but LLM client is not configured."
    
    # Build numbered post list with expanded content length so LLM has rich context
    formatted_posts = ""
    for i, p in enumerate(posts[:50], start=1):
        if hasattr(p['date'], 'strftime'):
            date_str = p['date'].strftime('%d/%m/%Y')
        else:
            raw_d = str(p.get('date', ''))[:10]
            parts = raw_d.split('-')
            date_str = f"{parts[2]}/{parts[1]}/{parts[0]}" if len(parts) == 3 else raw_d
        content = str(p['content'])[:1000].replace('\n', ' ')
        formatted_posts += f"[#{i}] [{p['platform'].upper()}] @{p['author']} ({date_str}): {content}\n"
    
    label_parts = []
    if handles:
        label_parts.append(", ".join(h if h.startswith("@") else f"@{h}" for h in handles))
    if keyword:
        label_parts.append(keyword)

    topic_label = " & ".join(label_parts).title() if label_parts else "Social Media Analysis"
    
    is_detailed_report = mode in ("report", "detailed", "complete")

    if is_detailed_report:
        system_role = (
            "You are a senior intelligence analyst producing comprehensive, highly detailed, and elaborative intelligence reports. "
            "You provide exhaustive contextual breakdowns, strategic impact assessments, and thorough narrative explanations with numbered inline citations like (see [#1]). "
            "You never provide brief or generic summaries, and you never include DATE headers, classification labels, document IDs, or TO/FROM/SUBJECT headers."
        )
        prompt = f"""You are a senior intelligence analyst. Write an elaborative, highly detailed, and comprehensive intelligence report about "{topic_label}" based on the numbered social media posts below.

CRITICAL INSTRUCTIONS ON DEPTH & EXPLANATION (follow strictly):
- Do NOT provide a brief, shallow, or generic response. Even if there are few posts, provide extensive, in-depth contextual analysis, operational background, and strategic evaluation.
- Title line: "{topic_label} — Detailed Situation & Intelligence Report"
- EXECUTIVE SUMMARY / STRATEGIC OVERVIEW:
  * Provide an in-depth strategic overview (2 to 3 detailed paragraphs).
  * Establish the broader geopolitical, security, or socio-economic context and synthesize key dynamics across all sources.
- THEMATIC & EVENT ANALYSIS SECTIONS (3 to 5 named sections in ALL CAPS, e.g. MARITIME & MILITARY DEVELOPMENTS, REGIONAL DIPLOMACY & HISTORICAL DISPUTES, STRATEGIC POSTURING, ALLIANCE DYNAMICS):
  * Under each section, write comprehensive, multi-paragraph narrative explanations.
  * For every event, thoroughly elaborate on what occurred, who the key figures or organizations are, specific claims or statements made, relevant background history, and the operational significance.
  * Explicitly analyze the cause-and-effect relationships, strategic motives of the involved actors, and regional or global implications.
- STRATEGIC IMPLICATIONS & RISK ASSESSMENT:
  * Provide an elaborate assessment of the immediate and medium-term consequences for regional stability, alliances, and defense postures.
- FORWARD OUTLOOK & INFERRED TRENDS:
  * Conclude with an in-depth forward-looking trajectory detailing emerging indicators, potential escalation or stabilization paths, and key factors analysts should monitor next.
- CITATIONS: After each factual assertion or claim, strictly cite the originating source using its reference number like: (see [#1]) or (see [#2], [#5]).
- Do NOT include any DATE: header line, CLASSIFICATION labels, DOCUMENT ID, or TO/FROM/SUBJECT email headers.
- Exhaustively cover all relevant findings from the provided posts; synthesize all available details.

Monitored Posts:
{formatted_posts}
"""
        max_tok = 3500
    else:
        # Substantive executive summary for daily digests and quick overviews
        system_role = (
            "You are a professional intelligence analyst. You write clean, substantive, and informative situation summaries with numbered inline source citations like (see [#1]). "
            "You provide meaningful operational context and strategic significance. You never add DATE lines, classification labels, document IDs, or TO/FROM/SUBJECT headers."
        )
        prompt = f"""You are an intelligence analyst. Write a substantive situation summary about "{topic_label}" based on the numbered social media posts below.

FORMATTING & DEPTH RULES (follow strictly):
- Start with: SITUATION REPORT: {topic_label.upper()}\nOVERVIEW:
- Write a substantive, well-explained strategic overview (2 to 3 paragraphs).
- Do NOT simply restate the posts in 1-2 brief sentences. For every event, statement, or action, provide the operational background, the key actors involved, and the broader geopolitical or security significance.
- Conclude with a clear paragraph on the strategic implications and what this signals for the region.
- After each factual statement or claim, cite the source like: (see [#1]) or (see [#2]).
- Do NOT include any DATE: header line, CLASSIFICATION labels, DOCUMENT ID, or TO/FROM/SUBJECT headers.

Monitored Posts:
{formatted_posts}
"""
        max_tok = 1500

    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": system_role},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=max_tok
        )
        summary_text = response.choices[0].message.content.strip()
        summary_text = re.sub(r'(?m)^DATE:\s*[^\n]*\n?', '', summary_text)
        return summary_text
    except Exception as e:
        logger.error(f"LLM Generation failed: {e}")
        return f"Found {len(posts)} posts, but failed to generate the LLM summary. Error: {e}"

def search_and_summarize(
    keyword: str,
    start_date: str,
    end_date: str,
    limit: int = 20,
    handles: list[str] = None,
    channels: list[str] = None,
    mode: str = "current"
) -> dict:
    """
    Search across social media tables using keyword, handles, and channels.
    Summarize the results.
    """
    handles = handles or []
    channels = channels or []
    
    logger.info(f"Searching social media for keyword='{keyword}' handles={handles} channels={channels} date range {start_date} to {end_date}")
    
    # 1. Fetch data from DB based on keyword, handles, channels
    try:
        fetched_posts = fetch_social_media_posts(
            keyword=keyword, 
            start_date=start_date, 
            end_date=end_date, 
            handles=handles, 
            channels=channels, 
            limit=limit
        )
    except Exception as e:
        return {
            "count": 0,
            "summary": str(e),
            "events": [],
            "all_sources": []
        }
    
    # 2. Summarize via LLM
    summary_text = _generate_llm_summary(keyword, fetched_posts, mode, handles=handles, start_date=start_date, end_date=end_date)
    
    # 3. Compile rich sources (numbered, matching [#N] citations in summary)
    all_sources = []
    for i, p in enumerate(fetched_posts, start=1):
        if hasattr(p['date'], 'strftime'):
            date_str = p['date'].strftime('%d/%m/%Y')
        else:
            raw_d = str(p.get('date', ''))[:10]
            parts = raw_d.split('-')
            date_str = f"{parts[2]}/{parts[1]}/{parts[0]}" if len(parts) == 3 else raw_d
        content_str = str(p.get('content', '')).strip()
        snippet = content_str[:300].replace('\n', ' ')
        all_sources.append({
            "num": i,
            "url": p.get("url", ""),
            "platform": p.get("platform", ""),
            "author": p.get("author", ""),
            "date": date_str,
            "keyword": keyword,
            "snippet": snippet,
            "content": content_str
        })
    
    def _format_out_date(d_val):
        if not d_val:
            return ""
        iso = _normalize_to_iso_date(d_val)
        parts = iso.split("-")
        if len(parts) == 3 and len(parts[0]) == 4:
            return f"{parts[2]}/{parts[1]}/{parts[0]}"
        return iso

    return {
        "count": len(fetched_posts),
        "summary": summary_text,
        "events": [],
        "all_sources": all_sources,
        "keyword": keyword,
        "start_date": _format_out_date(start_date),
        "end_date": _format_out_date(end_date)
    }
