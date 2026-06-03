#!/usr/bin/env python3
"""
Managed Escalation Tracker — Sync Script
Runs hourly via GitHub Actions.
  1. Fetches threads from Slack channel C0A8GNBCPRR
  2. Parses metadata (merchant, status, SLA, type, …)
  3. Upserts into Google Sheets
  4. Writes/updates data.json for GitHub Pages

Env vars required (set as GitHub Secrets):
  SLACK_BOT_TOKEN            xoxb-...
  GOOGLE_SERVICE_ACCOUNT_JSON  full JSON string of GCP service account credentials
  GOOGLE_SHEET_ID            spreadsheet ID
  LEADERSHIP_SLACK_USER_IDS  comma-separated Slack user IDs (optional)
"""

import os, json, re, time, datetime, math, sys
from zoneinfo import ZoneInfo

import requests
import gspread
from google.oauth2.service_account import Credentials

# ── Constants ─────────────────────────────────────────────────────────────────

CHANNEL_ID          = "C0A8GNBCPRR"
WORKSPACE_SUBDOMAIN = "razorpay"          # for thread URL construction
IST                 = ZoneInfo("Asia/Kolkata")
SLA_BUSINESS_HOURS  = 8
BH_START            = 10                  # 10:00 IST
BH_END              = 19                  # 19:00 IST
BH_DAYS             = {0, 1, 2, 3, 4, 5} # Mon=0 … Sat=5 (Python weekday)
DATA_JSON_PATH      = "data.json"
SCOPES              = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SHEET_TRACKER  = "Escalation Tracker"
SHEET_SUMMARY  = "Daily Summary"
SHEET_DASHBOARD = "SLA Dashboard"

TRACKER_HEADERS = [
    "Thread TS", "Merchant Name", "Who Raised", "Escalation Manager",
    "Created (IST)", "Escalation Context", "Current Status", "Resolution Status",
    "TTR (Business Hours)", "SLA Breach (8h)", "Escalation Month",
    "Merchant Unblocked", "Merchant Confirmation", "Mx Confirm Timestamp",
    "BL Approval", "Escalation Type", "IM Involved", "Thread URL",
    "Total Replies", "Last Activity (IST)", "Last Updated By",
    "Resolution Timestamp", "Open Days", "Week #", "Quarter",
]

# ── Keyword banks ─────────────────────────────────────────────────────────────

KW_RESOLVED        = ["resolved", "fixed", "completed", "closed", "deployed",
                      "merchant unblocked", "working now", "issue resolved",
                      "back to normal", "unblocked", "all good", "sorted",
                      "live now", "enabled now", "back to bau"]
KW_INVESTIGATING   = ["investigating", "looking into", "checking", "digging",
                      "analysing", "analyzing"]
KW_WAIT_INTERNAL   = ["waiting on", "pinged", "escalated to", "waiting for",
                      "pending with", "on hold", "checking with", "looped in"]
KW_WAIT_MERCHANT   = ["waiting on merchant", "merchant to confirm",
                      "pending merchant", "awaiting merchant"]
KW_MONITORING      = ["monitoring", "watching", "tracking", "keeping an eye"]
KW_UNBLOCKED       = ["merchant unblocked", "issue fixed", "working now",
                      "unblocked", "live now", "transactions going through",
                      "back to normal", "restored"]
KW_MX_CONFIRMED    = ["confirmed", "working now", "looks good",
                      "resolved from merchant", "thanks team", "thank you",
                      "issue resolved", "all good", "it works", "great", "perfect"]
KW_BL_APPROVAL     = ["approved", "approval received", "go ahead", "proceed",
                      "leadership approved", "green light", "cleared",
                      "authorised", "authorized"]
KW_IM              = ["im team", "incident management", "incident declared",
                      "war room", "im bridge", "major incident", "@incident"]
KW_LEADERSHIP      = ["shk", "shashank", "harshil", "khilan", "co-founder",
                      "cto", "ceo", "cxo", "director", "vp ", "founder"]

# ── Slack API ─────────────────────────────────────────────────────────────────

class SlackClient:
    BASE = "https://slack.com/api"
    RATE_SLEEP = 1.2   # seconds between calls (Tier-2: 50/min)

    def __init__(self, token):
        self.token = token
        self._user_cache = {}

    def _get(self, endpoint, params, retries=3):
        headers = {"Authorization": f"Bearer {self.token}"}
        for attempt in range(retries):
            try:
                resp = requests.get(f"{self.BASE}/{endpoint}", headers=headers,
                                    params=params, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    print(f"  Rate limited. Sleeping {wait}s…")
                    time.sleep(wait)
                    continue
                data = resp.json()
                if not data.get("ok"):
                    err = data.get("error", "unknown")
                    if err in ("ratelimited", "fatal_error", "internal_error"):
                        time.sleep(2 ** attempt)
                        continue
                    raise RuntimeError(f"Slack {endpoint} error: {err}")
                time.sleep(self.RATE_SLEEP)
                return data
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def history(self, oldest=None, latest=None, cursor=None, limit=200):
        params = {"channel": CHANNEL_ID, "limit": limit}
        if oldest: params["oldest"] = oldest
        if latest: params["latest"] = latest
        if cursor: params["cursor"] = cursor
        return self._get("conversations.history", params)

    def replies(self, thread_ts):
        params = {"channel": CHANNEL_ID, "ts": thread_ts, "limit": 1000}
        data = self._get("conversations.replies", params)
        return data.get("messages", [])

    def user_name(self, user_id):
        if not user_id:
            return "Unknown"
        if user_id in self._user_cache:
            return self._user_cache[user_id]
        try:
            data = self._get("users.info", {"user": user_id})
            profile = data["user"]["profile"]
            name = profile.get("display_name") or profile.get("real_name") or user_id
            self._user_cache[user_id] = name
            return name
        except Exception:
            self._user_cache[user_id] = user_id
            return user_id

    def fetch_root_messages(self, oldest_ts, latest_ts=None):
        """Returns all root-level messages in the channel within the time window."""
        messages, cursor = [], None
        while True:
            data = self.history(oldest=oldest_ts, latest=latest_ts, cursor=cursor)
            for m in data.get("messages", []):
                if m.get("thread_ts", m["ts"]) == m["ts"]:  # root only
                    messages.append(m)
            meta = data.get("response_metadata", {})
            cursor = meta.get("next_cursor")
            if not data.get("has_more") or not cursor:
                break
        return messages

# ── Time helpers ──────────────────────────────────────────────────────────────

def ts_to_dt(ts_str):
    return datetime.datetime.fromtimestamp(float(ts_str), tz=IST)

def fmt_ist(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else ""

def calc_business_hours(start: datetime.datetime, end: datetime.datetime) -> float:
    """Returns business hours (Mon–Sat 10–19 IST) between two tz-aware datetimes."""
    if not start or not end or end <= start:
        return 0.0
    total = 0.0
    cursor = start.astimezone(IST)
    end_ist = end.astimezone(IST)

    while cursor < end_ist:
        wd = cursor.weekday()   # 0=Mon … 6=Sun
        if wd in BH_DAYS:
            day_bh_start = cursor.replace(hour=BH_START, minute=0, second=0, microsecond=0)
            day_bh_end   = cursor.replace(hour=BH_END,   minute=0, second=0, microsecond=0)
            seg_start = max(cursor, day_bh_start)
            seg_end   = min(end_ist, day_bh_end)
            if seg_end > seg_start:
                total += (seg_end - seg_start).total_seconds() / 3600
        # Advance to next day
        cursor = (cursor + datetime.timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0)
    return round(total, 2)

def week_number(dt):
    return dt.isocalendar()[1]

def quarter(dt):
    return f"Q{(dt.month - 1) // 3 + 1}"

def esc_month(dt):
    return dt.strftime("%b-%Y")

def open_days(created, resolved=None):
    end = resolved or datetime.datetime.now(tz=IST)
    return max(1, math.ceil((end - created).total_seconds() / 86400))

# ── Parsing helpers ───────────────────────────────────────────────────────────

def contains_any(text, keywords):
    t = text.lower()
    return any(kw.lower() in t for kw in keywords)

def extract_merchant_name(text):
    # 1. Backtick
    m = re.search(r'`([^`]+)`', text)
    if m: return m.group(1).strip()
    # 2. Bold
    m = re.search(r'\*([^*]+)\*', text)
    if m and len(m.group(1).split()) <= 5:
        return m.group(1).strip()
    # 3. WA-forward pattern
    m = re.search(
        r'(?:wa|whatsapp)\s*(?:<>|from)?\s*(?:shk|harshil|khilan|[a-z]+)?\s*'
        r'([A-Z][A-Za-z][\w\s&.-]{1,40}?)(?:\s*[-–:|]|\s*\n)',
        text, re.IGNORECASE)
    if m: return m.group(1).strip()
    # 4. First 2+ capitalised words
    m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', text)
    if m: return m.group(1).strip()
    # 5. First capitalised token
    tokens = text.split()
    if tokens and re.match(r'^[A-Z]', tokens[0]):
        return tokens[0]
    return "Unknown"

def extract_who_raised(text, esc_manager_name):
    m = re.search(
        r'(?:wa|whatsapp)\s*(?:<>|from)?\s+([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?)',
        text, re.IGNORECASE)
    if m: return m.group(1).strip()
    return esc_manager_name

def classify_esc_type(text, sender_id, leadership_ids):
    if sender_id in leadership_ids:
        return "Leadership Escalation"
    if contains_any(text, KW_LEADERSHIP):
        return "Leadership Escalation"
    return "AM Escalation"

def determine_status(replies):
    for msg in reversed(replies[-5:]):
        t = (msg.get("text") or "").lower()
        if contains_any(t, KW_RESOLVED):       return "Resolved"
        if contains_any(t, KW_MONITORING):     return "Monitoring"
        if contains_any(t, KW_WAIT_MERCHANT):  return "Waiting on Merchant"
        if contains_any(t, KW_WAIT_INTERNAL):  return "Waiting on Internal Team"
        if contains_any(t, KW_INVESTIGATING):  return "Investigating"
    return "Open"

def detect_resolution(replies):
    for msg in reversed(replies):
        t = (msg.get("text") or "").lower()
        if contains_any(t, KW_RESOLVED):
            return True, ts_to_dt(msg["ts"])
    return False, None

def detect_mx_confirmation(replies, resolved_at):
    if not resolved_at:
        return False, None
    for msg in replies:
        msg_dt = ts_to_dt(msg["ts"])
        if msg_dt <= resolved_at:
            continue
        t = (msg.get("text") or "").lower()
        if contains_any(t, KW_MX_CONFIRMED):
            return True, msg_dt
    return False, None

def build_context(text):
    cleaned = re.sub(r'<@[A-Z0-9]+>', '', text)
    cleaned = re.sub(r'<#[A-Z0-9]+\|[^>]+>', '', cleaned)
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    cleaned = re.sub(r'[*_`~>]', '', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned[:300]

def thread_url(ts):
    safe = ts.replace(".", "")
    return f"https://{WORKSPACE_SUBDOMAIN}.slack.com/archives/{CHANNEL_ID}/p{safe}"

# ── Core parse ────────────────────────────────────────────────────────────────

def parse_thread(parent, replies, slack: SlackClient, leadership_ids: set) -> dict:
    created_dt    = ts_to_dt(parent["ts"])
    last_msg      = replies[-1] if replies else parent
    last_dt       = ts_to_dt(last_msg["ts"])
    esc_manager   = slack.user_name(parent.get("user"))
    who_raised    = extract_who_raised(parent.get("text", ""), esc_manager)
    full_text     = parent.get("text", "")
    all_text      = " ".join(r.get("text", "") for r in replies)

    resolved, resolved_dt = detect_resolution(replies)
    mx_confirmed, mx_confirm_dt = detect_mx_confirmation(replies, resolved_dt)
    ttr = calc_business_hours(created_dt, resolved_dt) if resolved and resolved_dt else None
    sla_breach = ("YES" if ttr and ttr > SLA_BUSINESS_HOURS else
                  "NO"  if ttr is not None else "Open")

    return {
        "thread_ts":       parent["ts"],
        "merchant_name":   extract_merchant_name(full_text),
        "who_raised":      who_raised,
        "esc_manager":     esc_manager,
        "created_ist":     fmt_ist(created_dt),
        "context":         build_context(full_text),
        "status":          determine_status(replies),
        "resolution_status": "Resolved" if resolved else "Unresolved",
        "ttr_hours":       ttr,
        "sla_breach":      sla_breach,
        "esc_month":       esc_month(created_dt),
        "unblocked":       "YES" if contains_any(all_text, KW_UNBLOCKED) else "NO",
        "mx_confirmed":    "YES" if mx_confirmed else "NO",
        "mx_confirm_ts":   fmt_ist(mx_confirm_dt) if mx_confirm_dt else "",
        "bl_approval":     "YES" if contains_any(all_text, KW_BL_APPROVAL) else "NO",
        "esc_type":        classify_esc_type(full_text, parent.get("user", ""), leadership_ids),
        "im_involved":     "YES" if contains_any(all_text, KW_IM) else "NO",
        "thread_url":      thread_url(parent["ts"]),
        "total_replies":   max(0, len(replies) - 1),
        "last_activity_ist": fmt_ist(last_dt),
        "last_updated_by": slack.user_name(last_msg.get("user")),
        "resolution_ts":   fmt_ist(resolved_dt) if resolved_dt else "",
        "open_days":       open_days(created_dt, resolved_dt),
        "week_number":     week_number(created_dt),
        "quarter":         quarter(created_dt),
    }

# ── Google Sheets writer ──────────────────────────────────────────────────────

def get_sheets_client():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set")
    creds_dict = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def ensure_sheet(spreadsheet, name, headers):
    try:
        ws = spreadsheet.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=2000, cols=len(headers))
    if ws.row_count < 1 or not ws.row_values(1):
        ws.insert_row(headers, 1)
    return ws

def thread_to_row(t: dict) -> list:
    return [
        t["thread_ts"], t["merchant_name"], t["who_raised"], t["esc_manager"],
        t["created_ist"], t["context"], t["status"], t["resolution_status"],
        t["ttr_hours"] if t["ttr_hours"] is not None else "",
        t["sla_breach"], t["esc_month"], t["unblocked"],
        t["mx_confirmed"], t["mx_confirm_ts"], t["bl_approval"],
        t["esc_type"], t["im_involved"], t["thread_url"],
        t["total_replies"], t["last_activity_ist"], t["last_updated_by"],
        t["resolution_ts"], t["open_days"], t["week_number"], t["quarter"],
    ]

def upsert_to_sheets(threads: list[dict], sheet_id: str):
    gc = get_sheets_client()
    ss = gc.open_by_key(sheet_id)
    ws = ensure_sheet(ss, SHEET_TRACKER, TRACKER_HEADERS)

    # Build index: thread_ts → row number
    existing_ts = ws.col_values(1)  # column A
    ts_to_row = {v: i + 1 for i, v in enumerate(existing_ts) if v}

    updates = []
    appends = []
    for t in threads:
        row = thread_to_row(t)
        if t["thread_ts"] in ts_to_row:
            updates.append((ts_to_row[t["thread_ts"]], row))
        else:
            appends.append(row)

    # Batch update existing rows
    if updates:
        cell_updates = []
        for (row_num, row_data) in updates:
            for col_idx, val in enumerate(row_data, 1):
                cell_updates.append(gspread.Cell(row_num, col_idx, val))
        ws.update_cells(cell_updates)
        print(f"  Updated {len(updates)} existing rows in Sheets.")

    # Append new rows
    if appends:
        ws.append_rows(appends, value_input_option="USER_ENTERED")
        print(f"  Appended {len(appends)} new rows to Sheets.")

def write_daily_summary(threads: list[dict], sheet_id: str):
    gc = get_sheets_client()
    ss = gc.open_by_key(sheet_id)
    summary_headers = [
        "Date", "Total", "Open", "Resolved", "SLA Breaches", "SLA %",
        "Avg TTR (h)", "Leadership", "AM", "IM Involved",
    ]
    ws = ensure_sheet(ss, SHEET_SUMMARY, summary_headers)

    by_date = {}
    for t in threads:
        date_key = t["created_ist"][:10] if t["created_ist"] else "Unknown"
        by_date.setdefault(date_key, []).append(t)

    rows = []
    for date, entries in sorted(by_date.items()):
        total     = len(entries)
        resolved  = sum(1 for e in entries if e["resolution_status"] == "Resolved")
        breaches  = sum(1 for e in entries if e["sla_breach"] == "YES")
        sla_pct   = round((total - breaches) / total * 100, 1) if total else 100.0
        ttrs      = [e["ttr_hours"] for e in entries if isinstance(e["ttr_hours"], (int, float)) and e["ttr_hours"] > 0]
        avg_ttr   = round(sum(ttrs) / len(ttrs), 2) if ttrs else ""
        leadership = sum(1 for e in entries if e["esc_type"] == "Leadership Escalation")
        am_count  = sum(1 for e in entries if e["esc_type"] == "AM Escalation")
        im_count  = sum(1 for e in entries if e["im_involved"] == "YES")
        rows.append([date, total, total - resolved, resolved, breaches, sla_pct, avg_ttr, leadership, am_count, im_count])

    if rows:
        # Clear and rewrite (keep header)
        if ws.row_count > 1:
            ws.delete_rows(2, ws.row_count)
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    print(f"  Daily Summary: {len(rows)} rows written.")

# ── data.json writer ──────────────────────────────────────────────────────────

def build_kpis(threads):
    total        = len(threads)
    resolved     = sum(1 for t in threads if t["resolution_status"] == "Resolved")
    breached     = sum(1 for t in threads if t["sla_breach"] == "YES")
    sla_open     = sum(1 for t in threads if t["sla_breach"] == "Open")
    sla_within   = total - breached - sla_open
    leadership   = sum(1 for t in threads if t["esc_type"] == "Leadership Escalation")
    l_within     = sum(1 for t in threads if t["esc_type"] == "Leadership Escalation" and t["sla_breach"] == "NO")
    am_count     = sum(1 for t in threads if t["esc_type"] == "AM Escalation")
    am_within    = sum(1 for t in threads if t["esc_type"] == "AM Escalation" and t["sla_breach"] == "NO")
    im_involved  = sum(1 for t in threads if t["im_involved"] == "YES")
    ttrs         = [t["ttr_hours"] for t in threads if isinstance(t["ttr_hours"], (int, float)) and t["ttr_hours"] > 0]
    avg_ttr      = round(sum(ttrs) / len(ttrs), 2) if ttrs else None
    mx_pending   = sum(1 for t in threads if t["resolution_status"] == "Unresolved" and t["mx_confirmed"] != "YES")

    completed = sla_within + breached  # exclude "Open" from SLA%
    sla_pct = round(sla_within / completed * 100, 1) if completed else None

    return {
        "total": total, "open": total - resolved, "resolved": resolved,
        "sla_within": sla_within, "sla_breached": breached, "sla_open": sla_open,
        "sla_pct": sla_pct,
        "leadership_total": leadership, "leadership_within": l_within,
        "am_total": am_count, "am_within": am_within,
        "im_involved": im_involved, "avg_ttr_hours": avg_ttr,
        "mx_confirm_pending": mx_pending,
    }

def build_by_month(threads):
    by_month = {}
    for t in threads:
        m = t["esc_month"] or "Unknown"
        by_month.setdefault(m, {"total": 0, "resolved": 0, "breached": 0,
                                "leadership": 0, "am": 0, "im": 0})
        by_month[m]["total"] += 1
        if t["resolution_status"] == "Resolved": by_month[m]["resolved"] += 1
        if t["sla_breach"] == "YES":             by_month[m]["breached"] += 1
        if t["esc_type"] == "Leadership Escalation": by_month[m]["leadership"] += 1
        if t["esc_type"] == "AM Escalation":         by_month[m]["am"] += 1
        if t["im_involved"] == "YES":                by_month[m]["im"] += 1
    return by_month

def write_data_json(threads: list[dict]):
    now_ist = datetime.datetime.now(tz=IST)
    payload = {
        "meta": {
            "generated_at_ist": fmt_ist(now_ist),
            "generated_at_unix": int(now_ist.timestamp()),
            "total_threads": len(threads),
            "channel_id": CHANNEL_ID,
        },
        "kpis": build_kpis(threads),
        "by_month": build_by_month(threads),
        "threads": sorted(threads, key=lambda t: t["thread_ts"], reverse=True),
    }
    with open(DATA_JSON_PATH, "w") as f:
        json.dump(payload, f, indent=2)
    print(f"  data.json written: {len(threads)} threads.")

# ── Incremental merge ─────────────────────────────────────────────────────────

def load_existing_threads() -> dict:
    """Returns dict of thread_ts → thread from existing data.json (if any)."""
    if not os.path.exists(DATA_JSON_PATH):
        return {}
    try:
        with open(DATA_JSON_PATH) as f:
            data = json.load(f)
        return {t["thread_ts"]: t for t in data.get("threads", [])}
    except Exception:
        return {}

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    slack_token = os.environ.get("SLACK_BOT_TOKEN")
    if not slack_token:
        print("ERROR: SLACK_BOT_TOKEN not set", file=sys.stderr)
        sys.exit(1)

    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    leadership_ids = set(
        filter(None, os.environ.get("LEADERSHIP_SLACK_USER_IDS", "").split(","))
    )

    # Determine sync window
    # On first run (no data.json) pull 365 days; otherwise pull last 2 days for updates
    existing = load_existing_threads()
    if existing:
        oldest_ts = str(time.time() - 2 * 86400)  # last 48h for updates
        print(f"Incremental sync: {len(existing)} existing threads, fetching last 48h updates…")
    else:
        oldest_ts = str(time.time() - 365 * 86400)  # full history
        print("First run: fetching full 365-day history…")

    slack = SlackClient(slack_token)

    print("Fetching root messages from Slack…")
    root_messages = slack.fetch_root_messages(oldest_ts)
    print(f"Found {len(root_messages)} root messages.")

    # Parse each thread
    newly_parsed = {}
    for i, msg in enumerate(root_messages, 1):
        ts = msg["ts"]
        print(f"  [{i}/{len(root_messages)}] Thread {ts[:10]}…", end=" ")
        try:
            replies = slack.replies(ts)
            parsed = parse_thread(msg, replies, slack, leadership_ids)
            newly_parsed[ts] = parsed
            print(parsed["merchant_name"])
        except Exception as e:
            print(f"ERROR: {e}")

    # Merge: newly parsed overrides existing (keeps old threads not in window)
    merged = {**existing, **newly_parsed}
    all_threads = list(merged.values())
    print(f"\nTotal threads after merge: {len(all_threads)}")

    # Write to Google Sheets if configured
    if sheet_id and os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON"):
        print("\nWriting to Google Sheets…")
        try:
            upsert_to_sheets(list(newly_parsed.values()), sheet_id)
            write_daily_summary(all_threads, sheet_id)
        except Exception as e:
            print(f"  Sheets write failed: {e}", file=sys.stderr)
    else:
        print("\nSkipping Sheets (GOOGLE_SHEET_ID or credentials not set).")

    # Always write data.json
    print("\nWriting data.json…")
    write_data_json(all_threads)
    print("\nSync complete.")

if __name__ == "__main__":
    main()
