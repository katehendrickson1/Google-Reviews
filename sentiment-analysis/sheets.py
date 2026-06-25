import logging
from datetime import date, timedelta

import gspread

from auth import sheets_credentials
from config import (
    APPROVED_THEMES,
    DASHBOARD_TAB,
    LOCATION_HOTSPOT_MIN,
    SHEET_ID,
    RAW_REVIEWS_TAB,
    SENTIMENT_CURRENT_TAB,
    SENTIMENT_HISTORY_TAB,
    SENTIMENT_REVIEWS_TAB,
    THEME_BREAKDOWN_TAB,
)
from models import Review

logger = logging.getLogger(__name__)

_HISTORY_HEADERS = [
    "Run Date", "Period Start", "Period End", "Scope",
    "Review Count", "Avg Star Rating", "Avg Sentiment Score",
    "Top Themes (Positive/Mixed)", "Top Themes (Negative)", "Urgent Callout Count",
]


def _client() -> gspread.Client:
    return gspread.authorize(sheets_credentials())


def _open_or_create(sheet: gspread.Spreadsheet, name: str, rows: int = 1000, cols: int = 20) -> gspread.Worksheet:
    try:
        return sheet.worksheet(name)
    except gspread.WorksheetNotFound:
        logger.info(f"Creating new worksheet '{name}'")
        return sheet.add_worksheet(name, rows=rows, cols=cols)


# ---------------------------------------------------------------------------
# READ
# ---------------------------------------------------------------------------

def read_reviews(since: date | None = None) -> list[Review]:
    """Read all reviews from the raw tab, optionally filtered to publishTime >= since."""
    gc = _client()
    ws = gc.open_by_key(SHEET_ID).worksheet(RAW_REVIEWS_TAB)
    rows = ws.get_all_records()

    reviews: list[Review] = []
    for row in rows:
        publish_time = str(row.get("publishTime") or "").strip()

        if since and publish_time:
            try:
                if date.fromisoformat(publish_time[:10]) < since:
                    continue
            except ValueError:
                pass  # malformed date — include the row

        try:
            star = int(row.get("★") or 0)
        except (ValueError, TypeError):
            star = 0

        reviews.append(Review(
            dedupe_key=str(row.get("dedupe_key") or "").strip(),
            place=str(row.get("place") or "").strip(),
            place_id=str(row.get("place_id") or "").strip(),
            author=str(row.get("author") or "").strip(),
            star_rating=star,
            publish_time=publish_time,
            relative_time=str(row.get("relativeTime") or "").strip(),
            text=str(row.get("New Reviews:") or "").strip(),
            date_run=str(row.get("date_run") or "").strip(),
        ))

    logger.info(f"Read {len(reviews)} reviews from '{RAW_REVIEWS_TAB}' (since={since})")
    return reviews


# ---------------------------------------------------------------------------
# WRITE — Sentiment - Current
# ---------------------------------------------------------------------------

def write_current(
    review_analyses: list[dict],
    summary: dict,
    period_start: date,
    period_end: date,
    empty_count: int,
    dup_count: int,
    run_date: date | None = None,
) -> None:
    """Overwrite the Sentiment - Current tab with this run's results."""
    if run_date is None:
        run_date = date.today()

    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    ws = _open_or_create(sheet, SENTIMENT_CURRENT_TAB)
    ws.clear()

    by_loc: dict = summary.get("by_location", {})
    total = summary.get("total_reviews", 0)

    # Weighted overall averages
    if by_loc and total:
        avg_star = round(
            sum(v.get("average_star_rating", 0) * v.get("review_count", 0) for v in by_loc.values()) / total, 2
        )
        avg_sentiment = round(
            sum(v.get("average_sentiment_score", 0) * v.get("review_count", 0) for v in by_loc.values()) / total, 2
        )
    else:
        avg_star = avg_sentiment = 0

    rows: list[list] = []

    # --- Run metadata ---
    rows += [
        ["RUN METADATA"],
        ["Run Date", "Period Start", "Period End", "Reviews Analyzed", "Star-Only (Empty)", "Duplicates Removed"],
        [str(run_date), str(period_start), str(period_end), total, empty_count, dup_count],
        [],
    ]

    # --- Overall summary ---
    rows += [
        ["OVERALL SUMMARY"],
        ["Avg Star Rating", "Avg Sentiment Score"],
        [avg_star, avg_sentiment],
        [],
        ["TOP POSITIVE DRIVERS"],
        [summary.get("top_positive_drivers", "")],
        [],
        ["TOP NEGATIVE DRIVERS"],
        [summary.get("top_negative_drivers", "")],
        [],
    ]

    # --- Top themes ---
    rows.append(["TOP THEMES"])
    rows.append(["Theme", "Count", "Sentiment Leaning"])
    for t in summary.get("overall_top_themes", []):
        rows.append([t.get("theme", ""), t.get("count", 0), t.get("sentiment_leaning", "")])
    rows.append([])

    # --- Per-location ---
    rows.append(["PER-LOCATION SUMMARY"])
    rows.append(["Location", "Reviews", "Avg Star", "Avg Sentiment", "Top Themes"])
    for loc, data in sorted(by_loc.items()):
        rows.append([
            loc,
            data.get("review_count", 0),
            round(data.get("average_star_rating", 0), 2),
            round(data.get("average_sentiment_score", 0), 2),
            ", ".join(data.get("top_themes", [])[:3]),
        ])
    rows.append([])

    # --- Theme breakdown by location ---
    from collections import Counter, defaultdict
    theme_by_loc: dict = defaultdict(Counter)
    for r in review_analyses:
        loc = r.get("location", "")
        for theme in r.get("themes", []):
            theme_by_loc[loc][theme] += 1

    rows.append(["THEME BREAKDOWN BY LOCATION"])
    rows.append(["Location", "Theme", "Review Count"])
    for loc in sorted(theme_by_loc):
        for theme, count in theme_by_loc[loc].most_common():
            rows.append([loc, theme, count])
    rows.append([])

    # --- Staff to recognize ---
    rows.append(["STAFF TO RECOGNIZE"])
    rows.append(["Name", "Location", "Mentions"])
    for s in summary.get("staff_to_recognize", []):
        rows.append([s.get("name", ""), s.get("location", ""), s.get("mention_count", 0)])
    rows.append([])

    # --- Urgent callouts ---
    rows.append(["URGENT OPS CALLOUTS"])
    rows.append(["Location", "Review Date", "Description"])
    for u in summary.get("urgent_callouts", []):
        rows.append([u.get("location", ""), u.get("review_date", ""), u.get("description", "")])
    rows.append([])

    # --- Sample quotes (positive then negative) ---
    rows.append(["SAMPLE QUOTES"])
    rows.append(["Sentiment", "Location", "Stars", "Quote"])
    for r in review_analyses:
        if r.get("representative_quote") and r.get("sentiment") == "positive":
            rows.append(["positive", r.get("location", ""), r.get("star_rating", ""), r["representative_quote"]])
            break
    for r in review_analyses:
        if r.get("representative_quote") and r.get("sentiment") == "negative":
            rows.append(["negative", r.get("location", ""), r.get("star_rating", ""), r["representative_quote"]])
            break

    ws.update("A1", rows)
    logger.info(f"Wrote {len(rows)} rows to '{SENTIMENT_CURRENT_TAB}'")


# ---------------------------------------------------------------------------
# WRITE — Sentiment - History (append-only)
# ---------------------------------------------------------------------------

def append_history(
    summary: dict,
    period_start: date,
    period_end: date,
    text_review_count: int,
    empty_count: int,
    run_date: date | None = None,
) -> None:
    """Append one Overall row + one row per location to the history tab."""
    if run_date is None:
        run_date = date.today()

    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    ws = _open_or_create(sheet, SENTIMENT_HISTORY_TAB)

    # Write headers if sheet is empty or first row doesn't match (e.g. old snake_case headers)
    existing = ws.get_all_values()
    if not existing or existing[0] != _HISTORY_HEADERS:
        if not existing:
            ws.append_row(_HISTORY_HEADERS)
            existing = [_HISTORY_HEADERS]
        else:
            ws.update("A1", [_HISTORY_HEADERS])
            existing[0] = _HISTORY_HEADERS

    # Skip if this period already has an Overall row — prevents duplicates on backfill reruns
    if len(existing) > 1:
        header = existing[0]
        try:
            ps_i = header.index("Period Start")
            pe_i = header.index("Period End")
            sc_i = header.index("Scope")
            if any(
                row[ps_i] == str(period_start) and row[pe_i] == str(period_end) and row[sc_i] == "Overall"
                for row in existing[1:]
                if len(row) > max(ps_i, pe_i, sc_i)
            ):
                logger.info("History already has %s → %s — skipping.", period_start, period_end)
                return
        except ValueError:
            pass  # column not found, proceed normally

    by_loc: dict = summary.get("by_location", {})
    total = summary.get("total_reviews", 0)
    urgent_count = len(summary.get("urgent_callouts", []))

    all_themes: list[dict] = summary.get("overall_top_themes", [])
    top_positive = ", ".join(
        t["theme"] for t in all_themes if t.get("sentiment_leaning") in ("positive", "mixed")
    )[:3 * 25]  # rough cap
    top_negative = ", ".join(
        t["theme"] for t in all_themes if t.get("sentiment_leaning") == "negative"
    )[:3 * 25]

    if by_loc and total:
        avg_star = round(
            sum(v.get("average_star_rating", 0) * v.get("review_count", 0) for v in by_loc.values()) / total, 2
        )
        avg_sentiment = round(
            sum(v.get("average_sentiment_score", 0) * v.get("review_count", 0) for v in by_loc.values()) / total, 2
        )
    else:
        avg_star = avg_sentiment = 0

    # Overall row
    ws.append_row([
        str(run_date), str(period_start), str(period_end), "Overall",
        total, avg_star, avg_sentiment,
        top_positive, top_negative, urgent_count,
    ])

    # Per-location rows
    urgent_by_loc: dict[str, int] = {}
    for u in summary.get("urgent_callouts", []):
        loc = u.get("location", "")
        urgent_by_loc[loc] = urgent_by_loc.get(loc, 0) + 1

    for loc, data in sorted(by_loc.items()):
        loc_themes = ", ".join(data.get("top_themes", [])[:3])
        ws.append_row([
            str(run_date), str(period_start), str(period_end), loc,
            data.get("review_count", 0),
            round(data.get("average_star_rating", 0), 2),
            round(data.get("average_sentiment_score", 0), 2),
            loc_themes, "", urgent_by_loc.get(loc, 0),
        ])

    logger.info(f"Appended history: Overall + {len(by_loc)} locations")


# ---------------------------------------------------------------------------
# WRITE — Sentiment - Reviews (per-review detail, overwritten each run)
# ---------------------------------------------------------------------------

_REVIEWS_HEADERS = [
    "review_id", "location", "star_rating", "publish_date",
    "sentiment", "sentiment_score",
    "themes", "positive_aspects", "negative_aspects",
    "staff_mentioned", "representative_quote", "needs_ops_followup",
]


def write_reviews(review_analyses: list[dict], run_date: date | None = None) -> None:
    """Append new review analyses to the Sentiment - Reviews tab, skipping any
    review_id already present. Safe to call repeatedly — idempotent per review_id."""
    if run_date is None:
        run_date = date.today()

    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    ws = _open_or_create(sheet, SENTIMENT_REVIEWS_TAB, rows=2000, cols=len(_REVIEWS_HEADERS))

    # Write header if tab is empty
    existing = ws.get_all_values()
    if not existing:
        ws.append_row(_REVIEWS_HEADERS)
        existing_ids: set[str] = set()
    else:
        existing_ids = {row[0] for row in existing[1:] if row}

    new_rows: list[list] = []
    for r in review_analyses:
        rid = r.get("review_id", "")
        if rid in existing_ids:
            continue
        publish_date = rid.split("|")[-1] if "|" in rid else ""
        new_rows.append([
            rid,
            r.get("location", ""),
            r.get("star_rating", ""),
            publish_date,
            r.get("sentiment", ""),
            r.get("sentiment_score", ""),
            ", ".join(r.get("themes", [])),
            " | ".join(r.get("positive_aspects", [])),
            " | ".join(r.get("negative_aspects", [])),
            ", ".join(r.get("staff_mentioned", [])),
            r.get("representative_quote", "") or "",
            r.get("needs_ops_followup", False),
        ])

    if new_rows:
        ws.append_rows(new_rows)
        logger.info("Appended %d new reviews to '%s' (%d already existed).",
                    len(new_rows), SENTIMENT_REVIEWS_TAB, len(review_analyses) - len(new_rows))
    else:
        logger.info("No new reviews to append to '%s' — all %d already present.",
                    SENTIMENT_REVIEWS_TAB, len(review_analyses))


def read_analyzed_reviews() -> dict[str, dict]:
    """Load previously stored per-review analyses from Sentiment - Reviews tab.
    Returns {review_id: analysis_dict} so callers can skip re-analyzing known reviews."""
    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    try:
        ws = sheet.worksheet(SENTIMENT_REVIEWS_TAB)
    except gspread.WorksheetNotFound:
        logger.info("No '%s' tab found — starting with empty cache.", SENTIMENT_REVIEWS_TAB)
        return {}

    rows = ws.get_all_records()
    cache: dict[str, dict] = {}
    for row in rows:
        rid = str(row.get("review_id", "")).strip()
        if not rid:
            continue
        needs_followup = row.get("needs_ops_followup", False)
        if isinstance(needs_followup, str):
            needs_followup = needs_followup.strip().lower() == "true"
        cache[rid] = {
            "review_id": rid,
            "location": str(row.get("location", "")),
            "star_rating": int(row.get("star_rating", 0) or 0),
            "sentiment": str(row.get("sentiment", "")),
            "sentiment_score": float(row.get("sentiment_score", 0) or 0),
            "themes": [t.strip() for t in str(row.get("themes", "")).split(",") if t.strip()],
            "positive_aspects": [a.strip() for a in str(row.get("positive_aspects", "")).split(" | ") if a.strip()],
            "negative_aspects": [a.strip() for a in str(row.get("negative_aspects", "")).split(" | ") if a.strip()],
            "staff_mentioned": [s.strip() for s in str(row.get("staff_mentioned", "")).split(",") if s.strip()],
            "representative_quote": str(row.get("representative_quote", "")) or None,
            "needs_ops_followup": needs_followup,
        }

    logger.info("Loaded %d cached review analyses from '%s'", len(cache), SENTIMENT_REVIEWS_TAB)
    return cache


# ---------------------------------------------------------------------------
# WRITE — Theme Sentiment Breakdown (formula-based, auto-updates from Reviews tab)
# ---------------------------------------------------------------------------

def write_theme_breakdown() -> None:
    """
    Write a formula-based tab that shows positive/negative/neutral/mixed counts
    per theme, pulling live from the Sentiment - Reviews tab via COUNTIFS.
    Cell B1 is a month filter (type YYYY-MM to scope to one month, leave blank for all time).
    Formulas are visible in each cell so the source is fully transparent.
    """
    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    ws = _open_or_create(sheet, THEME_BREAKDOWN_TAB, rows=22, cols=7)
    ws.clear()

    # Column positions in Sentiment - Reviews
    # A=review_id, B=location, C=star_rating, D=publish_date,
    # E=sentiment, F=sentiment_score, G=themes
    t = SENTIMENT_REVIEWS_TAB
    themes_col = "G"
    sentiment_col = "E"
    date_col = "D"
    filter_cell = "$B$1"  # month filter input

    def countifs(theme: str, sentiment_val: str) -> str:
        # TEXT($B$1,"YYYY-MM") normalizes B1 whether Sheets stored it as a date
        # value or a plain text string — avoids the serial-number concatenation bug.
        date_condition = f'IF({filter_cell}="","*",TEXT({filter_cell},"YYYY-MM")&"*")'
        return (
            f"=COUNTIFS('{t}'!{themes_col}:{themes_col},\"*{theme}*\","
            f"'{t}'!{sentiment_col}:{sentiment_col},\"{sentiment_val}\","
            f"'{t}'!{date_col}:{date_col},{date_condition})"
        )

    rows: list[list] = [
        ["Filter by month (YYYY-MM, leave blank for all time):", "", "", "", "", "", ""],
        ["", "", "", "", "", "", ""],
        ["Theme", "Positive", "Negative", "Neutral", "Mixed", "Total Mentioning", "% Neg or Mixed"],
    ]

    for i, theme in enumerate(APPROVED_THEMES):
        row_num = i + 4  # rows 1-3 are filter + blank + header
        total_cell = f"F{row_num}"
        neg_cell = f"C{row_num}"
        mixed_cell = f"E{row_num}"
        rows.append([
            theme,
            countifs(theme, "positive"),
            countifs(theme, "negative"),
            countifs(theme, "neutral"),
            countifs(theme, "mixed"),
            f"=SUM(B{row_num}:E{row_num})",
            f'=IF({total_cell}=0,"",TEXT(({neg_cell}+{mixed_cell})/{total_cell},"0%"))',
        ])

    ws.update("A1", rows, value_input_option="USER_ENTERED")
    logger.info(f"Wrote theme sentiment breakdown formulas to '{THEME_BREAKDOWN_TAB}'")


# ---------------------------------------------------------------------------
# WRITE — Formula Dashboard (one-time setup; formulas auto-update from Reviews tab)
# ---------------------------------------------------------------------------

_THEME_SHORT = {
    "staff_friendliness": "Friendly",
    "staff_helpfulness": "Helpful",
    "equipment_reliability": "Equipment",
    "wash_quality": "Wash Quality",
    "vehicle_damage": "Damage",
    "membership_subscription": "Membership",
    "pricing": "Pricing",
    "service_recovery": "Recovery",
    "vacuums_amenities": "Vacuums",
    "safety": "Safety",
    "cleanliness": "Cleanliness",
    "facility_condition": "Facility",
    "members_lounge": "Lounge",
}

_HOTSPOT_THEMES = [th for th in APPROVED_THEMES if th not in ("general_positive", "general_negative")]


def setup_formula_dashboard() -> None:
    """
    Write a formula-driven Dashboard tab. Run once to set up — formulas auto-update
    as new reviews land in Sentiment - Reviews. Re-run if new locations appear.
    Cell B2 is the month filter: type YYYY-MM to scope all sections to one month,
    leave blank for all-time view.
    """
    ORANGE     = {"red": 0.957, "green": 0.522, "blue": 0.098}
    DARK_BLUE  = {"red": 0.106, "green": 0.298, "blue": 0.569}
    LIGHT_BLUE = {"red": 0.839, "green": 0.890, "blue": 0.957}
    LIGHT_GRAY = {"red": 0.961, "green": 0.961, "blue": 0.961}
    LIGHT_RED  = {"red": 1.000, "green": 0.898, "blue": 0.898}
    AMBER      = {"red": 1.000, "green": 0.949, "blue": 0.800}
    WHITE      = {"red": 1.000, "green": 1.000, "blue": 1.000}
    DARK_TEXT  = {"red": 0.150, "green": 0.150, "blue": 0.150}

    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)

    # Read unique locations from Reviews tab
    try:
        reviews_ws = sheet.worksheet(SENTIMENT_REVIEWS_TAB)
        loc_col = reviews_ws.col_values(2)  # col B = location
        locations = sorted(set(v.strip() for v in loc_col[1:] if v.strip()))
    except gspread.WorksheetNotFound:
        locations = []
        logger.warning("'%s' tab not found — hotspot matrix will have no location rows.", SENTIMENT_REVIEWS_TAB)

    # Default month = last complete calendar month (stored as plain text "YYYY-MM")
    today = date.today()
    first_this_month = today.replace(day=1)
    default_month = (first_this_month - timedelta(days=1)).strftime("%Y-%m")

    t = SENTIMENT_REVIEWS_TAB  # shorthand for formula references
    n_theme_cols = len(_HOTSPOT_THEMES)
    NC = max(14, 1 + n_theme_cols)  # at least 14 columns

    ws = _open_or_create(sheet, DASHBOARD_TAB, rows=300, cols=NC + 2)
    ws.clear()

    marks: dict[str, int] = {}
    rows: list[list] = []

    def rn() -> int:
        return len(rows) + 1

    def push(*cells) -> None:
        rows.append((list(cells) + [""] * NC)[:NC])

    # ── Row 1: Title ──────────────────────────────────────────────────────
    marks["title"] = rn()
    push("SHINY SHELL CARWASH  ·  MONTHLY DASHBOARD")

    # ── Row 2: Month selector ─────────────────────────────────────────────
    marks["month_row"] = rn()
    push("Month (YYYY-MM) — leave blank for all time:", default_month)

    push()  # row 3 blank

    # ── Overall stats ─────────────────────────────────────────────────────
    marks["overall_hdr"] = rn()
    push("OVERALL STATS")

    marks["overall_col"] = rn()
    push("Total Reviews", "Avg Star Rating", "Avg Sentiment Score", "% Positive", "% Neutral/Mixed", "% Negative")

    marks["overall_data"] = rn()
    dr = rn()
    total_f   = f"=COUNTIF('{t}'!D2:D,$B$2&\"*\")"
    avgstar_f = f"=IFERROR(AVERAGEIF('{t}'!D2:D,$B$2&\"*\",'{t}'!C2:C),\"\")"
    avgsent_f = f"=IFERROR(AVERAGEIF('{t}'!D2:D,$B$2&\"*\",'{t}'!F2:F),\"\")"
    push(
        total_f,
        avgstar_f,
        avgsent_f,
        f"=IFERROR(COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!E2:E,\"positive\")/A{dr},\"\")",
        f"=IFERROR((COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!E2:E,\"neutral\")"
        f"+COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!E2:E,\"mixed\"))/A{dr},\"\")",
        f"=IFERROR(COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!E2:E,\"negative\")/A{dr},\"\")",
    )

    push()  # blank

    # ── Theme breakdown ───────────────────────────────────────────────────
    marks["themes_hdr"] = rn()
    push("THEME BREAKDOWN")

    marks["themes_col"] = rn()
    push("Theme", "Total", "Positive", "Negative", "Neutral", "Mixed", "% Neg/Mixed")

    marks["themes_data_start"] = rn()
    for theme in APPROVED_THEMES:
        tr = rn()
        push(
            theme,
            f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\")",
            f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\",'{t}'!E2:E,\"positive\")",
            f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\",'{t}'!E2:E,\"negative\")",
            f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\",'{t}'!E2:E,\"neutral\")",
            f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\",'{t}'!E2:E,\"mixed\")",
            f"=IFERROR((D{tr}+F{tr})/B{tr},\"\")",
        )
    marks["themes_data_end"] = rn() - 1

    push()  # blank

    # ── Location hotspot matrix ───────────────────────────────────────────
    marks["hotspot_hdr"] = rn()
    push(f"LOCATION HOTSPOTS  —  {LOCATION_HOTSPOT_MIN}+ negative or mixed reviews on same theme")

    marks["hotspot_note"] = rn()
    push(f"Count = negative + mixed. Red = {LOCATION_HOTSPOT_MIN}+  Amber = 1 or 2.")

    marks["hotspot_col"] = rn()
    push("Location", *[_THEME_SHORT.get(th, th) for th in _HOTSPOT_THEMES])

    marks["hotspot_data_start"] = rn()
    for loc in locations:
        cells: list = [loc]
        for theme in _HOTSPOT_THEMES:
            cells.append(
                f"=COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\","
                f"'{t}'!B2:B,\"{loc}\",'{t}'!E2:E,\"negative\")"
                f"+COUNTIFS('{t}'!D2:D,$B$2&\"*\",'{t}'!G2:G,\"*{theme}*\","
                f"'{t}'!B2:B,\"{loc}\",'{t}'!E2:E,\"mixed\")"
            )
        push(*cells)
    marks["hotspot_data_end"] = rn() - 1

    push()  # blank

    # ── Urgent flags (FILTER — spills downward) ───────────────────────────
    marks["urgent_hdr"] = rn()
    push("URGENT FLAGS THIS MONTH")

    marks["urgent_col"] = rn()
    push("Location", "Date", "Negative Aspects")

    marks["urgent_filter"] = rn()
    push(
        f"=IFERROR(FILTER({{'{t}'!B2:B,'{t}'!D2:D,'{t}'!I2:I}},"
        f"'{t}'!L2:L=TRUE,"
        f"($B$2=\"\")+($B$2<>\"\")*(LEFT('{t}'!D2:D,7)=$B$2)),"
        f"\"No urgent flags this month\")"
    )

    # Write part 1 (rows 1 through urgent filter)
    ws.update("A1", rows, value_input_option="USER_ENTERED")

    # ── Staff mentions (separate update — leaves 100 rows for FILTER spill) ──
    STAFF_START = marks["urgent_filter"] + 100
    marks["staff_hdr"] = STAFF_START
    marks["staff_col"] = STAFF_START + 1
    marks["staff_filter"] = STAFF_START + 2

    ws.update(
        f"A{STAFF_START}",
        [
            (["STAFF MENTIONS THIS MONTH"] + [""] * (NC - 1))[:NC],
            (["Location", "Date", "Staff Mentioned"] + [""] * (NC - 3))[:NC],
            [
                f"=IFERROR(FILTER({{'{t}'!B2:B,'{t}'!D2:D,'{t}'!J2:J}},"
                f"'{t}'!J2:J<>\"\","
                f"($B$2=\"\")+($B$2<>\"\")*(LEFT('{t}'!D2:D,7)=$B$2)),"
                f"\"No staff mentions this month\")"
            ] + [""] * (NC - 1),
        ],
        value_input_option="USER_ENTERED",
    )

    # ── Formatting ────────────────────────────────────────────────────────
    sid = ws.id

    def _grid(r1: int, r2: int | None = None, c1: int = 0, c2: int | None = None) -> dict:
        return {
            "sheetId": sid,
            "startRowIndex": r1 - 1,
            "endRowIndex": r2 if r2 is not None else r1,
            "startColumnIndex": c1,
            "endColumnIndex": c2 if c2 is not None else NC,
        }

    def _fmt(r1: int, r2: int | None = None, c1: int = 0, c2: int | None = None, **f) -> dict:
        return {
            "repeatCell": {
                "range": _grid(r1, r2, c1, c2),
                "cell": {"userEnteredFormat": f},
                "fields": "userEnteredFormat",
            }
        }

    def _merge(r1: int, c1: int = 0, c2: int | None = None) -> dict:
        return {"mergeCells": {
            "range": _grid(r1, r1, c1, c2 if c2 is not None else NC),
            "mergeType": "MERGE_ALL",
        }}

    def _col_w(col_idx: int, px: int) -> dict:
        return {
            "updateDimensionProperties": {
                "range": {"sheetId": sid, "dimension": "COLUMNS",
                          "startIndex": col_idx, "endIndex": col_idx + 1},
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        }

    def _num_fmt(r1: int, r2: int | None, c1: int, c2: int, pattern: str, typ: str = "NUMBER") -> dict:
        return {
            "repeatCell": {
                "range": _grid(r1, r2, c1, c2),
                "cell": {"userEnteredFormat": {"numberFormat": {"type": typ, "pattern": pattern}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        }

    # Delete any existing conditional format rules for this sheet
    try:
        spreadsheet_meta = sheet.fetch_sheet_metadata()
        sheet_data = next(
            (s for s in spreadsheet_meta.get("sheets", []) if s["properties"]["sheetId"] == sid), None
        )
        n_cf = len((sheet_data or {}).get("conditionalFormats", []))
        if n_cf:
            sheet.batch_update({"requests": [
                {"deleteConditionalFormatRule": {"sheetId": sid, "index": 0}}
                for _ in range(n_cf)
            ]})
    except Exception:
        pass  # non-fatal

    reqs = []

    # Reset entire sheet to white / normal text
    reqs.append(_fmt(1, STAFF_START + 3,
                     backgroundColor=WHITE,
                     textFormat={"fontSize": 10, "bold": False, "foregroundColor": DARK_TEXT}))

    # Title: orange, large, bold, white, merged
    reqs.append(_fmt(marks["title"],
                     backgroundColor=ORANGE,
                     textFormat={"bold": True, "fontSize": 15, "foregroundColor": WHITE},
                     horizontalAlignment="CENTER"))
    reqs.append(_merge(marks["title"]))

    # Section headers: dark blue, bold white, merged
    for key in ("overall_hdr", "themes_hdr", "hotspot_hdr", "urgent_hdr", "staff_hdr"):
        if key in marks:
            reqs.append(_fmt(marks[key],
                             backgroundColor=DARK_BLUE,
                             textFormat={"bold": True, "fontSize": 11, "foregroundColor": WHITE}))
            reqs.append(_merge(marks[key]))

    # Hotspot note: italic, gray
    if "hotspot_note" in marks:
        reqs.append(_fmt(marks["hotspot_note"],
                         textFormat={"italic": True, "fontSize": 9, "foregroundColor": {"red": 0.4, "green": 0.4, "blue": 0.4}},
                         backgroundColor=LIGHT_GRAY))
        reqs.append(_merge(marks["hotspot_note"]))

    # Column headers: light blue, bold
    for key in ("overall_col", "themes_col", "hotspot_col", "urgent_col", "staff_col"):
        if key in marks:
            reqs.append(_fmt(marks[key],
                             backgroundColor=LIGHT_BLUE,
                             textFormat={"bold": True, "fontSize": 10, "foregroundColor": DARK_TEXT}))

    # Month selector row: light gray background, B2 bold
    reqs.append(_fmt(marks["month_row"], backgroundColor=LIGHT_GRAY))
    reqs.append(_fmt(marks["month_row"], marks["month_row"], 1, 2,
                     textFormat={"bold": True, "fontSize": 11}))

    # Overall data: light gray, centered; format B as 1dp star, C as 2dp, D-F as %
    reqs.append(_fmt(marks["overall_data"],
                     backgroundColor=LIGHT_GRAY,
                     textFormat={"bold": True, "fontSize": 11},
                     horizontalAlignment="CENTER"))
    reqs.append(_num_fmt(marks["overall_data"], marks["overall_data"], 1, 2, "0.0"))   # avg star
    reqs.append(_num_fmt(marks["overall_data"], marks["overall_data"], 2, 3, "0.00"))  # avg sentiment
    reqs.append(_num_fmt(marks["overall_data"], marks["overall_data"], 3, 6, "0%"))    # pct cols

    # Theme rows: alternating white/light gray
    if "themes_data_start" in marks:
        for i in range(marks["themes_data_start"], marks["themes_data_end"] + 1):
            bg = WHITE if (i - marks["themes_data_start"]) % 2 == 0 else LIGHT_GRAY
            reqs.append(_fmt(i, backgroundColor=bg))
        # % col as percent
        reqs.append(_num_fmt(marks["themes_data_start"], marks["themes_data_end"] + 1, 6, 7, "0%"))

    # Hotspot col header: smaller font for theme abbreviations
    if "hotspot_col" in marks:
        reqs.append(_fmt(marks["hotspot_col"], marks["hotspot_col"], 1, 1 + n_theme_cols,
                         textFormat={"bold": True, "fontSize": 9}))

    # Hotspot location column: bold
    if marks.get("hotspot_data_start") and marks.get("hotspot_data_end"):
        if marks["hotspot_data_start"] <= marks["hotspot_data_end"]:
            reqs.append(_fmt(marks["hotspot_data_start"], marks["hotspot_data_end"] + 1,
                             0, 1, textFormat={"bold": True, "fontSize": 10}))
            # Data cells centered
            reqs.append(_fmt(marks["hotspot_data_start"], marks["hotspot_data_end"] + 1,
                             1, 1 + n_theme_cols, horizontalAlignment="CENTER"))

    # Urgent + staff FILTER rows: light styling
    for key in ("urgent_filter", "staff_filter"):
        if key in marks:
            reqs.append(_fmt(marks[key], backgroundColor={"red": 0.98, "green": 0.98, "blue": 0.98}))

    # Column widths: A=200, B-N=75 (theme cols), keep remaining default
    reqs.append(_col_w(0, 200))  # col A: location / theme label
    for ci in range(1, 1 + n_theme_cols):
        reqs.append(_col_w(ci, 78))

    sheet.batch_update({"requests": reqs})

    # Conditional formatting for hotspot matrix (separate batch_update call)
    if marks.get("hotspot_data_start") and marks.get("hotspot_data_end") \
            and marks["hotspot_data_start"] <= marks["hotspot_data_end"]:
        hotspot_range = _grid(marks["hotspot_data_start"], marks["hotspot_data_end"] + 1,
                               1, 1 + n_theme_cols)
        sheet.batch_update({"requests": [
            # Amber: > 0 (checked second so red rule at index 0 wins)
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [hotspot_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                        "format": {"backgroundColor": AMBER},
                    },
                },
                "index": 0,
            }},
            # Red: >= LOCATION_HOTSPOT_MIN (added at index 0 so it takes priority)
            {"addConditionalFormatRule": {
                "rule": {
                    "ranges": [hotspot_range],
                    "booleanRule": {
                        "condition": {"type": "NUMBER_GREATER_THAN_EQ",
                                      "values": [{"userEnteredValue": str(LOCATION_HOTSPOT_MIN)}]},
                        "format": {"backgroundColor": LIGHT_RED},
                    },
                },
                "index": 0,
            }},
        ]})

    logger.info(
        "Formula dashboard set up: %d rows + staff section at row %d. "
        "Edit B2 to filter by month (YYYY-MM).",
        len(rows), STAFF_START,
    )


# ---------------------------------------------------------------------------
# WRITE — Dashboard (exec-ready, formatted, PDF target)
# ---------------------------------------------------------------------------

def write_dashboard(
    review_analyses: list[dict],
    summary: dict,
    period_start: date,
    period_end: date,
    empty_count: int,
    dup_count: int,
    run_date: date | None = None,
) -> None:
    """Write a formatted exec-ready Dashboard tab. This is the PDF export target."""
    from collections import Counter, defaultdict

    if run_date is None:
        run_date = date.today()

    gc = _client()
    sheet = gc.open_by_key(SHEET_ID)
    ws = _open_or_create(sheet, DASHBOARD_TAB, rows=200, cols=6)
    ws.clear()

    NC = 6  # columns A–F

    # ── Color palette ────────────────────────────────────────────────────
    ORANGE     = {"red": 0.957, "green": 0.522, "blue": 0.098}
    DARK_BLUE  = {"red": 0.106, "green": 0.298, "blue": 0.569}
    LIGHT_BLUE = {"red": 0.839, "green": 0.890, "blue": 0.957}
    AMBER      = {"red": 1.000, "green": 0.949, "blue": 0.800}
    LIGHT_RED  = {"red": 1.000, "green": 0.898, "blue": 0.898}
    WHITE      = {"red": 1.000, "green": 1.000, "blue": 1.000}
    LIGHT_GRAY = {"red": 0.961, "green": 0.961, "blue": 0.961}
    DARK_TEXT  = {"red": 0.150, "green": 0.150, "blue": 0.150}

    # ── Derived metrics ──────────────────────────────────────────────────
    by_loc = summary.get("by_location", {})
    total = summary.get("total_reviews", 0)

    if by_loc and total:
        avg_star = round(
            sum(v["average_star_rating"] * v["review_count"] for v in by_loc.values()) / total, 1
        )
        avg_sentiment = round(
            sum(v["average_sentiment_score"] * v["review_count"] for v in by_loc.values()) / total, 2
        )
    else:
        avg_star = avg_sentiment = 0

    sc = Counter(r.get("sentiment", "") for r in review_analyses)
    n = len(review_analyses) or 1
    pct_pos    = f"{round(sc['positive'] / n * 100)}%"
    pct_neu    = f"{round((sc['neutral'] + sc['mixed']) / n * 100)}%"
    pct_neg_ov = f"{round(sc['negative'] / n * 100)}%"

    theme_pos: Counter = Counter()
    theme_neg: Counter = Counter()
    theme_total: Counter = Counter()
    for r in review_analyses:
        s = r.get("sentiment", "")
        for t in r.get("themes", []):
            theme_total[t] += 1
            if s == "positive":
                theme_pos[t] += 1
            elif s in ("negative", "mixed"):
                theme_neg[t] += 1

    # Location hotspots: (location, theme) with >= LOCATION_HOTSPOT_MIN neg/mixed reviews
    loc_theme_neg: dict = defaultdict(list)
    for r in review_analyses:
        if r.get("sentiment") == "negative":  # strictly negative only — mixed/positive don't count
            loc = r.get("location", "")
            for t in r.get("themes", []):
                loc_theme_neg[(loc, t)].append(r)

    _HOTSPOT_VALID = set(APPROVED_THEMES) - {"general_negative", "general_positive"}
    hotspots = []
    for (loc, theme), reviews in loc_theme_neg.items():
        if len(reviews) >= LOCATION_HOTSPOT_MIN and theme in _HOTSPOT_VALID:
            qr = next((r for r in reviews if r.get("representative_quote")), reviews[0])
            q = (qr.get("representative_quote") or "")[:120]
            hotspots.append({
                "location": loc, "theme": theme,
                "count": len(reviews),
                "quote": f'"{q}"' if q else "",
            })
    hotspots.sort(key=lambda x: x["count"], reverse=True)

    # ── Build row data ────────────────────────────────────────────────────
    rows: list[list] = []
    marks: dict[str, int] = {}  # section key → 1-based row number

    def rn() -> int:
        return len(rows) + 1

    def push(*cells) -> None:
        rows.append((list(cells) + [""] * NC)[:NC])

    same_month = (period_start.year == period_end.year and period_start.month == period_end.month)
    period_label = (
        period_start.strftime("%B %Y") if same_month
        else f"{period_start.strftime('%b %Y')} – {period_end.strftime('%b %Y')}"
    )
    star_str = "★" * int(avg_star) + "☆" * (5 - int(avg_star)) + f"  {avg_star}"

    marks["title"] = rn();    push(f"SHINY SHELL CARWASH  ·  {period_label.upper()}")
    marks["subtitle"] = rn(); push(f"{total} reviews analyzed   ·   Run date: {run_date}")
    push()

    marks["glance_hdr"] = rn(); push("AT A GLANCE")
    marks["glance_col"] = rn(); push("Avg Star Rating", "Avg Sentiment Score", "Positive", "Neutral / Mixed", "Negative", "Total Reviews")
    marks["glance_data"] = rn(); push(star_str, avg_sentiment, pct_pos, pct_neu, pct_neg_ov, total)
    push()

    marks["pos_hdr"] = rn(); push("WHAT'S WORKING")
    marks["pos_text"] = rn(); push(summary.get("top_positive_drivers", ""))
    push()

    marks["neg_hdr"] = rn(); push("AREAS TO WATCH")
    marks["neg_text"] = rn(); push(summary.get("top_negative_drivers", ""))
    push()

    marks["themes_hdr"] = rn(); push("TOP THEMES")
    marks["themes_col"] = rn(); push("Theme", "Reviews Mentioning", "Positive", "Negative / Mixed", "% Neg or Mixed", "")
    marks["themes_data_start"] = rn()
    for theme, count in sorted(theme_total.items(), key=lambda x: x[1], reverse=True):
        pos = theme_pos[theme]; neg = theme_neg[theme]
        push(theme, count, pos, neg, f"{round(neg / count * 100)}%" if count else "")
    marks["themes_data_end"] = rn() - 1
    push()

    staff = summary.get("staff_to_recognize", [])
    if staff:
        marks["staff_hdr"] = rn(); push("STAFF TO RECOGNIZE")
        marks["staff_col"] = rn(); push("Name", "Location", "Mentions")
        marks["staff_data_start"] = rn()
        for s in [s for s in staff if s.get("mention_count", 0) >= 3]:
            push(s.get("name", ""), s.get("location", ""), s.get("mention_count", 0))
        marks["staff_data_end"] = rn() - 1
        push()

    if hotspots:
        marks["hotspot_hdr"] = rn()
        push(f"LOCATION HOTSPOTS  —  {LOCATION_HOTSPOT_MIN}+ negative reviews on same theme at one location")
        marks["hotspot_col"] = rn(); push("Location", "Theme", "Neg Reviews", "Sample Quote")
        marks["hotspot_data_start"] = rn()
        for h in hotspots:
            push(h["location"], h["theme"], h["count"], h["quote"])
        marks["hotspot_data_end"] = rn() - 1
        push()

    urgent = summary.get("urgent_callouts", [])
    if urgent:
        # Group by location; look up themes from review_analyses for concise labels
        review_theme_lookup = {r.get("review_id", ""): r.get("themes", []) for r in review_analyses}

        urgent_by_loc: dict = defaultdict(list)
        for u in urgent:
            urgent_by_loc[u.get("location", "Unknown")].append(u)

        def _short_desc(desc: str, max_chars: int = 55) -> str:
            """Trim a full-sentence description to a short readable phrase."""
            # Stop at first clause boundary if one falls within max_chars
            for sep in ("(", ";", " - "):
                idx = desc.find(sep)
                if 0 < idx <= max_chars:
                    return desc[:idx].rstrip(" ,").rstrip(".")
            if len(desc) <= max_chars:
                return desc.rstrip(".")
            # Hard truncate at last word boundary before max_chars
            return desc[:max_chars].rsplit(" ", 1)[0].rstrip(" ,") + "…"

        consolidated_urgent = []
        for loc, callouts in sorted(urgent_by_loc.items(), key=lambda x: len(x[1]), reverse=True):
            short_descs = [_short_desc(c.get("description", "")) for c in callouts if c.get("description")]
            issue_summary = " · ".join(short_descs) if short_descs else "—"
            dates = sorted(d for d in (c.get("review_date", "") for c in callouts) if d)
            most_recent = dates[-1] if dates else ""
            consolidated_urgent.append({
                "location": loc,
                "count": len(callouts),
                "most_recent": most_recent,
                "issue_summary": issue_summary,
            })

        marks["urgent_hdr"] = rn(); push("URGENT FLAGS")
        marks["urgent_col"] = rn(); push("Location", "Issues", "Most Recent", "Summary")
        marks["urgent_data_start"] = rn()
        for u in consolidated_urgent:
            push(u["location"], u["count"], u["most_recent"], u["issue_summary"])
        marks["urgent_data_end"] = rn() - 1

    # ── Write values ──────────────────────────────────────────────────────
    # RAW prevents Sheets from converting "6%" → 0.06 or "2026-04-03" → serial number
    ws.update("A1", rows, value_input_option="RAW")

    # ── Batch formatting ──────────────────────────────────────────────────
    sid = ws.id

    def _grid(r1: int, r2: int = None, c1: int = 0, c2: int = NC) -> dict:
        """GridRange using 1-based inclusive row numbers → 0-based exclusive API format."""
        return {
            "sheetId": sid,
            "startRowIndex": r1 - 1,
            "endRowIndex": r2 if r2 is not None else r1,
            "startColumnIndex": c1,
            "endColumnIndex": c2,
        }

    def _fmt(r1: int, r2: int = None, c1: int = 0, c2: int = NC, **f) -> dict:
        return {
            "repeatCell": {
                "range": _grid(r1, r2, c1, c2),
                "cell": {"userEnteredFormat": f},
                "fields": "userEnteredFormat",
            }
        }

    def _merge(r1: int, c1: int = 0, c2: int = NC) -> dict:
        return {"mergeCells": {"range": _grid(r1, r1, c1, c2), "mergeType": "MERGE_ALL"}}

    def _col_w(col_idx: int, px: int) -> dict:
        return {
            "updateDimensionProperties": {
                "range": {"sheetId": sid, "dimension": "COLUMNS",
                          "startIndex": col_idx, "endIndex": col_idx + 1},
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        }

    reqs = []

    # Unmerge ALL cells first — ws.clear() only clears values, not merges.
    # Leftover merges from prior runs shift when row counts change and silently
    # hide data in columns B-D of whatever rows they now overlap.
    # Use ws.col_count (not NC=6) so we cover wider merges left by setup_formula_dashboard,
    # which creates 14+ columns — a partial-overlap unmerge causes a 400 from the Sheets API.
    reqs.append({"unmergeCells": {"range": _grid(1, 200, 0, ws.col_count)}})

    # Reset all formatting to a clean baseline
    reqs.append(_fmt(1, len(rows), backgroundColor=WHITE,
                     textFormat={"fontSize": 10, "bold": False, "foregroundColor": DARK_TEXT}))

    # Title (orange, large, centered, merged)
    reqs.append(_fmt(marks["title"],
                     backgroundColor=ORANGE,
                     textFormat={"bold": True, "fontSize": 16, "foregroundColor": WHITE},
                     horizontalAlignment="CENTER"))
    reqs.append(_merge(marks["title"]))

    # Subtitle (orange, small italic, merged)
    reqs.append(_fmt(marks["subtitle"],
                     backgroundColor=ORANGE,
                     textFormat={"fontSize": 10, "foregroundColor": WHITE, "italic": True},
                     horizontalAlignment="CENTER"))
    reqs.append(_merge(marks["subtitle"]))

    # Section headers: dark blue, white bold text, merged
    for key in ("glance_hdr", "pos_hdr", "neg_hdr", "themes_hdr",
                "staff_hdr", "hotspot_hdr", "urgent_hdr"):
        if key in marks:
            reqs.append(_fmt(marks[key],
                             backgroundColor=DARK_BLUE,
                             textFormat={"bold": True, "fontSize": 11, "foregroundColor": WHITE}))
            reqs.append(_merge(marks[key]))

    # Column headers: light blue, bold
    for key in ("glance_col", "themes_col", "staff_col", "hotspot_col", "urgent_col"):
        if key in marks:
            reqs.append(_fmt(marks[key],
                             backgroundColor=LIGHT_BLUE,
                             textFormat={"bold": True, "fontSize": 10, "foregroundColor": DARK_TEXT}))

    # At-a-glance data: light gray, bold, centered
    if "glance_data" in marks:
        reqs.append(_fmt(marks["glance_data"],
                         backgroundColor=LIGHT_GRAY,
                         textFormat={"bold": True, "fontSize": 11},
                         horizontalAlignment="CENTER"))

    # Narrative text: wrap + merge across all columns
    for key in ("pos_text", "neg_text"):
        if key in marks:
            reqs.append(_fmt(marks[key], wrapStrategy="WRAP",
                             textFormat={"fontSize": 10}))
            reqs.append(_merge(marks[key]))

    # Theme rows: alternating white/gray
    if "themes_data_start" in marks:
        for i in range(marks["themes_data_start"], marks["themes_data_end"] + 1):
            bg = WHITE if (i - marks["themes_data_start"]) % 2 == 0 else LIGHT_GRAY
            reqs.append(_fmt(i, backgroundColor=bg, textFormat={"fontSize": 10}))

    # Staff rows: alternating white/gray
    if "staff_data_start" in marks:
        for i in range(marks["staff_data_start"], marks["staff_data_end"] + 1):
            bg = WHITE if (i - marks["staff_data_start"]) % 2 == 0 else LIGHT_GRAY
            reqs.append(_fmt(i, backgroundColor=bg, textFormat={"fontSize": 10}))

    # Hotspot rows: amber background, quote column italic + wrap
    if "hotspot_data_start" in marks:
        for i in range(marks["hotspot_data_start"], marks["hotspot_data_end"] + 1):
            reqs.append(_fmt(i, backgroundColor=AMBER, textFormat={"fontSize": 10}))
            reqs.append(_fmt(i, i, 3, 4,  # column D only (quote)
                             backgroundColor=AMBER,
                             textFormat={"fontSize": 10, "italic": True},
                             wrapStrategy="WRAP"))

    # Urgent rows: light red, description column wraps
    if "urgent_data_start" in marks:
        for i in range(marks["urgent_data_start"], marks["urgent_data_end"] + 1):
            reqs.append(_fmt(i, backgroundColor=LIGHT_RED, textFormat={"fontSize": 10}))
            reqs.append(_fmt(i, i, 2, 3,  # column C only (description)
                             backgroundColor=LIGHT_RED,
                             textFormat={"fontSize": 10},
                             wrapStrategy="WRAP"))

    # Column widths: A=180, B=150, C=110, D=260 (quotes), E=130, F=60
    for col_idx, px in enumerate([180, 150, 110, 260, 130, 60]):
        reqs.append(_col_w(col_idx, px))

    sheet.batch_update({"requests": reqs})
    logger.info(f"Wrote dashboard ({len(rows)} rows) to '{DASHBOARD_TAB}'")
