"""
Per-review sentiment in the Shiny Shell database (shiny_shell_production),
for the dashboard's Customer Sentiment page. Tables and views are in the
Shiny Shell Database Migration repo:
  mysql-pilot/68_create_review_sentiment_tables.sql
    review_sentiment, review_themes, review_staff_mentions
  mysql-pilot/69_create_review_sentiment_views.sql
    v_google_reviews (deduplicated, with review_id and has_text)

Reviews are read from v_google_reviews (so the same review collected twice,
e.g. once auto-translated, is analyzed once) and results are keyed on
reviews.review_id.

Same connection settings as ../db_writer.py (the reviews_loader login):
  DB_INSTANCE_CONNECTION_NAME, DB_USER, DB_PASSWORD, DB_NAME (optional)
"""

import datetime
import logging
import sys
from collections import defaultdict
from pathlib import Path

from config import BASELINE_START
from dedup import make_review_id
from models import Review

# db_writer.py (repo root) already has the Cloud SQL connection with retries.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_writer import _connect, db_enabled  # noqa: E402

logger = logging.getLogger(__name__)

__all__ = [
    "db_enabled", "connect", "read_unanalyzed", "read_known_staff",
    "write_analyses", "read_analyses_cache",
]

SENTIMENTS = {"positive", "neutral", "negative", "mixed"}
SOURCE_DAILY = "gemini_daily"
SOURCE_SHEET_IMPORT = "sheet_import"

# Staff names already stored at a location are reused when a new name is
# within this edit distance (same rule as llm._normalize_staff_names), but
# only for names of at least MIN_FUZZY_NAME_LEN letters so short names like
# "Al" / "Ali" stay separate.
MAX_NAME_DISTANCE = 1
MIN_FUZZY_NAME_LEN = 4


def connect():
    """(connector, connection); close both when done."""
    return _connect()


def _iso(dt) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else ""


def read_unanalyzed(conn) -> list[Review]:
    """
    Text reviews since BASELINE_START with no review_sentiment row yet,
    ordered by location then date so a batch holds reviews from few
    locations (the prompt merges staff-name spellings within a location).
    Review.place is the Google place name used in the Sheet (e.g. "Mt. Airy").
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.review_id, g.location, a.source_location_label AS place,
                   g.author, g.star_rating, g.published_at, g.published_date,
                   g.review_text, g.collected_on
            FROM v_google_reviews g
            JOIN locations l ON l.canonical_name = g.location
            LEFT JOIN location_source_aliases a
              ON a.location_id = l.location_id
             AND a.source_system = 'google_places' AND a.valid_to IS NULL
            LEFT JOIN review_sentiment s ON s.review_id = g.review_id
            WHERE s.review_id IS NULL
              AND g.has_text = 1
              AND g.published_date >= %s
            ORDER BY g.location, g.published_date, g.review_id
            """,
            (BASELINE_START,),
        )
        rows = cur.fetchall()

    reviews = []
    seen = set()
    for r in rows:
        if r["review_id"] in seen:  # a location with two google_places aliases
            continue
        seen.add(r["review_id"])
        publish_time = _iso(r["published_at"]) or f"{r['published_date']}T00:00:00Z"
        reviews.append(Review(
            dedupe_key="",
            place=r["place"] or r["location"],
            place_id="",
            author=r["author"] or "",
            star_rating=int(r["star_rating"] or 0),
            publish_time=publish_time,
            relative_time="",
            text=(r["review_text"] or "").strip(),
            date_run=str(r["collected_on"]),
            db_review_id=int(r["review_id"]),
        ))
    return reviews


def read_known_staff(conn) -> dict[int, set[str]]:
    """{location_id: staff names already stored there}."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT r.location_id, m.staff_name
            FROM review_staff_mentions m
            JOIN reviews r ON r.review_id = m.review_id
            """
        )
        known: dict[int, set[str]] = defaultdict(set)
        for row in cur.fetchall():
            known[row["location_id"]].add(row["staff_name"])
        return known


def _review_locations(cur, review_ids: list[int]) -> dict[int, int]:
    if not review_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(review_ids))
    cur.execute(f"SELECT review_id, location_id FROM reviews WHERE review_id IN ({placeholders})", review_ids)
    return {row["review_id"]: row["location_id"] for row in cur.fetchall()}


def _match_name(name: str, known: set[str]) -> str:
    """Reuse a stored spelling of this name at the location when it's clearly the same person."""
    from rapidfuzz.distance import Levenshtein

    name = " ".join(w.capitalize() for w in name.strip().split())
    if not name:
        return name
    lower = name.lower()
    for k in known:
        if k.lower() == lower:
            return k
    if len(name) >= MIN_FUZZY_NAME_LEN:
        close = [k for k in known
                 if len(k) >= MIN_FUZZY_NAME_LEN
                 and Levenshtein.distance(k.lower(), lower) <= MAX_NAME_DISTANCE]
        if len(close) == 1:
            return close[0]
    return name


def _aspects(values) -> str | None:
    items = [str(v).strip() for v in (values or []) if str(v).strip()]
    return " | ".join(items) if items else None


def _score(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return round(max(-1.0, min(1.0, f)), 2)


def write_analyses(conn, analyses: dict[int, dict], source: str, model: str | None,
                   known_staff: dict[int, set[str]]) -> tuple[int, int]:
    """
    Upsert one result per review_id (themes and staff names replaced) in the
    caller's transaction. `known_staff` is updated with the names written.
    Returns (reviews written, staff mentions written).
    """
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    mentions = 0
    with conn.cursor() as cur:
        locations = _review_locations(cur, list(analyses))
        for review_id, a in analyses.items():
            sentiment = str(a.get("sentiment") or "").strip().lower()
            if sentiment not in SENTIMENTS:
                logger.warning("Review %s: unexpected sentiment %r -- stored as given", review_id, sentiment)
            quote = a.get("representative_quote")
            quote = str(quote).strip()[:500] if quote else None
            cur.execute(
                """
                INSERT INTO review_sentiment (
                  review_id, sentiment, sentiment_score, needs_ops_followup,
                  representative_quote, positive_aspects, negative_aspects,
                  analysis_source, model, prompt_version, analyzed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'v2', %s)
                ON DUPLICATE KEY UPDATE
                  sentiment = VALUES(sentiment),
                  sentiment_score = VALUES(sentiment_score),
                  needs_ops_followup = VALUES(needs_ops_followup),
                  representative_quote = VALUES(representative_quote),
                  positive_aspects = VALUES(positive_aspects),
                  negative_aspects = VALUES(negative_aspects),
                  analysis_source = VALUES(analysis_source),
                  model = VALUES(model),
                  prompt_version = VALUES(prompt_version),
                  analyzed_at = VALUES(analyzed_at)
                """,
                (review_id, sentiment[:16] or "neutral", _score(a.get("sentiment_score")),
                 1 if a.get("needs_ops_followup") is True else 0,
                 quote, _aspects(a.get("positive_aspects")), _aspects(a.get("negative_aspects")),
                 source, model, now),
            )
            cur.execute("DELETE FROM review_themes WHERE review_id = %s", (review_id,))
            themes = {str(t).strip().lower()[:40] for t in (a.get("themes") or []) if str(t).strip()}
            for theme in sorted(themes):
                cur.execute("INSERT INTO review_themes (review_id, theme) VALUES (%s, %s)", (review_id, theme))

            cur.execute("DELETE FROM review_staff_mentions WHERE review_id = %s", (review_id,))
            location_id = locations.get(review_id)
            known = known_staff[location_id]
            names = set()
            for raw in a.get("staff_mentioned") or []:
                name = _match_name(str(raw), known)[:64]
                if name:
                    names.add(name)
            for name in sorted(names):
                cur.execute("INSERT INTO review_staff_mentions (review_id, staff_name) VALUES (%s, %s)",
                            (review_id, name))
                known.add(name)
                mentions += 1
    return len(analyses), mentions


def read_analyses_cache(conn) -> dict[str, dict]:
    """
    Stored results in the shape the monthly run caches ({make_review_id:
    analysis}), so the monthly PDF uses the same results as the dashboard
    instead of re-analyzing. `location` is the Google place name, as in the
    Sheet.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.review_id, a.source_location_label AS place, g.location,
                   g.author, g.star_rating, g.published_at, g.published_date,
                   s.sentiment, s.sentiment_score, s.needs_ops_followup,
                   s.representative_quote, s.positive_aspects, s.negative_aspects,
                   (SELECT GROUP_CONCAT(t.theme ORDER BY t.theme SEPARATOR ',')
                      FROM review_themes t WHERE t.review_id = g.review_id) AS themes,
                   (SELECT GROUP_CONCAT(m.staff_name ORDER BY m.staff_name SEPARATOR ',')
                      FROM review_staff_mentions m WHERE m.review_id = g.review_id) AS staff
            FROM v_google_reviews g
            JOIN review_sentiment s ON s.review_id = g.review_id
            JOIN locations l ON l.canonical_name = g.location
            LEFT JOIN location_source_aliases a
              ON a.location_id = l.location_id
             AND a.source_system = 'google_places' AND a.valid_to IS NULL
            """
        )
        rows = cur.fetchall()

    cache: dict[str, dict] = {}
    for r in rows:
        review = Review(
            dedupe_key="", place=r["place"] or r["location"], place_id="",
            author=r["author"] or "", star_rating=int(r["star_rating"] or 0),
            publish_time=_iso(r["published_at"]) or f"{r['published_date']}T00:00:00Z",
            relative_time="", text="", date_run="",
        )
        rid = make_review_id(review)
        cache[rid] = {
            "review_id": rid,
            "location": review.place,
            "star_rating": review.star_rating,
            "sentiment": r["sentiment"],
            "sentiment_score": float(r["sentiment_score"]) if r["sentiment_score"] is not None else 0.0,
            "themes": (r["themes"] or "").split(",") if r["themes"] else [],
            "positive_aspects": (r["positive_aspects"] or "").split(" | ") if r["positive_aspects"] else [],
            "negative_aspects": (r["negative_aspects"] or "").split(" | ") if r["negative_aspects"] else [],
            "staff_mentioned": (r["staff"] or "").split(",") if r["staff"] else [],
            "representative_quote": r["representative_quote"],
            "needs_ops_followup": bool(r["needs_ops_followup"]),
        }
    return cache
