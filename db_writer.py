"""
Writes each weekly run straight into the Shiny Shell Cloud SQL database
(shiny_shell_production), alongside the Google Sheet tabs for now:

  google_place_snapshots   one row per location per run ("Google Reviews Data" tab)
  reviews                  one row per new review ("Reviews (raw)" tab)
  source_files             one provenance row per run

Same conversions as the original load (Shiny Shell Database Migration,
mysql-pilot/32 and 33): blanks -> NULL, review text line breaks -> one space,
publishTime -> DATETIME (UTC), star ratings that aren't whole numbers -> NULL.
Locations are matched by Google place ID through the 'google_places' rows in
location_source_aliases (mysql-pilot/45).

Like the sheet, a review whose dedupe_key is already in the table isn't added
again. A snapshot for a location and date that already exists is updated.

Required env vars (db_enabled() is False without the first, and nothing is written):
  DB_INSTANCE_CONNECTION_NAME   shiny-shell-data:us-central1:shiny-shell-database
  DB_USER, DB_PASSWORD          the reviews_loader login
  DB_NAME                       default: shiny_shell_production
Credentials: GOOGLE_APPLICATION_CREDENTIALS (the service account file the
workflow already writes), which needs the Cloud SQL Client role.
"""

import datetime
import hashlib
import os
import re
import time

SOURCE_TYPE = "google_places_api"


def db_enabled() -> bool:
    return bool(os.getenv("DB_INSTANCE_CONNECTION_NAME"))


def dedupe_key(place_id: str, publish_time: str, text: str) -> str:
    """Same key the sheet uses: place_id, publishTime, and a hash of the stripped text, tab-separated."""
    text_hash = hashlib.sha1((text or "").strip().encode("utf-8")).hexdigest()[:12]
    return f"{place_id}\t{publish_time}\t{text_hash}"


def _blank_to_none(v):
    return None if v is None or (isinstance(v, str) and v.strip() == "") else v


def _published_at(s):
    """'2026-10-04T18:22:05Z' -> datetime (UTC, naive); None if blank or unreadable."""
    s = _blank_to_none(s)
    if s is None:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return None


def _star_rating(v):
    """Whole-number stars only, as in the original load; anything else -> None."""
    v = _blank_to_none(v)
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f.is_integer() else None


def _one_line(text):
    text = _blank_to_none(text)
    return None if text is None else re.sub(r"\s*[\r\n]+\s*", " ", text.strip())


def _connect():
    import pymysql
    from google.cloud.sql.connector import Connector

    connector = Connector()
    # The first handshake on a fresh connector occasionally drops mid-login,
    # so retry a few times before giving up (same as FlexWash-Pipeline).
    last_exc = None
    for attempt in range(3):
        try:
            conn = connector.connect(
                os.environ["DB_INSTANCE_CONNECTION_NAME"],
                "pymysql",
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                db=os.getenv("DB_NAME", "shiny_shell_production"),
                autocommit=False,
                cursorclass=pymysql.cursors.DictCursor,
                charset="utf8mb4",
            )
            return connector, conn
        except Exception as e:
            last_exc = e
            print(f"  (database connection attempt {attempt + 1} failed: {e})")
            time.sleep(2 ** attempt)
    connector.close()
    raise last_exc


def write_run(run_date: str, summary_rows: list[dict], review_rows: list[dict], dry_run: bool = False) -> str:
    """
    Write one run (all in one transaction). summary_rows / review_rows are the
    dicts reviews.py builds for the sheet. Returns a one-line summary.
    """
    connector, conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.source_location_slug AS place_id, a.location_id
                FROM location_source_aliases a
                WHERE a.source_system = 'google_places' AND a.valid_to IS NULL
                """
            )
            locations = {r["place_id"]: r["location_id"] for r in cur.fetchall()}
            unknown = sorted({r["place_id"] for r in summary_rows + review_rows} - set(locations))
            if unknown:
                raise RuntimeError(f"place_id(s) with no 'google_places' alias in location_source_aliases: {unknown}")

            cur.execute(
                """
                INSERT INTO source_files (source_name, source_type, source_url_or_path, captured_at, notes)
                VALUES (%s, %s, %s, %s, 'Written directly by Google-Reviews reviews.py (db_writer.py)')
                ON DUPLICATE KEY UPDATE
                  source_file_id = LAST_INSERT_ID(source_file_id),
                  captured_at = VALUES(captured_at)
                """,
                (f"google_reviews_{run_date}", SOURCE_TYPE, "https://github.com/katehendrickson1/Google-Reviews",
                 datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
            )
            source_file_id = cur.lastrowid

            for s in summary_rows:
                cur.execute(
                    """
                    INSERT INTO google_place_snapshots (
                      location_id, observed_date, place_id, rating, review_count, new_reviews_week,
                      sentiment_label, sentiment_score, report_path, maps_url, source_file_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      rating = VALUES(rating),
                      review_count = VALUES(review_count),
                      new_reviews_week = VALUES(new_reviews_week),
                      sentiment_label = VALUES(sentiment_label),
                      sentiment_score = VALUES(sentiment_score),
                      report_path = VALUES(report_path),
                      maps_url = VALUES(maps_url),
                      source_file_id = VALUES(source_file_id)
                    """,
                    (locations[s["place_id"]], s["date"], s["place_id"], _blank_to_none(s.get("rating")),
                     _blank_to_none(s.get("review_count")), _blank_to_none(s.get("new_reviews_week")),
                     _blank_to_none(s.get("sentiment_label")), _blank_to_none(s.get("sentiment_score")),
                     _blank_to_none(s.get("report_path")), _blank_to_none(s.get("maps_url")), source_file_id),
                )

            keys = {}
            for r in review_rows:
                # Rows copied from the sheet bring the key the sheet stored.
                key = r.get("dedupe_key") or dedupe_key(r["place_id"], r.get("publishTime") or "", r.get("text") or "")
                keys.setdefault(key, r)
            existing = set()
            if keys:
                placeholders = ", ".join(["%s"] * len(keys))
                cur.execute(f"SELECT dedupe_key FROM reviews WHERE dedupe_key IN ({placeholders})", list(keys))
                existing = {row["dedupe_key"] for row in cur.fetchall()}
            new = {k: r for k, r in keys.items() if k not in existing}
            for key, r in new.items():
                cur.execute(
                    """
                    INSERT INTO reviews (
                      location_id, place_id, date_run, author, star_rating, published_at,
                      relative_time, review_text, dedupe_key, source_file_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (locations[r["place_id"]], r["place_id"], r["date_run"], _blank_to_none(r.get("author")),
                     _star_rating(r.get("rating")), _published_at(r.get("publishTime")),
                     _blank_to_none(r.get("relativeTime")), _one_line(r.get("text")), key, source_file_id),
                )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return (f"{'Would write' if dry_run else 'Wrote'} {len(summary_rows)} snapshot(s) and {len(new)} new review(s) "
                f"to the database ({len(keys) - len(new)} review(s) already there).")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        connector.close()
