"""
Google Reviews Sentiment Analysis — Phase 1 entry point.

Usage:
    python main.py --mode baseline          # all reviews from Oct 2025 to today
    python main.py --mode monthly           # reviews from last calendar month
    python main.py --mode baseline --dry-run  # print JSON, don't write to Sheet
    python main.py --mode daily             # analyze reviews in the database not yet analyzed
    python main.py --mode daily --limit 20 --dry-run   # try a few, write nothing
    python main.py --mode reanalyze         # redo results from an older prompt version (resumable)
    python main.py --mode import-sheet      # one-time: copy the Sheet's existing results into the database

Daily and import-sheet read and write the Shiny Shell database (see
sentiment_db.py) for the dashboard's Customer Sentiment page; they don't
touch the Sheet. When the database is configured, monthly and backfill use
the database's results first, so the PDF and the dashboard agree.

Credentials:
    Set GOOGLE_APPLICATION_CREDENTIALS to your service_account.json path,
    or place service_account.json at the repo root (Google-Reviews/).
"""

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
import calendar

from dotenv import load_dotenv

import sentiment_db
from config import BASELINE_START, BATCH_SIZE, GCP_LOCATION, GCP_PROJECT, GEMINI_MODEL, PROMPT_VERSION
from dedup import dedup_reviews, make_review_id
from llm import analyze_batch, generate_narrative
from models import Review
from sheets import append_history, read_analyzed_reviews, read_reviews, setup_formula_dashboard, write_current, write_dashboard, write_reviews, write_theme_breakdown

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _load_cache() -> dict[str, dict]:
    """Results from the Sheet, overridden by the database's (the dashboard's) when configured."""
    cache = read_analyzed_reviews()
    if sentiment_db.db_enabled():
        try:
            connector, conn = sentiment_db.connect()
            try:
                db_cache = sentiment_db.read_analyses_cache(conn)
            finally:
                conn.close()
                connector.close()
        except Exception:
            # The PDF still goes out on the Sheet's results.
            logger.exception("Couldn't read stored results from the database -- using the Sheet's only")
            return cache
        logger.info("Database: %d stored review analyses (used before the Sheet's)", len(db_cache))
        cache.update(db_cache)
    return cache


def run_daily(dry_run: bool = False, limit: int | None = None, reanalyze: bool = False) -> None:
    """
    Analyze every text review in the database that has no result yet, in
    batches, committing after each batch (a failure keeps earlier batches;
    the next run picks up the rest). The first run is the backfill.

    reanalyze=True also redoes results stored with an older prompt version
    (PROMPT_VERSION in config.py), e.g. the one-time v2 -> v3 re-run; run it
    again until it reports nothing left.
    """
    if not sentiment_db.db_enabled():
        raise RuntimeError("DB_INSTANCE_CONNECTION_NAME is not set -- daily mode needs the database")

    connector, conn = sentiment_db.connect()
    try:
        reviews = sentiment_db.read_unanalyzed(conn, outdated=reanalyze)
        logger.info("%s text reviews in the database: %d",
                    f"Unanalyzed or pre-{PROMPT_VERSION}" if reanalyze else "Unanalyzed", len(reviews))
        if limit:
            reviews = reviews[:limit]
        if not reviews:
            logger.info("Nothing to analyze.")
            return
        known_staff = sentiment_db.read_known_staff(conn)

        written = mentions = missing = 0
        total_batches = (len(reviews) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(reviews), BATCH_SIZE):
            batch = reviews[i : i + BATCH_SIZE]
            logger.info("Batch %d/%d: %d reviews", i // BATCH_SIZE + 1, total_batches, len(batch))
            result = analyze_batch(batch, GCP_PROJECT, GCP_LOCATION)

            # Keep only results that map back to a review in this batch.
            wanted = {str(r.db_review_id): r.db_review_id for r in batch}
            analyses: dict[int, dict] = {}
            for a in result.get("reviews", []):
                rid = wanted.get(str(a.get("review_id", "")).strip())
                if rid is not None:
                    analyses[rid] = a
            missing += len(batch) - len(analyses)
            if len(analyses) < len(batch):
                logger.warning("Batch returned %d of %d reviews; the rest are retried next run",
                               len(analyses), len(batch))

            if dry_run:
                print(json.dumps({str(k): v for k, v in analyses.items()}, indent=2, ensure_ascii=False))
                written += len(analyses)
                continue
            try:
                n, m = sentiment_db.write_analyses(
                    conn, analyses, sentiment_db.SOURCE_RERUN if reanalyze else sentiment_db.SOURCE_DAILY,
                    GEMINI_MODEL, known_staff)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            written += n
            mentions += m

        logger.info("%s %d review analyses (%d staff mentions); %d not returned by the model.",
                    "Dry run: would write" if dry_run else "Wrote", written, mentions, missing)
    finally:
        conn.close()
        connector.close()


def run_import_sheet(dry_run: bool = False) -> None:
    """
    One-time: copy the monthly runs' existing per-review results (Sentiment -
    Reviews tab) into the database for reviews not yet analyzed there, so past
    results match the PDFs already sent and cost nothing. Matched on place
    name + author + publish date (the Sheet's review_id); a key shared by two
    database reviews is skipped (left for daily mode). Run before the first
    daily run.
    """
    if not sentiment_db.db_enabled():
        raise RuntimeError("DB_INSTANCE_CONNECTION_NAME is not set -- import-sheet needs the database")

    sheet = read_analyzed_reviews()
    by_key = {(a.get("location", "").strip().lower(), rid): a for rid, a in sheet.items()}

    connector, conn = sentiment_db.connect()
    try:
        reviews = sentiment_db.read_unanalyzed(conn)
        ids_by_key: dict[tuple, list[int]] = defaultdict(list)
        for r in reviews:
            ids_by_key[(r.place.strip().lower(), make_review_id(r))].append(r.db_review_id)

        analyses: dict[int, dict] = {}
        ambiguous = 0
        for key, ids in ids_by_key.items():
            if key not in by_key:
                continue
            if len(ids) > 1:
                ambiguous += len(ids)
                continue
            analyses[ids[0]] = by_key[key]
        logger.info(
            "Sheet results: %d | unanalyzed database reviews: %d | matched: %d | ambiguous (skipped): %d | "
            "left for daily mode: %d",
            len(sheet), len(reviews), len(analyses), ambiguous, len(reviews) - len(analyses),
        )
        if dry_run or not analyses:
            return
        known_staff = sentiment_db.read_known_staff(conn)
        try:
            n, m = sentiment_db.write_analyses(
                conn, analyses, sentiment_db.SOURCE_SHEET_IMPORT, None, known_staff,
                prompt_version="v2")
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        logger.info("Imported %d review analyses (%d staff mentions).", n, m)
    finally:
        conn.close()
        connector.close()


def _monthly_period() -> tuple[date, date]:
    today = date.today()
    first_this_month = today.replace(day=1)
    last_month_end = first_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return last_month_start, last_month_end


def _merge_summaries(all_review_analyses: list[dict], summaries: list[dict]) -> dict:
    """
    Build an accurate merged summary from review-level data.
    Narrative text (top_positive_drivers, top_negative_drivers) is taken from
    the last batch since it's qualitative — good enough for multi-batch baseline runs.
    """
    loc_data: dict = defaultdict(lambda: {"count": 0, "stars": 0.0, "sentiment": 0.0, "themes": []})
    theme_sentiments: dict[str, list[str]] = defaultdict(list)

    for r in all_review_analyses:
        loc = r.get("location", "Unknown")
        loc_data[loc]["count"] += 1
        loc_data[loc]["stars"] += r.get("star_rating", 0)
        loc_data[loc]["sentiment"] += r.get("sentiment_score", 0)
        loc_data[loc]["themes"].extend(r.get("themes", []))
        for theme in r.get("themes", []):
            theme_sentiments[theme].append(r.get("sentiment", "neutral"))

    by_location: dict = {}
    for loc, d in loc_data.items():
        count = d["count"]
        theme_counts = Counter(d["themes"])
        by_location[loc] = {
            "review_count": count,
            "average_star_rating": round(d["stars"] / count, 2) if count else 0,
            "average_sentiment_score": round(d["sentiment"] / count, 2) if count else 0,
            "top_themes": [t for t, _ in theme_counts.most_common(5)],
        }

    all_theme_counts: Counter = Counter()
    for d in loc_data.values():
        all_theme_counts.update(d["themes"])

    overall_top_themes = []
    for theme, count in all_theme_counts.most_common():
        sentiments = theme_sentiments[theme]
        pos = sum(1 for s in sentiments if s == "positive")
        neg = sum(1 for s in sentiments if s == "negative")
        leaning = "positive" if pos > neg else "negative" if neg > pos else "mixed"
        overall_top_themes.append({"theme": theme, "count": count, "sentiment_leaning": leaning})

    # Staff: sum mention counts across batches for same name+location
    staff_index: dict[tuple[str, str], dict] = {}
    for summary in summaries:
        for s in summary.get("staff_to_recognize", []):
            key = (s["name"].lower(), s.get("location", "").lower())
            if key in staff_index:
                staff_index[key]["mention_count"] += s.get("mention_count", 0)
            else:
                staff_index[key] = {
                    "name": s["name"],
                    "location": s.get("location", ""),
                    "mention_count": s.get("mention_count", 0),
                }
    staff_to_recognize = sorted(staff_index.values(), key=lambda x: x["mention_count"], reverse=True)

    # Urgent callouts: collect all, dedupe by dedupe_key
    seen_keys: set[str] = set()
    urgent_callouts: list[dict] = []
    for summary in summaries:
        for u in summary.get("urgent_callouts", []):
            k = u.get("dedupe_key", "")
            if k and k not in seen_keys:
                seen_keys.add(k)
                urgent_callouts.append(u)

    last = summaries[-1]
    return {
        "total_reviews": len(all_review_analyses),
        "by_location": by_location,
        "overall_top_themes": overall_top_themes,
        "top_positive_drivers": last.get("top_positive_drivers", ""),
        "top_negative_drivers": last.get("top_negative_drivers", ""),
        "staff_to_recognize": staff_to_recognize,
        "urgent_callouts": urgent_callouts,
    }


def _build_summary_stats(all_review_analyses: list[dict]) -> dict:
    """
    Compute deterministic aggregate summary from review-level data — no LLM needed.
    Narrative fields (top_positive_drivers, top_negative_drivers) are left empty;
    callers should fill them from LLM output when available.
    """
    loc_data: dict = defaultdict(lambda: {"count": 0, "stars": 0.0, "sentiment": 0.0, "themes": []})
    theme_sentiments: dict[str, list[str]] = defaultdict(list)

    for r in all_review_analyses:
        loc = r.get("location", "Unknown")
        loc_data[loc]["count"] += 1
        loc_data[loc]["stars"] += float(r.get("star_rating", 0))
        loc_data[loc]["sentiment"] += float(r.get("sentiment_score", 0))
        loc_data[loc]["themes"].extend(r.get("themes", []))
        for theme in r.get("themes", []):
            theme_sentiments[theme].append(r.get("sentiment", "neutral"))

    by_location: dict = {}
    for loc, d in loc_data.items():
        count = d["count"]
        theme_counts = Counter(d["themes"])
        by_location[loc] = {
            "review_count": count,
            "average_star_rating": round(d["stars"] / count, 2) if count else 0,
            "average_sentiment_score": round(d["sentiment"] / count, 2) if count else 0,
            "top_themes": [t for t, _ in theme_counts.most_common(5)],
        }

    all_theme_counts: Counter = Counter()
    for d in loc_data.values():
        all_theme_counts.update(d["themes"])

    overall_top_themes = []
    for theme, count in all_theme_counts.most_common():
        sentiments = theme_sentiments[theme]
        pos = sum(1 for s in sentiments if s == "positive")
        neg = sum(1 for s in sentiments if s == "negative")
        leaning = "positive" if pos > neg else "negative" if neg > pos else "mixed"
        overall_top_themes.append({"theme": theme, "count": count, "sentiment_leaning": leaning})

    staff_counts: dict[tuple, int] = defaultdict(int)
    for r in all_review_analyses:
        loc = r.get("location", "")
        for name in r.get("staff_mentioned", []):
            if name:
                staff_counts[(name, loc)] += 1
    staff_to_recognize = sorted(
        [{"name": n, "location": l, "mention_count": c} for (n, l), c in staff_counts.items()],
        key=lambda x: x["mention_count"], reverse=True,
    )

    urgent_callouts = []
    for r in all_review_analyses:
        if r.get("needs_ops_followup"):
            neg_aspects = r.get("negative_aspects", [])
            description = neg_aspects[0] if neg_aspects else "Operational issue flagged"
            urgent_callouts.append({
                "location": r.get("location", ""),
                "dedupe_key": r.get("review_id", ""),
                "description": description,
            })

    return {
        "total_reviews": len(all_review_analyses),
        "by_location": by_location,
        "overall_top_themes": overall_top_themes,
        "top_positive_drivers": "",
        "top_negative_drivers": "",
        "staff_to_recognize": staff_to_recognize,
        "urgent_callouts": urgent_callouts,
    }


def _month_periods(start: date, end: date) -> list[tuple[date, date]]:
    """Return (month_start, month_end) tuples from start's month through end's month."""
    periods = []
    cur = start.replace(day=1)
    while cur <= end.replace(day=1):
        last_day = calendar.monthrange(cur.year, cur.month)[1]
        periods.append((cur, cur.replace(day=last_day)))
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)
    return periods


def run_backfill(dry_run: bool = False) -> None:
    """
    Read all reviews once, then loop month-by-month from BASELINE_START through
    the last complete calendar month. Appends one History row per month; writes
    a combined Sentiment - Reviews tab at the end. Never touches Sentiment - Current.
    """
    run_date = date.today()
    baseline_start = date.fromisoformat(BASELINE_START)

    first_this_month = run_date.replace(day=1)
    last_complete_end = first_this_month - timedelta(days=1)
    last_complete_start = last_complete_end.replace(day=1)

    periods = _month_periods(baseline_start, last_complete_start)
    logger.info(
        "Backfill: %d months (%s → %s)",
        len(periods), periods[0][0], periods[-1][1],
    )

    all_raw = read_reviews(since=baseline_start)
    logger.info("Total raw reviews loaded: %d", len(all_raw))

    cache = _load_cache()
    logger.info("Cache: %d previously analyzed reviews", len(cache))

    all_backfill_analyses: list[dict] = []

    for period_start, period_end in periods:
        logger.info("--- Backfill month: %s → %s ---", period_start, period_end)

        ps, pe = period_start, period_end
        month_reviews_raw = []
        for r in all_raw:
            if not r.publish_time:
                continue
            try:
                if ps <= date.fromisoformat(r.publish_time[:10]) <= pe:
                    month_reviews_raw.append(r)
            except ValueError:
                pass
        reviews, dup_count = dedup_reviews(month_reviews_raw)
        text_reviews = [r for r in reviews if r.text.strip()]
        empty_reviews = [r for r in reviews if not r.text.strip()]
        logger.info(
            "%s: %d text, %d empty, %d dupes",
            period_start.strftime("%Y-%m"), len(text_reviews), len(empty_reviews), dup_count,
        )

        if not text_reviews:
            logger.warning("No text reviews for %s — skipping LLM, no history row written.", period_start.strftime("%Y-%m"))
            continue

        review_lookup = {make_review_id(r): r for r in text_reviews}

        new_reviews = [r for r in text_reviews if make_review_id(r) not in cache]
        cached_analyses = [cache[make_review_id(r)] for r in text_reviews if make_review_id(r) in cache]
        logger.info(
            "  %s: %d cached, %d new",
            period_start.strftime("%Y-%m"), len(cached_analyses), len(new_reviews),
        )

        new_analyses: list[dict] = []
        summaries: list[dict] = []

        for i in range(0, len(new_reviews), BATCH_SIZE):
            batch = new_reviews[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(new_reviews) + BATCH_SIZE - 1) // BATCH_SIZE
            logger.info("  Batch %d/%d: %d reviews", batch_num, total_batches, len(batch))
            result = analyze_batch(batch, GCP_PROJECT, GCP_LOCATION)
            new_analyses.extend(result.get("reviews", []))
            summaries.append(result.get("summary", {}))

        month_analyses = cached_analyses + new_analyses
        final_summary = _build_summary_stats(month_analyses)
        if summaries:
            last = summaries[-1]
            final_summary["top_positive_drivers"] = last.get("top_positive_drivers", "")
            final_summary["top_negative_drivers"] = last.get("top_negative_drivers", "")

        for callout in final_summary.get("urgent_callouts", []):
            rid = callout.get("dedupe_key", "")
            review = review_lookup.get(rid)
            if review and review.publish_time:
                callout["review_date"] = review.publish_time[:10]

        if dry_run:
            print(json.dumps({
                "month": str(period_start)[:7],
                "reviews": month_analyses,
                "summary": final_summary,
            }, indent=2))
        else:
            append_history(
                final_summary,
                period_start, period_end,
                text_review_count=len(text_reviews),
                empty_count=len(empty_reviews),
                run_date=run_date,
            )

        all_backfill_analyses.extend(month_analyses)

    if not dry_run and all_backfill_analyses:
        write_reviews(all_backfill_analyses, run_date=run_date)
        write_theme_breakdown()
        logger.info("Backfill complete: %d total reviews written to Reviews tab.", len(all_backfill_analyses))
    elif dry_run:
        logger.info("Dry-run complete: %d total reviews analyzed.", len(all_backfill_analyses))


def run(mode: str, dry_run: bool = False) -> None:
    run_date = date.today()

    if mode == "baseline":
        period_start = date.fromisoformat(BASELINE_START)
        period_end = run_date
    elif mode == "monthly":
        period_start, period_end = _monthly_period()
    else:
        raise ValueError(f"Unknown mode: {mode}")

    logger.info("Mode: %s | Period: %s → %s", mode, period_start, period_end)

    # --- Read & deduplicate ---
    raw = read_reviews(since=period_start, until=period_end)
    reviews, dup_count = dedup_reviews(raw)
    logger.info("After dedup: %d reviews (%d duplicates removed)", len(reviews), dup_count)

    text_reviews = [r for r in reviews if r.text.strip()]
    empty_reviews = [r for r in reviews if not r.text.strip()]
    logger.info("Text: %d | Star-only (empty): %d", len(text_reviews), len(empty_reviews))

    review_lookup = {make_review_id(r): r for r in text_reviews}

    if not text_reviews:
        logger.warning("No text reviews found for this period — nothing to analyze.")
        return

    # --- Load cached analyses, call LLM only for new reviews ---
    cache = _load_cache()
    new_reviews = [r for r in text_reviews if make_review_id(r) not in cache]
    cached_analyses = [cache[make_review_id(r)] for r in text_reviews if make_review_id(r) in cache]
    logger.info("Cached: %d | New (need LLM): %d", len(cached_analyses), len(new_reviews))

    new_analyses: list[dict] = []
    llm_narrative = {"top_positive_drivers": "", "top_negative_drivers": ""}
    summaries: list[dict] = []

    for i in range(0, len(new_reviews), BATCH_SIZE):
        batch = new_reviews[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(new_reviews) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info("Batch %d/%d: %d reviews", batch_num, total_batches, len(batch))
        result = analyze_batch(batch, GCP_PROJECT, GCP_LOCATION)
        new_analyses.extend(result.get("reviews", []))
        summaries.append(result.get("summary", {}))

    if summaries:
        last = summaries[-1]
        llm_narrative = {
            "top_positive_drivers": last.get("top_positive_drivers", ""),
            "top_negative_drivers": last.get("top_negative_drivers", ""),
        }

    # --- Build final summary from all review-level data (deterministic) ---
    all_review_analyses = cached_analyses + new_analyses

    # If all reviews were cached, no batch LLM call ran — generate narrative separately
    if not summaries:
        llm_narrative = generate_narrative(all_review_analyses, GCP_PROJECT, GCP_LOCATION)
    final_summary = _build_summary_stats(all_review_analyses)
    final_summary["top_positive_drivers"] = llm_narrative["top_positive_drivers"]
    final_summary["top_negative_drivers"] = llm_narrative["top_negative_drivers"]

    # --- Enrich urgent callouts with review date ---
    for callout in final_summary.get("urgent_callouts", []):
        rid = callout.get("dedupe_key", "")
        review = review_lookup.get(rid)
        if review and review.publish_time:
            callout["review_date"] = review.publish_time[:10]

    # --- Output ---
    if dry_run:
        print(json.dumps({"reviews": all_review_analyses, "summary": final_summary}, indent=2))
        return

    write_current(
        all_review_analyses, final_summary,
        period_start, period_end,
        empty_count=len(empty_reviews),
        dup_count=dup_count,
        run_date=run_date,
    )
    append_history(
        final_summary,
        period_start, period_end,
        text_review_count=len(text_reviews),
        empty_count=len(empty_reviews),
        run_date=run_date,
    )
    write_reviews(all_review_analyses, run_date=run_date)
    write_theme_breakdown()
    write_dashboard(
        all_review_analyses, final_summary,
        period_start, period_end,
        empty_count=len(empty_reviews),
        dup_count=dup_count,
        run_date=run_date,
    )
    logger.info("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Google Reviews Sentiment Analysis")
    parser.add_argument(
        "--mode", choices=["baseline", "monthly", "backfill", "setup-dashboard", "daily", "import-sheet", "reanalyze"],
        required=True,
        help="baseline = all reviews from Oct 2025; monthly = last calendar month; backfill = month-by-month Oct 2025 → last complete month; setup-dashboard = write formula-driven Dashboard tab; daily = analyze database reviews not yet analyzed; import-sheet = one-time copy of the Sheet's results into the database; reanalyze = daily, plus redo results from an older prompt version",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print JSON / counts instead of writing to the Sheet or database",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="daily / reanalyze: analyze at most this many reviews (for testing)",
    )
    args = parser.parse_args()

    try:
        if args.mode == "daily":
            run_daily(dry_run=args.dry_run, limit=args.limit)
        elif args.mode == "reanalyze":
            run_daily(dry_run=args.dry_run, limit=args.limit, reanalyze=True)
        elif args.mode == "import-sheet":
            run_import_sheet(dry_run=args.dry_run)
        elif args.mode == "backfill":
            run_backfill(dry_run=args.dry_run)
        elif args.mode == "setup-dashboard":
            setup_formula_dashboard()
        else:
            run(args.mode, dry_run=args.dry_run)
    except Exception:
        logger.exception("Analysis failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
