"""
One-time: copy runs that only exist in the Google Sheet into the Shiny Shell
database -- the runs between the database's original load (sheet export of
2026-09-25) and the first run that wrote the database directly. After that,
reviews.py writes every run itself and this isn't needed.

Reads the "Google Reviews Data" and "Reviews (raw)" tabs for run dates in
[--start, --end] and writes each date through db_writer.write_run (same
conversions, same "skip reviews already in the table" rule; snapshots for a
location and date that already exist are updated).

Run locally, with the DB_* variables and service_account.json as for reviews.py:
    python backfill_db_from_sheet.py --start 2026-09-22 --end 2026-10-11 --dry-run
    python backfill_db_from_sheet.py --start 2026-09-22 --end 2026-10-11
Read-only on the sheet; never touches state_reviews.json, Slack, or the Places API.
"""

import argparse
import datetime
import sys

from db_writer import db_enabled, write_run
from reviews import SHEET_ID, get_gspread_client


def parse_date(s: str) -> str | None:
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError:
            pass
    return None


def number(s: str):
    """'1,234' -> '1234'; blanks stay blank."""
    return (s or "").replace(",", "").strip()


def tab_rows(sheet, title: str) -> list[dict]:
    values = sheet.worksheet(title).get_all_values()
    header = values[0]
    return [dict(zip(header, row + [""] * (len(header) - len(row)))) for row in values[1:]]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="First run date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="Last run date YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true", help="Roll back instead of committing")
    args = parser.parse_args()
    if not db_enabled():
        sys.exit("DB_INSTANCE_CONNECTION_NAME not set.")

    sheet = get_gspread_client().open_by_key(SHEET_ID)
    by_date: dict[str, dict] = {}
    for r in tab_rows(sheet, "Google Reviews Data"):
        d = parse_date(r.get("date", ""))
        if d and args.start <= d <= args.end:
            by_date.setdefault(d, {"summary": [], "reviews": []})["summary"].append(dict(
                r, date=d, rating=number(r.get("rating")), review_count=number(r.get("review_count")),
                new_reviews_week=number(r.get("new_reviews_week")), sentiment_score=number(r.get("sentiment_score"))))
    for r in tab_rows(sheet, "Reviews (raw)"):
        d = parse_date(r.get("date_run", ""))
        if d and args.start <= d <= args.end:
            by_date.setdefault(d, {"summary": [], "reviews": []})["reviews"].append(dict(r, date_run=d))

    if not by_date:
        sys.exit(f"No sheet rows with run dates {args.start}..{args.end}.")
    for d in sorted(by_date):
        rows = by_date[d]
        print(f"{d}: {write_run(d, rows['summary'], rows['reviews'], dry_run=args.dry_run)}")


if __name__ == "__main__":
    main()
