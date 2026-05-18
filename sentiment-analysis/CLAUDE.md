# CLAUDE.md — Google Reviews Sentiment Analysis Project

## Project Overview

This project performs sentiment and theme analysis on Google Reviews for a multi-location carwash company (20 locations). Reviews are scraped into a Google Sheet (existing KPI workbook). The goal is to run an automated monthly analysis that surfaces themes, sentiment trends, operational issues, and employees to recognize — and deliver it as a PDF email to the corporate team.

Owner: Kate (kate@coldwatercap.com)
Audience for reports: Boss, ops managers, marketing, customer service — the corporate team.
Volume: ~10–20 reviews per week across all 20 locations (~50–80/month total; ~2–4 per location per month).

## Goals

1. **Baseline analysis** — one-time deep analysis on all reviews from October 2025 through present. Establish theme taxonomy, surface biggest issues, identify staff to recognize, set a benchmark.
2. **Monthly recurring analysis** — overall corporate-wide themes, sentiment trends, urgent operational callouts, staff recognition list. Delivered as a PDF email to the corporate team.
3. **Quarterly per-location deep dive** — per-location reports only make sense at quarterly cadence because monthly per-location samples are too small (1–4 reviews/month/location) to be statistically meaningful.

## Tech Stack

- **Language:** Python (3.11+)
- **LLM:** Gemini 2.5 Flash-Lite via Google Cloud Vertex AI (switched from 3.1 which is still Preview; 2.5 is GA)
  - GCP project: `places-review-test-469517`
  - Vertex AI API and Generative Language API enabled
  - Vertex AI is used (NOT AI Studio) so customer review data is not used for model training
- **Data store:** Google Sheets (existing KPI workbook)
  - Read/write via Google Sheets API
- **Automation:** GitHub Actions (monthly cron)
  - Secrets stored in GitHub Actions Secrets
- **Email delivery:** Google Apps Script attached to the Sheet — exports the "Sentiment - Current" tab as PDF and emails the corporate team monthly
- **Auth approach:** Service account JSON stored in GitHub Secrets (for both Vertex AI and Sheets API)

## Architecture Overview

```
[Google Sheet: Raw Reviews tab]
              |
              v
[GitHub Action runs monthly cron, 1st of month]
              |
              v
[Python script]
  1. Read raw reviews from Sheet via Sheets API
  2. Filter out empty-text reviews (count separately, don't send to LLM)
  3. Batch reviews and send to Gemini 3.1 Flash Lite via Vertex AI
  4. Parse structured JSON response
  5. Cross-check sentiment vs star rating as sanity check
  6. Write results back to Sheet:
       - Overwrite "Sentiment - Current" tab (this month's snapshot)
       - Append to "Sentiment - History" tab (one row per location + one overall row per month)
              |
              v
[Google Apps Script triggered separately or by Sheet update]
  - Export "Sentiment - Current" tab as PDF
  - Email PDF to corporate distribution list
```

## Google Sheet Layout

### Existing tabs (already in the KPI workbook)

- **Raw Reviews tab** — contains all scraped Google reviews. Data is already being collected here.

### New tabs to create

- **Sentiment — Current** — overwritten each monthly run. This is the snapshot the PDF captures. Suggested sections:
  - Run metadata (date_run, review count, locations covered)
  - Overall sentiment + top themes table
  - Top positive drivers (free text, 2–3 sentences)
  - Top negative drivers (free text, 2–3 sentences)
  - Per-location summary table (review count, avg star, avg sentiment, top themes)
  - Staff to recognize (name, location, mention count)
  - Urgent ops callouts (location, dedupe_key, description)
  - Sample quotes (good + bad)
- **Sentiment — History** — append-only. One row per location per month + one "Overall" row per month. Columns at minimum:
  - run_date, period_start, period_end, scope (location name or "Overall"), review_count, avg_star, avg_sentiment_score, top_3_themes, top_negative_themes, urgent_count
- **Themes Library** — reference list of the approved theme tags (see Theme Taxonomy below). Used as documentation; Python script enforces this list against LLM output.

## Raw Reviews Data Schema

Columns currently in the Raw Reviews tab:

| Column         | Description                                                       |
| -------------- | ----------------------------------------------------------------- |
| `date_run`     | Date the row was scraped (e.g., 2025-10-13)                       |
| `place`        | Location name (e.g., "Layton", "Kearns", "West Valley", "Murray") |
| `place_id`     | Google Place ID                                                   |
| `author`       | Reviewer's display name                                           |
| `★`            | Star rating, integer 1–5                                          |
| `publishTime`  | ISO timestamp of original review post                             |
| `relativeTime` | Google's relative time string ("in the last week")                |
| `New Reviews:` | The review text (may be empty for star-only reviews)              |
| `dedupe_key`   | Hash for deduplication                                            |

### Data quirks to handle in code

1. **Empty-text reviews share dedupe_key** — every empty review has `da39a3ee5e6b` (SHA-1 of empty string). Don't treat as duplicates; deduplicate empty reviews by author + location + publishTime instead.
2. **Duplicate posts from same author** — some reviewers post the same review twice (e.g., name with and without accent). Deduplicate by (author_normalized, location, date, first_50_chars_of_text).
3. **Skip empty-text reviews from LLM call** — they cost tokens and return nothing useful. Count them separately in summary based on star rating only.
4. **Author name accents** — strip diacritics when deduplicating but preserve original in raw data.

## The Locked LLM Prompt (v2)

This prompt has been tested against a 17-review sample and validated. **Do not modify the theme list or core rules without discussion.**

```
You are analyzing customer reviews of a multi-location carwash company. Return ONE valid JSON object with two keys: "reviews" (array, one object per review) and "summary" (one object covering the full batch).

THEME RULES (read carefully):
- Use ONLY themes from this list: staff_friendliness, staff_helpfulness, equipment_reliability, wash_quality, vehicle_damage, membership_subscription, pricing, service_recovery, vacuums_amenities, safety, cleanliness, facility_condition, members_lounge, general_positive, general_negative.
- Do NOT invent new themes for one-off issues. Fold rare complaints into the closest existing theme. Examples: sticker/adhesive residue → wash_quality; broken kiosk → equipment_reliability; well-stocked or low-supply → facility_condition; long wait → equipment_reliability; rude billing dispute → membership_subscription; soda machine, free drinks, members lounge, drink station → members_lounge. Only invent a new theme if the SAME novel topic appears in 3+ reviews in this batch AND no existing theme can plausibly hold it.
- Tag ALL applicable themes per review, not just the dominant one. A review praising both friendliness AND helpfulness must get both tags. A review with three complaints gets three negative-leaning theme tags. Service recovery scenarios get service_recovery PLUS any other applicable themes.

STAFF NAME RULES:
- Within this batch, treat clear spelling variants of the same name AT THE SAME LOCATION as one employee. Use the most common spelling. Examples: "Julian" / "Julián" → "Julian"; "Yasmis" / "Yazmin" / "Yasmin" → "Yazmin". If genuinely unsure two names refer to the same person, keep them separate.
- Do NOT include the reviewer's own name in staff_mentioned, even if they sign the review (e.g., "Thanks, Walt Booker" — Walt is the reviewer, not staff).
- Use proper-case first names only (e.g., "Julian", not "julian" or "JULIAN").

PER-REVIEW FIELDS:
- review_id: the dedupe_key value
- location: the place name
- star_rating: integer 1-5
- sentiment: "positive" | "neutral" | "negative" | "mixed"
- sentiment_score: number from -1.0 (most negative) to +1.0 (most positive)
- themes: array of theme tags from the approved list (use ALL that apply)
- positive_aspects: short array of what was praised (or [])
- negative_aspects: short array of complaints (or [])
- staff_mentioned: array of normalized employee first names (or [])
- representative_quote: one short quote, max 20 words (or null if review text is empty)
- needs_ops_followup: true if review describes an operational issue worth investigating (equipment failure, vehicle damage, billing/membership problem, repeated bad experience at same location); else false

SUMMARY FIELDS:
- total_reviews
- by_location: object mapping each location → {review_count, average_star_rating, average_sentiment_score, top_themes}
- overall_top_themes: array of {theme, count, sentiment_leaning} sorted by count descending
- top_positive_drivers: 2-3 sentences on what's working well
- top_negative_drivers: 2-3 sentences on the biggest issues
- staff_to_recognize: array of {name, location, mention_count} sorted by mention_count desc — names must already be normalized per the rules above
- urgent_callouts: array of {location, dedupe_key, description} — one line each

Return ONLY valid JSON, no commentary, no markdown code fences.

Reviews:

[reviews go here, formatted as in the test]
```

### Review formatting for the prompt

Reviews should be fed in as a numbered list. Format each as:

```
N. Location: <place> | Author: <author> | Stars: <star_rating> | dedupe_key: <dedupe_key>
"<review text>"
```

If review text is empty, **do not send the review to the LLM** — count it in the post-processing summary based on star rating alone.

## Theme Taxonomy (locked)

| Theme                     | Use for                                                               |
| ------------------------- | --------------------------------------------------------------------- |
| `staff_friendliness`      | Friendly demeanor, warmth, smiles, polite interactions                |
| `staff_helpfulness`       | Active assistance, going above and beyond, guidance                   |
| `equipment_reliability`   | Wash equipment, sensors, kiosks, vacuums not working / breaking down  |
| `wash_quality`            | How well the wash actually cleans (water spots, residue, missed dirt) |
| `vehicle_damage`          | Any physical damage to a customer vehicle                             |
| `membership_subscription` | Membership plans, unlimited wash policies, billing, stickers          |
| `pricing`                 | Cost complaints or value praise                                       |
| `service_recovery`        | Issue was previously bad, was made right by staff/company             |
| `vacuums_amenities`       | Vacuums, interior cleaning stations, free amenities                   |
| `safety`                  | Anything safety-related (loose items, blocked sight lines, etc.)      |
| `cleanliness`             | Cleanliness of the facility itself                                    |
| `facility_condition`      | Stocked supplies, equipment present, general site upkeep              |
| `members_lounge`          | Soda machine, free drinks, drink station, members lounge experience   |
| `general_positive`        | Praise that doesn't fit a specific theme                              |
| `general_negative`        | Complaint that doesn't fit a specific theme                           |

**Do NOT add new themes ad hoc.** If a genuine new theme emerges (same issue in 3+ reviews in one batch), discuss before adding to taxonomy.

## Cadence Strategy

- **Monthly run** — overall corporate-wide analysis. Sent as PDF to corporate team. Surfaces top themes, urgent callouts, staff to recognize, sentiment trend vs. last month.
- **Quarterly run** — same monthly analysis PLUS per-location deep dives. Sample size at quarterly (6–12 reviews per location) is enough to say something meaningful per site.
- **Per-location callouts in monthly** — if a location has a notable spike in complaints or an urgent ops issue, surface it even in the monthly report, but mark "insufficient data for trend" if review count is < 5.
- **Baseline run** — one-time analysis of everything from October 2025 to first run date. This is the benchmark all future reports compare against.

## Known Data Quality Issues

1. **Empty-text reviews collide on dedupe_key.** Need to fix dedup logic upstream OR work around it in this script.
2. **Duplicate posts** (same review posted twice by same author with minor variation like accent characters). Need author+date+content dedup.
3. **No review-response tracking** — we don't currently track whether management responded to reviews. Future improvement.

## Plan of Action (Phased)

### Phase 1 — Local proof of concept

- [ ] Set up Python project structure
- [ ] Implement Sheets read (via Google Sheets API + service account)
- [ ] Implement empty-review filtering and upstream dedup fixes
- [ ] Implement Vertex AI client (Gemini 3.1 Flash Lite)
- [ ] Implement batched LLM calls using the locked v2 prompt
- [ ] Implement JSON parsing + validation against theme taxonomy
- [ ] Implement Sheets write (Current tab overwrite + History tab append)
- [ ] Run end-to-end locally on baseline data

### Phase 2 — Sheet UI polish

- [ ] Design and build the Sentiment - Current tab layout (formatted for PDF export)
- [ ] Build Sentiment - History tab structure
- [ ] Build Themes Library tab as reference

### Phase 3 — Automation

- [ ] Push code to GitHub repo
- [ ] Set up GitHub Actions monthly cron workflow
- [ ] Store service account JSON in GitHub Secrets
- [ ] Validate Action runs successfully end-to-end

### Phase 4 — Distribution

- [ ] Write Apps Script that exports Sentiment - Current as PDF
- [ ] Configure Apps Script trigger (time-based, monthly, after the GH Action runs)
- [ ] Configure email distribution list
- [ ] Test full pipeline

### Phase 5 — Refinement

- [ ] Run for 2–3 months
- [ ] Tune prompt or taxonomy based on what's surfacing
- [ ] Add quarterly per-location deep dive logic

## Open Decisions

- **Auth method** — resolved: service account JSON in GitHub Secrets.
- **Sheet ID** — resolved: `1rAMV-_Xh2Q8wpgAJWzgzYbHu96UO9NmsD1xGHr2Xz1E` (raw reviews tab gid: `7631771`). Already shared with service account.
- **Email distribution list** — who exactly gets the monthly PDF. Likely a Google Group.
- **History tab granularity** — one row per location-month, plus one "Overall" row per month, vs. a wider schema.

## Cost Expectations

At ~50–80 reviews/month, Gemini 3.1 Flash Lite costs will be pennies per run. The baseline analysis (400–600 reviews) will cost on the order of $0.05–$0.10. Not a concern for budgeting.

## Working Notes for Claude Code

- **Do not modify the v2 prompt or theme taxonomy without discussing with Kate first.** Both have been tested and locked.
- **Always filter empty-text reviews before sending to the LLM.** Don't waste tokens on them.
- **Star rating is a sanity check on LLM sentiment.** If LLM marks a 5-star review as negative or a 1-star as positive, log a warning — likely a parsing issue or sarcastic review.
- **Names are normalized within batch by the LLM.** Add a fuzzy-match post-processing step in Python as a safety net (e.g., Levenshtein distance < 2 + same location = same person).
- **The Sheet is the source of truth.** All outputs go back into the Sheet; nothing lives only in code/logs.
- **GCP project name:** `places-review-test-469517`
- **Locations seen so far:** Layton, Kearns, West Valley, Murray. There are 20 total — code should not hardcode location names.
