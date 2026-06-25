import json
import logging
import time

from google import genai
from google.genai import errors as genai_errors
from google.genai import types

from auth import vertex_credentials
from config import APPROVED_THEMES, GCP_LOCATION, GCP_PROJECT, GEMINI_MODEL
from dedup import make_review_id
from models import Review

logger = logging.getLogger(__name__)

# Locked v2 prompt — do not modify without discussing with Kate first.
# Tested against a 17-review sample and validated.
_PROMPT_TEMPLATE = """\
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

{reviews}"""


def _format_review(n: int, r: Review) -> str:
    return f'{n}. Location: {r.place} | Author: {r.author} | Stars: {r.star_rating} | dedupe_key: {make_review_id(r)}\n"{r.text}"'


def _strip_fences(text: str) -> str:
    """Strip markdown code fences if the model returns them despite instructions."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # drop first line (```json or ```) and last line (```)
        text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    return text.strip()


def _validate_themes(review_analyses: list[dict]) -> None:
    """Log warnings for any themes not in the approved list."""
    approved = set(APPROVED_THEMES)
    for r in review_analyses:
        bad = [t for t in r.get("themes", []) if t not in approved]
        if bad:
            logger.warning(
                "Unapproved theme(s) %s in review %s — keeping but flagging",
                bad, r.get("review_id")
            )


def _sanity_check_sentiment(review_analyses: list[dict]) -> None:
    """Warn when LLM sentiment contradicts star rating — likely a parsing issue or sarcasm."""
    for r in review_analyses:
        stars = r.get("star_rating", 0)
        sentiment = r.get("sentiment", "")
        if stars == 5 and sentiment == "negative":
            logger.warning("5-star review marked negative: %s", r.get("review_id"))
        elif stars == 1 and sentiment == "positive":
            logger.warning("1-star review marked positive: %s", r.get("review_id"))


def _normalize_staff_names(review_analyses: list[dict]) -> None:
    """
    Post-processing safety net: merge staff names within the same location
    when edit distance < 2 (catches cases the LLM misses across a large batch).
    """
    from collections import Counter, defaultdict
    from rapidfuzz.distance import Levenshtein

    names_by_loc: dict[str, list[str]] = defaultdict(list)
    for r in review_analyses:
        loc = r.get("location", "")
        names_by_loc[loc].extend(r.get("staff_mentioned", []))

    canonical: dict[tuple[str, str], str] = {}  # (loc, raw_name) -> canonical_name

    for loc, names in names_by_loc.items():
        unique = list(set(names))
        name_map: dict[str, str] = {}
        for name in sorted(unique):
            if name in name_map:
                continue
            group = [n for n in unique if Levenshtein.distance(name.lower(), n.lower()) < 2]
            freq = Counter(n for n in names if n in group)
            best = freq.most_common(1)[0][0] if freq else name
            for n in group:
                name_map[n] = best
        for name in unique:
            canonical[(loc, name)] = name_map.get(name, name)

    for r in review_analyses:
        loc = r.get("location", "")
        r["staff_mentioned"] = [
            canonical.get((loc, n), n) for n in r.get("staff_mentioned", [])
        ]


def _rebuild_staff_recognition(review_analyses: list[dict]) -> list[dict]:
    """Rebuild staff_to_recognize from normalized per-review data so the summary
    reflects the same name merging applied to individual reviews."""
    from collections import defaultdict
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for r in review_analyses:
        loc = r.get("location", "")
        for name in r.get("staff_mentioned", []):
            if name:
                counts[(name, loc)] += 1
    result = [
        {"name": name, "location": loc, "mention_count": count}
        for (name, loc), count in counts.items()
    ]
    return sorted(result, key=lambda x: x["mention_count"], reverse=True)


def _call_llm_raw(client: genai.Client, reviews: list[Review]) -> dict:
    """Single LLM call — returns parsed JSON dict with no validation applied."""
    formatted = "\n\n".join(_format_review(i + 1, r) for i, r in enumerate(reviews))
    prompt = _PROMPT_TEMPLATE.replace("{reviews}", formatted)

    logger.info("Sending %d reviews to %s", len(reviews), GEMINI_MODEL)
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            max_output_tokens=65535,
        ),
    )

    raw = response.text
    try:
        return json.loads(_strip_fences(raw))
    except json.JSONDecodeError as e:
        logger.error("JSON parse failed: %s\nRaw response (first 500 chars):\n%s", e, raw[:500])
        raise


def _call_llm_with_429_retry(client: genai.Client, reviews: list[Review]) -> dict:
    """Call _call_llm_raw with exponential backoff on 429 RESOURCE_EXHAUSTED."""
    delays = [60, 120, 180]
    for attempt, delay in enumerate(delays + [None]):
        try:
            return _call_llm_raw(client, reviews)
        except genai_errors.ClientError as e:
            if e.status_code != 429 or delay is None:
                raise
            logger.warning(
                "429 RESOURCE_EXHAUSTED — waiting %ds before retry %d/%d",
                delay, attempt + 1, len(delays),
            )
            time.sleep(delay)


def _analyze_with_retry(client: genai.Client, reviews: list[Review]) -> dict:
    """Call LLM with automatic batch-splitting retry on JSON truncation."""
    try:
        parsed = _call_llm_with_429_retry(client, reviews)
    except json.JSONDecodeError:
        if len(reviews) <= 10:
            raise  # can't split further
        mid = len(reviews) // 2
        logger.warning(
            "JSON truncated on %d reviews — retrying as two halves (%d + %d)",
            len(reviews), mid, len(reviews) - mid,
        )
        a = _analyze_with_retry(client, reviews[:mid])
        b = _analyze_with_retry(client, reviews[mid:])
        all_revs = a.get("reviews", []) + b.get("reviews", [])
        _normalize_staff_names(all_revs)
        summary = b.get("summary", {})
        summary["staff_to_recognize"] = _rebuild_staff_recognition(all_revs)
        logger.info("Merged split batches: %d total reviews", len(all_revs))
        return {"reviews": all_revs, "summary": summary}

    review_analyses: list[dict] = parsed.get("reviews", [])
    _validate_themes(review_analyses)
    _sanity_check_sentiment(review_analyses)
    _normalize_staff_names(review_analyses)
    parsed["summary"]["staff_to_recognize"] = _rebuild_staff_recognition(review_analyses)

    logger.info(
        "Batch complete: %d reviews analyzed, %d urgent callouts",
        len(review_analyses),
        len(parsed.get("summary", {}).get("urgent_callouts", [])),
    )
    return parsed


def analyze_batch(reviews: list[Review], project: str, location: str) -> dict:
    """
    Send a batch of text reviews to Gemini and return the parsed JSON response
    with keys "reviews" and "summary". Automatically splits the batch and retries
    if the response is truncated (JSON parse failure).
    """
    client = genai.Client(
        vertexai=True,
        project=project,
        location=location,
        credentials=vertex_credentials(),
    )
    return _analyze_with_retry(client, reviews)
