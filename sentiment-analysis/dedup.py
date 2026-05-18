import unicodedata
from models import Review


def _normalize(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()


def make_review_id(r: Review) -> str:
    """Stable composite identifier: normalized_author|publish_date.
    More unique than dedupe_key, which turns out to be the Google Place ID."""
    author = _normalize(r.author).replace(" ", "_")[:20]
    date = r.publish_time[:10] if r.publish_time else "nodate"
    return f"{author}|{date}"


def dedup_reviews(reviews: list[Review]) -> tuple[list[Review], int]:
    """
    Returns (deduped_reviews, duplicate_count).

    Two separate dedup strategies:
    - Empty-text reviews: key on author+location+date because all empty reviews
      share the same SHA-1 dedupe_key (hash of empty string).
    - Text reviews: key on normalized author+location+date+first-50-chars to catch
      the same review posted twice with minor variations (e.g. accent differences).
    """
    seen: set[tuple] = set()
    result: list[Review] = []
    dup_count = 0

    for r in reviews:
        if not r.text.strip():
            key = ("empty", _normalize(r.author), r.place.lower(), r.publish_time[:10])
        else:
            key = (
                "text",
                _normalize(r.author),
                r.place.lower(),
                r.publish_time[:10],
                r.text.strip()[:50].lower(),
            )

        if key in seen:
            dup_count += 1
            continue
        seen.add(key)
        result.append(r)

    return result, dup_count
