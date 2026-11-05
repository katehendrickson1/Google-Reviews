from dataclasses import dataclass


@dataclass
class Review:
    dedupe_key: str
    place: str
    place_id: str
    author: str
    star_rating: int
    publish_time: str
    relative_time: str
    text: str
    date_run: str
    # reviews.review_id when the review was read from the Shiny Shell
    # database (daily mode); sent to the LLM as the review's id so results
    # map back to the row exactly. None for reviews read from the Sheet.
    db_review_id: int | None = None
