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
