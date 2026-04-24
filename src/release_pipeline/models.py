from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal


MediaType = Literal["movie", "tv"]
EventType = Literal["movie_now_playing", "tv_on_the_air"]


@dataclass(frozen=True)
class Candidate:
    source: str
    media_type: MediaType
    tmdb_id: int
    event_type: EventType
    event_date_us: date
    title: str
    popularity: float
    poster_path: str

    @property
    def state_key(self) -> str:
        return ":".join(
            [
                self.source,
                self.media_type,
                str(self.tmdb_id),
                self.event_type,
                self.event_date_us.isoformat(),
            ]
        )

    @property
    def dedupe_key(self) -> str:
        return f"{self.source}:{self.media_type}:{self.tmdb_id}"


@dataclass(frozen=True)
class PublishableItem:
    source: str
    media_type: MediaType
    tmdb_id: int
    title: str
    original_title: str | None
    tagline: str | None
    lead_actors: list[str]
    event_type: EventType
    event_date_us: date
    popularity: float
    overview: str
    genres: list[str]
    poster_url: str
    tmdb_url: str
    vote_average: float | None
    runtime_minutes: int | None
    number_of_seasons: int | None = None

    @property
    def state_key(self) -> str:
        return ":".join(
            [
                self.source,
                self.media_type,
                str(self.tmdb_id),
                self.event_type,
                self.event_date_us.isoformat(),
            ]
        )

    @property
    def dedupe_key(self) -> str:
        return f"{self.source}:{self.media_type}:{self.tmdb_id}"


@dataclass(frozen=True)
class PublishedStateEntry:
    key: str
    dedupe_key: str
    media_type: MediaType
    tmdb_id: int
    event_type: EventType
    event_date_us: date
    published_at: datetime


@dataclass
class QueueEntry:
    slot_time: str
    item: PublishableItem
    published: bool = False
    published_at: datetime | None = None


@dataclass
class PublishQueue:
    business_date: date
    timezone_name: str
    items: list[QueueEntry]
