from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from .models import PublishedStateEntry


class PublishedStateStore:
    def __init__(
        self,
        path: Path,
        movie_dedupe_days: int,
        tv_dedupe_days: int,
        retention_days: int = 120,
    ) -> None:
        self.path = path
        self.movie_dedupe_days = movie_dedupe_days
        self.tv_dedupe_days = tv_dedupe_days
        self.retention_days = retention_days
        self.entries = self.load()

    def load(self) -> list[PublishedStateEntry]:
        if not self.path.exists():
            return []
        raw_text = self.path.read_text(encoding="utf-8").strip()
        if not raw_text:
            return []
        data = json.loads(raw_text)
        entries: list[PublishedStateEntry] = []
        for row in data.get("entries", []):
            entries.append(
                PublishedStateEntry(
                    key=row["key"],
                    dedupe_key=row["dedupe_key"],
                    media_type=row["media_type"],
                    tmdb_id=int(row["tmdb_id"]),
                    event_type=row["event_type"],
                    event_date_us=date.fromisoformat(row["event_date_us"]),
                    published_at=datetime.fromisoformat(row["published_at"]),
                )
            )
        return entries

    def has_event(self, key: str) -> bool:
        return any(entry.key == key for entry in self.entries)

    def was_recently_published(
        self,
        dedupe_key: str,
        business_date: date,
        media_type: str,
    ) -> bool:
        dedupe_days = (
            self.movie_dedupe_days if media_type == "movie" else self.tv_dedupe_days
        )
        threshold = business_date - timedelta(days=dedupe_days)
        return any(
            entry.dedupe_key == dedupe_key and entry.published_at.date() >= threshold
            for entry in self.entries
        )

    def record(
        self,
        *,
        key: str,
        dedupe_key: str,
        media_type: str,
        tmdb_id: int,
        event_type: str,
        event_date_us: date,
        published_at: datetime | None = None,
    ) -> None:
        self.entries.append(
            PublishedStateEntry(
                key=key,
                dedupe_key=dedupe_key,
                media_type=media_type,
                tmdb_id=tmdb_id,
                event_type=event_type,
                event_date_us=event_date_us,
                published_at=published_at or datetime.now(timezone.utc),
            )
        )

    def prune(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.retention_days)
        self.entries = [entry for entry in self.entries if entry.published_at >= cutoff]

    def save(self) -> None:
        self.prune()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "entries": [
                {
                    **asdict(entry),
                    "event_date_us": entry.event_date_us.isoformat(),
                    "published_at": entry.published_at.isoformat(),
                }
                for entry in sorted(
                    self.entries,
                    key=lambda item: (item.event_date_us, item.published_at),
                )
            ],
        }
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
