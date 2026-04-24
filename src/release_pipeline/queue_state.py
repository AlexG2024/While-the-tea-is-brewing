from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path

from .models import PublishQueue, PublishableItem, QueueEntry


class PublishQueueStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> PublishQueue | None:
        if not self.path.exists():
            return None
        raw_text = self.path.read_text(encoding="utf-8").strip()
        if not raw_text:
            return None
        data = json.loads(raw_text)
        items = [self._load_queue_entry(row) for row in data.get("items", [])]
        return PublishQueue(
            business_date=date.fromisoformat(data["business_date"]),
            timezone_name=data["timezone_name"],
            items=items,
        )

    def save(self, queue: PublishQueue) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "business_date": queue.business_date.isoformat(),
            "timezone_name": queue.timezone_name,
            "items": [self._dump_queue_entry(item) for item in queue.items],
        }
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def clear(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("", encoding="utf-8")

    def _load_queue_entry(self, payload: dict) -> QueueEntry:
        item_payload = payload["item"]
        return QueueEntry(
            slot_time=payload["slot_time"],
            item=PublishableItem(
                source=item_payload["source"],
                media_type=item_payload["media_type"],
                tmdb_id=int(item_payload["tmdb_id"]),
                title=item_payload["title"],
                original_title=item_payload.get("original_title"),
                tagline=item_payload.get("tagline"),
                lead_actors=list(item_payload.get("lead_actors", [])),
                event_type=item_payload["event_type"],
                event_date_us=date.fromisoformat(item_payload["event_date_us"]),
                popularity=float(item_payload.get("popularity") or 0.0),
                overview=item_payload["overview"],
                genres=list(item_payload.get("genres", [])),
                poster_url=item_payload["poster_url"],
                tmdb_url=item_payload["tmdb_url"],
                vote_average=item_payload.get("vote_average"),
                runtime_minutes=item_payload.get("runtime_minutes"),
                number_of_seasons=item_payload.get("number_of_seasons"),
            ),
            published=bool(payload.get("published", False)),
            published_at=(
                datetime.fromisoformat(payload["published_at"])
                if payload.get("published_at")
                else None
            ),
        )

    def _dump_queue_entry(self, entry: QueueEntry) -> dict:
        item_payload = asdict(entry.item)
        item_payload["event_date_us"] = entry.item.event_date_us.isoformat()
        return {
            "slot_time": entry.slot_time,
            "published": entry.published,
            "published_at": (
                entry.published_at.isoformat() if entry.published_at else None
            ),
            "item": item_payload,
        }
