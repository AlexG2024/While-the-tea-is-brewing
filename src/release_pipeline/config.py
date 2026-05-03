from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from zoneinfo import ZoneInfo


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _load_dotenv_file(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key in os.environ:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    tmdb_api_token: str
    telegram_bot_token: str
    telegram_chat_id: str
    timezone_name: str
    max_movie_posts_per_day: int
    max_tv_posts_per_day: int
    max_movie_candidate_pages: int
    max_tv_candidate_pages: int
    movie_dedupe_days: int
    tv_dedupe_days: int
    min_tmdb_user_score_percent: int
    state_path: Path
    queue_path: Path
    publish_slots: tuple[str, ...]
    dry_run: bool
    force_business_date: date | None
    request_timeout: int = 30

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    @classmethod
    def from_env(cls) -> "Settings":
        _load_dotenv_file(_project_root() / ".env")
        timezone_name = os.getenv("TZ", "Europe/Moscow")
        force_business_date_raw = os.getenv("FORCE_BUSINESS_DATE")
        force_business_date = (
            date.fromisoformat(force_business_date_raw)
            if force_business_date_raw
            else None
        )
        shared_dedupe_days = int(os.getenv("DEDUPE_DAYS", "30"))
        state_default = _project_root() / "state" / "posted_titles.json"
        queue_default = _project_root() / "state" / "publish_queue.json"
        state_path = Path(os.getenv("STATE_PATH", str(state_default))).resolve()
        queue_path = Path(os.getenv("QUEUE_PATH", str(queue_default))).resolve()
        publish_slots = tuple(
            slot.strip()
            for slot in os.getenv(
                "PUBLISH_SLOTS",
                "09:15,12:20,15:15,17:20,19:15,20:05",
            ).split(",")
            if slot.strip()
        )
        return cls(
            tmdb_api_token=os.environ["TMDB_API_TOKEN"],
            telegram_bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
            telegram_chat_id=os.environ["TELEGRAM_CHAT_ID"],
            timezone_name=timezone_name,
            max_movie_posts_per_day=int(os.getenv("MAX_MOVIE_POSTS_PER_DAY", "3")),
            max_tv_posts_per_day=int(os.getenv("MAX_TV_POSTS_PER_DAY", "3")),
            max_movie_candidate_pages=int(
                os.getenv("MAX_MOVIE_CANDIDATE_PAGES", "10")
            ),
            max_tv_candidate_pages=int(os.getenv("MAX_TV_CANDIDATE_PAGES", "10")),
            movie_dedupe_days=int(
                os.getenv("MOVIE_DEDUPE_DAYS", str(shared_dedupe_days))
            ),
            tv_dedupe_days=int(os.getenv("TV_DEDUPE_DAYS", str(shared_dedupe_days))),
            min_tmdb_user_score_percent=int(
                os.getenv("MIN_TMDB_USER_SCORE_PERCENT", "65")
            ),
            state_path=state_path,
            queue_path=queue_path,
            publish_slots=publish_slots,
            dry_run=_env_flag("DRY_RUN", False),
            force_business_date=force_business_date,
        )
