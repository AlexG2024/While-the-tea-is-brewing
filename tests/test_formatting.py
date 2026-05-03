from __future__ import annotations

from datetime import date
from pathlib import Path

from release_pipeline.config import Settings
from release_pipeline.models import PublishableItem
from release_pipeline.pipeline import ReleasePipeline
from release_pipeline.queue_state import PublishQueueStore
from release_pipeline.state import PublishedStateStore


class DummyTMDb:
    pass


class DummyTelegram:
    pass


def test_caption_is_trimmed_to_telegram_limit(tmp_path: Path) -> None:
    settings = Settings(
        tmdb_api_token="tmdb",
        telegram_bot_token="telegram",
        telegram_chat_id="@channel",
        timezone_name="America/New_York",
        max_movie_posts_per_day=3,
        max_tv_posts_per_day=3,
        max_movie_candidate_pages=10,
        max_tv_candidate_pages=10,
        movie_dedupe_days=120,
        tv_dedupe_days=60,
        min_tmdb_user_score_percent=65,
        state_path=tmp_path / "posted_titles.json",
        queue_path=tmp_path / "publish_queue.json",
        publish_slots=("09:15", "12:20", "15:15", "17:20", "19:15", "20:05"),
        dry_run=True,
        force_business_date=date(2026, 4, 24),
    )
    pipeline = ReleasePipeline(
        settings,
        DummyTMDb(),
        DummyTelegram(),
        PublishedStateStore(
            settings.state_path,
            movie_dedupe_days=settings.movie_dedupe_days,
            tv_dedupe_days=settings.tv_dedupe_days,
        ),
        queue_store=PublishQueueStore(settings.queue_path),
    )
    item = PublishableItem(
        source="tmdb",
        media_type="movie",
        tmdb_id=1,
        title="Long Movie",
        original_title="Long Movie Original",
        tagline="Tagline here",
        lead_actors=["Lead Actor", "Second Actor"],
        event_type="movie_now_playing",
        event_date_us=date(2026, 4, 24),
        popularity=100,
        overview=("Sentence one. " * 100).strip(),
        genres=["Drama", "Mystery"],
        poster_url="https://image.tmdb.org/t/p/w780/test.jpg",
        tmdb_url="https://www.themoviedb.org/movie/1",
        vote_average=7.1,
        runtime_minutes=157,
    )

    caption = pipeline.format_caption(item)

    assert len(caption) <= 1024
    assert "<b>Фильм - Long Movie Original (Long Movie)</b>" in caption
    assert "<b>Длительность:</b> 2ч 37м" in caption
    assert "\n\n<i>Tagline here</i>\n\n" in caption
    assert "<b>Жанр:</b> drama, mystery" in caption
    assert "<b>В главных ролях:</b> Lead Actor, Second Actor" in caption
