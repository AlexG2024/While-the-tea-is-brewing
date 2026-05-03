from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from release_pipeline.config import Settings
from release_pipeline.models import PublishQueue, QueueEntry
from release_pipeline.pipeline import ReleasePipeline
from release_pipeline.queue_state import PublishQueueStore
from release_pipeline.state import PublishedStateStore


class FakeTMDbClient:
    def __init__(self) -> None:
        self.popular_movie_results = []
        self.popular_movie_results_by_page = {}
        self.on_the_air_results = []
        self.on_the_air_results_by_page = {}
        self.popular_movie_pages_called = []
        self.on_the_air_pages_called = []
        self.movie_details = {}
        self.movie_alternative_titles = {}
        self.movie_credits = {}
        self.movie_release_dates = {}
        self.tv_details = {}
        self.tv_credits = {}

    def get_popular_movies(self, page=1):
        self.popular_movie_pages_called.append(page)
        if page in self.popular_movie_results_by_page:
            return self.popular_movie_results_by_page[page]
        return self.popular_movie_results if page == 1 else []

    def get_on_the_air_tv(self, page=1):
        self.on_the_air_pages_called.append(page)
        if page in self.on_the_air_results_by_page:
            return self.on_the_air_results_by_page[page]
        return self.on_the_air_results if page == 1 else []

    def get_movie_details(self, movie_id, language):
        return self.movie_details[(movie_id, language)]

    def get_movie_alternative_titles(self, movie_id, country):
        return self.movie_alternative_titles.get((movie_id, country), [])

    def get_movie_credits(self, movie_id, language):
        return self.movie_credits.get((movie_id, language), {"cast": []})

    def get_movie_release_dates(self, movie_id):
        return self.movie_release_dates.get(movie_id, {"results": []})

    def get_tv_details(self, tv_id, language):
        return self.tv_details[(tv_id, language)]

    def get_tv_credits(self, tv_id, language):
        return self.tv_credits.get((tv_id, language), {"cast": []})

    def image_url(self, poster_path):
        return f"https://image.tmdb.org/t/p/w780{poster_path}"

    def title_url(self, media_type, tmdb_id):
        return f"https://www.themoviedb.org/{media_type}/{tmdb_id}"


class FakeTelegramPublisher:
    def __init__(self) -> None:
        self.sent = []

    def send_photo(self, photo_url: str, caption: str) -> int:
        self.sent.append({"photo_url": photo_url, "caption": caption})
        return len(self.sent)


def make_settings(tmp_path: Path, *, dry_run: bool = False) -> Settings:
    return Settings(
        tmdb_api_token="tmdb",
        telegram_bot_token="telegram",
        telegram_chat_id="@channel",
        timezone_name="Europe/Moscow",
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
        dry_run=dry_run,
        force_business_date=date(2026, 4, 24),
    )


def make_pipeline(
    tmp_path: Path,
    *,
    dry_run: bool = False,
    now_local: datetime | None = None,
):
    settings = make_settings(tmp_path, dry_run=dry_run)
    tmdb = FakeTMDbClient()
    telegram = FakeTelegramPublisher()
    state = PublishedStateStore(
        settings.state_path,
        movie_dedupe_days=settings.movie_dedupe_days,
        tv_dedupe_days=settings.tv_dedupe_days,
    )
    queue_store = PublishQueueStore(settings.queue_path)
    now_fn = (lambda _: now_local) if now_local is not None else None
    pipeline = ReleasePipeline(
        settings,
        tmdb,
        telegram,
        state,
        queue_store=queue_store,
        now_fn=now_fn,
    )
    return pipeline, tmdb, telegram, state, queue_store


def add_movie(
    tmdb: FakeTMDbClient,
    movie_id: int,
    *,
    popularity: float = 50,
    ru_title: str = "Тестовый фильм",
    en_title: str = "Test Movie",
    ru_overview: str = "Русское описание.",
    vote_average: float = 7.4,
    runtime: int = 120,
    release_date: str = "2026-04-24",
    tagline: str = "",
) -> None:
    tmdb.popular_movie_results.append(
        {
            "id": movie_id,
            "title": en_title,
            "release_date": release_date,
            "poster_path": f"/{movie_id}.jpg",
            "popularity": popularity,
            "adult": False,
        }
    )
    tmdb.movie_details[(movie_id, "ru-RU")] = {
        "title": ru_title,
        "original_title": en_title,
        "tagline": tagline,
        "overview": ru_overview,
        "release_date": release_date,
        "poster_path": f"/{movie_id}.jpg",
        "genres": [{"name": "Драма"}],
        "vote_average": vote_average,
        "runtime": runtime,
        "adult": False,
    }
    tmdb.movie_details[(movie_id, "en-US")] = {
        "title": en_title,
        "original_title": en_title,
        "tagline": "English tagline",
        "overview": "English description.",
        "release_date": release_date,
        "poster_path": f"/{movie_id}.jpg",
        "genres": [{"name": "Drama"}],
        "vote_average": vote_average,
        "runtime": runtime,
        "adult": False,
    }
    tmdb.movie_credits[(movie_id, "ru-RU")] = {
        "cast": [
            {"name": "Первый актер", "order": 0},
            {"name": "Вторая актриса", "order": 1},
        ]
    }
    tmdb.movie_release_dates[movie_id] = {
        "results": [
            {
                "iso_3166_1": "US",
                "release_dates": [
                    {"release_date": f"{release_date}T00:00:00.000Z", "type": 3}
                ],
            }
        ]
    }


def add_tv_show(
    tmdb: FakeTMDbClient,
    tv_id: int,
    *,
    popularity: float = 50,
    ru_name: str = "Тестовый сериал",
    en_name: str = "Test Show",
    ru_overview: str = "Русское описание сериала.",
    vote_average: float = 7.4,
    first_air_date: str = "2026-04-01",
    number_of_seasons: int = 2,
    origin_country: list[str] | None = None,
    tagline: str = "",
) -> None:
    if origin_country is None:
        origin_country = ["US"]
    tmdb.on_the_air_results.append(
        {
            "id": tv_id,
            "name": en_name,
            "first_air_date": first_air_date,
            "poster_path": f"/tv-{tv_id}.jpg",
            "popularity": popularity,
        }
    )
    tmdb.tv_details[(tv_id, "ru-RU")] = {
        "name": ru_name,
        "original_name": en_name,
        "tagline": tagline,
        "overview": ru_overview,
        "first_air_date": first_air_date,
        "poster_path": f"/tv-{tv_id}.jpg",
        "genres": [{"name": "Драма"}],
        "vote_average": vote_average,
        "number_of_seasons": number_of_seasons,
        "origin_country": origin_country,
    }
    tmdb.tv_details[(tv_id, "en-US")] = {
        "name": en_name,
        "original_name": en_name,
        "tagline": "English TV tagline",
        "overview": "English TV description.",
        "first_air_date": first_air_date,
        "poster_path": f"/tv-{tv_id}.jpg",
        "genres": [{"name": "Drama"}],
        "vote_average": vote_average,
        "number_of_seasons": number_of_seasons,
        "origin_country": origin_country,
    }
    tmdb.tv_credits[(tv_id, "ru-RU")] = {
        "cast": [
            {"name": "Первый актер сериала", "order": 0},
            {"name": "Вторая актриса сериала", "order": 1},
        ]
    }


def test_no_releases_finishes_cleanly(tmp_path: Path) -> None:
    pipeline, _, telegram, _, _ = make_pipeline(tmp_path)
    summary = pipeline.run()

    assert summary.published_count == 0
    assert summary.failures == 0
    assert telegram.sent == []


def test_empty_state_file_is_treated_as_no_history(tmp_path: Path) -> None:
    state_path = tmp_path / "posted_titles.json"
    state_path.write_text("", encoding="utf-8")

    state = PublishedStateStore(
        state_path,
        movie_dedupe_days=120,
        tv_dedupe_days=60,
    )

    assert state.entries == []


def test_skips_movie_without_russian_overview(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(tmdb, 101, ru_overview="")

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_prefers_ru_alternative_title_when_it_differs_from_api_title(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(
        tmdb,
        111,
        popularity=60,
        ru_title="Проект «Аве Мария»",
        en_title="Project Hail Mary",
        ru_overview="Русское описание.",
        vote_average=8.2,
        runtime=157,
        release_date="2026-03-20",
        tagline="Спасти Землю любой ценой.",
    )
    tmdb.movie_alternative_titles[(111, "RU")] = [
        {"title": "Project Hail Mary"},
        {"title": "Проект «Конец света»"},
    ]
    tmdb.movie_credits[(111, "ru-RU")] = {
        "cast": [
            {"name": "Райан Гослинг", "order": 0},
            {"name": "Сандра Хюллер", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    assert "Project Hail Mary (Проект «Конец света»)" in telegram.sent[0]["caption"]
    assert "<b>Дата выпуска:</b> 20/03/2026" in telegram.sent[0]["caption"]
    assert "<b>В главных ролях:</b> Райан Гослинг, Сандра Хюллер" in telegram.sent[0]["caption"]
    assert "<i>Спасти Землю любой ценой.</i>" in telegram.sent[0]["caption"]


def test_keeps_cyrillic_ru_title_when_alternative_is_latin_transliteration(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(
        tmdb,
        550,
        popularity=28,
        ru_title="Бойцовский клуб",
        en_title="Fight Club",
        release_date="1999-10-15",
        vote_average=8.4,
        runtime=139,
    )
    tmdb.movie_alternative_titles[(550, "RU")] = [{"title": "Boytsovskiy klub"}]
    tmdb.movie_credits[(550, "ru-RU")] = {
        "cast": [
            {"name": "Эдвард Нортон", "order": 0},
            {"name": "Брэд Питт", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    assert "Fight Club (Бойцовский клуб)" in telegram.sent[0]["caption"]
    assert "<b>Дата выпуска:</b> 15/10/1999" in telegram.sent[0]["caption"]


def test_repeat_run_skips_duplicates(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(tmdb, 101)

    first_summary = pipeline.run()
    second_pipeline, second_tmdb, second_telegram, _, _ = make_pipeline(tmp_path)
    second_tmdb.popular_movie_results = tmdb.popular_movie_results
    second_tmdb.movie_details = tmdb.movie_details
    second_tmdb.movie_credits = tmdb.movie_credits
    second_tmdb.movie_release_dates = tmdb.movie_release_dates
    second_summary = second_pipeline.run()

    assert first_summary.published_count == 1
    assert len(telegram.sent) == 1
    assert second_summary.published_count == 0
    assert second_telegram.sent == []


def test_now_playing_caption_uses_cinema_label(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(
        tmdb,
        202,
        popularity=90,
        ru_title="Свежий фильм",
        en_title="Fresh Movie",
        ru_overview="Фильм уже идет в кинотеатрах США.",
        vote_average=8.2,
        runtime=105,
        tagline="Только в кино.",
    )
    tmdb.movie_credits[(202, "ru-RU")] = {
        "cast": [
            {"name": "Главный актер", "order": 0},
            {"name": "Вторая актриса", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    assert "Фильм - Fresh Movie (Свежий фильм)" in telegram.sent[0]["caption"]
    assert "<b>Длительность:</b> 1ч 45м" in telegram.sent[0]["caption"]
    assert "\n\n<i>Только в кино.</i>\n\n" in telegram.sent[0]["caption"]
    assert "<b>Дата выпуска:</b>" in telegram.sent[0]["caption"]


def test_skips_movies_below_min_user_score_percent(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(tmdb, 222, ru_title="Низкий рейтинг", en_title="Low Rated", vote_average=6.4)

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_skips_movies_without_russian_title(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_movie(
        tmdb,
        333,
        ru_title="English Only",
        en_title="English Only",
        ru_overview="Русское описание.",
        vote_average=7.0,
    )

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_missing_poster_skips_title(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    tmdb.popular_movie_results = [
        {
            "id": 404,
            "title": "No Poster",
            "release_date": "2026-04-24",
            "poster_path": None,
            "popularity": 10,
            "adult": False,
        }
    ]

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_daily_caps_limit_movies_and_tv_independently(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    pipeline.settings = Settings(
        tmdb_api_token="tmdb",
        telegram_bot_token="telegram",
        telegram_chat_id="@channel",
        timezone_name="Europe/Moscow",
        max_movie_posts_per_day=2,
        max_tv_posts_per_day=1,
        max_movie_candidate_pages=10,
        max_tv_candidate_pages=10,
        movie_dedupe_days=120,
        tv_dedupe_days=60,
        min_tmdb_user_score_percent=65,
        state_path=tmp_path / "posted_titles.json",
        queue_path=tmp_path / "publish_queue.json",
        publish_slots=("09:15", "12:20", "15:15", "17:20", "19:15", "20:05"),
        dry_run=False,
        force_business_date=date(2026, 4, 24),
    )

    for movie_id, popularity in ((1, 80), (2, 70), (3, 60)):
        add_movie(
            tmdb,
            movie_id,
            popularity=popularity,
            ru_title=f"Фильм {movie_id}",
            en_title=f"Movie {movie_id}",
            vote_average=7.0,
        )
    for tv_id, popularity in ((101, 90), (102, 85)):
        add_tv_show(
            tmdb,
            tv_id,
            popularity=popularity,
            ru_name=f"Сериал {tv_id}",
            en_name=f"Show {tv_id}",
            vote_average=7.0,
        )

    summary = pipeline.run()

    assert summary.published_count == 3
    assert len(telegram.sent) == 3
    assert "Фильм - Movie 1 (Фильм 1)" in telegram.sent[0]["caption"]
    assert "Фильм - Movie 2 (Фильм 2)" in telegram.sent[1]["caption"]
    assert "Сериал - Show 101 (Сериал 101)" in telegram.sent[2]["caption"]


def test_tv_caption_uses_first_air_date_and_seasons(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        808,
        popularity=77,
        ru_name="Разделение",
        en_name="Severance",
        ru_overview="Команда сотрудников проходит необычную процедуру разделения памяти.",
        vote_average=8.2,
        first_air_date="2022-02-18",
        number_of_seasons=2,
        tagline="Русский слоган сериала.",
    )
    tmdb.tv_credits[(808, "ru-RU")] = {
        "cast": [
            {"name": "Адам Скотт", "order": 0},
            {"name": "Бритт Лоуэр", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    caption = telegram.sent[0]["caption"]
    assert "Сериал - Severance (Разделение)" in caption
    assert "<i>Русский слоган сериала.</i>" in caption
    assert "<b>Дата первого выхода:</b> 18/02/2022" in caption
    assert "<b>Количество сезонов:</b> 2" in caption
    assert "<b>В главных ролях:</b> Адам Скотт, Бритт Лоуэр" in caption
    assert "<b>Длительность:</b>" not in caption


def test_transliterates_latin_actor_names_from_ru_credits(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        777,
        ru_name="Сериал 777",
        en_name="Show 777",
        ru_overview="Русское описание сериала.",
        vote_average=8.0,
    )
    tmdb.tv_credits[(777, "ru-RU")] = {
        "cast": [
            {"name": "Aytac Sasmaz", "order": 0},
            {"name": "Helin Kandemir", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    caption = telegram.sent[0]["caption"]
    assert "<b>В главных ролях:</b> Айтак Сасмаз, Хелин Кандемир" in caption


def test_transliterates_extended_latin_actor_names(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        779,
        ru_name="Сериал 779",
        en_name="Show 779",
        ru_overview="Русское описание сериала.",
        vote_average=8.0,
    )
    tmdb.tv_credits[(779, "ru-RU")] = {
        "cast": [
            {"name": "Demet Özdemir", "order": 0},
            {"name": "Çağla Şimşek", "order": 1},
        ]
    }

    summary = pipeline.run()

    assert summary.published_count == 1
    caption = telegram.sent[0]["caption"]
    assert "<b>В главных ролях:</b> Демет Оздемир, Чагла Шимшек" in caption


def test_genres_are_lowercased_and_split_on_conjunction(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        778,
        ru_name="Сериал 778",
        en_name="Show 778",
        ru_overview="Русское описание сериала.",
        vote_average=8.0,
    )
    tmdb.tv_details[(778, "ru-RU")]["genres"] = [
        {"name": "мультфильм"},
        {"name": "Боевик и Приключения"},
        {"name": "НФ и Фэнтези"},
        {"name": "детектив"},
    ]

    summary = pipeline.run()

    assert summary.published_count == 1
    caption = telegram.sent[0]["caption"]
    assert "<b>Жанр:</b> мультфильм, боевик, приключения, нф, фэнтези, детектив" in caption


def test_skips_non_us_tv_shows(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        780,
        ru_name="Турецкий сериал",
        en_name="Turkish Show",
        ru_overview="Русское описание сериала.",
        vote_average=8.0,
        origin_country=["TR"],
    )

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_skips_tv_without_russian_overview_or_title(tmp_path: Path) -> None:
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path)
    add_tv_show(
        tmdb,
        901,
        ru_name="English Only",
        en_name="English Only",
        ru_overview="Русское описание.",
        vote_average=7.0,
    )
    add_tv_show(
        tmdb,
        902,
        ru_name="Сериал без описания",
        en_name="No Overview",
        ru_overview="",
        vote_average=7.0,
    )

    summary = pipeline.run()

    assert summary.published_count == 0
    assert telegram.sent == []


def test_movie_and_tv_use_different_dedupe_windows(tmp_path: Path) -> None:
    state_path = tmp_path / "posted_titles.json"
    state = PublishedStateStore(
        state_path,
        movie_dedupe_days=120,
        tv_dedupe_days=60,
    )
    state.record(
        key="tmdb:movie:1:movie_now_playing:2026-01-01",
        dedupe_key="tmdb:movie:1",
        media_type="movie",
        tmdb_id=1,
        event_type="movie_now_playing",
        event_date_us=date(2026, 1, 1),
        published_at=datetime(2026, 1, 15, tzinfo=timezone.utc),
    )
    state.record(
        key="tmdb:tv:10:tv_on_the_air:2026-01-01",
        dedupe_key="tmdb:tv:10",
        media_type="tv",
        tmdb_id=10,
        event_type="tv_on_the_air",
        event_date_us=date(2026, 1, 1),
        published_at=datetime(2026, 2, 1, tzinfo=timezone.utc),
    )

    business_date = date(2026, 4, 24)

    assert state.was_recently_published("tmdb:movie:1", business_date, "movie") is True
    assert state.was_recently_published("tmdb:tv:10", business_date, "tv") is False


def test_prepare_queue_interleaves_movies_and_tv_by_slots(tmp_path: Path) -> None:
    pipeline, tmdb, _, _, queue_store = make_pipeline(tmp_path)
    add_movie(tmdb, 1, popularity=90, ru_title="Фильм 1", en_title="Movie 1", vote_average=7.0)
    add_movie(tmdb, 2, popularity=80, ru_title="Фильм 2", en_title="Movie 2", vote_average=7.0)
    add_tv_show(tmdb, 11, popularity=95, ru_name="Сериал 1", en_name="Show 1", vote_average=7.0)
    add_tv_show(tmdb, 12, popularity=85, ru_name="Сериал 2", en_name="Show 2", vote_average=7.0)

    summary = pipeline.prepare_queue()
    queue = queue_store.load()

    assert summary.queue_items == 4
    assert queue is not None
    assert [entry.slot_time for entry in queue.items] == ["09:15", "12:20", "15:15", "17:20"]
    assert [entry.item.media_type for entry in queue.items] == ["movie", "tv", "movie", "tv"]
    assert [entry.item.title for entry in queue.items] == ["Фильм 1", "Сериал 1", "Фильм 2", "Сериал 2"]


def test_collect_movie_candidates_reads_all_configured_pages(tmp_path: Path) -> None:
    pipeline, tmdb, _, _, _ = make_pipeline(tmp_path)
    pipeline.settings = Settings(
        tmdb_api_token="tmdb",
        telegram_bot_token="telegram",
        telegram_chat_id="@channel",
        timezone_name="Europe/Moscow",
        max_movie_posts_per_day=3,
        max_tv_posts_per_day=3,
        max_movie_candidate_pages=3,
        max_tv_candidate_pages=10,
        movie_dedupe_days=120,
        tv_dedupe_days=60,
        min_tmdb_user_score_percent=65,
        state_path=tmp_path / "posted_titles.json",
        queue_path=tmp_path / "publish_queue.json",
        publish_slots=("09:15", "12:20", "15:15", "17:20", "19:15", "20:05"),
        dry_run=False,
        force_business_date=date(2026, 4, 24),
    )
    tmdb.popular_movie_results_by_page = {
        1: [
            {
                "id": 1,
                "title": "Movie 1",
                "release_date": "2026-04-24",
                "poster_path": "/1.jpg",
                "popularity": 100,
                "adult": False,
            }
        ],
        2: [
            {
                "id": 2,
                "title": "Movie 2",
                "release_date": "2026-04-23",
                "poster_path": "/2.jpg",
                "popularity": 90,
                "adult": False,
            }
        ],
        3: [
            {
                "id": 3,
                "title": "Movie 3",
                "release_date": "2026-04-22",
                "poster_path": "/3.jpg",
                "popularity": 80,
                "adult": False,
            }
        ],
    }

    candidates = pipeline.collect_movie_candidates(date(2026, 4, 24))

    assert [candidate.tmdb_id for candidate in candidates] == [1, 2, 3]
    assert tmdb.popular_movie_pages_called == [1, 2, 3]


def test_prepare_queue_stops_fetching_extra_pages_after_reaching_daily_caps(tmp_path: Path) -> None:
    pipeline, tmdb, _, _, queue_store = make_pipeline(tmp_path)
    for movie_id, popularity in ((1, 100), (2, 95), (3, 90)):
        add_movie(
            tmdb,
            movie_id,
            popularity=popularity,
            ru_title=f"Фильм {movie_id}",
            en_title=f"Movie {movie_id}",
            vote_average=7.5,
        )
    tmdb.popular_movie_results_by_page[2] = [
        {
            "id": 4,
            "title": "Movie 4",
            "release_date": "2026-04-21",
            "poster_path": "/4.jpg",
            "popularity": 85,
            "adult": False,
        }
    ]
    for tv_id, popularity in ((11, 100), (12, 95), (13, 90)):
        add_tv_show(
            tmdb,
            tv_id,
            popularity=popularity,
            ru_name=f"Сериал {tv_id}",
            en_name=f"Show {tv_id}",
            vote_average=7.5,
        )
    tmdb.on_the_air_results_by_page[2] = [
        {
            "id": 14,
            "name": "Show 14",
            "first_air_date": "2026-04-20",
            "poster_path": "/tv-14.jpg",
            "popularity": 85,
        }
    ]

    summary = pipeline.prepare_queue()
    queue = queue_store.load()

    assert summary.queue_items == 6
    assert queue is not None
    assert len(queue.items) == 6
    assert tmdb.popular_movie_pages_called == [1]
    assert tmdb.on_the_air_pages_called == [1]


def test_publish_next_publishes_only_first_due_item_from_queue(tmp_path: Path) -> None:
    now_local = datetime(2026, 4, 24, 15, 20, tzinfo=ZoneInfo("Europe/Moscow"))
    pipeline, tmdb, telegram, _, queue_store = make_pipeline(tmp_path, now_local=now_local)
    add_movie(tmdb, 1, popularity=90, ru_title="Фильм 1", en_title="Movie 1", vote_average=7.0)
    add_tv_show(tmdb, 11, popularity=95, ru_name="Сериал 1", en_name="Show 1", vote_average=7.0)

    pipeline.prepare_queue()
    summary = pipeline.publish_next()
    queue = queue_store.load()

    assert summary.status == "published"
    assert summary.title == "Фильм 1"
    assert len(telegram.sent) == 1
    assert queue is not None
    assert queue.items[0].published is True
    assert queue.items[1].published is False


def test_publish_next_clears_stale_queue_without_publishing(tmp_path: Path) -> None:
    now_local = datetime(2026, 4, 24, 9, 30, tzinfo=ZoneInfo("Europe/Moscow"))
    pipeline, _, telegram, _, queue_store = make_pipeline(tmp_path, now_local=now_local)
    queue_store.save(
        PublishQueue(
            business_date=date(2026, 4, 23),
            timezone_name="Europe/Moscow",
            items=[],
        )
    )

    summary = pipeline.publish_next()

    assert summary.status == "stale_queue"
    assert summary.stale_queue_cleared is True
    assert telegram.sent == []
    assert queue_store.load() is None


def test_publish_next_waits_until_slot_time(tmp_path: Path) -> None:
    now_local = datetime(2026, 4, 24, 9, 10, tzinfo=ZoneInfo("Europe/Moscow"))
    pipeline, tmdb, telegram, _, _ = make_pipeline(tmp_path, now_local=now_local)
    add_movie(tmdb, 1, popularity=90, ru_title="Фильм 1", en_title="Movie 1", vote_average=7.0)

    pipeline.prepare_queue()
    summary = pipeline.publish_next()

    assert summary.status == "not_due"
    assert summary.slot_time == "09:15"
    assert telegram.sent == []
