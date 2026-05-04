from __future__ import annotations

import html
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timezone
from typing import Callable, Iterable

from .config import Settings
from .models import Candidate, PublishQueue, PublishableItem, QueueEntry
from .queue_state import PublishQueueStore
from .state import PublishedStateStore
from .telegram import TelegramPublisher
from .tmdb import TMDbClient


LOGGER = logging.getLogger(__name__)
SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _pick_text(primary: str | None, fallback: str | None) -> str | None:
    for value in (primary, fallback):
        if value and value.strip():
            return value.strip()
    return None


def _pick_genres(primary: dict, fallback: dict) -> list[str]:
    genres = primary.get("genres") or fallback.get("genres") or []
    return [genre["name"] for genre in genres if genre.get("name")]


def _format_genres(genres: list[str]) -> str:
    if not genres:
        return "Не указаны"
    normalized: list[str] = []
    for genre in genres:
        value = re.sub(r"\s+", " ", genre.strip())
        if not value:
            continue
        parts = [
            part.strip().lower()
            for part in re.split(r"\s+и\s+", value, flags=re.IGNORECASE)
            if part.strip()
        ]
        normalized.extend(parts)
    return ", ".join(normalized) if normalized else "Не указаны"


def _trim_sentences(text: str, *, max_sentences: int = 3, max_chars: int = 420) -> str:
    parts = [part.strip() for part in SENTENCE_SPLIT_RE.split(text.strip()) if part.strip()]
    if not parts:
        return text.strip()[:max_chars].rstrip()
    chosen: list[str] = []
    current_length = 0
    for part in parts[:max_sentences]:
        next_length = current_length + len(part) + (1 if chosen else 0)
        if next_length > max_chars:
            break
        chosen.append(part)
        current_length = next_length
    result = " ".join(chosen) if chosen else text.strip()[:max_chars].rstrip()
    if len(result) < len(text.strip()) and not result.endswith("..."):
        result = result.rstrip(". ") + "..."
    return result


def _normalize_title(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().casefold()


def _has_cyrillic(value: str | None) -> bool:
    return bool(value and re.search(r"[А-Яа-яЁё]", value))


def _format_release_date_us(value) -> str:
    return value.strftime("%d/%m/%Y")


def _transliterate_latin_name(value: str) -> str:
    digraphs = [
        ("sch", "щ"),
        ("shch", "щ"),
        ("yo", "ё"),
        ("zh", "ж"),
        ("kh", "х"),
        ("ts", "ц"),
        ("ch", "ч"),
        ("sh", "ш"),
        ("yu", "ю"),
        ("ya", "я"),
        ("ye", "е"),
        ("yi", "и"),
        ("iy", "ий"),
        ("ey", "ей"),
        ("oy", "ой"),
        ("uy", "уй"),
        ("ay", "ай"),
        ("ck", "к"),
        ("ph", "ф"),
        ("th", "т"),
        ("wh", "в"),
        ("qu", "кв"),
    ]
    special_chars = {
        "ö": "о",
        "Ö": "О",
        "ü": "ю",
        "Ü": "Ю",
        "ç": "ч",
        "Ç": "Ч",
        "ş": "ш",
        "Ş": "Ш",
        "ı": "ы",
        "I": "Ы",
        "İ": "И",
        "ğ": "г",
        "Ğ": "Г",
    }
    single_chars = {
        "a": "а",
        "b": "б",
        "c": "к",
        "d": "д",
        "e": "е",
        "f": "ф",
        "g": "г",
        "h": "х",
        "i": "и",
        "j": "дж",
        "k": "к",
        "l": "л",
        "m": "м",
        "n": "н",
        "o": "о",
        "p": "п",
        "q": "к",
        "r": "р",
        "s": "с",
        "t": "т",
        "u": "у",
        "v": "в",
        "w": "в",
        "x": "кс",
        "y": "и",
        "z": "з",
    }
    word_pattern = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-ž][A-Za-zÀ-ÖØ-öø-ÿĀ-ž'`-]*")

    def transliterate_word(word: str) -> str:
        result: list[str] = []
        index = 0
        lowered = word.lower()
        while index < len(word):
            matched = False
            for latin, cyrillic in digraphs:
                if lowered.startswith(latin, index):
                    result.append(cyrillic)
                    index += len(latin)
                    matched = True
                    break
            if matched:
                continue
            char = word[index]
            if char in {"'", "`"}:
                index += 1
                continue
            if char == "-":
                result.append("-")
                index += 1
                continue
            if char in special_chars:
                result.append(special_chars[char])
                index += 1
                continue
            replacement = single_chars.get(lowered[index], char)
            result.append(replacement)
            index += 1
        transliterated = "".join(result)
        if word.isupper():
            return transliterated.upper()
        if word[:1].isupper():
            return transliterated[:1].upper() + transliterated[1:]
        return transliterated

    return word_pattern.sub(lambda match: transliterate_word(match.group(0)), value)


def _normalize_person_name(value: str | None) -> str | None:
    if not value:
        return None
    name = value.strip()
    if not name:
        return None
    if _has_cyrillic(name):
        return name
    return _transliterate_latin_name(name)


def _parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value[:10])


def _format_vote_percent(vote_average: float | None) -> str | None:
    if vote_average is None:
        return None
    return f"{round(vote_average * 10):d}%"


def _format_runtime(runtime_minutes: int | None) -> str | None:
    if not runtime_minutes or runtime_minutes <= 0:
        return None
    hours = runtime_minutes // 60
    minutes = runtime_minutes % 60
    if hours and minutes:
        return f"{hours}ч {minutes}м"
    if hours:
        return f"{hours}ч"
    return f"{minutes}м"


def _format_number_of_seasons(number_of_seasons: int | None) -> str | None:
    if not number_of_seasons or number_of_seasons <= 0:
        return None
    return str(number_of_seasons)


def _pick_us_release_date(
    release_dates_payload: dict,
    details_ru: dict,
    details_en: dict,
) -> date | None:
    preferred_types = [3, 2, 4, 1, 5, 6]
    results = release_dates_payload.get("results") or []
    us_result = next((row for row in results if row.get("iso_3166_1") == "US"), None)
    if us_result:
        release_dates = us_result.get("release_dates") or []
        sorted_release_dates = sorted(
            release_dates,
            key=lambda item: (
                preferred_types.index(item.get("type"))
                if item.get("type") in preferred_types
                else len(preferred_types),
                item.get("release_date") or "",
            ),
        )
        for item in sorted_release_dates:
            raw_value = item.get("release_date") or ""
            if raw_value:
                return date.fromisoformat(raw_value[:10])

    raw_release_date = details_ru.get("release_date") or details_en.get("release_date")
    if raw_release_date:
        return date.fromisoformat(raw_release_date[:10])
    return None


def _pick_lead_actors(credits_ru: dict, credits_en: dict, limit: int = 2) -> list[str]:
    def top_cast_names(credits: dict) -> list[str]:
        cast = credits.get("cast") or []
        if not cast:
            return []
        sorted_cast = sorted(
            cast,
            key=lambda item: (
                item.get("order", 10**6),
                -(item.get("popularity") or 0),
            ),
        )
        names: list[str] = []
        for person in sorted_cast:
            name = _normalize_person_name(person.get("name"))
            if name and name not in names:
                names.append(name)
            if len(names) >= limit:
                break
        return names

    ru_names = top_cast_names(credits_ru)
    if ru_names:
        return ru_names
    return top_cast_names(credits_en)


def _pick_localized_movie_title(
    details_ru: dict,
    details_en: dict,
    alternative_titles_ru: list[dict],
) -> str:
    original_title = details_en.get("title") or details_ru.get("original_title") or details_ru.get("title")
    ru_title = details_ru.get("title")
    original_norm = _normalize_title(original_title)
    ru_norm = _normalize_title(ru_title)

    candidates: list[str] = []
    for row in alternative_titles_ru:
        title = (row.get("title") or "").strip()
        if title and _has_cyrillic(title):
            candidates.append(title)
    if ru_title:
        candidates.append(ru_title)
    if original_title:
        candidates.append(original_title)

    for candidate in candidates:
        candidate_norm = _normalize_title(candidate)
        if not candidate_norm:
            continue
        if candidate_norm != original_norm and candidate_norm != ru_norm:
            return candidate

    return ru_title or original_title or ""


def _pick_localized_tv_title(details_ru: dict, details_en: dict) -> str:
    return (
        details_ru.get("name")
        or details_ru.get("original_name")
        or details_en.get("name")
        or details_en.get("original_name")
        or ""
    )


@dataclass(frozen=True)
class RunSummary:
    business_date: str
    candidates_seen: int
    eligible_items: int
    would_publish_count: int
    published_count: int
    failures: int


@dataclass(frozen=True)
class PrepareQueueSummary:
    business_date: str
    queue_items: int
    movie_items: int
    tv_items: int
    candidates_seen: int
    reused_existing: bool
    saved_queue: bool


@dataclass(frozen=True)
class PublishNextSummary:
    business_date: str
    status: str
    published: bool
    title: str | None = None
    media_type: str | None = None
    slot_time: str | None = None
    stale_queue_cleared: bool = False
    remaining_items: int = 0


class ReleasePipeline:
    def __init__(
        self,
        settings: Settings,
        tmdb: TMDbClient,
        telegram: TelegramPublisher,
        state_store: PublishedStateStore,
        queue_store: PublishQueueStore | None = None,
        now_fn: Callable[[object], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.tmdb = tmdb
        self.telegram = telegram
        self.state_store = state_store
        self.queue_store = queue_store
        self.now_fn = now_fn or (lambda tz: datetime.now(tz))

    def business_date(self):
        if self.settings.force_business_date:
            return self.settings.force_business_date
        return self.now_fn(self.settings.timezone).date()

    def current_time(self) -> datetime:
        return self.now_fn(self.settings.timezone)

    def _movie_candidate_from_row(self, row: dict) -> Candidate | None:
        release_date = _parse_iso_date(row.get("release_date"))
        if not row.get("title") or release_date is None or not row.get("poster_path"):
            return None
        if row.get("adult"):
            return None
        return Candidate(
            source="tmdb",
            media_type="movie",
            tmdb_id=int(row["id"]),
            event_type="movie_now_playing",
            event_date_us=release_date,
            title=row["title"],
            popularity=float(row.get("popularity") or 0.0),
            poster_path=row["poster_path"],
        )

    def _tv_candidate_from_row(self, row: dict) -> Candidate | None:
        first_air_date = _parse_iso_date(row.get("first_air_date"))
        name = row.get("name") or row.get("original_name")
        if not name or first_air_date is None or not row.get("poster_path"):
            return None
        return Candidate(
            source="tmdb",
            media_type="tv",
            tmdb_id=int(row["id"]),
            event_type="tv_on_the_air",
            event_date_us=first_air_date,
            title=name,
            popularity=float(row.get("popularity") or 0.0),
            poster_path=row["poster_path"],
        )

    def _collect_movie_candidates_for_page(self, page: int) -> list[Candidate]:
        candidates: list[Candidate] = []
        for row in self.tmdb.get_popular_movies(page=page):
            candidate = self._movie_candidate_from_row(row)
            if candidate is not None:
                candidates.append(candidate)
        return self._dedupe_candidates(candidates)

    def _collect_tv_candidates_for_page(self, page: int) -> list[Candidate]:
        candidates: list[Candidate] = []
        for row in self.tmdb.get_on_the_air_tv(page=page):
            candidate = self._tv_candidate_from_row(row)
            if candidate is not None:
                candidates.append(candidate)
        return self._dedupe_candidates(candidates)

    def collect_movie_candidates(self, business_date) -> list[Candidate]:
        candidates: list[Candidate] = []
        for page in range(1, self.settings.max_movie_candidate_pages + 1):
            page_candidates = self._collect_movie_candidates_for_page(page)
            if not page_candidates:
                break
            candidates.extend(page_candidates)
        return self._dedupe_candidates(candidates)

    def collect_tv_candidates(self, business_date) -> list[Candidate]:
        candidates: list[Candidate] = []
        for page in range(1, self.settings.max_tv_candidate_pages + 1):
            page_candidates = self._collect_tv_candidates_for_page(page)
            if not page_candidates:
                break
            candidates.extend(page_candidates)
        return self._dedupe_candidates(candidates)

    def _dedupe_candidates(self, candidates: Iterable[Candidate]) -> list[Candidate]:
        by_key: dict[str, Candidate] = {}
        for candidate in candidates:
            current = by_key.get(candidate.state_key)
            if current is None or candidate.popularity > current.popularity:
                by_key[candidate.state_key] = candidate
        return sorted(by_key.values(), key=lambda item: item.popularity, reverse=True)

    def enrich_candidate(self, candidate: Candidate) -> PublishableItem | None:
        if candidate.media_type == "movie":
            return self._enrich_movie(candidate)
        if candidate.media_type == "tv":
            return self._enrich_tv(candidate)
        return None

    def _enrich_movie(self, candidate: Candidate) -> PublishableItem | None:
        details_ru = self.tmdb.get_movie_details(candidate.tmdb_id, "ru-RU")
        details_en = self.tmdb.get_movie_details(candidate.tmdb_id, "en-US")
        alternative_titles_ru = self.tmdb.get_movie_alternative_titles(candidate.tmdb_id, "RU")
        credits_ru = self.tmdb.get_movie_credits(candidate.tmdb_id, "ru-RU")
        credits_en = self.tmdb.get_movie_credits(candidate.tmdb_id, "en-US")
        release_dates_payload = self.tmdb.get_movie_release_dates(candidate.tmdb_id)
        if details_ru.get("adult") or details_en.get("adult"):
            return None
        overview = details_ru.get("overview")
        if not overview or not overview.strip():
            return None
        title = _pick_localized_movie_title(details_ru, details_en, alternative_titles_ru) or candidate.title
        poster_path = details_ru.get("poster_path") or details_en.get("poster_path") or candidate.poster_path
        if not poster_path:
            return None
        vote_average = details_ru.get("vote_average") or details_en.get("vote_average")
        if vote_average is None or (vote_average * 10) < self.settings.min_tmdb_user_score_percent:
            return None
        if not _has_cyrillic(title):
            return None
        release_date_us = _pick_us_release_date(release_dates_payload, details_ru, details_en)
        if release_date_us is None:
            return None
        return PublishableItem(
            source="tmdb",
            media_type="movie",
            tmdb_id=candidate.tmdb_id,
            title=title,
            original_title=details_en.get("title") or details_ru.get("original_title"),
            tagline=_pick_text(details_ru.get("tagline"), None),
            lead_actors=_pick_lead_actors(credits_ru, credits_en),
            event_type="movie_now_playing",
            event_date_us=release_date_us,
            popularity=candidate.popularity,
            overview=overview,
            genres=_pick_genres(details_ru, details_en),
            poster_url=self.tmdb.image_url(poster_path),
            tmdb_url=self.tmdb.title_url("movie", candidate.tmdb_id),
            vote_average=vote_average,
            runtime_minutes=details_ru.get("runtime") or details_en.get("runtime"),
        )

    def _enrich_tv(self, candidate: Candidate) -> PublishableItem | None:
        details_ru = self.tmdb.get_tv_details(candidate.tmdb_id, "ru-RU")
        details_en = self.tmdb.get_tv_details(candidate.tmdb_id, "en-US")
        credits_ru = self.tmdb.get_tv_credits(candidate.tmdb_id, "ru-RU")
        credits_en = self.tmdb.get_tv_credits(candidate.tmdb_id, "en-US")
        origin_countries = details_en.get("origin_country") or details_ru.get("origin_country") or []
        if "US" not in origin_countries:
            return None
        overview = details_ru.get("overview")
        if not overview or not overview.strip():
            return None
        title = _pick_localized_tv_title(details_ru, details_en) or candidate.title
        if not _has_cyrillic(title):
            return None
        poster_path = details_ru.get("poster_path") or details_en.get("poster_path") or candidate.poster_path
        if not poster_path:
            return None
        vote_average = details_ru.get("vote_average") or details_en.get("vote_average")
        if vote_average is None or (vote_average * 10) < self.settings.min_tmdb_user_score_percent:
            return None
        first_air_date = _parse_iso_date(details_ru.get("first_air_date")) or _parse_iso_date(
            details_en.get("first_air_date")
        )
        if first_air_date is None:
            return None
        return PublishableItem(
            source="tmdb",
            media_type="tv",
            tmdb_id=candidate.tmdb_id,
            title=title,
            original_title=details_en.get("name") or details_ru.get("original_name"),
            tagline=_pick_text(details_ru.get("tagline"), None),
            lead_actors=_pick_lead_actors(credits_ru, credits_en),
            event_type="tv_on_the_air",
            event_date_us=first_air_date,
            popularity=candidate.popularity,
            overview=overview,
            genres=_pick_genres(details_ru, details_en),
            poster_url=self.tmdb.image_url(poster_path),
            tmdb_url=self.tmdb.title_url("tv", candidate.tmdb_id),
            vote_average=vote_average,
            runtime_minutes=None,
            number_of_seasons=details_ru.get("number_of_seasons")
            or details_en.get("number_of_seasons"),
        )

    def format_caption(self, item: PublishableItem) -> str:
        type_label = {
            "movie_now_playing": "Фильм",
            "tv_on_the_air": "Сериал",
        }[item.event_type]
        title_line = f"{type_label} - {item.original_title or item.title}"
        if item.original_title and item.title and item.title != item.original_title:
            title_line += f" ({item.title})"

        def build_caption(overview_text: str) -> str:
            lines = [f"<b>{html.escape(title_line)}</b>", ""]
            if item.tagline:
                lines.extend([f"<i>{html.escape(item.tagline)}</i>", ""])
            date_label = (
                "Дата выпуска"
                if item.event_type == "movie_now_playing"
                else "Дата первого выхода"
            )
            lines.append(f"<b>{date_label}:</b> {_format_release_date_us(item.event_date_us)}")
            vote_percent = _format_vote_percent(item.vote_average)
            if vote_percent:
                lines.append(f"<b>Оценка пользователей:</b> {vote_percent}")
            lines.append(f"<b>Жанр:</b> {html.escape(_format_genres(item.genres))}")
            if item.event_type == "movie_now_playing":
                runtime_label = _format_runtime(item.runtime_minutes)
                if runtime_label:
                    lines.append(f"<b>Длительность:</b> {runtime_label}")
            else:
                seasons_label = _format_number_of_seasons(item.number_of_seasons)
                if seasons_label:
                    lines.append(f"<b>Количество сезонов:</b> {seasons_label}")
            if item.lead_actors:
                lines.append(
                    f"<b>В главных ролях:</b> {html.escape(', '.join(item.lead_actors))}"
                )
            lines.extend(["", "<b>Описание</b>", html.escape(overview_text)])
            return "\n".join(lines)

        full_caption = build_caption(item.overview.strip())
        if len(full_caption) <= 1024:
            return full_caption

        trimmed_overview = _trim_sentences(item.overview)
        caption = build_caption(trimmed_overview)
        if len(caption) <= 1024:
            return caption

        overview_budget = max(180, 1024 - (len(caption) - len(trimmed_overview)) - 3)
        trimmed_overview = _trim_sentences(item.overview, max_chars=overview_budget)
        caption = build_caption(trimmed_overview)
        return caption[:1021].rstrip() + "..."

    def publish(self, item: PublishableItem) -> int | None:
        caption = self.format_caption(item)
        if self.settings.dry_run:
            LOGGER.info("DRY_RUN publish skipped for %s", item.title)
            return None
        return self.telegram.send_photo(item.poster_url, caption)

    def _require_queue_store(self) -> PublishQueueStore:
        if self.queue_store is None:
            raise RuntimeError("PublishQueueStore is required for queue operations")
        return self.queue_store

    def _collect_publishable_sets(
        self,
        business_date: date,
    ) -> tuple[list[Candidate], list[Candidate], list[PublishableItem], list[PublishableItem]]:
        raw_movie_candidates, publishable_movies = self._collect_publishable_candidates(
            self._collect_movie_candidates_for_page,
            business_date,
            self.settings.max_movie_posts_per_day,
            self.settings.max_movie_candidate_pages,
        )
        raw_tv_candidates, publishable_tv = self._collect_publishable_candidates(
            self._collect_tv_candidates_for_page,
            business_date,
            self.settings.max_tv_posts_per_day,
            self.settings.max_tv_candidate_pages,
        )
        return raw_movie_candidates, raw_tv_candidates, publishable_movies, publishable_tv

    def _interleave_items(
        self,
        movies: list[PublishableItem],
        tv_items: list[PublishableItem],
    ) -> list[PublishableItem]:
        items: list[PublishableItem] = []
        max_length = max(len(movies), len(tv_items))
        for index in range(max_length):
            if index < len(movies):
                items.append(movies[index])
            if index < len(tv_items):
                items.append(tv_items[index])
        return items

    def _slot_datetime(self, business_date: date, slot_time: str) -> datetime:
        hours, minutes = (int(part) for part in slot_time.split(":", 1))
        return datetime.combine(
            business_date,
            dt_time(hour=hours, minute=minutes),
            tzinfo=self.settings.timezone,
        )

    def prepare_queue(self, *, force: bool = False) -> PrepareQueueSummary:
        business_date = self.business_date()
        queue_store = self._require_queue_store()
        existing_queue = queue_store.load()
        if (
            existing_queue is not None
            and existing_queue.business_date == business_date
            and not force
        ):
            movie_items = sum(1 for entry in existing_queue.items if entry.item.media_type == "movie")
            tv_items = sum(1 for entry in existing_queue.items if entry.item.media_type == "tv")
            return PrepareQueueSummary(
                business_date=business_date.isoformat(),
                queue_items=len(existing_queue.items),
                movie_items=movie_items,
                tv_items=tv_items,
                candidates_seen=0,
                reused_existing=True,
                saved_queue=False,
            )

        raw_movie_candidates, raw_tv_candidates, publishable_movies, publishable_tv = (
            self._collect_publishable_sets(business_date)
        )
        ordered_items = self._interleave_items(publishable_movies, publishable_tv)
        queue_items = [
            QueueEntry(slot_time=slot_time, item=item)
            for slot_time, item in zip(self.settings.publish_slots, ordered_items)
        ]
        queue = PublishQueue(
            business_date=business_date,
            timezone_name=self.settings.timezone_name,
            items=queue_items,
        )
        if not self.settings.dry_run:
            queue_store.save(queue)
        return PrepareQueueSummary(
            business_date=business_date.isoformat(),
            queue_items=len(queue_items),
            movie_items=len(publishable_movies),
            tv_items=len(publishable_tv),
            candidates_seen=len(raw_movie_candidates) + len(raw_tv_candidates),
            reused_existing=False,
            saved_queue=not self.settings.dry_run,
        )

    def publish_next(self) -> PublishNextSummary:
        business_date = self.business_date()
        queue_store = self._require_queue_store()
        queue = queue_store.load()
        if queue is None:
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="no_queue",
                published=False,
            )
        if queue.business_date < business_date:
            if not self.settings.dry_run:
                queue_store.clear()
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="stale_queue",
                published=False,
                stale_queue_cleared=not self.settings.dry_run,
            )
        if queue.business_date > business_date:
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="future_queue",
                published=False,
            )

        next_entry = next((entry for entry in queue.items if not entry.published), None)
        if next_entry is None:
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="queue_complete",
                published=False,
                remaining_items=0,
            )

        now_local = self.current_time()
        scheduled_at = self._slot_datetime(queue.business_date, next_entry.slot_time)
        remaining_items = sum(1 for entry in queue.items if not entry.published)
        if scheduled_at > now_local:
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="not_due",
                published=False,
                title=next_entry.item.title,
                media_type=next_entry.item.media_type,
                slot_time=next_entry.slot_time,
                remaining_items=remaining_items,
            )

        self.publish(next_entry.item)
        if self.settings.dry_run:
            return PublishNextSummary(
                business_date=business_date.isoformat(),
                status="would_publish",
                published=False,
                title=next_entry.item.title,
                media_type=next_entry.item.media_type,
                slot_time=next_entry.slot_time,
                remaining_items=remaining_items,
            )

        next_entry.published = True
        next_entry.published_at = now_local.astimezone(timezone.utc)
        self.state_store.record(
            key=next_entry.item.state_key,
            dedupe_key=next_entry.item.dedupe_key,
            media_type=next_entry.item.media_type,
            tmdb_id=next_entry.item.tmdb_id,
            event_type=next_entry.item.event_type,
            event_date_us=next_entry.item.event_date_us,
        )
        self.state_store.save()
        queue_store.save(queue)
        remaining_items_after = sum(1 for entry in queue.items if not entry.published)
        return PublishNextSummary(
            business_date=business_date.isoformat(),
            status="published",
            published=True,
            title=next_entry.item.title,
            media_type=next_entry.item.media_type,
            slot_time=next_entry.slot_time,
            remaining_items=remaining_items_after,
        )

    def show_queue(self) -> dict:
        business_date = self.business_date()
        queue = self._require_queue_store().load()
        if queue is None:
            return {
                "business_date": business_date.isoformat(),
                "status": "no_queue",
                "items": [],
            }
        return {
            "business_date": business_date.isoformat(),
            "queue_business_date": queue.business_date.isoformat(),
            "status": (
                "stale"
                if queue.business_date < business_date
                else "current" if queue.business_date == business_date else "future"
            ),
            "items": [
                {
                    "slot_time": entry.slot_time,
                    "title": entry.item.title,
                    "media_type": entry.item.media_type,
                    "published": entry.published,
                    "published_at": (
                        entry.published_at.isoformat() if entry.published_at else None
                    ),
                }
                for entry in queue.items
            ],
        }

    def _select_publishable_items(
        self,
        candidates: list[Candidate],
        business_date: date,
        limit: int,
    ) -> list[PublishableItem]:
        publishable_items: list[PublishableItem] = []
        seen_run_keys: set[str] = set()
        for candidate in candidates:
            if len(publishable_items) >= limit:
                break
            item = self._select_publishable_candidate(candidate, business_date, seen_run_keys)
            if item is not None:
                publishable_items.append(item)
        publishable_items.sort(key=lambda item: item.popularity, reverse=True)
        return publishable_items[:limit]

    def _select_publishable_candidate(
        self,
        candidate: Candidate,
        business_date: date,
        seen_run_keys: set[str],
    ) -> PublishableItem | None:
        if candidate.state_key in seen_run_keys:
            return None
        if self.state_store.has_event(candidate.state_key):
            return None
        if self.state_store.was_recently_published(
            candidate.dedupe_key,
            business_date,
            candidate.media_type,
        ):
            return None
        item = self.enrich_candidate(candidate)
        if item is None:
            return None
        if item.state_key in seen_run_keys:
            return None
        seen_run_keys.add(item.state_key)
        return item

    def run(self) -> RunSummary:
        business_date = self.business_date()
        LOGGER.info("Collecting candidates for %s", business_date.isoformat())
        raw_movie_candidates, raw_tv_candidates, publishable_movies, publishable_tv = (
            self._collect_publishable_sets(business_date)
        )
        publishable_items = publishable_movies + publishable_tv

        if self.settings.dry_run:
            for index, item in enumerate(publishable_items, start=1):
                LOGGER.info("Preview %s: %s", index, item.title)
                print(
                    f"\n--- PREVIEW {index} ---\n"
                    f"POSTER: {item.poster_url}\n\n"
                    f"{self.format_caption(item)}\n"
                )

        published_count = 0
        failures = 0
        for item in publishable_items:
            try:
                self.publish(item)
                if self.settings.dry_run:
                    LOGGER.info("Would publish %s", item.title)
                else:
                    self.state_store.record(
                        key=item.state_key,
                        dedupe_key=item.dedupe_key,
                        media_type=item.media_type,
                        tmdb_id=item.tmdb_id,
                        event_type=item.event_type,
                        event_date_us=item.event_date_us,
                    )
                    published_count += 1
                    LOGGER.info("Published %s", item.title)
            except Exception:
                failures += 1
                LOGGER.exception("Failed to publish %s", item.title)

        if not self.settings.dry_run:
            self.state_store.save()
        return RunSummary(
            business_date=business_date.isoformat(),
            candidates_seen=len(raw_movie_candidates) + len(raw_tv_candidates),
            eligible_items=len(publishable_items),
            would_publish_count=len(publishable_items),
            published_count=published_count,
            failures=failures,
        )
    def _collect_publishable_candidates(
        self,
        fetch_page: Callable[[int], list[Candidate]],
        business_date: date,
        limit: int,
        max_pages: int,
    ) -> tuple[list[Candidate], list[PublishableItem]]:
        raw_candidates: list[Candidate] = []
        publishable_items: list[PublishableItem] = []
        seen_raw_keys: set[str] = set()
        seen_run_keys: set[str] = set()

        for page in range(1, max_pages + 1):
            page_candidates = fetch_page(page)
            if not page_candidates:
                break
            for candidate in page_candidates:
                if candidate.state_key in seen_raw_keys:
                    continue
                seen_raw_keys.add(candidate.state_key)
                raw_candidates.append(candidate)
                if len(publishable_items) >= limit:
                    break
                item = self._select_publishable_candidate(
                    candidate,
                    business_date,
                    seen_run_keys,
                )
                if item is not None:
                    publishable_items.append(item)
            if len(publishable_items) >= limit:
                break

        raw_candidates.sort(key=lambda item: item.popularity, reverse=True)
        publishable_items.sort(key=lambda item: item.popularity, reverse=True)
        return raw_candidates, publishable_items
