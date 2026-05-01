# Telegram Release Pipeline

Python-пайплайн для Telegram-канала с дневной очередью публикаций:

- `3 фильма` из TMDb `Popular`
- `3 сериала` из TMDb `On The Air`
- публикация по расписанию в `Europe/Moscow`

## Как это работает

Проект теперь работает в 2 этапа.

1. `prepare-queue`
- собирает фильмы и сериалы на текущий день
- применяет quality gate
- строит очередь в порядке:
  - фильм 1
  - сериал 1
  - фильм 2
  - сериал 2
  - фильм 3
  - сериал 3
- сохраняет очередь в [publish_queue.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/publish_queue.json>)
- утром GitHub Actions повторяет попытку каждые `30 минут`, пока очередь на сегодня не будет подтверждена

2. `publish-next`
- проверяет текущее время по `Europe/Moscow`
- если слот уже наступил, публикует только `1` пост
- помечает элемент очереди как опубликованный
- записывает публикацию в [posted_titles.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/posted_titles.json>)

Дополнительные памятки:
- [OPERATIONS.md](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/OPERATIONS.md>)
- [TELEGRAM_LONG_POSTS_MEMO.md](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/TELEGRAM_LONG_POSTS_MEMO.md>)

## Расписание

- `Prepare Daily Queue`: каждые `30 минут` с `04:00` до `08:30` по Москве
- `09:15` — фильм 1
- `12:20` — сериал 1
- `15:15` — фильм 2
- `17:20` — сериал 2
- `19:15` — фильм 3
- `21:05` — сериал 3

## Источники и quality gate

Фильмы:
- TMDb `movie/popular`

Сериалы:
- TMDb `tv/on_the_air`

Поиск кандидатов:
- до `10` страниц для фильмов
- до `10` страниц для сериалов
- если уже набраны `3 фильма` и `3 сериала`, дальнейший прогон по страницам останавливается

Тайтл публикуется только если:
- есть постер
- есть русский `title`
- есть русский `overview`
- рейтинг TMDb не ниже `MIN_TMDB_USER_SCORE_PERCENT`

## Формат постов

Для фильмов:

```text
Фильм - Original Title (Русское название)

«Русский tagline»

Дата выпуска: 03/20/2026 (US)
Оценка пользователей: 82%
Жанр: фантастика, приключения
Длительность: 2ч 37м
В главных ролях: Актер 1, Актер 2

Описание
...
```

Для сериалов:

```text
Сериал - Original Name (Русское название)

«Русский tagline»

Дата первого выхода: 02/18/2022 (US)
Оценка пользователей: 82%
Жанр: драма, фантастика
Количество сезонов: 2
В главных ролях: Актер 1, Актер 2

Описание
...
```

## Структура

- [src/release_pipeline](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/src/release_pipeline>) — код пайплайна
- [tests](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/tests>) — тесты
- [state/posted_titles.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/posted_titles.json>) — история публикаций
- [state/publish_queue.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/publish_queue.json>) — очередь публикаций на день
- [prepare-daily-queue.yml](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/.github/workflows/prepare-daily-queue.yml>) — ежедневная подготовка очереди
- [publish-from-queue.yml](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/.github/workflows/publish-from-queue.yml>) — публикация по расписанию

## Переменные окружения

- `TMDB_API_TOKEN` — Bearer token TMDb
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `TELEGRAM_CHAT_ID` — `@channel_name` или numeric chat id
- `TZ` — по умолчанию `Europe/Moscow`
- `MAX_MOVIE_POSTS_PER_DAY` — по умолчанию `3`
- `MAX_TV_POSTS_PER_DAY` — по умолчанию `3`
- `MAX_MOVIE_CANDIDATE_PAGES` — по умолчанию `10`
- `MAX_TV_CANDIDATE_PAGES` — по умолчанию `10`
- `MOVIE_DEDUPE_DAYS` — по умолчанию `120`
- `TV_DEDUPE_DAYS` — по умолчанию `60`
- `MIN_TMDB_USER_SCORE_PERCENT` — по умолчанию `65`
- `PUBLISH_SLOTS` — по умолчанию `09:15,12:20,15:15,17:20,19:15,21:05`
- `STATE_PATH` — опционально, путь к state-файлу истории
- `QUEUE_PATH` — опционально, путь к queue-файлу

Опционально:
- `FORCE_BUSINESS_DATE=YYYY-MM-DD` — зафиксировать бизнес-дату
- `DRY_RUN=1` — выполнить команду без реальной отправки в Telegram

Шаблон лежит в [.env.example](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/.env.example>).

## Локальный запуск

```powershell
cd "E:\Codex Open AI\Codex Lessons\telegram-release-pipeline"
py -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
pytest
```

Пример `.env`:

```dotenv
TMDB_API_TOKEN="your_tmdb_api_read_access_token"
TELEGRAM_BOT_TOKEN="your_telegram_bot_token"
TELEGRAM_CHAT_ID="@your_channel"
TZ="Europe/Moscow"
MAX_MOVIE_POSTS_PER_DAY="3"
MAX_TV_POSTS_PER_DAY="3"
MAX_MOVIE_CANDIDATE_PAGES="10"
MAX_TV_CANDIDATE_PAGES="10"
MOVIE_DEDUPE_DAYS="120"
TV_DEDUPE_DAYS="60"
MIN_TMDB_USER_SCORE_PERCENT="65"
PUBLISH_SLOTS="09:15,12:20,15:15,17:20,19:15,21:05"
```

Подготовить очередь:

```powershell
py -m release_pipeline prepare-queue
```

Посмотреть очередь:

```powershell
py -m release_pipeline show-queue
```

Опубликовать следующий слот:

```powershell
py -m release_pipeline publish-next
```

Старый режим немедленного постинга для ручной отладки тоже сохранен:

```powershell
py -m release_pipeline run --dry-run
```

## GitHub Actions

Проект рассчитан на 2 workflow.

`prepare-daily-queue.yml`
- запускается раз в день
- собирает очередь на текущий день
- коммитит `publish_queue.json`

`publish-from-queue.yml`
- запускается каждые `5` минут
- вызывает `publish-next`
- публикует только следующий due post
- коммитит изменения в `publish_queue.json` и `posted_titles.json`

Нужные GitHub Secrets:
- `TMDB_API_TOKEN`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Дедупликация

- ключ события: `source:media_type:tmdb_id:event_type:event_date_us`
- фильмы не повторяются раньше чем через `120` дней
- сериалы не повторяются раньше чем через `60` дней
- `movie` и `tv` дедуплицируются независимо

## Правило свежести очереди

Очередь валидна только для своего дня по `Europe/Moscow`.

Это значит:
- если очередь была создана на вчера, `publish-next` ничего из нее не публикует
- stale queue очищается
- старые посты не догоняются на следующий день

## Ограничения MVP

- источники только TMDb `Popular` и `On The Air`
- без IMDb, стримингов, кастомной графики и LLM-редактуры
- если у TMDb нет русского title или overview, тайтл пропускается
- caption ограничен Telegram-лимитом для `sendPhoto`
