# Operations Runbook

Короткая памятка по ежедневной работе проекта [telegram-release-pipeline](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline>).

## Что делает система

Каждый день пайплайн:

1. собирает очередь из `3 фильмов + 3 сериалов`
2. сохраняет ее в [state/publish_queue.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/publish_queue.json>)
3. публикует посты по одному по расписанию
4. записывает историю публикаций в [state/posted_titles.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/posted_titles.json>)

Таймзона:
- `Europe/Moscow`

Подготовка очереди:
- `Prepare Daily Queue` пытается создать очередь каждые `30 минут` с `04:00` до `08:30`
- если очередь на сегодня уже есть, workflow завершается без изменений

Слоты публикации:
- `09:15` — фильм 1
- `12:20` — сериал 1
- `15:15` — фильм 2
- `17:20` — сериал 2
- `19:15` — фильм 3
- `20:05` — сериал 3

## Где что находится

Локальный проект:
- [README.md](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/README.md>)
- [OPERATIONS.md](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/OPERATIONS.md>)
- [TELEGRAM_LONG_POSTS_MEMO.md](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/TELEGRAM_LONG_POSTS_MEMO.md>)
- [state/posted_titles.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/posted_titles.json>)
- [state/publish_queue.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/publish_queue.json>)
- [.env.example](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/.env.example>)

GitHub:
- репозиторий: [While-the-tea-is-brewing](https://github.com/AlexG2024/While-the-tea-is-brewing)
- workflow подготовки очереди: [Prepare Daily Queue](https://github.com/AlexG2024/While-the-tea-is-brewing/actions/workflows/prepare-daily-queue.yml)
- workflow публикации: [Publish From Queue](https://github.com/AlexG2024/While-the-tea-is-brewing/actions/workflows/publish-from-queue.yml)

## Ежедневная нормальная работа

Обычно вручную ничего делать не нужно.

Каждый день:
- `Prepare Daily Queue` утром повторяет попытки создания очереди
- `Publish From Queue` запускается каждые `5 минут`
- когда наступает слот, публикуется только `1` пост

## Что проверять, если хочешь убедиться, что все живо

1. Открыть `Actions` в GitHub
2. Проверить, что `Prepare Daily Queue` зеленый
3. Проверить, что `Publish From Queue` периодически отрабатывает без падений
4. Открыть [state/publish_queue.json](https://github.com/AlexG2024/While-the-tea-is-brewing/blob/main/state/publish_queue.json)
5. Убедиться, что у уже прошедших слотов стоит:
   - `"published": true`

## Что делать, если очередь не создалась

Симптом:
- `Prepare Daily Queue` упал

Проверить:
1. `Actions` → открыть упавший run
2. посмотреть шаг, который упал:
   - `Run tests`
   - `Prepare queue`
   - `Verify queue for today`
   - `Commit updated queue`

Частые причины:
- проблема с `TMDB_API_TOKEN`
- временная проблема TMDb API
- сетевой сбой GitHub Actions
- ошибка в коде после новых изменений

Что делать:
1. исправить причину
2. вручную запустить `Prepare Daily Queue` через `Run workflow`

## Что делать, если пост не вышел в свое время

Симптом:
- в Telegram не пришел ожидаемый пост

Проверить:
1. workflow [Publish From Queue](https://github.com/AlexG2024/While-the-tea-is-brewing/actions/workflows/publish-from-queue.yml)
2. был ли run рядом с нужным временем
3. что написано в логах:
   - `not_due`
   - `published`
   - `stale_queue`
   - `no_queue`

Расшифровка:
- `not_due` — слот еще не наступил
- `published` — пост должен был уйти
- `stale_queue` — очередь была уже не на сегодняшний день и была очищена
- `no_queue` — очередь на день не была создана

Что делать:
1. если `no_queue`, вручную запустить `Prepare Daily Queue`
2. если после этого слот уже наступил, вручную запустить `Publish From Queue`

## Что делать, если GitHub лежал день или больше

Текущее поведение правильное и ожидаемое:
- старая очередь не догоняется
- вчерашние посты не публикуются на следующий день
- stale queue очищается

Что делать:
1. дождаться или вручную запустить новый `Prepare Daily Queue`
2. дальше система продолжит с текущего дня

## Что делать, если хочешь начать с чистого листа

Если нужно полностью сбросить историю:

1. удалить посты из Telegram вручную
2. очистить локально или в репозитории:
   - [state/posted_titles.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/posted_titles.json>)
   - [state/publish_queue.json](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/state/publish_queue.json>)

Безопасное пустое состояние:

```json
{
  "version": 1,
  "entries": []
}
```

Для queue-файла можно оставить просто пустой файл.

Потом:
1. запустить `Prepare Daily Queue`
2. при необходимости вручную запустить `Publish From Queue`

## Что делать, если нужно проверить локально

Из папки проекта:

```powershell
cd "E:\Codex Open AI\Codex Lessons\telegram-release-pipeline"
py -m release_pipeline prepare-queue
py -m release_pipeline show-queue
py -m release_pipeline --dry-run publish-next
```

Если нужен реальный локальный пост:

```powershell
py -m release_pipeline publish-next
```

## Секреты

В GitHub Secrets должны существовать:
- `TMDB_API_TOKEN`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Они лежат в:
- `Settings` → `Secrets and variables` → `Actions`

Важно:
- [.env](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/.env>) не пушить
- токены не хранить в README, коде, issue, comment или workflow logs

## Когда нужно менять код

Менять код стоит, если:
- ухудшилось качество фильмов/сериалов
- нужно поменять расписание
- нужно менять формат постов
- нужно добавить новые источники

Ключевые файлы:
- [src/release_pipeline/pipeline.py](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/src/release_pipeline/pipeline.py>)
- [src/release_pipeline/config.py](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/src/release_pipeline/config.py>)
- [src/release_pipeline/queue_state.py](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/src/release_pipeline/queue_state.py>)
- [tests/test_pipeline.py](</E:/Codex Open AI/Codex Lessons/telegram-release-pipeline/tests/test_pipeline.py>)

## Самая короткая памятка

Если все работает:
- ничего не трогать

Если не создалась очередь:
- вручную запустить `Prepare Daily Queue`

Если не вышел пост:
- проверить `Publish From Queue`

Если нужно начать заново:
- очистить `posted_titles.json` и `publish_queue.json`

Если сломалось что-то непонятное:
- смотреть `Actions` → логи конкретного workflow
