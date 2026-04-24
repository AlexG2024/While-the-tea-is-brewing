from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
import json
import logging
import os
import sys

from .config import Settings
from .pipeline import ReleasePipeline
from .queue_state import PublishQueueStore
from .state import PublishedStateStore
from .telegram import TelegramPublisher
from .tmdb import TMDbClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TMDb to Telegram release pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Collect and format posts without sending anything to Telegram",
    )
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Immediate publish flow")
    run_parser.set_defaults(command="run")

    prepare_queue_parser = subparsers.add_parser(
        "prepare-queue",
        help="Collect movies and TV shows and save the daily publish queue",
    )
    prepare_queue_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite today's queue if it already exists",
    )

    publish_next_parser = subparsers.add_parser(
        "publish-next",
        help="Publish the next due item from today's queue",
    )
    publish_next_parser.set_defaults(command="publish-next")

    show_queue_parser = subparsers.add_parser(
        "show-queue",
        help="Show the current publish queue state",
    )
    show_queue_parser.set_defaults(command="show-queue")
    return parser


def _to_json_payload(value):
    if is_dataclass(value):
        return asdict(value)
    return value


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(errors="replace")

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.dry_run:
        os.environ["DRY_RUN"] = "1"

    settings = Settings.from_env()
    tmdb = TMDbClient(settings.tmdb_api_token, timeout=settings.request_timeout)
    telegram = TelegramPublisher(
        settings.telegram_bot_token,
        settings.telegram_chat_id,
        timeout=settings.request_timeout,
    )
    state_store = PublishedStateStore(
        settings.state_path,
        movie_dedupe_days=settings.movie_dedupe_days,
        tv_dedupe_days=settings.tv_dedupe_days,
    )
    queue_store = PublishQueueStore(settings.queue_path)
    pipeline = ReleasePipeline(settings, tmdb, telegram, state_store, queue_store=queue_store)

    command = args.command or "run"
    if command == "run":
        summary = pipeline.run()
    elif command == "prepare-queue":
        summary = pipeline.prepare_queue(force=getattr(args, "force", False))
    elif command == "publish-next":
        summary = pipeline.publish_next()
    elif command == "show-queue":
        summary = pipeline.show_queue()
    else:
        parser.error(f"Unknown command: {command}")
        return 2

    print(json.dumps(_to_json_payload(summary), ensure_ascii=False, indent=2))
    return 0
