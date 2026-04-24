from __future__ import annotations

import requests


class TelegramPublisher:
    def __init__(self, bot_token: str, chat_id: str, timeout: int = 30) -> None:
        self.chat_id = chat_id
        self.timeout = timeout
        self.base_url = f"https://api.telegram.org/bot{bot_token}"
        self.session = requests.Session()

    def send_photo(self, photo_url: str, caption: str) -> int:
        response = self.session.post(
            f"{self.base_url}/sendPhoto",
            data={
                "chat_id": self.chat_id,
                "photo": photo_url,
                "caption": caption,
                "parse_mode": "HTML",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(f"Telegram API returned error: {payload}")
        return int(payload["result"]["message_id"])

