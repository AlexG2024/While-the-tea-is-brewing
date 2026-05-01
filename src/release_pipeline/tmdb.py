from __future__ import annotations

from datetime import date
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TMDbClient:
    API_BASE = "https://api.themoviedb.org/3"
    IMAGE_BASE = "https://image.tmdb.org/t/p/w780"
    SITE_BASE = "https://www.themoviedb.org"

    def __init__(self, api_token: str, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(
            total=4,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
                "Accept": "application/json",
            }
        )

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = self.session.get(
            f"{self.API_BASE}{path}",
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

    def get_popular_movies(self, *, page: int = 1) -> list[dict[str, Any]]:
        payload = self._get(
            "/movie/popular",
            params={
                "language": "ru-RU",
                "page": page,
            },
        )
        return payload.get("results", [])

    def get_on_the_air_tv(self, *, page: int = 1) -> list[dict[str, Any]]:
        payload = self._get(
            "/tv/on_the_air",
            params={
                "language": "ru-RU",
                "page": page,
            },
        )
        return payload.get("results", [])

    def get_movie_details(self, movie_id: int, language: str) -> dict[str, Any]:
        return self._get(f"/movie/{movie_id}", params={"language": language})

    def get_movie_alternative_titles(self, movie_id: int, country: str) -> list[dict[str, Any]]:
        payload = self._get(
            f"/movie/{movie_id}/alternative_titles",
            params={"country": country},
        )
        return payload.get("titles", [])

    def get_movie_credits(self, movie_id: int, language: str) -> dict[str, Any]:
        return self._get(f"/movie/{movie_id}/credits", params={"language": language})

    def get_movie_release_dates(self, movie_id: int) -> dict[str, Any]:
        return self._get(f"/movie/{movie_id}/release_dates")

    def get_tv_details(self, tv_id: int, language: str) -> dict[str, Any]:
        return self._get(f"/tv/{tv_id}", params={"language": language})

    def get_tv_credits(self, tv_id: int, language: str) -> dict[str, Any]:
        return self._get(f"/tv/{tv_id}/credits", params={"language": language})

    def image_url(self, poster_path: str) -> str:
        return f"{self.IMAGE_BASE}{poster_path}"

    def title_url(self, media_type: str, tmdb_id: int) -> str:
        return f"{self.SITE_BASE}/{media_type}/{tmdb_id}"
