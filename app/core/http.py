from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any

import requests

from app.core.exceptions import ProviderFetchError


@dataclass(frozen=True)
class RequestConfig:
    timeout_seconds: int = 20
    max_attempts: int = 3
    backoff_seconds: float = 1.0


class HttpClient:
    def __init__(self, config: RequestConfig | None = None):
        self.config = config or RequestConfig()

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self._request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self._request("POST", url, **kwargs)

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        timeout = kwargs.pop("timeout", self.config.timeout_seconds)
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                response = requests.request(method, url, timeout=timeout, **kwargs)
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt == self.config.max_attempts:
                    break
                time.sleep(self.config.backoff_seconds * attempt)
        raise ProviderFetchError(f"{method} {url} failed") from last_error

