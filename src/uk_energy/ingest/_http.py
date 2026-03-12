"""
_http.py — Shared HTTP client factory with retries and rate limiting.

All ingest modules use `get_client()` to avoid duplicating retry/timeout config.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from uk_energy.config import DEFAULT_TIMEOUT


def _log_retry(retry_state: Any) -> None:
    logger.warning(
        f"Retrying {retry_state.fn.__name__} "  # type: ignore[union-attr]
        f"(attempt {retry_state.attempt_number}): "
        f"{retry_state.outcome.exception()}"
    )


def get_client(
    timeout: int = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
) -> httpx.Client:
    """Return a synchronous httpx client with sensible defaults."""
    default_headers = {
        "User-Agent": "uk-energy-modelling/0.1 (github.com/limbajimba/uk-energy)",
        "Accept": "application/json, text/csv, */*",
    }
    if headers:
        default_headers.update(headers)
    return httpx.Client(
        timeout=httpx.Timeout(timeout),
        headers=default_headers,
        follow_redirects=True,
    )


class RateLimitedClient:
    """Synchronous HTTP client that respects a requests-per-second limit."""

    def __init__(
        self,
        rps: float = 1.0,
        timeout: int = DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._rps = rps
        self._min_interval = 1.0 / rps
        self._last_call: float = 0.0
        self._client = get_client(timeout=timeout, headers=headers)

    def _wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        sleep_for = self._min_interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)
        self._last_call = time.monotonic()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        after=_log_retry,
        reraise=True,
    )
    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """GET with rate limiting and automatic retries."""
        self._wait()
        logger.debug(f"GET {url}")
        response = self._client.get(url, **kwargs)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            logger.warning(f"Rate limited by {url}, sleeping {retry_after}s")
            time.sleep(retry_after)
            # Re-enter through rate limiter for the retry
            self._wait()
            response = self._client.get(url, **kwargs)
        response.raise_for_status()
        return response

    @retry(
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        after=_log_retry,
        reraise=True,
    )
    def post(self, url: str, **kwargs: Any) -> httpx.Response:
        """POST with rate limiting and automatic retries."""
        self._wait()
        logger.debug(f"POST {url}")
        response = self._client.post(url, **kwargs)
        response.raise_for_status()
        return response

    def __enter__(self) -> "RateLimitedClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.__exit__(*args)

    def close(self) -> None:
        self._client.close()
