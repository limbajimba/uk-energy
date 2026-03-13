"""
_http.py — Shared HTTP client with retries and rate limiting.

All ingest modules use ``get_client()`` or ``RateLimitedClient`` to avoid
duplicating retry / timeout / User-Agent configuration.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from uk_energy.config import DEFAULT_TIMEOUT


def _log_retry(retry_state: RetryCallState) -> None:
    """Log each retry attempt with attempt number and exception."""
    fn_name = getattr(retry_state.fn, "__name__", "unknown")
    exc = retry_state.outcome.exception() if retry_state.outcome else "n/a"
    logger.warning(
        f"Retrying {fn_name} (attempt {retry_state.attempt_number}): {exc}"
    )


def get_client(
    timeout: int = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
) -> httpx.Client:
    """Return a synchronous *httpx* client with project-wide defaults."""
    default_headers: dict[str, str] = {
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
    """Synchronous HTTP client that respects a requests-per-second ceiling.

    Features:
    * Token-bucket style rate limiter (one token per ``1/rps`` seconds).
    * Automatic retry with exponential back-off on transport errors and 5xx.
    * Explicit handling of HTTP 429 — honours the ``Retry-After`` header,
      then re-enters through the rate limiter before retrying.
    """

    def __init__(
        self,
        rps: float = 1.0,
        timeout: int = DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._min_interval: float = 1.0 / rps
        self._last_call: float = 0.0
        self._client: httpx.Client = get_client(timeout=timeout, headers=headers)

    def _wait(self) -> None:
        """Sleep until the rate-limit interval has elapsed."""
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
        """Rate-limited GET with automatic retries and 429 handling."""
        self._wait()
        logger.debug(f"GET {url}")
        response = self._client.get(url, **kwargs)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            logger.warning(f"Rate limited (429) by {url}, sleeping {retry_after}s")
            time.sleep(retry_after)
            # Re-enter through the rate limiter for the retry request
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
        """Rate-limited POST with automatic retries."""
        self._wait()
        logger.debug(f"POST {url}")
        response = self._client.post(url, **kwargs)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "60"))
            logger.warning(f"Rate limited (429) by {url}, sleeping {retry_after}s")
            time.sleep(retry_after)
            self._wait()
            response = self._client.post(url, **kwargs)

        response.raise_for_status()
        return response

    # ── Context manager ──────────────────────────────────────────────────────

    def __enter__(self) -> RateLimitedClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.close()

    def close(self) -> None:
        """Explicitly close the underlying transport."""
        self._client.close()
