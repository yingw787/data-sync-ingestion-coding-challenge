"""HTTP client for the DataSync Analytics API."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import config

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            headers={"X-API-Key": config.api_key},
            timeout=30.0,
        )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


@dataclass
class PageResult:
    data: list[dict[str, Any]]
    has_more: bool
    next_cursor: str | None


@retry(
    retry=retry_if_exception_type(OSError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1.0, min=1, max=30),
    reraise=True,
)
async def fetch_page(cursor: str | None, limit: int) -> PageResult:
    params: dict[str, str | int] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor

    client = get_client()

    try:
        response = await client.get(
            f"{config.api_base_url}/api/v1/events",
            params=params,
        )
    except (httpx.TransportError, httpx.TimeoutException) as exc:
        raise OSError(str(exc)) from exc

    # Proactive rate-limit pause
    remaining_hdr = response.headers.get("x-ratelimit-remaining")
    reset_hdr = response.headers.get("x-ratelimit-reset")
    if remaining_hdr is not None:
        remaining = int(remaining_hdr)
        if remaining <= config.rate_limit_buffer and reset_hdr is not None:
            wait = int(reset_hdr) - time.time() + 0.1
            if wait > 0:
                await asyncio.sleep(wait)

    if response.status_code == 429:
        raise OSError("429 rate limited")

    if response.status_code == 401:
        raise PermissionError("Invalid API key")

    if response.status_code in (400, 410):
        raise ValueError(f"Cursor expired or invalid (HTTP {response.status_code})")

    if response.status_code >= 500:
        raise OSError(f"Server error: HTTP {response.status_code}")

    response.raise_for_status()

    body = response.json()
    return PageResult(
        data=body.get("data") or [],
        has_more=bool(body.get("hasMore")),
        next_cursor=body.get("nextCursor"),
    )
