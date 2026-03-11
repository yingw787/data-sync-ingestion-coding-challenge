"""PostgreSQL operations: schema setup, batch upsert, checkpoint management."""

from __future__ import annotations

import json
from datetime import datetime, timezone
import os
from typing import Any

import asyncpg

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.getenv("DATABASE_URL"), min_size=2, max_size=10
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def setup_schema() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ingested_events (
                id              TEXT PRIMARY KEY,
                data            JSONB NOT NULL,
                event_timestamp TIMESTAMPTZ,
                ingested_at     TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
                ON ingested_events(event_timestamp)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
                id           INT PRIMARY KEY DEFAULT 1,
                cursor       TEXT NOT NULL,
                events_count BIGINT NOT NULL DEFAULT 0,
                updated_at   TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT single_row CHECK (id = 1)
            )
        """)


def parse_timestamp(event: dict[str, Any]) -> datetime | None:
    """Parse timestamp from an event dict. Handles ISO strings, Unix ms, Unix seconds."""
    raw = (
        event.get("timestamp")
        or event.get("created_at")
        or event.get("ts")
        or event.get("time")
    )
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        ms = raw if raw > 9_999_999_999 else raw * 1000
        try:
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def build_upsert_args(events: list[dict[str, Any]]) -> tuple[str, list[Any]]:
    """Build the VALUES clause and flat params list for a batch upsert."""
    placeholders: list[str] = []
    params: list[Any] = []
    for i, event in enumerate(events):
        base = i * 3
        placeholders.append(f"(${base + 1}, ${base + 2}::jsonb, ${base + 3})")
        params.extend([event["id"], json.dumps(event), parse_timestamp(event)])
    return ", ".join(placeholders), params


async def batch_upsert(events: list[dict[str, Any]], batch_size: int) -> int:
    """Insert events in batches, skipping duplicates. Returns count of newly inserted rows."""
    if not events:
        return 0

    pool = await get_pool()
    inserted = 0

    for i in range(0, len(events), batch_size):
        batch = events[i : i + batch_size]
        values_clause, params = build_upsert_args(batch)
        async with pool.acquire() as conn:
            result = await conn.execute(
                f"INSERT INTO ingested_events (id, data, event_timestamp) "
                f"VALUES {values_clause} ON CONFLICT (id) DO NOTHING",
                *params,
            )
            inserted += int(result.split()[-1])

    return inserted


async def get_checkpoint_cursor() -> str | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT cursor FROM ingestion_checkpoints WHERE id = 1"
        )
    return row["cursor"] if row else None


async def save_checkpoint(cursor: str, events_count: int) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ingestion_checkpoints (id, cursor, events_count, updated_at)
            VALUES (1, $1, $2, NOW())
            ON CONFLICT (id) DO UPDATE
                SET cursor = EXCLUDED.cursor,
                    events_count = EXCLUDED.events_count,
                    updated_at = NOW()
            """,
            cursor,
            events_count,
        )
