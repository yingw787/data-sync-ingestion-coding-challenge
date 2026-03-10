# DataSync Ingestion Challenge — Claude Notes

## Critical Facts

- **Ingestion container name:** `assignment-ingestion` (hardcoded in `run-ingestion.sh`)
- **Completion signal:** log exactly `ingestion complete` (lowercase, no punctuation — `run-ingestion.sh` greps for this)
- **Table name:** `ingested_events` (hardcoded in `run-ingestion.sh` COUNT query)
- **API base URL:** `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com`
- **API events endpoint:** `GET /api/v1/events?cursor=<token>&limit=<n>`
- **Auth header:** `X-API-Key` (header-based gives better rate limits per `.env.example` comment)

## API Key

- Timer starts on **first API call** — 90 min window
- Do NOT make exploratory calls before the real ingestion run
- Set real key in `.env`: `TARGET_API_KEY=<your_key>` + `API_BASE_URL=http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com`

## Stack

- **Language:** Python 3.12
- **Package manager:** uv (`pyproject.toml` in each package)
- **HTTP:** `httpx` (async, persistent connection pool)
- **PostgreSQL:** `asyncpg`
- **Mock API:** `fastapi` + `uvicorn` (testing only)
- **Tests:** `pytest` + `pytest-asyncio` + `respx`

## Architecture

- `packages/ingestion/` — Python ingestion service (asyncio pipeline, cursor pagination)
- `packages/mock-api/` — Local mock FastAPI server (testing only, simulates DataSync API)
- Single async process with fetch-ahead pipeline (no Redis/workers needed)

## Running

- **Test mode (default):** `sh run-ingestion.sh` or `docker compose up --build`
  - Uses mock API at `http://mock-api:3000`, 10,000 fake events
- **Production mode:** Set `TARGET_API_KEY` + `API_BASE_URL` in `.env`, then `sh run-ingestion.sh`
- **Unit tests only:** `cd packages/ingestion && uv run pytest`

## Known API Behaviors

- Cursor-based pagination (`nextCursor` in response, `cursor` query param)
- Rate limit headers: `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After` on 429
- Cursors have a lifecycle — may expire (400/410 → `CursorExpiredError` → restart from null)
- Timestamp formats vary across events — normalize carefully
- Undocumented endpoints likely exist — explore `/api/v1/` and the dashboard before full run
- Parallel cursors / offset-based pagination may be possible (would dramatically increase throughput)

## Database Schema

```sql
-- ingested_events: main storage, id is PRIMARY KEY for deduplication
-- ingestion_checkpoints: cursor + count saved periodically for resumability
```

## Resumability Strategy

1. On startup: load latest checkpoint (cursor + count)
2. All inserts: `ON CONFLICT (id) DO NOTHING` — idempotent
3. Expired cursor: restart from null, rely on deduplication
4. SIGTERM: save checkpoint before exit
5. Docker `restart: on-failure`: auto-restarts crashed container
