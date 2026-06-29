# Aruvi — Telegram Media Streaming Platform

Stream your Telegram media files (videos, audio) to any browser or Android TV using multi-bot parallel streaming with intelligent caching.

## Architecture

```
Browser/TV App → Cloudflare Tunnel → FastAPI (uvicorn) → PyroTGFork clients → Telegram MTProto
                                    ↕
                         Sliding Window Cache (500MB RAM global)
                                    ↕
                          NVMe Disk Cache (13GB, 3h TTL)
```

### Key Components

| Component | Description |
|-----------|-------------|
| **FastAPI backend** | REST API for auth, file listing, streaming |
| **14 Telegram bots** | 1 main bot + 13 helper bots for parallel chunk fetching |
| **StreamCache** | Position-aware sliding window: 300MB fwd + 100MB back per stream |
| **CacheManager** | Global 500MB RAM limit across all streams, spills to NVMe |
| **Disk cache** | All chunks persisted to NVMe (`data/chunks/`), 3h TTL, 13GB max |
| **Status monitor** | Live dashboard at `monitor.aaruvi.space` |

### Streaming Pipeline

1. **All-bot warmup** — all 14 bots fetch messages 1-20 at startup
2. **Fast-start** — first 13 chunks as 1-chunk batches across all 13 helpers
3. **Batch fetch** — remaining chunks in parallel (BATCH_SIZE=5 per bot)
4. **Sliding window** — 300MB ahead / 100MB behind stays in RAM; rest to NVMe
5. **100MB lookahead** — maintains cushion against Telegram latency spikes
6. **Global OOM guard** — evicts farthest chunks across all streams at 500MB

## Deployment

Deployed on **HidenCloud** (3GB ARM64, 15GB NVMe). Accessible via Cloudflare Tunnel:

| Domain | Service |
|--------|---------|
| `REDACTED_DOMAIN` | TelePlay web player |
| `REDACTED_DOMAIN` | opencode Web UI (debug) |
| `monitor.aaruvi.space` | Status dashboard |

### Bootstrap

```bash
# HidenCloud runs: python /home/container/run.py
# Which clones this repo fresh on every restart:
git clone https://github.com/Thirupathi-pirate/Aruvi-backend.git code/repo
```

`.env` (persistent, not in git):
```
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_BOT_TOKEN=
TELEGRAM_STORAGE_CHANNEL_ID=
TELEGRAM_HELPER_BOT_TOKENS=token2,token3,...,token14
DATABASE_URL=postgresql+asyncpg://...
TUNNEL_TOKEN=
```

### Daily Restart

Process exits at 3:30 AM IST — HidenCloud auto-restarts with fresh IP.

## Key Design Decisions

- **BATCH_SIZE=5** — 5 × 1MB chunks = 5MB per bot batch; reduced from 10 for faster first-byte
- **Global 500MB RAM** — not per-stream; prevents OOM with multiple concurrent streams
- **All chunks to NVMe** — both ahead and behind chunks persist; enables instant rewind without Telegram refetch
- **Per-bot fresh Message** — each worker fetches its own `get_messages()` to avoid cross-bot `FILE_REFERENCE_INVALID`
- **Sentinels for shutdown** — `concurrency` None tuples on task queue signal workers to stop
- **`reconnect_client` uses `start()`** — gets new auth key on `AuthKeyUnregistered`, not just `connect()`

## Tech Stack

- **Backend**: Python 3.13, FastAPI, SQLAlchemy async, PyroTGFork
- **Database**: PostgreSQL (Supabase) or SQLite
- **Cache**: In-memory + NVMe disk with 3h TTL
- **Tunnel**: Cloudflare Tunnel (cloudflared ARM64 binary)
- **Frontend**: React (pre-built, served as static files)
- **Platform**: HidenCloud ARM64 container

## License

MIT
