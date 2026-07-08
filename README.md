---
title: Aruvi Backend
emoji: 🎬
colorFrom: blue
colorTo: purple
sdk: docker
app_port: 7860
pinned: false
license: mit
---

# Aruvi — Telegram Media Streaming Platform

[![Hugging Face Spaces](https://img.shields.io/badge/%F0%9F%A4%97-Hugging%20Face%20Spaces-blue)](https://huggingface.co/spaces/Tiraaruvi/aruvi-backend)

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

## Deployments

### HidenCloud (current)

3GB ARM64, 15GB NVMe. Accessible via Cloudflare Tunnel:

| Domain | Service |
|--------|---------|
| `REDACTED_DOMAIN` | TelePlay web player |
| `REDACTED_DOMAIN` | opencode Web UI (debug) |
| `monitor.aaruvi.space` | Status dashboard |

```bash
# HidenCloud runs: python /home/container/run.py
# Fresh git clone on every restart:
git clone https://github.com/Thirupathi-pirate/Aruvi-backend.git code/repo
```

`.env`:
```
TELEGRAM_API_ID=
TELEGRAM_API_HASH=
TELEGRAM_BOT_TOKEN=
TELEGRAM_STORAGE_CHANNEL_ID=
TELEGRAM_HELPER_BOT_TOKENS=token2,token3,...,token14
DATABASE_URL=postgresql+asyncpg://...
TUNNEL_TOKEN=
```

Daily exit at 3:30 AM IST — fresh IP on restart.

### Hugging Face Spaces (migrating)

Deployed via Docker:

```bash
docker build -t aruvi-backend .
docker run -p 7860:7860 \
  -e TELEGRAM_API_ID=... \
  -e TELEGRAM_API_HASH=... \
  -e TELEGRAM_BOT_TOKEN=... \
  -e TELEGRAM_STORAGE_CHANNEL_ID=... \
  aruvi-backend
```

Env vars specific to HF Spaces:

| Variable | Required | Notes |
|----------|----------|-------|
| `CLOUDFLARE_WORKERS_TOKEN` | no | Auto-deploys CF Worker proxy for `api.telegram.org` |
| `CLOUDFLARE_PROXY_URL` | no | Pre-existing proxy URL (skip auto-setup) |
| `CLOUDFLARE_PROXY_SECRET` | no | Shared secret for proxy auth |
| `APP_START_CMD` | no | Default: `uvicorn app.main:app --host 0.0.0.0 --port 7860` |

## Key Design Decisions

- **BATCH_SIZE=5** — 5 × 1MB chunks = 5MB per bot batch; reduced from 10 for faster first-byte
- **Global 500MB RAM** — not per-stream; prevents OOM with multiple concurrent streams
- **All chunks to NVMe** — both ahead and behind chunks persist; enables instant rewind without Telegram refetch
- **Per-bot fresh Message** — each worker fetches its own `get_messages()` to avoid cross-bot `FILE_REFERENCE_INVALID`
- **Sentinels for shutdown** — `concurrency` None tuples on task queue signal workers to stop
- **`reconnect_client` uses `start()`** — gets new auth key on `AuthKeyUnregistered`, not just `connect()`

## Tech Stack

- **Backend**: Python 3.11, FastAPI, SQLAlchemy async, Kurigram (Pyrogram fork)
- **Database**: PostgreSQL (Supabase) or SQLite
- **Cache**: In-memory + NVMe disk with 3h TTL
- **Tunnel**: Cloudflare Tunnel (cloudflared) + Cloudflare Workers proxy
- **Frontend**: React (pre-built, served as static files)
- **Platform**: HidenCloud ARM64 container / Hugging Face Spaces

## License

MIT
