# TelePlay Backend — HidenCloud Deploy

## Entrypoint

`run.py` at repo root — HidenCloud runs `python /home/container/${PY_FILE}` with `PY_FILE=run.py`.

1. Copies root `.env` → `backend/.env`
2. Runs `pip install -r backend/requirements.txt`
3. Downloads `cloudflared-linux-arm64` and starts tunnel with `TUNNEL_TOKEN`
4. Installs opencode (if missing) via `curl -fsSL https://opencode.ai/install | bash`, starts `opencode web --hostname 127.0.0.1 --port 7444`
5. Threaded: TelePlay uvicorn on `127.0.0.1:7446`
6. Threaded: Monitor (status.html proxy) on `127.0.0.1:7442`

## Domain routing (Cloudflare Tunnel)

| Domain | Target |
|--------|--------|
| `aaruvi.aaruvi.space` | `localhost:7446` — TelePlay |
| `REDACTED_DOMAIN` | `localhost:7444` — opencode Web UI |
| `monitor.aaruvi.space` | `localhost:7442` — Status dashboard |

All services bind `127.0.0.1` — only reachable through tunnel.

## Architecture

- **FastAPI** app in `backend/app/main.py` — lifespan inits DB + starts Telegram clients
- **Routers** under `/api` prefix — `routers/__init__.py` is the source of truth for which exist
- **Bot handlers** registered via `from . import bot` (side-effect) — decorators `@tg_client.on_message`, `@tg_client.on_callback_query`
- **Client pool** built at **module level** in `telegram.py` — `clients[0]` = main bot, `clients[1:]` = 13 helpers. Each gets `pool_index` attr.
- **Monkey-patched Pyrogram** in `patch.py` (`PatchedClient`) — adds `wait_for_message`, `wait_for_callback_query`, fixes loop capture under uvicorn
- **Parallel streaming** in `streaming.py` — multi-client pool, BATCH_SIZE=20
- **Sliding window cache** — 200MB forward (pre-fetched) + 100MB backward (seek-back) in RAM per stream; remaining chunks on NVMe at `data/chunks/{chat_id}/{message_id}/{chunk_idx}`
- **Disk limit** — NVMe cache auto-evicts oldest files when exceeding 13GB (background task every hour)
- **OOM guard** in `status.py` — percentage-based at 80% of cgroup memory limit, auto-clears RAM caches
- **StreamCache** (`streaming.py:StreamCache`) — position-aware; `set_position(chunk_idx)` on each yield, `store()` routes within window to RAM, outside to disk; `get()` checks RAM then disk
- **Database** in `database.py` — auto-detects sqlite (default) vs postgresql from `DATABASE_URL`, auto-migrates missing columns
- **SPA catch-all** in `main.py` — serves `app/static/index.html` for any non-API route

## Config (`config.py`)

Reads from `.env` via pydantic-settings. Key fields:
- `SERVER_PORT` alias (default `7446`)
- `JWT_SECRET` — auto-generated if unset (sessions invalidate on restart)
- `DATABASE_URL` defaults to `sqlite:///./data/teleplay.db`
- `telegram_client_concurrency` = 5 (per-client semaphore)

## Env vars

| Variable | Required | Notes |
|----------|----------|-------|
| `TELEGRAM_API_ID` | yes | |
| `TELEGRAM_API_HASH` | yes | |
| `TELEGRAM_BOT_TOKEN` | yes | main bot |
| `TELEGRAM_STORAGE_CHANNEL_ID` | yes | private channel, bot is admin |
| `TELEGRAM_HELPER_BOT_TOKENS` | no | 13 tokens comma-separated |
| `DATABASE_URL` | no | defaults to SQLite |
| `AUTH_USERS` | no | comma-separated Telegram IDs |
| `JWT_SECRET` | no | auto-generated if missing |
| `WEB_BASE_URL` | no | default `http://localhost:3000` |
| `TUNNEL_TOKEN` | no | cloudflared token |
| `MEMORY` | no | default `16Gi`, set `3Gi` on HidenCloud |

## Platform quirks

- **ARM64** — `TgCrypto-pyrofork` for pre-built wheels, `cloudflared-linux-arm64` binary
- **3GB RAM** — sliding window 300MB×5 streams=1500MB max, app ~700MB, ~800MB headroom
- **No SSH** — debug via opencode Web UI at `REDACTED_DOMAIN`
- **`MEMORY=3Gi`** env var — status page reads this for accurate RAM display

## Dependencies

- `pyrotgfork` (not `pyrogram`)
- `TgCrypto-pyrofork` (ARM64 wheels)
- `uvloop` (auto-detected by uvicorn)
- No `huggingface_hub`, no `Dockerfile` (HF spaces code removed)

## Git

`.gitignore` excludes: `.env`, `__pycache__/`, `data/`, `session/`, `*.db`

## opencode.json

`opencode.json` binds port 7444 on 127.0.0.1 — no password set.
