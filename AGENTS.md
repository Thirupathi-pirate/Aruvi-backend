# TelePlay Backend — HidenCloud Deploy

## Frontend
Pre-built TelePlay React app at `backend/app/static/` (committed to git). Served by the SPA catch-all in `main.py`. To update: rebuild locally from `github.com/Thirupathi-pirate/teleplay-frontend`, copy `dist/` to `backend/app/static/`, commit, push, restart.

## Entrypoint

`run.py` at repo root delegates to `code/run.py`.

HidenCloud runs `python /home/container/${PY_FILE}` with `PY_FILE=run.py`.

### Bootstrap (`code/run.py`)

Two files stay on HidenCloud permanently:
- `code/run.py` — bootstrap script
- `code/.env` — secrets (gitignored)

On each restart:
1. Clones/pulls `github.com/Thirupathi-pirate/Aruvi-backend` into `code/repo/`
2. Copies `code/.env` into the cloned repo
3. `pip install` requirements
4. Downloads `cloudflared-linux-arm64` and starts tunnel with `TUNNEL_TOKEN`
5. Downloads `opencode-linux-arm64` (via urllib) and starts `opencode web --hostname 127.0.0.1 --port 7444`
6. Threaded: TelePlay uvicorn on `0.0.0.0:24696` (HidenCloud public port)
7. Threaded: Monitor (status.html proxy) on `127.0.0.1:7442`
8. Blocks until 3:30 AM IST, then `os._exit(0)` for fresh IP

## Domain routing (Cloudflare Tunnel)

| Domain | Target | Notes |
|--------|--------|-------|
| `REDACTED_DOMAIN` | `localhost:24696` — TelePlay (via tunnel) | CNAME to tunnel endpoint, proxied |
| `REDACTED_DOMAIN` | `localhost:7444` — opencode Web UI | via tunnel |
| `monitor.aaruvi.space` | `localhost:7442` — monitor proxy | tunnel proxies /api/* + /health to TelePlay via monitor |

TelePlay binds `0.0.0.0:24696` — directly accessible at `REDACTED_HOST:24696` (2 Mbps capped) and via tunnel at `REDACTED_DOMAIN` (22 Mbps).

## Architecture

- **FastAPI** app in `backend/app/main.py` — lifespan inits DB + starts Telegram clients
- **Routers** under `/api` prefix — `routers/__init__.py` is the source of truth for which exist
- **Bot handlers** registered via `from . import bot` (side-effect) — decorators `@tg_client.on_message`, `@tg_client.on_callback_query`
- **Client pool** built at **module level** in `telegram.py` — `clients[0]` = main bot, `clients[1:]` = 13 helpers. Each gets `pool_index` attr.
- **Monkey-patched Pyrogram** in `patch.py` (`PatchedClient`) — adds `wait_for_message`, `wait_for_callback_query`, fixes loop capture under uvicorn
- **Parallel streaming** in `streaming.py` — multi-client pool, BATCH_SIZE=15 (15×1MB chunks per bot batch), 14 workers pull from a shared task queue
- **Yield smoothing** (`streaming.py:670-687`) — waits for `MIN_PREBUFFER=5` chunks before first yield. Low value avoids 10s TTFB from Telegram DC init; 5 chunks arrive progressively within the first batch fetch (~3s).
- **Backpressure** (`streaming.py:661-668`) — `WINDOW_CHUNKS=200` semaphore caps in-flight resolved-but-unyielded chunks to 200 MB. Workers `acquire()` per chunk in `_fetch_batch`, yield loop `release()` after each yield. Prevents OOM for files >3 GB where 3584+ futures would otherwise fill RAM before the tunnel drains them. Peak: 200 MB + 100 MB cache + ~700 MB app base ≈ 1 GB, well within 1.8 GB headroom.
- **RAM-only ChunkCache** (`streaming.py:ChunkCache`) — 100MB per-video FIFO cache for backward-seek support. No disk spill. Managed globally by `CacheManager`.
- **OOM guard** in `status.py` — percentage-based at 80% of cgroup memory limit, auto-clears RAM caches
- **Database** in `database.py` — auto-detects sqlite (default) vs postgresql from `DATABASE_URL`, auto-migrates missing columns
- **SPA catch-all** in `main.py` — serves `app/static/index.html` for any non-API route

## Config (`config.py`)

Reads from `.env` via pydantic-settings. Key fields:
- `SERVER_PORT` alias (default `24696`)
- `JWT_SECRET` — auto-generated if unset (sessions invalidate on restart)
- `DATABASE_URL` defaults to `sqlite:///./data/teleplay.db`
- `telegram_client_concurrency` = 5 (per-client semaphore for Pyrogram)
- Tunable constants in `streaming.py`: `BATCH_SIZE=15` (chunks per RPC), `MIN_PREBUFFER=5` (chunks before first yield), `WINDOW_CHUNKS=200` (backpressure cap on in-flight resolved chunks), `_stream_semaphore=5` (concurrent video streams)

## Diagnostic endpoints (for playback testing)

Added in `backend/app/routers/diagnostic.py`:
- **`GET /api/diag/ping`** — requires `Bearer DEBUG_PASSWORD`. Returns `{"server_time": ..., "status": "ok"}`.
- **`GET /api/diag/stream?msg=MSG_ID&chat=CHAT_ID`** — requires `Bearer DEBUG_PASSWORD`. Streams actual media from any Telegram chat (defaults to storage channel). Adds diagnostic headers: `X-Diag-Ttfb-Ms`, `X-Diag-File-Size`, `X-Diag-Content-Type`, `X-Diag-Msg-Id`, `X-Diag-Chat-Id`. Uses the same `stream_file_chunks()` pipeline as real TelePlay streaming.

Frontend test page at **`/diag-player.html`** — pass `?chat=...&msg=...` as query params or use the UI. Shows live speed, buffered duration, TTFB.

To find a test video: pick any public Telegram channel with a video, note the chat ID and message ID, open:
`http://REDACTED_HOST:24696/diag-player.html?chat=-100XXX&msg=123`

## `CLOUDFLARE_API_TOKEN` on server

Must be in `code/.env` on HidenCloud so `cf_tunnel.py` setup runs at startup (ensures REDACTED_DOMAIN is on tunnel + DNS). Get the token from local `~/.bashrc`. If missing, `code/run.py` skips CF setup (non-fatal, logs warning).

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
| `CLOUDFLARE_API_TOKEN` | no | CF API token for tunnel/DNS mgmt. Must be in `code/.env`. |

## Platform quirks

- **ARM64** — `TgCrypto-pyrofork` for pre-built wheels, `cloudflared-linux-arm64` binary
- **3GB RAM** — global 500MB limit across all streams via `CacheManager._evict_one()`, app ~700MB, ~1.8GB headroom
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
