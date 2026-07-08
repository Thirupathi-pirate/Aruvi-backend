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
1. Fresh `git clone --depth=1` of `github.com/Thirupathi-pirate/Aruvi-backend` into `code/repo/`
2. Copies `code/.env` into the cloned repo
3. `pip install -r requirements.txt`
4. Downloads `cloudflared-linux-arm64` and starts tunnel with `TUNNEL_TOKEN`
5. CF tunnel/DNS setup via `cf_tunnel.py` (if `CLOUDFLARE_API_TOKEN` set)
6. Downloads `opencode-linux-arm64` (via urllib) and starts `opencode web --hostname 127.0.0.1 --port 7444`
7. Starts opencode Telegram Bot via `npx @grinev/opencode-telegram-bot` (if tokens set)
8. Threaded: TelePlay uvicorn on `0.0.0.0:24696`
9. Threaded: Monitor (status.html proxy + /api/ forwarder) on `127.0.0.1:7442`
10. Health-check loop (30s timeout), then blocks until 3:30 AM IST, then `os._exit(0)` for fresh IP

## Domain routing (Cloudflare Tunnel)

| Domain | Target | Notes |
|--------|--------|-------|
| `REDACTED_DOMAIN` | `localhost:24696` — TelePlay | CNAME to tunnel endpoint, proxied |
| `REDACTED_DOMAIN` | `localhost:7444` — opencode Web UI | via tunnel |
| `monitor.aaruvi.space` | `localhost:7442` — monitor proxy | proxies /api/* + /health to TelePlay |

TelePlay binds `0.0.0.0:24696` — directly accessible at `REDACTED_HOST:24696` (2 Mbps capped) and via tunnel at `REDACTED_DOMAIN` (22 Mbps).

## CI/CD

GitHub Actions at `.github/workflows/renew.yml` — HidenCloud auto-renewal (cron updated dynamically by the workflow itself based on due date). Runs `oyz8/HidenCloud` renew script via `seleniumbase`.

Push to `origin main` to deploy (HidenCloud `code/run.py` clones fresh on each restart — no separate workflow needed).

## Architecture

- **FastAPI** app in `backend/app/main.py` — lifespan inits DB + starts Telegram clients
- **Routers** — 9 routers exported from `routers/__init__.py`: files, folders, streaming, auth, tv, admin, gdrive, legal, diagnostic
- **Bot handlers** registered via `from . import bot` (side-effect) — decorators `@tg_client.on_message`, `@tg_client.on_callback_query`
- **Client pool** built at **module level** in `telegram.py` — `clients[0]` = main bot, `clients[1:]` = 13 helpers. Each gets `pool_index` attr.
- **Monkey-patched Pyrogram** in `patch.py` (`PatchedClient`) — adds `wait_for_message`, `wait_for_callback_query`, fixes loop capture under uvicorn
- **Parallel streaming** in `streaming.py` — multi-client pool, `BATCH_SIZE=15` (15×1MB chunks per bot batch), 14 workers pull from a shared task queue
- **Yield smoothing** (`streaming.py:740-746`) — waits for `MIN_PREBUFFER=5` chunks before first yield
- **Backpressure** (`streaming.py:725-732`) — `WINDOW_CHUNKS=350` semaphore caps in-flight resolved-but-unyielded chunks to 350 MB
- **RAM-only ChunkCache** (`streaming.py:ChunkCache`) — 100MB per-video FIFO cache, no disk spill, managed by `CacheManager`
- **OOM guard** in `status.py` — percentage-based at 80% of cgroup memory limit, auto-clears RAM caches
- **Database** in `database.py` — auto-detects sqlite (default) vs postgresql from `DATABASE_URL`, auto-migrates missing columns
- **SPA catch-all** in `main.py` — serves `app/static/index.html` for any non-API route

## Config (`config.py`)

Reads from `.env` via pydantic-settings. Key fields:
- `SERVER_PORT` alias (default `24696`)
- `JWT_SECRET` — auto-generated if unset (sessions invalidate on restart)
- `DATABASE_URL` defaults to `sqlite+aiosqlite:///./data/teleplay.db`
- `telegram_client_concurrency` = 5 (per-client semaphore for Pyrogram)
- Tunable constants in `streaming.py`: `BATCH_SIZE=15`, `MIN_PREBUFFER=5`, `WINDOW_CHUNKS=350`, `_stream_semaphore=5`

## Diagnostic endpoints

Added in `backend/app/routers/diagnostic.py`:
- **`GET /api/diag/ping`** — requires `Bearer DEBUG_PASSWORD`. Returns `{"server_time": ..., "status": "ok"}`.
- **`GET /api/diag/stream?msg=MSG_ID&chat=CHAT_ID`** — requires `Bearer DEBUG_PASSWORD`. Streams actual media from any Telegram chat. Adds diagnostic headers: `X-Diag-Ttfb-Ms`, `X-Diag-File-Size`, `X-Diag-Content-Type`, `X-Diag-Msg-Id`, `X-Diag-Chat-Id`.

Frontend test page at **`/diag-player.html`** — pass `?chat=...&msg=...` as query params. Shows live speed, buffered duration, TTFB.

## Env vars

| Variable | Required | Notes |
|----------|----------|-------|
| `TELEGRAM_API_ID` | yes | |
| `TELEGRAM_API_HASH` | yes | |
| `TELEGRAM_BOT_TOKEN` | yes | main bot |
| `TELEGRAM_STORAGE_CHANNEL_ID` | yes | private channel, bot is admin |
| `TELEGRAM_HELPER_BOT_TOKENS` | no | 13 tokens comma-separated |
| `TELEGRAM_BOT_SESSION_STRINGS` | no | session strings (alternative to tokens) |
| `DATABASE_URL` | no | defaults to SQLite |
| `AUTH_USERS` | no | comma-separated Telegram IDs |
| `ADMIN_IDS` | no | comma-separated admin IDs |
| `JWT_SECRET` | no | auto-generated if missing |
| `SERVER_PORT` | no | default `24696` |
| `MEMORY` | no | default `16Gi`, set `3Gi` on HidenCloud |
| `TUNNEL_TOKEN` | no | cloudflared tunnel token |
| `CLOUDFLARE_API_TOKEN` | no | CF API token for tunnel/DNS mgmt. Must be in `code/.env`. |
| `DEBUG_PASSWORD` | no | diagnostic endpoint auth |
| `OPENCODE_BOT_TOKEN` | no | opencode Telegram bot (grinev) |
| `OPENCODE_BOT_USER_ID` | no | allowed Telegram user ID for bot |

## Platform quirks

- **ARM64** — `TgCrypto-pyrofork` for pre-built wheels, `cloudflared-linux-arm64` binary
- **3GB RAM** — global 500MB limit across all streams via `CacheManager._evict_one()`, app ~700MB, ~1.8GB headroom
- **No SSH** — debug via opencode Web UI at `REDACTED_DOMAIN`, or Telegram bot
- **`MEMORY=3Gi`** env var — status page reads this for accurate RAM display
- **HidenCloud fresh IP** — process exits at 3:30 AM IST daily; container restarts with new IP
- **Git clone each restart** — `shutil.rmtree(REPO_DIR) + git clone --depth=1` on every boot (no incremental pull)

## HF Spaces Migration (In Progress)

New files added for HF Spaces deploy:
- `Dockerfile` — builds from `python:3.11-slim`, installs Node.js 20 + build-essential, copies `cf-proxy/` scripts + `backend/` app
- `cf-proxy/cloudflare-proxy-setup.py` — deploys Cloudflare Worker to proxy `api.telegram.org` (extracted from hf-wrapper, restricted to single domain for TOS)
- `cf-proxy/cloudflare-proxy.js` — Node.js `--require` hook that patches `http`/`https`/`fetch`/`undici` to route blocked domains through CF Worker
- `cf-proxy/start.sh` — simplified entrypoint: proxy setup → source env → `NODE_OPTIONS --require` → `APP_START_CMD`

No keepalive, health server, or HF Dataset backup — user explicitly requested CF proxy only.

Env vars for HF Spaces:
| Var | Required | Notes |
|-----|----------|-------|
| `CLOUDFLARE_WORKERS_TOKEN` | no | CF API token for Workers Scripts: Edit. Without it, proxy is skipped |
| `CLOUDFLARE_PROXY_URL` | no | Pre-existing proxy URL (skip auto-setup) |
| `CLOUDFLARE_PROXY_SECRET` | no | Shared secret for proxy auth |
| `APP_START_CMD` | yes | Default: `uvicorn app.main:app --host 0.0.0.0 --port 7860` |

## Dependencies

- `kurigram[fast]` (was `pyrotgfork`) — drop-in Pyrogram fork with Gifts, Stories, Topics support. Same `from pyrogram import ...` imports. `[fast]` bundles `tgcrypto` + `uvloop`.
- No `huggingface_hub`, no `TgCrypto-pyrofork`, no standalone `uvloop` dep

## Git

`.gitignore` excludes: `.env`, `backend/.env`, `code/.env`, `code/repo/`, `__pycache__/`, `data/`, `session/`, `*.session`, `*.db`

## opencode.json

`opencode.json` binds port 7444 on 127.0.0.1 — no password set.
