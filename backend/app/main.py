"""
FastAPI main application with Telegram MTProto client lifecycle.
"""
import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .database import init_db
from .rate_limit import limiter
from .telegram import start_telegram_client, stop_telegram_client
from .status import get_status, attach_ring_handler, clear_logs, _discover_cgroup_memory

from .routers import files_router, folders_router, streaming_router, auth_router, tv_router, admin_router, gdrive_router, legal_router

# Import bot to register handlers
from . import bot  # noqa

logging.getLogger("pyrogram").setLevel(logging.DEBUG)
logging.getLogger("pyrogram.dispatcher").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

settings = get_settings()


async def _clear_cache_4h():
    """Clear in-memory chunk cache every 4 hours. Preserves caches for active streams."""
    while True:
        await asyncio.sleep(4 * 3600)
        from .streaming import _cache_manager as cm, _forward_streams as fs
        active = {(info["chat_id"], mid) for mid, info in fs.items()}
        freed = cm.clear_all(exclude_keys=active)
        kept = len(active)
        logger.info("Chunk cache cleared: freed %.1f MB (%d active streams preserved)", freed / 1024 / 1024, kept)

_last_oom_clear = 0.0
_OOM_CLEAR_COOLDOWN = 60


async def _oom_guard_30s():
    """Background OOM guard: clear in-memory caches when RAM exceeds 80%."""
    global _last_oom_clear
    while True:
        await asyncio.sleep(30)
        try:
            cur, mx = _discover_cgroup_memory()
            now = time.monotonic()
            if cur is not None and mx is not None and cur > 0.8 * mx and now - _last_oom_clear > _OOM_CLEAR_COOLDOWN:
                _last_oom_clear = now
                from .streaming import _cache_manager as cm, _forward_streams as fs
                active = {(info["chat_id"], mid) for mid, info in list(fs.items())}
                freed = cm.clear_all(exclude_keys=active)
                kept = len(active)
                logger.warning("OOM guard: cleared %.1f MB from cache (%d active streams)", freed / 1024 / 1024, kept)
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan - start/stop Telegram client and init DB."""
    logger.info("Starting TelePlay Backend...")
    await init_db()
    logger.info("Database initialized")
    attach_ring_handler()
    startup_task = await start_telegram_client()
    logger.info("Telegram client started")

    # Start background cleanup  
    cleanup_task = asyncio.create_task(_cleanup_expired_codes())
    cache_clear_task = asyncio.create_task(_clear_cache_4h())
    oom_task = asyncio.create_task(_oom_guard_30s())
    
    yield
    
    cleanup_task.cancel()
    cache_clear_task.cancel()
    oom_task.cancel()
    startup_task.cancel()
    try:
        await cleanup_task
        await cache_clear_task
        await oom_task
        await startup_task
    except asyncio.CancelledError:
        pass
    
    logger.info("Shutting down...")
    await stop_telegram_client()
    logger.info("Telegram client stopped")


async def _cleanup_expired_codes():
    """Periodically delete expired login codes."""
    from .database import async_session
    from .models import LoginCode
    from sqlalchemy import delete, func
    while True:
        try:
            await asyncio.sleep(300)  # every 5 minutes
            async with async_session() as db:
                await db.execute(
                    delete(LoginCode).where(LoginCode.expires_at < func.now())
                )
                await db.commit()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass


app = FastAPI(
    title="TelePlay API",
    description="Stream files from Telegram to Android TV and Web",
    version="1.0.0",
    lifespan=lifespan,
)

# Add rate limiter to app state
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware - Properly configured for production
# List allowed origins explicitly instead of using "*"
allowed_origins = [
    settings.web_base_url,
    "https://REDACTED_DOMAIN",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "Range"],
    expose_headers=["Content-Range", "Accept-Ranges", "Content-Length"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    
    # Prevent MIME type sniffing
    response.headers["X-Content-Type-Options"] = "nosniff"
    
    # Prevent clickjacking (allow framing only for same origin)
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    
    # XSS protection (legacy but still useful)
    response.headers["X-XSS-Protection"] = "1; mode=block"
    
    # Referrer policy - don't leak URLs
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
    # Content Security Policy (adjust as needed for your frontend)
    # response.headers["Content-Security-Policy"] = "default-src 'self'"
    
    return response


# Include routers
app.include_router(auth_router, prefix="/api")
app.include_router(files_router, prefix="/api")
app.include_router(folders_router, prefix="/api")
app.include_router(streaming_router, prefix="/api")
app.include_router(tv_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(gdrive_router, prefix="/api")
app.include_router(legal_router)




@app.get("/health")
async def health():
    """Health check with client connection status."""
    from .telegram import tg_client
    return {
        "status": "healthy",
        "client_connected": tg_client.is_connected if tg_client else False,
    }

@app.post("/api/restart")
async def api_restart():
    """Restart the entire process (HidenCloud will re-launch)."""
    import sys
    logger.warning("Restart requested via /api/restart — exiting")
    sys.stdout.flush()
    os._exit(0)

@app.get("/diag")
async def diagnostic(request: Request):
    """Diagnostic endpoint (logs, client status, env info)."""
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {settings.debug_password}":
        raise HTTPException(status_code=401, detail="Invalid debug token")
    from .telegram import tg_client, clients, get_diag_logs
    return {
        "client_connected": tg_client.is_connected if tg_client else False,
        "num_clients": len(clients),
        "logs": get_diag_logs(),
    }


@app.get("/api/v")
async def api_v():
    return {"v": 2, "commit": "33c4c1a57a7a"}

@app.get("/api/status")
async def api_status():
    return get_status()


@app.post("/api/status/clear-logs")
async def api_clear_logs():
    clear_logs()
    return {"status": "ok"}


@app.get("/status", include_in_schema=False)
async def status_page():
    return FileResponse("app/static/status.html")


if os.path.exists("app/static/assets"):
    app.mount("/assets", StaticFiles(directory="app/static/assets"), name="assets")

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    """Serve the React SPA for any non-API routes."""
    if full_path == "api" or full_path.startswith("api/") or ".." in full_path:
        raise HTTPException(status_code=404, detail="Not found")

    static_file_path = f"app/static/{full_path}"
    if os.path.exists(static_file_path) and os.path.isfile(static_file_path):
        return FileResponse(static_file_path)

    if os.path.exists("app/static/index.html"):
        return FileResponse("app/static/index.html")

    return JSONResponse(status_code=404, content={"detail": "Not found"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host=settings.server_host,
        port=settings.server_port,
        reload=True
    )
