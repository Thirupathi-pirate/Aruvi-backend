"""
PyroTGFork MTProto client for Telegram interactions.
Handles both bot commands and file streaming via a client pool.
"""
import time
from .patch import Client
from pyrogram.types import Message
from .config import get_settings
from pathlib import Path
import asyncio
import logging


settings = get_settings()

BASE_DIR = Path(__file__).resolve().parent.parent
SESSION_DIR = BASE_DIR / "session"


def get_session_name(index: int) -> str:
    return str(SESSION_DIR / f"bot_{index}")


logger = logging.getLogger(__name__)

# In-memory log collector (capped at 200 to prevent memory growth)
_startup_logs: list[str] = []
_MAX_DIAG_LOGS = 200

def diag_log(msg):
    _startup_logs.append(msg)
    if len(_startup_logs) > _MAX_DIAG_LOGS:
        _startup_logs.pop(0)
    logger.info(msg)

def get_diag_logs():
    return list(_startup_logs)

# Build pool at module level
tokens = settings.all_bot_tokens
session_strings = settings.telegram_bot_session_strings
clients = []

diag_log(f"Creating {len(tokens)} client(s)...")
for i, token in enumerate(tokens):
    diag_log(f"Client {i}: building at module level...")
    kwargs = dict(
        api_id=settings.telegram_api_id,
        api_hash=settings.telegram_api_hash,
        bot_token=token,
        ipv6=False,
        max_concurrent_transmissions=settings.telegram_client_concurrency,
        no_updates=(i > 0),
    )
    if i < len(session_strings) and session_strings[i]:
        client = Client(name=":memory:", session_string=session_strings[i], **kwargs)
        diag_log(f"Client {i}: using in-memory session")
    else:
        client = Client(name=get_session_name(i), **kwargs)
    diag_log(f"Client {i}: built (is_connected={client.is_connected})")
    client.pool_index = i
    clients.append(client)

tg_client = clients[0]
diag_log("Module-level setup complete")


# ── lifecycle helpers ────────────────────────────────────────────────

async def start_one_client(i, c):
    max_attempts = 3
    connect_timeout = 20
    for attempt in range(1, max_attempts + 1):
        try:
            diag_log(f"Client {i}: starting (attempt {attempt}, is_connected={c.is_connected})")
            await asyncio.wait_for(c.start(), timeout=connect_timeout)
            diag_log(f"Client {i}: start() returned (is_connected={c.is_connected})")
            me = await c.get_me()
            label = "Main" if i == 0 else "Helper"
            diag_log(f"Client {i} ({label}) started → @{me.username}")
            return
        except Exception as e:
            err_str = str(e).lower()
            # Flood wait: sleep and retry
            if "flood_wait" in err_str or "flood" in err_str:
                import re
                match = re.search(r"(\d+)", err_str)
                wait = min(int(match.group(1)) if match else 60, 120)
                diag_log(f"Client {i}: flood wait {wait}s, retrying...")
                await asyncio.sleep(wait)
                continue
            if attempt < max_attempts:
                delay = 2 ** attempt
                diag_log(f"Client {i}: transient error (attempt {attempt}): {e}. Retrying in {delay}s...")
                await asyncio.sleep(delay)
                continue
            import traceback
            tb = traceback.format_exc()
            diag_log(f"Client {i} failed to start after {max_attempts} attempts: {e}\n{tb}")
    # If all attempts exhausted and this is main bot, propagate failure
    if i == 0 and not c.is_connected:
        raise RuntimeError(f"Bot 0 failed to connect after {max_attempts} attempts")


async def start_all_clients():
    logger.info("Starting %d Telegram client(s)...", len(clients))
    tasks = [start_one_client(i, c) for i, c in enumerate(clients)]
    await asyncio.gather(*tasks)


async def stop_one_client(c):
    try:
        if c.is_connected:
            await c.stop()
    except Exception:
        pass


async def stop_all_clients():
    for c in clients:
        await stop_one_client(c)


async def reconnect_client(client: Client) -> bool:
    """Disconnect and reconnect a Pyrogram client to get a fresh DC auth key.
    
    Call this when catching AuthKeyUnregistered — the old auth key expired.
    Returns True if reconnection succeeded, False otherwise.
    """
    try:
        if client.is_connected:
            await client.disconnect()
        await client.connect()
        diag_log(f"Client {getattr(client, 'pool_index', '?')} reconnected successfully")
        return True
    except Exception as e:
        diag_log(f"Client {getattr(client, 'pool_index', '?')} reconnect failed: {e}")
        return False


async def start_telegram_client():
    """Called from app lifespan — starts main bot immediately, helpers in background.
    
    Returns the background task so the caller can cancel it on shutdown.
    """
    await start_one_client(0, clients[0])
    diag_log("Main client started — app is ready to serve")
    task = asyncio.create_task(_finish_startup())
    return task


async def _finish_startup():
    """Start helper bots in parallel, then verify storage channel access."""
    if len(clients) > 1:
        tasks = [start_one_client(i, c) for i, c in enumerate(clients[1:], 1)]
        await asyncio.gather(*tasks)

    # Verify each bot can access the storage channel
    channel_id = settings.telegram_storage_channel_id
    if channel_id:
        for i, c in enumerate(clients):
            if not c.is_connected:
                diag_log(f"Client {i}: skipped channel check (not connected)")
                continue
            try:
                me = await c.get_me()
                msg = await c.get_messages(channel_id, 1)
                if msg:
                    diag_log(f"Client {i} (@{me.username}): channel access OK")
                else:
                    diag_log(f"Client {i} (@{me.username}): channel returned empty — add bot as admin")
            except Exception as e:
                diag_log(f"Client {i} (@{me.username}): CHANNEL_INVALID — add this bot as admin to channel {channel_id}")
                diag_log(f"  Bot token starts with: {getattr(c, 'bot_token', '?')[:8]}...")
                diag_log(f"  Error: {e}")


async def stop_telegram_client():
    """Called from app lifespan — stops the full pool."""
    await stop_all_clients()


# ── Message cache ────────────────────────────────────────────────────

_msg_cache: dict[int, tuple[float, Message]] = {}
MSG_CACHE_TTL = 300  # 5 minutes
_MSG_CACHE_MAX = 5000

def _msg_cache_evict():
    """Remove oldest entries if cache exceeds max size."""
    if len(_msg_cache) <= _MSG_CACHE_MAX:
        return
    # Sort by timestamp and remove oldest 20%
    by_age = sorted(_msg_cache.items(), key=lambda x: x[1][0])
    to_remove = len(_msg_cache) - int(_MSG_CACHE_MAX * 0.8)
    for mid, _ in by_age[:to_remove]:
        _msg_cache.pop(mid, None)

def invalidate_message_cache(message_id: int):
    _msg_cache.pop(message_id, None)

def invalidate_message_cache_batch(message_ids: list[int]):
    for mid in message_ids:
        _msg_cache.pop(mid, None)

# ── convenience helpers (always use tg_client) ───────────────────────

async def get_message_from_channel(message_id: int) -> Message:
    now = time.monotonic()
    if message_id in _msg_cache:
        ts, msg = _msg_cache[message_id]
        if now - ts < MSG_CACHE_TTL:
            return msg
    msg = await tg_client.get_messages(
        settings.telegram_storage_channel_id,
        message_id,
    )
    _msg_cache[message_id] = (now, msg)
    _msg_cache_evict()
    return msg


async def forward_to_storage_channel(message: Message) -> Message:
    return await message.copy(settings.telegram_storage_channel_id)


async def delete_from_storage_channel(message_ids: int | list[int]) -> bool:
    try:
        await tg_client.delete_messages(
            settings.telegram_storage_channel_id,
            message_ids,
        )
        return True
    except Exception:
        return False
