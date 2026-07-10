"""
Custom streaming utilities for Telegram media files.
Multi-client parallel streaming for maximum download speed.
"""
import asyncio
import re
import time
import logging
from typing import AsyncGenerator
from contextlib import asynccontextmanager

BATCH_SIZE = 10  # chunks per stream_media call
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk


def _get_media(message):
    """Get the media object from a message, trying video, document, audio."""
    return message.video or message.document or message.audio


def _get_upload_location(file_id_obj, thumb_size=""):
    """Create InputDocumentFileLocation for the decoded file.
    Pyrogram internally uses InputDocumentFileLocation for all
    non-photo file types (video, audio, document, voice, etc.)."""
    return raw.types.InputDocumentFileLocation(
        id=file_id_obj.media_id,
        access_hash=file_id_obj.access_hash,
        file_reference=file_id_obj.file_reference,
        thumb_size=thumb_size or "",
    )


class ChunkCache:
    """Per-video FIFO cache for already-yielded chunks (backward seek support).
    Key: chunk_idx -> bytes
    Max size: 1GB per video, evicts oldest entries when full.
    """
    def __init__(self, max_bytes: int = 1024 * 1024 * 1024):
        self._data: dict[int, bytes] = {}
        self._order: list[int] = []
        self._size = 0
        self._max = max_bytes
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def get(self, key: int) -> bytes | None:
        data = self._data.get(key)
        if data is not None:
            self._hits += 1
            return data
        self._misses += 1
        return None

    def put(self, key: int, data: bytes):
        if key in self._data or not data:
            return
        self._data[key] = data
        self._order.append(key)
        self._size += len(data)
        while self._size > self._max and self._order:
            old_key = self._order.pop(0)
            old_data = self._data.pop(old_key, None)
            if old_data:
                self._size -= len(old_data)
                self._evictions += 1
                if self._evictions == 1 or self._evictions % 10 == 0:
                    logger.info("Evicted %d chunks (%.1f MB)", self._evictions, self._size / 1024 / 1024)

    def clear(self) -> int:
        freed = self._size
        self._data.clear()
        self._order.clear()
        self._size = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        return freed

    @property
    def info(self) -> dict:
        return {
            "chunks": len(self._data),
            "size_mb": round(self._size / 1024 / 1024, 1),
            "max_mb": round(self._max / 1024 / 1024, 1),
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
        }



class CacheManager:
    """Manages per-video ChunkCache instances.
    Each (chat_id, message_id) pair gets its own 1GB FIFO cache,
    so concurrent streams don't evict each other's backward seek data.
    """
    def __init__(self, per_video_max: int = 1024 * 1024 * 1024):
        self._caches: dict[tuple[int, int], ChunkCache] = {}
        self._per_video_max = per_video_max

    def get_cache(self, chat_id: int, message_id: int) -> ChunkCache:
        key = (chat_id, message_id)
        if key not in self._caches:
            self._caches[key] = ChunkCache(max_bytes=self._per_video_max)
        return self._caches[key]

    def remove(self, chat_id: int, message_id: int):
        key = (chat_id, message_id)
        if key in self._caches:
            self._caches.pop(key).clear()

    def clear_all(self, exclude_keys: set[tuple[int, int]] | None = None) -> int:
        total = 0
        keys_to_clear = [k for k in self._caches if exclude_keys is None or k not in exclude_keys]
        for key in keys_to_clear:
            total += self._caches.pop(key).clear()
        return total

    @property
    def per_video(self) -> list[dict]:
        result = []
        for (chat_id, message_id), cache in self._caches.items():
            info = cache.info
            result.append({
                "chat_id": chat_id,
                "message_id": message_id,
                "chunks": info["chunks"],
                "size_mb": info["size_mb"],
                "max_mb": info["max_mb"],
                "hits": info["hits"],
                "misses": info["misses"],
                "evictions": info["evictions"],
            })
        return sorted(result, key=lambda x: x["size_mb"], reverse=True)

    @property
    def info(self) -> dict:
        total_chunks = 0
        total_size = 0
        total_max = 0
        total_hits = 0
        total_misses = 0
        total_evictions = 0
        for cache in self._caches.values():
            i = cache.info
            total_chunks += i["chunks"]
            total_size += i["size_mb"]
            total_max += i["max_mb"]
            total_hits += i["hits"]
            total_misses += i["misses"]
            total_evictions += i["evictions"]
        return {
            "chunks": total_chunks,
            "size_mb": round(total_size, 1),
            "max_mb": round(total_max, 1),
            "hits": total_hits,
            "misses": total_misses,
            "evictions": total_evictions,
        }


_cache_manager = CacheManager()
_forward_streams: dict[int, dict] = {}

# Disk cache size stub — disk spill removed; always 0
def _dc_disk_size() -> int:
    return 0


# ── Auto-restart when all streams finish ─────────────────────────────
_pending_restart: asyncio.TimerHandle | None = None

def _cancel_restart():
    global _pending_restart
    if _pending_restart is not None:
        _pending_restart.cancel()
        _pending_restart = None

def _do_restart():
    global _pending_restart
    _pending_restart = None
    _forward_streams.clear()
    freed = _cache_manager.clear_all()
    logger.warning("No active streams — cleared %.1f MB from cache", freed / 1024 / 1024)

def _schedule_restart(delay: float = 15.0):
    global _pending_restart
    _cancel_restart()
    loop = asyncio.get_running_loop()
    _pending_restart = loop.call_later(delay, _do_restart)


def get_forward_snapshot() -> list[dict]:
    # Prune stale entries (>30s since last update)
    now = time.monotonic()
    for mid in list(_forward_streams.keys()):
        if now - _forward_streams[mid].get("updated_at", 0) > 8 * 3600:
            _forward_streams.pop(mid, None)
    result = []
    for mid, info in list(_forward_streams.items()):
        futures = info.get("results", {})
        done = sum(1 for f in futures.values() if f.done())
        result.append({
            "message_id": mid,
            "chat_id": info["chat_id"],
            "prebuffer_mb": done,
            "max_mb": info.get("total_chunks", 2000),
        })
    return result


from pyrogram import Client
from pyrogram import raw
from pyrogram.file_id import FileId, FileType
from pyrogram.errors import FileReferenceExpired, FileReferenceInvalid, AuthKeyUnregistered
from pyrogram.session import Session, Auth

from .telegram import clients, reconnect_client
from .config import get_settings

settings = get_settings()

logger = logging.getLogger("streamer")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setLevel(logging.DEBUG)
    _h.setFormatter(logging.Formatter("streamer %(levelname)s: %(message)s"))
    logger.addHandler(_h)
    logger.propagate = False


# Global semaphores to limit concurrency per client across all streams
_client_semaphores = {}

# Limit total concurrent streams to prevent OOM from prebuffers stacking.
# Each stream can hold up to 2000 resolved 1 MB chunks (2 GB) awaiting yield.
# With LIMIT=10, max in-flight = 10 × 2 GB = 20 GB, within 3 GB cgroup with OOM guard.
_stream_semaphore = asyncio.Semaphore(5)
class ClientPoolEmpty(Exception):
    """No connected client available in the pool."""
    pass


class ClientPool:
    """Weighted-least-loaded client assignment pool.

    Tracks active workers, success rate (EMA), and flood-wait cooldown
    per client. Assigns by highest score: connected(+100) - active(x10)
    - remaining_cooldown(x100) - (1-success_rate)(x50).
    """

    def __init__(self, clients: list):
        self._clients = clients
        self._active: dict[int, int] = {}
        self._cooldown: dict[int, float] = {}  # monotonic deadline
        self._success: dict[int, float] = {}  # EMA success rate
        self._lock = asyncio.Lock()

    def _get_active(self, idx: int) -> int:
        return self._active.get(idx, 0)

    def _score(self, idx: int) -> float:
        client = self._clients[idx]
        if not client.is_connected:
            return 0.0
        score = 100.0  # base for being connected
        score -= self._get_active(idx) * 10.0
        remaining = max(0, self._cooldown.get(idx, 0) - time.monotonic())
        score -= remaining * 100.0  # heavy penalty while in cooldown
        score -= (1 - self._success.get(idx, 0.5)) * 50.0
        return max(score, 1.0)  # keep barely-positive so it's available

    async def acquire(self, timeout: float = 30.0):
        """Acquire the best available client. Raises ClientPoolEmpty if none."""
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                best_idx = -1
                best_score = -1.0
                for i in range(len(self._clients)):
                    s = self._score(i)
                    if s > best_score:
                        best_score = s
                        best_idx = i
                if best_idx >= 0 and best_score > 0:
                    self._active[best_idx] = self._get_active(best_idx) + 1
                    return self._clients[best_idx], best_idx
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ClientPoolEmpty("No connected client available")
            await asyncio.sleep(min(0.5, remaining))

    async def release(self, idx: int):
        async with self._lock:
            current = self._get_active(idx)
            if current > 0:
                self._active[idx] = current - 1

    def report_success(self, idx: int):
        rate = self._success.get(idx, 0.5)
        # EMA: alpha=0.3
        self._success[idx] = 0.3 * 1.0 + 0.7 * rate

    def report_failure(self, idx: int, flood_wait: int = 0):
        rate = self._success.get(idx, 0.5)
        self._success[idx] = 0.3 * 0.0 + 0.7 * rate
        if flood_wait > 0:
            deadline = time.monotonic() + min(max(flood_wait * 2, 30), 300)
            existing = self._cooldown.get(idx, 0)
            # Don't shorten existing cooldown
            if existing < deadline:
                self._cooldown[idx] = deadline

    @asynccontextmanager
    async def use_client(self, timeout: float = 30.0):
        client, idx = await self.acquire(timeout)
        try:
            yield client, idx
        finally:
            await self.release(idx)


# Lazy module-level pool instance
_client_pool: ClientPool | None = None

def get_client_pool() -> ClientPool:
    global _client_pool
    if _client_pool is None:
        _client_pool = ClientPool(clients)
    return _client_pool

def get_client_semaphore(client_index: int) -> asyncio.Semaphore:
    if client_index not in _client_semaphores:
        # Use the configured concurrency limit
        _client_semaphores[client_index] = asyncio.Semaphore(settings.telegram_client_concurrency)
    return _client_semaphores[client_index]

# Per-client reconnection lock: prevents concurrent reconnect racing with in-flight RPCs
_client_reconnect_locks: dict[int, asyncio.Lock] = {}

def get_client_reconnect_lock(client_index: int) -> asyncio.Lock:
    if client_index not in _client_reconnect_locks:
        _client_reconnect_locks[client_index] = asyncio.Lock()
    return _client_reconnect_locks[client_index]


# ── Chunk fetch helpers ────────────────────────────────────────────────────────

# ── Byte-accurate stream (GDrive) ───────────────────────────────────────────────

async def _byte_accurate_file_stream(client, message, file_size: int, offset_start: int, offset_end: int):
    """Download byte range using direct upload.GetFile with correct byte-level offsets.
    Yields (byte_offset, chunk_data) tuples. Non-CDN files only.
    """
    media = _get_media(message)
    if not media:
        raise ValueError("Message has no streamable media")
    file_id_obj = FileId.decode(media.file_id)
    location = _get_upload_location(file_id_obj)
    dc_id = file_id_obj.dc_id

    session = client.media_sessions.get(dc_id)
    if not session:
        session = Session(
            client, dc_id,
            await Auth(client, dc_id, await client.storage.test_mode()).create()
            if dc_id != await client.storage.dc_id()
            else await client.storage.auth_key(),
            await client.storage.test_mode(),
            is_media=True,
        )
        await session.start()
        if dc_id != await client.storage.dc_id():
            for _ in range(3):
                exported = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                try:
                    await session.invoke(
                        raw.functions.auth.ImportAuthorization(
                            id=exported.id, bytes=exported.bytes
                        )
                    )
                except (AuthKeyUnregistered, Exception) as _e:
                    if isinstance(_e, AuthKeyUnregistered) or "AUTH_BYTES_INVALID" in str(_e):
                        continue
                    raise
                else:
                    break
            else:
                raise AuthKeyUnregistered("Could not export auth to file DC")
        client.media_sessions[dc_id] = session

    MAX_CHUNK = 1024 * 1024
    pos = offset_start
    while pos < offset_end:
        try:
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=pos, limit=MAX_CHUNK, precise=True,
                ),
                sleep_threshold=client.sleep_threshold,
            )
        except (FileReferenceExpired, FileReferenceInvalid):
            refreshed = await client.get_messages(message.chat.id, message.id)
            refreshed_media = _get_media(refreshed) if refreshed else None
            if not refreshed_media:
                break
            file_id_obj = FileId.decode(refreshed_media.file_id)
            location = _get_upload_location(file_id_obj)
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=pos, limit=MAX_CHUNK, precise=True,
                ),
                sleep_threshold=client.sleep_threshold,
            )
        except Exception as _e:
            if "AUTH_KEY_UNREGISTERED" in str(_e) or "LIMIT_INVALID" in str(_e):
                client.media_sessions.pop(dc_id, None)
                logger.warning("Evicted stale session for DC %d (%s)", dc_id, str(_e)[:50])
            raise

        if isinstance(r, raw.types.upload.File):
            chunk = r.bytes
            if not chunk:
                break
            if pos + len(chunk) > offset_end:
                chunk = chunk[:offset_end - pos]
            yield pos, chunk
            pos += len(chunk)
        elif isinstance(r, raw.types.upload.FileCdnRedirect):
            raise NotImplementedError("CDN redirect not supported in byte-accurate stream")
        else:
            break


# ── Prefetch ───────────────────────────────────────────────────────────────────

async def prefetch_first_batch(client, message, from_bytes: int = 0):
    """Fire-and-forget: start caching the first batch before the generator runs."""
    media = _get_media(message) if message else None
    if not media:
        return
    file_size = media.file_size
    if from_bytes >= file_size:
        return
    chat_id = message.chat.id
    message_id = message.id
    CHUNK_SIZE = 1024 * 1024
    start_chunk = from_bytes // CHUNK_SIZE
    cache = _cache_manager.get_cache(chat_id, message_id)
    if cache.get(start_chunk) is not None:
        return
    try:
        prefetch_client = next((c for c in clients if c.is_connected), None)
        if not prefetch_client:
            prefetch_client = client
        c_idx = getattr(prefetch_client, "pool_index", 0)
        sem = get_client_semaphore(c_idx)
        msg = await prefetch_client.get_messages(chat_id, message_id)
        if not msg:
            return
        async with sem:
            async for part in prefetch_client.stream_media(msg, limit=BATCH_SIZE, offset=start_chunk):
                data = bytes(part)
                cache.put(start_chunk, data)
                start_chunk += 1
    except Exception:
        pass  # best-effort


# ── Main streaming generator ───────────────────────────────────────────────────

async def parallel_stream_generator(
    initial_message,
    offset: int,
    length: int,
    chunk_size: int = 1024 * 1024,
    concurrency: int = None,
):
    """
    Fetch file chunks in parallel using the client pool.
    Each worker uses its own client and fetches its own Message object
    to avoid cross-bot FILE_REFERENCE_INVALID errors.
    """
    pool_size = len(clients)
    if concurrency is None:
        concurrency = max(1, sum(1 for c in clients if c.is_connected))

    start_chunk = offset // chunk_size
    end_chunk = (offset + length - 1) // chunk_size
    total_chunks = end_chunk - start_chunk + 1

    chat_id = initial_message.chat.id
    message_id = initial_message.id

    # Pre-create Futures for ordered yielding
    loop = asyncio.get_running_loop()
    results = {
        (start_chunk + i): loop.create_future()
        for i in range(total_chunks)
    }

    # Cancel any pending auto-restart — a new stream just started
    _cancel_restart()

    # Register forward stream for monitor (done futures = prebuffer depth)
    _forward_streams[message_id] = {"chat_id": chat_id, "results": results, "total_chunks": total_chunks, "updated_at": time.monotonic()}

    # Check backward cache — pre-set futures for already-cached chunks
    video_cache = _cache_manager.get_cache(chat_id, message_id)
    cache_hits = 0
    uncached_ranges: list[tuple[int, int]] = []
    range_start = None
    for chunk_idx in range(start_chunk, end_chunk + 1):
        cached = video_cache.get(chunk_idx)
        if cached is not None:
            results[chunk_idx].set_result(cached)
            cache_hits += 1
            if range_start is not None:
                uncached_ranges.append((range_start, chunk_idx - 1))
                range_start = None
        else:
            if range_start is None:
                range_start = chunk_idx
    if range_start is not None:
        uncached_ranges.append((range_start, end_chunk))

    if cache_hits:
        logger.info("%d/%d cached (%d ranges)", cache_hits, total_chunks, len(uncached_ranges))
    else:
        logger.debug("No cache: fetching %d", total_chunks)

    # Task queue with batch ranges — only uncached chunks
    task_queue = asyncio.Queue()
    for rstart, rend in uncached_ranges:
        for batch_start in range(rstart, rend + 1, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE - 1, rend)
            task_queue.put_nowait((batch_start, batch_end))

    # ── Shared CDN session setup ─────────────────────────────────────────
    # One session, one TCP connection to Telegram's CDN, shared by all workers.
    # Eliminates per-bot API calls and CDN connection contention.
    _cdn_session = None
    _cdn_location = None
    _cdn_client_ref = None
    _refresh_lock = asyncio.Lock()

    media = _get_media(initial_message)
    if media:
        try:
            cdn_file_id = FileId.decode(media.file_id)
            cdn_dc_id = cdn_file_id.dc_id
            _cdn_location = _get_upload_location(cdn_file_id)
            _cdn_client_ref = next((c for c in clients if c.is_connected), clients[0])
            sess = _cdn_client_ref.media_sessions.get(cdn_dc_id)
            if not sess:
                sess = Session(
                    _cdn_client_ref, cdn_dc_id,
                    await Auth(_cdn_client_ref, cdn_dc_id, await _cdn_client_ref.storage.test_mode()).create()
                    if cdn_dc_id != await _cdn_client_ref.storage.dc_id()
                    else await _cdn_client_ref.storage.auth_key(),
                    await _cdn_client_ref.storage.test_mode(), is_media=True,
                )
                await sess.start()
                if cdn_dc_id != await _cdn_client_ref.storage.dc_id():
                    for _ in range(3):
                        exported = await _cdn_client_ref.invoke(
                            raw.functions.auth.ExportAuthorization(dc_id=cdn_dc_id))
                        try:
                            await sess.invoke(
                                raw.functions.auth.ImportAuthorization(id=exported.id, bytes=exported.bytes))
                            break
                        except Exception:
                            continue
                _cdn_client_ref.media_sessions[cdn_dc_id] = sess
            _cdn_session = sess
        except Exception as e:
            logger.warning("CDN session setup failed: %s, using per-client streaming", e)

    async def _fetch_batch_cdn(batch_start, batch_end):
        """Fetch batch via shared CDN session. Falls back to stream_media on error."""
        nonlocal _cdn_session, _cdn_location
        t0 = time.perf_counter()
        current = batch_start
        try:
            for chunk_offset in range(batch_start, batch_end + 1):
                offset = chunk_offset * CHUNK_SIZE
                try:
                    r = await _cdn_session.invoke(
                        raw.functions.upload.GetFile(
                            location=_cdn_location, offset=offset,
                            limit=CHUNK_SIZE, precise=True,
                        ),
                        sleep_threshold=getattr(_cdn_client_ref, 'sleep_threshold', 60),
                    )
                except (FileReferenceExpired, FileReferenceInvalid):
                    async with _refresh_lock:
                        refreshed = await _cdn_client_ref.get_messages(chat_id, message_id)
                        if refreshed:
                            refreshed_media = _get_media(refreshed)
                            if refreshed_media:
                                fid = FileId.decode(refreshed_media.file_id)
                                _cdn_location = _get_upload_location(fid)
                    r = await _cdn_session.invoke(
                        raw.functions.upload.GetFile(
                            location=_cdn_location, offset=offset,
                            limit=CHUNK_SIZE, precise=True,
                        ),
                        sleep_threshold=getattr(_cdn_client_ref, 'sleep_threshold', 60),
                    )

                if isinstance(r, raw.types.upload.File):
                    data = bytes(r.bytes)
                elif isinstance(r, raw.types.upload.FileCdnRedirect):
                    logger.info("CDN redirect for chunk %d, falling back to stream_media", chunk_offset)
                    return None  # signal fallback
                else:
                    break
                if not data:
                    break
                video_cache.put(current, data)
                if not results[current].done():
                    results[current].set_result(data)
                await _backpressure.acquire()
                current += 1
        except (ConnectionError, OSError, TimeoutError, Exception) as e:
            logger.warning("CDN session error: %s, falling back to stream_media", e)
            return None

        elapsed = time.perf_counter() - t0
        if elapsed > 2.5:
            logger.warning("Slow batch %d-%d: %.1fs (cdn)", batch_start, batch_end, elapsed)
        return current - 1 == batch_end

    async def _fetch_batch(batch_start, batch_end, cl, msg, sem):
        """Fetch a batch, assigning each chunk as it arrives.
        Forward-caches each chunk immediately so concurrent streams
        of the same file benefit before the yield loop.
        Acquires a backpressure permit per chunk to cap in-flight
        resolved-but-unyielded data — prevents OOM for huge files."""
        t0 = time.perf_counter()
        current = batch_start
        async with sem:
            async for part in cl.stream_media(msg, limit=batch_end - batch_start + 1, offset=batch_start):
                data = bytes(part)
                video_cache.put(current, data)
                if not results[current].done():
                    results[current].set_result(data)
                # Backpressure: wait until yield loop frees a slot
                await _backpressure.acquire()
                current += 1
        elapsed = time.perf_counter() - t0
        if elapsed > 2.5:
            logger.warning("Slow batch %d-%d: %.1fs (bot %d)", batch_start, batch_end, elapsed, getattr(cl, 'pool_index', '?'))
        return current - 1 == batch_end

    async def _fetch_one(chunk_offset, cl, msg, sem):
        """Fetch a single chunk, forward-caching it on success."""
        try:
            async with sem:
                d = bytearray()
                async for part in cl.stream_media(msg, limit=1, offset=chunk_offset):
                    d.extend(part)
            data = bytes(d)
            video_cache.put(chunk_offset, data)
            return data
        except (FileReferenceInvalid, FileReferenceExpired, AuthKeyUnregistered):
            raise
        except Exception:
            return None

    async def worker(worker_id: int):
        pool = get_client_pool()
        try:
            async with pool.use_client() as (client, c_idx):
                # Each worker normally fetches its own fresh Message so file references
                # are per-client and valid. Bot 0 gets the already-fetched initial_message
                # to save ~1s round-trip on first chunk.
                if c_idx == 0:
                    local_msg = initial_message
                else:
                    try:
                        local_msg = await client.get_messages(chat_id, message_id)
                    except Exception as e:
                        logger.error("Bot %d: failed to fetch message %d: %s", c_idx, message_id, e)
                        return
                if not local_msg:
                    logger.error("Bot %d: message %d not found, emitting empty chunks", c_idx, message_id)
                    while not task_queue.empty():
                        try:
                            batch_start, batch_end = task_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        for chunk_offset in range(batch_start, batch_end + 1):
                            if not results[chunk_offset].done():
                                results[chunk_offset].set_result(b"")
                            task_queue.task_done()
                    return

                # Get semaphore for this client to ensure we don't exceed max_concurrent_transmissions
                # This prevents the "Request refused" or internal queue buildup in Pyrogram
                semaphore = get_client_semaphore(c_idx)

                while not task_queue.empty():
                    try:
                        batch_start, batch_end = task_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                    batch_ok = False
                    try:
                        if _cdn_session is not None:
                            batch_ok = await _fetch_batch_cdn(batch_start, batch_end)
                            if batch_ok is None:
                                batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                        else:
                            batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                    except (FileReferenceInvalid, FileReferenceExpired):
                        logger.warning("Bot %d: batch file reference expired, re-fetching message", c_idx)
                        try:
                            local_msg = await client.get_messages(chat_id, message_id)
                            batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                        except Exception:
                            pass
                    except AuthKeyUnregistered:
                        logger.warning("Bot %d: auth key expired in batch, reconnecting...", c_idx)
                        async with get_client_reconnect_lock(c_idx):
                            if await reconnect_client(client):
                                try:
                                    local_msg = await client.get_messages(chat_id, message_id)
                                    batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.error("Bot %d failed batch %d-%d: %s", c_idx, batch_start, batch_end, e)

                    if batch_ok:
                        task_queue.task_done()
                        continue

                    # Fallback: fetch each chunk individually
                    for chunk_offset in range(batch_start, batch_end + 1):
                        try:
                            chunk_data = await _fetch_one(chunk_offset, client, local_msg, semaphore)
                            if chunk_data is not None:
                                if not results[chunk_offset].done():
                                    results[chunk_offset].set_result(chunk_data)
                                continue
                        except (FileReferenceInvalid, FileReferenceExpired):
                            logger.warning("Bot %d: file reference expired for chunk %d", c_idx, chunk_offset)
                            try:
                                local_msg = await client.get_messages(chat_id, message_id)
                                async with semaphore:
                                    d = bytearray()
                                    async for part in client.stream_media(local_msg, limit=1, offset=chunk_offset):
                                        d.extend(part)
                                data = bytes(d)
                                video_cache.put(chunk_offset, data)
                                if not results[chunk_offset].done():
                                    results[chunk_offset].set_result(data)
                                continue
                            except Exception as e2:
                                logger.error("Bot %d failed chunk %d after re-fetch: %s", c_idx, chunk_offset, e2)
                        except AuthKeyUnregistered:
                            logger.warning("Bot %d: auth key expired for chunk %d", c_idx, chunk_offset)
                            async with get_client_reconnect_lock(c_idx):
                                if await reconnect_client(client):
                                    try:
                                        local_msg = await client.get_messages(chat_id, message_id)
                                        async with semaphore:
                                            d = bytearray()
                                            async for part in client.stream_media(local_msg, limit=1, offset=chunk_offset):
                                                d.extend(part)
                                        data = bytes(d)
                                        video_cache.put(chunk_offset, data)
                                        if not results[chunk_offset].done():
                                            results[chunk_offset].set_result(data)
                                        continue
                                    except Exception as e2:
                                        logger.error("Bot %d failed chunk %d after reconnect: %s", c_idx, chunk_offset, e2)
                                else:
                                    logger.error("Bot %d: reconnect failed for chunk %d", c_idx, chunk_offset)
                        except Exception as e:
                            logger.error("Bot %d failed chunk %d: %s", c_idx, chunk_offset, e)
                        task_queue.task_done()
                pool.report_success(c_idx)
        except ClientPoolEmpty:
            logger.error("Worker %d: no connected client available", worker_id)
        except asyncio.TimeoutError:
            logger.error("Worker %d: timed out waiting for client", worker_id)

    # ── Backpressure: cap in-flight resolved chunks ─────────────────
    # At most WINDOW_CHUNKS chunks can be resolved but not yet yielded.
    # 300 MB window + 200 MB cache = 500 MB peak per stream.
    # 5 concurrent: 5 × 500 = 2.5 GB (16% of 16 GB).
    WINDOW_CHUNKS = 300
    _backpressure = asyncio.Semaphore(WINDOW_CHUNKS)

    # Launch workers
    worker_tasks = [
        asyncio.create_task(worker(i)) for i in range(concurrency)
    ]

    # ── Yield smoothing: prebuffer before yielding ─────────────────────
    # Wait for MIN_PREBUFFER chunks before the first yield so workers
    # build a pipeline ahead of the HTTP response stream. This absorbs
    # batch-to-batch jitter (flood waits, slow DC) without client-side
    # rebuffering. Set low (5) to avoid 10s TTFB from Telegram DC init;
    # 5 chunks arrive progressively within the first batch fetch (~3s).
    MIN_PREBUFFER = 5
    prebuffer_n = min(MIN_PREBUFFER, total_chunks)
    if prebuffer_n > 1:
        prebuffer_futs = [
            results[start_chunk + i] for i in range(prebuffer_n)
        ]
        await asyncio.gather(*prebuffer_futs)
        logger.info("Prebuffered %d / %d chunks (%.1f MB)",
                    prebuffer_n, total_chunks,
                    prebuffer_n * chunk_size / 1024 / 1024)
    elif prebuffer_n == 1:
        await results[start_chunk]

    stream_start = time.perf_counter()
    first_chunk_logged = False
    cache_served = 0
    bytes_yielded = 0
    try:
        for offset in range(total_chunks):
            chunk_idx = start_chunk + offset

            # Try cache first (backward seek), fall back to fetch result
            # After prebuffer, the first MIN_PREBUFFER chunks are already resolved.
            cached_data = video_cache.get(chunk_idx)
            if cached_data is not None:
                chunk_data = cached_data
                cache_served += 1
            else:
                chunk_data = await results[chunk_idx]
                video_cache.put(chunk_idx, chunk_data)

            bytes_yielded += len(chunk_data)
            if not first_chunk_logged:
                elapsed = time.perf_counter() - stream_start
                logger.info("Chunk %d in %.1fs (cached=%s)", chunk_idx, elapsed, cached_data is not None)
                first_chunk_logged = True
            yield chunk_data
            # Release backpressure permit — next in-flight chunk may resolve
            _backpressure.release()
            # Refresh forward stream timestamp every 100 chunks
            if offset % 100 == 0:
                if message_id in _forward_streams:
                    _forward_streams[message_id]["updated_at"] = time.monotonic()
            del results[chunk_idx]
    finally:
        # Cancel workers first, then clear results (avoids KeyError race)
        for w in worker_tasks:
            w.cancel()
        results.clear()
        _forward_streams.pop(message_id, None)
        # Schedule restart when no streams remain — frees page cache
        if not _forward_streams:
            _schedule_restart()
        # Cache kept alive across seek requests — OOM guard in main.py handles eviction
        elapsed = time.perf_counter() - stream_start
        cinfo = video_cache.info
        logger.info("Done: %d ch, %.1f MB, %.1fs", total_chunks, bytes_yielded / 1024 / 1024, elapsed)
        logger.info("Cache hits/evicts: %d/%d", cinfo["hits"], cinfo["evictions"])


async def stream_file(
    client: Client,          # kept for API compat; pool is used instead
    message,
    from_bytes: int,
    until_bytes: int,
) -> AsyncGenerator[bytes, None]:
    """Stream a file range using the multi-client pool.
    Limits concurrent streams to prevent OOM from prebuffers stacking.
    """
    CHUNK_SIZE = 1024 * 1024

    total_bytes_needed = until_bytes - from_bytes + 1
    bytes_yielded = 0
    bytes_to_skip = from_bytes % CHUNK_SIZE

    t0 = time.perf_counter()
    logger.debug("Streaming %d-%d (%d bytes)", from_bytes, until_bytes, total_bytes_needed)

    await _stream_semaphore.acquire()
    try:
        async for chunk in parallel_stream_generator(
            message, from_bytes, total_bytes_needed
        ):
            if bytes_to_skip > 0:
                chunk = chunk[bytes_to_skip:]
                bytes_to_skip = 0

            remaining = total_bytes_needed - bytes_yielded
            if len(chunk) > remaining:
                chunk = chunk[:remaining]

            yield chunk
            bytes_yielded += len(chunk)
            if bytes_yielded >= total_bytes_needed:
                break
    finally:
        _stream_semaphore.release()

    elapsed = time.perf_counter() - t0
    logger.info("stream_file %d-%d done: %.1f MB in %.1fs (%.1f Mbps)",
                from_bytes, until_bytes, bytes_yielded / 1024 / 1024, elapsed,
                bytes_yielded * 8 / elapsed / 1024 / 1024 if elapsed > 0 else 0)
