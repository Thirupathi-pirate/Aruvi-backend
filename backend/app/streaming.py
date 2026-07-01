"""
Custom streaming utilities for Telegram media files.
Multi-client parallel streaming for maximum download speed.
"""
import asyncio
import os
import re

import time
import logging
from typing import AsyncGenerator
from pathlib import Path

BATCH_SIZE = 5  # 5MB per batch (5 × 1MB chunks)
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk
MIN_CHUNK_SIZE = 100 * 1024  # 100KB — smaller chunks trigger retry with fresh file ref
DISK_CACHE_BASE = "data/chunks"
DISK_CACHE_TTL = 3 * 3600  # 3 hours
DISK_CACHE_MAX = 13 * 1024 * 1024 * 1024  # 13GB max

# Per-stream sliding window: 300MB forward + 100MB backward in RAM
# Global RAM limit: 500MB across ALL streams (enforced by CacheManager)
# Excess spills to NVMe disk (3h TTL)
FWD_WINDOW = 300  # 300MB forward cache per stream
BACK_WINDOW = 100  # 100MB backward cache per stream


def _dc_path(chat_id: int, message_id: int, chunk_idx: int) -> str:
    return os.path.join(DISK_CACHE_BASE, str(chat_id), str(message_id), str(chunk_idx))


def _dc_get(chat_id: int, message_id: int, chunk_idx: int) -> bytes | None:
    p = _dc_path(chat_id, message_id, chunk_idx)
    try:
        return Path(p).read_bytes()
    except (FileNotFoundError, IsADirectoryError, PermissionError):
        return None


def _dc_put(chat_id: int, message_id: int, chunk_idx: int, data: bytes):
    if not data:
        return
    p = _dc_path(chat_id, message_id, chunk_idx)
    try:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        Path(p).write_bytes(data)
        logger.debug("DCACHE PUT %s:%d chunk %d (%d bytes)", chat_id, message_id, chunk_idx, len(data))
    except OSError as e:
        logger.error("Disk cache write failed for %s: %s", p, e)


def _dc_cleanup_old():
    if not os.path.isdir(DISK_CACHE_BASE):
        return
    now = time.time()
    all_files: list[str] = []
    # Pass 1: delete individual files older than TTL, collect survivors for size enforcement
    for cid in os.listdir(DISK_CACHE_BASE):
        cpath = os.path.join(DISK_CACHE_BASE, cid)
        if not os.path.isdir(cpath):
            continue
        for mid in os.listdir(cpath):
            mdpath = os.path.join(cpath, mid)
            if not os.path.isdir(mdpath):
                continue
            for f in os.listdir(mdpath):
                fp = os.path.join(mdpath, f)
                if not os.path.isfile(fp):
                    continue
                if now - os.path.getmtime(fp) > DISK_CACHE_TTL:
                    try:
                        os.remove(fp)
                    except OSError:
                        pass
                else:
                    all_files.append(fp)
    # Pass 2: enforce 13GB limit on survivor files
    total = _dc_disk_size()
    if total > DISK_CACHE_MAX:
        all_files.sort(key=lambda fp: os.path.getmtime(fp))
        to_free = total - int(DISK_CACHE_MAX * 0.9)
        for fp in all_files:
            if to_free <= 0:
                break
            if not os.path.isfile(fp):
                continue
            try:
                sz = os.path.getsize(fp)
                os.remove(fp)
                to_free -= sz
            except OSError:
                pass
    # Pass 3: sweep empty directories
    for cid in os.listdir(DISK_CACHE_BASE):
        cpath = os.path.join(DISK_CACHE_BASE, cid)
        if not os.path.isdir(cpath):
            continue
        for mid in os.listdir(cpath):
            mdpath = os.path.join(cpath, mid)
            if os.path.isdir(mdpath) and not os.listdir(mdpath):
                try:
                    os.rmdir(mdpath)
                except OSError:
                    pass


_dc_disk_cache: tuple[float, int] = (0.0, 0)
_DC_DISK_TTL = 5.0  # seconds

def _dc_disk_size() -> int:
    global _dc_disk_cache
    now = time.monotonic()
    if now - _dc_disk_cache[0] < _DC_DISK_TTL:
        return _dc_disk_cache[1]
    total = 0
    if not os.path.isdir(DISK_CACHE_BASE):
        return 0
    for cid in os.listdir(DISK_CACHE_BASE):
        cpath = os.path.join(DISK_CACHE_BASE, cid)
        if not os.path.isdir(cpath):
            continue
        for mid in os.listdir(cpath):
            dpath = os.path.join(cpath, mid)
            if not os.path.isdir(dpath):
                continue
            for f in os.listdir(dpath):
                fp = os.path.join(dpath, f)
                if os.path.isfile(fp):
                    total += os.path.getsize(fp)
    _dc_disk_cache = (now, total)
    return total


class _NullCache:
    """No-op cache — used when parallel_stream_generator(cache=False).
    All methods are no-ops; get() always returns None (cache miss).
    Position is tracked for refill backpressure.
    """
    def __init__(self):
        self.position = 0
    def store(self, *args, **kwargs): pass
    def get(self, key): return None
    def set_position(self, pos): self.position = pos
    @property
    def info(self): return {"chunks": 0, "size_mb": 0, "hits": 0, "misses": 0, "evictions": 0}


class StreamCache:
    """Position-aware sliding window cache for one video stream.
    Chunks within [-BACK_WINDOW, +FWD_WINDOW] of current playback position
    stay in RAM. Older/farther chunks are spilled to NVMe.
    RAM is tracked globally via CacheManager (500MB across ALL streams).
    """
    def __init__(self, chat_id: int, message_id: int):
        self.chat_id = chat_id
        self.message_id = message_id
        self.position = 0
        self._data: dict[int, bytes] = {}
        self._size = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def set_position(self, pos: int):
        self.position = pos
        self._trim()

    def _trim(self):
        to_remove = [idx for idx in self._data if idx < self.position - BACK_WINDOW or idx > self.position + FWD_WINDOW]
        for idx in to_remove:
            data = self._data.pop(idx)
            self._size -= len(data)
            _cache_manager._remove_ram(len(data))
            _dc_put(self.chat_id, self.message_id, idx, data)
            self._evictions += 1

    def get(self, key: int) -> bytes | None:
        data = self._data.get(key)
        if data is not None:
            self._hits += 1
            return data
        d = _dc_get(self.chat_id, self.message_id, key)
        if d is not None:
            self._hits += 1
            dist = key - self.position
            if -BACK_WINDOW <= dist <= FWD_WINDOW:
                self._data[key] = d
                self._size += len(d)
                _cache_manager._add_ram(len(d))
            return d
        self._misses += 1
        return None

    def store(self, key: int, data: bytes):
        if not data:
            return
        dist = key - self.position
        if -BACK_WINDOW <= dist <= FWD_WINDOW:
            if key not in self._data:
                self._data[key] = data
                self._size += len(data)
                _cache_manager._add_ram(len(data))
        else:
            _dc_put(self.chat_id, self.message_id, key, data)

    def clear(self) -> int:
        freed = self._size
        for key, data in list(self._data.items()):
            _cache_manager._remove_ram(len(data))
        self._data.clear()
        self._size = 0
        return freed

    @property
    def info(self) -> dict:
        return {
            "chunks": len(self._data),
            "size_mb": round(self._size / 1024 / 1024, 1),
            "hits": self._hits,
            "misses": self._misses,
            "evictions": self._evictions,
        }


class CacheManager:
    def __init__(self):
        self._caches: dict[tuple[int, int], StreamCache] = {}
        self.ram_limit = 500 * 1024 * 1024  # 500MB global RAM limit
        self._total_ram = 0

    def _add_ram(self, delta: int):
        self._total_ram += delta
        while self._total_ram > self.ram_limit:
            if not self._evict_one():
                break

    def _remove_ram(self, delta: int):
        self._total_ram = max(0, self._total_ram - delta)

    def _evict_one(self) -> bool:
        if not self._caches:
            return False
        active = [c for c in self._caches.values() if c._size > 0]
        if not active:
            return False
        target = max(active, key=lambda c: c._size)
        if not target._data:
            self._total_ram = max(0, self._total_ram - target._size)
            target._size = 0
            return True
        farthest = max(target._data.keys(), key=lambda k: abs(k - target.position))
        data = target._data.pop(farthest)
        target._size -= len(data)
        self._total_ram -= len(data)
        _dc_put(target.chat_id, target.message_id, farthest, data)
        target._evictions += 1
        return True

    def get_cache(self, chat_id: int, message_id: int) -> StreamCache:
        key = (chat_id, message_id)
        if key not in self._caches:
            self._caches[key] = StreamCache(chat_id, message_id)
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
                "hits": info["hits"],
                "misses": info["misses"],
                "evictions": info["evictions"],
            })
        return sorted(result, key=lambda x: x["size_mb"], reverse=True)

    @property
    def info(self) -> dict:
        total_chunks = total_size = total_hits = total_misses = total_evictions = 0
        for cache in self._caches.values():
            i = cache.info
            total_chunks += i["chunks"]
            total_size += i["size_mb"]
            total_hits += i["hits"]
            total_misses += i["misses"]
            total_evictions += i["evictions"]
        return {
            "chunks": total_chunks,
            "size_mb": round(total_size, 1),
            "hits": total_hits,
            "misses": total_misses,
            "evictions": total_evictions,
        }


_cache_manager = CacheManager()
_forward_streams: dict[int, dict] = {}


def get_forward_snapshot() -> list[dict]:
    # Prune stale entries (>8h since last update)
    now = time.monotonic()
    for mid in list(_forward_streams.keys()):
        entry = _forward_streams.get(mid)
        if entry and now - entry.get("updated_at", 0) > 8 * 3600:
            _forward_streams.pop(mid, None)
    result = []
    for mid, info in list(_forward_streams.items()):
        futures = info.get("results", {})
        done = sum(1 for f in list(futures.values()) if f.done())
        result.append({
            "message_id": mid,
            "chat_id": info["chat_id"],
            "prebuffer_mb": done,
            "max_mb": info.get("total_chunks", 2000),
        })
    return result


from pyrogram import Client
from pyrogram import raw
from pyrogram.file_id import FileId
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
# Global RAM limit: 500MB across ALL streams (enforced by CacheManager).
# Each stream holds up to 300fwd + 100bwd within position window, but
# the 500MB global cap means evictions start well before 5 streams × 400MB.
_stream_semaphore = asyncio.Semaphore(5)

def get_client_semaphore(client_index: int) -> asyncio.Semaphore:
    if client_index not in _client_semaphores:
        # Use the configured concurrency limit
        _client_semaphores[client_index] = asyncio.Semaphore(settings.telegram_client_concurrency)
    return _client_semaphores[client_index]


async def prefetch_first_batch(client, message, from_bytes: int = 0):
    """Fire-and-forget: start caching the first batch before the generator runs.
    Uses a helper bot (not bot 0) for faster first-byte latency.
    Skips if already cached or if the message has no document."""
    if not message or not message.document:
        return
    file_size = message.document.file_size
    if from_bytes >= file_size:
        return
    chat_id = message.chat.id
    message_id = message.id
    start_chunk = from_bytes // CHUNK_SIZE
    cache = _cache_manager.get_cache(chat_id, message_id)
    if cache.get(start_chunk) is not None:
        return  # already cached
    try:
        helper = next((c for c in clients if getattr(c, 'pool_index', 0) != 0 and c.is_connected), None)
        if not helper:
            helper = client
        c_idx = getattr(helper, "pool_index", 0)
        sem = get_client_semaphore(c_idx)
        msg = await helper.get_messages(chat_id, message_id)
        if not msg:
            return
        async with sem:
            async for part in helper.stream_media(msg, limit=BATCH_SIZE, offset=start_chunk):
                data = bytes(part)
                cache.store(start_chunk, data)
                start_chunk += 1
    except Exception:
        pass  # best-effort


async def _retry_chunk_with_alt_client(
    failed_c_idx: int,
    chunk_idx: int,
    chat_id: int,
    message_id: int,
    results: dict,
):
    """Try fetching the chunk with a different client before giving up.
    
    Retries up to 5 full cycles with exponential backoff (2^cycle).
    Handles flood wait, auth expiry, and transient network errors.
    Only yields empty bytes as absolute last resort.
    Skips bot 0 (main bot) — slow at scraping.
    """
    pool_size = len(clients)
    max_cycles = 5
    for cycle in range(max_cycles):
        for offset in range(1, pool_size):
            alt_c_idx = (failed_c_idx + offset) % pool_size
            if alt_c_idx == failed_c_idx or alt_c_idx == 0:
                continue
            alt_client = clients[alt_c_idx]
            try:
                alt_msg = await alt_client.get_messages(chat_id, message_id)
                if not alt_msg:
                    continue
                async with get_client_semaphore(alt_c_idx):
                    data = bytearray()
                    async for part in alt_client.stream_media(
                        alt_msg, limit=1, offset=chunk_idx
                    ):
                        data.extend(part)
                chunk_bytes = bytes(data)
                if chunk_bytes and not results[chunk_idx].done():
                    results[chunk_idx].set_result(chunk_bytes)
                    return
            except AuthKeyUnregistered:
                logger.warning("Alt bot %d: auth key expired, reconnecting...", alt_c_idx)
                if await reconnect_client(alt_client):
                    try:
                        alt_msg = await alt_client.get_messages(chat_id, message_id)
                        if alt_msg:
                            async with get_client_semaphore(alt_c_idx):
                                data = bytearray()
                                async for part in alt_client.stream_media(
                                    alt_msg, limit=1, offset=chunk_idx
                                ):
                                    data.extend(part)
                            chunk_bytes = bytes(data)
                            if chunk_bytes and not results[chunk_idx].done():
                                results[chunk_idx].set_result(chunk_bytes)
                                return
                    except Exception:
                        pass
            except Exception as e:
                err_str = str(e).lower()
                if "flood" in err_str:
                    match = re.search(r"(\d+)", err_str)
                    wait = min(int(match.group(1)) if match else 60, 120)
                    logger.warning("Alt bot %d: flood wait %ds", alt_c_idx, wait)
                    await asyncio.sleep(wait)
                    continue
        if cycle < max_cycles - 1:
            delay = 2 ** cycle
            logger.debug("Retry cycle %d: sleeping %ds before next attempt", cycle, delay)
            await asyncio.sleep(delay)
    if not results[chunk_idx].done():
        logger.error("All retries exhausted for chunk %d — inserting empty bytes", chunk_idx)
        results[chunk_idx].set_result(b"")


async def _byte_accurate_file_stream(client, message, file_size: int, offset_start: int, offset_end: int):
    """Download byte range using direct upload.GetFile with correct byte-level offsets.
    
    Fixes Pyrogram's bug where offset advances by 1MB regardless of actual bytes returned.
    Yields (byte_offset, chunk_data) tuples. Non-CDN files only — raises on CDN redirect.
    """
    file_id_obj = FileId.decode(message.document.file_id)
    location = raw.types.InputDocumentFileLocation(
        id=file_id_obj.media_id,
        access_hash=file_id_obj.access_hash,
        file_reference=file_id_obj.file_reference,
        thumb_size=file_id_obj.thumbnail_size or "",
    )
    dc_id = file_id_obj.dc_id

    session = client.media_sessions.get(dc_id)
    if not session:
        session = client.media_sessions[dc_id] = Session(
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
                except AuthKeyUnregistered:
                    continue
                else:
                    break
            else:
                raise AuthKeyUnregistered("Could not export auth to file DC")

    MAX_CHUNK = 1024 * 1024
    pos = offset_start
    cache_key = None
    while pos < offset_end:
        remaining = min(MAX_CHUNK, offset_end - pos)
        try:
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=pos, limit=remaining,
                ),
                sleep_threshold=client.sleep_threshold,
            )
        except (FileReferenceExpired, FileReferenceInvalid):
            refreshed = await client.get_messages(message.chat.id, message.id)
            if not refreshed or not refreshed.document:
                break
            file_id_obj = FileId.decode(refreshed.document.file_id)
            location = raw.types.InputDocumentFileLocation(
                id=file_id_obj.media_id,
                access_hash=file_id_obj.access_hash,
                file_reference=file_id_obj.file_reference,
                thumb_size=file_id_obj.thumbnail_size or "",
            )
            r = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=pos, limit=remaining,
                ),
                sleep_threshold=client.sleep_threshold,
            )

        if isinstance(r, raw.types.upload.File):
            chunk = r.bytes
            if not chunk:
                break
            yield pos, chunk
            pos += len(chunk)
        elif isinstance(r, raw.types.upload.FileCdnRedirect):
            raise NotImplementedError("CDN redirect not supported in byte-accurate stream")
        else:
            break


async def parallel_stream_generator(
    initial_message,
    offset: int,
    length: int,
    chunk_size: int = 1024 * 1024,
    concurrency: int = None,
    cache: bool = True,
):
    """
    Fetch file chunks in parallel using the client pool.
    Each worker uses its own client and fetches its own Message object
    to avoid cross-bot FILE_REFERENCE_INVALID errors.
    When cache=False, chunks are served directly from futures without
    going through the sliding-window RAM cache (reduces memory for
    non-interactive downloads like GDrive uploads).
    """
    pool_size = len(clients)
    # Only helper bots (1-13) used for streaming — bot 0 is slow at scraping
    helper_pool = [c for c in clients if getattr(c, 'pool_index', 0) != 0]
    helper_count = len(helper_pool)
    if concurrency is None:
        concurrency = max(1, sum(1 for c in helper_pool if c.is_connected))

    if not any(c.is_connected for c in helper_pool):
        raise ConnectionError("No helper clients are connected")

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

    # Register forward stream for monitor (done futures = prebuffer depth)
    _forward_streams[message_id] = {"chat_id": chat_id, "results": results, "total_chunks": total_chunks, "updated_at": time.monotonic()}

    # Check cache (RAM → disk) — pre-set futures for cached chunks
    video_cache = _NullCache() if not cache else _cache_manager.get_cache(chat_id, message_id)
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

    # Task queue — only filled up to MAX_AHEAD ahead of current position
    # Prevents the forward buffer of resolved futures from exhausting RAM
    task_queue = asyncio.Queue()
    MAX_AHEAD = 300  # max 300MB of resolved futures ahead

    async def refill_queue():
        total_queued = 0
        for rstart, rend in uncached_ranges:
            # Fast-start: first N chunks as 1-chunk batches (parallel across all bots)
            fast_count = min(concurrency, rend - rstart + 1)
            for i in range(fast_count):
                while True:
                    if rstart + i <= video_cache.position + MAX_AHEAD:
                        await task_queue.put((rstart + i, rstart + i))
                        total_queued += 1
                        break
                    await asyncio.sleep(0.2)

            for batch_start in range(rstart + fast_count, rend + 1, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE - 1, rend)
                while True:
                    if batch_start <= video_cache.position + MAX_AHEAD:
                        await task_queue.put((batch_start, batch_end))
                        total_queued += 1
                        break
                    await asyncio.sleep(0.2)
        logger.info("REFILL done: %d batches queued for msg %d", total_queued, message_id)
        for _ in range(concurrency):
            await task_queue.put((None, None))

    async def _fetch_batch(batch_start, batch_end, cl, msg, sem):
        """Fetch a batch, assigning each chunk as it arrives.
        Forward-caches each chunk immediately so concurrent streams
        of the same file benefit before the yield loop."""
        c_idx = getattr(cl, 'pool_index', '?')
        logger.debug("BATCH START bot %d range %d-%d", c_idx, batch_start, batch_end)
        t0 = time.perf_counter()
        current = batch_start
        try:
            async with sem:
                async with asyncio.timeout(60):
                    async for part in cl.stream_media(msg, limit=batch_end - batch_start + 1, offset=batch_start):
                        if current > batch_end:
                            break
                        data = bytes(part)
                        if data and (len(data) >= MIN_CHUNK_SIZE or current == end_chunk):
                            video_cache.store(current, data)
                            if not results[current].done():
                                results[current].set_result(data)
                            current += 1
                        elif data:
                            logger.warning("BATCH TINY bot %d chunk %d in range %d-%d: %d bytes", c_idx, current, batch_start, batch_end, len(data))
                        else:
                            logger.warning("BATCH EMPTY bot %d chunk %d in range %d-%d", c_idx, current, batch_start, batch_end)
        except (asyncio.TimeoutError, ConnectionError, OSError, AuthKeyUnregistered) as e:
            logger.warning("BATCH ABORT bot %d %d-%d: %s", c_idx, batch_start, batch_end, e)
            return False
        elapsed = time.perf_counter() - t0
        nchunks = current - batch_start
        if elapsed > 2.5:
            logger.warning("BATCH SLOW bot %d %d-%d: %d ch in %.1fs (%.1f MB/s)", c_idx, batch_start, batch_end, nchunks, elapsed, nchunks / elapsed if elapsed else 0)
        else:
            logger.debug("BATCH OK bot %d %d-%d: %d ch in %.1fs", c_idx, batch_start, batch_end, nchunks, elapsed)
        return current - 1 == batch_end

    async def _fetch_one(chunk_offset, cl, msg, sem):
        """Fetch a single chunk, forward-caching it on success."""
        c_idx = getattr(cl, 'pool_index', '?')
        t0 = time.perf_counter()
        try:
            async with sem:
                d = bytearray()
                async for part in cl.stream_media(msg, limit=1, offset=chunk_offset):
                    d.extend(part)
            data = bytes(d)
            if not data:
                logger.warning("FETCH ONE EMPTY bot %d chunk %d in %.1fs", c_idx, chunk_offset, time.perf_counter() - t0)
                return None
            if len(data) < MIN_CHUNK_SIZE and chunk_offset != end_chunk:
                logger.warning("FETCH ONE TINY bot %d chunk %d: %d bytes in %.1fs", c_idx, chunk_offset, len(data), time.perf_counter() - t0)
                return None
            video_cache.store(chunk_offset, data)
            elapsed = time.perf_counter() - t0
            logger.debug("FETCH ONE bot %d chunk %d in %.1fs", c_idx, chunk_offset, elapsed)
            return data
        except (FileReferenceInvalid, FileReferenceExpired, AuthKeyUnregistered):
            raise
        except Exception as e:
            logger.warning("FETCH ONE FAIL bot %d chunk %d: %s", c_idx, chunk_offset, e)
            return None

    async def worker(worker_id: int):
        client = helper_pool[worker_id % helper_count]
        c_idx = getattr(client, "pool_index", -1)

        # Skip if this helper isn't connected
        if not client.is_connected:
            logger.error("WORKER %d: bot %d not connected", worker_id, c_idx)
            return

        # Each worker fetches its own fresh Message so file references are per-client
        t0 = time.perf_counter()
        try:
            local_msg = await client.get_messages(chat_id, message_id)
        except Exception as e:
            logger.error("WORKER %d bot %d: get_messages fail: %s", worker_id, c_idx, e)
            return
        if not local_msg:
            logger.error("WORKER %d bot %d: message %d not found", worker_id, c_idx, message_id)
            raise FileNotFoundError(f"Message {message_id} not found in storage channel")

        semaphore = get_client_semaphore(c_idx)
        batch_count = 0
        logger.debug("WORKER %d bot %d: started for msg %d", worker_id, c_idx, message_id)

        while True:
            try:
                batch_start, batch_end = await task_queue.get()
            except asyncio.CancelledError:
                break
            if batch_start is None:
                task_queue.task_done()
                break
            batch_count += 1

            batch_ok = False
            batch_retried = False
            try:
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
                if await reconnect_client(client):
                    try:
                        local_msg = await client.get_messages(chat_id, message_id)
                        batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                    except Exception:
                        pass
                batch_retried = True
            except Exception as e:
                logger.error("Bot %d failed batch %d-%d: %s", c_idx, batch_start, batch_end, e)

            if batch_ok:
                task_queue.task_done()
                continue

            # Bot disconnected — try reconnecting (skip if already attempted for AuthKeyUnregistered)
            if not batch_retried and await reconnect_client(client):
                logger.info("WORKER %d bot %d: reconnected, retrying batch %d-%d", worker_id, c_idx, batch_start, batch_end)
                try:
                    local_msg = await client.get_messages(chat_id, message_id)
                    if local_msg:
                        batch_ok = await _fetch_batch(batch_start, batch_end, client, local_msg, semaphore)
                except Exception as e2:
                    logger.error("WORKER %d bot %d: retry failed: %s", worker_id, c_idx, e2)

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
                        if data and (len(data) >= MIN_CHUNK_SIZE or chunk_offset == end_chunk):
                            video_cache.store(chunk_offset, data)
                            if not results[chunk_offset].done():
                                results[chunk_offset].set_result(data)
                            continue
                    except Exception as e2:
                        logger.error("Bot %d failed chunk %d after re-fetch: %s", c_idx, chunk_offset, e2)
                except AuthKeyUnregistered:
                    logger.warning("Bot %d: auth key expired for chunk %d", c_idx, chunk_offset)
                    if await reconnect_client(client):
                        try:
                            local_msg = await client.get_messages(chat_id, message_id)
                            async with semaphore:
                                d = bytearray()
                                async for part in client.stream_media(local_msg, limit=1, offset=chunk_offset):
                                    d.extend(part)
                            data = bytes(d)
                            if data and (len(data) >= MIN_CHUNK_SIZE or chunk_offset == end_chunk):
                                video_cache.store(chunk_offset, data)
                                if not results[chunk_offset].done():
                                    results[chunk_offset].set_result(data)
                                continue
                        except Exception as e2:
                            logger.error("Bot %d failed chunk %d after reconnect: %s", c_idx, chunk_offset, e2)
                    else:
                        logger.error("Bot %d: reconnect failed for chunk %d", c_idx, chunk_offset)
                except Exception as e:
                    logger.error("Bot %d failed chunk %d: %s", c_idx, chunk_offset, e)
                await _retry_chunk_with_alt_client(c_idx, chunk_offset, chat_id, message_id, results)
            task_queue.task_done()

        elapsed = time.perf_counter() - t0
        logger.debug("WORKER %d bot %d: done %d batches in %.1fs", worker_id, c_idx, batch_count, elapsed)

    # Launch workers + refill task
    refill_task = asyncio.create_task(refill_queue())
    worker_tasks = [
        asyncio.create_task(worker(i)) for i in range(concurrency)
    ]

    # Yield results in order with windowed 100MB pre-buffer
    # First 100 chunks yield immediately — zero startup delay.
    # From chunk 100 onward, before yielding chunk N, we wait for chunk N+100
    # to be ready. This maintains a 100MB lookahead cushion that absorbs
    # Telegram latency spikes without pausing ExoPlayer.
    PREBUFFER_CHUNKS = 100
    stream_start = time.perf_counter()
    first_chunk_logged = False
    bytes_yielded = 0
    try:
        for offset in range(total_chunks):
            chunk_idx = start_chunk + offset
            
            # From chunk 100 onward, ensure lookahead is ready
            if offset >= PREBUFFER_CHUNKS:
                lookahead_idx = chunk_idx + PREBUFFER_CHUNKS
                if lookahead_idx <= end_chunk:
                    try:
                        await asyncio.wait_for(results[lookahead_idx], timeout=2.0)
                    except asyncio.TimeoutError:
                        logger.debug("LOOKAHEAD timeout chunk %d (ahead %d), cushion %d ch", chunk_idx, lookahead_idx, (offset - PREBUFFER_CHUNKS + 1))
                    except asyncio.CancelledError:
                        pass
            
            # Update sliding window position before cache ops
            video_cache.set_position(chunk_idx)
            cached_data = video_cache.get(chunk_idx)
            if cached_data is not None:
                chunk_data = cached_data
            else:
                chunk_data = await results[chunk_idx]
                video_cache.store(chunk_idx, chunk_data)
            
            bytes_yielded += len(chunk_data)
            if not first_chunk_logged:
                elapsed = time.perf_counter() - stream_start
                logger.info("Chunk %d in %.1fs (cached=%s)", chunk_idx, elapsed, cached_data is not None)
                first_chunk_logged = True
            elif offset % 500 == 0:
                done_futures = sum(1 for f in list(results.values()) if f.done())
                if isinstance(video_cache, _NullCache):
                    logger.info("VERBOSE: yielded %d, done futures %d/%d, queue %d (cache bypass)", chunk_idx, done_futures, total_chunks, task_queue.qsize())
                else:
                    logger.info("VERBOSE: yielded %d, done futures %d/%d, queue %d, ram %d ch %.0f MB", chunk_idx, done_futures, total_chunks, task_queue.qsize(), len(video_cache._data), video_cache._size / 1024 / 1024)
            yield chunk_data
            # Refresh forward stream timestamp every 100 chunks
            if offset % 100 == 0:
                entry = _forward_streams.get(message_id)
                if entry:
                    entry["updated_at"] = time.monotonic()
            del results[chunk_idx]
    finally:
        _forward_streams.pop(message_id, None)
        refill_task.cancel()
        for w in worker_tasks:
            w.cancel()
        await asyncio.gather(refill_task, *worker_tasks, return_exceptions=True)
        _cache_manager.remove(chat_id, message_id)
        elapsed = time.perf_counter() - stream_start
        logger.debug("STREAM END msg %d: %d ch in %.1fs, peak forward %.0f MB", message_id, total_chunks, elapsed, _forward_streams.get(message_id, {}).get("results", {}))
        if total_chunks:
            logger.info("Done: %d ch, %.1f MB, %.1fs", total_chunks, bytes_yielded / 1024 / 1024, elapsed)
            cinfo = video_cache.info
            logger.info("Cache hits/evicts: %d/%d", cinfo["hits"], cinfo["evictions"])


async def stream_file(
    client: Client,          # kept for API compat; pool is used instead
    message,
    from_bytes: int,
    until_bytes: int,
) -> AsyncGenerator[bytes, None]:
    """Stream a file range using the multi-client pool.
    Limits concurrent streams to prevent OOM from 2 GB prebuffers stacking.
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
