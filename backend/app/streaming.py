"""
Custom streaming utilities for Telegram media files.
Multi-client parallel streaming for maximum download speed.
"""
import asyncio
import os
import re
import shutil
import time
import logging
from typing import AsyncGenerator
from pathlib import Path

BATCH_SIZE = 7  # chunks per stream_media call
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk
DISK_CACHE_BASE = "data/chunks"
DISK_CACHE_TTL = 3 * 3600  # 3 hours
DISK_CACHE_MAX = 13 * 1024 * 1024 * 1024  # 13GB max

# Sliding window: chunks within [-20, +50] of current playback position stay in RAM
# Conservative for 3GB RAM: 70MB/stream × 5 streams = 350MB max cache
# Excess chunks spill to NVMe disk automatically
FWD_WINDOW = 50   # 50MB forward cache (enough for ~10s of 4K video)
BACK_WINDOW = 20  # 20MB backward cache (enough for seek-back)
RAM_MAX = (FWD_WINDOW + BACK_WINDOW) * 1024 * 1024  # 70MB total


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
    except OSError as e:
        logger.error("Disk cache write failed for %s: %s", p, e)


def _dc_remove(chat_id: int, message_id: int):
    d = os.path.join(DISK_CACHE_BASE, str(chat_id), str(message_id))
    if os.path.isdir(d):
        shutil.rmtree(d, ignore_errors=True)


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


class StreamCache:
    """Position-aware sliding window cache for one video stream.
    Chunks within [-BACK_WINDOW, +FWD_WINDOW] of current playback position
    stay in RAM. Older/farther chunks are spilled to NVMe.
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
            # Bring back into RAM if now within window
            dist = key - self.position
            if -BACK_WINDOW <= dist <= FWD_WINDOW:
                self._data[key] = d
                self._size += len(d)
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
            while self._size > RAM_MAX:
                farthest = max(self._data.keys(), key=lambda k: abs(k - self.position))
                old = self._data.pop(farthest)
                self._size -= len(old)
                _dc_put(self.chat_id, self.message_id, farthest, old)
                self._evictions += 1
        else:
            _dc_put(self.chat_id, self.message_id, key, data)

    def clear(self) -> int:
        freed = self._size
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
from pyrogram.errors import FileReferenceExpired, FileReferenceInvalid, AuthKeyUnregistered

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

# Limit total concurrent streams to prevent OOM from 2 GB prebuffers stacking.
# Each stream can hold up to 2000 resolved 1 MB chunks (2 GB) awaiting yield.
# With LIMIT=5, max in-flight = 5 × 200 MB = 1 GB, headroom on 3 GB machine.
_stream_semaphore = asyncio.Semaphore(5)

def get_client_semaphore(client_index: int) -> asyncio.Semaphore:
    if client_index not in _client_semaphores:
        # Use the configured concurrency limit
        _client_semaphores[client_index] = asyncio.Semaphore(settings.telegram_client_concurrency)
    return _client_semaphores[client_index]


async def prefetch_first_batch(client, message, from_bytes: int = 0):
    """Fire-and-forget: start caching the first batch before the generator runs.
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
        c_idx = getattr(client, "pool_index", 0)
        sem = get_client_semaphore(c_idx)
        async with sem:
            async for part in client.stream_media(message, limit=BATCH_SIZE, offset=start_chunk):
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
                _cache_manager.get_cache(chat_id, message_id).store(chunk_idx, chunk_bytes)
                if not results[chunk_idx].done():
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
                            _cache_manager.get_cache(chat_id, message_id).store(chunk_idx, chunk_bytes)
                            if not results[chunk_idx].done():
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

    async def _fetch_batch(batch_start, batch_end, cl, msg, sem):
        """Fetch a batch, assigning each chunk as it arrives.
        Forward-caches each chunk immediately so concurrent streams
        of the same file benefit before the yield loop."""
        t0 = time.perf_counter()
        current = batch_start
        try:
            async with sem:
                async with asyncio.timeout(60):
                    async for part in cl.stream_media(msg, limit=batch_end - batch_start + 1, offset=batch_start):
                        if current > batch_end:
                            break
                        data = bytes(part)
                        video_cache.store(current, data)
                        _dc_put(chat_id, message_id, current, data)
                        if not results[current].done():
                            results[current].set_result(data)
                        current += 1
        except (asyncio.TimeoutError, ConnectionError, OSError, AuthKeyUnregistered) as e:
            logger.warning("Bot %d batch %d-%d aborted: %s", getattr(cl, 'pool_index', '?'), batch_start, batch_end, e)
            return False
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
            video_cache.store(chunk_offset, data)
            return data
        except (FileReferenceInvalid, FileReferenceExpired, AuthKeyUnregistered):
            raise
        except Exception:
            return None

    async def worker(worker_id: int):
        client = helper_pool[worker_id % helper_count]
        c_idx = getattr(client, "pool_index", -1)

        # Skip if this helper isn't connected
        if not client.is_connected:
            logger.error("Worker %d: helper bot %d not connected", worker_id, c_idx)
            return

        # Each worker fetches its own fresh Message so file references are per-client
        try:
            local_msg = await client.get_messages(chat_id, message_id)
        except Exception as e:
            logger.error("Bot %d: failed to fetch message %d: %s", c_idx, message_id, e)
            return
        if not local_msg:
            logger.error("Bot %d: message %d not found", c_idx, message_id)
            raise FileNotFoundError(f"Message {message_id} not found in storage channel")

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

    # Launch workers
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
    cache_served = 0
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
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass  # Don't block — yield the current chunk and continue
            
            # Update sliding window position before cache ops
            video_cache.set_position(chunk_idx)
            cached_data = video_cache.get(chunk_idx)
            if cached_data is not None:
                chunk_data = cached_data
                cache_served += 1
            else:
                chunk_data = await results[chunk_idx]
                video_cache.store(chunk_idx, chunk_data)
            
            bytes_yielded += len(chunk_data)
            if not first_chunk_logged:
                elapsed = time.perf_counter() - stream_start
                logger.info("Chunk %d in %.1fs (cached=%s)", chunk_idx, elapsed, cached_data is not None)
                first_chunk_logged = True
            yield chunk_data
            # Refresh forward stream timestamp every 100 chunks
            if offset % 100 == 0:
                entry = _forward_streams.get(message_id)
                if entry:
                    entry["updated_at"] = time.monotonic()
            del results[chunk_idx]
    finally:
        _forward_streams.pop(message_id, None)
        for w in worker_tasks:
            w.cancel()
        _dc_remove(chat_id, message_id)
        _cache_manager.remove(chat_id, message_id)
        elapsed = time.perf_counter() - stream_start
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
