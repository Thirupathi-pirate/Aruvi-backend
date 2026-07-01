"""
Custom streaming utilities for Telegram media files.
Multi-client parallel streaming for maximum download speed.
Based on the proven teleplay-backend architecture — no disk cache,
no sliding window, no MAX_AHEAD throttle. Simple and reliable.
"""
import asyncio
import logging
import re
import time
from typing import AsyncGenerator

BATCH_SIZE = 10  # 10MB per batch (10 x 1MB chunks)
CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk

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

# Limit total concurrent streams to prevent OOM from prebuffers stacking
_stream_semaphore = asyncio.Semaphore(5)


def get_client_semaphore(client_index: int) -> asyncio.Semaphore:
    if client_index not in _client_semaphores:
        _client_semaphores[client_index] = asyncio.Semaphore(settings.telegram_client_concurrency)
    return _client_semaphores[client_index]


# ── Cache classes ──────────────────────────────────────────────────────────────

class ChunkCache:
    """Bounded in-memory cache for video chunks. Evicts oldest entries when full."""
    def __init__(self, max_bytes: int = 100 * 1024 * 1024):
        self._data: dict[int, bytes] = {}
        self._size = 0
        self._max_bytes = max_bytes
        self._hits = 0
        self._misses = 0
        self._evictions = 0

    def store(self, key: int, data: bytes):
        if not data or key in self._data:
            return
        self._data[key] = data
        self._size += len(data)
        while self._size > self._max_bytes and self._data:
            k = next(iter(self._data))
            self._size -= len(self._data.pop(k))
            self._evictions += 1

    def get(self, key: int) -> bytes | None:
        data = self._data.get(key)
        if data is not None:
            self._hits += 1
        else:
            self._misses += 1
        return data

    def set_position(self, pos: int):
        pass

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


class _NullCache:
    """No-op cache — used when parallel_stream_generator(cache=False).
    All methods are no-ops; get() always returns None.
    Position is tracked for refill backpressure compatibility.
    """
    def __init__(self):
        self.position = 0
    def store(self, *args, **kwargs): pass
    def get(self, key): return None
    def set_position(self, pos): self.position = pos
    def clear(self) -> int: return 0
    @property
    def info(self) -> dict:
        return {"chunks": 0, "size_mb": 0, "hits": 0, "misses": 0, "evictions": 0}


# Per-stream cache management
_stream_caches: dict[tuple[int, int], ChunkCache] = {}


def _get_cache(chat_id: int, message_id: int) -> ChunkCache:
    key = (chat_id, message_id)
    if key not in _stream_caches:
        _stream_caches[key] = ChunkCache()
    return _stream_caches[key]


def _remove_cache(chat_id: int, message_id: int):
    key = (chat_id, message_id)
    if key in _stream_caches:
        _stream_caches.pop(key).clear()


class _CacheManagerCompat:
    """Compatibility wrapper so router's debug endpoint can access cache stats
    via _cache_manager.info and _cache_manager.per_video."""

    @property
    def info(self) -> dict:
        total_chunks = total_size = total_hits = total_misses = total_evictions = 0
        for cache in _stream_caches.values():
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

    @property
    def per_video(self) -> list[dict]:
        result = []
        for (chat_id, message_id), cache in list(_stream_caches.items()):
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

    def remove(self, chat_id: int, message_id: int):
        _remove_cache(chat_id, message_id)

    def clear_all(self, exclude_keys: set[tuple[int, int]] | None = None) -> int:
        total = 0
        keys_to_clear = [k for k in _stream_caches if exclude_keys is None or k not in exclude_keys]
        for key in keys_to_clear:
            total += _stream_caches.pop(key).clear()
        return total


_cache_manager = _CacheManagerCompat()
_forward_streams: dict[int, dict] = {}


def _dc_disk_size() -> int:
    """Stub — disk cache removed. Returns 0 so router debug endpoint doesn't crash."""
    return 0


def get_forward_snapshot() -> list[dict]:
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


# ── Chunk fetch helpers ────────────────────────────────────────────────────────

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
    """
    pool_size = len(clients)
    max_cycles = 5
    for cycle in range(max_cycles):
        for offset in range(1, pool_size):
            alt_c_idx = (failed_c_idx + offset) % pool_size
            if alt_c_idx == failed_c_idx:
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
    """Fire-and-forget: start caching the first batch before the generator runs.
    Uses any connected client. Skips if already cached or if the message has no document."""
    if not message or not message.document:
        return
    file_size = message.document.file_size
    if from_bytes >= file_size:
        return
    chat_id = message.chat.id
    message_id = message.id
    start_chunk = from_bytes // CHUNK_SIZE
    cache = _get_cache(chat_id, message_id)
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
                cache.store(start_chunk, data)
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
    cache: bool = True,
):
    """Fetch file chunks in parallel using ALL clients in the pool.

    Each worker uses its own client and fetches its own Message object
    to avoid cross-bot FILE_REFERENCE_INVALID errors.

    When cache=False, chunks are served directly from futures without
    going through ChunkCache (reduces memory for non-interactive
    downloads like GDrive uploads).
    """
    pool_size = len(clients)
    if concurrency is None:
        concurrency = max(1, sum(1 for c in clients if c.is_connected))

    if not any(c.is_connected for c in clients):
        raise ConnectionError("No clients are connected")

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
    _forward_streams[message_id] = {
        "chat_id": chat_id, "results": results,
        "total_chunks": total_chunks, "updated_at": time.monotonic(),
    }

    # Check cache — pre-set futures for cached chunks
    video_cache = _NullCache() if not cache else _get_cache(chat_id, message_id)
    video_cache.set_position(start_chunk)
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

    # Task queue — no MAX_AHEAD throttle, queue everything immediately
    task_queue = asyncio.Queue()

    async def refill_queue():
        total_queued = 0
        for rstart, rend in uncached_ranges:
            # Fast-start: first N chunks as 1-chunk batches (parallel across all bots)
            fast_count = min(concurrency, rend - rstart + 1)
            for i in range(fast_count):
                await task_queue.put((rstart + i, rstart + i))
                total_queued += 1

            for batch_start in range(rstart + fast_count, rend + 1, BATCH_SIZE):
                batch_end = min(batch_start + BATCH_SIZE - 1, rend)
                await task_queue.put((batch_start, batch_end))
                total_queued += 1
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
                async for part in cl.stream_media(msg, limit=batch_end - batch_start + 1, offset=batch_start):
                    if current > batch_end:
                        break
                    data = bytes(part)
                    if data:
                        video_cache.store(current, data)
                        if not results[current].done():
                            results[current].set_result(data)
                        current += 1
                    else:
                        logger.warning("BATCH EMPTY bot %d chunk %d in range %d-%d", c_idx, current, batch_start, batch_end)
        except (ConnectionError, OSError, AuthKeyUnregistered) as e:
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
        client = clients[worker_id % pool_size]
        c_idx = getattr(client, "pool_index", -1)

        if not client.is_connected:
            logger.error("WORKER %d: bot %d not connected", worker_id, c_idx)
            return

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
                        if data:
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
                            if data:
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

    refill_task = asyncio.create_task(refill_queue())
    worker_tasks = [
        asyncio.create_task(worker(i)) for i in range(concurrency)
    ]

    # Yield results in order with windowed prebuffer
    PREBUFFER_CHUNKS = 500
    stream_start = time.perf_counter()
    first_chunk_logged = False
    bytes_yielded = 0
    try:
        for offset in range(total_chunks):
            chunk_idx = start_chunk + offset

            # Maintain lookahead cushion that absorbs Telegram latency spikes
            if offset >= PREBUFFER_CHUNKS:
                lookahead_idx = chunk_idx + PREBUFFER_CHUNKS
                if lookahead_idx <= end_chunk:
                    try:
                        await asyncio.wait_for(results[lookahead_idx], timeout=2.0)
                    except asyncio.TimeoutError:
                        logger.debug("LOOKAHEAD timeout chunk %d (ahead %d)", chunk_idx, lookahead_idx)
                    except asyncio.CancelledError:
                        pass

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
                logger.info("VERBOSE: yielded %d, done futures %d/%d, queue %d", chunk_idx, done_futures, total_chunks, task_queue.qsize())
            yield chunk_data
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
        _remove_cache(chat_id, message_id)
        elapsed = time.perf_counter() - stream_start
        logger.debug("STREAM END msg %d: %d ch in %.1fs", message_id, total_chunks, elapsed)
        if total_chunks:
            logger.info("Done: %d ch, %.1f MB, %.1fs", total_chunks, bytes_yielded / 1024 / 1024, elapsed)


async def stream_file(
    client: Client,
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
