"""
Streaming API endpoints for media playback.
"""
import re
import asyncio
import logging
from fastapi import APIRouter, Depends, HTTPException, Request, Response, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ..database import get_db
from ..models import File, User
from ..auth import get_current_user, get_current_user_opt, verify_token, verify_token_payload

from ..telegram import get_message_from_channel, tg_client, clients
from ..streaming import stream_file as stream_file_chunks, prefetch_first_batch, _cache_manager, _forward_streams
from ..config import get_settings
from ..rate_limit import limiter

logger = logging.getLogger(__name__)
settings = get_settings()

router = APIRouter(prefix="/stream", tags=["Streaming"])


def parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    """Parse HTTP Range header for video seeking support."""
    if not range_header:
        return 0, file_size - 1

    # Suffix range: bytes=-500 (last N bytes)
    suffix_match = re.match(r'bytes=-(\d+)', range_header)
    if suffix_match:
        suffix_len = int(suffix_match.group(1))
        start = max(0, file_size - suffix_len)
        return start, file_size - 1

    match = re.match(r'bytes=(\d+)-(\d*)', range_header)
    if not match:
        return 0, file_size - 1

    start = int(match.group(1))
    end = int(match.group(2)) if match.group(2) else file_size - 1

    return start, min(end, file_size - 1)


@router.get("/debug")
async def streaming_debug(request: Request):
    # Allow Bearer aarsha or valid admin JWT
    auth = request.headers.get("Authorization", "")
    if auth != f"Bearer {settings.debug_password}":
        try:
            token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else ""
            tid = verify_token(token) if token else None
            if not tid:
                raise HTTPException(status_code=401, detail="Invalid or expired token")
            from ..database import async_session
            from ..models import User
            from sqlalchemy import select
            async with async_session() as db:
                r = await db.execute(select(User).where(User.telegram_id == tid))
                user = r.scalar_one_or_none()
            if not user or not user.is_admin:
                raise HTTPException(status_code=403, detail="Admin access required")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=401, detail="Invalid or expired token")

    cache_info = _cache_manager.info
    per_video = _cache_manager.per_video

    disk_bytes = _dc_disk_size()
    disk_mb = round(disk_bytes / 1024 / 1024, 1)

    bots = []
    for i, c in enumerate(clients):
        bots.append({
            "index": i,
            "label": "Main" if i == 0 else f"Helper {i}",
            "connected": c.is_connected,
        })

    forward_info = []
    for mid in list(_forward_streams.keys()):
        info = _forward_streams.get(mid)
        if not info:
            continue
        futures = info.get("results", {})
        done = sum(1 for f in list(futures.values()) if f.done())
        total = info.get("total_chunks", 0)
        forward_info.append({
            "message_id": mid,
            "done_futures": done,
            "total_futures": len(futures),
            "total_chunks": total,
        })

    return {
        "cache": {
            "ram_chunks": cache_info["chunks"],
            "ram_mb": cache_info["size_mb"],
            "hits": cache_info["hits"],
            "misses": cache_info["misses"],
            "evictions": cache_info["evictions"],
            "hit_rate_pct": round(
                cache_info["hits"] / (cache_info["hits"] + cache_info["misses"]) * 100, 1
            ) if (cache_info["hits"] + cache_info["misses"]) > 0 else 0,
            "per_video": per_video,
        },
        "disk_cache_mb": disk_mb,
        "bots": bots,
        "active_streams": forward_info,
        "active_stream_count": len(forward_info),
    }


@router.get("/{file_id}")
async def stream_file(
    file_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User | None = Depends(get_current_user_opt),
    download: int = Query(0, description="Set to 1 to force download"),
):
    """Stream file from Telegram with range request support for seeking."""
    # Fall back to download token if not authenticated normally
    if not current_user:
        token = request.query_params.get("token")
        if token:
            payload = verify_token_payload(token, token_type="download")
            if payload:
                tid = int(payload["sub"])
                token_version = payload.get("ver")
                result = await db.execute(select(User).where(User.telegram_id == tid))
                user = result.scalar_one_or_none()
                if user and (token_version is None or token_version >= user.auth_version):
                    current_user = user
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # Get file from database
    result = await db.execute(
        select(File).where(File.id == file_id, File.user_id == current_user.id)
    )
    file = result.scalar_one_or_none()
    
    if not file:
        raise HTTPException(status_code=404, detail="File not found")

    file_size = file.file_size

    # Parse range header
    range_header = request.headers.get("range")
    from_bytes, until_bytes = parse_range_header(range_header, file_size)

    # Validate range
    if (until_bytes > file_size) or (from_bytes < 0) or (from_bytes > until_bytes):
        return Response(
            status_code=416,
            content="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    # Get message from channel
    message = await get_message_from_channel(file.channel_message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found in channel")

    # Pre-fetch first batch to reduce load time
    asyncio.create_task(prefetch_first_batch(tg_client, message, from_bytes))

    async def file_streamer():
        """Generator that streams file chunks from Telegram MTProto.
        Streams to file_size on range requests so the player gets continuous
        data from seek position and can find a keyframe to start decoding."""
        try:
            if range_header:
                async with asyncio.timeout(300):
                    async for chunk in stream_file_chunks(
                        tg_client,
                        message,
                        from_bytes,
                        file_size
                    ):
                        yield chunk
            else:
                async for chunk in stream_file_chunks(
                    tg_client,
                    message,
                    from_bytes,
                    file_size
                ):
                    yield chunk
        except asyncio.TimeoutError:
            logger.warning("Stream timed out after 300s for file %d", file_id)
            raise
        except Exception as e:
            logger.error("Stream failed for file %d: %s", file_id, e)
            raise

    # Determine content disposition
    mime_type = file.mime_type or "application/octet-stream"
    disposition = "attachment" if download else ("inline" if ("video/" in mime_type or "audio/" in mime_type) else "attachment")

    from urllib.parse import quote
    encoded_filename = quote(file.file_name)

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f"{disposition}; filename*=utf-8''{encoded_filename}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=86400",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    return StreamingResponse(
        file_streamer(),
        status_code=206 if range_header else 200,
        media_type=mime_type,
        headers=headers
    )


@router.get("/{file_id}/thumbnail")
async def get_thumbnail(
    file_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get file thumbnail with caching."""
    result = await db.execute(
        select(File).where(File.id == file_id, File.user_id == current_user.id)
    )
    file = result.scalar_one_or_none()
    
    if not file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Serve from cache if available
    if file.thumbnail_data:
        mime = _detect_image_mime(file.thumbnail_data)
        return Response(content=file.thumbnail_data, media_type=mime)
    
    if not file.thumbnail_file_id:
        raise HTTPException(status_code=404, detail="Thumbnail not found")
    
    try:
        # Get the message and download thumbnail
        message = await get_message_from_channel(file.channel_message_id)
        if not message:
            raise HTTPException(status_code=404, detail="Message not found")

        # Extract thumbnail object
        thumbnail = None
        if message.video and message.video.thumbs:
            thumbnail = message.video.thumbs[0]
        elif message.document and message.document.thumbs:
            thumbnail = message.document.thumbs[0]
        elif message.audio and message.audio.thumbs:
            thumbnail = message.audio.thumbs[0]
        elif message.photo:
            thumbnail = message.photo[-1]
            
        if not thumbnail:
            if file.thumbnail_file_id:
                try:
                    thumb_bytes = await tg_client.download_media(
                        file.thumbnail_file_id,
                        in_memory=True
                    )
                    data = bytes(thumb_bytes.getbuffer()) if hasattr(thumb_bytes, 'getbuffer') else thumb_bytes
                except Exception:
                    raise HTTPException(status_code=404, detail="Thumbnail not found in message")
            else:
                raise HTTPException(status_code=404, detail="Thumbnail not found in message")
        else:
            thumb_bytes = await tg_client.download_media(thumbnail.file_id, in_memory=True)
            data = thumb_bytes.getvalue()
        
        # Cache for future requests
        file.thumbnail_data = data
        await db.commit()

        mime = _detect_image_mime(data)
        return Response(content=data, media_type=mime)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Thumbnail error for file {file_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to get thumbnail")


def _detect_image_mime(data: bytes) -> str:
    """Detect image MIME type from magic bytes."""
    if data[:4] == b"\x89PNG":
        return "image/png"
    if data[:2] == b"\xff\xd8":
        return "image/jpeg"
    if data[:2] == b"BM":
        return "image/bmp"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


@router.get("/s/{public_hash}")
@limiter.limit("60/minute")  # Rate limit public streaming to prevent abuse
async def stream_public_file(
    public_hash: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    download: int = Query(0, description="Set to 1 to force download"),
):
    """Stream file via public link (no auth required)."""
    # Get file by hash
    result = await db.execute(select(File).where(File.public_hash == public_hash))
    file = result.scalar_one_or_none()
    
    if not file:
        raise HTTPException(status_code=404, detail="File not found or link revoked")
        
    file_size = file.file_size
    
    # Parse range header
    range_header = request.headers.get("range")
    from_bytes, until_bytes = parse_range_header(range_header, file_size)
    
    # Validate range
    if (until_bytes > file_size) or (from_bytes < 0) or (from_bytes > until_bytes):
        return Response(
            status_code=416,
            content="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    # Get message from channel
    message = await get_message_from_channel(file.channel_message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Message not found in channel")

    # Pre-fetch first batch to reduce load time
    asyncio.create_task(prefetch_first_batch(tg_client, message, from_bytes))

    async def file_streamer():
        """Generator that streams file chunks from Telegram MTProto.
        Streams to file_size on range requests so the player gets continuous
        data from seek position and can find a keyframe to start decoding."""
        try:
            if range_header:
                async with asyncio.timeout(300):
                    async for chunk in stream_file_chunks(
                        tg_client,
                        message,
                        from_bytes,
                        file_size
                    ):
                        yield chunk
            else:
                async for chunk in stream_file_chunks(
                    tg_client,
                    message,
                    from_bytes,
                    file_size
                ):
                    yield chunk
        except asyncio.TimeoutError:
            logger.warning("Public stream timed out after 300s for hash %s", public_hash)
            raise
        except Exception as e:
            logger.error("Public stream failed for hash %s: %s", public_hash, e)
            raise

    # Determine content disposition
    mime_type = file.mime_type or "application/octet-stream"
    disposition = "attachment" if download else ("inline" if ("video/" in mime_type or "audio/" in mime_type) else "attachment")

    from urllib.parse import quote
    encoded_filename = quote(file.file_name)

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f"{disposition}; filename*=utf-8''{encoded_filename}",
        "Accept-Ranges": "bytes",
        "Cache-Control": "public, max-age=86400",
    }
    if range_header:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    return StreamingResponse(
        file_streamer(),
        status_code=206 if range_header else 200,
        media_type=mime_type,
        headers=headers
    )
