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
from ..auth import get_current_user
from ..telegram import get_message_from_channel, tg_client
from ..streaming import stream_file as stream_file_chunks, prefetch_first_batch
from ..rate_limit import limiter

logger = logging.getLogger(__name__)

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


@router.get("/{file_id}")
async def stream_file(
    file_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
    download: int = Query(0, description="Set to 1 to force download"),
):
    """Stream file from Telegram with range request support for seeking."""
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
        """Generator that streams file chunks from Telegram MTProto."""
        try:
            async with asyncio.timeout(300):
                async for chunk in stream_file_chunks(
                    tg_client,
                    message,
                    from_bytes,
                    until_bytes
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
        """Generator that streams file chunks from Telegram MTProto."""
        try:
            async with asyncio.timeout(300):
                async for chunk in stream_file_chunks(
                    tg_client,
                    message,
                    from_bytes,
                    until_bytes
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
