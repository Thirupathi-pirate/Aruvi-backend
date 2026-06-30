"""
Google Drive integration — OAuth + stream-through upload.
Never buffers the full file; streams Telegram chunks directly to Drive.
"""

import json
import logging
import secrets
from datetime import datetime, timezone

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from .config import get_settings
from .telegram import tg_client

_log = logging.getLogger(__name__)
settings = get_settings()

SCOPES = ["https://www.googleapis.com/auth/drive.file"]
CHUNK_SIZE = 1024 * 1024  # 1MB


def _flow() -> Flow:
    return Flow.from_client_config(
        {
            "web": {
                "client_id": settings.gdrive_client_id,
                "client_secret": settings.gdrive_client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": [settings.gdrive_redirect_uri],
            }
        },
        scopes=SCOPES,
    )


def generate_auth_url(telegram_id: int) -> tuple[str, str]:
    """Generate Google OAuth URL + nonce for the given Telegram user.

    Returns (url, nonce).  The nonce is embedded in the state param so the
    callback can look up which telegram_id to attach the token to.
    """
    flow = _flow()
    nonce = secrets.token_hex(8)
    state = f"{telegram_id}:{nonce}"
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        state=state,
        include_granted_scopes="true",
    )
    return auth_url, nonce


def exchange_code(code: str) -> dict:
    """Exchange an OAuth authorization code for a token dict.

    The dict is JSON-serialisable and suitable for storing in the DB.
    """
    flow = _flow()
    flow.fetch_token(code=code)
    creds = flow.credentials
    return _creds_to_dict(creds)


def _creds_to_dict(creds: UserCredentials) -> dict:
    return {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": list(creds.scopes),
        "expiry": creds.expiry.isoformat() if creds.expiry else None,
    }


def _creds_from_dict(d: dict) -> UserCredentials:
    expiry = None
    if d.get("expiry"):
        try:
            expiry = datetime.fromisoformat(d["expiry"])
        except Exception:
            pass
    return UserCredentials(
        token=d.get("token"),
        refresh_token=d.get("refresh_token"),
        token_uri=d.get("token_uri"),
        client_id=d.get("client_id"),
        client_secret=d.get("client_secret"),
        scopes=d.get("scopes", SCOPES),
        expiry=expiry,
    )


def refresh_token_dict(token_dict: dict) -> dict:
    """Refresh the access token if expired.  Returns the (possibly updated)
    token dict so the caller can persist it back to the DB."""
    creds = _creds_from_dict(token_dict)
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleAuthRequest())
        token_dict["token"] = creds.token
        if creds.expiry:
            token_dict["expiry"] = creds.expiry.isoformat()
    return token_dict


def get_access_token(token_dict: dict) -> str:
    """Return a valid access token string, refreshing if needed."""
    creds = _creds_from_dict(token_dict)
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleAuthRequest())
        token_dict["token"] = creds.token
        if creds.expiry:
            token_dict["expiry"] = creds.expiry.isoformat()
    return creds.token


def build_service(token_dict: dict):
    """Build an authenticated Google Drive API v3 service from a stored
    token dict.  Auto-refreshes the access token if expired."""
    creds = _creds_from_dict(token_dict)
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleAuthRequest())
        token_dict["token"] = creds.token
        if creds.expiry:
            token_dict["expiry"] = creds.expiry.isoformat()
    return build("drive", "v3", credentials=creds)


async def ensure_aruvi_folder(service) -> str:
    """Return the ID of the 'Aruvi' folder in the user's Drive.
    Creates it if it doesn't exist."""
    q = (
        "name='Aruvi'"
        " and mimeType='application/vnd.google-apps.folder'"
        " and trashed=false"
    )
    result = service.files().list(q=q, spaces="drive", fields="files(id)").execute()
    files = result.get("files", [])
    if files:
        return files[0]["id"]
    folder = (
        service.files()
        .create(
            body={"name": "Aruvi", "mimeType": "application/vnd.google-apps.folder"},
            fields="id",
        )
        .execute()
    )
    return folder["id"]


async def upload_streaming(
    token_dict: dict,
    msg: "Message",
    file_name: str,
    mime_type: str,
    file_size: int,
    folder_id: str,
) -> str:
    """Stream a Telegram Message directly to Google Drive using the
    resumable upload protocol.  No temp file is written.

    Returns the webViewLink to the uploaded file.
    """
    access_token = get_access_token(token_dict)

    # 1. Start resumable upload session
    metadata = json.dumps(
        {
            "name": file_name,
            "mimeType": mime_type,
            "parents": [folder_id],
        }
    )

    async with httpx.AsyncClient() as client:
        session_resp = await client.post(
            "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": mime_type,
                "X-Upload-Content-Length": str(file_size),
            },
            content=metadata,
        )
        session_resp.raise_for_status()
        upload_url = session_resp.headers["Location"]

        uploaded = 0
        async for chunk in tg_client.stream_media(msg, chunk_size=CHUNK_SIZE):
            chunk_bytes = chunk if isinstance(chunk, bytes) else bytes(chunk)
            start = uploaded
            end = uploaded + len(chunk_bytes) - 1
            total = file_size

            resp = await client.put(
                upload_url,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{total}",
                    "Content-Length": str(len(chunk_bytes)),
                },
                content=chunk_bytes,
            )
            if resp.status_code not in (200, 201, 308):
                _log.error(
                    "Drive upload chunk failed: %s %s",
                    resp.status_code,
                    resp.text,
                )
                resp.raise_for_status()

            uploaded += len(chunk_bytes)

    # 3. Retrieve the uploaded file's webViewLink
    file_resource = resp.json()
    file_id = file_resource.get("id")
    if not file_id:
        raise RuntimeError("Upload completed but no file ID returned")

    service = build_service(token_dict)
    file_meta = (
        service.files()
        .get(fileId=file_id, fields="webViewLink")
        .execute()
    )
    return file_meta.get(
        "webViewLink",
        f"https://drive.google.com/file/d/{file_id}/view",
    )
