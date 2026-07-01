"""
Google Drive integration — OAuth + two-phase upload.
Downloads the full file to NVMe temp via 13-bot parallel streaming,
then uploads sequentially to Google Drive with 10MB chunks.
"""

import asyncio
import hashlib
import json
import logging
import secrets
import time
from base64 import urlsafe_b64encode
from datetime import datetime
from pathlib import Path

import httpx
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from .config import get_settings
from .streaming import parallel_stream_generator

_log = logging.getLogger(__name__)
settings = get_settings()

SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# In-memory nonce store for OAuth CSRF protection
# {nonce: (telegram_id, timestamp, code_verifier)}
_nonce_store: dict[str, tuple[int, float, str]] = {}
_NONCE_TTL = 600  # 10 minutes


def _prune_nonces():
    now = time.monotonic()
    expired = [k for k, (_, ts, _) in _nonce_store.items() if now - ts > _NONCE_TTL]
    for k in expired:
        _nonce_store.pop(k, None)


def _pkce_challenge(verifier: str) -> str:
    """Compute S256 code_challenge from a code_verifier."""
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _flow() -> Flow:
    flow = Flow.from_client_config(
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
    flow.redirect_uri = settings.gdrive_redirect_uri
    return flow


def generate_auth_url(telegram_id: int) -> str:
    """Generate Google OAuth URL for the given Telegram user.

    Uses PKCE (S256) and embeds a nonce in the state param to prevent CSRF.
    The code_verifier is stored in-memory alongside the nonce.
    """
    _prune_nonces()
    flow = _flow()
    nonce = secrets.token_hex(16)
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = _pkce_challenge(code_verifier)
    _nonce_store[nonce] = (telegram_id, time.monotonic(), code_verifier)
    state = f"{telegram_id}:{nonce}"
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        state=state,
        include_granted_scopes="true",
        code_challenge=code_challenge,
        code_challenge_method="S256",
    )
    return auth_url


def consume_state(state: str) -> tuple[int, str]:
    """Verify and consume the OAuth state, returning (telegram_id, code_verifier).
    Raises ValueError if the nonce is invalid or expired.
    """
    _prune_nonces()
    try:
        telegram_id_str, nonce = state.split(":", 1)
        telegram_id = int(telegram_id_str)
    except (ValueError, IndexError):
        raise ValueError("Invalid state format")

    stored = _nonce_store.pop(nonce, None)
    if stored is None:
        raise ValueError("Invalid or expired nonce — please re-authorize")
    stored_id, _, code_verifier = stored
    if stored_id != telegram_id:
        raise ValueError("telegram_id mismatch in state")
    return telegram_id, code_verifier


def exchange_code(code: str, code_verifier: str) -> dict:
    """Exchange an OAuth authorization code for a token dict.

    Requires the code_verifier used during the authorization request (PKCE).
    The dict is JSON-serialisable and suitable for storing in the DB.
    """
    flow = _flow()
    flow.fetch_token(code=code, code_verifier=code_verifier)
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
    Creates it if it doesn't exist.  Handles TOCTOU race on create."""
    q = (
        "name='Aruvi'"
        " and mimeType='application/vnd.google-apps.folder'"
        " and trashed=false"
    )

    def _find() -> str | None:
        result = service.files().list(q=q, spaces="drive", fields="files(id)").execute()
        files = result.get("files", [])
        return files[0]["id"] if files else None

    folder_id = _find()
    if folder_id:
        return folder_id

    try:
        folder = (
            service.files()
            .create(
                body={"name": "Aruvi", "mimeType": "application/vnd.google-apps.folder"},
                fields="id",
            )
            .execute()
        )
        return folder["id"]
    except Exception:
        folder_id = _find()
        if folder_id:
            return folder_id
        raise


GDRIVE_UPLOAD_DIR = Path("data/gdrive_upload")
CHUNK_SIZE = 10 * 1024 * 1024
MAX_GDRIVE_FILE = 4 * 1024 * 1024 * 1024


async def upload_streaming(
    token_dict: dict,
    msg: "Message",
    file_name: str,
    mime_type: str,
    file_size: int,
    folder_id: str,
    progress_callback=None,
) -> str:
    """Download the full file to NVMe temp (via 13-bot parallel streaming,
    then upload sequentially to Google Drive with 10MB chunks.
    Deletes the temp file after upload.
    """
    if file_size > MAX_GDRIVE_FILE:
        raise ValueError("File exceeds 4GB limit for GDrive upload")

    access_token = get_access_token(token_dict)

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

        total = file_size

        # ── Phase 1: Download full file to NVMe temp (13-bot parallel) ──
        GDRIVE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        tmp = GDRIVE_UPLOAD_DIR / f"{msg.id}_{int(time.time())}.tmp"

        try:
            downloaded = 0
            last_report = 0
            with open(tmp, "wb") as f:
                async for chunk in parallel_stream_generator(msg, offset=0, length=total, concurrency=10, cache=False):
                    f.write(chunk)
                    downloaded += len(chunk)
                    now = time.monotonic()
                    if progress_callback and (now - last_report >= 1 or downloaded >= total):
                        await progress_callback(downloaded, total, "Downloading from Telegram")
                        last_report = now

            # ── Phase 2: Upload sequentially to Drive ──
            uploaded = 0
            last_report = 0
            resp = None
            with open(tmp, "rb") as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    start = uploaded
                    end = uploaded + len(chunk) - 1
                    resp = await _upload_block(client, upload_url, chunk, start, end, total)
                    uploaded += len(chunk)
                    now = time.monotonic()
                    if progress_callback and (now - last_report >= 1 or uploaded >= total):
                        await progress_callback(uploaded, total, "Uploading to Google Drive")
                        last_report = now

        finally:
            tmp.unlink(missing_ok=True)

    if resp is None:
        raise RuntimeError("No chunks were uploaded (empty file?)")

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


async def _upload_block(
    client: httpx.AsyncClient, upload_url: str, block: bytes,
    start: int, end: int, total: int,
) -> httpx.Response:
    """Upload a single block with retries."""
    last_error = None
    for attempt in range(3):
        try:
            resp = await client.put(
                upload_url,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{total}",
                    "Content-Length": str(len(block)),
                },
                content=block,
            )
            if resp.status_code in (200, 201, 308):
                return resp
            last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
            if resp.status_code < 500 and resp.status_code != 429:
                break
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            last_error = str(e)
        await asyncio.sleep(1 * (attempt + 1))
    raise RuntimeError(f"Upload block failed after 3 retries: {last_error}")
