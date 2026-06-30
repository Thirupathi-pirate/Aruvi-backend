"""
Google Drive OAuth callback endpoint.
User's browser hits this after approving Google consent.
We exchange the code for tokens, store on the User record, notify via bot.
"""

import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ..database import async_session
from ..models import User
from ..gdrive import exchange_code, consume_state, generate_auth_url
from ..config import get_settings
from ..auth import get_current_user_opt
from sqlalchemy import select

_log = logging.getLogger(__name__)
router = APIRouter(prefix="/gdrive", tags=["GDrive"])
settings = get_settings()


@router.get("/auth")
async def gdrive_auth(request: Request):
    """Redirect user to Google OAuth consent screen.

    Requires a valid token. If already connected, returns status.
    """
    user = await get_current_user_opt(request)
    if not user:
        return HTMLResponse(
            "<h2>❌ Authentication required.</h2>"
            "<p>Please login via Telegram first.</p>",
            status_code=401,
        )

    if user.gdrive_token:
        return HTMLResponse(
            "<h2>✅ Google Drive already connected.</h2>"
            "<p>You can close this tab.</p>"
        )

    auth_url = generate_auth_url(user.telegram_id)
    return RedirectResponse(auth_url)


@router.get("/auth/callback")
async def gdrive_auth_callback(request: Request):
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    error = request.query_params.get("error")

    if error:
        return HTMLResponse(
            f"<h2>❌ Authorization denied.</h2><p>{error}</p>",
            status_code=400,
        )

    if not code or not state:
        return HTMLResponse(
            "<h2>❌ Missing code or state parameter.</h2>",
            status_code=400,
        )

    try:
        telegram_id, code_verifier = consume_state(state)
    except ValueError as e:
        return HTMLResponse(
            f"<h2>❌ {e}</h2>",
            status_code=400,
        )

    try:
        token_dict = exchange_code(code, code_verifier)
    except Exception as e:
        _log.exception("GDrive token exchange failed for user %s", telegram_id)
        return HTMLResponse(
            f"<h2>❌ Token exchange failed.</h2><p>{e}</p>",
            status_code=500,
        )

    # Store token on user record
    async with async_session() as db:
        result = await db.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        user = result.scalar_one_or_none()
        if user:
            user.gdrive_token = json.dumps(token_dict)
            await db.commit()

    # Notify user via Telegram bot
    try:
        from ..telegram import tg_client
        await tg_client.send_message(
            telegram_id,
            "✅ **Google Drive connected!**\n\n"
            "Your Drive account is now linked. "
            "Use the button on any file to upload it to your "
            "**Aruvi** folder in Google Drive.",
        )
    except Exception as e:
        _log.warning("Could not notify user %s: %s", telegram_id, e)

    return HTMLResponse(
        "<h2>✅ Google Drive connected!</h2>"
        "<p>You can close this tab and return to Telegram.</p>"
    )
