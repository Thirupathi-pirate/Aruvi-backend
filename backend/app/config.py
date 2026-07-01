import secrets
import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache

logger = logging.getLogger(__name__)


def _auto_jwt_secret() -> str:
    logger.warning("JWT_SECRET not set — auto-generated (sessions invalidate on restart)")
    return secrets.token_hex(32)


class Settings(BaseSettings):
    # Telegram
    telegram_api_id: int
    telegram_api_hash: str
    telegram_bot_token: str

    telegram_helper_bot_tokens_str: str = Field("", alias="TELEGRAM_HELPER_BOT_TOKENS")
    telegram_bot_session_strings_str: str = Field("", alias="TELEGRAM_BOT_SESSION_STRINGS")

    auth_users_str: str = Field("", alias="AUTH_USERS")
    admin_ids_str: str = Field("", alias="ADMIN_IDS")

    @property
    def auth_users(self) -> list[int]:
        v = self.auth_users_str
        if not v:
            return []
        try:
            return [int(u.strip()) for u in v.split(",") if u.strip()]
        except ValueError:
            return []

    @property
    def admin_ids(self) -> set[int]:
        v = self.admin_ids_str
        if not v:
            return set()
        try:
            return {int(u.strip()) for u in v.split(",") if u.strip()}
        except ValueError:
            return set()

    @property
    def telegram_helper_bot_tokens(self) -> list[str]:
        v = self.telegram_helper_bot_tokens_str
        if not v:
            return []
        return [t.strip() for t in v.split(",") if t.strip()]

    @property
    def all_bot_tokens(self) -> list[str]:
        return [self.telegram_bot_token] + self.telegram_helper_bot_tokens

    @property
    def telegram_bot_session_strings(self) -> list[str]:
        v = self.telegram_bot_session_strings_str
        if not v:
            return []
        return [s.strip() for s in v.split(",") if s.strip()]

    telegram_storage_channel_id: int

    # Database (default: Supabase PostgreSQL)
    database_url: str = "postgresql+asyncpg://postgres.nphhqhlfjuixeebqtmvb:aarsha7736740245%40@aws-1-ap-northeast-1.pooler.supabase.com:5432/postgres"

    # JWT — set JWT_SECRET for persistent sessions across restarts
    # Generate with: openssl rand -hex 32
    jwt_secret: str = Field(default_factory=_auto_jwt_secret)
    jwt_expiry_minutes: int = 10080

    # Server
    server_host: str = "0.0.0.0"
    server_port: int = Field(7446, alias="SERVER_PORT")

    # Concurrency
    telegram_client_concurrency: int = 5

    # Timeouts (Telegram-Drive inspired)
    telegram_connect_timeout: int = 30
    telegram_timeout: int = 60

    # Proxy (Cloudflare WARP SOCKS5)
    telegram_proxy_enabled: bool = False

    # Google Drive
    gdrive_client_id: str = ""
    gdrive_client_secret: str = ""
    gdrive_redirect_uri: str = "https://REDACTED_DOMAIN/api/gdrive/auth/callback"

    # Debug
    debug_password: str = ""  # Set via .env: DEBUG_PASSWORD=yourpass

    # Web
    web_base_url: str = "https://REDACTED_DOMAIN"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()
