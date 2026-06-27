from slowapi import Limiter
from slowapi.util import get_remote_address


def _rate_limit_key(request) -> str:
    """Use Cf-Connecting-Ip header (set by Cloudflare Tunnel) for per-user
    rate limiting. Falls back to remote address for non-CF traffic."""
    cf_ip = request.headers.get("Cf-Connecting-Ip")
    if cf_ip:
        return cf_ip
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return get_remote_address(request)


limiter = Limiter(key_func=_rate_limit_key)
