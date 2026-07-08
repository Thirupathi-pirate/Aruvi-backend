"""
Cloudflare API helpers — ensure REDACTED_DOMAIN is served via tunnel.
Adds/verifies tunnel ingress + CNAME DNS so port 24696 routes through CF.
"""
import os
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://api.cloudflare.com/client/v4"
TUNNEL_HOSTNAME = "REDACTED_TUNNEL"


class CFApiError(Exception):
    pass


def _headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _get(token, path):
    with httpx.Client() as c:
        r = c.get(f"{API_BASE}{path}", headers=_headers(token), timeout=15)
        if r.status_code != 200:
            raise CFApiError(f"GET {path} failed: {r.status_code} {r.text[:200]}")
        data = r.json()
        if not data.get("success"):
            raise CFApiError(f"GET {path} API error: {data.get('errors', [])}")
        return data["result"]


def _put(token, path, body):
    with httpx.Client() as c:
        r = c.put(f"{API_BASE}{path}", headers=_headers(token), json=body, timeout=15)
        if r.status_code != 200:
            raise CFApiError(f"PUT {path} failed: {r.status_code} {r.text[:200]}")
        data = r.json()
        if not data.get("success"):
            raise CFApiError(f"PUT {path} API error: {data.get('errors', [])}")
        return data["result"]


def _post(token, path, body):
    with httpx.Client() as c:
        r = c.post(f"{API_BASE}{path}", headers=_headers(token), json=body, timeout=15)
        if r.status_code != 200:
            raise CFApiError(f"POST {path} failed: {r.status_code} {r.text[:200]}")
        data = r.json()
        if not data.get("success"):
            raise CFApiError(f"POST {path} API error: {data.get('errors', [])}")
        return data["result"]


def _delete(token, path):
    with httpx.Client() as c:
        r = c.delete(f"{API_BASE}{path}", headers=_headers(token), timeout=15)
        if r.status_code not in (200, 204):
            raise CFApiError(f"DELETE {path} failed: {r.status_code} {r.text[:200]}")
        if r.status_code == 204:
            return
        data = r.json()
        if not data.get("success"):
            raise CFApiError(f"DELETE {path} API error: {data.get('errors', [])}")


def get_account_id(token):
    accounts = _get(token, "/accounts")
    if not accounts:
        raise CFApiError("No Cloudflare accounts found")
    return accounts[0]["id"]


def find_tunnel(token, account_id, tunnel_name=None):
    tunnels = _get(token, f"/accounts/{account_id}/cfd_tunnel")
    if not tunnels:
        raise CFApiError("No tunnels found in account")
    if tunnel_name:
        for t in tunnels:
            if t.get("name") == tunnel_name:
                return t["id"], t["name"]
        raise CFApiError(f"Tunnel '{tunnel_name}' not found")
    return tunnels[0]["id"], tunnels[0].get("name", "unknown")


def get_zone_id(token):
    zones = _get(token, "/zones?name=aaruvi.space")
    if not zones:
        raise CFApiError("Zone 'aaruvi.space' not found")
    return zones[0]["id"]


def add_movie_ingress(token, account_id, tunnel_id):
    config = _get(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations")
    ingress = config.get("config", {}).get("ingress", [])

    catch_all = [r for r in ingress if r.get("hostname") is None]
    others = [r for r in ingress if r.get("hostname") is not None]

    existing = [r for r in others if r.get("hostname") == "REDACTED_DOMAIN"]
    if existing:
        logger.info("REDACTED_DOMAIN already in tunnel ingress")
        return False

    others.append({"hostname": "REDACTED_DOMAIN", "service": "http://localhost:24696"})
    config["config"]["ingress"] = others + catch_all
    _put(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations", config)
    logger.info("Added REDACTED_DOMAIN → localhost:24696 to tunnel ingress")
    return True


def _ensure_dns(token, zone_id, subdomain):
    """Add CNAME for <subdomain>.aaruvi.space → tunnel if missing."""
    fqdn = f"{subdomain}.aaruvi.space"
    records = _get(token, f"/zones/{zone_id}/dns_records?name={fqdn}")
    if records:
        existing = records[0]
        if existing.get("type") == "CNAME" and existing.get("content") == TUNNEL_HOSTNAME:
            logger.info("CNAME for %s already correct — skipping", fqdn)
            return True
        if existing.get("type") == "A":
            _delete(token, f"/zones/{zone_id}/dns_records/{existing['id']}")
            logger.info("Deleted stale A record for %s", fqdn)
    body = {
        "type": "CNAME",
        "name": subdomain,
        "content": TUNNEL_HOSTNAME,
        "ttl": 120,
        "proxied": True,
    }
    _post(token, f"/zones/{zone_id}/dns_records", body)
    logger.info("Created CNAME %s → %s", fqdn, TUNNEL_HOSTNAME)
    return True


def _ensure_ingress(token, account_id, tunnel_id, hostname, service_url):
    """Add a tunnel ingress rule if not already present."""
    config = _get(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations")
    ingress = config.get("config", {}).get("ingress", [])
    catch_all = [r for r in ingress if r.get("hostname") is None]
    others = [r for r in ingress if r.get("hostname") is not None]
    if any(r.get("hostname") == hostname for r in others):
        logger.info("%s already in tunnel ingress — skipping", hostname)
        return False
    others.append({"hostname": hostname, "service": service_url})
    config["config"]["ingress"] = others + catch_all
    _put(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations", config)
    logger.info("Added %s → %s to tunnel ingress", hostname, service_url)
    return True


def cleanup(token=None):
    token = token or os.environ.get("CLOUDFLARE_API_TOKEN")
    if not token:
        logger.warning("CLOUDFLARE_API_TOKEN not set — skipping CF setup")
        return

    try:
        aid = get_account_id(token)
        tid, tname = find_tunnel(token, aid)
        logger.info("Tunnel: %s (%s)", tname, tid)

        # Remove old movie ingress (now served via HF Space URL directly)
        config = _get(token, f"/accounts/{aid}/cfd_tunnel/{tid}/configurations")
        ingress = config.get("config", {}).get("ingress", [])
        catch_all = [r for r in ingress if r.get("hostname") is None]
        others = [r for r in ingress if r.get("hostname") is not None]
        before = len(others)
        others = [r for r in others if r.get("hostname") != "REDACTED_DOMAIN"]
        if len(others) < before:
            config["config"]["ingress"] = others + catch_all
            _put(token, f"/accounts/{aid}/cfd_tunnel/{tid}/configurations", config)
            logger.info("Removed REDACTED_DOMAIN from tunnel ingress (moved to HF Space URL)")

        # Add opencode ingress
        _ensure_ingress(token, aid, tid, "REDACTED_DOMAIN", "http://localhost:7444")

        # Add DNS for opencode subdomain
        zid = get_zone_id(token)
        _ensure_dns(token, zid, "opencode")
    except CFApiError as e:
        logger.error("CF setup failed: %s", e)
