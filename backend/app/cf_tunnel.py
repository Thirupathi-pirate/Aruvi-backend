"""
Cloudflare API helpers — remove REDACTED_DOMAIN from tunnel ingress + DNS
at startup so port 24696 can be exposed directly via HidenCloud.
"""
import os
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE = "https://api.cloudflare.com/client/v4"


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


def remove_movie_ingress(token, account_id, tunnel_id):
    config = _get(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations")
    ingress = config.get("config", {}).get("ingress", [])
    if not ingress:
        logger.warning("No ingress rules in tunnel config")
        return False

    before = len(ingress)
    config["config"]["ingress"] = [
        r for r in ingress if r.get("hostname") != "REDACTED_DOMAIN"
    ]
    if len(config["config"]["ingress"]) == before:
        logger.info("REDACTED_DOMAIN not found in tunnel ingress")
        return False

    _put(token, f"/accounts/{account_id}/cfd_tunnel/{tunnel_id}/configurations", config)
    logger.info("Removed REDACTED_DOMAIN from tunnel ingress")
    return True


def delete_movie_dns(token):
    zones = _get(token, "/zones?name=aaruvi.space")
    if not zones:
        logger.info("Zone 'aaruvi.space' not found — DNS skip")
        return False
    zone_id = zones[0]["id"]

    records = _get(token, f"/zones/{zone_id}/dns_records?name=REDACTED_DOMAIN")
    if not records:
        logger.info("No DNS record for REDACTED_DOMAIN")
        return False

    for rec in records:
        _delete(token, f"/zones/{zone_id}/dns_records/{rec['id']}")
        logger.info(
            "Deleted DNS record %s %s \u2192 %s",
            rec["type"], rec["name"], rec.get("content", "?"),
        )
    return True


def cleanup(token=None):
    token = token or os.environ.get("CLOUDFLARE_API_TOKEN")
    if not token:
        logger.warning("CLOUDFLARE_API_TOKEN not set \u2014 skipping CF cleanup")
        return

    try:
        aid = get_account_id(token)
        tid, tname = find_tunnel(token, aid)
        logger.info("Tunnel: %s (%s)", tname, tid)
        remove_movie_ingress(token, aid, tid)
        delete_movie_dns(token)
    except CFApiError as e:
        logger.error("CF cleanup failed: %s", e)
