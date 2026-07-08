#!/bin/bash
# Entrypoint for HF Spaces — CF Workers proxy + cloudflared dual mode + opencode
set -euo pipefail

_CF_ENV="/tmp/huggingpost-cloudflare-proxy.env"

# --- CF Workers proxy (api.telegram.org) ---
if [ -n "${CLOUDFLARE_WORKERS_TOKEN:-}" ]; then
    echo "Setting up Cloudflare proxy worker..."
    python3 /opt/cf-proxy/cloudflare-proxy-setup.py 2>&1 \
        || echo "Warning: Cloudflare proxy setup failed — continuing without proxy"
else
    echo "CLOUDFLARE_WORKERS_TOKEN not set — skipping Cloudflare proxy setup"
fi

if [ -f "${_CF_ENV}" ]; then
    . "${_CF_ENV}"
fi

if [ -n "${CLOUDFLARE_PROXY_URL:-}" ] && [ -f /opt/cf-proxy/cloudflare-proxy.js ]; then
    export NODE_OPTIONS="${NODE_OPTIONS:-} --require /opt/cf-proxy/cloudflare-proxy.js"
    echo "Cloudflare proxy active via NODE_OPTIONS"
fi

# --- cloudflared: SOCKS5 for MTProto outbound + ingress for opencode ---
if [ -n "${TUNNEL_TOKEN:-}" ]; then
    echo "Starting cloudflared SOCKS5 proxy..."
    cloudflared tunnel --socks5 localhost:1080 --token "$TUNNEL_TOKEN" \
        2>&1 | sed 's/^/[cloudflared-socks5] /' &

    echo "Starting cloudflared tunnel connector (ingress for opencode)..."
    cloudflared tunnel run --token "$TUNNEL_TOKEN" \
        2>&1 | sed 's/^/[cloudflared-ingress] /' &

    sleep 5
    export TELEGRAM_SOCKS5_PROXY="socks5://localhost:1080"
    echo "cloudflared SOCKS5 active on localhost:1080"
    echo "cloudflared tunnel ingress connector running"
else
    echo "TUNNEL_TOKEN not set — skipping cloudflared"
fi

# --- opencode web UI ---
if command -v opencode &>/dev/null && [ -f /app/opencode.json ]; then
    echo "Starting opencode web on port 7444..."
    opencode web --config /app/opencode.json 2>&1 \
        | sed 's/^/[opencode] /' &
else
    echo "opencode not found — skipping"
fi

# --- CF tunnel/DNS setup via API ---
if [ -n "${CLOUDFLARE_API_TOKEN:-}" ]; then
    echo "Running Cloudflare tunnel ingress/DNS setup..."
    python3 -c "
import sys; sys.path.insert(0, '/app')
from app.cf_tunnel import cleanup
cleanup()
" 2>&1 | sed 's/^/[cf-setup] /' || echo "Warning: CF setup failed — configure ingress manually"
fi

# --- Start the main app ---
if [ -z "${APP_START_CMD:-}" ]; then
    echo "ERROR: APP_START_CMD not set"
    exit 1
fi

echo "Starting application: ${APP_START_CMD}"
exec ${APP_START_CMD}
