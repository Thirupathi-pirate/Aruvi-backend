#!/bin/bash
# Simplified entrypoint for HF Spaces — only CF proxy, no keepalive/backup
set -euo pipefail

_CF_ENV="/tmp/huggingpost-cloudflare-proxy.env"

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

if [ -n "${TUNNEL_TOKEN:-}" ]; then
    echo "Starting cloudflared SOCKS5 proxy..."
    cloudflared tunnel --socks5 localhost:1080 --token "$TUNNEL_TOKEN" \
        2>&1 | sed 's/^/[cloudflared] /' &
    sleep 3
    export TELEGRAM_SOCKS5_PROXY="socks5://localhost:1080"
    echo "cloudflared SOCKS5 active on localhost:1080"
fi

if [ -z "${APP_START_CMD:-}" ]; then
    echo "ERROR: APP_START_CMD not set"
    exit 1
fi

echo "Starting application: ${APP_START_CMD}"
exec ${APP_START_CMD}
