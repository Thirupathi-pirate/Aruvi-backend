#!/bin/bash
set -euo pipefail

# cloudflared tunnel ingress (REDACTED_DOMAIN)
if [ -n "${TUNNEL_TOKEN:-}" ]; then
    echo "Starting cloudflared tunnel ingress..."
    cloudflared tunnel run --token "$TUNNEL_TOKEN" 2>&1 | sed 's/^/[cloudflared] /' &
    sleep 3
fi

# opencode web UI (debugging)
if command -v opencode &>/dev/null && [ -f /app/opencode.json ]; then
    echo "Starting opencode web on port 7444..."
    opencode web --config /app/opencode.json 2>&1 | sed 's/^/[opencode] /' &
fi

# TelePlay on port 7860 (served directly by HF Spaces)
exec uvicorn app.main:app --host 0.0.0.0 --port 7860
