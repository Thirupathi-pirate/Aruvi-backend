FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# cloudflared tunnel (ingress only — for opencode subdomain routing)
RUN curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
    -o /usr/local/bin/cloudflared && chmod +x /usr/local/bin/cloudflared

# opencode web UI (debugging)
RUN curl -fsSL https://github.com/anomalyco/opencode/releases/latest/download/opencode-linux-amd64 \
    -o /usr/local/bin/opencode && chmod +x /usr/local/bin/opencode

COPY backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY backend/ /app
COPY opencode.json /app/opencode.json
WORKDIR /app

COPY start.sh /start.sh
RUN chmod +x /start.sh

RUN mkdir -p /app/data /app/session

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -sf http://localhost:7860/health || exit 1

ENTRYPOINT ["/start.sh"]
