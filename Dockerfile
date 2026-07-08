FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gnupg build-essential \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
        | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && NODE_MAJOR=20 \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_${NODE_MAJOR}.x nodistro main" \
        > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir PyYAML

COPY cf-proxy/cloudflare-proxy-setup.py /opt/cf-proxy/cloudflare-proxy-setup.py
COPY cf-proxy/cloudflare-proxy.js         /opt/cf-proxy/cloudflare-proxy.js
COPY cf-proxy/start.sh                    /opt/cf-proxy/start.sh

RUN chmod +x /opt/cf-proxy/start.sh

COPY backend/ /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/data /app/session

ENV APP_START_CMD="uvicorn app.main:app --host 0.0.0.0 --port 7860"

EXPOSE 7860

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -sf http://localhost:7860/health || exit 1

ENTRYPOINT ["/opt/cf-proxy/start.sh"]
