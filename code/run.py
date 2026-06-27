import os, sys, subprocess, threading, urllib.request, shutil, time
from datetime import datetime, timezone, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))
REPO_URL = "https://github.com/Thirupathi-pirate/Aruvi-backend.git"
REPO_DIR = os.path.join(BASE, "repo")
ENV_FILE = os.path.join(BASE, ".env")

os.chdir(BASE)

# ── clone / pull repo ──────────────────────────────
if os.path.exists(REPO_DIR):
    r = subprocess.run(["git", "-C", REPO_DIR, "pull", "--ff-only"], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"git pull --ff-only failed, trying fetch+reset: {r.stderr.strip()}")
        subprocess.run(["git", "-C", REPO_DIR, "fetch", "origin", "main"], capture_output=True)
        r2 = subprocess.run(["git", "-C", REPO_DIR, "reset", "--hard", "origin/main"], capture_output=True, text=True)
        if r2.returncode != 0:
            print(f"git reset also failed: {r2.stderr.strip()} — continuing with stale repo")
else:
    r = subprocess.run(["git", "clone", "--depth=1", REPO_URL, REPO_DIR], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"git clone failed (stderr): {r.stderr.strip()}")
        sys.exit(1)

if not os.path.isdir(os.path.join(REPO_DIR, "backend")):
    print("ERROR: repo cloned but backend/ not found")
    sys.exit(1)

CODE_DIR = os.path.join(REPO_DIR, "backend")
os.chdir(CODE_DIR)

# ── copy .env into repo ─────────────────────────────
if os.path.exists(ENV_FILE):
    shutil.copy2(ENV_FILE, os.path.join(REPO_DIR, ".env"))
    shutil.copy2(ENV_FILE, os.path.join(CODE_DIR, ".env"))

os.makedirs(os.path.join(CODE_DIR, "data"), exist_ok=True)
os.makedirs(os.path.join(CODE_DIR, "session"), exist_ok=True)
os.environ["MEMORY"] = "3Gi"

# ── install deps & validate import ──────────────────
sys.path.insert(0, CODE_DIR)
req = os.path.join(CODE_DIR, "requirements.txt")
if os.path.exists(req):
    r = subprocess.run([sys.executable, "-m", "pip", "install", "-r", req, "-q"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        print(f"pip install failed:\n{r.stderr.strip()}")
        sys.exit(1)

try:
    from app.main import app
except Exception:
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ── TelePlay ────────────────────────────────────────
def run_teleplay():
    import uvicorn as uv
    uv.run(app, host="127.0.0.1", port=7446, log_level="info")

threading.Thread(target=run_teleplay, daemon=True).start()

# ── Monitor ─────────────────────────────────────────
STATIC_DIR = os.path.join(CODE_DIR, "app", "static")

def run_monitor():
    import uvicorn as uv
    import httpx

    async def monitor_app(scope, receive, send):
        if scope["type"] != "http":
            return
        path = scope["path"]
        if path in ("", "/"):
            path = "/status.html"
        rel = path[1:] if path.startswith("/") else path
        file_path = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not file_path.startswith(STATIC_DIR):
            await send({"type": "http.response.start", "status": 403,
                        "headers": [(b"content-type", b"text/plain")]})
            await send({"type": "http.response.body", "body": b"Forbidden"})
            return
        if os.path.isfile(file_path):
            ext = path.rsplit(".", 1)[-1]
            ct = {"html": "text/html", "js": "application/javascript",
                  "css": "text/css", "png": "image/png", "svg": "image/svg+xml",
                  "ico": "image/x-icon"}.get(ext, "application/octet-stream")
            with open(file_path, "rb") as f:
                content = f.read()
            await send({"type": "http.response.start", "status": 200,
                        "headers": [(b"content-type", ct.encode())]})
            await send({"type": "http.response.body", "body": content})
            return

        if path.startswith("/api/") or path in ("/health", "/diag"):
            body = b""
            more = True
            while more:
                msg = await receive()
                body += msg.get("body", b"")
                more = msg.get("more_body", False)
            qs = scope.get("query_string", b"")
            url = f"http://localhost:7446{path}"
            if qs:
                url += "?" + qs.decode("utf-8", errors="replace")
            fwd_headers = {}
            for k, v in scope.get("headers", []):
                kl = k.decode().lower()
                if kl != "host":
                    fwd_headers[kl] = v.decode("utf-8", errors="replace")
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.request(scope["method"], url,
                        content=body or None, headers=fwd_headers)
                    hdrs = [(k, v) for k, v in resp.headers.raw]
                    await send({"type": "http.response.start", "status": resp.status_code, "headers": hdrs})
                    await send({"type": "http.response.body", "body": resp.content})
                except Exception as e:
                    await send({"type": "http.response.start", "status": 502,
                                "headers": [(b"content-type", b"text/plain")]})
                    await send({"type": "http.response.body", "body": str(e).encode()})
            return

        await send({"type": "http.response.start", "status": 404,
                    "headers": [(b"content-type", b"text/plain")]})
        await send({"type": "http.response.body", "body": b"Not found"})

    uv.run(monitor_app, host="127.0.0.1", port=7442, log_level="info")

threading.Thread(target=run_monitor, daemon=True).start()

# ── helpers ─────────────────────────────────────────
def _load_env(key):
    if not os.path.exists(ENV_FILE):
        return None
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            if k.strip() == key:
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                    v = v[1:-1]
                return v
    return None

def _download(url, dest):
    if os.path.exists(dest):
        os.chmod(dest, 0o755)
        return
    print(f"Downloading {os.path.basename(dest)}...")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            with open(dest, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        os.chmod(dest, 0o755)
        print(f"  Done ({os.path.getsize(dest)//1048576}MiB)")
    except Exception as e:
        print(f"  Failed: {e}")

# ── Cloudflare Tunnel ───────────────────────────────
tunnel_token = _load_env("TUNNEL_TOKEN")
cf_bin = os.path.join(BASE, "..", "cloudflared")
if tunnel_token:
    _download("https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64", cf_bin)
    if os.path.exists(cf_bin):
        try:
            os.chmod(cf_bin, 0o755)
            subprocess.Popen([cf_bin, "tunnel", "run", "--token", tunnel_token],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("cloudflared tunnel started")
        except Exception as e:
            print(f"cloudflared start failed: {e}")
    else:
        print("cloudflared binary not available after download attempt")
else:
    print("TUNNEL_TOKEN not found in .env — tunnel skipped")

# ── opencode ─────────────────────────────────────────
opencode_bin = os.path.join(BASE, "..", "opencode")
_download("https://github.com/opencode-ai/opencode/releases/latest/download/opencode-linux-arm64", opencode_bin)
if os.path.exists(opencode_bin):
    try:
        subprocess.Popen([opencode_bin, "web", "--hostname", "127.0.0.1", "--port", "7444"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("opencode web ui started on :7444")
    except Exception as e:
        print(f"opencode start failed: {e}")

# ── startup health check ────────────────────────────
for i in range(30):
    try:
        with urllib.request.urlopen("http://127.0.0.1:7446/health", timeout=2):
            pass
        print("TelePlay is healthy")
        break
    except Exception:
        if i == 29:
            print("WARNING: TelePlay health check failed after 30s — continuing anyway")
        time.sleep(1)

# ── daily restart at 3:30 AM IST ─────────────────────
IST = timezone(timedelta(hours=5, minutes=30))

def _secs_until_0330_ist():
    now = datetime.now(IST)
    target = now.replace(hour=3, minute=30, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return (target - now).total_seconds()

while True:
    secs = _secs_until_0330_ist()
    h, m = divmod(int(secs), 3600)
    m, s = divmod(m, 60)
    print(f"Next restart at 3:30 AM IST (in {h}h {m}m {s}s)")
    time.sleep(secs)
    print("Scheduled restart — exiting for fresh IP")
    sys.stdout.flush()
    os._exit(0)