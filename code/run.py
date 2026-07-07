import os, sys, subprocess, threading, urllib.request, shutil, time, tarfile
from datetime import datetime, timezone, timedelta

BASE = os.path.dirname(os.path.abspath(__file__))
REPO_URL = "https://github.com/Thirupathi-pirate/Aruvi-backend.git"
REPO_DIR = os.path.join(BASE, "repo")

ENV_FILE = os.path.join(BASE, ".env")
if not os.path.exists(ENV_FILE):
    parent_env = os.path.join(BASE, "..", ".env")
    if os.path.exists(parent_env):
        ENV_FILE = os.path.abspath(parent_env)


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


def _export_env():
    """Load all vars from .env into os.environ (skip existing)."""
    if not os.path.exists(ENV_FILE):
        return
    with open(ENV_FILE) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            if k not in os.environ:
                os.environ[k] = v


# Export .env into environment so cf_tunnel etc can use os.environ.get()
_export_env()
os.chdir(BASE)

# ── fresh clone every time ─────────────────────────
if os.path.exists(REPO_DIR):
    shutil.rmtree(REPO_DIR)
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
os.environ["TELEGRAM_SESSION_DIR"] = os.path.join(CODE_DIR, "session")

# ── install deps & validate import ──────────────────
sys.path.insert(0, CODE_DIR)
import site
site.addsitedir(site.USER_SITE)
req = os.path.join(CODE_DIR, "requirements.txt")
if os.path.exists(req):
    r = subprocess.run([sys.executable, "-m", "pip", "install", "--user", "-r", req, "-q"],
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

# ── Cloudflare tunnel setup (ensure movie on tunnel) ──
# Token loaded from env (bashrc) or .env (loaded above into os.environ)
cf_token = os.environ.get("CLOUDFLARE_API_TOKEN")
if cf_token:
    try:
        from app.cf_tunnel import cleanup as cf_setup
        cf_setup()
        print("CF setup done \u2014 REDACTED_DOMAIN added to tunnel + DNS")
    except Exception as e:
        print(f"CF setup skipped (non-fatal): {e}")
else:
    print("CLOUDFLARE_API_TOKEN not set \u2014 CF setup skipped")

# ── TelePlay (bind 0.0.0.0 for HidenCloud direct port) ──
TELEPLAY_PORT = 24696
# If HidenCloud exposes a PORT env var (panel port allocation), log it
hiden_port = os.environ.get("PORT") or os.environ.get("HIDEN_PORT")
if hiden_port:
    print(f"HidenCloud port env var detected: {hiden_port}")

def run_teleplay():
    import uvicorn as uv
    uv.run(app, host="0.0.0.0", port=TELEPLAY_PORT, log_level="info")

threading.Thread(target=run_teleplay, daemon=True).start()
print(f"TelePlay listening on 0.0.0.0:{TELEPLAY_PORT}")
print("TelePlay on HidenCloud public port 24696 (direct) and via tunnel at REDACTED_DOMAIN")
print("Tunnel CNAME: REDACTED_DOMAIN -> REDACTED_TUNNEL")

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
            url = f"http://localhost:24696{path}"
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

def _download_tgz(url, dest_bin):
    if os.path.exists(dest_bin):
        os.chmod(dest_bin, 0o755)
        return
    tgz = dest_bin + ".tar.gz"
    print(f"Downloading {os.path.basename(dest_bin)}...")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            with open(tgz, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        with tarfile.open(tgz, "r:gz") as tar:
            for m in tar.getmembers():
                if m.name == "opencode" or m.name.endswith("/opencode"):
                    with open(dest_bin, "wb") as f:
                        f.write(tar.extractfile(m).read())
                    break
        os.chmod(dest_bin, 0o755)
        os.remove(tgz)
        print(f"  Done ({os.path.getsize(dest_bin)//1048576}MiB)")
    except Exception as e:
        print(f"  Failed: {e}")
        if os.path.exists(tgz):
            os.remove(tgz)

# ── Cloudflare Tunnel ───────────────────────────────
tunnel_token = _load_env("TUNNEL_TOKEN")
cf_bin = os.path.join(BASE, "cloudflared")
if tunnel_token:
    _download("https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64", cf_bin)
    if os.path.exists(cf_bin):
        try:
            os.chmod(cf_bin, 0o755)
            subprocess.Popen([cf_bin, "tunnel", "run", "--protocol", "http2", "--token", tunnel_token],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("cloudflared tunnel started (HTTP/2)")
        except Exception as e:
            print(f"cloudflared start failed: {e}")
    else:
        print("cloudflared binary not available after download attempt")
else:
    print("TUNNEL_TOKEN not found in .env — tunnel skipped")

# ── opencode ─────────────────────────────────────────
opencode_bin = os.path.join(BASE, "opencode")
_download_tgz("https://github.com/anomalyco/opencode/releases/latest/download/opencode-linux-arm64.tar.gz", opencode_bin)
if os.path.exists(opencode_bin):
    try:
        subprocess.Popen([opencode_bin, "web", "--hostname", "127.0.0.1", "--port", "7444"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("opencode started on :7444 (web + API)")
    except Exception as e:
        print(f"opencode start failed: {e}")

# ── opencode Telegram Bot (grinev) ──────────────────
opencode_bot_token = _load_env("OPENCODE_BOT_TOKEN")
opencode_bot_user_id = _load_env("OPENCODE_BOT_USER_ID")
if opencode_bot_token and opencode_bot_user_id:
    node_bin = shutil.which("node")
    npx_bin = shutil.which("npx")
    if not node_bin:
        node_dir = os.path.join(BASE, "node")
        node_archive = os.path.join(BASE, "node.tar.gz")
        node_bin = os.path.join(node_dir, "bin", "node")
        npx_bin = os.path.join(node_dir, "bin", "npx")
        if not os.path.exists(node_bin):
            print("Downloading Node.js for ARM64...")
            try:
                with urllib.request.urlopen(
                    "https://nodejs.org/dist/v22.14.0/node-v22.14.0-linux-arm64.tar.gz",
                    timeout=120,
                ) as resp:
                    with open(node_archive, "wb") as f:
                        while True:
                            chunk = resp.read(65536)
                            if not chunk:
                                break
                            f.write(chunk)
                os.makedirs(node_dir, exist_ok=True)
                with tarfile.open(node_archive, "r:gz") as tar:
                    for m in tar.getmembers():
                        name = m.name.removeprefix("node-v22.14.0-linux-arm64/")
                        if not name:
                            continue
                        dest = os.path.join(node_dir, name)
                        if m.isdir():
                            os.makedirs(dest, exist_ok=True)
                        elif m.issym() or m.islnk():
                            if os.path.exists(dest) or os.path.islink(dest):
                                os.remove(dest)
                            os.symlink(m.linkname, dest)
                        else:
                            with open(dest, "wb") as f:
                                f.write(tar.extractfile(m).read())
                            os.chmod(dest, 0o755 if name.startswith("bin/") else 0o644)
                os.remove(node_archive)
                print("  Node.js installed")
            except Exception as e:
                print(f"  Node.js download failed: {e}")
                node_bin, npx_bin = None, None
    if node_bin and os.path.exists(node_bin):
        bot_config_dir = os.path.expanduser("~/.config/opencode-telegram-bot")
        os.makedirs(bot_config_dir, exist_ok=True)
        with open(os.path.join(bot_config_dir, ".env"), "w") as f:
            f.write(f"TELEGRAM_BOT_TOKEN={opencode_bot_token}\n")
            f.write(f"TELEGRAM_ALLOWED_USER_ID={opencode_bot_user_id}\n")
            f.write("OPENCODE_API_URL=http://127.0.0.1:7444\n")
            f.write("OPENCODE_MODEL_PROVIDER=opencode\n")
            f.write("OPENCODE_MODEL_ID=big-pickle\n")
        env = os.environ.copy()
        env.pop("TELEGRAM_BOT_TOKEN", None)  # don't leak TelePlay bot token
        env["PATH"] = os.path.dirname(node_bin) + ":" + env.get("PATH", "")
        try:
            subprocess.Popen(
                [npx_bin, "@grinev/opencode-telegram-bot@latest"],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            print("OpenCode Telegram Bot started")
        except Exception as e:
            print(f"OpenCode Telegram Bot failed: {e}")
    else:
        print("Node.js not available — OpenCode Telegram Bot skipped")
else:
    print("OPENCODE_BOT_TOKEN / OPENCODE_BOT_USER_ID not set — OpenCode Telegram Bot skipped")

# ── startup health check ────────────────────────────
for i in range(30):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{TELEPLAY_PORT}/health", timeout=2):
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
    try:
        time.sleep(secs)
    except KeyboardInterrupt:
        print("Received stop signal — exiting")
        sys.stdout.flush()
        os._exit(0)
    print("Scheduled restart — exiting for fresh IP")
    sys.stdout.flush()
    os._exit(0)