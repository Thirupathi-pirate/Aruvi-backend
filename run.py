import os
import sys
import subprocess
import shutil
import threading
import urllib.request
import urllib.error

BASE = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)


def _load_env(key: str, env_path: str | None = None) -> str | None:
    """Simple .env parser — no external deps needed."""
    if env_path is None:
        env_path = os.path.join(BASE, '.env')
    if not os.path.exists(env_path):
        return None
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, _, v = line.partition('=')
            if k.strip() == key:
                v = v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                    v = v[1:-1]
                return v
    return None


def _download(url: str, dest: str):
    """Download a file using urllib (no curl/wget needed), streaming to disk."""
    if os.path.exists(dest):
        os.chmod(dest, 0o755)
        return
    print(f"Downloading {os.path.basename(dest)}...")
    try:
        with urllib.request.urlopen(url, timeout=120) as resp:
            total = int(resp.headers.get('Content-Length', 0))
            wrote = 0
            with open(dest, 'wb') as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
                    wrote += len(chunk)
                    if total:
                        pct = wrote * 100 // total
                        print(f"\r  {pct}% ({wrote//1048576}MiB / {total//1048576}MiB)", end='', flush=True)
        os.chmod(dest, 0o755)
        size = os.path.getsize(dest)
        print(f"\n  Done — {size} bytes ({size//1048576}MiB)")
    except Exception as e:
        print(f"  Failed: {e}")


# ── env ───────────────────────────────────────────────
root_env = os.path.join(BASE, '.env')
backend_env = os.path.join(BASE, 'backend', '.env')
if os.path.exists(root_env):
    shutil.copy2(root_env, backend_env)

os.makedirs('backend/data', exist_ok=True)
os.makedirs('backend/session', exist_ok=True)
os.environ['MEMORY'] = '3Gi'

# ── deps ──────────────────────────────────────────────
subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'backend/requirements.txt', '-q'], capture_output=True)

# ── TelePlay (internal :7446, thread) ─────────────────
backend_dir = os.path.join(BASE, 'backend')

def run_teleplay():
    sys.path.insert(0, backend_dir)
    os.chdir(backend_dir)
    from app.main import app
    import uvicorn as uv
    uv.run(app, host='127.0.0.1', port=7446, log_level='info')

threading.Thread(target=run_teleplay, daemon=True).start()

# ── Monitor (internal :7442, thread) ──────────────────
STATIC_DIR = os.path.join(backend_dir, 'app', 'static')

def run_monitor():
    import uvicorn as uv
    import httpx

    async def monitor_app(scope, receive, send):
        if scope['type'] != 'http':
            return

        path = scope['path']

        if path in ('', '/'):
            path = '/status.html'

        file_path = os.path.normpath(os.path.join(STATIC_DIR, path.lstrip('/')))
        if not file_path.startswith(STATIC_DIR):
            await send({'type': 'http.response.start', 'status': 403,
                        'headers': [(b'content-type', b'text/plain')]})
            await send({'type': 'http.response.body', 'body': b'Forbidden'})
            return
        if os.path.isfile(file_path):
            ext = path.rsplit('.', 1)[-1]
            ct = {'html': 'text/html', 'js': 'application/javascript',
                  'css': 'text/css', 'png': 'image/png', 'svg': 'image/svg+xml',
                  'ico': 'image/x-icon'}.get(ext, 'application/octet-stream')
            with open(file_path, 'rb') as f:
                content = f.read()
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'content-type', ct.encode())]})
            await send({'type': 'http.response.body', 'body': content})
            return

        if path.startswith('/api/') or path in ('/health', '/diag'):
            body = b''
            more = True
            while more:
                msg = await receive()
                body += msg.get('body', b'')
                more = msg.get('more_body', False)
            qs = scope.get('query_string', b'')
            url = f'http://localhost:7446{path}'
            if qs:
                url += '?' + qs.decode('utf-8', errors='replace')
            fwd_headers = {}
            for k, v in scope.get('headers', []):
                kl = k.decode().lower()
                if kl not in ('authorization', 'cookie', 'host', 'origin', 'referer'):
                    fwd_headers[kl] = v.decode('utf-8', errors='replace')
            async with httpx.AsyncClient(timeout=30) as client:
                try:
                    resp = await client.request(scope['method'], url,
                        content=body or None, headers=fwd_headers)
                    hdrs = [(b'content-type', resp.headers.get('content-type', 'application/json').encode())]
                    await send({'type': 'http.response.start', 'status': resp.status_code, 'headers': hdrs})
                    await send({'type': 'http.response.body', 'body': resp.content})
                except Exception as e:
                    await send({'type': 'http.response.start', 'status': 502,
                                'headers': [(b'content-type', b'text/plain')]})
                    await send({'type': 'http.response.body', 'body': str(e).encode()})
            return

        await send({'type': 'http.response.start', 'status': 404,
                    'headers': [(b'content-type', b'text/plain')]})
        await send({'type': 'http.response.body', 'body': b'Not found'})

    uv.run(monitor_app, host='127.0.0.1', port=7442, log_level='info')

threading.Thread(target=run_monitor, daemon=True).start()

# ── Cloudflare Tunnel ─────────────────────────────────
tunnel_token = _load_env('TUNNEL_TOKEN')
if not tunnel_token:
    tunnel_token = _load_env('TUNNEL_TOKEN', os.path.join(BASE, 'backend', '.env'))
if not tunnel_token:
    tunnel_token = _load_env('TUNNEL_TOKEN', os.path.join(BASE, 'backend', 'app', '.env'))
cf_bin = os.path.join(BASE, 'cloudflared')
if tunnel_token:
    _download('https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64', cf_bin)
    if os.path.exists(cf_bin):
        try:
            os.chmod(cf_bin, 0o755)
            cf_log = open(os.path.join(BASE, 'cf.log'), 'a')
            subprocess.Popen([cf_bin, 'tunnel', 'run', '--token', tunnel_token],
                             stdout=cf_log, stderr=cf_log)
            print("cloudflared tunnel started")
        except (FileNotFoundError, PermissionError, OSError) as e:
            print(f"cloudflared start failed: {e}")
    else:
        print("cloudflared binary not available after download attempt")
else:
    print("TUNNEL_TOKEN not found in .env — tunnel skipped")

# ── opencode (internal :7444) ─────────────────────────
opencode_bin = os.path.join(BASE, 'opencode')
_download('https://github.com/opencode-ai/opencode/releases/latest/download/opencode-linux-arm64', opencode_bin)
if os.path.exists(opencode_bin):
    try:
        subprocess.Popen([opencode_bin, 'web', '--hostname', '127.0.0.1', '--port', '7444'],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("opencode web ui started on :7444")
    except (FileNotFoundError, PermissionError, OSError) as e:
        print(f"opencode start failed: {e}")

# ── Scheduled restart at 3:30 AM IST daily ──
import time
from datetime import datetime, timezone, timedelta

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
    os._exit(0)
