from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

STYLE = """\
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
background:#0f0f0f;color:#e0e0e0;line-height:1.8}
.wrap{max-width:720px;margin:0 auto;padding:40px 24px}
h1{font-size:2rem;font-weight:700;color:#fff;margin-bottom:4px;
letter-spacing:-0.5px}
.sub{color:#888;font-size:0.9rem;margin-bottom:40px}
h2{font-size:1.15rem;font-weight:600;color:#fff;margin:32px 0 12px}
p,li{color:#bbb;margin-bottom:8px}
ul{padding-left:20px}
li{margin-bottom:4px}
a{color:#6ab4f8;text-decoration:none}
a:hover{text-decoration:underline}
.nav{margin-bottom:40px;display:flex;gap:16px}
.nav a{color:#888;font-size:0.9rem}
.nav a.active{color:#6ab4f8}
hr{border:none;border-top:1px solid #222;margin:40px 0}
.foot{color:#555;font-size:0.8rem}
</style>"""

NAV_BAR = """\
<div class="nav">
<a href="/privacy" class="active">Privacy Policy</a>
<a href="/terms">Terms of Service</a>
<a href="https://REDACTED_DOMAIN">Aruvi</a>
</div>"""

PAGE_TPL = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title} — Aruvi</title>{style}</head>
<body>
<div class="wrap">
{nav}
{content}
<hr>
<p class="foot">Hosted on <a href="https://www.hidencloud.com/">HidenCloud</a> · No data collected · {year}</p>
</div>
</body>
</html>"""


def page(title, content):
    return PAGE_TPL.format(
        title=title, style=STYLE, nav=NAV_BAR, content=content, year="2026"
    )


PRIVACY_CONTENT = """\
<h1>Privacy Policy</h1>
<p class="sub">We do not collect any personal data.</p>

<h2>What This Means</h2>
<ul>
<li>We do <strong>not</strong> collect, store, or share your name, email, IP address, location, device info, or browsing history.</li>
<li>Your Telegram ID is used only to identify your session and is never stored permanently.</li>
<li>If you connect Google Drive, tokens are stored only to upload files you request and can be revoked anytime.</li>
<li>We do <strong>not</strong> use cookies, trackers, or analytics of any kind.</li>
</ul>

<h2>Where Data Lives</h2>
<ul>
<li>The service runs on <a href="https://www.hidencloud.com/">HidenCloud</a> infrastructure.</li>
<li>Files are streamed directly from Telegram — we do not host or store them.</li>
<li>Google Drive uploads go directly from Telegram to your personal Drive — we never see your files.</li>
</ul>

<h2>Third Parties</h2>
<ul>
<li><strong>Telegram</strong> — file storage and delivery.</li>
<li><strong>Google Drive</strong> — only if you opt in to upload.</li>
<li><strong>HidenCloud</strong> — server hosting provider.</li>
</ul>

<h2>Contact</h2>
<p><a href="mailto:priyamolmpraveen2@gmail.com">priyamolmpraveen2@gmail.com</a></p>
"""


TERMS_CONTENT = """\
<h1>Terms of Service</h1>
<p class="sub">By using Aruvi you agree to these terms.</p>

<h2>Use of Service</h2>
<p>Aruvi lets you browse, stream, and download files from Telegram, and optionally upload them to your personal Google Drive.</p>

<h2>Your Responsibilities</h2>
<ul>
<li>Comply with Telegram's Terms of Service.</li>
<li>Do not distribute illegal or copyrighted material without authorization.</li>
<li>You are responsible for the content you access and share.</li>
</ul>

<h2>No Warranties</h2>
<p>The service is provided "as is" without any warranty. We are not liable for any damages arising from its use.</p>

<h2>Third-Party Services</h2>
<p>Telegram, Google Drive, and HidenCloud have their own terms that apply to your use of their platforms.</p>

<h2>Changes</h2>
<p>We may update these terms at any time. Continued use after changes constitutes acceptance.</p>

<h2>Contact</h2>
<p><a href="mailto:priyamolmpraveen2@gmail.com">priyamolmpraveen2@gmail.com</a></p>
"""


@router.get("/privacy", response_class=HTMLResponse)
async def privacy():
    return page("Privacy Policy", PRIVACY_CONTENT)


@router.get("/terms", response_class=HTMLResponse)
async def terms():
    return page("Terms of Service", TERMS_CONTENT)


@router.get("/privacy", response_class=HTMLResponse)
async def privacy():
    return PRIVACY


@router.get("/terms", response_class=HTMLResponse)
async def terms():
    return TERMS
