from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

PRIVACY = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Privacy Policy — Aruvi</title>
<style>body{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;padding:0 20px;line-height:1.7;color:#222}a{color:#1a73e8}h1{color:#111}h2{color:#333;margin-top:2em}</style>
</head>
<body>
<h1>Privacy Policy</h1>
<p><em>Last updated: June 2026</em></p>

<h2>1. What We Collect</h2>
<ul>
<li><strong>Telegram ID</strong> — to associate your files and preferences with your account.</li>
<li><strong>File metadata</strong> — file names, sizes, types of files you browse or download.</li>
<li><strong>Google Drive tokens</strong> — only if you connect your Google Drive; used solely to upload files you request.</li>
</ul>

<h2>2. How We Use Data</h2>
<ul>
<li>Provide the streaming and download service.</li>
<li>Upload files to your Google Drive on your explicit request.</li>
<li>Never sell, share, or use your data for any other purpose.</li>
</ul>

<h2>3. Data Storage</h2>
<ul>
<li>Your Telegram ID and file metadata are stored in our database.</li>
<li>Files themselves are stored in a private Telegram channel and streamed on demand — we do not host them on our servers.</li>
<li>Google Drive tokens are stored encrypted and only used to perform uploads you initiate.</li>
</ul>

<h2>4. Third-Party Services</h2>
<ul>
<li><strong>Telegram</strong> — files are stored in Telegram's infrastructure.</li>
<li><strong>Google Drive</strong> — if you opt in, files are uploaded to your personal Google Drive.</li>
</ul>

<h2>5. Data Deletion</h2>
<p>Contact us to delete your account and associated data. Google Drive tokens can be revoked by you at any time via your Google Account settings.</p>

<h2>6. Contact</h2>
<p>Email: <a href="mailto:priyamolmpraveen2@gmail.com">priyamolmpraveen2@gmail.com</a></p>
</body>
</html>"""

TERMS = """\
<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Terms of Service — Aruvi</title>
<style>body{font-family:system-ui,sans-serif;max-width:720px;margin:40px auto;padding:0 20px;line-height:1.7;color:#222}a{color:#1a73e8}h1{color:#111}h2{color:#333;margin-top:2em}</style>
</head>
<body>
<h1>Terms of Service</h1>
<p><em>Last updated: June 2026</em></p>

<h2>1. Acceptance</h2>
<p>By using Aruvi ("the Service"), you agree to these Terms. If you do not agree, do not use the Service.</p>

<h2>2. Description</h2>
<p>Aruvi is a tool that lets you browse, stream, and download files from Telegram, and optionally upload them to your personal Google Drive.</p>

<h2>3. User Responsibilities</h2>
<ul>
<li>You must comply with Telegram's Terms of Service.</li>
<li>You must not use the Service to distribute illegal or copyrighted material without authorization.</li>
<li>You are responsible for the content you access and share.</li>
</ul>

<h2>4. Limited Liability</h2>
<p>The Service is provided "as is" without warranties. We are not liable for any damages arising from your use of the Service, including data loss or service interruptions.</p>

<h2>5. Third-Party Services</h2>
<p>The Service relies on Telegram and Google Drive. Their respective terms and policies apply to your use of those platforms.</p>

<h2>6. Changes</h2>
<p>We may update these Terms at any time. Continued use after changes constitutes acceptance.</p>

<h2>7. Contact</h2>
<p>Email: <a href="mailto:priyamolmpraveen2@gmail.com">priyamolmpraveen2@gmail.com</a></p>
</body>
</html>"""


@router.get("/privacy", response_class=HTMLResponse)
async def privacy():
    return PRIVACY


@router.get("/terms", response_class=HTMLResponse)
async def terms():
    return TERMS
