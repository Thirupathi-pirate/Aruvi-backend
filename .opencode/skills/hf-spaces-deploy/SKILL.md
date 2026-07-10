# HF Spaces Deploy Skill

## Purpose
Deploy code and manage secrets for Hugging Face Spaces projects. Use **`huggingface_hub` Python API** — the `hf` CLI is unreliable for scripting.

## Prerequisites
- `HF_TOKEN` in `~/.bashrc` (or passed directly to Python)
- Python package `huggingface_hub` installed

## Git Push to HF Spaces
```bash
git push hf-spaces main
```
HF Spaces auto-builds on push. Verify the remote:
```bash
git remote -v
# hf-spaces → https://huggingface.co/spaces/{owner}/{space-name}.git
```

## Manage Secrets (via Python — always)

### Set/Update a Secret
```python
from huggingface_hub import HfApi
api = HfApi(token="hf_...")  # or omit to use HF_TOKEN env var

api.add_space_secret(
    repo_id="owner/space-name",       # e.g. "wpbtvr/teleplay-backend"
    key="SECRET_NAME",                # e.g. "TELEGRAM_HELPER_BOT_TOKENS"
    value="the-secret-value"
)
```

### Restart the Space (to pick up new secrets/code)
```python
api.restart_space("owner/space-name")
```

### Check Space Status
```python
space = api.get_space_runtime("owner/space-name")
print(space.stage)  # "RUNNING", "BUILDING", "STARTING", etc.
```

## Important Notes
- **Do NOT use `hf` CLI** for tokens/secrets — its `auth login` is broken with existing `HF_TOKEN` env var.
- **Do NOT source `~/.bashrc` in Python scripts** — it sets `HF_TOKEN` which can conflict. Pass token directly to `HfApi(token=...)`.
- `.env` is gitignored — secrets stay local. Use `api.add_space_secret()` for HF Spaces.
- HF Spaces auto-restarts after a git push, but secrets changes need `api.restart_space()`.
- Tokens expire — generate new ones at https://huggingface.co/settings/tokens with **Write** scope.

## Quick Reference

| Action | Command |
|--------|---------|
| Push code | `git push hf-spaces main` |
| Set secret | `api.add_space_secret("owner/space", "KEY", "val")` |
| Restart | `api.restart_space("owner/space")` |
| Check status | `api.get_space_runtime("owner/space")` |
| List repos | `hf repos ls` |
| Login (CLI) | `unset HF_TOKEN && hf auth login` |
