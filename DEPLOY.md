# OptionFlow v4 — Deployment Guide

## Files needed
```
engine.py
requirements.txt
Procfile
runtime.txt
```

---

## Option 1 — Railway (Recommended, free)

**Steps:**
1. Create a free account at https://railway.app
2. Install Railway CLI: `npm install -g @railway/cli`  OR use the web UI
3. Create a new GitHub repo, push all 4 files
4. In Railway → New Project → Deploy from GitHub repo
5. Add environment variables in Railway dashboard:
   ```
   ANTHROPIC_API_KEY    = sk-ant-xxxxx
   TELEGRAM_BOT_TOKEN  = 123456:ABCxxxxx
   TELEGRAM_CHAT_ID    = 987654321
   ```
6. Railway auto-detects the Procfile and deploys
7. Your app is live at: `https://your-project.railway.app`

**Free tier:** 500 hours/month (~20 hours/day). Enough for market hours.

---

## Option 2 — Render (free, always-on)

1. Create account at https://render.com
2. New → Web Service → Connect GitHub repo
3. Build command: `pip install -r requirements.txt`
4. Start command: `gunicorn engine:app --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:$PORT`
5. Add env vars in Render dashboard (same as above)
6. Free tier spins down after 15min inactivity (first load takes ~30s to wake up)

---

## Option 3 — VPS (₹200–500/month, most reliable)

**Providers:** DigitalOcean, Hetzner, AWS Lightsail, Vultr

**Setup on Ubuntu:**
```bash
# 1. Connect to your VPS
ssh root@your-server-ip

# 2. Install Python
apt update && apt install python3-pip python3-venv -y

# 3. Upload your files (from your Mac)
scp engine.py requirements.txt root@your-server-ip:/opt/optionflow/

# 4. On the server
cd /opt/optionflow
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 5. Set env vars
export ANTHROPIC_API_KEY=sk-ant-xxxxx
export TELEGRAM_BOT_TOKEN=xxxxx
export TELEGRAM_CHAT_ID=xxxxx

# 6. Run with screen (keeps running after you disconnect)
screen -S optionflow
gunicorn engine:app --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:8000

# Detach from screen: Ctrl+A then D
# Reattach later: screen -r optionflow

# 7. Access at http://your-server-ip:8000
```

**To make it a proper service (auto-restart on reboot):**
```bash
# Create systemd service
cat > /etc/systemd/system/optionflow.service << EOF
[Unit]
Description=OptionFlow Trading Dashboard
After=network.target

[Service]
User=root
WorkingDirectory=/opt/optionflow
Environment="ANTHROPIC_API_KEY=sk-ant-xxxxx"
Environment="TELEGRAM_BOT_TOKEN=xxxxx"
Environment="TELEGRAM_CHAT_ID=xxxxx"
ExecStart=/opt/optionflow/venv/bin/gunicorn engine:app --workers 1 --threads 4 --timeout 120 --bind 0.0.0.0:8000
Restart=always

[Install]
WantedBy=multi-user.target
EOF

systemctl enable optionflow
systemctl start optionflow
systemctl status optionflow
```

---

## Security — if making it public

If your URL is publicly accessible, add basic password protection.
Add this to engine.py after the imports:

```python
from functools import wraps
from flask import request, Response

DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

def check_auth(password):
    return password == DASHBOARD_PASSWORD

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.password):
            return Response('Login required', 401,
                {'WWW-Authenticate': 'Basic realm="OptionFlow"'})
        return f(*args, **kwargs)
    return decorated
```

Then add `@requires_auth` above each `@app.route`.

Set `DASHBOARD_PASSWORD=yourpassword` in your env vars.

---

## Recommended: Railway for simplicity, VPS for reliability

- Railway/Render: zero server management, deploy in 5 min
- VPS: full control, always-on, cheapest at scale (~₹300/month Hetzner)
