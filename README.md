# all-zeroes

Download arbitrarily large files from the internet through a zero-rated Nextcloud instance. A VPS backend chunks files into pieces and uploads them one at a time to a Nextcloud share; the client downloads each piece (free), acks it, and the backend advances to the next chunk.

## Prerequisites

- Go 1.22+
- SQLite (provided via CGO-free `modernc.org/sqlite` — no system library needed)
- `rclone` configured with a `gdrive` remote (GDrive tier only)
- A Nextcloud instance with a public share (WebDAV URL + token)

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `state.db` | Path to SQLite database file |
| `SCRATCH_DIR` | `./scratch` | Directory for VPS-staged files |
| `LISTEN_ADDR` | `127.0.0.1:8080` | Address and port to bind |
| `ALLOWED_ORIGINS` | `http://localhost:5173` | Comma-separated CORS origins. Use `http://localhost:*` to allow any localhost port |
| `USERS_FILE` | `users.json` | Path to user seed file |
| `RCLONE_REMOTE` | `gdrive:zerorated` | rclone destination for GDrive tier |
| `MAX_CONCURRENT_ACQUIRES` | `4` | Global cap on simultaneous downloads |
| `CHUNK_SIZE_BYTES` | `1610612736` | 1.5 GB |

## users.json format

```json
[
  {"api_key": "change-me-32-chars-secret", "name": "me", "is_owner": true},
  {"api_key": "friend-key-also-secret-here", "name": "friend1", "is_owner": false}
]
```

Users are upserted at startup — edit the file and restart to add/remove users. The file is only read at boot.

## Deployment (DigitalOcean + Caddy + systemd)

See [`deploy/`](deploy/) for the Caddyfile, systemd unit, and GitHub Actions workflow.

### 1-Click Terminal Setup Wizard

If you are deploying to a fresh Ubuntu Droplet (or any systemd-based Linux), you can use the interactive setup wizard. 

[![Deploy to DO](https://img.shields.io/badge/Deploy_to_DO-terminal-blue?style=for-the-badge&logo=digitalocean)](https://raw.githubusercontent.com/lazyseahorse/all-zeroes/main/deploy/setup.sh)

Right-click the button above to copy the script link, or simply run the following command in your Droplet console:

```bash
bash <(curl -sL https://raw.githubusercontent.com/lazyseahorse/all-zeroes/main/deploy/setup.sh)
```

The wizard will install dependencies, compile the application, generate your API key, and configure Caddy and systemd automatically.

### Manual Setup

```bash
# On the VPS (one-time setup)
sudo useradd -r -s /sbin/nologin zerorated
sudo mkdir -p /var/lib/zerorated/{scratch} /opt/zerorated /etc/zerorated
sudo cp users.json /etc/zerorated/users.json

# Build and copy binary (CI does this automatically after setup)
GOOS=linux GOARCH=amd64 go build -o zerorated ./backend/cmd/server
scp zerorated user@vps:/opt/zerorated/server

# Enable service
sudo cp deploy/zerorated.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now zerorated
```

## Telegram bot

An optional Telegram bot that mirrors the PWA workflow: send a URL or file, pick options (stage, no-chunk, deliver-now), and the bot messages you each chunk link with a Done/Cancel button. Polls the backend every 4 seconds.

### Setup

After the main backend is running, SSH into the VPS and run:

```bash
bash <(curl -sL https://raw.githubusercontent.com/lazyseahorse/all-zeroes/main/deploy/tg-bot.sh)
```

The wizard prompts for your bot token (from [@BotFather](https://t.me/BotFather)), your chat ID (from [@userinfobot](https://t.me/userinfobot)), and your Nextcloud credentials, then builds and installs the bot as a systemd service.

### Bot env vars

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Token from @BotFather |
| `TELEGRAM_ALLOWED_CHAT_IDS` | Comma-separated chat IDs that may use the bot |
| `BACKEND_URL` | Backend base URL (default `http://localhost:8080`) |
| `BACKEND_API_KEY` | API key from `users.json` |
| `NEXTCLOUD_URL` | WebDAV share URL |
| `NEXTCLOUD_TOKEN` | Nextcloud share token |

### Bot commands

| Command | Description |
|---|---|
| `/list` | Show all jobs with status |
| `/cancel <id-prefix>` | Cancel and delete a job |
| `/deliver <id-prefix>` | Start delivery for a staged job (when deliver-now is off) |

## Project structure

```
backend/
  cmd/server/        main backend entry point
  cmd/tgbot/         Telegram bot (separate Go module)
  internal/
    api/             HTTP handlers, router, middleware
    db/              SQLite schema + queries
    jobs/            job runner goroutines
    nextcloud/       thin WebDAV client
    gdrive/          rclone wrapper
pwa/                 vanilla JS PWA
deploy/              Caddyfile, systemd units, setup scripts
```

## Build phases

| Phase | What ships |
|---|---|
| 1 | Skeleton: server boots, auth works, `/healthz` responds |
| 2 | VPS-staged end-to-end: submit → download → ack → done |
| 3 | SSE + PWA on GitHub Pages |
| 4 | Stream tier (Range-based, no scratch disk) |
| 5 | GDrive tier (rclone) |
| 6 | Retries, sha256, concat-command UI, SSE reconnect |
