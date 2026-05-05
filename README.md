# all-zeroes

Download arbitrarily large files from the internet through a zero-rated Nextcloud instance. A VPS backend chunks files into pieces and uploads them one at a time to a Nextcloud share; the client downloads each piece (free), acks it, and the backend advances to the next chunk.

## Prerequisites

- Go 1.22+
- SQLite (provided via CGO-free `modernc.org/sqlite` — no system library needed)
- `rclone` configured with a `gdrive` remote (GDrive tier only)
- A Nextcloud instance with a public share (WebDAV URL + token)

## Running locally

```bash
cd backend

# First time only: fetch dependencies
go mod tidy

# Create a users file (copy and edit the example)
cp users.example.json users.json
# edit users.json with your API key(s)

# Run the server
DB_PATH=./state.db \
SCRATCH_DIR=./scratch \
LISTEN_ADDR=127.0.0.1:8080 \
ALLOWED_ORIGINS="http://localhost:5173,http://localhost:*" \
USERS_FILE=./users.json \
go run ./cmd/server

# Smoke test
curl http://localhost:8080/healthz
# {"status":"ok"}

# Auth test
curl -H "Authorization: Bearer your-api-key-here" http://localhost:8080/api/jobs
# 501 not implemented  (correct — Phase 2 fills this in)

curl http://localhost:8080/api/jobs
# 401 unauthorized
```

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

## Project structure

```
backend/
  cmd/server/        entry point
  internal/
    api/             HTTP handlers, router, middleware
    db/              SQLite schema + queries
    jobs/            job runner goroutines  (Phase 2+)
    nextcloud/       thin WebDAV client     (Phase 2+)
    gdrive/          rclone wrapper         (Phase 5+)
pwa/                 vanilla JS PWA         (Phase 3+)
deploy/              Caddyfile, systemd, CI
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
