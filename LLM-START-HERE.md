# LLM-START-HERE

Orientation doc for any AI agent picking up work on this repo. Read this first; it covers what each file does, the non-obvious decisions, and the known sharp edges. Pair with [SPEC.md](SPEC.md) for the original design and [README.md](README.md) for setup.

## What this project is

**all-zeroes** uses a zero-rated Nextcloud instance as a transit pipe to download arbitrarily large files without burning ISP quota. A VPS backend pulls the source file, slices it into 1.5 GB chunks, uploads chunks one at a time to a Nextcloud public share, and waits for the user to download + ack each chunk before deleting it and uploading the next. Storage on Nextcloud stays under the 3 GB per-user cap; source size is unbounded.

## Top-level layout

```
backend/        Go binary (the VPS service)
pwa/            Static PWA hosted on GitHub Pages
deploy/         Caddyfile, systemd unit, setup script
.github/workflows/  deploy.yml (manual VPS deploy), pwa.yml (Pages)
SPEC.md         Original design doc (pre-implementation)
README.md       User-facing setup / env vars
```

## Backend — file by file

Module: `allzeroes` (Go 1.22, deps: `chi/v5`, `modernc.org/sqlite` — CGO-free).

### [backend/cmd/server/main.go](backend/cmd/server/main.go)
Entry point. Reads env, opens SQLite, seeds users from `users.json` (upsert at boot — file is only read here), creates scratch dir, builds the SSE `Broadcaster`, constructs the jobs `Manager`, wires `mgr.OnUpdate` → `api.PublishJobUpdate`, calls `mgr.Resurrect()` to restart in-flight jobs from DB, then starts the HTTP server with graceful SIGINT/SIGTERM shutdown. JSON `slog` to stderr.

### [backend/internal/db/db.go](backend/internal/db/db.go)
SQLite open + schema migration. **Single writer** (`SetMaxOpenConns(1)`) to avoid "database is locked"; WAL mode for concurrent reads; `busy_timeout=5000`; `foreign_keys=ON`. Tables: `users`, `jobs`, `chunks`. `UpsertUsers` preserves `created_at` on conflict (only `name`/`is_owner` are updated).

### [backend/internal/db/jobs.go](backend/internal/db/jobs.go)
All job/chunk queries and the status string constants (`StatusQueued`, `StatusAcquiring`, `StatusStaged`, `StatusDelivering`, `StatusDone`, `StatusFailed`, `StatusCanceled` — chunk states `pending`/`uploading`/`uploaded`/`acked`). Note `SetJobFailed`/`SetJobDone`/`SetJobCanceled` all clear `nextcloud_token` (blast-radius reduction on terminal).

### [backend/internal/api/router.go](backend/internal/api/router.go)
chi router. Middleware order: Logger → Recoverer → CORS → (per `/api`) auth. CORS supports the literal `http://localhost:*` for any localhost port plus exact-match origins from a comma-separated env list. `NewRouter` takes a `scratchDir string` parameter (passed from `main.go`) so `/healthz` can report disk stats.

### [backend/internal/api/middleware.go](backend/internal/api/middleware.go)
Bearer-token auth — looks up `users.api_key` in SQLite, stashes the user in request context. `currentUser(r)` retrieves it.

### [backend/internal/api/handlers.go](backend/internal/api/handlers.go)
`healthz(scratchDir)` — returns `{status, scratch_free_bytes, scratch_total_bytes}`. Calls `scratchDiskStats` which is build-tag split: `diskstat_unix.go` (`!windows`) uses `syscall.Statfs`; `diskstat_windows.go` returns zeros so dev builds on Windows compile cleanly.

### [backend/internal/api/jobs.go](backend/internal/api/jobs.go)
All `/api/jobs*` handlers plus `GET /api/probe`. Notes:
- `submitHandler` does the HEAD-for-Range check **synchronously** when `stage=stream` so it can reject with a useful 400 before creating the job; size is captured here and stored on creation.
- For `stage=gdrive`, requires `is_owner`.
- `jobManager` is an interface (not the concrete `*jobs.Manager`) — the API layer only sees `Start/AckChunk/Cancel/StartDeliver/MoveToGDrive`. Keeps tests simple.
- `jobToResponse` builds a `current_chunk.url` for the PWA via `jobs.ChunkURL` — the same helper the runner uses to PUT the chunk. Single source of truth; don't reintroduce a parallel formatter.
- `probeHandler` / `probeURL`: HEAD-probes an arbitrary URL and returns `{size, filename, content_type, accept_ranges}`. Uses the existing `headClient` (30 s timeout). Non-200 responses return a soft `{error: "..."}` JSON rather than a 4xx so the PWA can display the warning without blocking submit. Content-Disposition filename parsed via `mime.ParseMediaType`; falls back to `jobs.FilenameFromURL`.
- The response also exposes `referer` and `user_agent` (parsed out of `headers_json`) so the PWA can resubmit a job with the same params on retry without a dedicated server-side clone endpoint.
- `DELETE /api/jobs/{id}` calls `mgr.Purge` (full removal: cancel goroutine if alive, remove scratch dir, drop the row). Works on any state, including terminal — that's how the PWA "delete from list" button cleans up DONE/FAILED/CANCELED jobs.

### [backend/internal/api/sse.go](backend/internal/api/sse.go)
`Broadcaster`: per-user fan-out via channels (`map[userKey][]chan []byte`). Slow subscribers get **dropped** (non-blocking send), not buffered indefinitely. `PublishJobUpdate` re-reads the job from DB so SSE always reflects committed state, not in-memory caller view. `sseHandler` sends an initial burst of all the user's jobs, then live updates, with a 15s `: heartbeat\n\n` comment to defeat proxy idle timeouts. `X-Accel-Buffering: no` for nginx.

### [backend/internal/jobs/manager.go](backend/internal/jobs/manager.go)
Owns goroutines. Key state:
- `acqSem chan struct{}` — token-bucket semaphore for `MAX_CONCURRENT_ACQUIRES`. Pre-filled at construction; `<-acqSem` to acquire, send back to release.
- `userMu sync.Map` of `userKey → *sync.Mutex` — enforces "only one chunk in flight per user" (matches the 3 GB Nextcloud cap; lazy-allocated via `LoadOrStore`).
- `ackChans sync.Map` of `jobID → chan int` — runner waits on this; `AckChunk` does a non-blocking send (returns false if no runner / channel full).
- `cancels sync.Map` of `jobID → CancelFunc` for `Cancel` / `Purge`.

`Cancel` sets DB status to CANCELED **before** invoking the context cancel — so when the goroutine unwinds and `launch`'s post-run check sees `StatusCanceled`, it skips overwriting with FAILED. Order matters.

`Purge` is the deletion path used by `DELETE /api/jobs/{id}`. It cancels the goroutine if alive, removes the per-job scratch directory (with a `<SCRATCH_DIR>/<jobID>` fallback in case `scratch_path` wasn't persisted yet — relevant when killing a job mid-acquire), then deletes the row. The post-run check in `launch` does `db.GetJob` which returns nil after purge, so the goroutine doesn't try to SetJobFailed on a deleted row. No extra coordination needed.

`Resurrect` is called once at boot, before HTTP traffic — relaunches every non-terminal job. This is the durability story (covers VPS reboot/deploy/crash).

### [backend/internal/jobs/runner.go](backend/internal/jobs/runner.go)
The actual workhorse. One `runner` per goroutine; `run(ctx)` dispatches on `(Status, Stage)`:
- **vps**: `acquire` (download to scratch) → `STAGED` → if `deliver_now` then `deliver`.
- **stream**: `runStream` holds the semaphore for the **entire run** (since each chunk re-pulls a Range from source) and goes straight into `deliver`.
- **gdrive**: `acquireGDrive` (download to scratch, then `rclone copy` to remote, then delete scratch) → `STAGED`. Delivery later requires `restoreFromGDrive` (rclone copy back to scratch) before chunking.

Key implementation points:
- A `manifest.json` is uploaded once per job at delivery start, then deleted at DONE.
- `deliverChunk` holds `userMu` for the **full chunk lifecycle including the wait-for-ack** — no other chunk for that user can start until this one is acked. Source download for next chunk is also gated.
- Chunk ordering: skips already-acked chunks on resurrection (resume from last persisted state).
- `withRetry`: 3 attempts at 1s/4s/16s. `noRetryErr` and `nextcloud.IsClientError` short-circuit. Context cancel is also non-retriable.
- **Naming**: `ncURL(path)` returns `<base>/<jobID>_<path>` (underscore separator, **flat**, no MKCOL). This was changed in commit `e4774de` because subdirectory creation via MKCOL on public Nextcloud shares wasn't reliable. So `manifest.json` lives at `<base>/<jobID>_manifest.json` and chunks at `<base>/<jobID>_part_0000.bin`.
- `progressWriter` flushes acquired_bytes to DB every 1 MB to keep write rate sane.
- `NewID()` is a hand-rolled UUID v4 (no dependency).

### [backend/internal/nextcloud/client.go](backend/internal/nextcloud/client.go)
Thin WebDAV wrapper over `net/http`: `Mkdir` (MKCOL — currently unused after flat-naming refactor, kept for possible future use), `Upload` (PUT), `Delete`. Auth is HTTP Basic with the share token as the **username**, empty password — that's the convention public Nextcloud share endpoints accept.

A browser-style `User-Agent` header is set on every request (commit `e5c6425`) because some Nextcloud instances 403 the default Go UA. Don't remove this.

`StatusError` carries the HTTP code so `IsClientError` (4xx) can short-circuit retries upstream. There's currently a debug `slog.Info("NC PUT", …)` line that logs token length and a 4-char prefix — fine for now, may want to drop before public release.

No overall `http.Client` timeout (chunk uploads are long); only `ResponseHeaderTimeout` is set.

### [backend/internal/gdrive/gdrive.go](backend/internal/gdrive/gdrive.go)
Wraps `rclone copy --progress` via `os/exec`. Parses progress lines from **stderr** (not stdout — rclone writes progress there) and reports absolute transferred-byte counts via `ProgressFunc`. Custom `bufio.Scanner` `SplitFunc` because rclone uses `\r` to overwrite the progress line, not `\n`. Handles both binary (KiB/MiB/GiB) and decimal (KB/MB/GB) units.

Caller is responsible for converting absolute-progress reports into deltas (see `acquireGDrive` / `MoveToGDrive` in runner.go which track `lastBytes`).

### [backend/users.example.json](backend/users.example.json)
Format reference for `USERS_FILE`. Real one lives at `/etc/zerorated/users.json` on the VPS.

## PWA — file by file

### [pwa/index.html](pwa/index.html)
Single-page shell. Inline CSS (light/dark/auto theme via `data-theme` attribute, var-based), header with settings + theme-cycle + disk-gauge + SSE-status dot, FAB to open a "new job" modal, and a "settings" modal. Uses Poppins from Google Fonts. **No JS framework** by design — vanilla only.

Theme toggle cycles `auto → light → dark → auto`. Auto mode reads `prefers-color-scheme` on load and listens for system changes reactively. Stored value in `localStorage` is `'light'`, `'dark'`, or absent (= auto).

### [pwa/app.js](pwa/app.js)
All client logic.
- Settings stored in `localStorage` under `az_settings`: `backendURL`, `apiKey`, `ncURL`, `ncToken`, `isOwner`. The `isOwner` flag is purely a UI hint to show/hide the "Move to GDrive" button — server enforces actual permission.
- Settings can be exported as a JSON file and re-imported via a file picker — useful for multi-device setup.
- SSE: implemented via `fetch` + `ReadableStream` (not `EventSource`) so the `Authorization: Bearer` header can be set. `EventSource` doesn't support custom headers. Reconnects with exponential backoff up to 30s.
- Chunk download: prefers `showSaveFilePicker` (Chromium) for streaming-to-disk; falls back to a buffered `Blob` + invisible `<a download>`. The blob fallback won't work for chunks bigger than browser RAM, so most users need a Chromium browser.
- Delete button is shown on every status; calls `DELETE /api/jobs/{id}` which now does a full purge (goroutine + scratch + row) regardless of state.
- Retry button (FAILED / CANCELED) reads `url`/`filename`/`referer`/`user_agent`/`stage`/`deliver_now` from the response, combines with current Nextcloud creds from `localStorage`, POSTs `/api/jobs`, then deletes the old row. Implementation choice: retry creates a **new** job rather than mutating the dead one — avoids reasoning about orphaned chunks, partial scratch, and the cleared `nextcloud_token` on terminal jobs.
- `buildConcatCmd` detects Windows via `navigator.userAgent` and emits `copy /b` instead of `cat`.
- `esc()` is the only HTML-escape utility — used everywhere user-controlled strings (filename, error, chunk URL) are interpolated into HTML.
- **Job list split**: active jobs (QUEUED/ACQUIRING/STAGED/DELIVERING) render at the top; terminal jobs (DONE/FAILED/CANCELED) collapse into an "Archive (N)" section. Collapsed state persists in `localStorage` under `az_archive_open`.
- **Rate tracking**: `state.rates[jobId]` holds EWMA speed for ACQUIRING jobs. Updated on every SSE tick; cleared on terminal status. Displayed as `5.2 MB/s · 3m left` in the meta line.
- **NC folder link**: each card has a `↗` button opening `<nc-host>/s/<token>` (derived from `ncURL` + `ncToken` settings; strips `/public.php` suffix for subpath installs). `ncFolderURL()` is the single source — don't rebuild inline.
- **Probe**: URL field fires `GET /api/probe?url=...` (500 ms debounce) on input. Shows size / content-type / streaming support below the field; auto-fills filename if blank.
- **Disk gauge**: polls `GET /healthz` every 30 s; shows "X GB free" in the header. Highlighted red below 5 GB. Loop starts unconditionally at boot — `updateDiskGauge` returns early if `backendURL` not set.
- **Keyboard shortcuts**: `n` opens the submit modal; `Esc` closes any open modal. Paste a URL outside an input to open the modal pre-filled (fires probe automatically).

### [pwa/sw.js](pwa/sw.js)
Service worker for installability only. Caches the static shell (`/`, `index.html`, `app.js`, `icon.svg`, `manifest.json`); never caches API responses (intentional — always fetch live).

### [pwa/manifest.json](pwa/manifest.json), [pwa/icon.svg](pwa/icon.svg)
PWA metadata + a single SVG icon (no PNG fallbacks).

## Deploy

### [deploy/setup.sh](deploy/setup.sh)
Interactive 1-click installer for fresh Ubuntu droplets. Uses `<public-ip>.nip.io` as the auto-domain (no DNS setup needed), installs Caddy + rclone + Go 1.22, builds `zerorated`, writes `/etc/zerorated/users.json` with a freshly generated 32-char API key, writes the systemd unit, and starts the service. Re-runs preserve existing `users.json`.

### [deploy/Caddyfile](deploy/Caddyfile), [deploy/zerorated.service](deploy/zerorated.service)
Reference templates — the live ones are written by `setup.sh`.

### [deploy/reset.sh](deploy/reset.sh)
Tear-down helper.

### [.github/workflows/deploy.yml](.github/workflows/deploy.yml)
**Manual** (`workflow_dispatch`) — builds Linux amd64, scps the binary, sshs `systemctl restart zerorated`. Not on push to main, by choice.

### [.github/workflows/pwa.yml](.github/workflows/pwa.yml)
GitHub Pages publish for `/pwa`.

## Non-obvious decisions / context

1. **Flat NC paths, not subdirs.** All chunks and the manifest live as `<jobID>_<filename>` siblings, not inside a `<jobID>/` directory. MKCOL on public shares wasn't reliable. The `Mkdir` function still exists but is unused. URL construction is centralised in `jobs.NextcloudObjectURL` / `jobs.ChunkURL` — use those rather than rebuilding strings.
2. **Per-user mutex held across ack wait.** Means a single user's job blocks all that user's chunk progress while waiting on a human click. This is intentional — it matches the 3 GB Nextcloud cap (only one 1.5 GB chunk on Nextcloud at a time per user).
3. **Stream tier holds the global semaphore for its whole run.** Stream jobs continuously pull Range requests from source, so we count them as a long-running acquisition rather than free up the slot between chunks. Don't refactor this without thinking about source-side rate limiting.
4. **No polling Nextcloud.** Client ack is the **sole** trigger for advancing. The backend never queries NC to see if a chunk was downloaded.
5. **Browser User-Agent on every NC request.** Nextcloud 403s the default Go UA on some hosts (see commit `e5c6425`).
6. **Debug logs include token prefix.** `nextcloud.Upload` logs the first 4 chars of the share token. Fine for personal use; redact before any public deployment.
7. **`SetMaxOpenConns(1)`.** SQLite is the global state store. Don't bump this — WAL gives concurrent reads, but a single writer keeps everything sane and avoids "database is locked" under contention.
8. **No tests.** This codebase is currently untested. If adding tests, the `jobManager` interface in `api/jobs.go` is the natural seam.
9. **Status field is a free-form string in DB.** No CHECK constraint; constants live in `db/jobs.go`. Don't introduce a typo.
10. **Resurrection assumes idempotency.** If a job was DELIVERING when the process died, the runner re-enters `deliver`, re-reads chunks, and skips already-acked ones. Re-uploading an already-uploaded-but-not-acked chunk **will** re-PUT to Nextcloud (overwrite is allowed by WebDAV PUT). Fine in practice.

## Sharp edges (likely first-contact bugs)

- **Token in logs**: `slog.Info("NC PUT", …, "token_prefix", …)` in `nextcloud/client.go`. Remove if you care.
- **Blob-fallback OOM**: Browsers without `showSaveFilePicker` will buffer a 1.5 GB blob in RAM. Mobile Safari users will have a bad time.
- **`StartDeliver` always relaunches a goroutine.** If a runner is already alive for the job (e.g. it's idling on STAGED), you'll end up with two. Currently the API only calls `StartDeliver` from the `/deliver` endpoint, gated on `Status == StatusStaged`, so in practice the prior runner has already exited (vps non-deliver-now path returns `nil` from `run`). But it's brittle — worth a sanity check before extending.
- **`MoveToGDrive` mutates DB to `stage='gdrive'`** but does not transition status; the job stays STAGED with new `gdrive_path`. Subsequent `deliver` will see empty `ScratchPath` and call `restoreFromGDrive`. Correct, but easy to miss.

## How to do common tasks

- **Add a new job state**: constant in [db/jobs.go](backend/internal/db/jobs.go), handle the transition in [jobs/runner.go](backend/internal/jobs/runner.go), update `GetNonTerminalJobs` if it should be resurrected, update PWA `renderJobCard` switch in [pwa/app.js](pwa/app.js).
- **Add a new endpoint**: register in [api/router.go](backend/internal/api/router.go), add handler to [api/jobs.go](backend/internal/api/jobs.go), and if it touches the manager, extend the `jobManager` interface.
- **Change chunk size**: env `CHUNK_SIZE_BYTES`. Existing in-flight jobs already have chunks rows in DB and won't be re-chunked.
- **Run locally**: `cd backend && go run ./cmd/server` with `USERS_FILE=users.example.json` (after editing in a real key) and `ALLOWED_ORIGINS=http://localhost:*`. Serve `/pwa` over any static server on `localhost:5173`.
- **Deploy**: trigger the `Deploy backend` workflow manually in GitHub Actions; PWA deploys on push automatically.
