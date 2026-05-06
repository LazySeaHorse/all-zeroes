package jobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"allzeroes/internal/db"
	"allzeroes/internal/gdrive"
	"allzeroes/internal/nextcloud"
)

var downloadClient = &http.Client{
	Transport: &http.Transport{
		ResponseHeaderTimeout: 60 * time.Second,
	},
}

type runner struct {
	db     *sql.DB
	cfg    Config
	job    *db.Job
	ackCh  chan int
	acqSem chan struct{}
	userMu *sync.Mutex
	notify func(userKey, jobID string)
}

func (r *runner) run(ctx context.Context) error {
	switch r.job.Status {
	case db.StatusQueued, db.StatusAcquiring:
		switch r.job.Stage {
		case "stream":
			return r.runStream(ctx)
		case "gdrive":
			if err := r.acquireGDrive(ctx); err != nil {
				return err
			}
			if !r.job.DeliverNow {
				return nil // sits in GDrive; POST /deliver later
			}
			return r.deliver(ctx)
		default: // vps
			if err := r.acquire(ctx); err != nil {
				return err
			}
			if !r.job.DeliverNow {
				return nil
			}
			return r.deliver(ctx)
		}

	case db.StatusStaged:
		if !r.job.DeliverNow {
			return nil
		}
		return r.deliver(ctx)

	case db.StatusDelivering:
		return r.deliver(ctx)
	}
	return nil
}

// runStream handles stream-tier jobs: streams each chunk directly source → Nextcloud.
// The global semaphore is held for the full duration since we continuously pull from source.
func (r *runner) runStream(ctx context.Context) error {
	select {
	case <-r.acqSem:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { r.acqSem <- struct{}{} }()

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusAcquiring); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	return r.deliver(ctx) // deliver() handles setup + chunk loop
}

// acquire downloads the source URL to scratch disk.
func (r *runner) acquire(ctx context.Context) error {
	// Block until a semaphore slot is free.
	select {
	case <-r.acqSem:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { r.acqSem <- struct{}{} }()

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusAcquiring); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	if err := r.downloadToScratch(ctx); err != nil {
		return err
	}
	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusStaged); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	slog.Info("job staged", "id", r.job.ID)
	return nil
}

// downloadToScratch performs the actual HTTP download. Must be called with the
// semaphore already held (either by acquire or by acquireGDrive).
// It does NOT transition the job status — callers do that.
func (r *runner) downloadToScratch(ctx context.Context) error {
	jobDir := filepath.Join(r.cfg.ScratchDir, r.job.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("mkdir scratch: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.job.URL, nil)
	if err != nil {
		return err
	}
	applyHeaders(req, r.job.HeadersJSON)

	resp, err := downloadClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("source returned %d", resp.StatusCode)
	}

	// Determine filename: Content-Disposition > URL path > job record.
	filename := r.job.Filename
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if name := params["filename"]; name != "" {
				filename = name
			}
		}
	}
	scratchPath := filepath.Join(jobDir, sanitizeFilename(filename))

	f, err := os.Create(scratchPath)
	if err != nil {
		return fmt.Errorf("create scratch file: %w", err)
	}
	defer f.Close()

	if err := db.SetJobScratchPath(r.db, r.job.ID, scratchPath); err != nil {
		return err
	}
	r.job.ScratchPath = scratchPath

	pw := &progressWriter{db: r.db, jobID: r.job.ID}
	if _, err := io.Copy(f, io.TeeReader(resp.Body, pw)); err != nil {
		return fmt.Errorf("download: %w", err)
	}
	if err := pw.flush(); err != nil {
		return err
	}

	// Update size now that we know it exactly.
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if err := db.SetJobSize(r.db, r.job.ID, info.Size()); err != nil {
		return err
	}
	size := info.Size()
	r.job.Size = &size
	return nil

}

// deliver chunks and uploads the staged file to Nextcloud.
func (r *runner) deliver(ctx context.Context) error {
	// For gdrive-staged jobs that have no scratch file yet, restore from GDrive first.
	if r.job.Stage == "gdrive" && r.job.ScratchPath == "" {
		if err := r.restoreFromGDrive(ctx); err != nil {
			return fmt.Errorf("restore from GDrive: %w", err)
		}
	}

	// Check if chunks are already set up (resurrection of DELIVERING job).
	existing, err := db.GetChunks(r.db, r.job.ID)
	if err != nil {
		return err
	}

	var chunks []db.Chunk
	if len(existing) == 0 {
		// Fresh delivery: compute chunks and upload manifest.
		chunks, err = r.setupDelivery(ctx)
		if err != nil {
			return err
		}
	} else {
		chunks = existing
	}

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusDelivering); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)

	// preUpload tracks an in-flight background upload of the next chunk so it
	// can be running while the user downloads and acks the current chunk.
	type preUpload struct {
		chunk *db.Chunk
		errCh chan error // buffered(1); goroutine sends result here
	}
	var pending *preUpload

	for i := range chunks {
		if chunks[i].Status == db.ChunkAcked {
			continue // already done; skip on resurrection
		}

		// ── Phase 1: ensure this chunk is uploaded ────────────────────────────

		if pending != nil && pending.chunk.Idx == chunks[i].Idx {
			// Upload was pre-started in the previous iteration; wait for it.
			select {
			case err := <-pending.errCh:
				if err != nil {
					return fmt.Errorf("upload chunk %d: %w", chunks[i].Idx, err)
				}
			case <-ctx.Done():
				<-pending.errCh // drain so the goroutine can exit
				return ctx.Err()
			}
			pending = nil
		} else {
			// Upload synchronously (first chunk, or no pre-start available).
			db.SetChunkStatus(r.db, r.job.ID, chunks[i].Idx, db.ChunkUploading) //nolint:errcheck
			r.notify(r.job.UserKey, r.job.ID)
			if err := r.doUploadChunk(ctx, &chunks[i]); err != nil {
				return fmt.Errorf("upload chunk %d: %w", chunks[i].Idx, err)
			}
		}

		if err := db.SetChunkStatus(r.db, r.job.ID, chunks[i].Idx, db.ChunkUploaded); err != nil {
			return err
		}
		slog.Info("chunk uploaded", "job", r.job.ID, "chunk", chunks[i].Idx, "size", chunks[i].Size)
		r.notify(r.job.UserKey, r.job.ID)

		// ── Phase 2: pre-start uploading the next chunk ───────────────────────
		// This runs concurrently while the user downloads and acks the current chunk.
		// At most 2 chunks are on Nextcloud at once: the one being downloaded + this one.

		nextIdx := i + 1
		for nextIdx < len(chunks) && chunks[nextIdx].Status == db.ChunkAcked {
			nextIdx++ // skip already-acked chunks (resurrection edge case)
		}
		if nextIdx < len(chunks) {
			errCh := make(chan error, 1)
			db.SetChunkStatus(r.db, r.job.ID, chunks[nextIdx].Idx, db.ChunkUploading) //nolint:errcheck
			r.notify(r.job.UserKey, r.job.ID)
			nextChunk := &chunks[nextIdx]
			go func() {
				errCh <- r.doUploadChunk(ctx, nextChunk)
			}()
			pending = &preUpload{chunk: nextChunk, errCh: errCh}
		}

		// ── Phase 3: wait for user ack, then delete ───────────────────────────

		chunkURL := r.ncChunkURL(chunks[i].Idx)
		select {
		case <-r.ackCh:
		case <-ctx.Done():
			if pending != nil {
				<-pending.errCh // drain so the goroutine can exit
			}
			return ctx.Err()
		}

		nextcloud.Delete(ctx, chunkURL, r.job.NextcloudToken) //nolint:errcheck
		if err := db.SetChunkStatus(r.db, r.job.ID, chunks[i].Idx, db.ChunkAcked); err != nil {
			return err
		}
		slog.Info("chunk acked", "job", r.job.ID, "chunk", chunks[i].Idx)
		r.notify(r.job.UserKey, r.job.ID)
	}

	// All chunks acked. Clean up.
	ncManifestURL := r.ncURL("manifest.json")
	nextcloud.Delete(ctx, ncManifestURL, r.job.NextcloudToken) //nolint:errcheck

	if r.job.ScratchPath != "" {
		os.RemoveAll(filepath.Dir(r.job.ScratchPath))
	}
	if err := db.SetJobDone(r.db, r.job.ID); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	slog.Info("job done", "id", r.job.ID)
	return nil
}

// acquireGDrive downloads the source file to scratch, then rclone-copies it to GDrive.
// After the rclone copy succeeds, scratch is removed to free VPS disk space.
func (r *runner) acquireGDrive(ctx context.Context) error {
	select {
	case <-r.acqSem:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { r.acqSem <- struct{}{} }()

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusAcquiring); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)

	// Step 1: download to VPS scratch (semaphore already held).
	if err := r.downloadToScratch(ctx); err != nil {
		return err
	}

	// Step 2: rclone copy scratch → GDrive.
	// dst is the remote directory (rclone puts the file inside it preserving filename).
	dst := r.cfg.RcloneRemote // e.g. "gdrive:zerorated"
	gdrivePath := dst + "/" + filepath.Base(r.job.ScratchPath)

	slog.Info("rclone upload start", "id", r.job.ID, "dst", gdrivePath)

	// Track progress: rclone reports absolute transferred bytes.
	var lastBytes int64
	err := gdrive.Copy(ctx, r.job.ScratchPath, dst, func(transferred int64) {
		delta := transferred - lastBytes
		if delta > 0 {
			db.AddJobAcquiredBytes(r.db, r.job.ID, delta) //nolint:errcheck
			lastBytes = transferred
		}
	})
	if err != nil {
		return fmt.Errorf("rclone copy: %w", err)
	}

	if err := db.SetJobGdrivePath(r.db, r.job.ID, gdrivePath); err != nil {
		return err
	}
	r.job.GdrivePath = gdrivePath

	// Remove scratch file now that it's safely in GDrive.
	os.RemoveAll(filepath.Dir(r.job.ScratchPath))
	r.job.ScratchPath = ""

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusStaged); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	slog.Info("job staged in gdrive", "id", r.job.ID, "path", gdrivePath)
	return nil
}

// restoreFromGDrive copies the file from GDrive back to VPS scratch so deliver() can chunk it.
func (r *runner) restoreFromGDrive(ctx context.Context) error {
	if r.job.GdrivePath == "" {
		return errors.New("gdrive_path is empty — cannot restore")
	}

	jobDir := filepath.Join(r.cfg.ScratchDir, r.job.ID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		return fmt.Errorf("mkdir scratch: %w", err)
	}

	// rclone copy <remote>/<file> <local-dir>
	// We pass the full remote path; rclone copies the file into jobDir.
	slog.Info("rclone restore start", "id", r.job.ID, "src", r.job.GdrivePath)
	if err := gdrive.Copy(ctx, r.job.GdrivePath, jobDir, nil); err != nil {
		return fmt.Errorf("rclone restore: %w", err)
	}

	scratchPath := filepath.Join(jobDir, filepath.Base(r.job.GdrivePath))
	if err := db.SetJobScratchPath(r.db, r.job.ID, scratchPath); err != nil {
		return err
	}
	r.job.ScratchPath = scratchPath
	slog.Info("job restored from gdrive", "id", r.job.ID, "scratch", scratchPath)
	return nil
}

// MoveToGDrive moves a STAGED vps-tier job's scratch file into GDrive.
// It is called from the move-to-gdrive API handler.
func (r *runner) MoveToGDrive(ctx context.Context) error {
	if r.job.Stage != "vps" {
		return errors.New("only vps-staged jobs can be moved to gdrive")
	}
	if r.job.ScratchPath == "" {
		return errors.New("job has no scratch file")
	}

	dst := r.cfg.RcloneRemote
	gdrivePath := dst + "/" + filepath.Base(r.job.ScratchPath)
	slog.Info("rclone move start", "id", r.job.ID, "dst", gdrivePath)

	var lastBytes int64
	if err := gdrive.Copy(ctx, r.job.ScratchPath, dst, func(transferred int64) {
		delta := transferred - lastBytes
		if delta > 0 {
			db.AddJobAcquiredBytes(r.db, r.job.ID, delta) //nolint:errcheck
			lastBytes = transferred
		}
	}); err != nil {
		return fmt.Errorf("rclone copy: %w", err)
	}

	if err := db.SetJobGdrivePath(r.db, r.job.ID, gdrivePath); err != nil {
		return err
	}

	os.RemoveAll(filepath.Dir(r.job.ScratchPath))

	// Update stage to gdrive in DB so deliver() knows where to restore from.
	r.db.Exec(`UPDATE jobs SET stage='gdrive', scratch_path=NULL, updated_at=? WHERE id=?`, //nolint:errcheck
		time.Now().Unix(), r.job.ID)

	r.notify(r.job.UserKey, r.job.ID)
	slog.Info("vps job moved to gdrive", "id", r.job.ID, "path", gdrivePath)
	return nil
}

// setupDelivery initialises chunks, creates the NC directory, and uploads the manifest.
// For VPS: opens scratch file and pre-computes SHA256 per chunk.
// For stream: uses the size from the HEAD check; SHA256 is computed during upload.
func (r *runner) setupDelivery(ctx context.Context) ([]db.Chunk, error) {
	var fileSize int64
	var chunks []db.Chunk

	if r.job.Stage == "stream" {
		if r.job.Size == nil {
			return nil, errors.New("stream job has no file size")
		}
		fileSize = *r.job.Size
		cs := r.effectiveChunkSize(fileSize)
		n := numChunks(fileSize, cs)
		chunks = make([]db.Chunk, n)
		for i := 0; i < n; i++ {
			_, size := chunkRange(i, fileSize, cs)
			chunks[i] = db.Chunk{JobID: r.job.ID, Idx: i, Size: size, Status: db.ChunkPending}
		}
	} else {
		f, err := os.Open(r.job.ScratchPath)
		if err != nil {
			return nil, fmt.Errorf("open scratch: %w", err)
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			return nil, err
		}
		fileSize = info.Size()
		if r.job.Size == nil {
			if err := db.SetJobSize(r.db, r.job.ID, fileSize); err != nil {
				return nil, err
			}
			r.job.Size = &fileSize
		}

		cs := r.effectiveChunkSize(fileSize)
		n := numChunks(fileSize, cs)
		chunks = make([]db.Chunk, n)
		for i := 0; i < n; i++ {
			_, size := chunkRange(i, fileSize, cs)
			chunks[i] = db.Chunk{
				JobID:  r.job.ID,
				Idx:    i,
				Size:   size,
				Status: db.ChunkPending,
			}
		}
	}


	manifest := buildManifest(r.job.ID, r.job.Filename, fileSize, chunks)
	data, _ := json.Marshal(manifest)
	if err := withRetry(ctx, func() error {
		return nextcloud.Upload(ctx, r.ncURL("manifest.json"), r.job.NextcloudToken,
			bytes.NewReader(data), int64(len(data)))
	}); err != nil {
		return nil, fmt.Errorf("upload manifest: %w", err)
	}

	if err := db.InsertChunks(r.db, r.job.ID, chunks); err != nil {
		return nil, err
	}
	return chunks, nil
}

// doUploadChunk performs only the Nextcloud upload for a chunk (with retry).
// It does not touch DB status or send notifications — callers handle that.
func (r *runner) doUploadChunk(ctx context.Context, chunk *db.Chunk) error {
	chunkURL := r.ncChunkURL(chunk.Idx)
	return withRetry(ctx, func() error {
		if r.job.Stage == "stream" {
			return r.streamChunkOnce(ctx, chunk, chunkURL)
		}
		return r.uploadChunkOnce(ctx, chunk, chunkURL)
	})
}

// streamChunkOnce fetches one byte-range from the source and pipes it directly to Nextcloud.
func (r *runner) streamChunkOnce(ctx context.Context, chunk *db.Chunk, chunkURL string) error {
	fileSize := *r.job.Size
	start, size := chunkRange(chunk.Idx, fileSize, r.effectiveChunkSize(fileSize))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.job.URL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+size-1))
	applyHeaders(req, r.job.HeadersJSON)

	resp, err := downloadClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return noRetryErr{fmt.Errorf("source returned %d", resp.StatusCode)}
		}
		return fmt.Errorf("source returned %d (expected 206)", resp.StatusCode)
	}

	return nextcloud.Upload(ctx, chunkURL, r.job.NextcloudToken, resp.Body, size)
}

func (r *runner) uploadChunkOnce(ctx context.Context, chunk *db.Chunk, chunkURL string) error {
	f, err := os.Open(r.job.ScratchPath)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := info.Size()
	start, size := chunkRange(chunk.Idx, fileSize, r.effectiveChunkSize(fileSize))
	section := io.NewSectionReader(f, start, size)
	return nextcloud.Upload(ctx, chunkURL, r.job.NextcloudToken, section, size)
}

// --- Helpers ---

// effectiveChunkSize returns the chunk size to use for this job.
// When no_chunk is set, the full file is treated as one chunk.
func (r *runner) effectiveChunkSize(fileSize int64) int64 {
	if r.job.NoChunk {
		return fileSize
	}
	return r.cfg.ChunkSize
}

func (r *runner) ncURL(path string) string {
	return NextcloudObjectURL(r.job.NextcloudURL, r.job.ID, path)
}

func (r *runner) ncChunkURL(idx int) string {
	return ChunkURL(r.job.NextcloudURL, r.job.ID, idx)
}

// NextcloudObjectURL returns the URL for an object owned by jobID under the
// given Nextcloud share base. Files are stored flat (no subdirectory) with
// the job ID prefixed onto the filename.
func NextcloudObjectURL(ncBase, jobID, name string) string {
	base := strings.TrimRight(ncBase, "/")
	return base + "/" + jobID + "_" + name
}

// ChunkURL returns the Nextcloud URL of a specific chunk part.
func ChunkURL(ncBase, jobID string, idx int) string {
	return NextcloudObjectURL(ncBase, jobID, fmt.Sprintf("part_%04d.bin", idx))
}

func numChunks(fileSize, chunkSize int64) int {
	return int((fileSize + chunkSize - 1) / chunkSize)
}

func chunkRange(idx int, fileSize, chunkSize int64) (start, size int64) {
	start = int64(idx) * chunkSize
	end := start + chunkSize
	if end > fileSize {
		end = fileSize
	}
	return start, end - start
}

// noRetryErr marks an error that withRetry should not attempt again.
type noRetryErr struct{ error }

func withRetry(ctx context.Context, fn func() error) error {
	delays := []time.Duration{time.Second, 4 * time.Second, 16 * time.Second}
	var last error
	for attempt := 0; ; attempt++ {
		last = fn()
		if last == nil {
			return nil
		}
		if _, ok := last.(noRetryErr); ok || nextcloud.IsClientError(last) {
			return last // non-retriable
		}
		if errors.Is(last, context.Canceled) || errors.Is(last, context.DeadlineExceeded) {
			return last
		}
		if attempt >= len(delays) {
			return last
		}
		slog.Warn("transient error, retrying",
			"attempt", attempt+1,
			"delay", delays[attempt].String(),
			"err", last.Error())
		select {
		case <-time.After(delays[attempt]):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type manifestChunk struct {
	Idx  int   `json:"idx"`
	Size int64 `json:"size"`
}

type manifestDoc struct {
	JobID       string          `json:"job_id"`
	Filename    string          `json:"filename"`
	Size        int64           `json:"size"`
	ChunksTotal int             `json:"chunks_total"`
	Chunks      []manifestChunk `json:"chunks"`
}

func buildManifest(jobID, filename string, size int64, chunks []db.Chunk) manifestDoc {
	mc := make([]manifestChunk, len(chunks))
	for i, c := range chunks {
		mc[i] = manifestChunk{Idx: c.Idx, Size: c.Size}
	}
	return manifestDoc{
		JobID:       jobID,
		Filename:    filename,
		Size:        size,
		ChunksTotal: len(chunks),
		Chunks:      mc,
	}
}

type progressWriter struct {
	db      *sql.DB
	jobID   string
	buf     int64
	total   int64
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	pw.buf += int64(len(p))
	pw.total += int64(len(p))
	if pw.buf >= 1<<20 { // flush every 1 MB
		db.AddJobAcquiredBytes(pw.db, pw.jobID, pw.buf) //nolint:errcheck
		pw.buf = 0
	}
	return len(p), nil
}

func (pw *progressWriter) flush() error {
	if pw.buf > 0 {
		return db.AddJobAcquiredBytes(pw.db, pw.jobID, pw.buf)
	}
	return nil
}

func applyHeaders(req *http.Request, headersJSON string) {
	if headersJSON == "" {
		return
	}
	var h struct {
		Referer   string `json:"referer"`
		UserAgent string `json:"user_agent"`
	}
	if err := json.Unmarshal([]byte(headersJSON), &h); err != nil {
		return
	}
	if h.Referer != "" {
		req.Header.Set("Referer", h.Referer)
	}
	if h.UserAgent != "" {
		req.Header.Set("User-Agent", h.UserAgent)
	}
}

func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	if name == "." || name == "" {
		return "download"
	}
	return name
}

// NewID generates a random UUID v4.
func NewID() string {
	var b [16]byte
	rand.Read(b[:]) //nolint:errcheck
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// FilenameFromURL extracts the last path segment of u as a filename fallback.
func FilenameFromURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "download"
	}
	parts := strings.Split(u.Path, "/")
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" {
			name, _ := url.PathUnescape(parts[i])
			return sanitizeFilename(name)
		}
	}
	return "download"
}
