package jobs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
		if err := r.acquire(ctx); err != nil {
			return err
		}
		if !r.job.DeliverNow {
			return nil // wait for explicit POST /deliver
		}
		return r.deliver(ctx)

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

	if err := db.SetJobStatus(r.db, r.job.ID, db.StatusStaged); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	slog.Info("job staged", "id", r.job.ID, "size", info.Size())
	return nil
}

// deliver chunks and uploads the staged file to Nextcloud.
func (r *runner) deliver(ctx context.Context) error {
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

	for i := range chunks {
		if chunks[i].Status == db.ChunkAcked {
			continue // already done; resume from here on resurrection
		}
		if err := r.deliverChunk(ctx, &chunks[i]); err != nil {
			return err
		}
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

// setupDelivery pre-computes SHA256 for each chunk, inserts chunk rows, and uploads the manifest.
func (r *runner) setupDelivery(ctx context.Context) ([]db.Chunk, error) {
	f, err := os.Open(r.job.ScratchPath)
	if err != nil {
		return nil, fmt.Errorf("open scratch: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if r.job.Size == nil {
		if err := db.SetJobSize(r.db, r.job.ID, fileSize); err != nil {
			return nil, err
		}
		r.job.Size = &fileSize
	}

	n := numChunks(fileSize, r.cfg.ChunkSize)
	chunks := make([]db.Chunk, n)
	for i := 0; i < n; i++ {
		start, size := chunkRange(i, fileSize, r.cfg.ChunkSize)
		h := sha256.New()
		if _, err := io.Copy(h, io.NewSectionReader(f, start, size)); err != nil {
			return nil, fmt.Errorf("sha256 chunk %d: %w", i, err)
		}
		chunks[i] = db.Chunk{
			JobID:  r.job.ID,
			Idx:    i,
			Size:   size,
			SHA256: hex.EncodeToString(h.Sum(nil)),
			Status: db.ChunkPending,
		}
	}

	// Create job directory on Nextcloud.
	if err := withRetry(ctx, func() error {
		return nextcloud.Mkdir(ctx, r.ncURL(""), r.job.NextcloudToken)
	}); err != nil {
		return nil, fmt.Errorf("NC mkdir: %w", err)
	}

	// Upload manifest.
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

func (r *runner) deliverChunk(ctx context.Context, chunk *db.Chunk) error {
	r.userMu.Lock()
	defer r.userMu.Unlock()

	db.SetChunkStatus(r.db, r.job.ID, chunk.Idx, db.ChunkUploading) //nolint:errcheck

	chunkURL := r.ncChunkURL(chunk.Idx)
	err := withRetry(ctx, func() error {
		return r.uploadChunkOnce(ctx, chunk, chunkURL)
	})
	if err != nil {
		return fmt.Errorf("upload chunk %d: %w", chunk.Idx, err)
	}

	if err := db.SetChunkStatus(r.db, r.job.ID, chunk.Idx, db.ChunkUploaded); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)

	// Wait for user ack.
	select {
	case <-r.ackCh:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Delete chunk from Nextcloud before releasing the user mutex.
	nextcloud.Delete(ctx, chunkURL, r.job.NextcloudToken) //nolint:errcheck

	if err := db.SetChunkStatus(r.db, r.job.ID, chunk.Idx, db.ChunkAcked); err != nil {
		return err
	}
	r.notify(r.job.UserKey, r.job.ID)
	return nil
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
	start, size := chunkRange(chunk.Idx, info.Size(), r.cfg.ChunkSize)
	section := io.NewSectionReader(f, start, size)
	return nextcloud.Upload(ctx, chunkURL, r.job.NextcloudToken, section, size)
}

// --- Helpers ---

func (r *runner) ncURL(path string) string {
	base := strings.TrimRight(r.job.NextcloudURL, "/")
	if path == "" {
		return base + "/" + r.job.ID + "/"
	}
	return base + "/" + r.job.ID + "/" + path
}

func (r *runner) ncChunkURL(idx int) string {
	return r.ncURL(fmt.Sprintf("part_%04d.bin", idx))
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

func withRetry(ctx context.Context, fn func() error) error {
	delays := []time.Duration{time.Second, 4 * time.Second, 16 * time.Second}
	var last error
	for attempt := 0; ; attempt++ {
		last = fn()
		if last == nil {
			return nil
		}
		if nextcloud.IsClientError(last) {
			return last // 4xx: do not retry
		}
		if errors.Is(last, context.Canceled) || errors.Is(last, context.DeadlineExceeded) {
			return last
		}
		if attempt >= len(delays) {
			return last
		}
		select {
		case <-time.After(delays[attempt]):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type manifestChunk struct {
	Idx    int    `json:"idx"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
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
		mc[i] = manifestChunk{Idx: c.Idx, Size: c.Size, SHA256: c.SHA256}
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
