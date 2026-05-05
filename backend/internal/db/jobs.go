package db

import (
	"database/sql"
	"time"
)

// Job status constants.
const (
	StatusQueued     = "QUEUED"
	StatusAcquiring  = "ACQUIRING"
	StatusStaged     = "STAGED"
	StatusDelivering = "DELIVERING"
	StatusDone       = "DONE"
	StatusFailed     = "FAILED"
	StatusCanceled   = "CANCELED"
)

// Chunk status constants.
const (
	ChunkPending   = "pending"
	ChunkUploading = "uploading"
	ChunkUploaded  = "uploaded"
	ChunkAcked     = "acked"
)

// Job represents a row in the jobs table.
type Job struct {
	ID             string
	UserKey        string
	URL            string
	HeadersJSON    string // empty if null
	Filename       string
	Size           *int64
	Stage          string
	Status         string
	Error          *string
	AcquiredBytes  int64
	ScratchPath    string // empty if null
	GdrivePath     string // empty if null
	NextcloudURL   string
	NextcloudToken string
	DeliverNow     bool
	CreatedAt      int64
	UpdatedAt      int64
}

// Chunk represents a row in the chunks table.
type Chunk struct {
	JobID      string
	Idx        int
	Size       int64
	SHA256     string // empty until computed
	Status     string
	UploadedAt *int64
	AckedAt    *int64
}

func CreateJob(db *sql.DB, j *Job) error {
	now := time.Now().Unix()
	j.CreatedAt = now
	j.UpdatedAt = now
	var size *int64
	if j.Size != nil {
		size = j.Size
	}
	_, err := db.Exec(`
		INSERT INTO jobs
			(id, user_key, url, headers_json, filename, size, stage, status,
			 acquired_bytes, nextcloud_url, nextcloud_token, deliver_now,
			 created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?, 0,?,?,?, ?,?)`,
		j.ID, j.UserKey, j.URL, nullStr(j.HeadersJSON), j.Filename, size, j.Stage, j.Status,
		j.NextcloudURL, j.NextcloudToken, boolInt(j.DeliverNow),
		now, now,
	)
	return err
}

func GetJob(db *sql.DB, id string) (*Job, error) {
	row := db.QueryRow(`
		SELECT id, user_key, url, headers_json, filename, size, stage, status,
		       error, acquired_bytes, scratch_path, gdrive_path,
		       nextcloud_url, nextcloud_token, deliver_now, created_at, updated_at
		FROM jobs WHERE id = ?`, id)
	return scanJob(row)
}

func GetJobForUser(db *sql.DB, id, userKey string) (*Job, error) {
	row := db.QueryRow(`
		SELECT id, user_key, url, headers_json, filename, size, stage, status,
		       error, acquired_bytes, scratch_path, gdrive_path,
		       nextcloud_url, nextcloud_token, deliver_now, created_at, updated_at
		FROM jobs WHERE id = ? AND user_key = ?`, id, userKey)
	return scanJob(row)
}

func ListJobsByUser(db *sql.DB, userKey string) ([]Job, error) {
	rows, err := db.Query(`
		SELECT id, user_key, url, headers_json, filename, size, stage, status,
		       error, acquired_bytes, scratch_path, gdrive_path,
		       nextcloud_url, nextcloud_token, deliver_now, created_at, updated_at
		FROM jobs WHERE user_key = ?
		ORDER BY created_at DESC`, userKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

// GetNonTerminalJobs returns all jobs not in a terminal state, for resurrection on startup.
func GetNonTerminalJobs(db *sql.DB) ([]Job, error) {
	rows, err := db.Query(`
		SELECT id, user_key, url, headers_json, filename, size, stage, status,
		       error, acquired_bytes, scratch_path, gdrive_path,
		       nextcloud_url, nextcloud_token, deliver_now, created_at, updated_at
		FROM jobs WHERE status NOT IN ('DONE','FAILED','CANCELED')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, rows.Err()
}

func SetJobStatus(db *sql.DB, id, status string) error {
	_, err := db.Exec(`UPDATE jobs SET status=?, updated_at=? WHERE id=?`,
		status, time.Now().Unix(), id)
	return err
}

func SetJobSize(db *sql.DB, id string, size int64) error {
	_, err := db.Exec(`UPDATE jobs SET size=?, updated_at=? WHERE id=?`,
		size, time.Now().Unix(), id)
	return err
}

func SetJobScratchPath(db *sql.DB, id, path string) error {
	_, err := db.Exec(`UPDATE jobs SET scratch_path=?, updated_at=? WHERE id=?`,
		path, time.Now().Unix(), id)
	return err
}

func AddJobAcquiredBytes(db *sql.DB, id string, delta int64) error {
	_, err := db.Exec(`UPDATE jobs SET acquired_bytes=acquired_bytes+?, updated_at=? WHERE id=?`,
		delta, time.Now().Unix(), id)
	return err
}

func SetJobFailed(db *sql.DB, id, errMsg string) error {
	_, err := db.Exec(
		`UPDATE jobs SET status='FAILED', error=?, nextcloud_token='', updated_at=? WHERE id=?`,
		errMsg, time.Now().Unix(), id)
	return err
}

func SetJobDone(db *sql.DB, id string) error {
	_, err := db.Exec(
		`UPDATE jobs SET status='DONE', nextcloud_token='', updated_at=? WHERE id=?`,
		time.Now().Unix(), id)
	return err
}

func SetJobCanceled(db *sql.DB, id string) error {
	_, err := db.Exec(
		`UPDATE jobs SET status='CANCELED', nextcloud_token='', updated_at=? WHERE id=?`,
		time.Now().Unix(), id)
	return err
}

// --- Chunks ---

func InsertChunks(db *sql.DB, jobID string, chunks []Chunk) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.Prepare(`
		INSERT INTO chunks (job_id, idx, size, sha256, status)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range chunks {
		if _, err := stmt.Exec(jobID, c.Idx, c.Size, nullStr(c.SHA256), c.Status); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func GetChunks(db *sql.DB, jobID string) ([]Chunk, error) {
	rows, err := db.Query(`
		SELECT job_id, idx, size, sha256, status, uploaded_at, acked_at
		FROM chunks WHERE job_id = ? ORDER BY idx`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chunks []Chunk
	for rows.Next() {
		var c Chunk
		var sha sql.NullString
		if err := rows.Scan(&c.JobID, &c.Idx, &c.Size, &sha, &c.Status, &c.UploadedAt, &c.AckedAt); err != nil {
			return nil, err
		}
		if sha.Valid {
			c.SHA256 = sha.String
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

func SetChunkSHA256(db *sql.DB, jobID string, idx int, digest string) error {
	_, err := db.Exec(`UPDATE chunks SET sha256=? WHERE job_id=? AND idx=?`, digest, jobID, idx)
	return err
}

func SetChunkStatus(db *sql.DB, jobID string, idx int, status string) error {
	now := time.Now().Unix()
	switch status {
	case ChunkUploaded:
		_, err := db.Exec(`UPDATE chunks SET status=?, uploaded_at=? WHERE job_id=? AND idx=?`,
			status, now, jobID, idx)
		return err
	case ChunkAcked:
		_, err := db.Exec(`UPDATE chunks SET status=?, acked_at=? WHERE job_id=? AND idx=?`,
			status, now, jobID, idx)
		return err
	default:
		_, err := db.Exec(`UPDATE chunks SET status=? WHERE job_id=? AND idx=?`,
			status, jobID, idx)
		return err
	}
}

// --- helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func scanJob(s scanner) (*Job, error) {
	var j Job
	var headersJSON, scratchPath, gdrivePath, errStr sql.NullString
	var size sql.NullInt64
	var deliverNow int64

	err := s.Scan(
		&j.ID, &j.UserKey, &j.URL, &headersJSON, &j.Filename, &size, &j.Stage, &j.Status,
		&errStr, &j.AcquiredBytes, &scratchPath, &gdrivePath,
		&j.NextcloudURL, &j.NextcloudToken, &deliverNow, &j.CreatedAt, &j.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if headersJSON.Valid {
		j.HeadersJSON = headersJSON.String
	}
	if size.Valid {
		j.Size = &size.Int64
	}
	if errStr.Valid {
		j.Error = &errStr.String
	}
	if scratchPath.Valid {
		j.ScratchPath = scratchPath.String
	}
	if gdrivePath.Valid {
		j.GdrivePath = gdrivePath.String
	}
	j.DeliverNow = deliverNow != 0
	return &j, nil
}

func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
