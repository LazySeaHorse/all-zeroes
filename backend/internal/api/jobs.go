package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"allzeroes/internal/db"
	"allzeroes/internal/jobs"
)

// jobManager is the subset of jobs.Manager the API layer needs.
type jobManager interface {
	Start(j *db.Job)
	AckChunk(jobID string, idx int) bool
	Cancel(jobID string)
	StartDeliver(j *db.Job)
}

// submitRequest is the body for POST /api/jobs.
type submitRequest struct {
	URL            string `json:"url"`
	Referer        string `json:"referer"`
	UserAgent      string `json:"user_agent"`
	Filename       string `json:"filename"`
	Stage          string `json:"stage"`
	DeliverNow     *bool  `json:"deliver_now"`
	NextcloudURL   string `json:"nextcloud_url"`
	NextcloudToken string `json:"nextcloud_token"`
}

// jobResponse is the JSON shape returned for a single job.
type jobResponse struct {
	ID            string     `json:"id"`
	URL           string     `json:"url"`
	Filename      string     `json:"filename"`
	Size          *int64     `json:"size"`
	Status        string     `json:"status"`
	Stage         string     `json:"stage"`
	Error         *string    `json:"error"`
	AcquiredBytes int64      `json:"acquired_bytes"`
	ChunksTotal   int        `json:"chunks_total"`
	ChunksDone    int        `json:"chunks_done"`
	CurrentChunk  *chunkInfo `json:"current_chunk"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type chunkInfo struct {
	Idx    int    `json:"idx"`
	URL    string `json:"url"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

func submitHandler(mgr jobManager, database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)

		var req submitRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if req.URL == "" {
			http.Error(w, "url is required", http.StatusBadRequest)
			return
		}
		if req.NextcloudURL == "" || req.NextcloudToken == "" {
			http.Error(w, "nextcloud_url and nextcloud_token are required", http.StatusBadRequest)
			return
		}

		stage := req.Stage
		if stage == "" {
			stage = "vps"
		}
		switch stage {
		case "vps":
			// supported
		case "stream", "gdrive":
			http.Error(w, stage+" stage not implemented yet", http.StatusNotImplemented)
			return
		default:
			http.Error(w, "stage must be vps, stream, or gdrive", http.StatusBadRequest)
			return
		}

		deliverNow := true
		if req.DeliverNow != nil {
			deliverNow = *req.DeliverNow
		}

		filename := req.Filename
		if filename == "" {
			filename = jobs.FilenameFromURL(req.URL)
		}

		var headersJSON string
		if req.Referer != "" || req.UserAgent != "" {
			b, _ := json.Marshal(map[string]string{
				"referer":    req.Referer,
				"user_agent": req.UserAgent,
			})
			headersJSON = string(b)
		}

		j := &db.Job{
			ID:             jobs.NewID(),
			UserKey:        user.APIKey,
			URL:            req.URL,
			HeadersJSON:    headersJSON,
			Filename:       filename,
			Stage:          stage,
			Status:         db.StatusQueued,
			NextcloudURL:   req.NextcloudURL,
			NextcloudToken: req.NextcloudToken,
			DeliverNow:     deliverNow,
		}
		if err := db.CreateJob(database, j); err != nil {
			http.Error(w, "failed to create job", http.StatusInternalServerError)
			return
		}

		mgr.Start(j)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(jobToResponse(j, nil)) //nolint:errcheck
	}
}

func listJobsHandler(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)
		jobList, err := db.ListJobsByUser(database, user.APIKey)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		type listResp struct {
			Jobs []jobResponse `json:"jobs"`
		}
		resp := listResp{Jobs: make([]jobResponse, 0, len(jobList))}
		for i := range jobList {
			chunks, _ := db.GetChunks(database, jobList[i].ID)
			resp.Jobs = append(resp.Jobs, jobToResponse(&jobList[i], chunks))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	}
}

func getJobHandler(database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)
		j, err := db.GetJobForUser(database, chi.URLParam(r, "id"), user.APIKey)
		if err != nil || j == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		chunks, _ := db.GetChunks(database, j.ID)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jobToResponse(j, chunks)) //nolint:errcheck
	}
}

func chunkDoneHandler(mgr jobManager, database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)
		id := chi.URLParam(r, "id")

		j, err := db.GetJobForUser(database, id, user.APIKey)
		if err != nil || j == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if j.Status != db.StatusDelivering {
			http.Error(w, "job is not delivering", http.StatusConflict)
			return
		}

		var body struct {
			Idx int `json:"idx"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		chunks, err := db.GetChunks(database, id)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		var target *db.Chunk
		for i := range chunks {
			if chunks[i].Idx == body.Idx {
				target = &chunks[i]
				break
			}
		}
		if target == nil {
			http.Error(w, "chunk not found", http.StatusNotFound)
			return
		}
		if target.Status == db.ChunkAcked {
			w.WriteHeader(http.StatusNoContent) // idempotent
			return
		}
		if target.Status != db.ChunkUploaded {
			http.Error(w, "chunk not ready for ack", http.StatusConflict)
			return
		}

		if !mgr.AckChunk(id, body.Idx) {
			http.Error(w, "job runner not active", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func deleteJobHandler(mgr jobManager, database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)
		j, err := db.GetJobForUser(database, chi.URLParam(r, "id"), user.APIKey)
		if err != nil || j == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		mgr.Cancel(j.ID)
		w.WriteHeader(http.StatusNoContent)
	}
}

func deliverHandler(mgr jobManager, database *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user := currentUser(r)
		j, err := db.GetJobForUser(database, chi.URLParam(r, "id"), user.APIKey)
		if err != nil || j == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if j.Status != db.StatusStaged {
			http.Error(w, "job is not STAGED", http.StatusConflict)
			return
		}
		mgr.StartDeliver(j)
		w.WriteHeader(http.StatusAccepted)
	}
}

// jobToResponse converts a Job and its chunks to the response shape.
func jobToResponse(j *db.Job, chunks []db.Chunk) jobResponse {
	var total, done int
	var current *chunkInfo

	ncBase := j.NextcloudURL
	if len(ncBase) > 0 && ncBase[len(ncBase)-1] != '/' {
		ncBase += "/"
	}

	for i := range chunks {
		total++
		if chunks[i].Status == db.ChunkAcked {
			done++
		}
		if current == nil &&
			(chunks[i].Status == db.ChunkUploaded || chunks[i].Status == db.ChunkUploading) {
			current = &chunkInfo{
				Idx:    chunks[i].Idx,
				URL:    ncBase + j.ID + "/" + fmt.Sprintf("part_%04d.bin", chunks[i].Idx),
				Size:   chunks[i].Size,
				SHA256: chunks[i].SHA256,
			}
		}
	}

	return jobResponse{
		ID:            j.ID,
		URL:           j.URL,
		Filename:      j.Filename,
		Size:          j.Size,
		Status:        j.Status,
		Stage:         j.Stage,
		Error:         j.Error,
		AcquiredBytes: j.AcquiredBytes,
		ChunksTotal:   total,
		ChunksDone:    done,
		CurrentChunk:  current,
		CreatedAt:     time.Unix(j.CreatedAt, 0).UTC(),
		UpdatedAt:     time.Unix(j.UpdatedAt, 0).UTC(),
	}
}
