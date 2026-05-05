package jobs

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"

	"allzeroes/internal/db"
)

// Config holds tunable parameters read from environment.
type Config struct {
	ScratchDir        string
	ChunkSize         int64
	MaxConcurrentAcqs int
	RcloneRemote      string // Phase 5
}

// Manager owns all active job goroutines.
type Manager struct {
	db       *sql.DB
	cfg      Config
	acqSem   chan struct{}
	userMu   sync.Map // string -> *sync.Mutex
	ackChans sync.Map // string -> chan int
	cancels  sync.Map // string -> context.CancelFunc

	// OnUpdate is called after any job/chunk state change. Replaced in Phase 3 with SSE fan-out.
	OnUpdate func(userKey, jobID string)
}

func New(database *sql.DB, cfg Config) *Manager {
	m := &Manager{
		db:       database,
		cfg:      cfg,
		acqSem:   make(chan struct{}, cfg.MaxConcurrentAcqs),
		OnUpdate: func(_, _ string) {},
	}
	// Pre-fill semaphore slots.
	for i := 0; i < cfg.MaxConcurrentAcqs; i++ {
		m.acqSem <- struct{}{}
	}
	return m
}

// Resurrect restarts goroutines for all non-terminal jobs found in the DB.
// Must be called once at startup before accepting HTTP traffic.
func (m *Manager) Resurrect() error {
	jobs, err := db.GetNonTerminalJobs(m.db)
	if err != nil {
		return err
	}
	for i := range jobs {
		j := jobs[i]
		slog.Info("resurrecting job", "id", j.ID, "status", j.Status)
		m.launch(&j)
	}
	return nil
}

// Start registers and launches a goroutine for a newly created job.
func (m *Manager) Start(j *db.Job) {
	m.launch(j)
}

// AckChunk signals the runner that chunk idx has been acked by the user.
// Returns false if the job has no active runner (not in delivering state).
func (m *Manager) AckChunk(jobID string, idx int) bool {
	v, ok := m.ackChans.Load(jobID)
	if !ok {
		return false
	}
	ch := v.(chan int)
	select {
	case ch <- idx:
		return true
	default:
		// Channel full — ack already pending.
		return false
	}
}

// Cancel marks a job canceled and stops its goroutine.
func (m *Manager) Cancel(jobID string) {
	// Mark canceled before canceling context so the runner doesn't overwrite with FAILED.
	db.SetJobCanceled(m.db, jobID) //nolint:errcheck
	if v, ok := m.cancels.Load(jobID); ok {
		v.(context.CancelFunc)()
	}
}

// StartDeliver begins delivery for a STAGED job (called from the /deliver endpoint).
func (m *Manager) StartDeliver(j *db.Job) {
	// Update the job's deliver_now flag and relaunch.
	m.db.Exec(`UPDATE jobs SET deliver_now=1 WHERE id=?`, j.ID) //nolint:errcheck
	j.DeliverNow = true
	j.Status = db.StatusStaged
	m.launch(j)
}

// MoveToGDrive moves a STAGED vps job to GDrive in a background goroutine.
// Returns immediately; the operation runs asynchronously.
func (m *Manager) MoveToGDrive(j *db.Job) {
	go func() {
		r := &runner{
			db:     m.db,
			cfg:    m.cfg,
			job:    j,
			ackCh:  make(chan int, 1),
			acqSem: m.acqSem,
			notify: m.OnUpdate,
			userMu: m.userMutex(j.UserKey),
		}
		if err := r.MoveToGDrive(context.Background()); err != nil {
			slog.Error("move-to-gdrive failed", "id", j.ID, "err", err)
			db.SetJobFailed(m.db, j.ID, err.Error()) //nolint:errcheck
			m.OnUpdate(j.UserKey, j.ID)
		}
	}()
}

func (m *Manager) launch(j *db.Job) {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancels.Store(j.ID, cancel)

	go func() {
		defer m.cancels.Delete(j.ID)

		ackCh := make(chan int, 1)
		m.ackChans.Store(j.ID, ackCh)
		defer m.ackChans.Delete(j.ID)

		r := &runner{
			db:     m.db,
			cfg:    m.cfg,
			job:    j,
			ackCh:  ackCh,
			acqSem: m.acqSem,
			notify: m.OnUpdate,
			userMu: m.userMutex(j.UserKey),
		}

		if err := r.run(ctx); err != nil {
			// Only mark failed if not already in a terminal state (e.g., CANCELED).
			current, _ := db.GetJob(m.db, j.ID)
			if current != nil && current.Status != db.StatusCanceled &&
				current.Status != db.StatusDone {
				slog.Error("job failed", "id", j.ID, "err", err)
				db.SetJobFailed(m.db, j.ID, err.Error()) //nolint:errcheck
				m.OnUpdate(j.UserKey, j.ID)
			}
		}
	}()
}

func (m *Manager) userMutex(userKey string) *sync.Mutex {
	v, _ := m.userMu.LoadOrStore(userKey, &sync.Mutex{})
	return v.(*sync.Mutex)
}
