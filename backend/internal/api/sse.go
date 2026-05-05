package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"allzeroes/internal/db"
)

// Broadcaster fans job-update events out to all SSE subscribers for a given user.
type Broadcaster struct {
	mu   sync.Mutex
	subs map[string][]chan []byte // userKey -> subscriber channels
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: make(map[string][]chan []byte)}
}

// Subscribe returns a read-only channel for the user and an unsubscribe func.
func (b *Broadcaster) Subscribe(userKey string) (<-chan []byte, func()) {
	ch := make(chan []byte, 32)
	b.mu.Lock()
	b.subs[userKey] = append(b.subs[userKey], ch)
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subs[userKey]
		for i, c := range subs {
			if c == ch {
				b.subs[userKey] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		close(ch)
	}
}

// Publish sends data to all subscribers for the user. Drops if a subscriber is slow.
func (b *Broadcaster) Publish(userKey string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs[userKey] {
		select {
		case ch <- data:
		default:
		}
	}
}

// PublishJobUpdate fetches the current job state from DB and broadcasts it.
// Called by jobs.Manager.OnUpdate.
func PublishJobUpdate(database *sql.DB, br *Broadcaster, userKey, jobID string) {
	j, err := db.GetJob(database, jobID)
	if err != nil || j == nil {
		return
	}
	chunks, _ := db.GetChunks(database, j.ID)
	data, err := json.Marshal(jobToResponse(j, chunks))
	if err != nil {
		return
	}
	br.Publish(userKey, data)
}

func sseHandler(database *sql.DB, br *Broadcaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming not supported", http.StatusInternalServerError)
			return
		}

		user := currentUser(r)

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // disable nginx buffering

		// Send current state of all user's jobs as initial burst.
		jobList, _ := db.ListJobsByUser(database, user.APIKey)
		for i := range jobList {
			chunks, _ := db.GetChunks(database, jobList[i].ID)
			data, _ := json.Marshal(jobToResponse(&jobList[i], chunks))
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		flusher.Flush()

		ch, unsub := br.Subscribe(user.APIKey)
		defer unsub()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case data, open := <-ch:
				if !open {
					return
				}
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			case <-ticker.C:
				fmt.Fprintf(w, ": heartbeat\n\n")
				flusher.Flush()
			case <-r.Context().Done():
				return
			}
		}
	}
}
