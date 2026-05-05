package api

import (
	"database/sql"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func NewRouter(database *sql.DB, mgr jobManager, br *Broadcaster, allowedOrigins string) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware(allowedOrigins))

	r.Get("/healthz", healthz)

	r.Route("/api", func(r chi.Router) {
		r.Use(authenticate(database))

		r.Post("/jobs", submitHandler(mgr, database))
		r.Get("/jobs", listJobsHandler(database))
		r.Get("/jobs/stream", sseHandler(database, br))
		r.Get("/jobs/{id}", getJobHandler(database))
		r.Post("/jobs/{id}/chunk-done", chunkDoneHandler(mgr, database))
		r.Post("/jobs/{id}/deliver", deliverHandler(mgr, database))
		r.Post("/jobs/{id}/move-to-gdrive", notImplemented) // Phase 5
		r.Delete("/jobs/{id}", deleteJobHandler(mgr, database))
	})

	return r
}

func corsMiddleware(allowedOrigins string) func(http.Handler) http.Handler {
	parts := strings.Split(allowedOrigins, ",")
	exact := make(map[string]bool, len(parts))
	localhostWild := false
	for _, o := range parts {
		o = strings.TrimSpace(o)
		if o == "http://localhost:*" {
			localhostWild = true
		} else {
			exact[o] = true
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			allowed := exact[origin] ||
				(localhostWild && strings.HasPrefix(origin, "http://localhost:"))

			if allowed {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
