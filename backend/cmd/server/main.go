package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"allzeroes/internal/api"
	"allzeroes/internal/db"
)

func main() {
	dbPath := envOr("DB_PATH", "state.db")
	scratchDir := envOr("SCRATCH_DIR", "./scratch")
	listenAddr := envOr("LISTEN_ADDR", "127.0.0.1:8080")
	allowedOrigins := envOr("ALLOWED_ORIGINS", "http://localhost:5173")
	usersFile := envOr("USERS_FILE", "users.json")

	_ = scratchDir // used in Phase 2

	database, err := db.Open(dbPath)
	if err != nil {
		slog.Error("open database", "err", err)
		os.Exit(1)
	}
	defer database.Close()

	users, err := loadUsers(usersFile)
	if err != nil {
		slog.Error("load users file", "file", usersFile, "err", err)
		os.Exit(1)
	}
	if err := db.UpsertUsers(database, users); err != nil {
		slog.Error("upsert users", "err", err)
		os.Exit(1)
	}
	slog.Info("users loaded", "count", len(users))

	router := api.NewRouter(database, allowedOrigins)

	srv := &http.Server{
		Addr:        listenAddr,
		Handler:     router,
		ReadTimeout: 30 * time.Second,
		// WriteTimeout intentionally unset: SSE connections are long-lived.
		IdleTimeout: 120 * time.Second,
	}

	slog.Info("listening", "addr", listenAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type userSeed struct {
	APIKey  string `json:"api_key"`
	Name    string `json:"name"`
	IsOwner bool   `json:"is_owner"`
}

func loadUsers(path string) ([]db.User, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var seeds []userSeed
	if err := json.NewDecoder(f).Decode(&seeds); err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	users := make([]db.User, len(seeds))
	for i, s := range seeds {
		isOwner := 0
		if s.IsOwner {
			isOwner = 1
		}
		users[i] = db.User{
			APIKey:    s.APIKey,
			Name:      s.Name,
			IsOwner:   isOwner,
			CreatedAt: now,
		}
	}
	return users, nil
}
