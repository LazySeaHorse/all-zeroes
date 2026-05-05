package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"allzeroes/internal/api"
	"allzeroes/internal/db"
	"allzeroes/internal/jobs"
)

func main() {
	// Structured JSON logs — easy to grep and forward to log aggregators.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	dbPath := envOr("DB_PATH", "state.db")
	scratchDir := envOr("SCRATCH_DIR", "./scratch")
	listenAddr := envOr("LISTEN_ADDR", "127.0.0.1:8080")
	allowedOrigins := envOr("ALLOWED_ORIGINS", "http://localhost:5173")
	usersFile := envOr("USERS_FILE", "users.json")
	rcloneRemote := envOr("RCLONE_REMOTE", "gdrive:zerorated")
	maxAcqs := envInt("MAX_CONCURRENT_ACQUIRES", 4)
	chunkSize := envInt64("CHUNK_SIZE_BYTES", 1_610_612_736) // 1.5 GB

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

	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		slog.Error("create scratch dir", "err", err)
		os.Exit(1)
	}

	br := api.NewBroadcaster()

	mgr := jobs.New(database, jobs.Config{
		ScratchDir:        scratchDir,
		ChunkSize:         chunkSize,
		MaxConcurrentAcqs: maxAcqs,
		RcloneRemote:      rcloneRemote,
	})
	mgr.OnUpdate = func(userKey, jobID string) {
		api.PublishJobUpdate(database, br, userKey, jobID)
	}
	if err := mgr.Resurrect(); err != nil {
		slog.Error("resurrect jobs", "err", err)
		os.Exit(1)
	}

	router := api.NewRouter(database, mgr, br, allowedOrigins)

	srv := &http.Server{
		Addr:        listenAddr,
		Handler:     router,
		ReadTimeout: 30 * time.Second,
		// WriteTimeout unset: SSE connections are long-lived.
		IdleTimeout: 120 * time.Second,
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutting down gracefully…")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			slog.Error("shutdown error", "err", err)
		}
	}()

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

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
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
