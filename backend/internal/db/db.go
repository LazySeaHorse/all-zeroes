package db

import (
	"database/sql"
	"strings"

	_ "modernc.org/sqlite"
)

// User mirrors the users table.
type User struct {
	APIKey    string
	Name      string
	IsOwner   int // 0 or 1; use bool helpers at call sites
	CreatedAt int64
}

func Open(path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Single writer prevents "database is locked"; WAL allows concurrent reads.
	database.SetMaxOpenConns(1)

	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA foreign_keys=ON`,
		`PRAGMA busy_timeout=5000`,
	} {
		if _, err := database.Exec(pragma); err != nil {
			database.Close()
			return nil, err
		}
	}

	if err := migrate(database); err != nil {
		database.Close()
		return nil, err
	}
	return database, nil
}

func migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			api_key    TEXT PRIMARY KEY,
			name       TEXT    NOT NULL,
			is_owner   INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id             TEXT PRIMARY KEY,
			user_key       TEXT    NOT NULL REFERENCES users(api_key),
			url            TEXT    NOT NULL,
			headers_json   TEXT,
			filename       TEXT    NOT NULL,
			size           INTEGER,
			stage          TEXT    NOT NULL,
			status         TEXT    NOT NULL,
			error          TEXT,
			acquired_bytes INTEGER NOT NULL DEFAULT 0,
			scratch_path   TEXT,
			gdrive_path    TEXT,
			nextcloud_url  TEXT    NOT NULL,
			nextcloud_token TEXT   NOT NULL,
			deliver_now    INTEGER NOT NULL,
			created_at     INTEGER NOT NULL,
			updated_at     INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chunks (
			job_id      TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
			idx         INTEGER NOT NULL,
			size        INTEGER NOT NULL,
			sha256      TEXT,
			status      TEXT    NOT NULL,
			uploaded_at INTEGER,
			acked_at    INTEGER,
			PRIMARY KEY (job_id, idx)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_status_user ON jobs(user_key, status)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return err
		}
	}

	// Additive column migrations — ignore "duplicate column name" on re-runs.
	for _, s := range []string{
		`ALTER TABLE jobs ADD COLUMN no_chunk INTEGER NOT NULL DEFAULT 0`,
	} {
		if _, err := db.Exec(s); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return err
		}
	}
	return nil
}

// UpsertUsers inserts or updates users from the seed file.
// created_at is preserved on conflict (only name/is_owner are updated).
func UpsertUsers(db *sql.DB, users []User) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	stmt, err := tx.Prepare(`
		INSERT INTO users (api_key, name, is_owner, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(api_key) DO UPDATE SET
			name     = excluded.name,
			is_owner = excluded.is_owner
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, u := range users {
		if _, err := stmt.Exec(u.APIKey, u.Name, u.IsOwner, u.CreatedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetUserByKey returns nil, nil when no matching user exists.
func GetUserByKey(db *sql.DB, apiKey string) (*User, error) {
	var u User
	err := db.QueryRow(
		`SELECT api_key, name, is_owner, created_at FROM users WHERE api_key = ?`,
		apiKey,
	).Scan(&u.APIKey, &u.Name, &u.IsOwner, &u.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}
