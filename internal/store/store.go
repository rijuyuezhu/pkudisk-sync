package store

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 3

// Store is the durable semantic authority for sync roots, committed baselines,
// external-side-effect intents, and conflicts.
type Store struct {
	db         *sql.DB
	now        func() time.Time
	syncRootMu sync.Mutex
}

// Open opens or creates a SQLite state database and applies schema migrations.
func Open(ctx context.Context, dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("database path must not be empty")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// One writer connection keeps transaction ordering simple. SQLite WAL still
	// allows readers from other processes if that is ever needed for debugging.
	db.SetMaxOpenConns(1)

	s := &Store{db: db, now: func() time.Time { return time.Now().UTC() }}
	if err := s.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initialize(ctx context.Context) error {
	for _, pragma := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
	} {
		if _, err := s.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("apply %q: %w", pragma, err)
		}
	}
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite: %w", err)
	}
	return s.migrate(ctx)
}

func (s *Store) migrate(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version > schemaVersion {
		return fmt.Errorf("state database schema version %d is newer than supported version %d", version, schemaVersion)
	}
	if version == schemaVersion {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if version == 0 {
		if _, err := tx.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("create schema v1: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
			return fmt.Errorf("set schema version: %w", err)
		}
		version = 1
	}
	if version == 1 {
		if _, err := tx.ExecContext(ctx, `
ALTER TABLE sync_roots
ADD COLUMN initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1))`); err != nil {
			return fmt.Errorf("migrate schema v1 to v2: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
			return fmt.Errorf("set schema version: %w", err)
		}
		version = 2
	}
	if version == 2 {
		if _, err := tx.ExecContext(ctx, `
ALTER TABLE sync_roots
ADD COLUMN symlink_mode TEXT NOT NULL DEFAULT 'follow'
CHECK (symlink_mode IN ('follow', 'reject', 'ignore'))`); err != nil {
			return fmt.Errorf("migrate sync roots v2 to v3: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
ALTER TABLE operations
ADD COLUMN local_target_path TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate operations v2 to v3: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 3"); err != nil {
			return fmt.Errorf("set schema version: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration: %w", err)
	}
	return nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

const schemaV1 = `
CREATE TABLE sync_roots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE,
    local_root TEXT NOT NULL UNIQUE,
    remote_name TEXT NOT NULL,
    remote_root TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    poll_interval_seconds INTEGER NOT NULL CHECK (poll_interval_seconds >= 0),
    created_at_ns INTEGER NOT NULL,
    UNIQUE(remote_name, remote_root)
);

CREATE TABLE entries (
    sync_root_id INTEGER NOT NULL,
    rel_path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('file', 'dir')),

    baseline_local_present INTEGER NOT NULL CHECK (baseline_local_present IN (0, 1)),
    baseline_local_size INTEGER NOT NULL,
    baseline_local_mtime_ns INTEGER NOT NULL,

    baseline_remote_present INTEGER NOT NULL CHECK (baseline_remote_present IN (0, 1)),
    baseline_remote_id TEXT NOT NULL,
    baseline_remote_rev TEXT NOT NULL,
    baseline_remote_size INTEGER NOT NULL,
    baseline_remote_mtime_us INTEGER NOT NULL,


    CHECK (baseline_local_present = 1 OR baseline_remote_present = 1),
    CHECK (baseline_local_present = 1 OR (baseline_local_size = 0 AND baseline_local_mtime_ns = 0)),
    CHECK (baseline_local_present = 0 OR kind = 'file' OR baseline_local_size = 0),
    CHECK (
        (baseline_remote_present = 0 AND baseline_remote_id = '' AND baseline_remote_rev = '' AND baseline_remote_size = 0 AND baseline_remote_mtime_us = 0)
        OR
        (baseline_remote_present = 1 AND baseline_remote_id <> '' AND
            ((kind = 'file' AND baseline_remote_rev <> '') OR (kind = 'dir' AND baseline_remote_rev = '' AND baseline_remote_size = 0)))
    ),

    PRIMARY KEY(sync_root_id, rel_path),
    FOREIGN KEY(sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
);
CREATE INDEX entries_remote_id_idx ON entries(sync_root_id, baseline_remote_id)
    WHERE baseline_remote_present = 1;

CREATE TABLE operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_root_id INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('ensure-remote', 'ensure-local', 'delete-remote', 'delete-local', 'move-remote', 'move-local')),
    entry_kind TEXT NOT NULL CHECK (entry_kind IN ('file', 'dir')),
    src_path TEXT NOT NULL,
    dst_path TEXT NOT NULL,

    expected_local_present INTEGER NOT NULL CHECK (expected_local_present IN (0, 1)),
    expected_local_kind TEXT NOT NULL,
    expected_local_size INTEGER NOT NULL,
    expected_local_mtime_ns INTEGER NOT NULL,

    expected_remote_absent INTEGER NOT NULL CHECK (expected_remote_absent IN (0, 1)),
    expected_remote_id TEXT NOT NULL,
    expected_remote_rev TEXT NOT NULL,


    phase TEXT NOT NULL CHECK (phase IN ('planned', 'running', 'recovering', 'blocked')),
    attempts INTEGER NOT NULL CHECK (attempts >= 0),
    last_error TEXT NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,

    CHECK (
        (expected_remote_absent = 1 AND expected_remote_id = '' AND expected_remote_rev = '')
        OR
        (expected_remote_absent = 0 AND expected_remote_id <> '' AND
            ((entry_kind = 'file' AND expected_remote_rev <> '') OR (entry_kind = 'dir' AND expected_remote_rev = '')))
    ),

    FOREIGN KEY(sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
);
CREATE INDEX operations_root_phase_idx ON operations(sync_root_id, phase, id);

CREATE TABLE conflicts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sync_root_id INTEGER NOT NULL,
    rel_path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('both-modified', 'local-delete-remote-edit', 'remote-delete-local-edit', 'simultaneous-create', 'kind-mismatch')),

    local_present INTEGER NOT NULL CHECK (local_present IN (0, 1)),
    local_kind TEXT NOT NULL,
    local_size INTEGER NOT NULL,
    local_mtime_ns INTEGER NOT NULL,

    remote_present INTEGER NOT NULL CHECK (remote_present IN (0, 1)),
    remote_kind TEXT NOT NULL,
    remote_id TEXT NOT NULL,
    remote_rev TEXT NOT NULL,
    remote_size INTEGER NOT NULL,
    remote_mtime_us INTEGER NOT NULL,

    resolved INTEGER NOT NULL CHECK (resolved IN (0, 1)),
    created_at_ns INTEGER NOT NULL,
    resolved_at_ns INTEGER NOT NULL,

    FOREIGN KEY(sync_root_id) REFERENCES sync_roots(id) ON DELETE CASCADE
);
CREATE INDEX conflicts_root_resolved_idx ON conflicts(sync_root_id, resolved, id);
`
