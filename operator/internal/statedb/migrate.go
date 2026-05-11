// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// runMigrations applies any unapplied migrations to the database.
func runMigrations(ctx context.Context, db *sql.DB) error {
	// Bootstrap the migrations tracking table.
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS _migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`)
	if err != nil {
		return fmt.Errorf("create _migrations table: %w", err)
	}

	// Collect already-applied versions.
	rows, err := db.QueryContext(ctx, "SELECT version FROM _migrations")
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return fmt.Errorf("scan migration version: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate migration rows: %w", err)
	}

	// Read migration files from the embedded FS.
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	type migration struct {
		version  int
		filename string
	}
	var pending []migration

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		version, err := parseVersion(name)
		if err != nil {
			slog.Warn("skipping non-migration file", "file", name, "error", err)
			continue
		}
		if !applied[version] {
			pending = append(pending, migration{version: version, filename: name})
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].version < pending[j].version
	})

	for _, m := range pending {
		content, err := migrationFS.ReadFile(filepath.Join("migrations", m.filename))
		if err != nil {
			return fmt.Errorf("read migration %s: %w", m.filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx for migration %d: %w", m.version, err)
		}

		for _, stmt := range splitStatements(string(content)) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("migration %d: %w", m.version, err)
			}
		}

		if _, err := tx.ExecContext(ctx, "INSERT INTO _migrations (version) VALUES (?)", m.version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %d: %w", m.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.version, err)
		}

		slog.Info("applied migration", "version", m.version, "file", m.filename)
	}

	return nil
}

// parseVersion extracts the version number from a migration filename like "001_initial.sql".
func parseVersion(filename string) (int, error) {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	parts := strings.SplitN(base, "_", 2)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid migration filename: %s", filename)
	}
	return strconv.Atoi(parts[0])
}

// splitStatements splits SQL content on ";\n" boundaries and returns non-empty trimmed statements.
func splitStatements(content string) []string {
	raw := strings.Split(content, ";\n")
	var stmts []string
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}
