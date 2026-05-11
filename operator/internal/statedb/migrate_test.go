// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMigrations_CreatesAllTables(t *testing.T) {
	db := openTestDB(t)
	err := runMigrations(context.Background(), db)
	require.NoError(t, err)

	// Check all expected tables exist.
	tables := []string{
		"_migrations",
		"workflow_handlers",
		"workflow_events",
		"workflow_ticks",
		"workflow_state",
	}
	for _, table := range tables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		assert.NoError(t, err, "table %q should exist", table)
		assert.Equal(t, table, name)
	}
}

func TestMigrations_Idempotent(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	err := runMigrations(ctx, db)
	require.NoError(t, err)

	// Running a second time should not error.
	err = runMigrations(ctx, db)
	require.NoError(t, err)
}

func TestMigrations_VersionTracked(t *testing.T) {
	db := openTestDB(t)
	err := runMigrations(context.Background(), db)
	require.NoError(t, err)

	var version int
	err = db.QueryRow("SELECT version FROM _migrations WHERE version = 1").Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, 1, version)
}
