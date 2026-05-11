// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupBackupServer(t *testing.T) *Server {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	cfg := Config{
		DBPath: dbPath,
		Port:   0,
		Token:  "test-token",
		NoS3:   true,
	}
	srv := New(cfg)

	var err error
	srv.db, err = sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", dbPath))
	require.NoError(t, err)
	srv.db.SetMaxOpenConns(1)
	t.Cleanup(func() { srv.db.Close() })

	err = runMigrations(context.Background(), srv.db)
	require.NoError(t, err)

	srv.app = fiber.New()
	srv.app.Use(authMiddleware(cfg.Token))
	srv.app.Get("/healthz", srv.handleHealthz)
	srv.app.Post("/db", srv.handleTransaction)
	srv.app.Get("/db/backup", srv.handleBackup)
	srv.app.Post("/db/restore", srv.handleRestore)
	srv.healthy = true
	return srv
}

func doBackupRequest(t *testing.T, srv *Server, method, path string, body io.Reader, contentType string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, path, body)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	return resp
}

func TestBackup_ReturnsValidDB(t *testing.T) {
	srv := setupBackupServer(t)

	// Insert some data.
	txReq := Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)"},
			{Statement: "INSERT INTO test (id, val) VALUES (1, 'hello')"},
		},
	}
	b, _ := json.Marshal(txReq)
	resp := doBackupRequest(t, srv, http.MethodPost, "/db", bytes.NewReader(b), "application/json")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Get backup.
	resp = doBackupRequest(t, srv, http.MethodGet, "/db/backup", nil, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.True(t, len(body) >= 16, "backup too small: %d bytes", len(body))

	// Check SQLite magic bytes.
	assert.Equal(t, sqliteMagic, body[:16])

	// Write to temp file and verify it's openable.
	tmpPath := filepath.Join(t.TempDir(), "backup.db")
	err = os.WriteFile(tmpPath, body, 0644)
	require.NoError(t, err)

	backupDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=ro", tmpPath))
	require.NoError(t, err)
	defer backupDB.Close()

	var val string
	err = backupDB.QueryRow("SELECT val FROM test WHERE id = 1").Scan(&val)
	require.NoError(t, err)
	assert.Equal(t, "hello", val)
}

func TestBackup_Compressed(t *testing.T) {
	srv := setupBackupServer(t)

	resp := doBackupRequest(t, srv, http.MethodGet, "/db/backup?compress=true", nil, "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Decompress and check for SQLite magic.
	gz, err := gzip.NewReader(bytes.NewReader(body))
	require.NoError(t, err)
	defer gz.Close()

	decompressed, err := io.ReadAll(gz)
	require.NoError(t, err)
	require.True(t, len(decompressed) >= 16, "decompressed backup too small: %d bytes", len(decompressed))
	assert.Equal(t, sqliteMagic, decompressed[:16])
}

func TestRestore_ReplacesDatabase(t *testing.T) {
	srv := setupBackupServer(t)

	// Insert data into the original DB.
	txReq := Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)"},
			{Statement: "INSERT INTO test (id, val) VALUES (1, 'original')"},
		},
	}
	b, _ := json.Marshal(txReq)
	resp := doBackupRequest(t, srv, http.MethodPost, "/db", bytes.NewReader(b), "application/json")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create a different SQLite database to restore from.
	altPath := filepath.Join(t.TempDir(), "alt.db")
	altDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", altPath))
	require.NoError(t, err)
	altDB.SetMaxOpenConns(1)
	_, err = altDB.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
	require.NoError(t, err)
	_, err = altDB.Exec("INSERT INTO test (id, val) VALUES (1, 'restored')")
	require.NoError(t, err)
	altDB.Close()

	altBytes, err := os.ReadFile(altPath)
	require.NoError(t, err)

	// Restore.
	resp = doBackupRequest(t, srv, http.MethodPost, "/db/restore", bytes.NewReader(altBytes), "application/octet-stream")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Query the restored data.
	txReq = Request{
		Transaction: []RequestItem{
			{Query: "SELECT val FROM test WHERE id = 1"},
		},
	}
	b, _ = json.Marshal(txReq)
	resp = doBackupRequest(t, srv, http.MethodPost, "/db", bytes.NewReader(b), "application/json")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var r Response
	err = json.NewDecoder(resp.Body).Decode(&r)
	require.NoError(t, err)
	require.Len(t, r.Results, 1)
	assert.True(t, r.Results[0].Success)
	require.Len(t, r.Results[0].ResultSet, 1)
	assert.Equal(t, "restored", r.Results[0].ResultSet[0]["val"])
}

func TestRestore_InvalidFile(t *testing.T) {
	srv := setupBackupServer(t)

	resp := doBackupRequest(t, srv, http.MethodPost, "/db/restore", bytes.NewReader([]byte("this is not a sqlite database at all")), "application/octet-stream")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRestore_TooSmall(t *testing.T) {
	srv := setupBackupServer(t)

	resp := doBackupRequest(t, srv, http.MethodPost, "/db/restore", bytes.NewReader([]byte("tiny")), "application/octet-stream")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRestore_RunsMigrations(t *testing.T) {
	srv := setupBackupServer(t)

	// Create a bare SQLite database with no tables.
	barePath := filepath.Join(t.TempDir(), "bare.db")
	bareDB, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", barePath))
	require.NoError(t, err)
	bareDB.SetMaxOpenConns(1)
	_, err = bareDB.Exec("CREATE TABLE dummy (id INTEGER)")
	require.NoError(t, err)
	bareDB.Close()

	bareBytes, err := os.ReadFile(barePath)
	require.NoError(t, err)

	// Restore the bare DB.
	resp := doBackupRequest(t, srv, http.MethodPost, "/db/restore", bytes.NewReader(bareBytes), "application/octet-stream")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify that migrations ran by checking for the workflow_handlers table.
	txReq := Request{
		Transaction: []RequestItem{
			{Query: "SELECT name FROM sqlite_master WHERE type='table' AND name='workflow_handlers'"},
		},
	}
	b, _ := json.Marshal(txReq)
	resp = doBackupRequest(t, srv, http.MethodPost, "/db", bytes.NewReader(b), "application/json")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var r Response
	err = json.NewDecoder(resp.Body).Decode(&r)
	require.NoError(t, err)
	require.Len(t, r.Results, 1)
	assert.True(t, r.Results[0].Success)
	require.Len(t, r.Results[0].ResultSet, 1)
	assert.Equal(t, "workflow_handlers", r.Results[0].ResultSet[0]["name"])
}
