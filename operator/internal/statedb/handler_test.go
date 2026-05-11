// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupTestServer(t *testing.T) *Server {
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

func doRequest(t *testing.T, srv *Server, method, path string, body any) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	return resp
}

func parseResponse(t *testing.T, resp *http.Response) Response {
	t.Helper()
	var r Response
	err := json.NewDecoder(resp.Body).Decode(&r)
	require.NoError(t, err)
	return r
}

func TestTransaction_InsertAndQuery(t *testing.T) {
	srv := setupTestServer(t)

	// Create a test table.
	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"},
			{Statement: "INSERT INTO test (id, name) VALUES (1, 'alice')"},
			{Statement: "INSERT INTO test (id, name) VALUES (2, 'bob')"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Query back.
	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Query: "SELECT id, name FROM test ORDER BY id"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 1)
	assert.True(t, r.Results[0].Success)
	assert.Equal(t, []string{"id", "name"}, r.Results[0].ResultHeaders)
	require.Len(t, r.Results[0].ResultSet, 2)
	assert.Equal(t, "alice", r.Results[0].ResultSet[0]["name"])
	assert.Equal(t, "bob", r.Results[0].ResultSet[1]["name"])
}

func TestTransaction_NamedParams(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"},
			{
				Statement: "INSERT INTO test (id, name) VALUES (:id, :name)",
				Values:    json.RawMessage(`{"id": 1, "name": "charlie"}`),
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{
				Query:  "SELECT name FROM test WHERE id = :id",
				Values: json.RawMessage(`{"id": 1}`),
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 1)
	assert.True(t, r.Results[0].Success)
	require.Len(t, r.Results[0].ResultSet, 1)
	assert.Equal(t, "charlie", r.Results[0].ResultSet[0]["name"])
}

func TestTransaction_PositionalParams(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"},
			{
				Statement: "INSERT INTO test (id, name) VALUES (?, ?)",
				Values:    json.RawMessage(`[1, "dave"]`),
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{
				Query:  "SELECT name FROM test WHERE id = ?",
				Values: json.RawMessage(`[1]`),
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 1)
	assert.True(t, r.Results[0].Success)
	require.Len(t, r.Results[0].ResultSet, 1)
	assert.Equal(t, "dave", r.Results[0].ResultSet[0]["name"])
}

func TestTransaction_ValuesBatch(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"},
			{
				Statement: "INSERT INTO test (id, name) VALUES (?, ?)",
				ValuesBatch: []json.RawMessage{
					json.RawMessage(`[1, "eve"]`),
					json.RawMessage(`[2, "frank"]`),
					json.RawMessage(`[3, "grace"]`),
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 2)
	assert.True(t, r.Results[1].Success)
	assert.Equal(t, []int64{1, 1, 1}, r.Results[1].RowsUpdatedBatch)

	// Verify all rows inserted.
	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Query: "SELECT COUNT(*) as cnt FROM test"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r = parseResponse(t, resp)
	assert.Equal(t, float64(3), r.Results[0].ResultSet[0]["cnt"])
}

func TestTransaction_NoFail(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY)"},
			{
				Statement: "INSERT INTO test (id) VALUES (1)",
			},
			{
				// This will fail (duplicate key) but noFail=true means we continue.
				Statement: "INSERT INTO test (id) VALUES (1)",
				NoFail:    true,
			},
			{
				Statement: "INSERT INTO test (id) VALUES (2)",
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 4)
	assert.True(t, r.Results[0].Success)
	assert.True(t, r.Results[1].Success)
	assert.False(t, r.Results[2].Success) // duplicate key
	assert.NotEmpty(t, r.Results[2].Error)
	assert.True(t, r.Results[3].Success) // continues despite prior failure
}

func TestTransaction_NoFailFalse_Rollback(t *testing.T) {
	srv := setupTestServer(t)

	// Create the table first.
	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY)"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// This transaction should fail and roll back entirely because noFail=false (default).
	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "INSERT INTO test (id) VALUES (1)"},
			{Statement: "INSERT INTO test (id) VALUES (1)"}, // duplicate key, noFail=false
		},
	})
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)

	// The first insert should have been rolled back.
	resp = doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Query: "SELECT COUNT(*) as cnt FROM test"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	assert.Equal(t, float64(0), r.Results[0].ResultSet[0]["cnt"])
}

func TestTransaction_RejectTransactionControl(t *testing.T) {
	srv := setupTestServer(t)

	stmts := []string{"BEGIN", "COMMIT", "ROLLBACK", "END"}
	for _, stmt := range stmts {
		t.Run(stmt, func(t *testing.T) {
			resp := doRequest(t, srv, http.MethodPost, "/db", Request{
				Transaction: []RequestItem{
					{Statement: stmt},
				},
			})
			assert.NotEqual(t, http.StatusOK, resp.StatusCode)
		})
	}
}

func TestTransaction_MixedReadsWrites(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{
			{Statement: "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)"},
			{Statement: "INSERT INTO test (id, val) VALUES (1, 'x')"},
			{Query: "SELECT val FROM test WHERE id = 1"},
			{Statement: "UPDATE test SET val = 'y' WHERE id = 1"},
			{Query: "SELECT val FROM test WHERE id = 1"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := parseResponse(t, resp)
	require.Len(t, r.Results, 5)

	// First query sees 'x'.
	assert.True(t, r.Results[2].Success)
	require.Len(t, r.Results[2].ResultSet, 1)
	assert.Equal(t, "x", r.Results[2].ResultSet[0]["val"])

	// Second query sees 'y' after the update.
	assert.True(t, r.Results[4].Success)
	require.Len(t, r.Results[4].ResultSet, 1)
	assert.Equal(t, "y", r.Results[4].ResultSet[0]["val"])
}

func TestTransaction_EmptyTransaction(t *testing.T) {
	srv := setupTestServer(t)

	resp := doRequest(t, srv, http.MethodPost, "/db", Request{
		Transaction: []RequestItem{},
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestTransaction_InvalidBody(t *testing.T) {
	srv := setupTestServer(t)

	req := httptest.NewRequest(http.MethodPost, "/db", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-token")
	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
