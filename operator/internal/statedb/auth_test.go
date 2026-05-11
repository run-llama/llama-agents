// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func setupAuthServer(t *testing.T, token string) *Server {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	cfg := Config{
		DBPath: dbPath,
		Port:   0,
		Token:  token,
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
	if token != "" {
		srv.app.Use(authMiddleware(token))
	}
	srv.app.Get("/healthz", srv.handleHealthz)
	srv.app.Post("/db", srv.handleTransaction)
	srv.healthy = true
	return srv
}

func TestAuth_ValidToken(t *testing.T) {
	srv := setupAuthServer(t, "secret-token")
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Authorization", "Bearer secret-token")

	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_InvalidToken(t *testing.T) {
	srv := setupAuthServer(t, "secret-token")
	req := httptest.NewRequest(http.MethodPost, "/db", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")

	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_MissingToken(t *testing.T) {
	srv := setupAuthServer(t, "secret-token")
	req := httptest.NewRequest(http.MethodPost, "/db", nil)
	// No Authorization header.

	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAuth_HealthzExempt(t *testing.T) {
	srv := setupAuthServer(t, "secret-token")
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	// No Authorization header, but /healthz should still work.

	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAuth_NoTokenConfigured(t *testing.T) {
	srv := setupAuthServer(t, "")
	// When no token is configured, no middleware is applied, so all requests should pass.
	req := httptest.NewRequest(http.MethodPost, "/db", nil)

	resp, err := srv.app.Test(req, -1)
	require.NoError(t, err)
	// Should not be 401 -- it will fail with 400 (bad body) or similar, but not auth failure.
	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
}
