// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	lss3 "github.com/benbjohnson/litestream/s3"
	"github.com/gofiber/fiber/v2"
)

// Config holds the server configuration parsed from CLI flags and env vars.
type Config struct {
	DBPath     string
	Port       int
	Token      string
	S3Bucket   string
	S3Path     string
	S3Region   string
	S3Endpoint string
	NoS3       bool
}

// Server is the top-level statedb server. It owns the SQLite database,
// Litestream replication, and the HTTP interface.
type Server struct {
	cfg     Config
	db      *sql.DB
	mu      sync.Mutex
	store   *litestream.Store
	app     *fiber.App
	dbPath  string
	healthy bool
}

// New creates a new Server with the given config.
func New(cfg Config) *Server {
	return &Server{
		cfg:    cfg,
		dbPath: cfg.DBPath,
	}
}

// Start initializes Litestream (if S3 configured), opens the database,
// runs migrations, and starts the HTTP server. It blocks until the server
// is shut down or encounters a fatal error.
func (s *Server) Start(ctx context.Context) error {
	// 1. Litestream init (restore from S3 if local DB missing).
	if !s.cfg.NoS3 {
		if err := s.initLitestream(ctx); err != nil {
			return fmt.Errorf("init litestream: %w", err)
		}
	}

	// 2. Open the app database connection.
	if err := s.reopenDB(); err != nil {
		return err
	}

	// 3. Run migrations.
	if err := runMigrations(ctx, s.db); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}

	// 4. Set up and start the Fiber HTTP server.
	s.app = fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           120 * time.Second,
	})

	if s.cfg.Token != "" {
		s.app.Use(authMiddleware(s.cfg.Token))
	}

	s.app.Get("/healthz", s.handleHealthz)
	s.app.Post("/db", s.handleTransaction)
	s.app.Get("/db/backup", s.handleBackup)
	s.app.Post("/db/restore", s.handleRestore)

	s.healthy = true

	addr := fmt.Sprintf(":%d", s.cfg.Port)
	slog.Info("starting statedb server", "addr", addr, "db", s.dbPath, "s3", !s.cfg.NoS3)
	return s.app.Listen(addr)
}

// Shutdown gracefully stops the server. It drains HTTP connections, closes
// the database, and performs a final Litestream WAL sync.
func (s *Server) Shutdown(ctx context.Context) error {
	slog.Info("shutting down statedb server")

	// 1. Drain HTTP connections.
	if s.app != nil {
		if err := s.app.ShutdownWithContext(ctx); err != nil {
			slog.Error("http shutdown", "error", err)
		}
	}

	// 2. Close the app database.
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			slog.Error("database close", "error", err)
		}
	}

	// 3. Final WAL sync to S3.
	if s.store != nil {
		slog.Info("performing final WAL sync to S3")
		if err := s.store.Close(ctx); err != nil {
			return fmt.Errorf("litestream store close: %w", err)
		}
	}

	slog.Info("statedb server stopped")
	return nil
}

func (s *Server) handleHealthz(c *fiber.Ctx) error {
	if !s.healthy {
		return c.SendStatus(fiber.StatusServiceUnavailable)
	}
	return c.SendStatus(fiber.StatusOK)
}

func (s *Server) initLitestream(ctx context.Context) error {
	slog.Info("initializing litestream", "bucket", s.cfg.S3Bucket, "path", s.cfg.S3Path)

	// S3 client.
	client := lss3.NewReplicaClient()
	client.Bucket = s.cfg.S3Bucket
	client.Path = s.cfg.S3Path
	client.Region = s.cfg.S3Region
	if s.cfg.S3Endpoint != "" {
		client.Endpoint = s.cfg.S3Endpoint
		client.ForcePathStyle = true // needed for MinIO/SeaweedFS
	}

	// Litestream DB + replica.
	lsdb := litestream.NewDB(s.dbPath)
	replica := litestream.NewReplicaWithClient(lsdb, client)
	replica.SyncInterval = 1 * time.Second
	lsdb.Replica = replica

	// Restore from S3 if local file is absent.
	if err := lsdb.EnsureExists(ctx); err != nil {
		return fmt.Errorf("ensure db exists: %w", err)
	}

	// Store manages background replication + compaction.
	s.store = litestream.NewStore([]*litestream.DB{lsdb}, litestream.DefaultCompactionLevels)
	s.store.ShutdownSyncTimeout = 30 * time.Second
	if err := s.store.Open(ctx); err != nil {
		return fmt.Errorf("open litestream store: %w", err)
	}

	slog.Info("litestream replication started")
	return nil
}
