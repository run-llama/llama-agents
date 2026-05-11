// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/gofiber/fiber/v2"
)

var sqliteMagic = []byte("SQLite format 3\x00")

func (s *Server) handleBackup(c *fiber.Ctx) error {
	tmp, err := os.CreateTemp("", "statedb-backup-*.sqlite")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpPath)

	s.mu.Lock()
	_, err = s.db.ExecContext(c.UserContext(), fmt.Sprintf("VACUUM INTO '%s'", tmpPath))
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("vacuum into backup: %w", err)
	}

	// Read the entire backup into memory so that deferred cleanup doesn't
	// race with Fiber's asynchronous response writing.
	data, err := os.ReadFile(tmpPath)
	if err != nil {
		return fmt.Errorf("read backup file: %w", err)
	}

	if c.Query("compress") == "true" {
		c.Set("Content-Type", "application/octet-stream")
		c.Set("Content-Encoding", "gzip")
		c.Set("Content-Disposition", "attachment; filename=statedb.sqlite.gz")

		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(data); err != nil {
			return fmt.Errorf("gzip compress: %w", err)
		}
		if err := gz.Close(); err != nil {
			return fmt.Errorf("gzip close: %w", err)
		}
		return c.Send(buf.Bytes())
	}

	c.Set("Content-Type", "application/octet-stream")
	c.Set("Content-Disposition", "attachment; filename=statedb.sqlite")
	return c.Send(data)
}

func (s *Server) handleRestore(c *fiber.Ctx) error {
	body := c.Body()
	if len(body) < 16 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "request body too small"})
	}

	// Validate SQLite magic bytes.
	for i := 0; i < 16; i++ {
		if body[i] != sqliteMagic[i] {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "not a valid SQLite database"})
		}
	}

	// Write to a temp file first.
	tmp, err := os.CreateTemp("", "statedb-restore-*.sqlite")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp file: %w", err)
	}
	tmp.Close()

	s.healthy = false
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		s.healthy = true
	}()

	if err := s.db.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close database: %w", err)
	}

	// Replace the database file.
	if err := os.Rename(tmpPath, s.dbPath); err != nil {
		// Rename failed; try to reopen the old DB.
		os.Remove(tmpPath)
		if reopenErr := s.reopenDB(); reopenErr != nil {
			slog.Error("failed to reopen database after rename failure", "error", reopenErr)
		}
		return fmt.Errorf("replace database file: %w", err)
	}

	if err := s.reopenDB(); err != nil {
		return fmt.Errorf("reopen database: %w", err)
	}

	if err := runMigrations(context.Background(), s.db); err != nil {
		return fmt.Errorf("run migrations after restore: %w", err)
	}

	slog.Info("database restored successfully")
	return c.JSON(fiber.Map{"status": "restored"})
}

func (s *Server) reopenDB() error {
	var err error
	s.db, err = sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)", s.dbPath))
	if err != nil {
		return fmt.Errorf("reopen database: %w", err)
	}
	s.db.SetMaxOpenConns(1)
	return nil
}
