// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"llama-agents-operator/internal/statedb"

	_ "modernc.org/sqlite"
)

func main() {
	var cfg statedb.Config

	flag.StringVar(&cfg.DBPath, "path", envOr("DB_PATH", "/data/statedb.db"), "SQLite file path")
	flag.IntVar(&cfg.Port, "port", 8080, "Listen port")
	flag.StringVar(&cfg.Token, "token", os.Getenv("DB_AUTH_TOKEN"), "Auth token for Bearer authentication")
	flag.StringVar(&cfg.S3Bucket, "s3-bucket", os.Getenv("S3_BUCKET"), "S3 bucket for Litestream replication")
	flag.StringVar(&cfg.S3Path, "s3-path", os.Getenv("S3_PATH"), "S3 key prefix")
	flag.StringVar(&cfg.S3Region, "s3-region", os.Getenv("S3_REGION"), "AWS region")
	flag.StringVar(&cfg.S3Endpoint, "s3-endpoint", os.Getenv("S3_ENDPOINT"), "Custom S3 endpoint (MinIO/SeaweedFS)")
	flag.BoolVar(&cfg.NoS3, "no-s3", false, "Disable Litestream replication (local dev mode)")
	flag.Parse()

	srv := statedb.New(cfg)

	// Start server in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(context.Background())
	}()

	// Wait for interrupt or server error.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		slog.Info("received signal", "signal", sig)
	case err := <-errCh:
		if err != nil {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}

	// Graceful shutdown with 30s timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
