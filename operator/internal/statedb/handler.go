// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/gofiber/fiber/v2"
)

// txError is used with panic/recover to abort a transaction when a non-noFail
// statement fails. This matches ws4sqlite's proven approach for transaction control flow.
type txError struct {
	code    int
	message string
	reqIdx  int
}

func (e txError) Error() string {
	return fmt.Sprintf("request %d: %s", e.reqIdx, e.message)
}

// authMiddleware returns Fiber middleware that validates Bearer token auth.
// The /healthz endpoint is exempt from auth.
func authMiddleware(token string) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if c.Path() == "/healthz" {
			return c.Next()
		}
		auth := c.Get("Authorization")
		if auth != "Bearer "+token {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		return c.Next()
	}
}

// ckSQL rejects statements that try to control transaction boundaries.
func ckSQL(sql string) error {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	for _, prefix := range []string{"BEGIN", "COMMIT", "ROLLBACK", "END"} {
		if strings.HasPrefix(upper, prefix) {
			return fmt.Errorf("transaction control statements are not allowed: %s", prefix)
		}
	}
	return nil
}

// handleTransaction processes a batch of SQL statements/queries within a single transaction.
func (s *Server) handleTransaction(c *fiber.Ctx) error {
	var req Request
	if err := c.BodyParser(&req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "invalid request body: " + err.Error(),
		})
	}

	if len(req.Transaction) == 0 {
		return c.Status(http.StatusBadRequest).JSON(fiber.Map{
			"error": "transaction must contain at least one item",
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	results, err := s.execTransaction(c.UserContext(), req.Transaction)
	if err != nil {
		if txErr, ok := err.(txError); ok {
			return c.Status(txErr.code).JSON(fiber.Map{
				"reqIdx": txErr.reqIdx,
				"error":  txErr.message,
			})
		}
		return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(Response{Results: results})
}

func (s *Server) execTransaction(ctx context.Context, items []RequestItem) (results []ResponseItem, retErr error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if r := recover(); r != nil {
			_ = tx.Rollback()
			if txErr, ok := r.(txError); ok {
				retErr = txErr
				return
			}
			panic(r) // re-panic for unexpected errors
		}
	}()

	results = make([]ResponseItem, len(items))
	for i, item := range items {
		results[i] = s.execItem(ctx, tx, item, i)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}
	return results, nil
}

func (s *Server) execItem(ctx context.Context, tx *sql.Tx, item RequestItem, idx int) ResponseItem {
	hasQuery := item.Query != ""
	hasStatement := item.Statement != ""

	if hasQuery == hasStatement {
		return s.itemError(item, idx, http.StatusBadRequest, "exactly one of 'query' or 'statement' must be set")
	}

	if hasQuery {
		if err := ckSQL(item.Query); err != nil {
			return s.itemError(item, idx, http.StatusBadRequest, err.Error())
		}
		return s.execQuery(ctx, tx, item, idx)
	}

	if err := ckSQL(item.Statement); err != nil {
		return s.itemError(item, idx, http.StatusBadRequest, err.Error())
	}

	if len(item.ValuesBatch) > 0 {
		return s.execStatementBatch(ctx, tx, item, idx)
	}
	return s.execStatement(ctx, tx, item, idx)
}

func (s *Server) execQuery(ctx context.Context, tx *sql.Tx, item RequestItem, idx int) ResponseItem {
	params, err := raw2params(item.Values)
	if err != nil {
		return s.itemError(item, idx, http.StatusBadRequest, err.Error())
	}

	rows, err := tx.QueryContext(ctx, item.Query, params...)
	if err != nil {
		return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
	}

	var resultSet []map[string]any
	for rows.Next() {
		scanDest := make([]any, len(cols))
		scanPtrs := make([]any, len(cols))
		for i := range scanDest {
			scanPtrs[i] = &scanDest[i]
		}

		if err := rows.Scan(scanPtrs...); err != nil {
			return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
		}

		row := make(map[string]any, len(cols))
		for i, col := range cols {
			row[col] = scanDest[i]
		}
		resultSet = append(resultSet, row)
	}

	if err := rows.Err(); err != nil {
		return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
	}

	return ResponseItem{
		Success:       true,
		ResultHeaders: cols,
		ResultSet:     resultSet,
	}
}

func (s *Server) execStatement(ctx context.Context, tx *sql.Tx, item RequestItem, idx int) ResponseItem {
	params, err := raw2params(item.Values)
	if err != nil {
		return s.itemError(item, idx, http.StatusBadRequest, err.Error())
	}

	result, err := tx.ExecContext(ctx, item.Statement, params...)
	if err != nil {
		return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
	}

	affected, _ := result.RowsAffected()
	return ResponseItem{
		Success:     true,
		RowsUpdated: &affected,
	}
}

func (s *Server) execStatementBatch(ctx context.Context, tx *sql.Tx, item RequestItem, idx int) ResponseItem {
	batch := make([]int64, 0, len(item.ValuesBatch))

	for _, raw := range item.ValuesBatch {
		params, err := raw2params(raw)
		if err != nil {
			return s.itemError(item, idx, http.StatusBadRequest, err.Error())
		}

		result, err := tx.ExecContext(ctx, item.Statement, params...)
		if err != nil {
			return s.itemError(item, idx, http.StatusInternalServerError, err.Error())
		}

		affected, _ := result.RowsAffected()
		batch = append(batch, affected)
	}

	return ResponseItem{
		Success:          true,
		RowsUpdatedBatch: batch,
	}
}

// itemError records a failure. If noFail is false, it panics to abort the transaction.
func (s *Server) itemError(item RequestItem, idx int, code int, msg string) ResponseItem {
	if !item.NoFail {
		panic(txError{code: code, message: msg, reqIdx: idx})
	}
	return ResponseItem{
		Success: false,
		Error:   msg,
	}
}
