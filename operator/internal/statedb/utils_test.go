// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaw2Params_Array(t *testing.T) {
	raw := json.RawMessage(`[1, "hello", true, null]`)
	params, err := raw2params(raw)
	require.NoError(t, err)
	require.Len(t, params, 4)
	assert.Equal(t, float64(1), params[0]) // JSON numbers unmarshal as float64
	assert.Equal(t, "hello", params[1])
	assert.Equal(t, true, params[2])
	assert.Nil(t, params[3])
}

func TestRaw2Params_Object(t *testing.T) {
	raw := json.RawMessage(`{"name": "alice", "age": 30}`)
	params, err := raw2params(raw)
	require.NoError(t, err)
	require.Len(t, params, 2)

	// Convert to a map for order-independent comparison.
	named := make(map[string]any)
	for _, p := range params {
		np, ok := p.(sql.NamedArg)
		require.True(t, ok, "expected sql.NamedArg, got %T", p)
		named[np.Name] = np.Value
	}
	assert.Equal(t, "alice", named["name"])
	assert.Equal(t, float64(30), named["age"])
}

func TestRaw2Params_Null(t *testing.T) {
	raw := json.RawMessage(`null`)
	params, err := raw2params(raw)
	require.NoError(t, err)
	assert.Nil(t, params)
}

func TestRaw2Params_Empty(t *testing.T) {
	params, err := raw2params(nil)
	require.NoError(t, err)
	assert.Nil(t, params)

	params, err = raw2params(json.RawMessage{})
	require.NoError(t, err)
	assert.Nil(t, params)
}

func TestCkSQL_RejectsTransactionControl(t *testing.T) {
	rejected := []string{
		"BEGIN",
		"begin",
		"BEGIN TRANSACTION",
		"COMMIT",
		"commit",
		"ROLLBACK",
		"rollback",
		"END",
		"end",
		"  BEGIN",   // leading whitespace
		"\tCOMMIT",  // leading tab
	}
	for _, stmt := range rejected {
		t.Run(stmt, func(t *testing.T) {
			err := ckSQL(stmt)
			assert.Error(t, err, "expected ckSQL to reject %q", stmt)
		})
	}
}

func TestCkSQL_AllowsNormal(t *testing.T) {
	allowed := []string{
		"SELECT * FROM foo",
		"INSERT INTO bar (x) VALUES (1)",
		"UPDATE baz SET x = 1",
		"DELETE FROM qux",
		"CREATE TABLE t (id INTEGER PRIMARY KEY)",
		"DROP TABLE IF EXISTS t",
		"PRAGMA journal_mode",
	}
	for _, stmt := range allowed {
		t.Run(stmt, func(t *testing.T) {
			err := ckSQL(stmt)
			assert.NoError(t, err, "expected ckSQL to allow %q", stmt)
		})
	}
}
