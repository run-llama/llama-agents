// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import (
	"database/sql"
	"encoding/json"
	"fmt"
)

// raw2params parses a json.RawMessage into either positional or named params.
//   - JSON array  → positional params ([]any)
//   - JSON object → named params (converted to sql.Named values)
//   - null/empty  → nil (no params)
func raw2params(raw json.RawMessage) ([]any, error) {
	if isEmptyRaw(raw) {
		return nil, nil
	}

	trimmed := trimLeft(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}

	switch trimmed[0] {
	case '[':
		var arr []any
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("parsing positional params: %w", err)
		}
		return arr, nil

	case '{':
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("parsing named params: %w", err)
		}
		return vals2named(m), nil

	default:
		return nil, fmt.Errorf("values must be a JSON array or object, got %q", trimmed[0])
	}
}

// vals2named converts a map[string]any to []any of sql.Named values,
// suitable for passing to db.Exec/db.Query with :name placeholders.
func vals2named(m map[string]any) []any {
	params := make([]any, 0, len(m))
	for k, v := range m {
		params = append(params, sql.Named(k, v))
	}
	return params
}

// isEmptyRaw returns true if the raw message is nil, zero-length, or the JSON literal "null".
func isEmptyRaw(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	trimmed := trimLeft(raw)
	return len(trimmed) >= 4 &&
		trimmed[0] == 'n' && trimmed[1] == 'u' && trimmed[2] == 'l' && trimmed[3] == 'l'
}

// trimLeft returns the slice with leading whitespace removed.
func trimLeft(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\n' || b[0] == '\r') {
		b = b[1:]
	}
	return b
}
