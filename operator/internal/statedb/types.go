// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package statedb

import "encoding/json"

// Request is the top-level JSON body for POST /db.
type Request struct {
	Transaction []RequestItem `json:"transaction"`
}

// RequestItem is a single statement or query in a transaction.
type RequestItem struct {
	// Exactly one of Query or Statement must be set.
	Query       string            `json:"query,omitempty"`
	Statement   string            `json:"statement,omitempty"`
	NoFail      bool              `json:"noFail,omitempty"`
	Values      json.RawMessage   `json:"values,omitempty"`
	ValuesBatch []json.RawMessage `json:"valuesBatch,omitempty"`
}

// Response is the top-level JSON response from POST /db.
type Response struct {
	Results []ResponseItem `json:"results"`
}

// ResponseItem is the result of a single statement or query.
type ResponseItem struct {
	Success          bool             `json:"success"`
	RowsUpdated      *int64           `json:"rowsUpdated,omitempty"`
	RowsUpdatedBatch []int64          `json:"rowsUpdatedBatch,omitempty"`
	ResultHeaders    []string         `json:"resultHeaders,omitempty"`
	ResultSet        []map[string]any `json:"resultSet,omitempty"`
	Error            string           `json:"error,omitempty"`
}
