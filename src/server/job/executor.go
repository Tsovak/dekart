package job

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/storage"

	"github.com/rs/zerolog"
)

// Executor executes queries against a specific database and returns results.
// This is the core interface that each database implementation must provide.
type Executor interface {
	// Execute runs the query and writes results to storage.
	// It's a pure function that focuses only on query execution logic.
	// Thread safety, state management, and lifecycle are handled by ManagedJob.
	Execute(ctx context.Context, req ExecutionRequest) (*ExecutionResult, error)
}

// ExecutionRequest contains all information needed to execute a query.
type ExecutionRequest struct {
	QueryText  string                   // The SQL query to execute
	Storage    storage.StorageObject    // Where to write results
	Connection *proto.Connection        // Database connection details
	Logger     zerolog.Logger           // Logger for this execution
}

// ExecutionResult contains the outcome of a successful query execution.
type ExecutionResult struct {
	TotalRows      int64   // Number of rows returned
	ProcessedBytes int64   // Bytes scanned/processed by the database
	ResultSize     int64   // Size of result data in bytes
	DWJobID        *string // Data warehouse job ID (e.g., BigQuery job ID)
	ResultURI      *string // URI where results are stored (e.g., S3 location)
}
