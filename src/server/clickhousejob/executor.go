package chjob

import (
	"context"
	"database/sql"
	"dekart/src/server/job"
	"fmt"
	"strings"
)

// Executor implements job.Executor for ClickHouse.
type Executor struct {
	db             *sql.DB
	outputLocation string
	s3Config       s3Config
	reportID       string
	queryID        string
}

// NewExecutor creates a new ClickHouse executor.
func NewExecutor(db *sql.DB, outputLocation string, s3Config s3Config, reportID, queryID string) *Executor {
	return &Executor{
		db:             db,
		outputLocation: outputLocation,
		s3Config:       s3Config,
		reportID:       reportID,
		queryID:        queryID,
	}
}

// Execute runs a ClickHouse query by exporting to S3 and then copying to storage.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	// Construct S3 path for export
	s3Path := fmt.Sprintf("%s/%s/%s/%s/result.csv",
		e.s3Config.Endpoint,
		strings.TrimPrefix(e.outputLocation, "s3://"),
		e.reportID,
		e.queryID)

	// Export to S3 using ClickHouse's s3() function
	// SETTINGS s3_truncate_on_insert=1 overwrites the file if it exists
	exportQuery := fmt.Sprintf(`
		INSERT INTO FUNCTION
			s3('%s', '%s', '%s', 'CSVWithNames')
		%s
		SETTINGS s3_truncate_on_insert=1
	`, s3Path, e.s3Config.AccessKey, e.s3Config.SecretKey, strings.TrimSuffix(req.QueryText, ";"))

	_, err := e.db.ExecContext(ctx, exportQuery)
	if err != nil {
		req.Logger.Error().Err(err).Msg("Error executing clickhouse query")
		return nil, err
	}

	// Copy results from S3 to storage
	resultLocation := fmt.Sprintf("%s/%s/%s/result.csv", e.outputLocation, e.reportID, e.queryID)
	err = req.Storage.CopyFromS3(ctx, resultLocation)
	if err != nil {
		return nil, err
	}

	// Get result size
	size, err := req.Storage.GetSize(ctx)
	if err != nil {
		return nil, err
	}

	return &job.ExecutionResult{
		TotalRows:      0, // ClickHouse doesn't provide this easily after export
		ProcessedBytes: 0, // Not available
		ResultSize:     *size,
	}, nil
}
