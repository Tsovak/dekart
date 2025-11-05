package pgjob

import (
	"context"
	"database/sql"
	"dekart/src/server/job"
	"encoding/csv"
	"fmt"
)

// Executor implements job.Executor for PostgreSQL.
type Executor struct {
	db *sql.DB
}

// NewExecutor creates a new PostgreSQL executor.
func NewExecutor(db *sql.DB) *Executor {
	return &Executor{db: db}
}

// Execute runs a PostgreSQL query and writes results to storage.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	// Execute query
	rows, err := e.db.QueryContext(ctx, req.QueryText)
	if err != nil {
		req.Logger.Error().Err(err).Str("queryText", req.QueryText).Msg("Error executing query")
		return nil, err
	}
	defer rows.Close()

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		req.Logger.Error().Err(err).Msg("Error getting column types")
		return nil, err
	}

	// Get storage writer
	writer := req.Storage.GetWriter(ctx)
	csvWriter := csv.NewWriter(writer)

	// Write header row
	headers := make([]string, len(columnTypes))
	for i, col := range columnTypes {
		headers[i] = col.Name()
	}
	if err := csvWriter.Write(headers); err != nil {
		writer.Close()
		return nil, err
	}

	// Prepare value holders
	values := make([]interface{}, len(columnTypes))
	for i := range values {
		values[i] = new(sql.NullString)
	}

	// Write data rows
	rowCount := int64(0)
	for rows.Next() {
		// Check context cancellation
		select {
		case <-ctx.Done():
			csvWriter.Flush()
			writer.Close()
			return nil, ctx.Err()
		default:
		}

		if err := rows.Scan(values...); err != nil {
			req.Logger.Warn().Err(err).Msg("Error scanning row, skipping")
			continue
		}

		csvRow := make([]string, len(columnTypes))
		for i, val := range values {
			if ns, ok := val.(*sql.NullString); ok {
				csvRow[i] = ns.String
			} else {
				return nil, fmt.Errorf("incorrect type of data: %T", val)
			}
		}

		if err := csvWriter.Write(csvRow); err != nil {
			csvWriter.Flush()
			writer.Close()
			return nil, err
		}

		rowCount++
	}

	// Check for errors from iterating over rows
	if err := rows.Err(); err != nil {
		csvWriter.Flush()
		writer.Close()
		return nil, err
	}

	// Flush and close writer
	csvWriter.Flush()
	if err := writer.Close(); err != nil {
		return nil, err
	}

	// Get result size
	resultSize, err := req.Storage.GetSize(ctx)
	if err != nil {
		return nil, err
	}

	return &job.ExecutionResult{
		TotalRows:      rowCount,
		ProcessedBytes: 0, // PostgreSQL doesn't provide this metric easily
		ResultSize:     *resultSize,
	}, nil
}
