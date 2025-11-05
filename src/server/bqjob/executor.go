package bqjob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/bqstorage"
	"dekart/src/server/bqutils"
	"dekart/src/server/job"
	"encoding/csv"
	"fmt"
	"regexp"

	"cloud.google.com/go/bigquery"
)

// Executor implements job.Executor for BigQuery.
type Executor struct {
	maxBytesBilled int64
}

// NewExecutor creates a new BigQuery executor with optional max bytes billed limit.
func NewExecutor(maxBytesBilled int64) *Executor {
	return &Executor{
		maxBytesBilled: maxBytesBilled,
	}
}

var orderByRe = regexp.MustCompile(`(?ims)order[\s]+by`)

// Execute runs a BigQuery query and writes results to storage.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	// Get BigQuery client
	client, err := bqutils.GetClient(ctx, req.Connection)
	if err != nil {
		req.Logger.Warn().Err(err).Msg("bigquery.NewClient failed")
		return nil, err
	}
	defer client.Close()

	// Configure query
	query := client.Query(req.QueryText)
	query.MaxBytesBilled = e.maxBytesBilled

	// Run query
	bqJob, err := query.Run(ctx)
	if err != nil {
		req.Logger.Warn().Err(err).Msg("query.Run failed")
		return nil, err
	}

	// Wait for query completion
	queryStatus, err := bqJob.Wait(ctx)
	if err != nil {
		req.Logger.Error().Err(err).Msg("query.Wait failed")
		return nil, err
	}

	if queryStatus == nil {
		return nil, fmt.Errorf("queryStatus is nil")
	}

	if err := queryStatus.Err(); err != nil {
		return nil, err
	}

	// Check if we're using BigQuery storage (results stay in BigQuery)
	_, isBigQueryStorage := req.Storage.(bqstorage.BigQueryStorageObject)
	if isBigQueryStorage {
		// Get table metadata
		table, err := e.getResultTable(ctx, client, bqJob)
		if err != nil {
			return nil, err
		}

		metadata, err := table.Metadata(ctx)
		if err != nil {
			return nil, err
		}

		bqJobID := bqJob.ID()
		return &job.ExecutionResult{
			TotalRows:      int64(metadata.NumRows),
			ProcessedBytes: queryStatus.Statistics.TotalBytesProcessed,
			ResultSize:     0, // Not applicable for BigQuery storage
			DWJobID:        &bqJobID,
		}, nil
	}

	// Get result table and metadata
	table, err := e.getResultTable(ctx, client, bqJob)
	if err != nil {
		return nil, err
	}

	metadata, err := table.Metadata(ctx)
	if err != nil {
		return nil, err
	}

	// Determine read streams based on ORDER BY
	maxReadStreams := int32(10)
	if orderByRe.MatchString(req.QueryText) {
		maxReadStreams = 1 // preserve order
	}

	// Read results and write to storage
	csvRows := make(chan []string, metadata.NumRows)
	errors := make(chan error, 1)

	// Read table rows into csvRows channel
	go bqutils.Read(ctx, errors, csvRows, table, req.Logger, maxReadStreams)

	// Write csvRows to storage
	go e.writeCSV(ctx, req.Storage, csvRows, errors)

	// Wait for completion
	if err := <-errors; err != nil {
		return nil, err
	}

	// Get result size
	resultSize, err := req.Storage.GetSize(ctx)
	if err != nil {
		return nil, err
	}

	var processedBytes int64
	if queryStatus.Statistics != nil {
		processedBytes = queryStatus.Statistics.TotalBytesProcessed
	}

	return &job.ExecutionResult{
		TotalRows:      int64(metadata.NumRows),
		ProcessedBytes: processedBytes,
		ResultSize:     *resultSize,
	}, nil
}

// getResultTable gets the result table from a BigQuery job.
func (e *Executor) getResultTable(ctx context.Context, client *bigquery.Client, bqJob *bigquery.Job) (*bigquery.Table, error) {
	table, err := bqutils.GetTableFromJob(bqJob)
	if err != nil {
		return nil, err
	}

	if table == nil {
		// Try getting job again by ID
		jobFromJobId, err := client.JobFromID(ctx, bqJob.ID())
		if err != nil {
			return nil, err
		}
		table, err = bqutils.GetTableFromJob(jobFromJobId)
		if err != nil {
			return nil, err
		}
	}

	if table == nil {
		return nil, fmt.Errorf("result table is nil")
	}

	return table, nil
}

// writeCSV writes CSV rows from channel to storage.
func (e *Executor) writeCSV(ctx context.Context, storage interface {
	GetWriter(context.Context) interface {
		Write([]byte) (int, error)
		Close() error
	}
	GetSize(context.Context) (*int64, error)
}, csvRows chan []string, errors chan error) {
	writer := storage.GetWriter(ctx)
	csvWriter := csv.NewWriter(writer)

	for csvRow := range csvRows {
		select {
		case <-ctx.Done():
			csvWriter.Flush()
			writer.Close()
			errors <- ctx.Err()
			return
		default:
		}

		if err := csvWriter.Write(csvRow); err != nil {
			csvWriter.Flush()
			writer.Close()
			errors <- err
			return
		}
	}

	csvWriter.Flush()
	if err := writer.Close(); err != nil {
		errors <- err
		return
	}

	errors <- nil
}
