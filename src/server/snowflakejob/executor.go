package snowflakejob

import (
	"context"
	"database/sql"
	"dekart/src/server/job"
	"dekart/src/server/snowflakeutils"
	"dekart/src/server/storage"
	"encoding/csv"
	"sync"

	sf "github.com/snowflakedb/gosnowflake"
)

// Executor implements job.Executor for Snowflake.
type Executor struct {
	db        *sql.DB
	connector sf.Connector
}

// NewExecutor creates a new Snowflake executor.
func NewExecutor(db *sql.DB, connector sf.Connector) *Executor {
	return &Executor{
		db:        db,
		connector: connector,
	}
}

// Execute runs a Snowflake query with async metadata fetching.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	// Check if using Snowflake storage (results stay in Snowflake temp table)
	_, isSnowflakeStorage := req.Storage.(storage.SnowflakeStorageObject)

	// Setup channels for async metadata fetching
	queryIDChan := make(chan string, 1)
	resultsReady := make(chan bool, 1)
	metadataResult := make(chan *queryMetadata, 1)
	metadataWg := &sync.WaitGroup{}
	metadataWg.Add(1)

	// Start async metadata fetch
	go e.fetchQueryMetadata(ctx, queryIDChan, resultsReady, metadataResult, metadataWg, isSnowflakeStorage, req.Logger)

	// Execute query with query ID channel
	rows, err := e.db.QueryContext(
		sf.WithQueryIDChan(ctx, queryIDChan),
		req.QueryText,
	)
	if err != nil {
		req.Logger.Warn().Err(err).Msg("Error querying snowflake")
		return nil, err
	}
	defer rows.Close()

	var resultSize int64
	var queryID *string

	if isSnowflakeStorage {
		// Results stay in Snowflake temp table, no need to write to storage
		resultsReady <- true
	} else {
		// Write results to storage
		size, err := e.writeResults(ctx, rows, req.Storage, resultsReady, req.Logger)
		if err != nil {
			return nil, err
		}
		resultSize = size
	}

	// Wait for metadata fetch to complete
	metadataWg.Wait()

	// Get metadata result
	var metadata *queryMetadata
	select {
	case metadata = <-metadataResult:
	default:
		// Metadata fetch failed or timed out
		metadata = &queryMetadata{}
	}

	// If using Snowflake storage, return query ID
	if isSnowflakeStorage && metadata.queryID != "" {
		queryID = &metadata.queryID
	}

	return &job.ExecutionResult{
		TotalRows:      metadata.totalRows,
		ProcessedBytes: metadata.processedBytes,
		ResultSize:     resultSize,
		DWJobID:        queryID,
	}, nil
}

// queryMetadata holds metadata fetched asynchronously
type queryMetadata struct {
	queryID        string
	totalRows      int64
	processedBytes int64
}

// fetchQueryMetadata asynchronously fetches query metadata
func (e *Executor) fetchQueryMetadata(
	ctx context.Context,
	queryIDChan chan string,
	resultsReady chan bool,
	metadataResult chan *queryMetadata,
	wg *sync.WaitGroup,
	isSnowflakeStorage bool,
	logger interface{ Warn() interface{ Msg(string) }; Err(error) interface{ Send() } },
) {
	defer wg.Done()

	select {
	case queryID := <-queryIDChan:
		select {
		case <-ctx.Done():
			logger.Warn().Msg("Context Done before query status received")
			metadataResult <- &queryMetadata{}
			return
		case <-resultsReady:
			conn, err := e.connector.Connect(ctx)
			if err != nil {
				logger.Err(err).Send()
				metadataResult <- &queryMetadata{}
				return
			}
			status, err := conn.(sf.SnowflakeConnection).GetQueryStatus(ctx, queryID)
			if err != nil {
				logger.Err(err).Send()
				metadataResult <- &queryMetadata{}
				return
			}

			metadata := &queryMetadata{
				queryID:        queryID,
				totalRows:      status.ProducedRows,
				processedBytes: status.ScanBytes,
			}
			metadataResult <- metadata
			return
		}
	case <-ctx.Done():
		logger.Warn().Msg("Context Done before queryID received")
		metadataResult <- &queryMetadata{}
	}
}

// writeResults writes query results to storage as CSV
func (e *Executor) writeResults(
	ctx context.Context,
	rows *sql.Rows,
	storage interface {
		GetWriter(context.Context) interface {
			Write([]byte) (int, error)
			Close() error
		}
		GetSize(context.Context) (*int64, error)
	},
	resultsReady chan bool,
	logger interface{ Error() interface{ Err(error) interface{ Msg(string) } } },
) (int64, error) {
	writer := storage.GetWriter(ctx)
	csvWriter := csv.NewWriter(writer)

	firstRow := true
	for rows.Next() {
		// Check context
		select {
		case <-ctx.Done():
			csvWriter.Flush()
			writer.Close()
			return 0, ctx.Err()
		default:
		}

		if firstRow {
			firstRow = false
			resultsReady <- true

			// Write header row
			columnNames, err := snowflakeutils.GetColumns(rows)
			if err != nil {
				logger.Error().Err(err).Msg("Error getting column names")
				csvWriter.Flush()
				writer.Close()
				return 0, err
			}
			csvWriter.Write(columnNames)
		}

		// Write data row
		csvRow, err := snowflakeutils.GetRow(rows)
		if err != nil {
			logger.Error().Err(err).Msg("Error getting row")
			csvWriter.Flush()
			writer.Close()
			return 0, err
		}
		csvWriter.Write(csvRow)
	}

	if firstRow {
		// No rows - signal results ready
		resultsReady <- true
	}

	// Flush and close
	csvWriter.Flush()
	if err := writer.Close(); err != nil {
		return 0, err
	}

	// Get result size
	size, err := storage.GetSize(ctx)
	if err != nil {
		return 0, err
	}

	return *size, nil
}
