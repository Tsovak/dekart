package wherobotsjob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/conn"
	"dekart/src/server/job"
	"dekart/src/server/secrets"
	"dekart/src/server/storage"
	"dekart/src/server/user"
	"dekart/src/server/wherobotsdb"
)

// Executor implements job.Executor for Wherobots.
type Executor struct {
	connection *proto.Connection
	apiKey     string
}

// NewExecutor creates a new Wherobots executor.
func NewExecutor(connection *proto.Connection, apiKey string) *Executor {
	return &Executor{
		connection: connection,
		apiKey:     apiKey,
	}
}

// Execute runs a Wherobots query and optionally copies results to storage.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	// Create Wherobots connection
	wConn, err := wherobotsdb.Connect(
		ctx,
		e.connection.WherobotsHost,
		"",
		e.apiKey,
		wherobotsdb.Runtime(e.connection.WherobotsRuntime),
		wherobotsdb.Region(e.connection.WherobotsRegion),
		0,
		0,
	)
	if err != nil {
		req.Logger.Warn().Err(err).Msg("Failed to create wherobots connection")
		return nil, err
	}
	defer wConn.Close()

	// Execute query
	cursor := wConn.Cursor()
	defer cursor.Close()

	if err := cursor.Execute(req.QueryText); err != nil {
		return nil, err
	}

	// Get result URI (Wherobots stores results externally)
	resultURI, err := cursor.GetResultURI()
	if err != nil {
		return nil, err
	}

	// Get result size
	size, err := cursor.GetResultSize()
	if err != nil {
		return nil, err
	}

	// Check if we need to copy to Google Cloud Storage
	_, isGoogleCloudStorage := req.Storage.(storage.GoogleCloudStorageObject)
	if isGoogleCloudStorage {
		// Copy result from presigned S3 URL to storage
		sourceObj := storage.NewPresignedS3Object(resultURI)
		err := sourceObj.CopyTo(ctx, req.Storage.GetWriter(ctx))
		if err != nil {
			return nil, err
		}
	}

	return &job.ExecutionResult{
		TotalRows:      0, // Wherobots doesn't provide this easily
		ProcessedBytes: 0, // Not available
		ResultSize:     size,
		ResultURI:      &resultURI,
	}, nil
}
