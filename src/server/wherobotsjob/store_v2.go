package wherobotsjob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/conn"
	"dekart/src/server/job"
	"dekart/src/server/secrets"
	"dekart/src/server/user"
	"dekart/src/server/wherobotsdb"
	"fmt"

	"github.com/rs/zerolog/log"
)

// StoreV2 implements the new job.Store interface for Wherobots.
type StoreV2 struct {
	*job.BaseStore
}

// NewStoreV2 creates a new Wherobots store using the refactored architecture.
func NewStoreV2() *StoreV2 {
	store := &StoreV2{}

	// We can't use BaseStore directly because the executor needs
	// connection and apiKey from the request context
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		// This will be overridden in CreateJob
		return nil
	})

	return store
}

// CreateJob overrides BaseStore to extract connection and apiKey from context.
func (s *StoreV2) CreateJob(ctx context.Context, req job.JobRequest) (*job.ManagedJob, <-chan int32, error) {
	// Extract connection from context
	connection := conn.FromCtx(ctx)
	if connection.WherobotsKey == nil {
		err := fmt.Errorf("wherobotsKey must be provided")
		log.Error().Err(err).Msg("Invalid wherobots connection info")
		return nil, nil, err
	}

	// Decrypt API key
	apiKey := secrets.SecretToString(connection.WherobotsKey, user.GetClaims(ctx))

	// Create executor with connection and apiKey
	executor := NewExecutor(connection, apiKey)

	// Build job
	j, err := job.NewJobBuilder().
		WithReportID(req.ReportID).
		WithQueryID(req.QueryID).
		WithQueryText(req.QueryText).
		WithExecutor(executor).
		Build(ctx)

	if err != nil {
		return nil, nil, err
	}

	// Register job for lifecycle management
	s.BaseStore.Registry().Register(j)

	return j, j.Status(), nil
}

// TestConnection verifies that Wherobots DB credentials are valid.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	claims := user.GetClaims(ctx)
	if claims == nil {
		return nil, fmt.Errorf("unauthenticated: claims are required")
	}

	conn := req.Connection
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Check secrets
	if conn.WherobotsKey == nil {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   "WherobotsKey is required",
		}, nil
	}

	apiKey := secrets.SecretToString(conn.WherobotsKey, claims)
	_, err := wherobotsdb.GetSession(
		ctx,
		conn.WherobotsHost,
		"",
		apiKey,
		wherobotsdb.Runtime(conn.WherobotsRuntime),
		wherobotsdb.Region(conn.WherobotsRegion),
		0, // waitTimeout
		0,
	)

	if err != nil {
		log.Warn().Err(err).Msg("Failed to connect to Wherobots when testing connection")
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.TestConnectionResponse{Success: true}, nil
}
