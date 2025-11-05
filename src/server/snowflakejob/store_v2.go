package snowflakejob

import (
	"context"
	"database/sql"
	"dekart/src/proto"
	"dekart/src/server/conn"
	"dekart/src/server/job"
	"dekart/src/server/secrets"
	"dekart/src/server/snowflakeutils"
	"dekart/src/server/user"
	"fmt"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StoreV2 implements the new job.Store interface for Snowflake.
type StoreV2 struct {
	*job.BaseStore
}

// NewStoreV2 creates a new Snowflake store using the refactored architecture.
func NewStoreV2() *StoreV2 {
	store := &StoreV2{}

	// We can't use BaseStore directly because the executor needs
	// connection info from the request context
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		// This will be overridden in CreateJob
		return nil
	})

	return store
}

// CreateJob overrides BaseStore to create Snowflake connection from context.
func (s *StoreV2) CreateJob(ctx context.Context, req job.JobRequest) (*job.ManagedJob, <-chan int32, error) {
	// Get connection from context
	connection := conn.FromCtx(ctx)
	connector := snowflakeutils.GetConnector(connection)

	// Create database connection
	db := sql.OpenDB(connector)
	err := db.Ping()
	if err != nil {
		log.Error().Err(err).Msg("Failed to ping snowflake")
		return nil, nil, err
	}

	// Create executor
	executor := NewExecutor(db, connector)

	// Build job
	j, err := job.NewJobBuilder().
		WithReportID(req.ReportID).
		WithQueryID(req.QueryID).
		WithQueryText(req.QueryText).
		WithExecutor(executor).
		Build(ctx)

	if err != nil {
		db.Close()
		return nil, nil, err
	}

	// Register job for lifecycle management
	s.BaseStore.Registry().Register(j)

	return j, j.Status(), nil
}

// TestConnection verifies that Snowflake credentials are valid.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	claims := user.GetClaims(ctx)
	if claims == nil {
		return nil, status.Error(codes.Unauthenticated, "claims are required")
	}

	conn := req.Connection
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	if conn.SnowflakeKey == nil {
		return nil, status.Error(codes.InvalidArgument, "snowflake_key is required")
	}

	// Validate private key can be parsed
	privateKey := secrets.SecretToString(conn.SnowflakeKey, claims)
	_, err := snowflakeutils.ParsePrivateKey(privateKey)
	if err != nil {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Convert client secret to server secret
	conn.SnowflakeKey = secrets.ClientToServer(conn.SnowflakeKey, claims)

	// Test connection
	connector := snowflakeutils.GetConnector(conn)
	db := sql.OpenDB(connector)
	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("snowflake.Ping failed when testing connection")
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &proto.TestConnectionResponse{Success: true}, nil
}
