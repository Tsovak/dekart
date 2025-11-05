package chjob

import (
	"context"
	"database/sql"
	"dekart/src/proto"
	"dekart/src/server/job"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog/log"
)

// s3Config is needed to export results to S3 using ClickHouse's s3() function.
type s3Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

// StoreV2 implements the new job.Store interface for ClickHouse.
type StoreV2 struct {
	*job.BaseStore
	db             *sql.DB
	outputLocation string
	s3Config       s3Config
}

// NewStoreV2 creates a new ClickHouse store using the refactored architecture.
func NewStoreV2() *StoreV2 {
	dbConnStr := os.Getenv("DEKART_CLICKHOUSE_DATA_CONNECTION")
	outputLocation := os.Getenv("DEKART_CLICKHOUSE_S3_OUTPUT_LOCATION")
	if outputLocation == "" {
		log.Fatal().Msgf("clickhouse data connection requires DEKART_CLICKHOUSE_S3_OUTPUT_LOCATION")
	}

	s3Cfg := s3Config{
		Endpoint:  os.Getenv("AWS_ENDPOINT"),
		AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}

	// Verify required environment variables
	if s3Cfg.Endpoint == "" {
		log.Fatal().Msgf("dekart clickhouse requires AWS_ENDPOINT")
	}
	if s3Cfg.AccessKey == "" {
		log.Fatal().Msgf("dekart clickhouse requires AWS_ACCESS_KEY_ID")
	}
	if s3Cfg.SecretKey == "" {
		log.Fatal().Msgf("dekart clickhouse requires AWS_SECRET_ACCESS_KEY")
	}

	// Verify endpoint scheme
	if !strings.HasPrefix(s3Cfg.Endpoint, "http://") &&
		!strings.HasPrefix(s3Cfg.Endpoint, "https://") {
		log.Fatal().Msgf("invalid AWS endpoint scheme, must be http:// or https://")
	}

	opt, err := clickhouse.ParseDSN(dbConnStr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse clickhouse connection string")
	}

	db := clickhouse.OpenDB(opt)

	// Test connection
	_, err = db.Exec("SELECT 1")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to clickhouse")
	}

	store := &StoreV2{
		db:             db,
		outputLocation: outputLocation,
		s3Config:       s3Cfg,
	}

	// Note: We cannot use BaseStore directly because the executor needs
	// reportID and queryID to construct the S3 path. We'll override CreateJob.
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		// This will be overridden in CreateJob
		return nil
	})

	return store
}

// CreateJob overrides BaseStore to pass reportID and queryID to the executor.
func (s *StoreV2) CreateJob(ctx context.Context, req job.JobRequest) (*job.ManagedJob, <-chan int32, error) {
	// Create executor with reportID and queryID
	executor := NewExecutor(s.db, s.outputLocation, s.s3Config, req.ReportID, req.QueryID)

	j, err := job.NewJobBuilder().
		WithReportID(req.ReportID).
		WithQueryID(req.QueryID).
		WithQueryText(req.QueryText).
		WithExecutor(executor).
		Build(ctx)

	if err != nil {
		return nil, nil, err
	}

	// Use BaseStore's registry for lifecycle management
	s.BaseStore.Registry().Register(j)

	return j, j.Status(), nil
}

// TestConnection tests the ClickHouse connection.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	_, err := s.db.ExecContext(ctx, "SELECT 1")
	if err != nil {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &proto.TestConnectionResponse{Success: true}, nil
}

// Close closes the database connection.
func (s *StoreV2) Close() error {
	return s.db.Close()
}
