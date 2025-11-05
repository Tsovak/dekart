package bqjob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/bqutils"
	"dekart/src/server/job"
	"dekart/src/server/user"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"
	bqStoragePb "google.golang.org/genproto/googleapis/cloud/bigquery/storage/v1"
)

// StoreV2 implements the new job.Store interface for BigQuery.
type StoreV2 struct {
	*job.BaseStore
	maxBytesBilled int64
}

// NewStoreV2 creates a new BigQuery store using the refactored architecture.
func NewStoreV2() *StoreV2 {
	store := &StoreV2{}

	// Parse max bytes billed configuration
	maxBytesBilledStr := os.Getenv("DEKART_BIGQUERY_MAX_BYTES_BILLED")
	if maxBytesBilledStr != "" {
		maxBytesBilled, err := strconv.ParseInt(maxBytesBilledStr, 10, 64)
		if err != nil {
			log.Fatal().Msgf("Cannot parse DEKART_BIGQUERY_MAX_BYTES_BILLED")
		}
		store.maxBytesBilled = maxBytesBilled
	} else {
		log.Warn().Msgf("DEKART_BIGQUERY_MAX_BYTES_BILLED is not set! Use the maximum bytes billed setting to limit query costs.")
	}

	// Initialize BaseStore with executor factory
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		return NewExecutor(store.maxBytesBilled)
	})

	return store
}

// CreateJob overrides BaseStore to add playground mode check.
func (s *StoreV2) CreateJob(ctx context.Context, req job.JobRequest) (*job.ManagedJob, <-chan int32, error) {
	// In playground mode, enforce max bytes billed
	if user.CheckWorkspaceCtx(ctx).IsPlayground {
		if s.maxBytesBilled == 0 {
			log.Warn().Msg("Playground mode active but DEKART_BIGQUERY_MAX_BYTES_BILLED not set")
		}
	}

	// Use BaseStore's CreateJob
	return s.BaseStore.CreateJob(ctx, req)
}

// TestConnection tests the BigQuery connection and permissions.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	// Test basic client connection
	client, err := bqutils.GetClient(ctx, req.Connection)
	if err != nil {
		log.Warn().Err(err).Msg("bigquery.NewClient failed")
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	defer client.Close()

	// Try listing datasets
	it := client.Datasets(ctx)
	_, err = it.Next()
	if err != nil && err != iterator.Done {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Test BigQuery Storage API permissions
	bqReadClient, err := bqutils.GetReadClient(ctx, req.Connection)
	if err != nil {
		log.Warn().Err(err).Msg("bigquery.NewBigQueryReadClient failed")
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	defer bqReadClient.Close()

	// Try creating a read session on a public dataset
	createReadSessionRequest := &bqStoragePb.CreateReadSessionRequest{
		Parent: "projects/" + req.Connection.BigqueryProjectId,
		ReadSession: &bqStoragePb.ReadSession{
			Table:      "projects/bigquery-public-data/datasets/samples/tables/shakespeare",
			DataFormat: bqStoragePb.DataFormat_AVRO,
		},
	}

	_, err = bqReadClient.CreateReadSession(ctx, createReadSessionRequest)
	if err != nil {
		if strings.Contains(err.Error(), "PermissionDenied") {
			return &proto.TestConnectionResponse{
				Success: false,
				Error:   err.Error(),
			}, nil
		}
	}

	return &proto.TestConnectionResponse{Success: true}, nil
}
