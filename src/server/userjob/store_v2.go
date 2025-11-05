package userjob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/bqjob"
	"dekart/src/server/conn"
	"dekart/src/server/job"
	"dekart/src/server/snowflakejob"
	"dekart/src/server/wherobotsjob"
)

// StoreV2 implements the new job.Store interface as a router.
// It delegates to different store implementations based on connection type.
type StoreV2 struct {
	snowflakeStore *snowflakejob.StoreV2
	wherobotsStore *wherobotsjob.StoreV2
	bqStore        *bqjob.StoreV2
}

// NewStoreV2 creates a new UserJob store that routes to appropriate implementations.
func NewStoreV2() *StoreV2 {
	return &StoreV2{
		snowflakeStore: snowflakejob.NewStoreV2(),
		wherobotsStore: wherobotsjob.NewStoreV2(),
		bqStore:        bqjob.NewStoreV2(),
	}
}

// CreateJob routes to the appropriate store based on connection type.
func (s *StoreV2) CreateJob(ctx context.Context, req job.JobRequest) (*job.ManagedJob, <-chan int32, error) {
	connection := conn.FromCtx(ctx)

	switch connection.ConnectionType {
	case proto.ConnectionType_CONNECTION_TYPE_SNOWFLAKE:
		return s.snowflakeStore.CreateJob(ctx, req)
	case proto.ConnectionType_CONNECTION_TYPE_WHEROBOTS:
		return s.wherobotsStore.CreateJob(ctx, req)
	default:
		return s.bqStore.CreateJob(ctx, req)
	}
}

// Create implements the old Store interface for backward compatibility.
func (s *StoreV2) Create(reportID string, queryID string, queryText string, connCtx context.Context) (job.Job, chan int32, error) {
	j, statusChan, err := s.CreateJob(connCtx, job.JobRequest{
		ReportID:  reportID,
		QueryID:   queryID,
		QueryText: queryText,
	})
	if err != nil {
		return nil, nil, err
	}
	return j, j.Status(), nil
}

// GetJob retrieves a job from any of the underlying stores.
func (s *StoreV2) GetJob(jobID string) (*job.ManagedJob, bool) {
	// Try each store in order
	if j, found := s.snowflakeStore.GetJob(jobID); found {
		return j, true
	}
	if j, found := s.wherobotsStore.GetJob(jobID); found {
		return j, true
	}
	if j, found := s.bqStore.GetJob(jobID); found {
		return j, true
	}
	return nil, false
}

// CancelJob cancels a job in any of the underlying stores.
func (s *StoreV2) CancelJob(jobID string) bool {
	// Try each store
	if s.snowflakeStore.CancelJob(jobID) {
		return true
	}
	if s.wherobotsStore.CancelJob(jobID) {
		return true
	}
	if s.bqStore.CancelJob(jobID) {
		return true
	}
	return false
}

// Cancel implements the old Store interface.
func (s *StoreV2) Cancel(jobID string) bool {
	return s.CancelJob(jobID)
}

// CancelAll cancels all jobs in all underlying stores.
func (s *StoreV2) CancelAll(ctx context.Context) {
	s.snowflakeStore.CancelAll(ctx)
	s.wherobotsStore.CancelAll(ctx)
	s.bqStore.CancelAll(ctx)
}

// TestConnection routes to the appropriate store's TestConnection.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	switch req.Connection.ConnectionType {
	case proto.ConnectionType_CONNECTION_TYPE_SNOWFLAKE:
		return s.snowflakeStore.TestConnection(ctx, req)
	case proto.ConnectionType_CONNECTION_TYPE_WHEROBOTS:
		return s.wherobotsStore.TestConnection(ctx, req)
	default:
		return s.bqStore.TestConnection(ctx, req)
	}
}
