package job

import (
	"context"
	"dekart/src/proto"
)

// StoreV2 is the new store interface that uses the refactored architecture.
// This can coexist with the old Store interface during migration.
type StoreV2 interface {
	// CreateJob creates and registers a new job
	CreateJob(ctx context.Context, req JobRequest) (*ManagedJob, <-chan int32, error)

	// GetJob retrieves a job by ID
	GetJob(jobID string) (*ManagedJob, bool)

	// CancelJob cancels a specific job
	CancelJob(jobID string) bool

	// CancelAll cancels all jobs
	CancelAll(ctx context.Context)

	// TestConnection tests the connection
	TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error)
}

// JobRequest contains the parameters for creating a new job.
type JobRequest struct {
	ReportID  string
	QueryID   string
	QueryText string
}

// BaseStore provides common store functionality via composition.
// Database-specific stores can embed this to get full Store implementation.
type BaseStore struct {
	registry       *Registry
	createExecutor func() Executor
}

// NewBaseStore creates a new base store with the given executor factory.
func NewBaseStore(createExecutor func() Executor) *BaseStore {
	return &BaseStore{
		registry:       NewRegistry(),
		createExecutor: createExecutor,
	}
}

// CreateJob creates and registers a new job.
func (s *BaseStore) CreateJob(ctx context.Context, req JobRequest) (*ManagedJob, <-chan int32, error) {
	executor := s.createExecutor()

	job, err := NewJobBuilder().
		WithReportID(req.ReportID).
		WithQueryID(req.QueryID).
		WithQueryText(req.QueryText).
		WithExecutor(executor).
		Build(ctx)

	if err != nil {
		return nil, nil, err
	}

	s.registry.Register(job)

	return job, job.Status(), nil
}

// GetJob retrieves a job by ID.
func (s *BaseStore) GetJob(jobID string) (*ManagedJob, bool) {
	return s.registry.Get(jobID)
}

// CancelJob cancels a specific job by ID.
func (s *BaseStore) CancelJob(jobID string) bool {
	return s.registry.CancelJob(jobID)
}

// CancelAll cancels all active jobs.
func (s *BaseStore) CancelAll(ctx context.Context) {
	s.registry.CancelAll(ctx)
}

// Create implements the old Store interface for backward compatibility.
// This allows BaseStore to be used with existing code.
func (s *BaseStore) Create(reportID string, queryID string, queryText string, userCtx context.Context) (Job, chan int32, error) {
	job, statusChan, err := s.CreateJob(userCtx, JobRequest{
		ReportID:  reportID,
		QueryID:   queryID,
		QueryText: queryText,
	})
	if err != nil {
		return nil, nil, err
	}
	// Return the status channel as a regular chan (not read-only)
	return job, job.Status(), nil
}

// Cancel implements the old Store interface for backward compatibility.
func (s *BaseStore) Cancel(jobID string) bool {
	return s.CancelJob(jobID)
}

// Registry returns the underlying registry for advanced use cases.
func (s *BaseStore) Registry() *Registry {
	return s.registry
}
