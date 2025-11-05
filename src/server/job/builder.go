package job

import (
	"context"
	"dekart/src/server/conn"
	"dekart/src/server/user"
	"dekart/src/server/uuid"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

// JobBuilder uses the builder pattern to create ManagedJob instances.
// This eliminates the error-prone two-step initialization.
type JobBuilder struct {
	reportID  string
	queryID   string
	queryText string
	executor  Executor
	timeout   time.Duration
}

// NewJobBuilder creates a new JobBuilder with default values.
func NewJobBuilder() *JobBuilder {
	return &JobBuilder{
		timeout: 10 * time.Minute, // default timeout
	}
}

// WithReportID sets the report ID.
func (b *JobBuilder) WithReportID(id string) *JobBuilder {
	b.reportID = id
	return b
}

// WithQueryID sets the query ID.
func (b *JobBuilder) WithQueryID(id string) *JobBuilder {
	b.queryID = id
	return b
}

// WithQueryText sets the query text.
func (b *JobBuilder) WithQueryText(text string) *JobBuilder {
	b.queryText = text
	return b
}

// WithExecutor sets the executor.
func (b *JobBuilder) WithExecutor(executor Executor) *JobBuilder {
	b.executor = executor
	return b
}

// WithTimeout sets the execution timeout.
func (b *JobBuilder) WithTimeout(timeout time.Duration) *JobBuilder {
	b.timeout = timeout
	return b
}

// Build creates a ManagedJob from the builder configuration.
func (b *JobBuilder) Build(userCtx context.Context) (*ManagedJob, error) {
	// Validate required fields
	if b.executor == nil {
		return nil, fmt.Errorf("executor is required")
	}
	if b.queryText == "" {
		return nil, fmt.Errorf("queryText is required")
	}

	// Generate job ID
	jobID := uuid.GetUUID()

	// Create context with timeout (similar to BasicJob.Init)
	ctx, cancel := context.WithTimeout(
		conn.CopyConnectionCtx(
			userCtx,
			user.CopyUserContext(userCtx, context.Background()),
		),
		b.timeout,
	)

	// Create state
	state := NewState(jobID, b.reportID, b.queryID)

	// Create logger
	logger := log.With().
		Str("jobID", jobID).
		Str("reportID", b.reportID).
		Str("queryID", b.queryID).
		Logger()

	// Create managed job
	return NewManagedJob(state, b.executor, ctx, cancel, b.queryText, logger), nil
}
