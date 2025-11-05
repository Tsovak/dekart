package job

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/storage"

	"github.com/rs/zerolog"
)

// ManagedJob orchestrates job execution, combining executor, state, and lifecycle.
// It implements the Job interface but uses composition instead of embedding.
type ManagedJob struct {
	state     *State
	executor  Executor
	ctx       context.Context
	cancel    context.CancelFunc
	queryText string
	logger    zerolog.Logger
}

// NewManagedJob creates a new managed job.
// Use JobBuilder instead of calling this directly.
func NewManagedJob(state *State, executor Executor, ctx context.Context, cancel context.CancelFunc, queryText string, logger zerolog.Logger) *ManagedJob {
	return &ManagedJob{
		state:     state,
		executor:  executor,
		ctx:       ctx,
		cancel:    cancel,
		queryText: queryText,
		logger:    logger,
	}
}

// Run executes the job asynchronously.
func (m *ManagedJob) Run(storageObject storage.StorageObject, connection *proto.Connection) error {
	m.state.SetStatus(int32(proto.QueryJob_JOB_STATUS_PENDING))

	go func() {
		defer m.cancel()
		defer m.state.Close()

		m.state.SetStatus(int32(proto.QueryJob_JOB_STATUS_RUNNING))

		result, err := m.executor.Execute(m.ctx, ExecutionRequest{
			QueryText:  m.queryText,
			Storage:    storageObject,
			Connection: connection,
			Logger:     m.logger,
		})

		if err != nil {
			m.logger.Error().Err(err).Msg("Job execution failed")
			m.state.SetError(err)
			return
		}

		m.state.SetStatus(int32(proto.QueryJob_JOB_STATUS_READING_RESULTS))
		m.state.SetResult(result)
		m.state.SetStatus(int32(proto.QueryJob_JOB_STATUS_DONE))
	}()

	return nil
}

// Cancel cancels the job execution.
func (m *ManagedJob) Cancel() {
	m.cancel()
}

// GetCtx returns the job's context.
func (m *ManagedJob) GetCtx() context.Context {
	return m.ctx
}

// State returns the job's state (read-only access recommended).
func (m *ManagedJob) State() *State {
	return m.state
}

// Status returns the status update channel.
func (m *ManagedJob) Status() chan int32 {
	return m.state.StatusChan()
}

// Job interface implementation (for backward compatibility)

func (m *ManagedJob) GetID() string {
	return m.state.ID()
}

func (m *ManagedJob) GetReportID() string {
	return m.state.ReportID()
}

func (m *ManagedJob) GetQueryID() string {
	return m.state.QueryID()
}

func (m *ManagedJob) GetResultID() *string {
	return m.state.GetResultID()
}

func (m *ManagedJob) IsResultReady() bool {
	return m.state.ResultReady()
}

func (m *ManagedJob) GetDWJobID() *string {
	return m.state.DWJobID()
}

func (m *ManagedJob) GetResultURI() *string {
	return m.state.ResultURI()
}

func (m *ManagedJob) GetTotalRows() int64 {
	return m.state.TotalRows()
}

func (m *ManagedJob) GetProcessedBytes() int64 {
	return m.state.ProcessedBytes()
}

func (m *ManagedJob) GetResultSize() int64 {
	return m.state.ResultSize()
}

func (m *ManagedJob) Err() string {
	return m.state.Err()
}
