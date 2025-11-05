package job

import (
	"dekart/src/proto"
	"sync"
)

// State manages job state with automatic thread safety.
// All access to state fields is protected by RWMutex.
type State struct {
	mu sync.RWMutex

	// Identity
	id       string
	reportID string
	queryID  string

	// Status
	status     int32
	statusChan chan int32

	// Results
	resultReady    bool
	totalRows      int64
	processedBytes int64
	resultSize     int64
	dwJobID        *string
	resultURI      *string

	// Error
	err string
}

// NewState creates a new State with the given identifiers.
func NewState(id, reportID, queryID string) *State {
	return &State{
		id:         id,
		reportID:   reportID,
		queryID:    queryID,
		status:     int32(proto.QueryJob_JOB_STATUS_UNSPECIFIED),
		statusChan: make(chan int32, 10), // buffered to prevent blocking
	}
}

// Thread-safe getters (use RLock for better read concurrency)

func (s *State) ID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

func (s *State) ReportID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reportID
}

func (s *State) QueryID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.queryID
}

func (s *State) Status() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *State) ResultReady() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resultReady
}

func (s *State) TotalRows() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.totalRows
}

func (s *State) ProcessedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processedBytes
}

func (s *State) ResultSize() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resultSize
}

func (s *State) DWJobID() *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dwJobID
}

func (s *State) ResultURI() *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resultURI
}

func (s *State) Err() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err
}

// GetResultID returns the job ID if result is ready, nil otherwise.
func (s *State) GetResultID() *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.resultReady {
		id := s.id
		return &id
	}
	return nil
}

// StatusChan returns the status update channel.
func (s *State) StatusChan() chan int32 {
	return s.statusChan
}

// Thread-safe setters (use Lock for exclusive write access)

// SetStatus updates the status and sends it to the status channel.
func (s *State) SetStatus(status int32) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()

	// Send to channel without holding lock (non-blocking)
	select {
	case s.statusChan <- status:
	default:
		// Channel full, skip (this shouldn't happen with buffered channel)
	}
}

// SetResult atomically updates all result-related fields.
func (s *State) SetResult(result *ExecutionResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.resultReady = true
	s.totalRows = result.TotalRows
	s.processedBytes = result.ProcessedBytes
	s.resultSize = result.ResultSize
	s.dwJobID = result.DWJobID
	s.resultURI = result.ResultURI
}

// SetProcessedBytes updates the processed bytes (useful for streaming updates).
func (s *State) SetProcessedBytes(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processedBytes = bytes
}

// SetError sets the error message and updates status.
func (s *State) SetError(err error) {
	s.mu.Lock()
	s.err = err.Error()
	s.status = int32(proto.QueryJob_JOB_STATUS_UNSPECIFIED)
	s.mu.Unlock()

	// Send status update
	select {
	case s.statusChan <- int32(proto.QueryJob_JOB_STATUS_UNSPECIFIED):
	default:
	}
}

// Close closes the status channel. Should be called when job is done.
func (s *State) Close() {
	close(s.statusChan)
}
