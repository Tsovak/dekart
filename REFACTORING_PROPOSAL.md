# Job Architecture Refactoring Proposal

## Executive Summary

This document proposes a comprehensive refactoring of the Job architecture in `dekart/src/server/`. The current design uses struct embedding (e.g., `job.BasicStore`, `job.BasicJob`) which creates awkward method calls and unclear abstractions. This proposal introduces a cleaner architecture using composition, dependency injection, and clear separation of concerns.

## Current Problems

### 1. Awkward Embedding Pattern
```go
// Current - feels weird
type Store struct {
    job.BasicStore  // What is "Basic"? Why exposed?
}

// Calling methods is unclear
s.StoreJob(job)  // Is this Store's method or BasicStore's?
```

### 2. Two-Step Initialization (Error-Prone)
```go
// Current - easy to forget Init()
job := &Job{
    BasicJob: job.BasicJob{
        ReportID: reportID,
        QueryID: queryID,
        QueryText: queryText,
    },
}
job.Init(userCtx)  // MUST be called manually!
```

### 3. Massive Boilerplate in Every Store.Create()
Every implementation repeats the same 10 lines:
```go
func (s *Store) Create(...) {
    job := &Job{
        BasicJob: job.BasicJob{...},
        // specific fields
    }
    job.Init(userCtx)
    s.StoreJob(job)
    go s.RemoveJobWhenDone(job)
    return job, job.Status(), nil
}
```

### 4. Mixed Responsibilities in BasicJob
BasicJob does too much:
- State management (ResultReady, ResultSize, TotalRows, ProcessedBytes, etc.)
- Concurrency primitives (sync.Mutex, context.Context, context.CancelFunc)
- Communication (status chan)
- Lifecycle (Init, Cancel, CancelWithError)
- Domain logic (GetResultID checks ResultReady)

### 5. Manual Thread Safety (Verbose & Error-Prone)
```go
// Every implementation must do this manually
j.Lock()
j.ProcessedBytes = bytes
j.ResultSize = size
j.ResultReady = true
j.Unlock()
```

### 6. Status Channel Management Unclear
- Created in Init(), never closed
- Multiple goroutines write to it
- No clear ownership or lifecycle
- Potential for goroutine leaks

### 7. Testing Difficulties
- Hard to mock embedded structs
- Can't easily test state transitions
- Tight coupling makes unit testing difficult

---

## Proposed Architecture

### Layer 1: Executor (Database-Specific Logic)

**Clean interface for query execution:**

```go
// executor.go
package job

// Executor executes queries against a specific database
type Executor interface {
    // Execute runs the query and writes results to storage
    Execute(ctx context.Context, req ExecutionRequest) (*ExecutionResult, error)
}

type ExecutionRequest struct {
    QueryText     string
    Storage       storage.StorageObject
    Connection    *proto.Connection
    Logger        zerolog.Logger
}

type ExecutionResult struct {
    TotalRows       int64
    ProcessedBytes  int64
    ResultSize      int64
    DWJobID         *string  // e.g., BigQuery job ID
    ResultURI       *string  // e.g., S3 location
}
```

**Example implementation:**
```go
// bqjob/executor.go
package bqjob

type Executor struct {
    maxBytesBilled int64
}

func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    client, err := bqutils.GetClient(ctx, req.Connection)
    if err != nil {
        return nil, err
    }

    query := client.Query(req.QueryText)
    query.MaxBytesBilled = e.maxBytesBilled

    bqJob, err := query.Run(ctx)
    if err != nil {
        return nil, err
    }

    // Wait for query completion
    status, err := bqJob.Wait(ctx)
    if err != nil {
        return nil, err
    }

    // Read and write results
    // ... (simplified, actual logic stays similar)

    return &job.ExecutionResult{
        TotalRows: rows,
        ProcessedBytes: bytes,
        ResultSize: size,
    }, nil
}
```

### Layer 2: Job State (Thread-Safe State Management)

**Encapsulated state with automatic thread safety:**

```go
// state.go
package job

// State manages job state with automatic thread safety
type State struct {
    mu sync.RWMutex

    // Identity
    id       string
    reportID string
    queryID  string

    // Status
    status      proto.QueryJob_JobStatus
    statusChan  chan proto.QueryJob_JobStatus

    // Results
    resultReady    bool
    totalRows      int64
    processedBytes int64
    resultSize     int64
    dwJobID        *string
    resultURI      *string

    // Error
    err error
}

// Thread-safe getters (using RLock)
func (s *State) Status() proto.QueryJob_JobStatus {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.status
}

func (s *State) ResultReady() bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.resultReady
}

// Thread-safe setters (using Lock)
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

func (s *State) SetStatus(status proto.QueryJob_JobStatus) {
    s.mu.Lock()
    s.status = status
    s.mu.Unlock()

    // Non-blocking send (don't hold lock)
    select {
    case s.statusChan <- status:
    default:
    }
}

func (s *State) SetError(err error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.err = err
    s.status = proto.QueryJob_JOB_STATUS_UNSPECIFIED
}

// GetResultID returns job ID if result is ready
func (s *State) GetResultID() *string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    if s.resultReady {
        id := s.id
        return &id
    }
    return nil
}
```

### Layer 3: ManagedJob (Orchestrates Execution)

**Composition of executor + state + lifecycle:**

```go
// managed_job.go
package job

// ManagedJob orchestrates job execution and lifecycle
type ManagedJob struct {
    state     *State
    executor  Executor
    ctx       context.Context
    cancel    context.CancelFunc
    queryText string
    logger    zerolog.Logger
}

// Run executes the job asynchronously
func (m *ManagedJob) Run(storage storage.StorageObject, conn *proto.Connection) error {
    m.state.SetStatus(proto.QueryJob_JOB_STATUS_PENDING)

    go func() {
        defer m.cancel()

        m.state.SetStatus(proto.QueryJob_JOB_STATUS_RUNNING)

        result, err := m.executor.Execute(m.ctx, ExecutionRequest{
            QueryText:  m.queryText,
            Storage:    storage,
            Connection: conn,
            Logger:     m.logger,
        })

        if err != nil {
            m.logger.Error().Err(err).Msg("Job execution failed")
            m.state.SetError(err)
            return
        }

        m.state.SetStatus(proto.QueryJob_JOB_STATUS_READING_RESULTS)
        m.state.SetResult(result)
        m.state.SetStatus(proto.QueryJob_JOB_STATUS_DONE)
    }()

    return nil
}

// Cancel cancels the job
func (m *ManagedJob) Cancel() {
    m.cancel()
}

// Context returns the job's context
func (m *ManagedJob) Context() context.Context {
    return m.ctx
}

// State returns read-only access to state
func (m *ManagedJob) State() *State {
    return m.state
}

// StatusChan returns the status update channel
func (m *ManagedJob) StatusChan() <-chan proto.QueryJob_JobStatus {
    return m.state.statusChan
}
```

### Layer 4: JobBuilder (Clean Initialization)

**Builder pattern eliminates two-step initialization:**

```go
// builder.go
package job

type JobBuilder struct {
    reportID  string
    queryID   string
    queryText string
    executor  Executor
    timeout   time.Duration
}

func NewJobBuilder() *JobBuilder {
    return &JobBuilder{
        timeout: 10 * time.Minute,
    }
}

func (b *JobBuilder) WithReportID(id string) *JobBuilder {
    b.reportID = id
    return b
}

func (b *JobBuilder) WithQueryID(id string) *JobBuilder {
    b.queryID = id
    return b
}

func (b *JobBuilder) WithQueryText(text string) *JobBuilder {
    b.queryText = text
    return b
}

func (b *JobBuilder) WithExecutor(executor Executor) *JobBuilder {
    b.executor = executor
    return b
}

func (b *JobBuilder) WithTimeout(timeout time.Duration) *JobBuilder {
    b.timeout = timeout
    return b
}

func (b *JobBuilder) Build(userCtx context.Context) (*ManagedJob, error) {
    if b.executor == nil {
        return nil, fmt.Errorf("executor is required")
    }

    // Create context with timeout
    ctx, cancel := context.WithTimeout(
        conn.CopyConnectionCtx(
            userCtx,
            user.CopyUserContext(userCtx, context.Background()),
        ),
        b.timeout,
    )

    // Create state
    state := &State{
        id:         uuid.GetUUID(),
        reportID:   b.reportID,
        queryID:    b.queryID,
        status:     proto.QueryJob_JOB_STATUS_UNSPECIFIED,
        statusChan: make(chan proto.QueryJob_JobStatus, 10), // buffered
    }

    // Create managed job
    return &ManagedJob{
        state:     state,
        executor:  b.executor,
        ctx:       ctx,
        cancel:    cancel,
        queryText: b.queryText,
        logger:    log.With().Str("reportID", b.reportID).Str("queryID", b.queryID).Logger(),
    }, nil
}
```

### Layer 5: Registry (Job Storage Management)

**Replaces BasicStore with clearer abstraction:**

```go
// registry.go
package job

// Registry manages active jobs
type Registry struct {
    mu   sync.RWMutex
    jobs map[string]*ManagedJob
}

func NewRegistry() *Registry {
    return &Registry{
        jobs: make(map[string]*ManagedJob),
    }
}

// Register adds a job and starts cleanup goroutine
func (r *Registry) Register(job *ManagedJob) {
    r.mu.Lock()
    r.jobs[job.State().ID()] = job
    r.mu.Unlock()

    // Automatic cleanup when job finishes
    go func() {
        <-job.Context().Done()
        r.Unregister(job.State().ID())
    }()
}

// Get retrieves a job by ID
func (r *Registry) Get(jobID string) (*ManagedJob, bool) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    job, found := r.jobs[jobID]
    return job, found
}

// Unregister removes a job
func (r *Registry) Unregister(jobID string) {
    r.mu.Lock()
    defer r.mu.Unlock()
    delete(r.jobs, jobID)
}

// CancelJob cancels a specific job
func (r *Registry) CancelJob(jobID string) bool {
    job, found := r.Get(jobID)
    if !found {
        return false
    }
    job.Cancel()
    return true
}

// CancelAll cancels all active jobs
func (r *Registry) CancelAll(ctx context.Context) {
    r.mu.RLock()
    jobs := make([]*ManagedJob, 0, len(r.jobs))
    for _, job := range r.jobs {
        jobs = append(jobs, job)
    }
    r.mu.RUnlock()

    for _, job := range jobs {
        select {
        case <-ctx.Done():
            return
        default:
            job.Cancel()
        }
    }
}

// List returns all active jobs
func (r *Registry) List() []*ManagedJob {
    r.mu.RLock()
    defer r.mu.RUnlock()

    jobs := make([]*ManagedJob, 0, len(r.jobs))
    for _, job := range r.jobs {
        jobs = append(jobs, job)
    }
    return jobs
}
```

### Layer 6: Store Interface & Implementations

**Simplified Store interface:**

```go
// store.go
package job

type Store interface {
    // CreateJob creates and registers a new job
    CreateJob(ctx context.Context, req JobRequest) (*ManagedJob, <-chan proto.QueryJob_JobStatus, error)

    // GetJob retrieves a job by ID
    GetJob(jobID string) (*ManagedJob, bool)

    // CancelJob cancels a specific job
    CancelJob(jobID string) bool

    // CancelAll cancels all jobs
    CancelAll(ctx context.Context)

    // TestConnection tests the connection
    TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error)
}

type JobRequest struct {
    ReportID  string
    QueryID   string
    QueryText string
}

// BaseStore provides common store functionality via composition
type BaseStore struct {
    registry  *Registry
    createExecutor func() Executor
}

func NewBaseStore(createExecutor func() Executor) *BaseStore {
    return &BaseStore{
        registry:       NewRegistry(),
        createExecutor: createExecutor,
    }
}

func (s *BaseStore) CreateJob(ctx context.Context, req JobRequest) (*ManagedJob, <-chan proto.QueryJob_JobStatus, error) {
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

    return job, job.StatusChan(), nil
}

func (s *BaseStore) GetJob(jobID string) (*ManagedJob, bool) {
    return s.registry.Get(jobID)
}

func (s *BaseStore) CancelJob(jobID string) bool {
    return s.registry.CancelJob(jobID)
}

func (s *BaseStore) CancelAll(ctx context.Context) {
    s.registry.CancelAll(ctx)
}
```

---

## Implementation Examples

### BigQuery Store (After Refactoring)

```go
// bqjob/store.go
package bqjob

import (
    "context"
    "dekart/src/proto"
    "dekart/src/server/job"
    "os"
    "strconv"
)

type Store struct {
    *job.BaseStore
    maxBytesBilled int64
}

func NewStore() *Store {
    store := &Store{}

    // Parse max bytes billed
    if maxBytesStr := os.Getenv("DEKART_BIGQUERY_MAX_BYTES_BILLED"); maxBytesStr != "" {
        if maxBytes, err := strconv.ParseInt(maxBytesStr, 10, 64); err == nil {
            store.maxBytesBilled = maxBytes
        }
    }

    // Initialize BaseStore with executor factory
    store.BaseStore = job.NewBaseStore(func() job.Executor {
        return &Executor{
            maxBytesBilled: store.maxBytesBilled,
        }
    })

    return store
}

func (s *Store) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
    client, err := bqutils.GetClient(ctx, req.Connection)
    if err != nil {
        return &proto.TestConnectionResponse{Success: false, Error: err.Error()}, nil
    }
    defer client.Close()

    return &proto.TestConnectionResponse{Success: true}, nil
}
```

### BigQuery Executor (After Refactoring)

```go
// bqjob/executor.go
package bqjob

import (
    "context"
    "dekart/src/server/bqutils"
    "dekart/src/server/job"
    "regexp"
)

type Executor struct {
    maxBytesBilled int64
}

var orderByRe = regexp.MustCompile(`(?ims)order[\s]+by`)

func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    client, err := bqutils.GetClient(ctx, req.Connection)
    if err != nil {
        return nil, err
    }
    defer client.Close()

    query := client.Query(req.QueryText)
    query.MaxBytesBilled = e.maxBytesBilled

    bqJob, err := query.Run(ctx)
    if err != nil {
        return nil, err
    }

    status, err := bqJob.Wait(ctx)
    if err != nil {
        return nil, err
    }
    if err := status.Err(); err != nil {
        return nil, err
    }

    // Get table and metadata
    table, err := bqutils.GetTableFromJob(bqJob)
    if err != nil {
        return nil, err
    }

    metadata, err := table.Metadata(ctx)
    if err != nil {
        return nil, err
    }

    // Determine read streams based on ORDER BY
    maxReadStreams := int32(10)
    if orderByRe.MatchString(req.QueryText) {
        maxReadStreams = 1 // preserve order
    }

    // Read results and write to storage
    csvRows := make(chan []string, metadata.NumRows)
    errors := make(chan error, 1)

    go bqutils.Read(ctx, errors, csvRows, table, req.Logger, maxReadStreams)
    go writeCSV(ctx, req.Storage, csvRows, errors)

    if err := <-errors; err != nil {
        return nil, err
    }

    size, err := req.Storage.GetSize(ctx)
    if err != nil {
        return nil, err
    }

    var processedBytes int64
    if status.Statistics != nil {
        processedBytes = status.Statistics.TotalBytesProcessed
    }

    return &job.ExecutionResult{
        TotalRows:      int64(metadata.NumRows),
        ProcessedBytes: processedBytes,
        ResultSize:     *size,
    }, nil
}

func writeCSV(ctx context.Context, storage storage.StorageObject, csvRows chan []string, errors chan error) {
    writer := storage.GetWriter(ctx)
    csvWriter := csv.NewWriter(writer)

    for row := range csvRows {
        if err := csvWriter.Write(row); err != nil {
            errors <- err
            return
        }
    }

    csvWriter.Flush()
    if err := writer.Close(); err != nil {
        errors <- err
        return
    }

    errors <- nil
}
```

### PostgreSQL Store (After Refactoring)

```go
// pgjob/store.go
package pgjob

import (
    "context"
    "database/sql"
    "dekart/src/proto"
    "dekart/src/server/job"
    "os"

    _ "github.com/lib/pq"
)

type Store struct {
    *job.BaseStore
    db *sql.DB
}

func NewStore() *Store {
    dbConnStr := os.Getenv("DEKART_POSTGRES_DATASOURCE_CONNECTION")
    db, err := sql.Open("postgres", dbConnStr)
    if err != nil {
        log.Fatal().Err(err).Msg("failed to connect to postgres")
    }

    store := &Store{db: db}

    store.BaseStore = job.NewBaseStore(func() job.Executor {
        return &Executor{db: db}
    })

    return store
}

func (s *Store) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
    if err := s.db.PingContext(ctx); err != nil {
        return &proto.TestConnectionResponse{Success: false, Error: err.Error()}, nil
    }
    return &proto.TestConnectionResponse{Success: true}, nil
}
```

### PostgreSQL Executor (After Refactoring)

```go
// pgjob/executor.go
package pgjob

import (
    "context"
    "database/sql"
    "dekart/src/server/job"
    "encoding/csv"
)

type Executor struct {
    db *sql.DB
}

func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    rows, err := e.db.QueryContext(ctx, req.QueryText)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    columnTypes, err := rows.ColumnTypes()
    if err != nil {
        return nil, err
    }

    writer := req.Storage.GetWriter(ctx)
    csvWriter := csv.NewWriter(writer)

    // Write header
    headers := make([]string, len(columnTypes))
    for i, col := range columnTypes {
        headers[i] = col.Name()
    }
    csvWriter.Write(headers)

    // Write rows
    rowCount := int64(0)
    values := make([]interface{}, len(columnTypes))
    for i := range values {
        values[i] = new(sql.NullString)
    }

    for rows.Next() {
        if err := rows.Scan(values...); err != nil {
            continue // skip bad rows
        }

        csvRow := make([]string, len(columnTypes))
        for i, val := range values {
            if ns, ok := val.(*sql.NullString); ok {
                csvRow[i] = ns.String
            }
        }

        csvWriter.Write(csvRow)
        rowCount++
    }

    csvWriter.Flush()
    writer.Close()

    size, err := req.Storage.GetSize(ctx)
    if err != nil {
        return nil, err
    }

    return &job.ExecutionResult{
        TotalRows:  rowCount,
        ResultSize: *size,
    }, nil
}
```

---

## Migration Strategy

### Phase 1: Add New Code (Non-Breaking)
1. Create new packages: `job/state.go`, `job/executor.go`, `job/managed_job.go`, `job/builder.go`, `job/registry.go`
2. Keep existing `BasicJob` and `BasicStore` for backward compatibility
3. Add adapters if needed

### Phase 2: Migrate One Implementation
1. Start with simplest: PostgreSQL
2. Create `pgjob/executor.go` and `pgjob/store_v2.go`
3. Test thoroughly
4. Compare performance

### Phase 3: Migrate Remaining Implementations
1. BigQuery
2. Athena
3. ClickHouse
4. Snowflake
5. Wherobots
6. UserJob (router)

### Phase 4: Update Main Entry Point
1. Update `main.go` to use new Store implementations
2. Update `dekart/server.go` if needed

### Phase 5: Remove Old Code
1. Delete `BasicJob` and `BasicStore`
2. Delete old implementation files
3. Update documentation

---

## Benefits Summary

### 1. **Cleaner Abstractions**
- No more awkward embedding
- Clear separation of concerns
- Obvious ownership and responsibilities

### 2. **Easier to Use**
```go
// Before (error-prone)
job := &Job{
    BasicJob: job.BasicJob{...},
}
job.Init(userCtx)  // Easy to forget!
s.StoreJob(job)
go s.RemoveJobWhenDone(job)

// After (foolproof)
job, statusChan, err := store.CreateJob(ctx, job.JobRequest{...})
```

### 3. **Automatic Thread Safety**
```go
// Before (manual locking)
j.Lock()
j.ResultReady = true
j.ResultSize = size
j.Unlock()

// After (automatic)
j.State().SetResult(result)  // Locking handled internally
```

### 4. **Better Testing**
```go
// Mock executor for testing
type MockExecutor struct {
    result *job.ExecutionResult
    err    error
}

func (m *MockExecutor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    return m.result, m.err
}

// Test job without database
job, _ := job.NewJobBuilder().
    WithExecutor(&MockExecutor{result: &job.ExecutionResult{...}}).
    Build(ctx)
```

### 5. **No Boilerplate**
- Store implementations become 20-30 lines instead of 60+
- Executor implementations focus only on database logic
- No repeated lifecycle management code

### 6. **Progressive Enhancement**
- Easy to add features (e.g., retries, timeouts, metrics)
- Can add middleware-style patterns
- Can compose executors

### 7. **Better Performance**
- Buffered status channels reduce blocking
- Registry uses map instead of slice (O(1) lookups)
- RWMutex for better read concurrency

---

## Estimated Effort

- **Phase 1** (New code): 2-3 days
- **Phase 2** (PostgreSQL migration): 1 day
- **Phase 3** (Other implementations): 3-4 days
- **Phase 4** (Integration): 1 day
- **Phase 5** (Cleanup): 1 day

**Total: ~2 weeks** with testing

---

## Risks & Mitigations

### Risk 1: Breaking Changes
**Mitigation**: Keep old code during migration, use feature flags

### Risk 2: Performance Regression
**Mitigation**: Benchmark before/after, load testing

### Risk 3: Subtle Bugs
**Mitigation**: Extensive integration tests, parallel running of old/new

### Risk 4: Team Learning Curve
**Mitigation**: Document well, pair programming, gradual rollout

---

## Conclusion

This refactoring addresses all the identified pain points:
- ✅ No more awkward embedding
- ✅ No more two-step initialization
- ✅ No more boilerplate in every Store.Create()
- ✅ Clear separation of concerns
- ✅ Automatic thread safety
- ✅ Better testing
- ✅ Cleaner, more maintainable code

The new architecture uses well-established patterns (Builder, Registry, Dependency Injection) and makes the codebase significantly easier to understand, maintain, and extend.
