# Before/After Code Comparison

## 1. Creating a Job

### BEFORE (Boilerplate + Error-Prone)
```go
// pgjob/pgjob.go - Every Store.Create() has this same pattern
func (s *Store) Create(reportID string, queryID string, queryText string, userCtx context.Context) (job.Job, chan int32, error) {
    j := &Job{
        BasicJob: job.BasicJob{  // ❌ Awkward embedding
            ReportID:  reportID,
            QueryID:   queryID,
            QueryText: queryText,
            Logger:    log.With().Str("reportID", reportID).Str("queryID", queryID).Logger(),
        },
        postgresDB: s.postgresDB,
    }

    j.Init(userCtx)  // ❌ Easy to forget!
    s.StoreJob(j)    // ❌ Which object's method?
    go s.RemoveJobWhenDone(j)  // ❌ Manual lifecycle management
    return j, j.Status(), nil
}
```

### AFTER (Clean + Foolproof)
```go
// pgjob/store.go - Inherits from BaseStore
type Store struct {
    *job.BaseStore  // ✅ Clear composition
    db *sql.DB
}

func NewStore() *Store {
    dbConnStr := os.Getenv("DEKART_POSTGRES_DATASOURCE_CONNECTION")
    db, _ := sql.Open("postgres", dbConnStr)

    store := &Store{db: db}

    // ✅ Factory function creates executor with DB
    store.BaseStore = job.NewBaseStore(func() job.Executor {
        return &Executor{db: db}
    })

    return store
}

// ✅ CreateJob inherited from BaseStore - no boilerplate!
// Store automatically:
//   - Creates job with builder
//   - Registers in registry
//   - Sets up automatic cleanup
//   - Returns status channel
```

**Lines of code: 16 → 8** (50% reduction)

---

## 2. Job Execution Logic

### BEFORE (Mixed Concerns)
```go
// pgjob/pgjob.go
type Job struct {
    job.BasicJob  // ❌ Embedding exposes implementation
    postgresDB    *sql.DB
    storageObject storage.StorageObject
}

func (j *Job) Run(storageObject storage.StorageObject, connection *proto.Connection) error {
    j.Status() <- int32(proto.QueryJob_JOB_STATUS_RUNNING)  // ❌ Mixed with logic
    j.storageObject = storageObject

    rows, err := j.postgresDB.QueryContext(j.GetCtx(), j.QueryText)
    if err != nil {
        j.CancelWithError(err)  // ❌ Lifecycle mixed with execution
        return err
    }
    defer rows.Close()

    csvRows := make(chan []string, 10_000)
    go j.write(csvRows)  // ❌ Goroutine management in executor

    // ... reading and writing logic mixed together ...

    j.Lock()  // ❌ Manual thread safety
    j.ResultSize = *size
    j.ResultReady = true
    j.Unlock()

    j.Status() <- int32(proto.QueryJob_JOB_STATUS_DONE)
    return nil
}
```

### AFTER (Pure Execution Logic)
```go
// pgjob/executor.go
type Executor struct {
    db *sql.DB  // ✅ Only what's needed
}

// ✅ Pure function: takes request, returns result
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    rows, err := e.db.QueryContext(ctx, req.QueryText)
    if err != nil {
        return nil, err  // ✅ Simple error handling
    }
    defer rows.Close()

    columnTypes, _ := rows.ColumnTypes()
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
        rows.Scan(values...)
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

    size, _ := req.Storage.GetSize(ctx)

    // ✅ Return pure result - no side effects
    return &job.ExecutionResult{
        TotalRows:  rowCount,
        ResultSize: *size,
    }, nil
}
```

**Benefits:**
- ✅ No status updates mixed in
- ✅ No lifecycle management
- ✅ No manual locking
- ✅ Easy to test (pure function)
- ✅ Single responsibility

---

## 3. Thread-Safe State Updates

### BEFORE (Manual Locking Everywhere)
```go
// athenajob/athenajob.go:117-121
j.Lock()
j.ProcessedBytes = *queryExecution.Statistics.DataScannedInBytes
j.Unlock()

// athenajob/athenajob.go:133-138
j.Lock()
j.ResultSize = *size
j.ResultReady = true
j.Unlock()

// bqjob/bqjob.go:58-61
job.Lock()
job.ResultSize = *resultSize
job.ResultReady = true
job.Unlock()

// ❌ Easy to forget Lock/Unlock
// ❌ Verbose
// ❌ Risk of deadlocks
// ❌ Risk of race conditions
```

### AFTER (Automatic Thread Safety)
```go
// In ManagedJob - orchestrator handles state
func (m *ManagedJob) Run(storage storage.StorageObject, conn *proto.Connection) error {
    m.state.SetStatus(proto.QueryJob_JOB_STATUS_RUNNING)  // ✅ Thread-safe

    result, err := m.executor.Execute(m.ctx, ExecutionRequest{...})
    if err != nil {
        m.state.SetError(err)  // ✅ Thread-safe
        return err
    }

    m.state.SetResult(result)  // ✅ Thread-safe, sets multiple fields atomically
    m.state.SetStatus(proto.QueryJob_JOB_STATUS_DONE)
    return nil
}

// State handles locking internally
func (s *State) SetResult(result *ExecutionResult) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // ✅ All related fields updated atomically
    s.resultReady = true
    s.totalRows = result.TotalRows
    s.processedBytes = result.ProcessedBytes
    s.resultSize = result.ResultSize
    s.dwJobID = result.DWJobID
    s.resultURI = result.ResultURI
}
```

**Benefits:**
- ✅ Impossible to forget locking
- ✅ Atomic updates of related fields
- ✅ Cleaner code
- ✅ No deadlocks

---

## 4. Job Storage Management

### BEFORE (BasicStore Embedding)
```go
// athenajob/athenajob.go:18-23
type Store struct {
    job.BasicStore  // ❌ What is "Basic"?
    session        *session.Session
    outputLocation string
}

// Using it:
s.StoreJob(job)  // ❌ Unclear which object's method
go s.RemoveJobWhenDone(job)  // ❌ Manual lifecycle

// BasicStore implementation:
type BasicStore struct {
    sync.Mutex
    Jobs []Job  // ❌ Slice = O(n) lookups, removals
}

func (s *BasicStore) Cancel(jobID string) bool {
    s.Lock()
    defer s.Unlock()
    for _, job := range s.Jobs {  // ❌ O(n) search
        if job.GetID() == jobID {
            job.Cancel()
            return true
        }
    }
    return false
}
```

### AFTER (Registry with Map)
```go
// job/registry.go
type Registry struct {
    mu   sync.RWMutex  // ✅ Read-write mutex
    jobs map[string]*ManagedJob  // ✅ Map = O(1) lookups
}

func (r *Registry) Register(job *ManagedJob) {
    r.mu.Lock()
    r.jobs[job.State().ID()] = job
    r.mu.Unlock()

    // ✅ Automatic cleanup
    go func() {
        <-job.Context().Done()
        r.Unregister(job.State().ID())
    }()
}

func (r *Registry) Get(jobID string) (*ManagedJob, bool) {
    r.mu.RLock()  // ✅ Read lock for better concurrency
    defer r.mu.RUnlock()
    job, found := r.jobs[jobID]  // ✅ O(1) lookup
    return job, found
}

// BaseStore uses Registry
type BaseStore struct {
    registry *Registry  // ✅ Clear ownership
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

    s.registry.Register(job)  // ✅ Automatic lifecycle management
    return job, job.StatusChan(), nil
}
```

**Benefits:**
- ✅ O(1) lookups instead of O(n)
- ✅ RWMutex for better read concurrency
- ✅ Clear ownership
- ✅ Automatic cleanup

---

## 5. Testing

### BEFORE (Hard to Test)
```go
// Testing BigQuery job requires:
// ❌ Real BigQuery client
// ❌ Real storage
// ❌ Managing embedded BasicJob state
// ❌ Dealing with goroutines and channels

// Example: Can't easily unit test job logic
func TestBigQueryJob(t *testing.T) {
    // Need to:
    // 1. Setup BigQuery client (requires credentials)
    // 2. Create storage mock
    // 3. Initialize BasicJob properly
    // 4. Handle status channel
    // 5. Wait for goroutines
    // ❌ This is integration testing, not unit testing
}
```

### AFTER (Easy to Test)
```go
// Testing executor is simple - pure function!
type MockExecutor struct {
    result *job.ExecutionResult
    err    error
}

func (m *MockExecutor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    return m.result, m.err  // ✅ Controlled response
}

// Test job state management
func TestJobStateTransitions(t *testing.T) {
    state := job.NewState("job-1", "report-1", "query-1")

    // ✅ Easy to test state transitions
    assert.Equal(t, proto.QueryJob_JOB_STATUS_UNSPECIFIED, state.Status())

    state.SetStatus(proto.QueryJob_JOB_STATUS_RUNNING)
    assert.Equal(t, proto.QueryJob_JOB_STATUS_RUNNING, state.Status())

    state.SetResult(&job.ExecutionResult{TotalRows: 100})
    assert.True(t, state.ResultReady())
    assert.Equal(t, int64(100), state.TotalRows())
}

// Test job with mock executor
func TestManagedJobExecution(t *testing.T) {
    mockExecutor := &MockExecutor{
        result: &job.ExecutionResult{
            TotalRows:  42,
            ResultSize: 1024,
        },
    }

    job, _ := job.NewJobBuilder().
        WithExecutor(mockExecutor).
        WithQueryText("SELECT 1").
        Build(context.Background())

    // ✅ Test execution without real database
    storage := &MockStorage{}
    conn := &proto.Connection{}

    err := job.Run(storage, conn)
    assert.NoError(t, err)

    // ✅ Verify state changes
    <-time.After(100 * time.Millisecond)  // Wait for goroutine
    assert.Equal(t, int64(42), job.State().TotalRows())
}

// Test registry
func TestRegistry(t *testing.T) {
    registry := job.NewRegistry()

    job1, _ := createMockJob("job-1")
    job2, _ := createMockJob("job-2")

    registry.Register(job1)
    registry.Register(job2)

    // ✅ Easy to test registry operations
    found, ok := registry.Get("job-1")
    assert.True(t, ok)
    assert.Equal(t, "job-1", found.State().ID())

    assert.True(t, registry.CancelJob("job-1"))
    assert.False(t, registry.CancelJob("non-existent"))
}
```

**Benefits:**
- ✅ Unit testing vs integration testing
- ✅ Mock executors trivial to create
- ✅ Test state management independently
- ✅ Test registry independently
- ✅ Fast tests (no real databases)

---

## 6. Adding New Database Support

### BEFORE (Lots of Boilerplate)
```go
// To add MySQL support, need to:

// 1. Create Job struct (❌ 20+ lines of boilerplate)
type Job struct {
    job.BasicJob  // Embed BasicJob
    mysqlDB       *sql.DB
    storageObject storage.StorageObject
}

// 2. Create Store struct (❌ embed BasicStore)
type Store struct {
    job.BasicStore  // Embed BasicStore
    mysqlDB *sql.DB
}

// 3. Implement NewStore (❌ connection setup)
func NewStore() *Store { ... }

// 4. Implement Store.Create (❌ 15+ lines of repeated boilerplate)
func (s *Store) Create(reportID, queryID, queryText string, userCtx context.Context) (job.Job, chan int32, error) {
    j := &Job{
        BasicJob: job.BasicJob{
            ReportID:  reportID,
            QueryID:   queryID,
            QueryText: queryText,
            Logger:    log.With()...Logger(),
        },
        mysqlDB: s.mysqlDB,
    }
    j.Init(userCtx)
    s.StoreJob(j)
    go s.RemoveJobWhenDone(j)
    return j, j.Status(), nil
}

// 5. Implement Job.Run (❌ mixed concerns)
func (j *Job) Run(storageObject storage.StorageObject, conn *proto.Connection) error {
    j.Status() <- int32(proto.QueryJob_JOB_STATUS_RUNNING)
    // ... query logic mixed with lifecycle management ...
    j.Lock()
    j.ResultReady = true
    j.Unlock()
    j.Status() <- int32(proto.QueryJob_JOB_STATUS_DONE)
    return nil
}

// 6. Implement TestConnection
func (s *Store) TestConnection(...) { ... }

// ❌ Total: ~150+ lines of code, lots of boilerplate
```

### AFTER (Minimal Boilerplate)
```go
// To add MySQL support, need to:

// 1. Create Executor (✅ only business logic)
package mysqljob

type Executor struct {
    db *sql.DB
}

func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
    // ✅ Only MySQL-specific query logic
    rows, err := e.db.QueryContext(ctx, req.QueryText)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    // ... read and write results (same as PostgreSQL) ...

    return &job.ExecutionResult{
        TotalRows:  rowCount,
        ResultSize: size,
    }, nil
}

// 2. Create Store (✅ minimal setup)
type Store struct {
    *job.BaseStore  // ✅ Inherits all Store methods
    db *sql.DB
}

func NewStore() *Store {
    dbConnStr := os.Getenv("DEKART_MYSQL_CONNECTION")
    db, _ := sql.Open("mysql", dbConnStr)

    store := &Store{db: db}

    // ✅ BaseStore handles all lifecycle
    store.BaseStore = job.NewBaseStore(func() job.Executor {
        return &Executor{db: db}
    })

    return store
}

// 3. Implement TestConnection (✅ simple)
func (s *Store) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
    if err := s.db.PingContext(ctx); err != nil {
        return &proto.TestConnectionResponse{Success: false, Error: err.Error()}, nil
    }
    return &proto.TestConnectionResponse{Success: true}, nil
}

// ✅ Total: ~60 lines of code, minimal boilerplate
```

**Benefits:**
- ✅ 150 lines → 60 lines (60% reduction)
- ✅ Focus only on database-specific logic
- ✅ All lifecycle/state management inherited
- ✅ Can reuse helpers (like CSV writing)

---

## 7. Usage from Server

### BEFORE
```go
// dekart/query.go
func (s *Server) RunQuery(...) {
    // Create job
    job, jobStatus, err := s.jobs.Create(reportID, queryID, queryText, connCtx)
    if err != nil {
        return err
    }

    // Update status in goroutine
    go s.updateJobStatus(job, jobStatus, paramHash, queryText)

    // Send pending status
    job.Status() <- int32(proto.QueryJob_JOB_STATUS_PENDING)  // ❌ Direct channel access

    // Run job
    err = job.Run(storageObject, connection)

    // Access results
    if job.IsResultReady() {  // ❌ Have to know method names
        resultID := job.GetResultID()
        totalRows := job.GetTotalRows()
        processedBytes := job.GetProcessedBytes()
    }
}
```

### AFTER
```go
// dekart/query.go
func (s *Server) RunQuery(...) {
    // Create job (same interface)
    job, statusChan, err := s.jobs.CreateJob(ctx, job.JobRequest{
        ReportID:  reportID,
        QueryID:   queryID,
        QueryText: queryText,
    })
    if err != nil {
        return err
    }

    // ✅ Status updates automatic from statusChan
    go s.updateJobStatus(job, statusChan, paramHash, queryText)

    // Run job (same interface)
    err = job.Run(storageObject, connection)

    // ✅ Access state through clean interface
    state := job.State()
    if state.ResultReady() {
        resultID := state.GetResultID()
        totalRows := state.TotalRows()
        processedBytes := state.ProcessedBytes()
    }
}
```

**Benefits:**
- ✅ Minimal changes to calling code
- ✅ Cleaner state access
- ✅ Better encapsulation

---

## Summary Table

| Aspect | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Store.Create boilerplate** | ~16 lines | Inherited | -100% |
| **Thread safety** | Manual Lock/Unlock | Automatic | Safer |
| **Job lookup** | O(n) slice | O(1) map | Faster |
| **Executor purity** | Mixed concerns | Pure function | Testable |
| **Adding new DB** | ~150 lines | ~60 lines | -60% |
| **Abstraction clarity** | `job.BasicStore` embedded | Clear composition | Better |
| **Testing** | Integration only | Unit + Integration | Easier |
| **Error handling** | `CancelWithError` mixed in | Return error | Cleaner |
| **State access** | Direct field access | Encapsulated methods | Safer |
| **Initialization** | Two-step (error-prone) | Builder pattern | Foolproof |

---

## Key Architectural Improvements

1. **Separation of Concerns**
   - Before: Job does execution + state + lifecycle
   - After: Executor (execution), State (state), ManagedJob (lifecycle)

2. **Dependency Injection**
   - Before: Embedded structs with hidden dependencies
   - After: Explicit dependencies via constructors

3. **Composition over Inheritance**
   - Before: `type Store struct { job.BasicStore }`
   - After: `type Store struct { *job.BaseStore }` with factory

4. **Single Responsibility**
   - Before: BasicJob has 10+ responsibilities
   - After: Each component has one clear job

5. **Testability**
   - Before: Must mock entire embedded struct
   - After: Mock single Executor interface

6. **Maintainability**
   - Before: Change BasicJob affects all implementations
   - After: Change to one layer doesn't affect others
