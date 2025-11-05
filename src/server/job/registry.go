package job

import (
	"context"
	"sync"
)

// Registry manages active jobs with O(1) lookups using a map.
// It replaces BasicStore's slice-based storage.
type Registry struct {
	mu   sync.RWMutex
	jobs map[string]*ManagedJob
}

// NewRegistry creates a new job registry.
func NewRegistry() *Registry {
	return &Registry{
		jobs: make(map[string]*ManagedJob),
	}
}

// Register adds a job to the registry and starts automatic cleanup.
func (r *Registry) Register(job *ManagedJob) {
	r.mu.Lock()
	r.jobs[job.GetID()] = job
	r.mu.Unlock()

	// Automatic cleanup when job context is done
	go func() {
		<-job.GetCtx().Done()
		r.Unregister(job.GetID())
	}()
}

// Get retrieves a job by ID.
func (r *Registry) Get(jobID string) (*ManagedJob, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, found := r.jobs[jobID]
	return job, found
}

// Unregister removes a job from the registry.
func (r *Registry) Unregister(jobID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.jobs, jobID)
}

// CancelJob cancels a specific job by ID.
func (r *Registry) CancelJob(jobID string) bool {
	job, found := r.Get(jobID)
	if !found {
		return false
	}
	job.Cancel()
	return true
}

// CancelAll cancels all active jobs.
func (r *Registry) CancelAll(ctx context.Context) {
	// Get all jobs while holding read lock
	r.mu.RLock()
	jobs := make([]*ManagedJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		jobs = append(jobs, job)
	}
	r.mu.RUnlock()

	// Cancel jobs without holding lock
	for _, job := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			job.Cancel()
		}
	}
}

// List returns all active jobs.
func (r *Registry) List() []*ManagedJob {
	r.mu.RLock()
	defer r.mu.RUnlock()

	jobs := make([]*ManagedJob, 0, len(r.jobs))
	for _, job := range r.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

// Count returns the number of active jobs.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.jobs)
}
