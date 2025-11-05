package athenajob

import (
	"context"
	"dekart/src/server/job"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

// Executor implements job.Executor for AWS Athena.
type Executor struct {
	session        *session.Session
	outputLocation string
}

// NewExecutor creates a new Athena executor.
func NewExecutor(session *session.Session, outputLocation string) *Executor {
	return &Executor{
		session:        session,
		outputLocation: outputLocation,
	}
}

// Execute runs an Athena query and copies results from S3.
func (e *Executor) Execute(ctx context.Context, req job.ExecutionRequest) (*job.ExecutionResult, error) {
	client := athena.New(e.session)

	// Determine workgroup
	var athenaWorkgroup *string
	// Note: Workgroup configuration would come from connection or environment
	// For now, using default (primary) workgroup

	// Start query execution
	queryString := req.QueryText
	out, err := client.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
		QueryString: &queryString,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: &e.outputLocation,
		},
		WorkGroup: athenaWorkgroup,
	})
	if err != nil {
		req.Logger.Error().Err(err).Msg("Error starting query execution")
		return nil, err
	}

	queryExecutionId := *out.QueryExecutionId

	// Poll for query completion
	queryExecution, err := e.pollQueryExecution(ctx, client, queryExecutionId)
	if err != nil {
		return nil, err
	}

	// Copy results from S3
	err = req.Storage.CopyFromS3(ctx, *queryExecution.ResultConfiguration.OutputLocation)
	if err != nil {
		return nil, err
	}

	// Get result size
	size, err := req.Storage.GetSize(ctx)
	if err != nil {
		return nil, err
	}

	var processedBytes int64
	if queryExecution.Statistics != nil && queryExecution.Statistics.DataScannedInBytes != nil {
		processedBytes = *queryExecution.Statistics.DataScannedInBytes
	}

	return &job.ExecutionResult{
		TotalRows:      0, // Athena doesn't provide this easily
		ProcessedBytes: processedBytes,
		ResultSize:     *size,
	}, nil
}

// pollQueryExecution polls Athena for query completion.
func (e *Executor) pollQueryExecution(ctx context.Context, client *athena.Athena, queryExecutionId string) (*athena.QueryExecution, error) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	input := &athena.GetQueryExecutionInput{
		QueryExecutionId: &queryExecutionId,
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			out, err := client.GetQueryExecutionWithContext(ctx, input)
			if err != nil {
				return nil, err
			}

			status := *out.QueryExecution.Status.State
			switch status {
			case "RUNNING", "QUEUED":
				continue
			case "SUCCEEDED":
				return out.QueryExecution, nil
			default:
				reason := "unknown reason"
				if out.QueryExecution.Status.StateChangeReason != nil {
					reason = *out.QueryExecution.Status.StateChangeReason
				}
				return nil, fmt.Errorf("query failed. status: %s; reason: %s", status, reason)
			}
		}
	}
}
