package athenajob

import (
	"context"
	"dekart/src/proto"
	"dekart/src/server/job"
	"dekart/src/server/storage"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/rs/zerolog/log"
)

// StoreV2 implements the new job.Store interface for AWS Athena.
type StoreV2 struct {
	*job.BaseStore
	session        *session.Session
	outputLocation string
}

// NewStoreV2 creates a new Athena store using the refactored architecture.
func NewStoreV2(storage storage.Storage) *StoreV2 {
	conf := aws.NewConfig().
		WithMaxRetries(3).
		WithS3ForcePathStyle(true)

	outputLocation := os.Getenv("DEKART_ATHENA_S3_OUTPUT_LOCATION")
	if outputLocation == "" {
		log.Fatal().Msgf("athena data connection requires DEKART_ATHENA_S3_OUTPUT_LOCATION")
	}

	session := session.Must(session.NewSession(conf))

	store := &StoreV2{
		session:        session,
		outputLocation: fmt.Sprintf("s3://%s", outputLocation),
	}

	// Initialize BaseStore with executor factory
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		return NewExecutor(session, store.outputLocation)
	})

	return store
}

// TestConnection tests the Athena connection.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	// For Athena, we can just check if we can create a session
	// More thorough testing would require running a simple query
	if s.session == nil {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   "failed to create AWS session",
		}, nil
	}

	return &proto.TestConnectionResponse{Success: true}, nil
}
