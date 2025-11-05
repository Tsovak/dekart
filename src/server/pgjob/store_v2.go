package pgjob

import (
	"context"
	"database/sql"
	"dekart/src/proto"
	"dekart/src/server/job"
	"os"

	_ "github.com/lib/pq" // postgres driver
	"github.com/rs/zerolog/log"
)

// StoreV2 implements the new job.Store interface for PostgreSQL.
type StoreV2 struct {
	*job.BaseStore
	db *sql.DB
}

// NewStoreV2 creates a new PostgreSQL store using the refactored architecture.
func NewStoreV2() *StoreV2 {
	dbConnStr := os.Getenv("DEKART_POSTGRES_DATASOURCE_CONNECTION")
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to postgres")
	}

	store := &StoreV2{db: db}

	// Initialize BaseStore with executor factory
	store.BaseStore = job.NewBaseStore(func() job.Executor {
		return NewExecutor(db)
	})

	return store
}

// TestConnection tests the PostgreSQL connection.
func (s *StoreV2) TestConnection(ctx context.Context, req *proto.TestConnectionRequest) (*proto.TestConnectionResponse, error) {
	if err := s.db.PingContext(ctx); err != nil {
		return &proto.TestConnectionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &proto.TestConnectionResponse{Success: true}, nil
}

// Close closes the database connection.
func (s *StoreV2) Close() error {
	return s.db.Close()
}
