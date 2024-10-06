package dekart

import (
	"context"
	"database/sql"
	"dekart/src/proto"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

func rowsToQueries(queryRows *sql.Rows) ([]*proto.Query, error) {
	queries := make([]*proto.Query, 0)
	for queryRows.Next() {
		var queryText string
		query := proto.Query{}
		var createdAt time.Time
		var updatedAt time.Time
		if err := queryRows.Scan(
			&query.Id,
			&queryText,
			&query.JobStatus,
			&query.JobResultId,
			&query.JobError,
			&query.JobDuration,
			&query.TotalRows,
			&query.BytesProcessed,
			&query.ResultSize,
			&createdAt,
			&updatedAt,
			&query.QuerySource,
			&query.QuerySourceId,
		); err != nil {
			log.Fatal().Err(err).Send()
		}

		switch query.QuerySource {
		case proto.Query_QUERY_SOURCE_UNSPECIFIED:
			err := fmt.Errorf("unknown query source query id=%s", query.Id)
			log.Err(err).Send()
			return nil, err
		case proto.Query_QUERY_SOURCE_INLINE:
			query.QueryText = queryText
		}
		query.CreatedAt = createdAt.Unix()
		query.UpdatedAt = updatedAt.Unix()
		switch query.JobStatus {
		case proto.Query_JOB_STATUS_UNSPECIFIED:
			query.JobDuration = 0
		case proto.Query_JOB_STATUS_DONE:
			if query.JobResultId != "" {
				query.JobDuration = 0
			}
		}
		queries = append(queries, &query)
	}
	return queries, nil
}

func (s Server) getQueries(ctx context.Context, datasets []*proto.Dataset) ([]*proto.Query, error) {
	queryIds := make([]string, 0)
	for _, dataset := range datasets {
		if dataset.QueryId != "" {
			queryIds = append(queryIds, dataset.QueryId)
		}
	}
	if len(queryIds) > 0 {
		// Quote each queryId and join them with commas
		quotedQueryIds := make([]string, len(queryIds))
		for i, id := range queryIds {
			quotedQueryIds[i] = "'" + id + "'"
		}
		queryIdsStr := strings.Join(quotedQueryIds, ",")
		var queryRows *sql.Rows
		var err error
		if IsSqlite() {
			queryRows, err = s.db.QueryContext(ctx,
				`select
				id,
				query_text,
				job_status,
				case when job_result_id is null then '' else cast(job_result_id as VARCHAR) end as job_result_id,
				case when job_error is null then '' else job_error end as job_error,
				case
					when job_started is null
					then 0
					else CAST((strftime('%s', CURRENT_TIMESTAMP)  - strftime('%s', job_started))*1000 as BIGINT)
				end as job_duration,
				total_rows,
				bytes_processed,
				result_size,
				created_at,
				updated_at,
				query_source,
				query_source_id
			from queries where id IN (`+queryIdsStr+`) order by created_at asc`,
			)
		} else {
			queryRows, err = s.db.QueryContext(ctx,
				`select
				id,
				query_text,
				job_status,
				case when job_result_id is null then '' else cast(job_result_id as VARCHAR) end as job_result_id,
				case when job_error is null then '' else job_error end as job_error,
				case
					when job_started is null
					then 0
					else CAST((extract('epoch' from CURRENT_TIMESTAMP)  - extract('epoch' from job_started))*1000 as BIGINT)
				end as job_duration,
				total_rows,
				bytes_processed,
				result_size,
				created_at,
				updated_at,
				query_source,
				query_source_id
			from queries where id = ANY($1) order by created_at asc`,
				pq.Array(queryIds),
			)
		}
		if err != nil {
			log.Fatal().Err(err).Msgf("select from queries failed, ids: %s", queryIdsStr)
		}
		defer queryRows.Close()
		return rowsToQueries(queryRows)
	}
	return make([]*proto.Query, 0), nil
}

// getQueriesLegacy is used to get queries which are not associated with a dataset
func (s Server) getQueriesLegacy(ctx context.Context, reportID string) ([]*proto.Query, error) {
	queryRows, err := s.db.QueryContext(ctx,
		`select
			id,
			query_text,
			job_status,
			case when job_result_id is null then '' else cast(job_result_id as VARCHAR) end as job_result_id,
			case when job_error is null then '' else job_error end as job_error,
			case
				when job_started is null
				then 0
				else CAST((extract('epoch' from CURRENT_TIMESTAMP)  - extract('epoch' from job_started))*1000 as BIGINT)
			end as job_duration,
			total_rows,
			bytes_processed,
			result_size,
			created_at,
			updated_at,
			query_source,
			query_source_id
		from queries
		where report_id=$1
		order by created_at asc`,
		reportID,
	)
	if err != nil {
		log.Err(err).Str("reportID", reportID).Msg("select from queries failed")
		return nil, err
	}
	defer queryRows.Close()
	return rowsToQueries(queryRows)
}
