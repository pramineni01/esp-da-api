package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"

	_ "github.com/go-sql-driver/mysql"
)

type DBRepo interface {
	/* Global Methods */
	GetDADB() *sqlx.DB
	GetDatasetDB(datasetID int) *sqlx.DB

	/* Schema Resolvers*/

	// Dataset methods
	LoadDatasets([]*Dataset) map[int]*DADataset
	GetDADataset(ctx context.Context, id int) (*DADataset, error)

	// Datatable methods
	GetDAVersion(context.Context, int, *string, *string) (*DAVersion, error)
	GetDADatatable(context.Context, int, *string, *string, *string) (*DADatatable, error)

	// Dimension methods
	GetDADimensionLevels(context.Context, int, *string, *string, *string, bool) ([]*DADimensionLevel, error)
	GetDADimensionAndDimLevelsByDatatable(context.Context, int, int) (map[string][]string, error)

	// Measure methods
	GetDAMeasures(context.Context, int, *bool) ([]DADatasetMeasure, error)
	GetDADatatableMeasures(context.Context, int, *string) ([]*DAMeasure, error)
	GetMapAggrMeasuresByColumnName(context.Context, int, []string, bool) (map[string]*AggrMeasureForMap, map[int]*MeasureForMap, error)

	// Versions methods
	GetVersion(context.Context, int, GetVersionParams) (*Version, error)
	CreateVersion(context.Context, int, CreateVersionParams) (*Version, error)
	ApplyVersionError(context.Context, int, int64) error
	ProcessErroneousVersions(context.Context, []*Dataset) error

	// Ingest methods
	StagingTable(context.Context, int, *DADatatable, *string, *DAQueryScopeInput, []string, *string, bool, int) (*DAStagingTable, error)
	GetIngestsbyID(context.Context, int, []int64) (map[int64]*Ingest, error)
	ProcessIngestTables(context.Context, int, *int32, *string, map[int64]*Ingest, *Version) error
	DeleteIngests(context.Context, []*Dataset) error

	// Data access methods
	PeformDataQuery(context.Context, *DAQueryInput) (*DAQueryResultConnection, error)
	PeformDataSearchQuery(context.Context, *DADimMembersQueryInput) (*DAQueryResultConnection, error)
	CreateView(context.Context, int, CreateDataviewParams) (*DASQLQuery, error)
	GetDataframeQueries(context.Context, GetDataframeQueriesParams) (*DADataframeQueries, error)
	GetTotalRows(context.Context, *DAPageInfo) (*int, error)

	// Data update methods
	ApplyDataUpdates(context.Context, UpdateDataInput) (string, error)

	// Cache Layers
	RebuildCacheLayers(context.Context, int) error
	AddCacheLayerForDTAndDimLvls(ctx context.Context, datasetId int, dataTableId int, dimLvls []int) error

	// Utils
	GetTimeApplied(context.Context, int, *string, *string) (*time.Time, error)

	/* Entity Resolvers*/
	FindDADatasetByID(context.Context, FindDADatasetByIDParams) (*DADataset, error)
	FindDADatatableByDatasetIDAndID(context.Context, FindDADatatableByDatasetIDAndIDParams) (*DADatatable, error)
	FindDADimensionByDatasetIDAndColumnName(context.Context, FindDADimensionByDatasetIDAndColumnNameParams) (*DADimension, error)
	FindDADimensionLevelByDatasetIDAndColumnName(context.Context, FindDADimensionLevelByDatasetIDAndColumnNameParams) (*DADimensionLevel, error)
	FindDADimensionMemberByDatasetIDAndID(context.Context, FindDADimensionMemberByDatasetIDAndIDParams) (*DADimensionMember, error)
	FindDAMeasureByDatasetIDAndColumnName(context.Context, FindDAMeasureByDatasetIDAndColumnNameParams) (*DAMeasure, error)
	FindDAAggregatedMeasureByDatasetIDAndColumnName(context.Context, FindDAAggregatedMeasureByDatasetIDAndColumnNameParams) (*DAAggregatedMeasure, error)
	FindDABranchByDatasetIDAndID(context.Context, FindDABranchByDatasetIDAndIDParams) (*DABranch, error)
}

type RepoSvc struct {
	*QueriesDA
	*Queries
}

func NewRepo(db_da *sqlx.DB) RepoSvc {
	return RepoSvc{
		QueriesDA: &QueriesDA{db: db_da},
	}
}

func (r *RepoSvc) NewQueries(dbs map[int]*sqlx.DB, dss map[int]*DADataset, redisConn *redis.UniversalClient) {
	r.Queries = &Queries{
		dbs:      dbs,
		datasets: dss,
	}
	if redisConn != nil {
		r.Queries.rdb = *redisConn
	}
}

type QueriesDA struct {
	db *sqlx.DB
}

type Queries struct {
	dbs      map[int]*sqlx.DB
	rdb      redis.UniversalClient
	datasets map[int]*DADataset
}

func (q *QueriesDA) GetDADB() *sqlx.DB {
	return q.db
}

func (q *Queries) GetDatasetDB(datasetID int) *sqlx.DB {
	return q.dbs[datasetID]
}

func (q *Queries) RunTransaction(ctx context.Context, datasetId int, logic func(*sqlx.Tx) error) error {
	// Start a transaction
	tx, err := q.GetDatasetDB(datasetId).BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		log.Printf("ERROR: datamodels/db_svc.go/RunTransaction: %s", err)
		return err
	}

	// Run the logic
	err = logic(tx)
	if err != nil {
		// Rollback
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			log.Printf("ERROR: datamodels/db_svc.go/RunTransaction: %s [unable to rollback]", rollbackErr)
		}
		return err
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("ERROR: datamodels/db_svc.go/RunTransaction: %s [Commit transaction]", err)
		return err
	}

	return nil
}

func (q *Queries) RunQueryBulk(ctx context.Context, datasetId int, queries []string, bulkSize int, waitOnError bool) (err error) {
	if len(queries) <= 0 {
		return nil
	}

	// Channel to signal the completion of queries
	ch := make(chan error, bulkSize)
	runningQueries := 0

	// Process query handler
	processQuery := func() {
		// Pop the first prefix
		var query string
		query, queries = queries[0], queries[1:]

		// Process the query
		runningQueries++
		go func(query string) {
			t1 := time.Now()
			log.Printf("INFO: datamodels/db_svc.go/RunQueryBulk: Start running query=%s", query)
			ch <- q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
				result, err := tx.ExecContext(ctx, query)
				if err != nil {
					log.Printf("ERROR: datamodels/db_svc.go/RunQueryBulk: While running query=%s [%s]", query, err)
					return err
				}
				rowsAffected, _ := result.RowsAffected()
				log.Printf("INFO: datamodels/db_svc.go/RunQueryBulk: Finished running query=%s (affected-rows=%d, elapsed-time=%s)",
					query, rowsAffected, time.Now().Sub(t1))
				return nil
			})
		}(query)
	}

	// Run query handler
	runQueries := func() bool {
		log.Printf("INFO: datamodels/db_svc.go/RunQueryBulk: ***** Bulk stats: pending-queries=%d, queries-in-progress=%d", len(queries), runningQueries)
		if len(queries) == 0 {
			// There are no queries
			return false
		}
		for i := runningQueries; i < bulkSize && len(queries) > 0; i++ {
			processQuery()
		}
		return true
	}

	runQueries()
	errorsReturn := []string{}
	for {
		select {
		case queryErr := <-ch:
			runningQueries--
			if waitOnError {
				if queryErr != nil {
					errorsReturn = append(errorsReturn, queryErr.Error())
				} else if !runQueries() {
					// There are no more queries to execute
				}
				if runningQueries <= 0 {
					if len(errorsReturn) == 0 {
						return nil
					}
					return errors.New(strings.Join(errorsReturn, "\n"))
				}
			} else {
				if queryErr != nil {
					// There was an error break the loop
					return queryErr
				}
				if !runQueries() {
					// There are no more queries to execute
					if runningQueries <= 0 {
						return nil
					}
				}
			}
		case <-ctx.Done():
			log.Printf("WARNING: datamodels/db_svc.go/RunQueryBulk: Stopping (%s).", ctx.Err())
			return ctx.Err()
		}
	}
}
