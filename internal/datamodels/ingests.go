package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"github.com/jmoiron/sqlx"
)

type DAStagingTable struct {
	DatasetID string
	Id        string
	TableName string
}

type Ingest struct {
	IngestID        int64          `db:"ingest_id"`
	DatatableID     int            `db:"datatable_id"`
	UserID          sql.NullString `db:"user_id"`
	IngestName      sql.NullString `db:"ingest_name"`
	IngestScope     sql.NullString `db:"ingest_scope"`
	BranchID        sql.NullInt32  `db:"branch_id"`
	IngestMerged    bool           `db:"ingest_merged"`
	IngestDeleted   bool           `db:"ingest_deleted"`
	DeleteTimestamp time.Time      `db:"delete_timestamp"`
	AllData         sql.NullBool   `db:"all_data"`
	Measures        map[int]*MeasureForMap
}

func (q *Queries) StagingTable(ctx context.Context, datasetId int, datatable *DADatatable, branchID *string, scope *DAQueryScopeInput, measures []string, userID *string, allData bool, numPartitions int) (*DAStagingTable, error) {
	// Validate the query
	var validationUserId *string
	if !allData {
		validationUserId = userID
		if validationUserId == nil {
			allData = true
		}
	}
	validation, err := q.validateUpdate(ctx, validateUpdateParams{
		datasetId:     datasetId,
		datatableName: datatable.TableName,
		measures:      measures,
		scope:         scope,
		version:       nil,
		userID:        validationUserId,
	})
	if err != nil {
		fmt.Printf("ERROR: datamodels/ingest.go/StagingTable: [validateQuery] %s", err)
		return nil, err
	}

	if len(validation.measuresData) == 0 {
		log.Printf("ERROR datamodels/ingests.go/StagingTable: No measures are found for this request.")
		return nil, errors.New("No measures are found for this request.")
	}

	dimLevels, err := q.GetDADimensionLevels(ctx, datasetId, nil, nil, &datatable.Id, true)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/StagingTable: %s [GetDADimensionLevels]", err)
		return nil, err
	}
	if len(dimLevels) == 0 {
		log.Printf("ERROR datamodels/ingests.go/StagingTable: No dimension levels are found for this request or no default_keyset is defined.")
		return nil, errors.New("No dimension levels are found for this request or no default_keyset is defined.")
	}

	jsonScope, err := q.queryScopeToFullScopeJson(ctx, datasetId, userID, scope, true)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/StagingTable: %s [QueryScopeInputToAccessViewScopeString]", err)
		return nil, err
	}

	// Apply all changes in a single transaction
	var ingestId int64
	err = q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
		ingest_params := insertIngestParams{
			DatatableID: datatable.Id,
			UserID:      userID,
			BranchID:    branchID,
			Name:        nil,
			Scope:       jsonScope,
			Merged:      false,
			Deleted:     false,
			AllData:     allData,
		}
		ingestId, err = q.insertIngest(ctx, tx, datasetId, &ingest_params)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/StagingTable: %s [insertIngest]", err)
			return err
		}

		err = q.insertIngestMeasures(ctx, tx, datasetId, ingestId, validation.measuresData)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/StagingTable: %s [insertIngestMeasures]", err)
			return err
		}

		err = q.createIngest(ctx, datasetId, tx, ingestId, dimLevels, validation.measuresData, numPartitions)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/StagingTable: %s [createIngest]", err)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	result := DAStagingTable{
		DatasetID: fmt.Sprintf("%d", datasetId),
		Id:        strconv.FormatInt(ingestId, 10),
		TableName: "ingest_" + strconv.FormatInt(ingestId, 10),
	}
	return &result, nil
}

const getIngestsByID = `
SELECT *
FROM ingests
WHERE ingest_id IN (?)
`

const getIngestsMeasuresByID = `
SELECT  ingest_id,
        m.measure_id,
		m.measure_column_name,
		mt.measure_type_column_name,
		mt.measure_type_id
FROM ingest_measures
	NATURAL JOIN
	measures m
	NATURAL JOIN
	measure_types mt
WHERE ingest_id IN (?)
`

func (q *Queries) GetIngestsbyID(ctx context.Context, datasetId int, ingestIds []int64) (map[int64]*Ingest, error) {
	query, inargs, err := sqlx.In(getIngestsByID, ingestIds)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [In(getIngestsByID)]", err)
		return map[int64]*Ingest{}, err
	}

	db := q.GetDatasetDB(datasetId)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, inargs...)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [QueryxContext(getIngestsByID)]", err)
		return map[int64]*Ingest{}, err
	}
	result := map[int64]*Ingest{}
	for rows.Next() {
		ingest := Ingest{
			Measures: map[int]*MeasureForMap{},
		}

		err := rows.StructScan(&ingest)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [StructScan(getIngestsByID)]", err)
			return map[int64]*Ingest{}, err
		}
		result[ingest.IngestID] = &ingest
	}

	query, inargs, err = sqlx.In(getIngestsMeasuresByID, ingestIds)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [In(getIngestsMeasuresByID)]", err)
		return map[int64]*Ingest{}, err
	}

	query = db.Rebind(query)
	rows, err = db.QueryxContext(ctx, query, inargs...)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [QueryxContext(getIngestsMeasuresByID)]", err)
		return map[int64]*Ingest{}, err
	}
	for rows.Next() {
		var ingestID int64
		measure := MeasureForMap{}

		err = rows.Scan(
			&ingestID,
			&measure.MeasureID,
			&measure.MeasureColumnName,
			&measure.MeasureDataType,
			&measure.MeasureDataTypeID,
		)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/GetIngestsbyID: %s [StructScan(getIngestsMeasuresByID)]", err)
			return map[int64]*Ingest{}, err
		}
		result[ingestID].Measures[measure.MeasureID] = &measure
	}

	return result, nil
}

func (q *Queries) ProcessIngestTables(ctx context.Context, datasetId int, branchID *int32, userID *string, ingestsMap map[int64]*Ingest, version *Version) error {
	var branch *DABranch = nil
	branchLabel := "live"
	if branchID != nil {
		var err error
		branch, err = q.getDABranchByID(ctx, datasetId, branchID)
		if err != nil {
			return err
		}
		branchLabel = branch.ID
	}
	log.Printf("datamodels/ingests.go/ProcessIngestTables: Processing ingest tables for version=%d, branch=%s, details=%#4v", version.VersionID, branchLabel, ingestsMap)

	// Get a write lock
	mutex := q.GetUpdateLock(ctx, datasetId, branchID, LockSettings{
		Expiry:     time.Duration(4) * time.Hour,
		Tries:      2160,
		RetryDelay: 10 * time.Second,
	})
	if err := mutex.Lock(ctx); err != nil {
		log.Printf("ERROR: datamodels/ingests.go/ProcessIngestTables: %s [Acquire Lock]", err)
		return err
	}
	defer func() {
		// Drop temporary ingest_{INGEST_ID}_dimension_data tables
		q.dropTmpIngestDimData(ctx, datasetId, ingestsMap)

		// Release lock
		ok, err := mutex.Unlock(ctx)
		if err != nil {
			log.Printf("ERROR: datamodels/ingests.go/ProcessIngestTables: %s [Unlock Failed]", err)
		} else if !ok {
			log.Printf("ERROR: datamodels/ingests.go/ProcessIngestTables [Unlock Failed]")
		}
	}()
	log.Printf("datamodels/ingests.go/ProcessIngestTables: Lock Acquired")

	// Version => set status="IN_PROGRESS" and started_timestamp=NOW()
	err := q.startVersion(ctx, datasetId, version.VersionID)
	if err != nil {
		return err
	}
	log.Printf("datamodels/ingests.go/ProcessIngestTables: The version=%d has been marked in progress", version.VersionID)

	// Set the KeepAlive running
	done := make(chan bool)
	go func(ctx context.Context, datasetId int, versionID int64) {
		ticker := time.NewTicker(config.VERSION_KEEP_ALIVE_INTERVAL * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case _ = <-ticker.C:
				if err := q.keepAliveVersion(ctx, datasetId, versionID); err != nil {
					log.Printf("ERROR: datamodels/ingests.go/ProcessIngestTables: %s [update keepAliveVersion]", err)
				}
			}
		}
	}(ctx, datasetId, version.VersionID)

	// Create temporary ingest_{INGEST_ID}_dimension_data table
	err = q.createTmpIngestDimData(ctx, datasetId, ingestsMap)
	if err != nil {
		return err
	}

	// Apply all changes to base
	log.Printf("datamodels/ingests.go/ProcessIngestTables: Applying ingest changes to base")
	ingestIds, err := q.applyIngestChanges(ctx, datasetId, branch, ingestsMap)
	if err != nil {
		return err
	}

	// Apply changes to cache layers
	log.Printf("datamodels/ingests.go/ProcessIngestTables: Applying ingest changes to cache layers")
	err = q.applyIngestChangesCacheLayers(ctx, datasetId, userID, branch, ingestsMap)
	if err != nil {
		return err
	}

	if branch != nil {
		// Update branch-datatable relationship
		for _, ingestTable := range ingestsMap {
			for _, measure := range ingestTable.Measures {
				err = q.addBranchDatatable(ctx, datasetId, branch.id, measure.DatatableID, measure.MeasureDataTypeID)
				if err != nil {
					return err
				}
			}
		}
	}

	// Mark ingest tables as merged
	log.Printf("datamodels/ingests.go/ProcessIngestTables: Updating ingest table (marking them as merged)")
	err = q.setIngestAsMerged(ctx, datasetId, ingestIds)
	if err != nil {
		return err
	}
	done <- true

	// Mark version as applied
	time.Sleep(1 * time.Second) // Add a slow delay so the version time does not match with the changes
	err = q.applyVersion(ctx, datasetId, version.VersionID)
	if err != nil {
		return err
	}
	log.Printf("The version=%d has been marked as applied", version.VersionID)

	return nil
}

func (q *Queries) DeleteIngests(ctx context.Context, datasets []*Dataset) error {
	for _, dataset := range datasets {
		datasetId, err := strconv.Atoi(dataset.ID)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/DeleteIngests: %s", err)
			return err
		}
		// Get Ingests with delete_timestamp < NOW
		ingests, err := q.getIngestsToDelete(ctx, datasetId)
		if err != nil {
			log.Printf("ERROR datamodels/ingests.go/DeleteIngests: Getting ingests from dataset=%d %s", datasetId, err)
			continue
		}

		for _, ingest_id := range ingests {
			q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
				log.Printf("INFO: Deleting ingest=%d from dataset=%d", ingest_id, datasetId)

				// Drop the ingest_x table
				dropQuery := `DROP TABLE ingest_` + strconv.FormatInt(ingest_id, 10)
				_, err = tx.ExecContext(ctx, dropQuery)
				if err != nil {
					log.Printf("ERROR datamodels/ingests.go/DeleteIngests: [dropping table ingest_%d from dataset=%d] %s", ingest_id, datasetId, err)
				}

				// Update the ingest status to 'deleted'
				updateQuery := `UPDATE ingests
								SET ingest_deleted = 1
								WHERE ingest_id = ?;`
				_, err = tx.ExecContext(ctx, updateQuery, ingest_id)
				if err != nil {
					log.Printf("ERROR datamodels/ingests.go/DeleteIngests: [marking ingest=%d from dataset=%d as deleted ] %s", ingest_id, datasetId, err)
				}

				log.Printf("INFO: The ingest=%d from dataset=%d was successfully deleted.", ingest_id, datasetId)
				return nil
			})
		}
	}
	return nil
}

// Private methods ------------------------------------------------------------

func (q *Queries) getIngestsToDelete(ctx context.Context, datasetId int) ([]int64, error) {
	query := `SELECT ingest_id
			  FROM ingests
			  WHERE delete_timestamp < NOW(6)
			  AND ingest_deleted = 0`
	db := q.GetDatasetDB(datasetId)
	rows, err := db.QueryxContext(ctx, query)
	if err != nil {
		return nil, err
	}
	result := []int64{}
	for rows.Next() {
		var ingest_id int64
		err := rows.Scan(&ingest_id)
		if err != nil {
			return nil, err
		}
		result = append(result, ingest_id)
	}

	return result, nil
}

func (q *Queries) createIngest(ctx context.Context, datasetID int, tx *sqlx.Tx, ingestID int64, dimLevels []*DADimensionLevel, measures []*DAMeasure, numPartitions int) error {
	table_name := "ingest_" + strconv.FormatInt(ingestID, 10)
	create_table_query := "CREATE TABLE IF NOT EXISTS " + table_name + " ("

	dimLevelsIds := []string{}
	for _, dimLevel := range dimLevels {
		create_table_query += dimLevel.ColumnName + "_id" + " INT UNSIGNED NOT NULL,"
		dimLevelsIds = append(dimLevelsIds, dimLevel.ColumnName+"_id")
	}
	for _, measure := range measures {
		create_table_query += measure.ColumnName + " " + measure.ColumnType + ","
	}
	if numPartitions > 0 {
		create_table_query += "partition_id TINYINT UNSIGNED,"
	}
	create_table_query += "PRIMARY KEY (" + strings.Join(dimLevelsIds, ", ")
	if numPartitions > 0 {
		create_table_query += ", partition_id"
	}
	create_table_query += "),"
	for _, dimLevel := range dimLevels {
		create_table_query += "KEY " + dimLevel.ColumnName + "_id_idx (" + dimLevel.ColumnName + "_id) USING BTREE,"
	}
	create_table_query = create_table_query[0 : len(create_table_query)-1]
	create_table_query += ")"

	partitionsQuery := ""
	if numPartitions > 0 {
		partitionsQuery += "PARTITION BY LIST (partition_id) ("
		for i := 0; i < numPartitions; i++ {
			partitionsQuery += "PARTITION p" + strconv.Itoa(i) + " VALUES IN (" + strconv.Itoa(i+1) + "),"
		}
		partitionsQuery = partitionsQuery[0 : len(partitionsQuery)-1]
		partitionsQuery += ")"
	}
	create_table_query += partitionsQuery

	_, err := tx.ExecContext(ctx, create_table_query)
	if err != nil {
		log.Printf("ERROR: datamodels/ingests.go/createIngest: %s", err)
		return err
	}

	return nil
}

type insertIngestParams struct {
	DatatableID string
	UserID      *string
	BranchID    *string
	Name        *string
	Scope       string
	Merged      bool
	Deleted     bool
	AllData     bool
}

func (q *Queries) insertIngest(ctx context.Context, tx *sqlx.Tx, datasetID int, args *insertIngestParams) (int64, error) {
	query := `INSERT INTO ingests (
                  datatable_id,
				  user_id, 
				  ingest_name,
				  ingest_scope,
				  branch_id,
				  ingest_merged,
				  ingest_deleted,
				  delete_timestamp,
				  all_data
			  ) VALUES (
                ?,?,?,?,?,?,?,?,?
			  )`
	result, err := tx.ExecContext(ctx, query,
		args.DatatableID,
		args.UserID,
		args.Name,
		args.Scope,
		args.BranchID,
		args.Merged,
		args.Deleted,
		time.Now().Add(time.Hour*config.INGEST_DELETE_TIMESTAMP),
		args.AllData,
	)
	if err != nil {
		log.Printf("ERROR: datamodels/ingests.go/insertIngest: %s", err)
		return 0, err
	}

	ingestId, err := result.LastInsertId()
	if err != nil {
		log.Printf("ERROR: datamodels/ingests.go/insertIngest: %s", err)
		return 0, err
	}

	ingestIdInt := int64(ingestId)
	return ingestIdInt, nil
}

func (q *Queries) insertIngestMeasures(ctx context.Context, tx *sqlx.Tx, datasetID int, ingest_id int64, measures []*DAMeasure) error {
	query := `INSERT INTO ingest_measures (
		          ingest_id,
				  measure_id
			  ) VALUES `

	args := make([]interface{}, 0)
	for _, measure := range measures {
		query += "(?, ?),"
		args = append(args, ingest_id, measure.ID)
	}
	query = query[0 : len(query)-1]

	_, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		log.Printf("ERROR: datamodels/ingests.go/insertIngestMeasures: %s", err)
		return err
	}

	return nil
}

const createTmpIngestQuery = `CREATE OR REPLACE TABLE ingest_%d_dimension_data ENGINE=MEMORY AS
SELECT * FROM ingest_%d NATURAL JOIN dt%d_dimension_data %s`

func (q *Queries) createTmpIngestDimData(ctx context.Context, datasetId int, ingestsMap map[int64]*Ingest) error {
	db := q.GetDatasetDB(datasetId)

	for ingestID, ingestTable := range ingestsMap {
		// Get scope from ingest table
		ingestScope := ""
		if ingestTable.IngestScope.Valid {
			ingestScope, err := fullScopeJsonToWhereConditions([]byte(ingestTable.IngestScope.String))
			if err != nil {
				log.Printf("ERROR: datamodels/ingests.go/createTmpIngestDimData: %s [applyIngestChanges]", err)
				return err
			}
			if len(ingestScope) > 0 {
				ingestScope = " WHERE " + ingestScope
			}
		}

		// Create tmp ingest_%d_dimension_data table
		query := fmt.Sprintf(createTmpIngestQuery, ingestID, ingestID, ingestTable.DatatableID, ingestScope)

		t1 := time.Now()
		log.Printf("INFO: datamodels/ingests.go/createTmpIngestDimData: Running query=%s", query)
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			log.Printf("ERROR: datamodels/ingests.go/createTmpIngestDimData: %s [CREATE TABLE ingest_%d_dimension_data]", err, ingestID)
			return err
		}
		log.Printf("INFO: datamodels/ingests.go/createTmpIngestDimData: Finished running query=%s (elapsed-time=%s)",
			query, time.Now().Sub(t1))
	}
	return nil
}

const dropTmpIngestQuery = `DROP TABLE ingest_%d_dimension_data`

func (q *Queries) dropTmpIngestDimData(ctx context.Context, datasetId int, ingestsMap map[int64]*Ingest) {
	db := q.GetDatasetDB(datasetId)

	for ingestID, _ := range ingestsMap {
		query := fmt.Sprintf(dropTmpIngestQuery, ingestID)

		log.Printf("INFO: datamodels/ingests.go/dropTmpIngestDimData: Running query=%s", query)
		_, err := db.ExecContext(ctx, query)
		if err != nil {
			log.Printf("ERROR: datamodels/ingests.go/dropTmpIngestDimData: %s [DROP TABLE ingest_%d_dimension_data]", err, ingestID)
		}
	}
}

const updateIngestQuery = `INSERT INTO dt%d_base_measures_%s
SELECT base_id, %d AS measure_id, %s AS value
FROM ingest_%d_dimension_data
ON DUPLICATE KEY UPDATE value=VALUE(value)`

const updateIngestBranchQuery = `INSERT INTO dt%d_base_measures_%s_branches
SELECT base_id, %d AS measure_id, %s AS value, %d AS branch_id
FROM ingest_%d_dimension_data
ON DUPLICATE KEY UPDATE value=VALUE(value)`

func (q *Queries) applyIngestChanges(ctx context.Context, datasetId int, branch *DABranch, ingestsMap map[int64]*Ingest) ([]int64, error) {
	ingestIds := []int64{}
	queries := []string{}

	for ingestID, ingestTable := range ingestsMap {
		ingestIds = append(ingestIds, ingestID)

		// Build the list of queries to be executed
		for measureID, measure := range ingestTable.Measures {
			var query string

			if branch == nil {
				query = fmt.Sprintf(updateIngestQuery, ingestTable.DatatableID, measure.MeasureDataType,
					measureID, measure.MeasureColumnName, ingestID)
			} else {
				query = fmt.Sprintf(updateIngestBranchQuery, ingestTable.DatatableID, measure.MeasureDataType,
					measureID, measure.MeasureColumnName, branch.id, ingestID)
			}
			queries = append(queries, query)
		}
	}

	// Run the queries in bulks
	err := q.RunQueryBulk(ctx, datasetId, queries, config.INGEST_UPDATE_BULK_SIZE, false)
	if err != nil {
		return nil, err
	}
	return ingestIds, nil
}

func (q *Queries) setIngestAsMerged(ctx context.Context, datasetId int, ingestIds []int64) error {
	// Update ingest tables (ingest_merged=true)
	const updateIngestsByID = `
	UPDATE ingests
	SET ingest_merged = TRUE
	WHERE ingest_id IN (?)
	`

	updateQuery, inargs, err := sqlx.In(updateIngestsByID, ingestIds)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/setIngestAsMerged: %s [In(updateIngestsByID)]", err)
		return err
	}

	db := q.GetDatasetDB(datasetId)
	updateQuery = db.Rebind(updateQuery)
	_, err = db.ExecContext(ctx, updateQuery, inargs...)
	if err != nil {
		log.Printf("ERROR datamodels/ingests.go/setIngestAsMerged: %s [updateIngestsByID]", err)
		return err
	}
	return nil
}
