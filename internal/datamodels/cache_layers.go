package datamodels

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"text/template"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"github.com/gobuffalo/packr/v2"
	"github.com/jmoiron/sqlx"
)

type CacheLayer struct {
	ID                 int  `db:"cache_layer_id"`
	DatatableID        int  `db:"datatable_id"`
	CacheLayerRowCount int  `db:"cache_layer_row_count"`
	Enabled            bool `db:"enabled"`
}

func (q *Queries) RebuildCacheLayers(ctx context.Context, datasetId int) error {
	// Get dim levels by cache layer
	cacheLayerDimLevelsMap, err := q.getDimLevelsByCacheLayer(ctx, datasetId, config.HARDCODED_DATATABLE_ID, false)
	if err != nil {
		return err
	}

	// Process each cache layer
	db := q.GetDatasetDB(datasetId)
	for cacheLayerId, dimLevels := range cacheLayerDimLevelsMap {
		log.Printf("Start processing cache-layer=%d", cacheLayerId)

		//Get cache layer measures
		measuresByType, err := q.GetDTCacheLayerMeasuresByType(ctx, datasetId, config.HARDCODED_DATATABLE_ID, nil)
		if err != nil {
			return err
		}

		// Tables to disable/enable indexes
		tableNames := []string{
			fmt.Sprintf("cache_layer_%d_dimension_data", cacheLayerId),
			fmt.Sprintf("cache_layer_%d_dimension_data_locale_%d", cacheLayerId, config.DEFAULT_LOCALE_ID),
			fmt.Sprintf("cache_layer_%d_dimension_data_search_locale_%d", cacheLayerId, config.DEFAULT_LOCALE_ID),
		}

		deleteDeprecatedQueries := []string{}

		// Iterate on the different measure types
		for measureType, _ := range measuresByType {
			table := fmt.Sprintf("cache_layer_%d_measures_%s", cacheLayerId, measureType)
			tableNames = append(tableNames, table)

			// Delete deprecated measures
			queryDeprecatedMeasure := `
			DELETE FROM cache
			USING %s cache
				INNER JOIN cache_layer_%d_dimension_data_deprecated deprecated
				ON ( cache.cache_layer_%d_id = deprecated.cache_layer_%d_id );
			`
			queryDeprecatedMeasure = fmt.Sprintf(queryDeprecatedMeasure, table, cacheLayerId, cacheLayerId, cacheLayerId)
			deleteDeprecatedQueries = append(deleteDeprecatedQueries, queryDeprecatedMeasure)
		}

		tablesConstraints, err := q.CacheLayerConstraints(ctx, datasetId, tableNames)
		if err != nil {
			log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: %s [getting tables indexes]", err)
			return err
		}

		defer func() {
			// Enable indexes
			err = q.enableIndexes(ctx, datasetId, tablesConstraints)
			if err != nil {
				log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: %s [enabling indexes]", err)
				return
			} else {
				log.Printf("INFO: Enabled indexes for cache-layer=%d", cacheLayerId)
			}
		}()

		// Disable indexes
		err = q.disableIndexes(ctx, datasetId, tablesConstraints)
		if err != nil {
			log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: %s [disabling indexes]", err)
			return err
		} else {
			log.Printf("INFO: Disabled indexes for cache-layer=%d", cacheLayerId)
		}

		dropTablesTmp := []string{}

		// Aux vars
		dimLevelClauses := []string{}
		dimLevelFullClauses := []string{}
		dimLevelValuesSql := ""
		dimLevelFullValuesSql := ""
		for _, dimLevel := range dimLevels {
			dimLevelClauses = append(dimLevelClauses, dimLevel+"_id")
			dimLevelFullClauses = append(dimLevelFullClauses, dimLevel+"_id", dimLevel+"_name", dimLevel+"_description", dimLevel+"_external_id")
			dimLevelValuesSql += fmt.Sprintf("%s_id=VALUES(%s_id), ", dimLevel, dimLevel)
			dimLevelFullValuesSql += fmt.Sprintf("%s_id=VALUES(%s_id), ", dimLevel, dimLevel)
			dimLevelFullValuesSql += fmt.Sprintf("%s_name=VALUES(%s_name), ", dimLevel, dimLevel)
			dimLevelFullValuesSql += fmt.Sprintf("%s_description=VALUES(%s_description), ", dimLevel, dimLevel)
			dimLevelFullValuesSql += fmt.Sprintf("%s_external_id=VALUES(%s_external_id), ", dimLevel, dimLevel)
		}
		dimLevelSql := strings.Join(dimLevelClauses, ", ")
		dimLevelFullSql := strings.Join(dimLevelFullClauses, ", ")
		if len(dimLevelValuesSql) > 0 {
			dimLevelValuesSql = dimLevelValuesSql[:len(dimLevelValuesSql)-2]
			dimLevelFullValuesSql = dimLevelFullValuesSql[:len(dimLevelFullValuesSql)-2]
		}

		// Create dimension_data_tmp
		queryCreateTableTmp := `
		CREATE OR REPLACE TABLE cache_layer_%d_dimension_data_tmp ENGINE=MEMORY AS
		(
			SELECT MIN(base_id) AS cache_layer_%d_id, %s, COUNT(*) AS count
			FROM dt%d_dimension_data_locale_%d
			GROUP BY %s
		)`
		queryCreateTableTmp = fmt.Sprintf(queryCreateTableTmp, cacheLayerId, cacheLayerId, dimLevelFullSql,
			config.HARDCODED_DATATABLE_ID, config.DEFAULT_LOCALE_ID, dimLevelSql)
		dropTablesTmp = append(dropTablesTmp, fmt.Sprintf("DROP TABLE cache_layer_%d_dimension_data_tmp", cacheLayerId))

		insertQueries := []string{}
		// Update dimension_data
		query := `
		INSERT INTO cache_layer_%d_dimension_data (cache_layer_%d_id, %s, count)
		SELECT cache_layer_%d_id, %s, count
		FROM cache_layer_%d_dimension_data_tmp
		ON DUPLICATE KEY UPDATE
		  %s 
		`
		query = fmt.Sprintf(query, cacheLayerId, cacheLayerId, dimLevelSql, cacheLayerId, dimLevelSql, cacheLayerId, dimLevelValuesSql+", count=VALUES(count)")
		insertQueries = append(insertQueries, query)

		// Update dimension_data_locale
		query = `
		INSERT INTO cache_layer_%d_dimension_data_locale_%d (cache_layer_%d_id, %s)
		SELECT cache_layer_%d_id, %s
		FROM cache_layer_%d_dimension_data_tmp
		ON DUPLICATE KEY UPDATE
		  %s 
		`
		query = fmt.Sprintf(query, cacheLayerId, config.DEFAULT_LOCALE_ID, cacheLayerId, dimLevelFullSql, cacheLayerId, dimLevelFullSql, cacheLayerId, dimLevelFullValuesSql)
		insertQueries = append(insertQueries, query)

		// Update dimension_data_search
		query = `
		INSERT INTO cache_layer_%d_dimension_data_search_locale_%d (cache_layer_%d_id, %s)
		SELECT cache_layer_%d_id, %s
		FROM cache_layer_%d_dimension_data_tmp
		ON DUPLICATE KEY UPDATE
		  %s 
		`
		query = fmt.Sprintf(query, cacheLayerId, config.DEFAULT_LOCALE_ID, cacheLayerId, dimLevelFullSql, cacheLayerId, dimLevelFullSql, cacheLayerId, dimLevelFullValuesSql)
		insertQueries = append(insertQueries, query)

		// Updates measures
		updateQueriesMeasures, err := q.generateRebuildCacheLayerMeasureQueries(config.HARDCODED_DATATABLE_ID, cacheLayerId, dimLevelSql, measuresByType)
		if err != nil {
			log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: [generateRebuildCacheLayerMeasureQueries] %s", err)
			return err
		}
		insertQueries = append(insertQueries, updateQueriesMeasures...)

		// Create dimension_data_tmp
		queryCreateTableDeprecated := `
		CREATE OR REPLACE TABLE cache_layer_%d_dimension_data_deprecated ENGINE=MEMORY AS
		(
			SELECT cache_layer_%d_id FROM cache_layer_%d_dimension_data
			EXCEPT
			SELECT cache_layer_%d_id FROM cache_layer_%d_dimension_data_tmp
		)`
		queryCreateTableDeprecated = fmt.Sprintf(queryCreateTableDeprecated, cacheLayerId, cacheLayerId, cacheLayerId, cacheLayerId, cacheLayerId)
		dropTablesTmp = append(dropTablesTmp, fmt.Sprintf("DROP TABLE cache_layer_%d_dimension_data_deprecated", cacheLayerId))

		// Delete deprecated dimension_data
		query = `
		DELETE FROM cache
		USING cache_layer_%d_dimension_data cache
			  INNER JOIN cache_layer_%d_dimension_data_deprecated deprecated
			   ON ( cache.cache_layer_%d_id = deprecated.cache_layer_%d_id );
		`
		query = fmt.Sprintf(query, cacheLayerId, cacheLayerId, cacheLayerId, cacheLayerId)
		deleteDeprecatedQueries = append(deleteDeprecatedQueries, query)

		// Delete deprecated dimension_data_locale
		query = `
		DELETE FROM cache
		USING cache_layer_%d_dimension_data_locale_%d cache
			  INNER JOIN cache_layer_%d_dimension_data_deprecated deprecated
			   ON ( cache.cache_layer_%d_id = deprecated.cache_layer_%d_id );
		`
		query = fmt.Sprintf(query, cacheLayerId, config.DEFAULT_LOCALE_ID, cacheLayerId, cacheLayerId, cacheLayerId)
		deleteDeprecatedQueries = append(deleteDeprecatedQueries, query)

		// Delete deprecated dimension_data_search
		query = `
		DELETE FROM cache
		USING cache_layer_%d_dimension_data_search_locale_%d cache
			  INNER JOIN cache_layer_%d_dimension_data_deprecated deprecated
			   ON ( cache.cache_layer_%d_id = deprecated.cache_layer_%d_id );
		`
		query = fmt.Sprintf(query, cacheLayerId, config.DEFAULT_LOCALE_ID, cacheLayerId, cacheLayerId, cacheLayerId)
		deleteDeprecatedQueries = append(deleteDeprecatedQueries, query)

		// Execute all queries
		log.Printf("Executing the following query:\n%s\n", queryCreateTableTmp)
		_, err = db.ExecContext(ctx, queryCreateTableTmp)
		if err != nil {
			log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: %s [CREATE OR REPLACE TABLE cache_layer_dimension_data_tmp]", err)
			return err
		}

		// Run the insert (dim data, locale, search) and update measures in bulks
		err = q.RunQueryBulk(ctx, datasetId, insertQueries, config.DATA_UPDATE_BULK_SIZE, true)
		if err != nil {
			return err
		}

		log.Printf("Executing the following query:\n%s\n", queryCreateTableDeprecated)
		_, err = db.ExecContext(ctx, queryCreateTableDeprecated)
		if err != nil {
			log.Printf("ERROR datamodels/cache_layers.go/RebuildCacheLayers: %s [CREATE OR REPLACE TABLE cache_layer_dimension_data_deprecated]", err)
			return err
		}

		// Run the delete deprecated in bulks
		err = q.RunQueryBulk(ctx, datasetId, deleteDeprecatedQueries, config.DATA_UPDATE_BULK_SIZE, true)
		if err != nil {
			return err
		}

		// Run the drop tables in bulks
		err = q.RunQueryBulk(ctx, datasetId, dropTablesTmp, config.DATA_UPDATE_BULK_SIZE, true)
		if err != nil {
			return err
		}

		log.Printf("datamodels/cache_layers.go/RebuildCacheLayers: Finished processing cache-layer=%d", cacheLayerId)
	}

	return nil
}

func (q *Queries) disableIndexes(ctx context.Context, datasetId int, indexes map[string]*CacheLayerConstraint) error {
	err := q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
		var query string

		for tableName, indexes := range indexes {
			log.Printf("datamodels/cache_layers.go/disableIndexes: Disabling indexes for table %s", tableName)

			// Disable indexes
			for index := range indexes.Indexes {
				log.Printf("datamodels/cache_layers.go/disableIndexes: dropping Index=%s", index)
				query = fmt.Sprintf("ALTER TABLE %s DROP INDEX %s", tableName, index)
				_, err := tx.ExecContext(ctx, query)
				if err != nil {
					log.Printf("ERROR datamodels/cache_layers.go/disableIndexes: %s", err)
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (q *Queries) enableIndexes(ctx context.Context, datasetId int, indexes map[string]*CacheLayerConstraint) error {
	err := q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
		var query string

		for tableName, indexes := range indexes {
			log.Printf("datamodels/cache_layers.go/enableIndexes: Enabling indexes for table %s", tableName)

			// Enable indexes
			for indexName, indexColumns := range indexes.Indexes {
				query = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (", indexName, tableName)
				indexFields := strings.Join(indexColumns, ",")
				query += indexFields + ")"
				log.Printf("datamodels/cache_layers.go/enableIndexes: Enabling index %s", indexName)
				log.Printf("Executing the following query:\n%s\n", query)
				_, err := tx.ExecContext(ctx, query)
				if err != nil {
					log.Printf("ERROR datamodels/cache_layers.go/enableIndexes: [enabling index %s]=%s", indexName, err)
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

type getMatchingCacheLayerParams struct {
	DatasetID       int
	DatatableID     int
	DimensionLevels []string
}

func (q *Queries) getMatchingCacheLayer(ctx context.Context, args getMatchingCacheLayerParams) (*int, error) {
	cacheLayersFilteredByDimLevels, err := q.getCacheLayersFilteredByDimLevels(ctx, args.DatasetID, args.DatatableID, args.DimensionLevels)
	if err != nil {
		log.Printf("ERROR: datamodels/cache_layers.go/getMatchingCacheLayer: %s", err)
		return nil, err
	}

	if _, ok := cacheLayersFilteredByDimLevels[config.HARDCODED_CACHE_LAYER_ID]; ok {
		cacheLayerId := config.HARDCODED_CACHE_LAYER_ID
		return &cacheLayerId, nil
	}

	return nil, nil
}

func (q *Queries) getCacheLayersFilteredByDimLevels(ctx context.Context, datasetId int, datatableId int, dimensionLevels []string) (map[int]int, error) {
	cacheLayers, err := q.getDimLevelsByCacheLayer(ctx, datasetId, datatableId, true)
	if err != nil {
		log.Printf("ERROR: datamodels/cache_layers.go/getCacheLayersFilteredByDimLevels: %s", err)
		return nil, err
	}
	matchingCacheLayers := make(map[int]int)
	var queriedDimLevel_found bool

	for cacheLayerId, cacheLayerDimLevels := range cacheLayers {
		for _, queriedDimLevel := range dimensionLevels {
			queriedDimLevel_found = false
			for _, cacheLayerDimLevel := range cacheLayerDimLevels {
				if queriedDimLevel == cacheLayerDimLevel {
					queriedDimLevel_found = true
					break
				}
			}
			if !queriedDimLevel_found {
				break
			}
		}
		if !queriedDimLevel_found {
			continue
		}
		matchingCacheLayers[cacheLayerId] = len(cacheLayerDimLevels)
	}

	return matchingCacheLayers, nil
}

// Private methods ------------------------------------------------------------

func (q *Queries) getCacheLayers(ctx context.Context, datasetId int, datatableId *int) ([]*CacheLayer, error) {
	query := `SELECT cl.cache_layer_id,
					 cl.datatable_id,
					 cl.cache_layer_row_count,
					 cl.enabled
			  FROM cache_layers cl
			  WHERE enabled = 1`

	var args []interface{}
	if datatableId != nil {
		query += ` AND datatable_id = ?`
		args = append(args, datatableId)
	}

	db := q.GetDatasetDB(datasetId)
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Printf("ERROR: datamodels/cache_layers.go/getCacheLayers: %s", err)
		return nil, err
	}

	cacheLayers := []*CacheLayer{}
	for rows.Next() {
		var cacheLayer CacheLayer
		err := rows.StructScan(&cacheLayer)
		if err != nil {
			log.Printf("ERROR: datamodels/cache_layers.go/getCacheLayers: %s", err)
			return nil, err
		}
		cacheLayers = append(cacheLayers, &cacheLayer)
	}

	return cacheLayers, nil
}

func (q *Queries) getDimLevelsByCacheLayer(ctx context.Context, datasetId int, datatableID int, enabled bool) (map[int][]string, error) {
	db := q.GetDatasetDB(datasetId)

	// Get dim levels by cache layer
	query := `SELECT cache_layer_id, dimension_level_column_name
			FROM cache_layers
				NATURAL JOIN cache_layer_dimension_levels
				NATURAL JOIN dimension_levels
			WHERE datatable_id = ?`

	if enabled {
		query += " AND cache_layers.enabled = 1"
	}

	rows, err := db.QueryxContext(ctx, query, datatableID)
	if err != nil {
		log.Printf("ERROR: datamodels/cache_layers.go/getDimLevelsByCacheLayer: %s", err)
		return nil, err
	}
	cacheLayerDimLevelsMap := map[int][]string{}
	for rows.Next() {
		var cacheLayerId int
		var dimLevelColumn string

		err = rows.Scan(
			&cacheLayerId,
			&dimLevelColumn,
		)
		if err != nil {
			log.Printf("ERROR: datamodels/cache_layers.go/getDimLevelsByCacheLayer: %s", err)
			return nil, err
		}
		cacheLayerDimLevelsMap[cacheLayerId] = append(cacheLayerDimLevelsMap[cacheLayerId], dimLevelColumn)
	}

	return cacheLayerDimLevelsMap, nil
}

const updateCacheLayerMeasureQuery = `INSERT INTO cache_layer_%d_measures_%s
  SELECT MIN(m.base_id) AS cache_layer_%d_id,
         %d AS measure_aggregation_id,
		 %s( CAST( m.value AS %s ) ) AS value
  FROM dt%d_dimension_data
	   NATURAL JOIN dt%d_base_measures_%s m
  WHERE m.measure_id = %d
  GROUP BY %s
  ON DUPLICATE KEY UPDATE value=VALUE(value)`

func (q *Queries) generateRebuildCacheLayerMeasureQueries(datatableID int, cacheLayerID int, dimLevelSql string, measuresByType map[string][]*CacheLayerMeasure) ([]string, error) {
	if datatableID != config.HARDCODED_DATATABLE_ID {
		return nil, nil
	}

	queries := []string{}
	for measureType, cacheLayerMeasures := range measuresByType {
		for _, cacheLayerMeasure := range cacheLayerMeasures {
			query := fmt.Sprintf(updateCacheLayerMeasureQuery, cacheLayerID, measureType,
				cacheLayerID, cacheLayerMeasure.AggrMeasureId,
				cacheLayerMeasure.AggrMeasureType, cacheLayerMeasure.MeasureCastType, datatableID,
				datatableID, measureType, cacheLayerMeasure.MeasureID, dimLevelSql)

			queries = append(queries, query)
		}
	}

	return queries, nil
}

const updateCacheLayerIngestQuery = `INSERT INTO cache_layer_%d_measures_%s
  WITH
  scope AS (
	SELECT DISTINCT %s
	FROM ingest_%d_dimension_data
  )
  SELECT MIN(base_id) AS cache_layer_%d_id,
         %d AS measure_aggregation_id,
         %s( CAST(value AS %s) ) AS value
  FROM dt%d_dimension_data
	   NATURAL JOIN scope
	   NATURAL JOIN dt%d_base_measures_%s
  WHERE measure_id = %d
  GROUP BY %s
ON DUPLICATE KEY UPDATE value=VALUE(value)`

const updateCacheLayerIngestBranchQuery = `INSERT INTO cache_layer_%d_measures_%s_branches
  WITH
  scope AS (
	SELECT DISTINCT %s
	FROM ingest_%d_dimension_data
  )
  SELECT MIN( COALESCE(b.base_id, m.base_id) ) AS cache_layer_%d_id,
         %d AS measure_aggregation_id,
		 %s( CAST( COALESCE(b.value, m.value) AS %s ) ) AS value,
		 %d AS branch_id
  FROM dt%d_dimension_data dd
	   NATURAL JOIN scope
	   LEFT JOIN dt%d_base_measures_%s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
	     ON (dd.base_id = m.base_id AND m.measure_id = %d)
	   LEFT JOIN dt%d_base_measures_%s_branches b
	     ON (dd.base_id = b.base_id AND b.measure_id = %d AND b.branch_id = %d)
  GROUP BY %s
ON DUPLICATE KEY UPDATE value=VALUE(value)`

func (q *Queries) applyIngestChangesCacheLayers(ctx context.Context, datasetId int, userID *string, branch *DABranch, ingestsMap map[int64]*Ingest) error {
	// Get cache layer measures
	measuresIDs := []int{}
	for _, ingestTable := range ingestsMap {
		if ingestTable.DatatableID != config.HARDCODED_DATATABLE_ID {
			continue
		}
		for measureID, _ := range ingestTable.Measures {
			measuresIDs = append(measuresIDs, measureID)
		}
	}
	if len(measuresIDs) == 0 {
		fmt.Printf("INFO: datamodels/cache_layers.go/applyIngestChangesCacheLayers: There's nothing to update")
		return nil
	}
	measuresByType, err := q.GetDTCacheLayerMeasuresByType(ctx, datasetId, config.HARDCODED_DATATABLE_ID, measuresIDs)
	if err != nil {
		return err
	}

	// Get dim levels by cache layer
	cacheLayerDimLevelsMap, err := q.getDimLevelsByCacheLayer(ctx, datasetId, config.HARDCODED_DATATABLE_ID, true)
	if err != nil {
		return err
	}
	dimLevelClauses := []string{}
	for _, dimLevel := range cacheLayerDimLevelsMap[config.HARDCODED_CACHE_LAYER_ID] {
		dimLevelClauses = append(dimLevelClauses, dimLevel+"_id")
	}
	dimLevelSql := strings.Join(dimLevelClauses, ", ")

	// Construct queries
	queries := []string{}
	for ingestID, ingestTable := range ingestsMap {
		if ingestTable.DatatableID != config.HARDCODED_DATATABLE_ID {
			continue
		}

		for measureType, cacheLayerMeasures := range measuresByType {
			for _, cacheLayerMeasure := range cacheLayerMeasures {
				var query string
				if branch == nil {
					query = fmt.Sprintf(updateCacheLayerIngestQuery, config.HARDCODED_CACHE_LAYER_ID, measureType, dimLevelSql, ingestID,
						config.HARDCODED_CACHE_LAYER_ID, cacheLayerMeasure.AggrMeasureId, cacheLayerMeasure.AggrMeasureType,
						cacheLayerMeasure.MeasureCastType, config.HARDCODED_DATATABLE_ID, config.HARDCODED_DATATABLE_ID,
						measureType, cacheLayerMeasure.MeasureID, dimLevelSql)
				} else {
					branchTime := branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)
					query = fmt.Sprintf(updateCacheLayerIngestBranchQuery, config.HARDCODED_CACHE_LAYER_ID, measureType, dimLevelSql, ingestID,
						config.HARDCODED_CACHE_LAYER_ID, cacheLayerMeasure.AggrMeasureId, cacheLayerMeasure.AggrMeasureType,
						cacheLayerMeasure.MeasureCastType, branch.id, config.HARDCODED_DATATABLE_ID, config.HARDCODED_DATATABLE_ID,
						measureType, branchTime, cacheLayerMeasure.MeasureID, config.HARDCODED_DATATABLE_ID, measureType,
						cacheLayerMeasure.MeasureID, branch.id, dimLevelSql)
				}
				queries = append(queries, query)
			}
		}
	}

	// Run the queries in bulks
	err = q.RunQueryBulk(ctx, datasetId, queries, config.INGEST_UPDATE_BULK_SIZE, false)
	if err != nil {
		return err
	}

	return nil
}

const updateCacheLayerQuery = `INSERT INTO cache_layer_%d_measures_%s
  WITH
  scope AS (
	SELECT DISTINCT %s
	FROM dt%d_dimension_data
	WHERE 1=1 %s
  )
  SELECT MIN(m.base_id) AS cache_layer_%d_id,
         %d AS measure_aggregation_id,
		 %s( CAST( m.value AS %s ) ) AS value
  FROM dt%d_dimension_data
	   NATURAL JOIN scope
	   NATURAL JOIN dt%d_base_measures_%s m 
  WHERE m.measure_id = %d
  GROUP BY %s
ON DUPLICATE KEY UPDATE value=VALUE(value)`

const updateCacheLayerBranchQuery = `INSERT INTO cache_layer_%d_measures_%s_branches
  WITH
  scope AS (
	SELECT DISTINCT %s
	FROM dt%d_dimension_data FOR SYSTEM_TIME AS OF TIMESTAMP'%s'
	WHERE 1=1 %s
  )
  SELECT MIN( COALESCE(b.base_id, m.base_id) ) AS cache_layer_%d_id,
         %d AS measure_aggregation_id,
		 %s( CAST( COALESCE(b.value, m.value) AS %s ) ) AS value,
		 %d AS branch_id
  FROM dt%d_dimension_data FOR SYSTEM_TIME AS OF TIMESTAMP'%s' dd
	   NATURAL JOIN scope
	   LEFT JOIN dt%d_base_measures_%s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
	     ON (dd.base_id = m.base_id AND m.measure_id = %d)
	   LEFT JOIN dt%d_base_measures_%s_branches b
		ON (dd.base_id = b.base_id AND b.measure_id = %d AND b.branch_id = %d)
  GROUP BY %s
ON DUPLICATE KEY UPDATE value=VALUE(value)`

func (q *Queries) getCacheLayerUpdateQueries(ctx context.Context, datatable *DADatatable, branch *DABranch, measureId int, scopeSql string) ([]string, error) {
	if datatable.id != config.HARDCODED_DATATABLE_ID {
		return nil, nil
	}

	measuresByType, err := q.GetDTCacheLayerMeasuresByType(ctx, datatable.datasetId, config.HARDCODED_DATATABLE_ID, []int{measureId})
	if err != nil {
		return nil, err
	}

	// Get dim levels by cache layer
	cacheLayerDimLevelsMap, err := q.getDimLevelsByCacheLayer(ctx, datatable.datasetId, config.HARDCODED_DATATABLE_ID, true)
	if err != nil {
		return nil, err
	}
	dimLevelClauses := []string{}
	for _, dimLevel := range cacheLayerDimLevelsMap[config.HARDCODED_CACHE_LAYER_ID] {
		dimLevelClauses = append(dimLevelClauses, dimLevel+"_id")
	}
	dimLevelSql := strings.Join(dimLevelClauses, ", ")

	queries := []string{}
	if branch != nil {
		// Update branch
		branchTime := branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)

		for measureType, cacheLayerMeasures := range measuresByType {
			for _, cacheLayerMeasure := range cacheLayerMeasures {
				query := fmt.Sprintf(updateCacheLayerBranchQuery, config.HARDCODED_CACHE_LAYER_ID, measureType, dimLevelSql,
					config.HARDCODED_DATATABLE_ID, branchTime, scopeSql, config.HARDCODED_CACHE_LAYER_ID, cacheLayerMeasure.AggrMeasureId,
					cacheLayerMeasure.AggrMeasureType, cacheLayerMeasure.MeasureCastType, branch.id, config.HARDCODED_DATATABLE_ID, branchTime,
					config.HARDCODED_DATATABLE_ID, measureType, branchTime,
					cacheLayerMeasure.MeasureID, config.HARDCODED_DATATABLE_ID, measureType,
					cacheLayerMeasure.MeasureID, branch.id, dimLevelSql)

				queries = append(queries, query)
			}
		}
	} else {
		// Update main branch
		for measureType, cacheLayerMeasures := range measuresByType {
			for _, cacheLayerMeasure := range cacheLayerMeasures {
				query := fmt.Sprintf(updateCacheLayerQuery, config.HARDCODED_CACHE_LAYER_ID, measureType, dimLevelSql,
					config.HARDCODED_DATATABLE_ID, scopeSql, config.HARDCODED_CACHE_LAYER_ID, cacheLayerMeasure.AggrMeasureId,
					cacheLayerMeasure.AggrMeasureType, cacheLayerMeasure.MeasureCastType, config.HARDCODED_DATATABLE_ID,
					config.HARDCODED_DATATABLE_ID, measureType, cacheLayerMeasure.MeasureID, dimLevelSql)

				queries = append(queries, query)
			}
		}
	}

	return queries, nil
}

func (q *Queries) AddCacheLayerForDTAndDimLvls(ctx context.Context, datasetId int, datatableId int, dimLvlIds []int) error {
	db := q.GetDatasetDB(datasetId)

	/// validations
	// 	validate datatable_id is one of datatables.datatable_id
	var partition int
	err := db.QueryRowContext(ctx, "SELECT num_partitions FROM datatables where datatable_id=?", datatableId).Scan(&partition)
	switch {
	case err == sql.ErrNoRows:
		log.Println("No datatable with id: ", datatableId)
		return err
	case err != nil:
		log.Println("query error: ", err)
		return err
	}

	//  validate dimension level ids are among of dimension_levels.dimension_level_id
	dimLvls, err := q.getDimLvls(ctx, db, datatableId, dimLvlIds)
	if err != nil {
		return err
	}

	locales, err := q.getLocales(ctx, db)
	if err != nil {
		return err
	}

	measureTypes, err := q.getMeasureTypes(ctx, db, datatableId)
	if err != nil {
		return err
	}

	return q.RunTransaction(ctx, datasetId, func(tx *sqlx.Tx) error {
		// Take the datatable_id, query the max(cache_layer_id)+1 into "cl" in cache_layers.
		// Insert new row into cache_layers (this will the newly added cache layer)
		const qryInsertCacheLyr = `INSERT INTO cache_layers
					  	(datatable_id, cache_layer_row_count, enabled, start_timestamp, end_timestamp)
						VALUES(?, 0, 1, NULL, NULL)
						RETURNING cache_layer_id`

		var clId int
		err = db.QueryRowContext(ctx, qryInsertCacheLyr, datatableId).Scan(&clId)
		if err != nil {
			log.Fatalf("ERROR datamodels/cache_layers.go/AddCacheLayerForDTAndDimLvls: %s", err)
			return err
		}

		// map new cache layer to dimension levels
		var qryInsertCacheToDimLvlMappings strings.Builder
		qryInsertCacheToDimLvlMappings.WriteString("INSERT INTO cache_layer_dimension_levels values ")
		for _, id := range dimLvlIds {
			qryInsertCacheToDimLvlMappings.WriteString(fmt.Sprintf("(%d, %d),", clId, id))
		}
		query := qryInsertCacheToDimLvlMappings.String()[:qryInsertCacheToDimLvlMappings.Len()-1]

		_, err = db.ExecContext(ctx, query)
		if err != nil {
			log.Fatalf("ERROR datamodels/cache_layers.go/AddCacheLayerForDTAndDimLvls: %s", err)
			return err
		}

		log.Printf("Start creating cache-layer=%d", clId)
		err := q.createCacheTables(ctx, tx, clId, dimLvls, locales, measureTypes, partition)
		return err
	})
}

func (q *Queries) createCacheTables(ctx context.Context, tx *sqlx.Tx, clId int, dimLvls []string, locales []int, measureTypes []string, partitionCount int) error {
	data := struct {
		CacheLayerID    int
		DimensionLevels []string
		Partitions      int
		LocaleID        int
		MeasureType     string
	}{clId, dimLvls, partitionCount, 0, ""}

	// load db query stmt templates
	templates := packr.New("create_cache", "../../internal/datamodels/db-templates/create-cache-layer")
	for _, tmplFile := range templates.List() {
		if rawStr, err := templates.FindString(tmplFile); err != nil {
			log.Fatal("createCacheTables() error loading template: ", err)
		} else {
			var stmt strings.Builder

			switch tmplFile {
			case "create-cache-layer-dim-data.tmpl":
				{
					log.Printf("Creating cache-layer-dim-data tables")
					tmpl, err := template.New("cacheLayerTmpl").Parse(rawStr)
					if err != nil {
						log.Println("Error while template parsing: ", err)
						return err
					}

					err = tmpl.Execute(&stmt, data)
					if err != nil {
						log.Println("Error while template execution: ", err)
						return err
					}

					// execute stmt over database
					if _, err = tx.ExecContext(ctx, stmt.String()); err != nil {
						if err != nil {
							log.Println("Error while template execution: ", err)
							return err
						}
					}
					stmt.Reset()
				}

			case "create-cache-layer-dim-data-locale.tmpl", "create-cache-layer-dim-data-search-locale.tmpl":
				{
					log.Printf("Creating create-cache-layer-dim-data-locale tables")
					tmpl, err := template.New("cacheLayerTmpl").Parse(rawStr)
					if err != nil {
						log.Println("Error while template parsing: ", err)
						return err
					}

					for _, localeId := range locales {
						data.LocaleID = localeId
						err = tmpl.Execute(&stmt, data)
						if err != nil {
							log.Println("Error while template execution: ", err)
							return err
						}

						// execute stmt over database
						if _, err = tx.ExecContext(ctx, stmt.String()); err != nil {
							if err != nil {
								log.Println("Error while template execution: ", err)
								return err
							}
						}
						stmt.Reset()
					}
				}

			case "create-cache-layer-measures.tmpl", "create-cache-layer-measures-branches.tmpl":
				{
					tmpl, err := template.New("cacheLayerTmpl").Parse(rawStr)
					if err != nil {
						log.Println("Error while template parsing: ", err)
						return err
					}

					for _, measureType := range measureTypes {
						log.Printf("Creating tables for measure-type=%s", measureType)

						data.MeasureType = measureType
						err = tmpl.Execute(&stmt, data)
						if err != nil {
							log.Println("Error while template execution: ", err)
							return err
						}

						// execute stmt over database
						if _, err = tx.ExecContext(ctx, stmt.String()); err != nil {
							if err != nil {
								log.Println("Error while template execution: ", err)
								return err
							}
						}
						stmt.Reset()
					}
				}
			}
		}
	}

	return nil
}

func (q Queries) getDimLvls(ctx context.Context, db *sqlx.DB, datatableId int, dimLvlIds []int) ([]string, error) {
	dimLvls := make([]string, 0)

	s := fmt.Sprintf("(%s)", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(dimLvlIds)), ","), "[]"))
	err := db.SelectContext(ctx, &dimLvls, fmt.Sprintf("SELECT dimension_level_column_name FROM dimension_levels WHERE dimension_level_id IN %s", s))
	switch {
	case err == sql.ErrNoRows:
		log.Println("No datatable with id: ", datatableId)
		return nil, err

	case err != nil:
		log.Println("query error: ", err)
		return nil, err
	default:
		if len(dimLvls) != len(dimLvlIds) {
			log.Println("Error while validating provided dim level ids: ", dimLvlIds)
			return nil, err
		}
	}
	return dimLvls, nil
}

func (q Queries) getLocales(ctx context.Context, db *sqlx.DB) ([]int, error) {
	locales := make([]int, 0)

	err := db.SelectContext(ctx, &locales, "SELECT locale_id FROM locales")
	switch {
	case err == sql.ErrNoRows:
		log.Printf("locales table is empty\n")
		return nil, err
	case err != nil:
		log.Printf("query error: %v\n", err)
		return nil, err
	}

	return locales, nil
}

func (q Queries) getMeasureTypes(ctx context.Context, db *sqlx.DB, datatableId int) ([]string, error) {
	types := make([]string, 0)

	err := db.SelectContext(ctx, &types, "SELECT DISTINCT measure_type_column_name FROM measures  NATURAL JOIN measure_types WHERE datatable_id=?", datatableId)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("locales table is empty\n")
		return nil, err
	case err != nil:
		log.Printf("query error: %v\n", err)
		return nil, err
	}

	return types, nil
}
