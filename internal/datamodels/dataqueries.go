package datamodels

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"github.com/jmoiron/sqlx"
)

func (q *Queries) PeformDataQuery(ctx context.Context, query *DAQueryInput) (*DAQueryResultConnection, error) {
	datasetID, err := strconv.Atoi(query.DatasetID)
	if err != nil || q.dbs[datasetID] == nil {
		error := errors.New(fmt.Sprintf("datasetID %s not found", query.DatasetID))
		log.Printf("ERROR datamodels/dataqueries.go/PeformDataQuery: %s", error)
		return nil, error
	}

	// Get user id and locale from context
	userID, ok := getUserID(ctx)
	if !ok {
		log.Printf("ERROR: datamodels/dataqueries.go/PeformDataQuery [getUserID]: No found userID in context")
		return nil, errors.New("No found userID in context")
	}
	localeID, err := getLocaleId(ctx)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/PeformDataQuery: %s [GetLocaleId]", err)
		return nil, err
	}

	// Validate the query
	validation, err := q.validateQuery(ctx, validateQueryParams{
		datasetId:       datasetID,
		datatableName:   query.Datatable,
		dimLevels:       query.DimensionLevels,
		aggrMeasures:    query.AggregatedMeasures,
		scope:           query.Scope,
		sort:            query.Sort,
		postAggFilter:   query.PostAggFilter,
		postAggGrouping: query.PostAggGrouping,
		branchID:        query.BranchID,
		version:         query.Version,
		userID:          &userID,
	})
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataqueries.go/PeformDataQuery: [validateQuery] %s", err)
		return nil, err
	}

	var first int
	if query.First != nil {
		first = *query.First
	} else {
		first = config.DEFAULT_DATA_QUERY_FIRST
	}

	// Aggregating Dimensions for Cache Layer
	dimensionLevels := q.collectQueryDimLevels(query.DimensionLevels, query.Scope)

	// Try to get a cache layer
	cacheLayerId, _ := q.getMatchingCacheLayer(ctx, getMatchingCacheLayerParams{
		DatasetID:       datasetID,
		DatatableID:     validation.datatable.id,
		DimensionLevels: dimensionLevels,
	})

	// Run the query
	queryConn, err := q.dataQuery(ctx, dataQueryParams{
		DatasetID:          datasetID,
		DatatableID:        validation.datatable.id,
		DimensionLevels:    query.DimensionLevels,
		AggregatedMeasures: query.AggregatedMeasures,
		AggrMeasuresMap:    validation.aggrMeasuresMap,
		MeasuresMap:        validation.measuresMap,
		UserID:             userID,
		LocaleID:           localeID,
		Version:            validation.version,
		Branch:             validation.branch,
		CacheLayerId:       cacheLayerId,
		Scope:              query.Scope,
		PostAggFilter:      query.PostAggFilter,
		Sort:               query.Sort,
		PostAggGrouping:    query.PostAggGrouping,
		First:              first,
		After:              query.After,
	})
	if err != nil {
		return nil, err
	}

	return queryConn, nil
}

func (q *Queries) PeformDataSearchQuery(ctx context.Context, query *DADimMembersQueryInput) (*DAQueryResultConnection, error) {
	datasetID, err := strconv.Atoi(query.DatasetID)
	if err != nil || q.dbs[datasetID] == nil {
		error := errors.New(fmt.Sprintf("datasetID %s not found", query.DatasetID))
		log.Printf("ERROR datamodels/dataqueries.go/PeformDataSearchQuery: %s", error)
		return nil, error
	}

	// Get user id and locale from context
	userID, ok := getUserID(ctx)
	if !ok {
		log.Printf("ERROR: datamodels/dataqueries.go/PeformDataSearchQuery [getUserID]: No found userID in context")
		return nil, errors.New("No found userID in context")
	}
	localeID, err := getLocaleId(ctx)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/PeformDataQuery: %s [GetLocaleId]", err)
		return nil, err
	}

	// Validate the query
	validation, err := q.validateQuery(ctx, validateQueryParams{
		datasetId:     datasetID,
		datatableName: query.Datatable,
		dimLevels:     query.DimensionLevels,
		aggrMeasures:  nil,
		scope:         query.Scope,
		sort:          query.Sort,
		branchID:      query.BranchID,
		version:       query.Version,
		userID:        &userID,
	})
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataqueries.go/PeformDataQuery: [validateQuery] %s", err)
		return nil, err
	}

	var first int
	if query.First != nil {
		first = *query.First
	} else {
		first = config.DEFAULT_DATA_QUERY_SEARCH_FIRST
	}

	// Aggregating Dimensions for Cache Layer
	dimensionLevels := q.collectQueryDimLevels(query.DimensionLevels, query.Scope)

	// Try to get a cache layer
	cacheLayerId, _ := q.getMatchingCacheLayer(ctx, getMatchingCacheLayerParams{
		DatasetID:       datasetID,
		DatatableID:     validation.datatable.id,
		DimensionLevels: dimensionLevels,
	})

	// Run the query
	queryConn, err := q.dataQuerySearch(ctx, dataQuerySearchParams{
		DatasetID:       datasetID,
		DatatableID:     validation.datatable.id,
		DimensionLevels: query.DimensionLevels,
		DimensionSearch: query.Search,
		UserID:          userID,
		LocaleID:        localeID,
		Version:         validation.version,
		Branch:          validation.branch,
		CacheLayerId:    cacheLayerId,
		Scope:           query.Scope,
		Sort:            query.Sort,
		First:           first,
		After:           query.After,
	})
	if err != nil {
		return nil, err
	}

	return queryConn, nil
}

// Private methods ------------------------------------------------------------

type queryCursor struct {
	queryId     string
	after       int
	versionTime string
}

type dataQueryParams struct {
	DatasetID          int
	DatatableID        int
	DimensionLevels    []string
	AggregatedMeasures []string
	AggrMeasuresMap    map[string]*AggrMeasureForMap
	MeasuresMap        map[int]*MeasureForMap
	UserID             string
	LocaleID           int
	Version            *DAVersion
	Branch             *DABranch
	CacheLayerId       *int
	Scope              *DAQueryScopeInput
	PostAggFilter      *DAQueryPostAggFilterInput
	Sort               *DAQuerySortInput
	PostAggGrouping    *DAQueryPostAggGroupingInput
	First              int
	After              *string
}

func (q *Queries) dataQuery(ctx context.Context, args dataQueryParams) (*DAQueryResultConnection, error) {

	var argsQuery []interface{}
	versionTime := args.Version.AppliedTimestamp.Format(config.DEFAULT_TIME_FORMAT)
	branchTime := versionTime
	if args.Branch != nil && args.Branch.FromTimestamp.Valid {
		branchTime = args.Branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)
	}
	queryOffset := -1

	var cursor *queryCursor
	if args.After != nil {
		cursor = q.decodeQueryCursor(*args.After)
		if cursor != nil {
			// If we have a cursor take the version time from it
			versionTime = cursor.versionTime
			queryOffset = cursor.after
		}
	}

	var queryDimLevel string
	var groupByDimLevel string
	var orderByDimLevel string
	var postAggFilter string
	dimensionLevels := map[string]string{}
	for _, dimLevel := range args.DimensionLevels {
		queryDimLevel += dimLevel + "_id, "
		queryDimLevel += dimLevel + "_name, "
		queryDimLevel += dimLevel + "_description, "
		queryDimLevel += dimLevel + "_external_id, "

		groupByDimLevel += dimLevel + "_id, "
		dimensionLevels[dimLevel] = dimLevel + "_name"
	}
	queryDimLevel = queryDimLevel[:len(queryDimLevel)-2]
	groupByDimLevel = groupByDimLevel[:len(groupByDimLevel)-2]

	var baseMeasureId string
	var dimDataLocaleTable string
	if args.CacheLayerId == nil {
		baseMeasureId = "base_id"
		dimDataLocaleTable = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA_LOCALE, args.DatatableID, args.LocaleID)
	} else {
		baseMeasureId = fmt.Sprintf("cache_layer_%d_id", *args.CacheLayerId)
		dimDataLocaleTable = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA_LOCALE, *args.CacheLayerId, args.LocaleID)
	}

	// Construct CTE expressions
	cteTableName := []string{}
	cteTablesMap := map[string]string{}
	aggrMeasureDataExtension := []*AggrMeasureForMap{}

	if args.CacheLayerId == nil {
		// Use the measures map
		for _, measureInfo := range args.MeasuresMap {
			tableName := fmt.Sprintf("dt%d_m%d", args.DatatableID, measureInfo.MeasureID)
			measureTableName := fmt.Sprintf(config.DEFAULT_DATATABLE_BASE_MEASURES, args.DatatableID, measureInfo.MeasureDataType)

			query := fmt.Sprintf("%s AS ( SELECT measure_id, %s, value ", tableName, baseMeasureId)
			query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableName, branchTime)
			query += fmt.Sprintf("WHERE measure_id = %d ", measureInfo.MeasureID)
			query += ")"

			cteTableName = append(cteTableName, tableName)
			cteTablesMap[tableName] = query

			if args.Branch != nil {
				tableName := fmt.Sprintf("dt%d_m%d_branches", args.DatatableID, measureInfo.MeasureID)
				measureTableNameBranch := fmt.Sprintf(config.DEFAULT_DATATABLE_BASE_MEASURES_BRANCHES, args.DatatableID, measureInfo.MeasureDataType)

				query := fmt.Sprintf("%s AS ( SELECT measure_id, %s, value ", tableName, baseMeasureId)
				query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableNameBranch, versionTime)
				query += fmt.Sprintf("WHERE measure_id = %d AND branch_id = %d ", measureInfo.MeasureID, args.Branch.id)
				query += ")"

				cteTableName = append(cteTableName, tableName)
				cteTablesMap[tableName] = query
			}
		}
		// Collect data extension measures
		for _, aggMeasureInfo := range args.AggrMeasuresMap {
			if aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				aggrMeasureDataExtension = append(aggrMeasureDataExtension, aggMeasureInfo)
				continue
			}
		}
	} else {
		// Use the aggregated measures map
		for _, aggMeasureInfo := range args.AggrMeasuresMap {
			// Ignore base_only and formulas here
			if aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_BASE_ONLY ||
				aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
				continue
			}
			// Collect data extension measures
			if aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				aggrMeasureDataExtension = append(aggrMeasureDataExtension, aggMeasureInfo)
				continue
			}
			if aggMeasureInfo.MeasureID == nil {
				// This shouldn't really happen
				continue
			}

			tableName := fmt.Sprintf("dt%d_m%d", args.DatatableID, aggMeasureInfo.MAggregationID)
			measureInfo := args.MeasuresMap[*aggMeasureInfo.MeasureID]
			measureTableName := fmt.Sprintf(config.DEFAULT_CACHELAYER_BASE_MEASURES, *args.CacheLayerId, measureInfo.MeasureDataType)

			query := fmt.Sprintf("%s AS ( SELECT measure_aggregation_id, %s, value ", tableName, baseMeasureId)
			query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableName, branchTime)
			query += fmt.Sprintf("WHERE measure_aggregation_id = %d ", aggMeasureInfo.MAggregationID)
			query += ")"

			cteTableName = append(cteTableName, tableName)
			cteTablesMap[tableName] = query

			if args.Branch != nil {
				tableName := fmt.Sprintf("dt%d_m%d_branches", args.DatatableID, aggMeasureInfo.MAggregationID)
				measureTableNameBranch := fmt.Sprintf(config.DEFAULT_CACHELAYER_BASE_MEASURES_BRANCHES, *args.CacheLayerId, measureInfo.MeasureDataType)

				query := fmt.Sprintf("%s AS ( SELECT measure_aggregation_id, %s, value ", tableName, baseMeasureId)
				query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableNameBranch, versionTime)
				query += fmt.Sprintf("WHERE measure_aggregation_id = %d AND branch_id = %d ", aggMeasureInfo.MAggregationID, args.Branch.id)
				query += ")"

				cteTableName = append(cteTableName, tableName)
				cteTablesMap[tableName] = query
			}
		}
	}
	sort.Strings(cteTableName)
	cteTables := []string{}
	for _, tableName := range cteTableName {
		cteTables = append(cteTables, cteTablesMap[tableName])
	}

	// Get the matching data extensions
	aggrMeasureExtensionMap, err := q.getMatchingDataExtension(ctx, getMatchingDataExtensionParams{
		DatasetID:            args.DatasetID,
		AggrMeasureExtension: aggrMeasureDataExtension,
		DimensionLevels:      args.DimensionLevels,
	})
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
		return nil, err
	}

	// Sort AggrMeasuresMap
	aggrMeasuresKeys := make([]string, 0, len(args.AggrMeasuresMap))
	for aggMeasureName := range args.AggrMeasuresMap {
		aggrMeasuresKeys = append(aggrMeasuresKeys, aggMeasureName)
	}
	sort.Strings(aggrMeasuresKeys)

	extensionJoin := ""
	extensionTableAdded := map[int]bool{}

	// Construct the pivoted table
	pivoted := "pivoted as ( SELECT " + queryDimLevel
	for _, aggMeasureName := range aggrMeasuresKeys {
		aggMeasure := args.AggrMeasuresMap[aggMeasureName]
		if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
			continue
		}

		if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
			if aggrMeasureExtensionMap != nil {
				dataextensionId, ok := aggrMeasureExtensionMap[aggMeasureName]
				if ok {
					if _, ok := extensionTableAdded[dataextensionId]; !ok {
						extensionJoin += fmt.Sprintf("NATURAL LEFT JOIN dataextensions_%d AS ext%d ", dataextensionId, dataextensionId)
						extensionTableAdded[dataextensionId] = true
					}
					pivoted += fmt.Sprintf(", ext%d.%s AS %s ", dataextensionId, aggMeasureName, aggMeasureName)
				}
			}
			continue
		}

		if aggMeasure.MeasureID == nil {
			// This shouldn't really happen
			continue
		}

		measureInfo, ok := args.MeasuresMap[*aggMeasure.MeasureID]
		if !ok {
			err := errors.New(fmt.Sprintf("Couldn't find info for measure-id=%d", *aggMeasure.MeasureID))
			log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
			return nil, err
		}

		if args.CacheLayerId == nil {
			aggFunc := DAAggregationTypeMax
			if aggMeasure.MAggregationType != config.MEASURE_AGGREGATION_TYPE_BASE_ONLY {
				aggFunc = aggMeasure.MAggregationType
			}

			if args.Branch == nil {
				pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_id=%d THEN CAST(dt%d_m%d.value AS %s) END) AS %s ",
					aggFunc, args.DatatableID, measureInfo.MeasureID, measureInfo.MeasureID,
					args.DatatableID, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
			} else {
				pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_id=%d OR dt%d_m%d_branches.measure_id=%d THEN CAST( IF(dt%d_m%d_branches.measure_id IS NOT NULL, dt%d_m%d_branches.value, dt%d_m%d.value) AS %s) END) AS %s ",
					aggFunc, args.DatatableID, measureInfo.MeasureID, measureInfo.MeasureID,
					args.DatatableID, measureInfo.MeasureID, measureInfo.MeasureID,
					args.DatatableID, measureInfo.MeasureID, args.DatatableID, measureInfo.MeasureID,
					args.DatatableID, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
			}
		} else {
			if args.Branch == nil {
				pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_aggregation_id=%d THEN CAST(dt%d_m%d.value AS %s) END) AS %s ",
					aggMeasure.MAggregationType, args.DatatableID, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
					args.DatatableID, aggMeasure.MAggregationID, aggMeasure.MAggregationCastType, aggMeasureName)
			} else {
				pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_aggregation_id=%d OR dt%d_m%d_branches.measure_aggregation_id=%d THEN CAST( IF(dt%d_m%d_branches.measure_aggregation_id IS NOT NULL, dt%d_m%d_branches.value, dt%d_m%d.value) AS %s) END) AS %s ",
					aggMeasure.MAggregationType, args.DatatableID, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
					args.DatatableID, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
					args.DatatableID, aggMeasure.MAggregationID, args.DatatableID, aggMeasure.MAggregationID,
					args.DatatableID, aggMeasure.MAggregationID, aggMeasure.MAggregationCastType, aggMeasureName)
			}
		}
	}
	pivoted += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' AS base ", dimDataLocaleTable, branchTime)
	for _, tableName := range cteTableName {
		pivoted += fmt.Sprintf("LEFT JOIN %s ON (%s.%s = base.%s) ", tableName, tableName, baseMeasureId, baseMeasureId)
	}
	pivoted += extensionJoin

	whereScope, err := q.queryScopeToWhereConditions(ctx, args.DatasetID, args.DatatableID, &args.UserID, args.Scope, false)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [queryScopeToWhereConditions]", err)
		return nil, err
	}
	if whereScope != nil && len(*whereScope) > 0 {
		pivoted += "WHERE " + *whereScope + " "
	}

	pivoted += "GROUP BY " + groupByDimLevel
	pivoted += ")"

	// Construct the final query
	cteQuery := "WITH "
	if len(cteTables) > 0 {
		cteQuery += strings.Join(cteTables, ", ")
		cteQuery += ", " + pivoted + " "
	} else {
		cteQuery += pivoted + " "
	}

	countQuery := ""
	if args.PostAggGrouping == nil {
		countQuery = "SELECT DISTINCT COUNT(*) OVER ()"
	} else {
		countQuery = fmt.Sprintf("SELECT %s", queryDimLevel)
	}
	query := fmt.Sprintf("SELECT %s", queryDimLevel)
	for _, aggMeasureName := range args.AggregatedMeasures {
		aggMeasure, ok := args.AggrMeasuresMap[aggMeasureName]
		if !ok {
			log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", aggMeasureName)
			continue
		}

		if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
			query += fmt.Sprintf(", (%s) AS %s", *aggMeasure.MAggregationFormula, aggMeasureName)
			if args.PostAggGrouping != nil {
				countQuery += fmt.Sprintf(", (%s) AS %s", *aggMeasure.MAggregationFormula, aggMeasureName)
			}
		} else if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
			extensionValue := "NULL"
			if aggrMeasureExtensionMap != nil {
				if _, ok := aggrMeasureExtensionMap[aggMeasureName]; ok {
					extensionValue = aggMeasureName
				}
			}
			query += fmt.Sprintf(", %s AS %s", extensionValue, aggMeasureName)
			if args.PostAggGrouping != nil {
				countQuery += fmt.Sprintf(", %s AS %s", extensionValue, aggMeasureName)
			}
		} else {
			query += fmt.Sprintf(", %s", aggMeasureName)
			if args.PostAggGrouping != nil {
				countQuery += fmt.Sprintf(", %s", aggMeasureName)
			}
		}
	}
	query += " FROM pivoted"
	countQuery += " FROM pivoted"
	query += " GROUP BY " + groupByDimLevel
	countQuery += " GROUP BY " + groupByDimLevel
	// Add the post aggr filter
	if args.PostAggFilter != nil {
		if len((*args.PostAggFilter).MeasureFilters) > 0 {
			(*args.PostAggFilter).AND = (*args.PostAggFilter).MeasureFilters
		}
		if len((*args.PostAggFilter).AND) > 0 && len((*args.PostAggFilter).OR) > 0 {
			postAggFilter += "("
		}
		for _, each := range (*args.PostAggFilter).AND {
			currentFilter := ""
			if each.Operator == DARelationalOperatorIn {
				if len(each.Value) == 0 {
					err := errors.New(fmt.Sprintf("Value is empty to measure %s", each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter AND Operator IN]", err)
					return nil, err
				}
				if each.MeasureMultiplier != 0 {
					currentFilter += fmt.Sprintf("%s * %f ", each.MeasureColumnName, each.MeasureMultiplier)
				} else {
					currentFilter += each.MeasureColumnName + " "
				}
				currentFilter += each.Operator.GetOperator() + " (?) AND "
				var inargs []interface{}
				currentFilter, inargs, err = sqlx.In(currentFilter, each.Value)
				if err != nil {
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter AND Operator IN]", err)
					return nil, err
				}
				argsQuery = append(argsQuery, inargs...)

			} else {
				if len(each.Value) == 0 && len(each.DstMeasureColumnName) == 0 {
					err := errors.New(fmt.Sprintf("Value or DstMeasureColumnName both empty are not allowed to measure %s", each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter AND Operator %s]", err, each.Operator.GetOperator())
					return nil, err
				} else if len(each.Value) > 1 {
					err := errors.New(fmt.Sprintf("Operator %s does not allow more than one value to measure %s", each.Operator.GetOperator(), each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter AND Operator %s]", err, each.Operator.GetOperator())
					return nil, err
				}

				if each.MeasureMultiplier != 0 {
					currentFilter += fmt.Sprintf("%s * %f ", each.MeasureColumnName, each.MeasureMultiplier)
				} else {
					currentFilter += each.MeasureColumnName + " "
				}
				currentFilter += each.Operator.GetOperator() + " "

				if len(each.DstMeasureColumnName) > 0 {
					if each.DstMultiplier != 0 {
						currentFilter += fmt.Sprintf("%s * %f AND ", each.DstMeasureColumnName, each.DstMultiplier)
					} else {
						currentFilter += each.DstMeasureColumnName + " AND "
					}
				} else {
					if each.DstMultiplier != 0 {
						currentFilter += each.Value[0] + fmt.Sprintf(" * %f AND ", each.DstMultiplier)
					} else {
						currentFilter += each.Value[0] + " AND "
					}
				}
			}
			postAggFilter += currentFilter
		}
		if len((*args.PostAggFilter).AND) > 0 && len((*args.PostAggFilter).OR) > 0 {
			postAggFilter = postAggFilter[:len(postAggFilter)-4] + ") AND ( "
		} else if len((*args.PostAggFilter).AND) > 0 {
			postAggFilter = postAggFilter[:len(postAggFilter)-4]
		}

		if len((*args.PostAggFilter).AND) > 0 && len((*args.PostAggFilter).OR) > 0 {
			postAggFilter += ""
		}
		for _, each := range (*args.PostAggFilter).OR {
			currentFilter := ""
			if each.Operator == DARelationalOperatorIn {
				if len(each.Value) == 0 {
					err := errors.New(fmt.Sprintf("Value is empty to measure %s", each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter OR Operator IN]", err)
					return nil, err
				}
				if each.MeasureMultiplier != 0 {
					currentFilter += fmt.Sprintf("%s * %f ", each.MeasureColumnName, each.MeasureMultiplier)
				} else {
					currentFilter += each.MeasureColumnName + " "
				}
				currentFilter += each.Operator.GetOperator() + " (?) OR "
				var inargs []interface{}
				currentFilter, inargs, err = sqlx.In(currentFilter, each.Value)
				if err != nil {
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter OR Operator IN]", err)
					return nil, err
				}
				argsQuery = append(argsQuery, inargs...)

			} else {
				if len(each.Value) == 0 && len(each.DstMeasureColumnName) == 0 {
					err := errors.New(fmt.Sprintf("Value or DstMeasureColumnName both empty are not allowed to measure %s", each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter OR Operator %s]", err, each.Operator.GetOperator())
					return nil, err
				} else if len(each.Value) > 1 {
					err := errors.New(fmt.Sprintf("Operator %s does not allow more than one value to measure %s", each.Operator.GetOperator(), each.MeasureColumnName))
					log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggFilter OR Operator %s]", err, each.Operator.GetOperator())
					return nil, err
				}

				if each.MeasureMultiplier != 0 {
					currentFilter += fmt.Sprintf("%s * %f ", each.MeasureColumnName, each.MeasureMultiplier)
				} else {
					currentFilter += each.MeasureColumnName + " "
				}
				currentFilter += each.Operator.GetOperator() + " "

				if len(each.DstMeasureColumnName) > 0 {
					if each.DstMultiplier != 0 {
						currentFilter += fmt.Sprintf("%s * %f OR ", each.DstMeasureColumnName, each.DstMultiplier)
					} else {
						currentFilter += each.DstMeasureColumnName + " OR "
					}
				} else {
					if each.DstMultiplier != 0 {
						currentFilter += each.Value[0] + fmt.Sprintf(" * %f OR ", each.DstMultiplier)
					} else {
						currentFilter += each.Value[0] + " OR "
					}
				}
			}
			postAggFilter += currentFilter
		}
		if len((*args.PostAggFilter).AND) > 0 && len((*args.PostAggFilter).OR) > 0 {
			query += " HAVING " + postAggFilter[:len(postAggFilter)-3] + ") "
			countQuery += " HAVING " + postAggFilter[:len(postAggFilter)-3] + ") "
		} else if len((*args.PostAggFilter).AND) > 0 {
			query += " HAVING " + postAggFilter
			countQuery += " HAVING " + postAggFilter
		} else if len((*args.PostAggFilter).OR) > 0 {
			query += " HAVING " + postAggFilter[:len(postAggFilter)-3]
			countQuery += " HAVING " + postAggFilter[:len(postAggFilter)-3]
		}
	}

	// Add the post agg grouping
	postGroupingDimLevels := map[string]bool{}
	var aggMapPostAggrGrouping map[string]*AggrMeasureForMap
	if args.PostAggGrouping != nil && len((*args.PostAggGrouping).GroupByColumns) > 0 {
		aggMapPostAggrGrouping, err = q.GetMapAggrMeasuresByColumnNameBasic(ctx, args.DatasetID, (*args.PostAggGrouping).AggregatedMeasures)
		if err != nil {
			log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s [PostAggGrouping]", err)
			return nil, err
		}

		postGroupDimLevels := []string{}
		groupByColumns := ""
		for _, colName := range (*args.PostAggGrouping).GroupByColumns {
			if _, ok := aggMapPostAggrGrouping[colName]; ok {
				groupByColumns += colName
			} else if _, ok := args.AggrMeasuresMap[colName]; ok {
				groupByColumns += colName
			} else {
				postGroupingDimLevels[colName] = true
				postGroupDimLevels = append(postGroupDimLevels, colName)

				groupByColumns += colName + "_id, "
				groupByColumns += colName + "_name, "
				groupByColumns += colName + "_description, "
				groupByColumns += colName + "_external_id"
			}
			groupByColumns += ", "
		}
		groupByColumns = groupByColumns[:len(groupByColumns)-2]

		var aggrMeasureExtensionMap map[string]int
		if len(postGroupDimLevels) > 0 {
			// Collect data extension measures
			aggrMeasureDataExtension := []*AggrMeasureForMap{}
			for _, aggMeasureName := range (*args.PostAggGrouping).AggregatedMeasures {
				aggMeasure, ok := aggMapPostAggrGrouping[aggMeasureName]
				if !ok {
					log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", aggMeasureName)
					continue
				}
				if aggMeasure.MAggregationType != config.MEASURE_AGGREGATION_TYPE_EXTENSION {
					continue
				}
				aggrMeasureDataExtension = append(aggrMeasureDataExtension, aggMeasure)
			}

			// Get the matching data extensions
			aggrMeasureExtensionMap, _ = q.getMatchingDataExtension(ctx, getMatchingDataExtensionParams{
				DatasetID:            args.DatasetID,
				AggrMeasureExtension: aggrMeasureDataExtension,
				DimensionLevels:      postGroupDimLevels,
			})
		}

		extensionJoin := ""
		extensionTableAdded := map[int]bool{}
		queryMeasures := ""
		for _, aggMeasureName := range (*args.PostAggGrouping).AggregatedMeasures {
			aggMeasure, ok := aggMapPostAggrGrouping[aggMeasureName]
			if !ok {
				log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", aggMeasureName)
				continue
			}

			if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
				queryMeasures += fmt.Sprintf(", (%s) AS %s", *aggMeasure.MAggregationFormula, aggMeasureName)
			} else if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				extensionValue := "NULL"
				if aggrMeasureExtensionMap != nil {
					if dataextensionId, ok := aggrMeasureExtensionMap[aggMeasureName]; ok {
						extensionValue = fmt.Sprintf("ext%d.%s", dataextensionId, aggMeasureName)
						if _, ok := extensionTableAdded[dataextensionId]; !ok {
							extensionJoin += fmt.Sprintf("NATURAL LEFT JOIN dataextensions_%d AS ext%d ", dataextensionId, dataextensionId)
							extensionTableAdded[dataextensionId] = true
						}
					}
				}
				queryMeasures += fmt.Sprintf(", %s AS %s", extensionValue, aggMeasureName)
			} else {
				queryMeasures += fmt.Sprintf(", %s(%s) AS %s", aggMeasure.MAggregationType, aggMeasureName, aggMeasureName)
			}
		}

		query = fmt.Sprintf("SELECT %s %s FROM (%s) AS q %s GROUP BY %s", groupByColumns, queryMeasures, query, extensionJoin, groupByColumns)
		countQuery = fmt.Sprintf("SELECT DISTINCT COUNT(*) OVER () FROM (%s) AS q %s GROUP BY %s", countQuery, extensionJoin, groupByColumns)
	}

	// Add the query sorting
	if args.Sort != nil {
		for _, each := range (*args.Sort).Entries {
			if v, found := dimensionLevels[each.ColumnName]; found {
				orderByDimLevel += v + " " + each.Direction.String() + ", "
			} else {
				orderByDimLevel += each.ColumnName + " " + each.Direction.String() + ", "
			}
		}
		query += " ORDER BY " + orderByDimLevel[:len(orderByDimLevel)-2]
	}
	query += fmt.Sprintf(" LIMIT %d", config.DEFAULT_DATA_QUERY_LIMIT)
	query = cteQuery + query
	countQuery = cteQuery + countQuery

	// Calculate the query ID
	finalQuery := SqlQuery2StringTypes(query, argsQuery...)
	h := sha1.New()
	h.Write([]byte(finalQuery))
	queryId := hex.EncodeToString(h.Sum(nil))
	log.Printf("datamodels/dataqueries.go/DataQuery: Request for query-id=%s , query-sql=%s", queryId, finalQuery)

	finalCountQuery := SqlQuery2StringTypes(countQuery, argsQuery...)

	// Init the cursor
	if cursor == nil {
		cursor = &queryCursor{
			queryId:     queryId,
			after:       -1,
			versionTime: versionTime,
		}
	}
	if cursor.queryId == queryId {
		// Try to get results from redis
		results := q.getQueryResultsfromCache(ctx, *cursor, args.First)
		if results != nil {
			results.PageInfo.queryId = queryId
			results.PageInfo.countQuery = finalCountQuery
			results.PageInfo.datasetID = args.DatasetID
			log.Printf("datamodels/dataqueries.go/DataQuery: The query-id=%s was resolved from cache", queryId)
			return results, nil
		}
	} else {
		cursor.queryId = queryId
	}

	// Run the query
	db := q.GetDatasetDB(args.DatasetID)
	query = db.Rebind(query)
	log.Printf("datamodels/dataqueries.go/DataQuery: Running query-id=%s in the DB", queryId)
	rows, err := db.QueryxContext(ctx, query, argsQuery...)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
		return nil, err
	}

	// Return results
	results := DAQueryResultConnection{
		Edges: []*DAQueryResultEdge{},
		PageInfo: &DAPageInfo{
			HasNextPage: false,
			queryId:     queryId,
			countQuery:  finalCountQuery,
			datasetID:   args.DatasetID,
		},
	}

	countRows := 0
	cacheKey := fmt.Sprintf(config.DATA_QUERY_CACHE_KEY, cursor.queryId)
	cacheShardId := 0
	cacheShard := map[int]DAQueryResultCache{}
	cacheShardCount := 0

	for rows.Next() {
		// Scan the row
		rowMap := map[string]interface{}{}
		err := rows.MapScan(rowMap)
		if err != nil {
			log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
			return nil, err
		}

		rowResult := DAQueryResult{
			DimensionMembers: []*DADimensionMember{},
			MeasureValues:    []*string{},
		}
		cacheResult := DAQueryResultCache{
			DimensionMembers: []*DADimensionMember{},
			MeasureValues:    []sql.NullString{},
		}

		if args.PostAggGrouping == nil {
			// Build the DimensionMembers
			for _, dimLevel := range args.DimensionLevels {
				var descPointer *string = nil
				if rowMap[dimLevel+"_description"] != nil {
					desc := ValueToString(rowMap[dimLevel+"_description"], "", nil)
					descPointer = &desc
				}
				var extIDPointer *string = nil
				if rowMap[dimLevel+"_external_id"] != nil {
					extID := ValueToString(rowMap[dimLevel+"_external_id"], "", nil)
					extIDPointer = &extID
				}

				rowResult.DimensionMembers = append(rowResult.DimensionMembers, &DADimensionMember{
					DatasetID:                fmt.Sprintf("%d", args.DatasetID),
					ID:                       ValueToString(rowMap[dimLevel+"_id"], "", nil),
					DimensionLevelColumnName: dimLevel,
					Name:                     ValueToString(rowMap[dimLevel+"_name"], "", nil),
					Description:              descPointer,
					ExternalID:               extIDPointer,
				})
			}

			// Build the MeasureValues
			for _, aggMeasureName := range args.AggregatedMeasures {
				aggMeasure, ok := args.AggrMeasuresMap[aggMeasureName]
				if !ok {
					log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", aggMeasureName)
					continue
				}
				if rowMap[aggMeasureName] == nil {
					rowResult.MeasureValues = append(rowResult.MeasureValues, nil)
					cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: false})
				} else {
					valueM := ValueToString(rowMap[aggMeasureName], "0", &aggMeasure.MAggregationCastType)
					rowResult.MeasureValues = append(rowResult.MeasureValues, &valueM)
					cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: true, String: valueM})
				}
			}
		} else {
			// Build the DimensionMembers and MeasureValues
			for _, column := range (*args.PostAggGrouping).GroupByColumns {
				if _, ok := postGroupingDimLevels[column]; ok {
					// Add a DimensionMember
					var descPointer *string = nil
					if rowMap[column+"_description"] != nil {
						desc := ValueToString(rowMap[column+"_description"], "", nil)
						descPointer = &desc
					}
					var extIDPointer *string = nil
					if rowMap[column+"_external_id"] != nil {
						extID := ValueToString(rowMap[column+"_external_id"], "", nil)
						extIDPointer = &extID
					}

					rowResult.DimensionMembers = append(rowResult.DimensionMembers, &DADimensionMember{
						DatasetID:                fmt.Sprintf("%d", args.DatasetID),
						ID:                       ValueToString(rowMap[column+"_id"], "", nil),
						DimensionLevelColumnName: column,
						Name:                     ValueToString(rowMap[column+"_name"], "", nil),
						Description:              descPointer,
						ExternalID:               extIDPointer,
					})
				} else {
					aggMeasure, ok := args.AggrMeasuresMap[column]
					if !ok {
						log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", column)
						continue
					}
					// Add a MeasureValue
					if rowMap[column] == nil {
						rowResult.MeasureValues = append(rowResult.MeasureValues, nil)
						cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: false})
					} else {
						valueM := ValueToString(rowMap[column], "0", &aggMeasure.MAggregationCastType)
						rowResult.MeasureValues = append(rowResult.MeasureValues, &valueM)
						cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: true, String: valueM})
					}
				}
			}
			for _, column := range (*args.PostAggGrouping).AggregatedMeasures {
				aggMeasure, ok := aggMapPostAggrGrouping[column]
				if !ok {
					log.Printf("WARNING: datamodels/dataqueries.go/DataQuery: ignoring agg-measure=%s", column)
					continue
				}
				if rowMap[column] == nil {
					rowResult.MeasureValues = append(rowResult.MeasureValues, nil)
					cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: false})
				} else {
					valueM := ValueToString(rowMap[column], "0", &aggMeasure.MAggregationCastType)
					rowResult.MeasureValues = append(rowResult.MeasureValues, &valueM)
					cacheResult.MeasureValues = append(cacheResult.MeasureValues, sql.NullString{Valid: true, String: valueM})
				}

			}
		}

		if cacheShardCount >= config.DEFAULT_DATA_QUERY_CACHE_SHARD {
			// Save the current shard
			q.setCacheQueryShard(ctx, cacheKey, strconv.Itoa(cacheShardId), cacheShard)
			cacheShardId += 1
			cacheShard = map[int]DAQueryResultCache{}
			cacheShardCount = 0

		}
		cacheResult.DimensionMembers = rowResult.DimensionMembers
		cacheShard[countRows] = cacheResult
		cacheShardCount++

		if countRows > queryOffset && countRows <= args.First+queryOffset {
			// Add this row in the result set
			cursor.after = countRows
			results.Edges = append(results.Edges, &DAQueryResultEdge{
				Node:   &rowResult,
				Cursor: q.encodeQueryCursor(*cursor),
			})
		}
		countRows++
	}

	if countRows > 0 {
		if len(cacheShard) > 0 {
			// Save the current shard
			q.setCacheQueryShard(ctx, cacheKey, strconv.Itoa(cacheShardId), cacheShard)
		}
		// Set the cache expiration
		q.setCacheQueryExpiration(ctx, cacheKey, config.BASE_CACHE_EXPIRATION())

		if countRows >= args.First+queryOffset {
			results.PageInfo.HasNextPage = true
		}
	}

	return &results, nil
}

type dataQuerySearchParams struct {
	DatasetID       int
	DatatableID     int
	DimensionLevels []string
	DimensionSearch *DAQuerySearchInput
	UserID          string
	LocaleID        int
	Version         *DAVersion
	Branch          *DABranch
	CacheLayerId    *int
	Scope           *DAQueryScopeInput
	Sort            *DAQuerySortInput
	First           int
	After           *string
}

func (q *Queries) dataQuerySearch(ctx context.Context, args dataQuerySearchParams) (*DAQueryResultConnection, error) {
	// ToDo:
	// - Apply validations

	var argsQuery []interface{}
	versionTime := args.Version.AppliedTimestamp.Format(config.DEFAULT_TIME_FORMAT)
	queryOffset := -1

	var cursor *queryCursor
	if args.After != nil {
		cursor = q.decodeQueryCursor(*args.After)
		if cursor != nil {
			// If we have a cursor take the version time from it
			versionTime = cursor.versionTime
			queryOffset = cursor.after
		}
	}

	var queryDimLevel string
	var groupByDimLevel string
	var orderByDimLevel string
	dimensionLevels := map[string]string{}
	for _, dimLevel := range args.DimensionLevels {
		queryDimLevel += dimLevel + "_id, "
		queryDimLevel += dimLevel + "_name, "
		queryDimLevel += dimLevel + "_description, "
		queryDimLevel += dimLevel + "_external_id, "

		groupByDimLevel += dimLevel + "_id, "
		dimensionLevels[dimLevel] = dimLevel + "_name"
	}
	queryDimLevel = queryDimLevel[:len(queryDimLevel)-2]
	groupByDimLevel = groupByDimLevel[:len(groupByDimLevel)-2]

	// Construct the query
	query := "SELECT " + queryDimLevel

	whereScope, err := q.queryScopeToWhereConditions(ctx, args.DatasetID, args.DatatableID, &args.UserID, args.Scope, false)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
		return nil, err
	}
	queryWHERE := " WHERE 1=1"
	if whereScope != nil && len(*whereScope) > 0 {
		queryWHERE += " AND " + *whereScope + " "
	}
	// Add the search filter
	hasSearch := false
	if args.DimensionSearch != nil {
		for _, dimFilterInput := range (*args.DimensionSearch).DimensionFilters {
			for _, dimLevelFilterInput := range dimFilterInput.LevelFilters {
				queryWHERE += fmt.Sprintf(" AND ( MATCH(%s_name) AGAINST(?)", dimLevelFilterInput.DimensionLevelColumnName)
				queryWHERE += fmt.Sprintf(" OR MATCH(%s_description) AGAINST(?)", dimLevelFilterInput.DimensionLevelColumnName)
				queryWHERE += fmt.Sprintf(" OR MATCH(%s_external_id) AGAINST(?))", dimLevelFilterInput.DimensionLevelColumnName)
				argsQuery = append(argsQuery, dimLevelFilterInput.Keyword)
				argsQuery = append(argsQuery, dimLevelFilterInput.Keyword)
				argsQuery = append(argsQuery, dimLevelFilterInput.Keyword)
				hasSearch = true
			}
		}
	}

	// Query Sort
	if args.Sort != nil {
		for _, each := range (*args.Sort).Entries {
			if v, found := dimensionLevels[each.ColumnName]; found {
				orderByDimLevel += v + " " + each.Direction.String() + ", "
			} else {
				orderByDimLevel += each.ColumnName + " " + each.Direction.String() + ", "
			}
		}
		orderByDimLevel = "ORDER BY " + orderByDimLevel[:len(orderByDimLevel)-2]
	}

	tableName := ""
	if args.CacheLayerId == nil {
		tableName = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA_LOCALE, args.DatatableID, args.LocaleID)
		if hasSearch {
			tableName = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA_SEARCH_LOCALE, args.DatatableID, args.LocaleID)
		}
	} else {
		tableName = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA_LOCALE, *args.CacheLayerId, args.LocaleID)
		if hasSearch {
			tableName = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA_SEARCH_LOCALE, *args.CacheLayerId, args.LocaleID)
		}
	}

	query = fmt.Sprintf("%s FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' AS base %s GROUP BY %s %s LIMIT %d",
		query, tableName, versionTime, queryWHERE, groupByDimLevel, orderByDimLevel, config.DEFAULT_DATA_QUERY_LIMIT)

	countQuery := fmt.Sprintf("SELECT DISTINCT COUNT(*) OVER () FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' AS base %s GROUP BY %s",
		tableName, versionTime, queryWHERE, groupByDimLevel)

	// Calculate the query ID
	finalQuery := SqlQuery2StringTypes(query, argsQuery...)
	h := sha1.New()
	h.Write([]byte(finalQuery))
	queryId := hex.EncodeToString(h.Sum(nil))
	log.Printf("datamodels/dataqueries.go/dataQuerySearch: Request for query-id=%s , query-sql=%s", queryId, finalQuery)

	finalCountQuery := SqlQuery2StringTypes(countQuery, argsQuery...)

	// Init the cursor
	if cursor == nil {
		cursor = &queryCursor{
			queryId:     queryId,
			after:       -1,
			versionTime: versionTime,
		}
	}
	if cursor.queryId == queryId {
		// Try to get results from redis
		results := q.getQueryResultsfromCache(ctx, *cursor, args.First)
		if results != nil {
			results.PageInfo.queryId = queryId
			results.PageInfo.countQuery = finalCountQuery
			results.PageInfo.datasetID = args.DatasetID
			log.Printf("datamodels/dataqueries.go/dataQuerySearch: The query-id=%s was resolved from cache", queryId)
			return results, nil
		}
	} else {
		cursor.queryId = queryId
	}

	// Run the query
	db := q.GetDatasetDB(args.DatasetID)
	query = db.Rebind(query)
	log.Printf("datamodels/dataqueries.go/dataQuerySearch: Running query-id=%s in the DB", queryId)
	rows, err := db.QueryxContext(ctx, query, argsQuery...)
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/dataQuerySearch: %s", err)
		return nil, err
	}

	// Return results
	results := DAQueryResultConnection{
		Edges: []*DAQueryResultEdge{},
		PageInfo: &DAPageInfo{
			HasNextPage: false,
			queryId:     queryId,
			countQuery:  finalCountQuery,
			datasetID:   args.DatasetID,
		},
	}

	countRows := 0
	cacheKey := fmt.Sprintf(config.DATA_QUERY_CACHE_KEY, cursor.queryId)
	cacheShardId := 0
	cacheShard := map[int]DAQueryResultCache{}
	cacheShardCount := 0

	for rows.Next() {
		// Scan the row
		rowMap := map[string]interface{}{}
		err := rows.MapScan(rowMap)
		if err != nil {
			log.Printf("ERROR: datamodels/dataqueries.go/dataQuerySearch: %s", err)
			return nil, err
		}

		rowResult := DAQueryResult{
			DimensionMembers: []*DADimensionMember{},
		}
		cacheResult := DAQueryResultCache{
			DimensionMembers: []*DADimensionMember{},
			MeasureValues:    []sql.NullString{},
		}

		// Build the DimensionMembers
		for _, dimLevel := range args.DimensionLevels {
			var descPointer *string = nil
			if rowMap[dimLevel+"_description"] != nil {
				desc := ValueToString(rowMap[dimLevel+"_description"], "", nil)
				descPointer = &desc
			}
			var extIDPointer *string = nil
			if rowMap[dimLevel+"_external_id"] != nil {
				extID := ValueToString(rowMap[dimLevel+"_external_id"], "", nil)
				extIDPointer = &extID
			}

			rowResult.DimensionMembers = append(rowResult.DimensionMembers, &DADimensionMember{
				DatasetID:                fmt.Sprintf("%d", args.DatasetID),
				ID:                       ValueToString(rowMap[dimLevel+"_id"], "", nil),
				DimensionLevelColumnName: dimLevel,
				Name:                     ValueToString(rowMap[dimLevel+"_name"], "", nil),
				Description:              descPointer,
				ExternalID:               extIDPointer,
			})
		}

		if cacheShardCount >= config.DEFAULT_DATA_QUERY_CACHE_SHARD {
			// Save the current shard
			q.setCacheQueryShard(ctx, cacheKey, strconv.Itoa(cacheShardId), cacheShard)
			cacheShardId += 1
			cacheShard = map[int]DAQueryResultCache{}
			cacheShardCount = 0

		}
		cacheResult.DimensionMembers = rowResult.DimensionMembers
		cacheShard[countRows] = cacheResult
		cacheShardCount++

		if countRows > queryOffset && countRows <= args.First+queryOffset {
			// Add this row in the result set
			cursor.after = countRows
			results.Edges = append(results.Edges, &DAQueryResultEdge{
				Node:   &rowResult,
				Cursor: q.encodeQueryCursor(*cursor),
			})
		}
		countRows++
	}

	if countRows > 0 {
		if len(cacheShard) > 0 {
			// Save the current shard
			q.setCacheQueryShard(ctx, cacheKey, strconv.Itoa(cacheShardId), cacheShard)
		}
		// Set the cache expiration
		q.setCacheQueryExpiration(ctx, cacheKey, config.BASE_CACHE_EXPIRATION())

		if countRows >= args.First+queryOffset {
			results.PageInfo.HasNextPage = true
		}
	}

	return &results, nil
}

func (q *Queries) getQueryResultsfromCache(ctx context.Context, cursor queryCursor, limit int) *DAQueryResultConnection {
	cacheKey := fmt.Sprintf(config.DATA_QUERY_CACHE_KEY, cursor.queryId)
	shardId := (cursor.after + 1) / config.DEFAULT_DATA_QUERY_CACHE_SHARD
	field := strconv.Itoa(shardId)
	after := cursor.after

	if q.rdb.Exists(ctx, cacheKey).Val() == 0 || !q.rdb.HExists(ctx, cacheKey, field).Val() {
		return nil
	}

	var pageFlag bool
	results := DAQueryResultConnection{
		Edges: []*DAQueryResultEdge{},
		PageInfo: &DAPageInfo{
			HasNextPage: false,
		},
	}

	val, _ := q.getCacheQueryShard(ctx, cacheKey, field)
	for i := after + 1; i <= (after + limit + 1); i++ {
		shardId = i / config.DEFAULT_DATA_QUERY_CACHE_SHARD
		//change shard
		if shardId > (i-1)/config.DEFAULT_DATA_QUERY_CACHE_SHARD {
			//get hash val
			val, _ = q.getCacheQueryShard(ctx, cacheKey, strconv.Itoa(shardId))
			if len(val) > 0 && (i == after+limit+1) {
				pageFlag = true
			}
		}

		//check if data exists or not
		if i-shardId*config.DEFAULT_DATA_QUERY_CACHE_SHARD < len(val) {
			if i < after+limit+1 {
				cursor.after = i
				rowMapCache := val[i]
				rowMap := DAQueryResult{DimensionMembers: rowMapCache.DimensionMembers, MeasureValues: []*string{}}
				for _, val := range rowMapCache.MeasureValues {
					if val.Valid {
						var value string
						value = val.String
						rowMap.MeasureValues = append(rowMap.MeasureValues, &value)
					} else {
						rowMap.MeasureValues = append(rowMap.MeasureValues, nil)
					}
				}
				results.Edges = append(results.Edges, &DAQueryResultEdge{
					Node:   &rowMap,
					Cursor: q.encodeQueryCursor(cursor),
				})
			} else {
				pageFlag = true
			}
		} else {
			pageFlag = false
			break
		}
	}
	results.PageInfo.HasNextPage = pageFlag

	return &results
}

func (q *Queries) encodeQueryCursor(cursor queryCursor) string {
	cursorStr := fmt.Sprintf("%s/%d/%s", cursor.queryId, cursor.after, cursor.versionTime)
	return base64.StdEncoding.EncodeToString([]byte(cursorStr))
}

func (q *Queries) decodeQueryCursor(cursorB64 string) *queryCursor {
	cursor, err := base64.StdEncoding.DecodeString(cursorB64)
	if err != nil {
		log.Printf("WARNING: datamodels/dataqueries.go/decodeQueryCursor: Invalid cursor (%s)", err)
		return nil
	}

	// Parsing the Cursor
	cursorParts := strings.Split(string(cursor), "/")
	if len(cursorParts) != 3 {
		log.Printf("WARNING: datamodels/dataqueries.go/decodeQueryCursor: Invalid cursor (invalid parts)")
		return nil
	}
	after, err := strconv.Atoi(cursorParts[1])
	if err != nil {
		log.Printf("WARNING: datamodels/dataqueries.go/decodeQueryCursor: Invalid cursor (%s)", err)
		return nil
	}

	return &queryCursor{
		queryId:     cursorParts[0],
		after:       after,
		versionTime: cursorParts[2],
	}
}

func (q *Queries) setCacheQueryShard(ctx context.Context, key string, field string, value interface{}) (bool, error) {
	var b bytes.Buffer
	e := gob.NewEncoder(&b)
	if err := e.Encode(value); err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/setCacheQueryShard: %s", err)
		return false, err
	}
	serializedValue := b.Bytes()
	err := q.rdb.HSet(ctx, key, field, serializedValue).Err()
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/setCacheQueryShard: %s", err)
		return false, err
	}
	return true, nil
}

func (q *Queries) setCacheQueryTotalRows(ctx context.Context, key string, totalRows int) (bool, error) {
	err := q.rdb.HSet(ctx, key, "totalRows", totalRows).Err()
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/setCacheQueryTotalRows: %s", err)
		return false, err
	}
	return true, nil
}

func (q *Queries) setCacheQueryExpiration(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	err := q.rdb.Expire(ctx, key, expiration).Err()
	if err != nil {
		log.Printf("ERROR: datamodels/dataqueries.go/setCacheQueryExpiration: %s", err)
		return false, err
	}
	return true, nil
}

func (q *Queries) getCacheQueryShard(ctx context.Context, key string, field string) (map[int]DAQueryResultCache, error) {
	deserializedValue := map[int]DAQueryResultCache{}
	serializedValue, _ := q.rdb.HGet(ctx, key, field).Bytes()
	if len(serializedValue) > 0 {
		b := bytes.NewBuffer(serializedValue)
		dec := gob.NewDecoder(b)
		err := dec.Decode(&deserializedValue)
		if err != nil {
			log.Printf("ERROR: datamodels/dataqueries.go/getCacheQueryShard: %s", err)
		}
	}
	return deserializedValue, nil
}

func (q *Queries) getCacheQueryTotalRows(ctx context.Context, key string) (int, error) {
	return q.rdb.HGet(ctx, key, "totalRows").Int()
}

func (q *Queries) GetTotalRows(ctx context.Context, obj *DAPageInfo) (*int, error) {
	if len(obj.queryId) == 0 || len(obj.countQuery) == 0 {
		return nil, nil
	}

	cacheKey := fmt.Sprintf(config.DATA_QUERY_CACHE_KEY, obj.queryId)
	var totalRows int
	var err error
	totalRows, err = q.getCacheQueryTotalRows(ctx, cacheKey)
	if err != nil {
		// if not defined in cache
		log.Printf("datamodels/dataqueries.go/GetTotalRows: Running count-query=%s in the DB for queryId=%s", obj.countQuery, obj.queryId)
		db := q.GetDatasetDB(obj.datasetID)
		err = db.QueryRowContext(ctx, obj.countQuery).Scan(&totalRows)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Printf("ERROR: datamodels/dataqueries.go/GetTotalRows: getting total rows=%s", err)
				return nil, err
			}
			totalRows = 0
		}
		q.setCacheQueryTotalRows(ctx, cacheKey, totalRows)
	}
	return &totalRows, nil

}

func (q *Queries) collectQueryDimLevels(queryDimensionLevels []string, queryScope *DAQueryScopeInput) []string {
	if queryScope == nil {
		return queryDimensionLevels
	}

	var dimensionLevels []string
	dimensions := make(map[string]string)
	for _, each := range queryDimensionLevels {
		if _, ok := dimensions[each]; !ok {
			dimensions[each] = each
			dimensionLevels = append(dimensionLevels, each)
		}
	}

	for _, each := range queryScope.DimensionFilters {
		for _, dimensionLevel := range each.AND {
			if _, ok := dimensions[dimensionLevel.DimensionLevelColumnName]; !ok {
				dimensions[dimensionLevel.DimensionLevelColumnName] = dimensionLevel.DimensionLevelColumnName
				dimensionLevels = append(dimensionLevels, dimensionLevel.DimensionLevelColumnName)
			}
		}

		for _, dimensionLevel := range each.OR {
			if _, ok := dimensions[dimensionLevel.DimensionLevelColumnName]; !ok {
				dimensions[dimensionLevel.DimensionLevelColumnName] = dimensionLevel.DimensionLevelColumnName
				dimensionLevels = append(dimensionLevels, dimensionLevel.DimensionLevelColumnName)
			}
		}
	}
	return dimensionLevels

}
