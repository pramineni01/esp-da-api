package datamodels

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
)

// Model

type DASQLQuery struct {
	Metadata           []DASQLQueryColumn
	Query              string
	PartitionedQueries []string
}

type DASQLQueryColumn struct {
	Name string            `json:"name"`
	Type DAMeasureCastType `json:"type"`
}

type DADataViewAlias struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// Queries

// Create View

type CreateDataviewParams struct {
	Datatable           *DADatatable
	DimensionLevels     []string
	AggregatedMeasures  []string
	Version             *string
	BranchID            *string
	UserID              *string
	Scope               *DAQueryScopeInput
	AllData             bool
	LocaleID            *string
	DimMemberAttributes []DADimensionMemberAttribute
	Aliases             []*DADataViewAlias
	Partitioned         bool
}

func (q *Queries) CreateView(ctx context.Context, datasetID int, args CreateDataviewParams) (*DASQLQuery, error) {
	// ToDo:
	// - Apply validations (eg: make sure BASE_ONLY measures are at the lowest level)

	// Validate the query
	var userId *string
	if !args.AllData {
		userId = args.UserID
	}
	validation, err := q.validateQuery(ctx, validateQueryParams{
		datasetId:     datasetID,
		datatableName: args.Datatable.TableName,
		dimLevels:     args.DimensionLevels,
		aggrMeasures:  args.AggregatedMeasures,
		scope:         args.Scope,
		branchID:      args.BranchID,
		version:       args.Version,
		userID:        userId,
	})
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataviews.go/CreateView: [validateQuery] %s", err)
		return nil, err
	}

	versionTime := validation.version.AppliedTimestamp.Format(config.DEFAULT_TIME_FORMAT)
	branchTime := versionTime
	if validation.branch != nil && validation.branch.FromTimestamp.Valid {
		branchTime = validation.branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)
	}

	localeMode := false
	localeID := config.DEFAULT_LOCALE_ID
	if len(args.DimMemberAttributes) > 0 {
		localeMode = true
		if args.LocaleID != nil && len(*args.LocaleID) > 0 {
			localeID, err = strconv.Atoi(*args.LocaleID)
			if err != nil {
				log.Printf("ERROR: datamodels/dataviews.go/CreateView [LocaleId]: %s", err)
				return nil, err
			}
		}
	}

	mapAliases := map[string]string{}
	if len(args.Aliases) > 0 {
		for _, alias := range args.Aliases {
			mapAliases[alias.Src] = alias.Dst
		}
	}

	queryDimLevel := ""
	queryDimLevelAliases := ""
	groupByDimLevel := ""
	for _, dimLevel := range args.DimensionLevels {
		queryDimLevel += dimLevel + "_id, "
		queryDimLevelAliases += dimLevel + "_id"
		if dst, ok := mapAliases[dimLevel+"_id"]; ok {
			queryDimLevelAliases += fmt.Sprintf(" AS %s", dst)
		}
		queryDimLevelAliases += ", "
		if localeMode {
			for _, dimMemAtt := range args.DimMemberAttributes {
				switch dimMemAtt {
				case DADimensionMemberAttributeName:
					queryDimLevel += dimLevel + "_name, "
					queryDimLevelAliases += dimLevel + "_name"
					if dst, ok := mapAliases[dimLevel+"_name"]; ok {
						queryDimLevelAliases += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevelAliases += ", "
				case DADimensionMemberAttributeDescription:
					queryDimLevel += dimLevel + "_description, "
					queryDimLevelAliases += dimLevel + "_description"
					if dst, ok := mapAliases[dimLevel+"_description"]; ok {
						queryDimLevelAliases += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevelAliases += ", "
				case DADimensionMemberAttributeExternalID:
					queryDimLevel += dimLevel + "_external_id, "
					queryDimLevelAliases += dimLevel + "_external_id"
					if dst, ok := mapAliases[dimLevel+"_external_id"]; ok {
						queryDimLevelAliases += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevelAliases += ", "
				}
			}
		}
		groupByDimLevel += dimLevel + "_id, "
	}
	queryDimLevel = queryDimLevel[:len(queryDimLevel)-2]
	queryDimLevelAliases = queryDimLevelAliases[:len(queryDimLevelAliases)-2]
	groupByDimLevel = groupByDimLevel[:len(groupByDimLevel)-2]

	// Try to get a cache layer
	cacheLayerId, _ := q.getMatchingCacheLayer(ctx, getMatchingCacheLayerParams{
		DatasetID:       datasetID,
		DatatableID:     validation.datatable.id,
		DimensionLevels: args.DimensionLevels,
	})

	var baseMeasureId string
	var dimDataTable string
	if cacheLayerId == nil {
		baseMeasureId = "base_id"
		dimDataTable = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA, validation.datatable.id)
		if localeMode {
			dimDataTable = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA_LOCALE, validation.datatable.id, localeID)
		}
	} else {
		baseMeasureId = fmt.Sprintf("cache_layer_%d_id", *cacheLayerId)
		dimDataTable = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA, *cacheLayerId)
		if localeMode {
			dimDataTable = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA_LOCALE, *cacheLayerId, localeID)
		}
	}

	getDataViewQuery := func(partition *int) (string, error) {
		var argsQuery []interface{}

		partitionSql := ""
		if partition != nil {
			partitionSql = fmt.Sprintf(" PARTITION (p%d)", *partition)
		}

		// Construct CTE expressions
		cteTableName := []string{}
		cteTablesMap := map[string]string{}

		if cacheLayerId == nil {
			// Use the measures map
			for _, measureInfo := range validation.measuresMap {
				tableName := fmt.Sprintf("dt%d_m%d", validation.datatable.id, measureInfo.MeasureID)
				measureTableName := fmt.Sprintf(config.DEFAULT_DATATABLE_BASE_MEASURES, validation.datatable.id, measureInfo.MeasureDataType)

				query := fmt.Sprintf("%s AS ( SELECT measure_id, %s, value ", tableName, baseMeasureId)
				query += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableName, partitionSql, branchTime)
				query += fmt.Sprintf("WHERE measure_id = %d ", measureInfo.MeasureID)
				query += ")"

				cteTableName = append(cteTableName, tableName)
				cteTablesMap[tableName] = query

				if validation.branch != nil {
					tableName := fmt.Sprintf("dt%d_m%d_branches", validation.datatable.id, measureInfo.MeasureID)
					measureTableNameBranch := fmt.Sprintf(config.DEFAULT_DATATABLE_BASE_MEASURES_BRANCHES, validation.datatable.id, measureInfo.MeasureDataType)

					query := fmt.Sprintf("%s AS ( SELECT measure_id, %s, value ", tableName, baseMeasureId)
					query += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableNameBranch, partitionSql, versionTime)
					query += fmt.Sprintf("WHERE measure_id = %d AND branch_id = %d ", measureInfo.MeasureID, validation.branch.id)
					query += ")"

					cteTableName = append(cteTableName, tableName)
					cteTablesMap[tableName] = query
				}
			}
		} else {
			// Use the aggregated measures map
			for _, aggMeasureInfo := range validation.aggrMeasuresMap {
				// Ignore base_only extension and formulas here
				if aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_BASE_ONLY ||
					aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA ||
					aggMeasureInfo.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
					continue
				}
				if aggMeasureInfo.MeasureID == nil {
					// This shouldn't really happen
					continue
				}

				tableName := fmt.Sprintf("dt%d_m%d", validation.datatable.id, aggMeasureInfo.MAggregationID)
				measureInfo := validation.measuresMap[*aggMeasureInfo.MeasureID]
				measureTableName := fmt.Sprintf(config.DEFAULT_CACHELAYER_BASE_MEASURES, *cacheLayerId, measureInfo.MeasureDataType)

				query := fmt.Sprintf("%s AS ( SELECT measure_aggregation_id, %s, value ", tableName, baseMeasureId)
				query += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableName, partitionSql, branchTime)
				query += fmt.Sprintf("WHERE measure_aggregation_id = %d ", aggMeasureInfo.MAggregationID)
				query += ")"

				cteTableName = append(cteTableName, tableName)
				cteTablesMap[tableName] = query

				if validation.branch != nil {
					tableName := fmt.Sprintf("dt%d_m%d_branches", validation.datatable.id, aggMeasureInfo.MAggregationID)
					measureTableNameBranch := fmt.Sprintf(config.DEFAULT_CACHELAYER_BASE_MEASURES_BRANCHES, *cacheLayerId, measureInfo.MeasureDataType)

					query := fmt.Sprintf("%s AS ( SELECT measure_aggregation_id, %s, value ", tableName, baseMeasureId)
					query += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableNameBranch, partitionSql, versionTime)
					query += fmt.Sprintf("WHERE measure_aggregation_id = %d AND branch_id = %d ", aggMeasureInfo.MAggregationID, validation.branch.id)
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

		// Construct the pivoted table
		pivoted := "pivoted as ( SELECT " + queryDimLevel
		for aggMeasureName, aggMeasure := range validation.aggrMeasuresMap {
			if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA ||
				aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				continue
			}
			if aggMeasure.MeasureID == nil {
				// This shouldn't really happen
				continue
			}
			measureInfo, ok := validation.measuresMap[*aggMeasure.MeasureID]
			if !ok {
				err := errors.New(fmt.Sprintf("Couldn't find info for measure-id=%d", *aggMeasure.MeasureID))
				log.Printf("ERROR: datamodels/dataqueries.go/DataQuery: %s", err)
				return "", err
			}

			if cacheLayerId == nil {
				aggFunc := DAAggregationTypeMax
				if aggMeasure.MAggregationType != config.MEASURE_AGGREGATION_TYPE_BASE_ONLY {
					aggFunc = aggMeasure.MAggregationType
				}

				if validation.branch == nil {
					pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_id=%d THEN CAST(dt%d_m%d.value AS %s) END) AS %s ",
						aggFunc, validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
						validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
				} else {
					pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_id=%d OR dt%d_m%d_branches.measure_id=%d THEN CAST( IF(dt%d_m%d_branches.measure_id IS NOT NULL, dt%d_m%d_branches.value, dt%d_m%d.value) AS %s) END) AS %s ",
						aggFunc, validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
						validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
						validation.datatable.id, measureInfo.MeasureID, validation.datatable.id, measureInfo.MeasureID,
						validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
				}
			} else {
				if validation.branch == nil {
					pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_aggregation_id=%d THEN CAST(dt%d_m%d.value AS %s) END) AS %s ",
						aggMeasure.MAggregationType, validation.datatable.id, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
						validation.datatable.id, aggMeasure.MAggregationID, aggMeasure.MAggregationCastType, aggMeasureName)
				} else {
					pivoted += fmt.Sprintf(", %s(CASE WHEN dt%d_m%d.measure_aggregation_id=%d OR dt%d_m%d_branches.measure_aggregation_id=%d THEN CAST( IF(dt%d_m%d_branches.measure_aggregation_id IS NOT NULL, dt%d_m%d_branches.value, dt%d_m%d.value) AS %s) END) AS %s ",
						aggMeasure.MAggregationType, validation.datatable.id, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
						validation.datatable.id, aggMeasure.MAggregationID, aggMeasure.MAggregationID,
						validation.datatable.id, aggMeasure.MAggregationID, validation.datatable.id, aggMeasure.MAggregationID,
						validation.datatable.id, aggMeasure.MAggregationID, aggMeasure.MAggregationCastType, aggMeasureName)
				}
			}
		}
		pivoted += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' AS base ", dimDataTable, partitionSql, branchTime)
		for _, tableName := range cteTableName {
			pivoted += fmt.Sprintf("LEFT JOIN %s ON (%s.%s = base.%s) ", tableName, tableName, baseMeasureId, baseMeasureId)
		}

		whereScope, err := q.queryScopeToWhereConditions(ctx, datasetID, validation.datatable.id, userId, args.Scope, false)
		if err != nil {
			log.Printf("ERROR: datamodels/dataviews.go/CreateView: %s [queryScopeToWhereConditions]", err)
			return "", err
		}
		if whereScope != nil && len(*whereScope) > 0 {
			pivoted += "WHERE " + *whereScope + " "
		}

		pivoted += "GROUP BY " + groupByDimLevel
		pivoted += ")"

		// Construct the final query
		query := "WITH "
		query += strings.Join(cteTables, ", ")
		query += ", " + pivoted + " "
		query += "SELECT " + queryDimLevelAliases
		for _, aggMeasureName := range args.AggregatedMeasures {
			aggMeasure, ok := validation.aggrMeasuresMap[aggMeasureName]
			if !ok {
				log.Printf("WARNING: datamodels/dataviews.go/CreateView: ignoring agg-measure=%s", aggMeasureName)
				continue
			}

			if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
				asName := aggMeasureName
				if dst, ok := mapAliases[aggMeasureName]; ok {
					asName = dst
				}
				query += fmt.Sprintf(", (%s) AS %s", *aggMeasure.MAggregationFormula, asName)
			} else if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				// ignore extension type
			} else {
				if dst, ok := mapAliases[aggMeasureName]; ok {
					query += fmt.Sprintf(", %s AS %s", aggMeasureName, dst)
				} else {
					query += fmt.Sprintf(", %s", aggMeasureName)
				}
			}
		}
		query += " FROM pivoted"

		query = SqlQuery2String(query, argsQuery...)
		query = strings.ReplaceAll(query, "\n", " ")
		log.Printf("INFO: datamodels/dataviews.go/CreateView: Returning query=%s", query)
		return query, nil
	}

	result := DASQLQuery{}
	if args.Partitioned {
		result.PartitionedQueries = []string{}
		for p := 0; p < args.Datatable.Partitions; p++ {
			query, err := getDataViewQuery(&p)
			if err != nil {
				return nil, err
			}
			result.PartitionedQueries = append(result.PartitionedQueries, query)
		}
	} else {
		query, err := getDataViewQuery(nil)
		if err != nil {
			return nil, err
		}

		result.Query = query
	}

	// Resolve Metadata
	result.Metadata = []DASQLQueryColumn{}
	for _, dl := range args.DimensionLevels {
		name := dl + "_id"
		if dst, ok := mapAliases[name]; ok {
			name = dst
		}
		queryColumn := DASQLQueryColumn{
			Name: name,
			Type: DAMeasureCastTypeUnsignedInteger,
		}
		result.Metadata = append(result.Metadata, queryColumn)
		if localeMode {
			var queryColumn DASQLQueryColumn
			for _, dimMemAtt := range args.DimMemberAttributes {
				switch dimMemAtt {
				case DADimensionMemberAttributeName:
					name := dl + "_name"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DASQLQueryColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				case DADimensionMemberAttributeDescription:
					name := dl + "_description"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DASQLQueryColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				case DADimensionMemberAttributeExternalID:
					name := dl + "_external_id"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DASQLQueryColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				}
				result.Metadata = append(result.Metadata, queryColumn)
			}
		}
	}
	for _, aggMeasureName := range args.AggregatedMeasures {
		aggMeasure := validation.aggrMeasuresMap[aggMeasureName]
		name := aggMeasure.MAggregationColumnName
		if dst, ok := mapAliases[name]; ok {
			name = dst
		}

		queryColumn := DASQLQueryColumn{
			Name: name,
			Type: aggMeasure.MAggregationCastType,
		}
		result.Metadata = append(result.Metadata, queryColumn)
	}

	return &result, nil
}
