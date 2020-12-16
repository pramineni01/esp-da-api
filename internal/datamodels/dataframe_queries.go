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

type DADataframeColumn struct {
	Name string            `json:"name"`
	Type DAMeasureCastType `json:"type"`
}

type DADataframeDimLevelQuery struct {
	Query     string               `json:"query"`
	Partition int                  `json:"partition"`
	Metadata  []*DADataframeColumn `json:"metadata"`
}

type DADataframeMeasureQuery struct {
	MeasureName string               `json:"measureName"`
	Query       string               `json:"query"`
	Partition   int                  `json:"partition"`
	Metadata    []*DADataframeColumn `json:"metadata"`
}

type DADataframeQueries struct {
	Partitions      int                         `json:"partitions"`
	DimLevelQueries []*DADataframeDimLevelQuery `json:"dimLevelQueries"`
	MeasureQueries  []*DADataframeMeasureQuery  `json:"measureQueries"`
}

type GetDataframeQueriesParams struct {
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
}

const dataframeDimLevelQuery = `
	SELECT %s
	FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s'
	WHERE 1=1 %s
	GROUP BY %s`

const dataframeDimLevelPartitionedQuery = `
	SELECT %s
	FROM %s PARTITION (p%d) FOR SYSTEM_TIME AS OF TIMESTAMP'%s'
	WHERE 1=1 %s`

func (q *Queries) GetDataframeQueries(ctx context.Context, args GetDataframeQueriesParams) (*DADataframeQueries, error) {
	datasetId := args.Datatable.datasetId
	log.Printf("INFO: datamodels/dataframe_queries.go/GetDataframeQueries: Start Processing ------------------------------------------")

	// Validate the query
	var userId *string
	if !args.AllData {
		userId = args.UserID
	}
	validation, err := q.validateQuery(ctx, validateQueryParams{
		datasetId:     datasetId,
		datatableName: args.Datatable.TableName,
		dimLevels:     args.DimensionLevels,
		aggrMeasures:  args.AggregatedMeasures,
		scope:         args.Scope,
		branchID:      args.BranchID,
		version:       args.Version,
		userID:        userId,
	})
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s [validateQuery]", err)
		return nil, err
	}

	// Is this query at the lowest level
	dimLevelsByDim, err := q.getDimLevelsByDim(ctx, args.Datatable, args.DimensionLevels)
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s [getDimLevelsByDim]", err)
		return nil, err
	}
	lowestLevel, err := q.areDimLevelsAtLowestLevel(ctx, datasetId, args.Datatable.id, dimLevelsByDim)
	if err != nil {
		fmt.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s [areDimLevelsAtLowestLevel]", err)
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
				log.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s [LocaleId]", err)
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

	// Try to get a cache layer
	dimensionLevels := q.collectQueryDimLevels(args.DimensionLevels, args.Scope)
	cacheLayerId, _ := q.getMatchingCacheLayer(ctx, getMatchingCacheLayerParams{
		DatasetID:       datasetId,
		DatatableID:     validation.datatable.id,
		DimensionLevels: dimensionLevels,
	})

	var baseMeasureId string
	var dimDataTable string
	var dimDataTableWLocales string
	if cacheLayerId == nil {
		baseMeasureId = "base_id"
		dimDataTable = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA, validation.datatable.id)
		dimDataTableWLocales = dimDataTable
		if localeMode {
			dimDataTableWLocales = fmt.Sprintf(config.DEFAULT_DATATABLE_DIMENSION_DATA_LOCALE, validation.datatable.id, localeID)
		}
	} else {
		baseMeasureId = fmt.Sprintf("cache_layer_%d_id", *cacheLayerId)
		dimDataTable = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA, *cacheLayerId)
		dimDataTableWLocales = dimDataTable
		if localeMode {
			dimDataTableWLocales = fmt.Sprintf(config.DEFAULT_CACHELAYER_DIMENSION_DATA_LOCALE, *cacheLayerId, localeID)
		}
	}

	// Create the result
	result := &DADataframeQueries{
		Partitions:      1,
		DimLevelQueries: []*DADataframeDimLevelQuery{},
		MeasureQueries:  []*DADataframeMeasureQuery{},
	}

	// Load the DimLevelQueries
	// ----------------------------------------------------
	var queryDimLevel string
	if lowestLevel {
		queryDimLevel = fmt.Sprintf("%s AS base_id, ", baseMeasureId)
	} else {
		queryDimLevel = fmt.Sprintf("MIN(%s) AS base_id, ", baseMeasureId)
	}

	groupByDimLevel := ""
	for _, dimLevel := range args.DimensionLevels {
		queryDimLevel += dimLevel + "_id"
		if dst, ok := mapAliases[dimLevel+"_id"]; ok {
			queryDimLevel += fmt.Sprintf(" AS %s", dst)
		}
		queryDimLevel += ", "
		if localeMode {
			for _, dimMemAtt := range args.DimMemberAttributes {
				switch dimMemAtt {
				case DADimensionMemberAttributeName:
					queryDimLevel += dimLevel + "_name"
					if dst, ok := mapAliases[dimLevel+"_name"]; ok {
						queryDimLevel += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevel += ", "
				case DADimensionMemberAttributeDescription:
					queryDimLevel += dimLevel + "_description"
					if dst, ok := mapAliases[dimLevel+"_description"]; ok {
						queryDimLevel += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevel += ", "
				case DADimensionMemberAttributeExternalID:
					queryDimLevel += dimLevel + "_external_id"
					if dst, ok := mapAliases[dimLevel+"_external_id"]; ok {
						queryDimLevel += fmt.Sprintf(" AS %s", dst)
					}
					queryDimLevel += ", "
				}
			}
		}
		groupByDimLevel += dimLevel + "_id, "
	}
	queryDimLevel = queryDimLevel[:len(queryDimLevel)-2]
	groupByDimLevel = groupByDimLevel[:len(groupByDimLevel)-2]

	whereScope, err := q.queryScopeToWhereConditions(ctx, datasetId, validation.datatable.id, userId, args.Scope, false)
	if err != nil {
		log.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s [queryScopeToWhereConditions]", err)
		return nil, err
	}
	whereClause := ""
	if whereScope != nil && len(*whereScope) > 0 {
		whereClause = "AND " + *whereScope + " "
	}

	// Resolve dim levels metadata
	dimLevelsMetadata := []*DADataframeColumn{
		&DADataframeColumn{
			Name: "base_id",
			Type: DAMeasureCastTypeUnsignedInteger,
		},
	}
	for _, dl := range args.DimensionLevels {
		name := dl + "_id"
		if dst, ok := mapAliases[name]; ok {
			name = dst
		}
		dimLevelsMetadata = append(dimLevelsMetadata, &DADataframeColumn{
			Name: name,
			Type: DAMeasureCastTypeUnsignedInteger,
		})

		if localeMode {
			for _, dimMemAtt := range args.DimMemberAttributes {
				var queryColumn DADataframeColumn
				switch dimMemAtt {
				case DADimensionMemberAttributeName:
					name := dl + "_name"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DADataframeColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				case DADimensionMemberAttributeDescription:
					name := dl + "_description"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DADataframeColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				case DADimensionMemberAttributeExternalID:
					name := dl + "_external_id"
					if dst, ok := mapAliases[name]; ok {
						name = dst
					}
					queryColumn = DADataframeColumn{
						Name: name,
						Type: DAMeasureCastTypeChar,
					}
				}
				dimLevelsMetadata = append(dimLevelsMetadata, &queryColumn)
			}
		}
	}

	if lowestLevel {
		result.Partitions = args.Datatable.Partitions
		for p := 0; p < args.Datatable.Partitions; p++ {
			query := fmt.Sprintf(dataframeDimLevelPartitionedQuery, queryDimLevel, dimDataTableWLocales, p, versionTime, whereClause)
			result.DimLevelQueries = append(result.DimLevelQueries, &DADataframeDimLevelQuery{
				Query:     SqlQueryTrim(query),
				Partition: p,
				Metadata:  dimLevelsMetadata,
			})
			log.Printf("INFO: datamodels/dataframe_queries.go/GetDataframeQueries: For dim levels returning query=%s [partition=%d]", query, p)
		}
	} else {
		query := fmt.Sprintf(dataframeDimLevelQuery, queryDimLevel, dimDataTableWLocales, versionTime, whereClause, groupByDimLevel)
		result.DimLevelQueries = append(result.DimLevelQueries, &DADataframeDimLevelQuery{
			Query:     SqlQueryTrim(query),
			Partition: 0,
			Metadata:  dimLevelsMetadata,
		})
		log.Printf("INFO: datamodels/dataframe_queries.go/GetDataframeQueries: For dim levels returning query=%s", query)
	}

	// Load the MeasureQueries
	// ----------------------------------------------------
	getMeasureQueriesQuery := func(aggMeasureName string, partition *int) (string, error) {
		aggMeasure := validation.aggrMeasuresMap[aggMeasureName]
		var argsQuery []interface{}

		partitionSql := ""
		partitionLabel := ""
		if partition != nil {
			partitionSql = fmt.Sprintf(" PARTITION (p%d)", *partition)
			partitionLabel = fmt.Sprintf("[partition=%d]", *partition)
		}

		// Define the list of aggregated measures and measures involved
		aggMeasureDependencies := []*AggrMeasureForMap{aggMeasure}
		measureDependencies := []*MeasureForMap{}
		if aggMeasure.MeasureID != nil {
			measureDependencies = append(measureDependencies, validation.measuresMap[*aggMeasure.MeasureID])
		}
		if aggMeasure.MADependencies != nil {
			// Process all dependencies
			for _, childAggMeasureName := range aggMeasure.MADependencies {
				childAggMeasure := validation.aggrMeasuresMap[childAggMeasureName]
				if childAggMeasure.MeasureID == nil {
					err = fmt.Errorf("aggregated-measure=%s, formulas of formulas not supported", aggMeasureName)
					log.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s", err)
					return "", err
				}

				aggMeasureDependencies = append(aggMeasureDependencies, childAggMeasure)
				measureDependencies = append(measureDependencies, validation.measuresMap[*childAggMeasure.MeasureID])
			}
		}

		// Construct CTE expressions
		cteTableName := []string{}
		cteTablesMap := map[string]string{}

		if cacheLayerId == nil {
			// Use the measures map
			for _, measureInfo := range measureDependencies {
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
			for _, aggMeasureInfo := range aggMeasureDependencies {
				// Ignore base_only and formulas here
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
				query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableName, branchTime)
				query += fmt.Sprintf("WHERE measure_aggregation_id = %d ", aggMeasureInfo.MAggregationID)
				query += ")"

				cteTableName = append(cteTableName, tableName)
				cteTablesMap[tableName] = query

				if validation.branch != nil {
					tableName := fmt.Sprintf("dt%d_m%d_branches", validation.datatable.id, aggMeasureInfo.MAggregationID)
					measureTableNameBranch := fmt.Sprintf(config.DEFAULT_CACHELAYER_BASE_MEASURES_BRANCHES, *cacheLayerId, measureInfo.MeasureDataType)

					query := fmt.Sprintf("%s AS ( SELECT measure_aggregation_id, %s, value ", tableName, baseMeasureId)
					query += fmt.Sprintf("FROM %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' ", measureTableNameBranch, versionTime)
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
		singleMeasureMode := len(measureDependencies) == 1
		singleTableMode := false
		pivoted := "pivoted as ( "
		if lowestLevel {
			if len(cteTableName) == 1 && len(whereClause) == 0 {
				singleTableMode = true
				pivoted += fmt.Sprintf("SELECT %s.%s AS base_id ", cteTableName[0], baseMeasureId)
			} else {
				pivoted += fmt.Sprintf("SELECT base.%s AS base_id ", baseMeasureId)
			}
		} else {
			pivoted += fmt.Sprintf("SELECT MIN(base.%s) AS base_id ", baseMeasureId)
		}
		for _, aggMeasure := range aggMeasureDependencies {
			if aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA ||
				aggMeasure.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
				continue
			}
			if aggMeasure.MeasureID == nil {
				// This shouldn't really happen
				continue
			}

			aggMeasureName := aggMeasure.MAggregationColumnName
			measureInfo, ok := validation.measuresMap[*aggMeasure.MeasureID]
			if !ok {
				err := errors.New(fmt.Sprintf("Couldn't find info for measure-id=%d", *aggMeasure.MeasureID))
				log.Printf("ERROR: datamodels/dataframe_queries.go/GetDataframeQueries: %s", err)
				return "", err
			}

			if cacheLayerId == nil {
				if lowestLevel && singleMeasureMode {
					if validation.branch == nil {
						pivoted += fmt.Sprintf(", CASE WHEN dt%d_m%d.measure_id=%d THEN CAST(dt%d_m%d.value AS %s) END AS %s ",
							validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
							validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
					} else {
						pivoted += fmt.Sprintf(", CASE WHEN dt%d_m%d.measure_id=%d OR dt%d_m%d_branches.measure_id=%d THEN CAST( IF(dt%d_m%d_branches.measure_id IS NOT NULL, dt%d_m%d_branches.value, dt%d_m%d.value) AS %s) END AS %s ",
							validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
							validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureID,
							validation.datatable.id, measureInfo.MeasureID, validation.datatable.id, measureInfo.MeasureID,
							validation.datatable.id, measureInfo.MeasureID, measureInfo.MeasureCastType, aggMeasureName)
					}
				} else {
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
		if singleTableMode {
			pivoted += fmt.Sprintf("FROM %s", cteTableName[0])
			if !singleMeasureMode {
				pivoted += " GROUP BY base_id"
			}
		} else {
			pivoted += fmt.Sprintf("FROM %s %s FOR SYSTEM_TIME AS OF TIMESTAMP'%s' AS base ", dimDataTable, partitionSql, branchTime)
			for _, tableName := range cteTableName {
				pivoted += fmt.Sprintf("LEFT JOIN %s ON (%s.%s = base.%s) ", tableName, tableName, baseMeasureId, baseMeasureId)
			}
			if len(whereClause) > 0 {
				pivoted += "WHERE 1=1 " + whereClause + " "
			}
			if !lowestLevel {
				pivoted += "GROUP BY " + groupByDimLevel
			}
		}
		pivoted += " )"

		// Construct the final query
		query := "WITH "
		query += strings.Join(cteTables, ", ")
		query += ", " + pivoted + " "
		query += "SELECT base_id "
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
		query += " FROM pivoted"

		query = SqlQuery2String(query, argsQuery...)
		query = SqlQueryTrim(query)
		log.Printf("INFO: datamodels/dataframe_queries.go/GetDataframeQueries: For aggr-measure=%s returning query=%s %s",
			aggMeasureName, query, partitionLabel)
		return query, nil
	}

	for _, aggMeasureName := range args.AggregatedMeasures {
		finalAggMeasureName := aggMeasureName
		if dst, ok := mapAliases[aggMeasureName]; ok {
			finalAggMeasureName = dst
		}

		// Resolve dim levels metadata
		aggMeasure := validation.aggrMeasuresMap[aggMeasureName]
		measureMetadata := []*DADataframeColumn{
			&DADataframeColumn{
				Name: "base_id",
				Type: DAMeasureCastTypeUnsignedInteger,
			},
			&DADataframeColumn{
				Name: finalAggMeasureName,
				Type: aggMeasure.MAggregationCastType,
			},
		}

		if lowestLevel {
			for p := 0; p < args.Datatable.Partitions; p++ {
				query, err := getMeasureQueriesQuery(aggMeasureName, &p)
				if err != nil {
					return nil, err
				}
				result.MeasureQueries = append(result.MeasureQueries, &DADataframeMeasureQuery{
					MeasureName: finalAggMeasureName,
					Query:       query,
					Partition:   p,
					Metadata:    measureMetadata,
				})
			}
		} else {
			query, err := getMeasureQueriesQuery(aggMeasureName, nil)
			if err != nil {
				return nil, err
			}
			result.MeasureQueries = append(result.MeasureQueries, &DADataframeMeasureQuery{
				MeasureName: finalAggMeasureName,
				Query:       query,
				Partition:   0,
				Metadata:    measureMetadata,
			})
		}
	}

	return result, nil
}
