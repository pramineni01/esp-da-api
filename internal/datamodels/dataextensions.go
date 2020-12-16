package datamodels

import (
	"context"
	"database/sql"
	"log"
	"strconv"

	"github.com/jmoiron/sqlx"
)

type DataExtension struct {
	ID             string `db:"dataextension_id"`
	Name           string `db:"name"`
	num_partitions int    `db:"num_partitions"`

	// internal usage
	id int
}

type DataExtensionDimensionLevels struct {
	ID               string `db:"dataextension_id"`
	DimensionLevelID string `db:"dimension_level_id"`

	// internal usage
	id         int
	dimLevelId int
}

// Private methods ------------------------------------------------------------

type getMatchingDataExtensionParams struct {
	DatasetID            int
	DimensionLevels      []string
	AggrMeasureExtension []*AggrMeasureForMap
}

func (q *Queries) getMatchingDataExtension(ctx context.Context, args getMatchingDataExtensionParams) (map[string]int, error) {
	if len(args.DimensionLevels) == 0 || len(args.AggrMeasureExtension) == 0 {
		return nil, nil
	}
	db := q.GetDatasetDB(args.DatasetID)

	aggrMeasureExtensionMap := map[string]int{}
	ids := []int{}
	aggMExtensionByExtID := map[int][]string{}
	for _, aggrMeasure := range args.AggrMeasureExtension {
		id, _ := strconv.Atoi(*aggrMeasure.MAggregationExtensionID)
		ids = append(ids, id)
		if _, ok := aggMExtensionByExtID[id]; !ok {
			aggMExtensionByExtID[id] = []string{}
		}
		aggMExtensionByExtID[id] = append(aggMExtensionByExtID[id], aggrMeasure.MAggregationColumnName)
	}
	query := `SELECT ddl.dataextension_id, GROUP_CONCAT(dl.dimension_level_column_name ORDER BY dl.dimension_level_column_name)
		FROM dataextension_dimension_levels ddl 
		NATURAL JOIN dimension_levels dl 
		NATURAL JOIN dataextensions de
		WHERE ddl.dataextension_id IN (?) AND de.enabled = 1
		GROUP BY ddl.dataextension_id`

	query, argsIn, err := sqlx.In(query, ids)
	if err != nil {
		log.Printf("ERROR: datamodels/dataextensions.go/getMatchingDataExtension: %s", err)
		return nil, err
	}

	rows, err := db.QueryxContext(ctx, query, argsIn...)
	if err != nil {
		log.Printf("ERROR: datamodels/dataextensions.go/getMatchingDataExtension: %s", err)
		return nil, err
	}
	for rows.Next() {
		var extID int
		var dimLevelColumnGroup string

		err = rows.Scan(
			&extID,
			&dimLevelColumnGroup,
		)
		if err != nil {
			if err != sql.ErrNoRows {
				log.Printf("ERROR: datamodels/dataextensions.go/getMatchingDataExtension: %s", err)
				return nil, err
			}
			break // It is not added as valid extension
		}
		if len(dimLevelColumnGroup) == 0 {
			continue // It is not added as valid extension
		}
		// Validate dim Levels in extension
		if DimLevelGroupCheck(dimLevelColumnGroup, args.DimensionLevels) {
			for _, aggrMeasureColumnName := range aggMExtensionByExtID[extID] {
				if _, ok := aggrMeasureExtensionMap[aggrMeasureColumnName]; !ok {
					aggrMeasureExtensionMap[aggrMeasureColumnName] = extID
				}
			}
		}
	}

	return aggrMeasureExtensionMap, nil
}
