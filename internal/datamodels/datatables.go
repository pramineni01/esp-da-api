package datamodels

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DADatatable struct {
	DatasetID  string
	Id         string `db:"datatable_id"`
	TableName  string `db:"datatable_name"`
	Partitions int    `db:"num_partitions"`

	// internal usage
	datasetId int
	id        int
}

func (DADatatable) IsEntity() {}

type FindDADatatableByDatasetIDAndIDParams struct {
	DatasetID   int
	DatatableID *string
}

func (q *Queries) FindDADatatableByDatasetIDAndID(ctx context.Context, args FindDADatatableByDatasetIDAndIDParams) (*DADatatable, error) {
	return q.GetDADatatable(ctx, args.DatasetID, args.DatatableID, nil, nil)
}

// GetDatatable
func (q *Queries) GetDADatatable(ctx context.Context, datasetID int, id *string, measureColumnName *string, tableName *string) (*DADatatable, error) {

	args_map := make(map[string]interface{})
	str_query := `SELECT d.datatable_id,
						 d.datatable_name,
						 num_partitions
				  FROM datatables d`
	str_join := ` INNER JOIN measures m ON m.datatable_id = d.datatable_id`
	str_where := ` WHERE 1=1 `

	if tableName != nil {
		if *tableName != "" {
			str_where += `AND d.datatable_name = :tableName`
			args_map["tableName"] = tableName
		}
	}

	if measureColumnName != nil {
		if *measureColumnName != "" {
			str_query += str_join
			str_where += `AND m.measure_column_name = :measureColumnName `
			args_map["measureColumnName"] = measureColumnName
		}
	}

	if id != nil {
		if int_id, _ := strconv.Atoi(*id); int_id >= 0 {
			str_where += `AND d.datatable_id = :id`
			args_map["id"] = id
		}
	}

	str_query += str_where

	query, xargs, err := sqlx.Named(str_query, args_map)
	if err != nil {
		log.Printf("ERROR: datamodels/datatables.go/GetDatatable -> %s", err)
		return nil, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)
	row := db.QueryRowxContext(ctx, query, xargs...)
	err = row.Err()
	if err != nil {
		log.Printf("ERROR: datamodels/datatables.go/GetDatatable -> %s", err)
		return nil, err
	}

	return rowToDatatable(row, datasetID)
}

// Private methods ------------------------------------------------------------

func rowToDatatable(row *sqlx.Row, datasetID int) (*DADatatable, error) {
	datatable := DADatatable{
		DatasetID: strconv.Itoa(datasetID),
		datasetId: datasetID,
	}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/datatables.go/rowToDatatable: %s", err)
		return nil, err
	}
	err = row.StructScan(&datatable)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/datatables.go/rowToDatatable: %s", err)
		}
		return nil, err
	}

	datatable.id, _ = strconv.Atoi(datatable.Id)
	return &datatable, nil
}

func (q *Queries) areDimLevelsAtLowestLevel(ctx context.Context, datasetID int, datatableID int, dimLevelsByDim map[string][]string) (bool, error) {
	args := []interface{}{}
	subqueries := []string{}
	dimensionList := []string{}
	for dimensionColumnName, dimLevels := range dimLevelsByDim {
		dimensionList = append(dimensionList, dimensionColumnName)
		subqueries = append(subqueries, `
			NOT EXISTS (SELECT * FROM datatable_keyset_dimension_levels dkdl
				NATURAL JOIN dimensions d2
				NATURAL JOIN datatable_keysets dk2
				NATURAL JOIN dimension_levels dl
			WHERE dk2.keyset_id = dk.keyset_id AND d2.dimension_column_name = ? AND dl.dimension_level_column_name NOT IN (?))
		`)
		args = append(args, dimensionColumnName, dimLevels)
	}
	args = append(args, dimensionList, datatableID)
	query := fmt.Sprintf(`
		SELECT dk.dimension_id, (%s) AND d.dimension_column_name IN (?) AS matched
		FROM datatable_keysets dk
		NATURAL JOIN dimensions d
		WHERE dk.datatable_id = ?
		GROUP BY dk.dimension_id
		HAVING NOT matched;
	`, strings.Join(subqueries, " AND "))
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		log.Printf("ERROR datamodels/datatables.go/areDimLevelsAtLowestLevel: %s", err)
		return false, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		if err == sql.ErrNoRows {
			return true, nil
		}
		log.Printf("ERROR datamodels/datatables.go/areDimLevelsAtLowestLevel: %s", err)
		return false, err
	}
	// If any unmatched dimension levels were found, return false
	for rows.Next() {
		return false, nil
	}
	return true, nil
}

func (q *Queries) isScopeAtLowestLevel(ctx context.Context, datasetID int, datatableID int, scope DAQueryScopeInput) (bool, error) {

	dimLevelsByDim := map[string][]string{}
	for _, dimension := range scope.DimensionFilters {
		dimensionLevelList := []string{}
		for _, dimensionLevel := range dimension.AND {
			dimensionLevelList = append(dimensionLevelList, dimensionLevel.DimensionLevelColumnName)
		}
		if _, ok := dimLevelsByDim[dimension.DimensionColumnName]; !ok {
			dimLevelsByDim[dimension.DimensionColumnName] = []string{}
		}
		dimLevelsByDim[dimension.DimensionColumnName] = append(dimLevelsByDim[dimension.DimensionColumnName], dimensionLevelList...)
	}
	return q.areDimLevelsAtLowestLevel(ctx, datasetID, datatableID, dimLevelsByDim)
}
