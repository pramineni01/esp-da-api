package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"

	"github.com/jmoiron/sqlx"
)

// Model

type DADimensionLevel struct {
	DatasetID   string
	ID          string `db:"dimension_level_id"`
	ColumnName  string `db:"dimension_level_column_name"`
	DimensionID string `db:"dimension_id"`
	Dimension   DADimension
}

func (DADimensionLevel) IsEntity() {}

type DADimension struct {
	DatasetID  string
	ID         string `db:"dimension_id"`
	ColumnName string `db:"dimension_column_name"`
}

func (DADimension) IsEntity() {}

type DADimensionMember struct {
	DatasetID                string
	ID                       string  `db:"dimension_member_id"`
	DimensionLevelColumnName string  `db:"dimension_level_id"`
	Name                     string  `db:"dimension_member_name"`
	Description              *string `db:"dimension_member_description"`
	ExternalID               *string `db:"dimension_member_external_id"`
}

func (DADimensionMember) IsEntity() {}

type DADimensionMemberAttribute string

const (
	DADimensionMemberAttributeName        DADimensionMemberAttribute = "NAME"
	DADimensionMemberAttributeDescription DADimensionMemberAttribute = "DESCRIPTION"
	DADimensionMemberAttributeExternalID  DADimensionMemberAttribute = "EXTERNAL_ID"
)

var AllDADimensionMemberAttribute = []DADimensionMemberAttribute{
	DADimensionMemberAttributeName,
	DADimensionMemberAttributeDescription,
	DADimensionMemberAttributeExternalID,
}

func (e DADimensionMemberAttribute) IsValid() bool {
	switch e {
	case DADimensionMemberAttributeName, DADimensionMemberAttributeDescription, DADimensionMemberAttributeExternalID:
		return true
	}
	return false
}

func (e DADimensionMemberAttribute) String() string {
	return string(e)
}

func (e *DADimensionMemberAttribute) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DADimensionMemberAttribute(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DADimensionMemberAttribute", str)
	}
	return nil
}

func (e DADimensionMemberAttribute) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// Entity Resolver ------------------------------------------------------------

type FindDADimensionByDatasetIDAndColumnNameParams struct {
	DatasetID           int
	DimensionColumnName *string
}

func (q *Queries) FindDADimensionByDatasetIDAndColumnName(ctx context.Context, args FindDADimensionByDatasetIDAndColumnNameParams) (*DADimension, error) {
	return q.getDADimension(ctx, args.DatasetID, args.DimensionColumnName)
}

type FindDADimensionLevelByDatasetIDAndColumnNameParams struct {
	DatasetID                int
	DimensionLevelColumnName *string
}

func (q *Queries) FindDADimensionLevelByDatasetIDAndColumnName(ctx context.Context, args FindDADimensionLevelByDatasetIDAndColumnNameParams) (*DADimensionLevel, error) {
	return q.getDADimensionLevel(ctx, args.DatasetID, args.DimensionLevelColumnName)
}

type FindDADimensionMemberByDatasetIDAndIDParams struct {
	DatasetID         int
	DimensionMemberID *string
}

func (q *Queries) FindDADimensionMemberByDatasetIDAndID(ctx context.Context, args FindDADimensionMemberByDatasetIDAndIDParams) (*DADimensionMember, error) {
	return q.getDADimensionMember(ctx, args.DatasetID, args.DimensionMemberID)
}

// Queries

func (q *Queries) GetDADimensionLevels(ctx context.Context, datasetID int, dimensionID *string, dimensionColumnName *string, datatableID *string, keysets bool) ([]*DADimensionLevel, error) {
	args_map := make(map[string]interface{})
	str_query := `SELECT dl.dimension_level_id,
	                     dl.dimension_level_column_name,
						 dl.dimension_id
				  FROM dimension_levels dl`
	str_where := ` WHERE 1=1 `

	if dimensionID != nil {
		if int_id, _ := strconv.Atoi(*dimensionID); int_id >= 0 {
			str_where += `AND dl.dimension_id = :id`
			args_map["id"] = int_id
		}
	}

	if dimensionColumnName != nil && *dimensionColumnName != "" {
		str_join := ` INNER JOIN dimensions d ON d.dimension_id = dl.dimension_id`
		str_query += str_join
		str_where += ` AND d.dimension_column_name = :dimensionColumnName `
		args_map["dimensionColumnName"] = dimensionColumnName
	}

	if datatableID != nil && *datatableID != "" {
		str_join := ` INNER JOIN datatable_dimension_levels ddl ON dl.dimension_level_id = ddl.dimension_level_id`
		str_query += str_join
		str_where += ` AND ddl.datatable_id = :datatableID `
		args_map["datatableID"] = datatableID

		if keysets {
			str_join := ` INNER JOIN datatable_keysets dk ON dk.dimension_id = dl.dimension_id`
			str_query += str_join
			str_where += ` AND dk.datatable_id = :datatableID`

			str_join = ` INNER JOIN datatable_keyset_dimension_levels dkdl ON dkdl.keyset_id = dk.keyset_id AND dl.dimension_level_id = dkdl.dimension_level_id`
			str_query += str_join
			str_where += ` AND dk.default_keyset IS TRUE`
		}
	}

	str_query += str_where
	query, xargs, err := sqlx.Named(str_query, args_map)
	if err != nil {
		log.Printf("ERROR: datamodels/dimensions.go/GetDADimensionLevels: %s", err)
		return nil, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)

	rows, err := db.QueryxContext(ctx, query, xargs...)
	if err != nil {
		return nil, err
	}

	dimensionLevels := []*DADimensionLevel{}
	for rows.Next() {
		var dimensionLevel DADimensionLevel
		err := rows.StructScan(&dimensionLevel)
		if err != nil {
			return dimensionLevels, err
		}
		dimension, err := q.getDADimension(ctx, datasetID, &dimensionLevel.DimensionID)
		if err != nil {
			return nil, err
		}
		dimensionLevel.Dimension = *dimension
		dimensionLevels = append(dimensionLevels, &dimensionLevel)
	}

	return dimensionLevels, nil
}

func (q *Queries) GetDADimensionAndDimLevelsByDatatable(ctx context.Context, datasetID int, datatableID int) (map[string][]string, error) {

	query := `
		SELECT d.dimension_column_name, dl.dimension_level_column_name
		FROM dimension_levels dl
			NATURAL JOIN dimensions d
			NATURAL JOIN datatable_dimension_levels ddl
		WHERE ddl.datatable_id = ?
	`

	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)

	rows, err := db.QueryxContext(ctx, query, datatableID)
	if err != nil {
		log.Printf("ERROR: datamodels/dimensions.go/GetDADimensionAndDimLevelsByDatatable: %s", err)
		return nil, err
	}

	mapDimAndDimLevels := map[string][]string{}
	for rows.Next() {
		var dimColumnName, dimLevelColumnName string
		err := rows.Scan(&dimColumnName, &dimLevelColumnName)
		if err != nil {
			return nil, err
		}
		if _, ok := mapDimAndDimLevels[dimColumnName]; !ok {
			mapDimAndDimLevels[dimColumnName] = []string{}
		}
		mapDimAndDimLevels[dimColumnName] = append(mapDimAndDimLevels[dimColumnName], dimLevelColumnName)
	}
	return mapDimAndDimLevels, nil
}

// Private methods ------------------------------------------------------------

func (q *Queries) getDADimension(ctx context.Context, datasetID int, id *string) (*DADimension, error) {
	query := `SELECT d.dimension_id,
				     d.dimension_column_name
			  FROM dimensions d
			  WHERE d.dimension_id = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToDimension(row, datasetID)
}

func (q *Queries) getDADimensionLevelByColumnName(ctx context.Context, datasetId int, id string) (*DADimensionLevel, error) {
	query := `SELECT *
			  FROM dimension_levels
			  WHERE dimension_level_column_name = ?`

	db := q.GetDatasetDB(datasetId)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToDimensionLevel(row, datasetId)
}

func rowToDimension(row *sqlx.Row, datasetID int) (*DADimension, error) {
	dimension := DADimension{}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/dimensions.go/rowToDimension: %s", err)
		return nil, err
	}
	err = row.StructScan(&dimension)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/dimensions.go/rowToDimension: %s", err)
		}
		return nil, err
	}

	dimension.DatasetID = strconv.Itoa(datasetID)

	return &dimension, nil
}

func (q *Queries) getDADimensionLevel(ctx context.Context, datasetID int, id *string) (*DADimensionLevel, error) {
	query := `SELECT dl.dimension_level_id,
					dl.dimension_level_column_name,
					dl.dimension_id
				FROM dimension_levels dl
			  	WHERE dl.dimension_level_column_name = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToDimensionLevel(row, datasetID)
}

func rowToDimensionLevel(row *sqlx.Row, datasetID int) (*DADimensionLevel, error) {
	dimension_level := DADimensionLevel{}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/dimensions.go/rowToDimensionLevel: %s", err)
		return nil, err
	}
	err = row.StructScan(&dimension_level)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/dimensions.go/rowToDimensionLevel: %s", err)
		}
		return nil, err
	}

	dimension_level.DatasetID = strconv.Itoa(datasetID)

	return &dimension_level, nil
}

func (q *Queries) getDADimensionMember(ctx context.Context, datasetID int, id *string) (*DADimensionMember, error) {
	query := `SELECT dm.dimension_member_id,
					dm.dimension_level_id,
					dm.dimension_member_name,
					dm.dimension_member_description,
					dm.dimension_member_external_id
				FROM dimension_members dm
			  	WHERE dm.dimension_member_id = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToDimensionMember(row, datasetID)
}

func rowToDimensionMember(row *sqlx.Row, datasetID int) (*DADimensionMember, error) {
	dimension_member := DADimensionMember{}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/dimensions.go/rowToDimensionMember: %s", err)
		return nil, err
	}
	err = row.StructScan(&dimension_member)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/dimensions.go/rowToDimensionMember: %s", err)
		}
		return nil, err
	}

	dimension_member.DatasetID = strconv.Itoa(datasetID)

	return &dimension_member, nil
}

func (q *Queries) getDimLevelsByDim(ctx context.Context, datatable *DADatatable, dimLevels []string) (map[string][]string, error) {
	allDimLevel, err := q.GetDADimensionAndDimLevelsByDatatable(ctx, datatable.datasetId, datatable.id)
	if err != nil {
		return nil, err
	}

	dimLevelsByDim := map[string][]string{}
	for columnName, allDimLevelList := range allDimLevel {
		for _, dimLevelColumnName := range allDimLevelList {
			if DimFilterContainsColumnName(dimLevels, dimLevelColumnName) {
				if _, ok := dimLevelsByDim[columnName]; !ok {
					dimLevelsByDim[columnName] = []string{}
				}
				dimLevelsByDim[columnName] = append(dimLevelsByDim[columnName], dimLevelColumnName)
			}

		}
	}
	return dimLevelsByDim, nil
}

func (q *Queries) getDimLevelsByScope(ctx context.Context, datasetID int, scope *DAQueryScopeInput) ([]string, error) {
	if scope == nil || len(scope.DimensionFilters) == 0 {
		return nil, nil
	}
	dimLevels := []string{}
	for _, dimensionFilter := range scope.DimensionFilters {
		dimfilters := map[string][]*DADimensionLevelFilterInput{}
		if len(dimensionFilter.AND) > 0 {
			dimfilters["AND"] = dimensionFilter.AND
		}
		if len(dimensionFilter.OR) > 0 {
			dimfilters["OR"] = dimensionFilter.OR
		}
		for _, levelFilter := range dimfilters {
			for _, dimensionLevelFilterInput := range levelFilter {
				if !DimFilterContainsColumnName(dimLevels, dimensionLevelFilterInput.DimensionLevelColumnName) {
					dimLevels = append(dimLevels, dimensionLevelFilterInput.DimensionLevelColumnName)
				}

			}
		}
	}
	sort.Strings(dimLevels)
	return dimLevels, nil
}

func (q *Queries) getQueryDimLevelsDataExtByScope(ctx context.Context, datasetID int, scope *DAQueryScopeInput) (*string, *string, error) {
	dimLevels := []string{}
	queryDimLevels := ""
	queryDimLevelsValues := ""
	for _, dimensionFilter := range scope.DimensionFilters {
		dimfilters := map[string][]*DADimensionLevelFilterInput{}
		if len(dimensionFilter.AND) > 0 {
			dimfilters["AND"] = dimensionFilter.AND
		}
		if len(dimensionFilter.OR) > 0 {
			dimfilters["OR"] = dimensionFilter.OR
		}
		for _, levelFilter := range dimfilters {
			for _, dimensionLevelFilterInput := range levelFilter {
				if !DimFilterContainsColumnName(dimLevels, dimensionLevelFilterInput.DimensionLevelColumnName) {
					if len(dimensionLevelFilterInput.Values) != 1 {
						err := errors.New("Only one value per dimension level in scope is allowed for this functionality")
						log.Printf("ERROR datamodels/dimensions.go/getQueryDimLevelsDataExtByScope: %s", err)
						return nil, nil, err
					}
					queryDimLevels += dimensionLevelFilterInput.DimensionLevelColumnName + "_id, "
					queryDimLevelsValues += dimensionLevelFilterInput.Values[0] + ", "
				} else {
					err := errors.New("Not allow in scope dimension level column name with same name")
					log.Printf("ERROR datamodels/dimensions.go/getQueryDimLevelsDataExtByScope: %s", err)
					return nil, nil, err
				}

			}
		}
	}
	if len(queryDimLevels) == 0 || len(queryDimLevelsValues) == 0 {
		return nil, nil, nil
	}
	queryDimLevels = queryDimLevels[:len(queryDimLevels)-2]
	queryDimLevelsValues = queryDimLevelsValues[:len(queryDimLevelsValues)-2]
	return &queryDimLevels, &queryDimLevelsValues, nil
}
