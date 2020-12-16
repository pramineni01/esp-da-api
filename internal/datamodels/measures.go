package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"github.com/jmoiron/sqlx"
)

type DAMeasureCastType string

const (
	DAMeasureCastTypeBinary          DAMeasureCastType = "BINARY"
	DAMeasureCastTypeChar            DAMeasureCastType = "CHAR"
	DAMeasureCastTypeDate            DAMeasureCastType = "DATE"
	DAMeasureCastTypeDatetime        DAMeasureCastType = "DATETIME"
	DAMeasureCastTypeDouble          DAMeasureCastType = "DOUBLE"
	DAMeasureCastTypeFloat           DAMeasureCastType = "FLOAT"
	DAMeasureCastTypeSignedInteger   DAMeasureCastType = "SIGNED INTEGER"
	DAMeasureCastTypeUnsignedInteger DAMeasureCastType = "UNSIGNED INTEGER"
	DAMeasureCastTypeTime            DAMeasureCastType = "TIME"
)

var AllDAMeasureCastType = []DAMeasureCastType{
	DAMeasureCastTypeBinary,
	DAMeasureCastTypeChar,
	DAMeasureCastTypeDate,
	DAMeasureCastTypeDatetime,
	DAMeasureCastTypeDouble,
	DAMeasureCastTypeFloat,
	DAMeasureCastTypeSignedInteger,
	DAMeasureCastTypeUnsignedInteger,
	DAMeasureCastTypeTime,
}

func (e DAMeasureCastType) IsValid() bool {
	switch e {
	case DAMeasureCastTypeBinary, DAMeasureCastTypeChar, DAMeasureCastTypeDate, DAMeasureCastTypeDatetime, DAMeasureCastTypeDouble, DAMeasureCastTypeFloat, DAMeasureCastTypeSignedInteger, DAMeasureCastTypeUnsignedInteger, DAMeasureCastTypeTime:
		return true
	}
	return false
}

func (e DAMeasureCastType) String() string {
	return string(e)
}

func (e *DAMeasureCastType) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DAMeasureCastType(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DAMeasureCastType", str)
	}
	return nil
}

func (e DAMeasureCastType) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

type DADatasetMeasure interface {
	IsDADatasetMeasure()
}

type DAMeasure struct {
	DatasetID     string            `json:"datasetID"`
	ID            string            `json:"id" db:"measure_id"`
	DatatableID   string            `json:"datatableID" db:"datatable_id"`
	MeasureTypeID string            `json:"measureTypeID" db:"measure_type_id"`
	ColumnName    string            `json:"columnName" db:"measure_column_name"`
	ColumnType    string            `db:"measure_column_type"`
	CastType      DAMeasureCastType `json:"castType"      db:"measure_cast_type" `

	// internal usage
	id int
}

func (DAMeasure) IsEntity()           {}
func (DAMeasure) IsDADatasetMeasure() {}

type DAMeasureType struct {
	ID         string `db:"measure_type_id"`
	ColumnType string `db:"measure_column_type"`
	ColumnName string `db:"measure_type_column_name"`
}

type DAAggregatedMeasure struct {
	ID              string            `json:"id" db:"measure_aggregation_id"`
	Formula         *string           `json:"formula"`
	ColumnName      string            `json:"columnName" db:"measure_aggregation_column_name"`
	MeasureID       string            `json:"measure_id" db:"measure_id"`
	AggregationType DAAggregationType `json:"aggregationType" db:"measure_aggregation_type"`
	CastType        DAMeasureCastType `json:"castType"        db:"measure_aggregation_cast_type"`
	DataExtensionID string            `json:"dataextension_id" db:"dataextension_id"`

	Measure   *DAMeasure `json:"measure"`
	DatasetID string     `json:"datasetID"`
}

type DAMeasureFilterInput struct {
	MeasureColumnName    string               `json:"measureColumnName"`
	MeasureMultiplier    float64              `json:"measureMultiplier"`
	Operator             DARelationalOperator `json:"operator"`
	Value                []string             `json:"value"`
	DstMeasureColumnName string               `json:"dstMeasureColumnName"`
	DstMultiplier        float64              `json:"dstMultiplier"`
}

func (DAAggregatedMeasure) IsEntity()           {}
func (DAAggregatedMeasure) IsDADatasetMeasure() {}

type DAAggregationType string

const (
	DAAggregationTypeCount     DAAggregationType = "COUNT"
	DAAggregationTypeSum       DAAggregationType = "SUM"
	DAAggregationTypeMin       DAAggregationType = "MIN"
	DAAggregationTypeMax       DAAggregationType = "MAX"
	DAAggregationTypeBaseOnly  DAAggregationType = "BASE_ONLY"
	DAAggregationTypeFormula   DAAggregationType = "FORMULA"
	DAAggregationTypeExtension DAAggregationType = "EXTENSION"
)

var AllDAAggregationType = []DAAggregationType{
	DAAggregationTypeCount,
	DAAggregationTypeSum,
	DAAggregationTypeMin,
	DAAggregationTypeMax,
	DAAggregationTypeBaseOnly,
	DAAggregationTypeFormula,
	DAAggregationTypeExtension,
}

func (e DAAggregationType) IsValid() bool {
	switch e {
	case DAAggregationTypeCount, DAAggregationTypeSum, DAAggregationTypeMin, DAAggregationTypeMax, DAAggregationTypeBaseOnly, DAAggregationTypeFormula, DAAggregationTypeExtension:
		return true
	}
	return false
}

func (e DAAggregationType) String() string {
	return string(e)
}

func (e *DAAggregationType) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DAAggregationType(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DAAggregationType", str)
	}
	return nil
}

func (e DAAggregationType) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

// Entity resolver

type FindDAMeasureByDatasetIDAndColumnNameParams struct {
	DatasetID         int
	MeasureColumnName *string
}

func (q *Queries) FindDAMeasureByDatasetIDAndColumnName(ctx context.Context, args FindDAMeasureByDatasetIDAndColumnNameParams) (*DAMeasure, error) {
	return q.getDAMeasureByColumnName(ctx, args.DatasetID, args.MeasureColumnName)
}

type FindDAAggregatedMeasureByDatasetIDAndColumnNameParams struct {
	DatasetID                   int
	AggregatedMeasureColumnName *string
}

func (q *Queries) FindDAAggregatedMeasureByDatasetIDAndColumnName(ctx context.Context, args FindDAAggregatedMeasureByDatasetIDAndColumnNameParams) (*DAAggregatedMeasure, error) {
	return q.getDAAggregatedMeasureByColumnName(ctx, args.DatasetID, args.AggregatedMeasureColumnName)
}

func (q *Queries) getDAMeasuresByColumnNameList(ctx context.Context, datasetID int, datatableID *string, columnNames *[]string) ([]*DAMeasure, error) {
	query := `SELECT m.measure_id,
	                m.measure_column_name,
					mt.measure_column_type
             FROM measures m
             INNER JOIN measure_types mt ON m.measure_type_id = mt.measure_type_id
             WHERE m.datatable_id = :datatableID `

	args_map := make(map[string]interface{})
	args_map["datatableID"] = datatableID

	query, args, err := sqlx.Named(query, args_map)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/getDAMeasuresByColumnNameList: %s", err)
		return nil, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)

	in_query, _, err := sqlx.In(" AND m.measure_column_name IN(?)", *columnNames)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/getDAMeasuresByColumnNameList: %s", err)
		return nil, err
	}
	query += in_query
	query = db.Rebind(query)

	s := make([]interface{}, len(*columnNames))
	for i, v := range *columnNames {
		s[i] = v
	}
	args = append(args, s...)

	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/getDAMeasuresByColumnNameList: %s", err)
		return nil, err
	}

	measures := []*DAMeasure{}
	for rows.Next() {
		var measure DAMeasure
		err := rows.StructScan(&measure)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/getDAMeasuresByColumnNameList: %s", err)
			return measures, err
		}
		measure.DatasetID = strconv.Itoa(datasetID)
		measure.id, _ = strconv.Atoi(measure.ID)
		measures = append(measures, &measure)
	}

	return measures, nil
}

func (q *Queries) GetDAMeasures(ctx context.Context, datasetID int, aggregated *bool) ([]DADatasetMeasure, error) {
	result := []DADatasetMeasure{}

	db := q.GetDatasetDB(datasetID)
	if aggregated == nil || !*aggregated {
		query := `SELECT m.measure_id,
                             m.datatable_id,
							 m.measure_column_name,
							 mt.measure_cast_type
					  FROM measures m
						   NATURAL JOIN
						   measure_types mt`

		rows, err := db.QueryxContext(ctx, query)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDAMeasures: %s", err)
			return nil, err
		}

		for rows.Next() {
			measure := DAMeasure{}
			if err := rows.StructScan(&measure); err != nil {
				log.Printf("ERROR: datamodels/measures.go/GetDAMeasures: %s", err)
				return nil, err
			}
			measure.DatasetID = strconv.Itoa(datasetID)
			measure.id, _ = strconv.Atoi(measure.ID)
			result = append(result, measure)
		}
	}

	if aggregated == nil || *aggregated {
		query := `SELECT ma.measure_aggregation_id,
                             ma.measure_aggregation_type,
                             ma.measure_formula,
							 ma.measure_aggregation_column_name,
							 ma.measure_aggregation_cast_type,
							 ma.dataextension_id,
                             m.measure_id,
                             m.datatable_id,
							 m.measure_column_name,
							 mt.measure_cast_type
                      FROM measure_aggregations ma
					  INNER JOIN measures m ON ma.measure_id = m.measure_id
					             NATURAL JOIN
						         measure_types mt`

		rows, err := db.QueryxContext(ctx, query)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDAMeasures: %s", err)
			return nil, err
		}

		for rows.Next() {
			measureAgg := &DAAggregatedMeasure{}
			measure := &DAMeasure{}

			err = rows.Scan(
				&measureAgg.ID,
				&measureAgg.AggregationType,
				&measureAgg.Formula,
				&measureAgg.ColumnName,
				&measureAgg.CastType,
				&measureAgg.DataExtensionID,
				&measure.ID,
				&measure.DatatableID,
				&measure.ColumnName,
				&measure.CastType,
			)

			if err != nil {
				log.Printf("ERROR: datamodels/measures.go/GetDAMeasures: %s", err)
				return nil, err
			}

			measure.DatasetID = strconv.Itoa(datasetID)
			measure.id, _ = strconv.Atoi(measure.ID)
			measureAgg.Measure = measure
			measureAgg.DatasetID = strconv.Itoa(datasetID)

			result = append(result, measureAgg)
		}
	}

	return result, nil
}

func (q *Queries) GetDADatatableMeasures(ctx context.Context, datasetID int, datatableID *string) ([]*DAMeasure, error) {
	str_query := `SELECT m.measure_id,
                         m.datatable_id,
						 m.measure_column_name,
						 mt.measure_cast_type
				  FROM measures m
					   NATURAL JOIN
					   measure_types mt
                  WHERE m.datatable_id = ?`

	db := q.GetDatasetDB(datasetID)
	rows, err := db.QueryxContext(ctx, str_query, datatableID)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetDADatatableMeasures: %s", err)
		return nil, err
	}

	measures := []*DAMeasure{}
	for rows.Next() {
		var measure DAMeasure
		if err := rows.StructScan(&measure); err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDADatatableMeasures: %s", err)
			return nil, err
		}
		measure.DatasetID = strconv.Itoa(datasetID)
		measure.id, _ = strconv.Atoi(measure.ID)
		measures = append(measures, &measure)
	}

	return measures, nil
}

func (q *Queries) GetDADatatableMeasuresByIDs(ctx context.Context, datasetID int, datatableID *string, measuresIDs []int) ([]*DAMeasure, error) {
	str_query := `SELECT m.measure_id,
                         m.datatable_id,
						 m.measure_column_name,
						 mt.measure_cast_type
				  FROM measures m
					   NATURAL JOIN
					   measure_types mt
                  WHERE m.datatable_id = ?`

	db := q.GetDatasetDB(datasetID)

	in_query, _, err := sqlx.In(" AND  m.measure_id IN (?)", measuresIDs)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetDADatatableMeasuresByIDs: %s", err)
		return nil, err
	}
	str_query += in_query
	str_query = db.Rebind(str_query)
	args := []interface{}{}
	args = append(args, datatableID)
	args = append(args, measuresIDs)

	rows, err := db.QueryxContext(ctx, str_query, args)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetDADatatableMeasuresByIDs: %s", err)
		return nil, err
	}

	measures := []*DAMeasure{}
	for rows.Next() {
		var measure DAMeasure
		if err := rows.StructScan(&measure); err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDADatatableMeasuresByIDs: %s", err)
			return nil, err
		}
		measure.DatasetID = strconv.Itoa(datasetID)
		measure.id, _ = strconv.Atoi(measure.ID)
		measures = append(measures, &measure)
	}

	return measures, nil
}

type GetMapAggrMeasuresByColumnNameParams struct {
	DatatableID  string
	AggrMeasures []string
}

type AggrMeasureForMap struct {
	MAggregationID          int
	MAggregationColumnName  string
	MAggregationType        DAAggregationType
	MeasureID               *int
	MAggregationFormula     *string
	MAggregationExtensionID *string
	MAggregationCastType    DAMeasureCastType // 'BINARY','CHAR','DATE','DATETIME','DOUBLE','FLOAT','SIGNED INTEGER','UNSIGNED INTEGER','TIME'
	MADependencies          []string
}

type MeasureForMap struct {
	DatatableID       int
	MeasureID         int
	MeasureColumnName string
	MeasureDataType   string            // int, string, float .. (dt<DataTable ID>_base_measures_<Data Type>)
	MeasureCastType   DAMeasureCastType // 'BINARY','CHAR' ...
	MeasureDataTypeID int
}

const getaggrMeasuresMapByColumnName = `
	SELECT ma.measure_aggregation_id,
			ma.measure_aggregation_column_name,
			ma.measure_aggregation_type,
			ma.measure_id,
		    ma.measure_formula,
			ma.measure_aggregation_cast_type,
			ma.dataextension_id
	FROM measure_aggregations ma
	WHERE ma.measure_aggregation_column_name IN (?)`

const getAggrMeasuresByFormulaDependencies = `
	SELECT  mad.target_measure_aggregation_id AS target_measure_aggregation_id,
	        ma.measure_aggregation_id,
       		ma.measure_aggregation_column_name,
       		ma.measure_aggregation_type,
			ma.measure_id,
			ma.measure_aggregation_cast_type,
			ma.dataextension_id
	FROM measure_aggregation_dependencies mad,
		measure_aggregations ma
	WHERE mad.target_measure_aggregation_id IN (?) AND
		mad.source_measure_aggregation_id = ma.measure_aggregation_id`

const getMeasureDetails = `
	SELECT  m.measure_id,
			m.measure_column_name,
			m.datatable_id,
			mt.measure_type_column_name,
			mt.measure_cast_type,
			mt.measure_type_id
	FROM measures m
		NATURAL JOIN
		measure_types mt
	WHERE m.measure_id IN (?)`

func (q *Queries) GetMapAggrMeasuresByColumnName(ctx context.Context, datasetID int, aggrMeasures []string, firstLevel bool) (map[string]*AggrMeasureForMap, map[int]*MeasureForMap, error) {
	if len(aggrMeasures) == 0 {
		err := errors.New("Aggregated measures are empty")
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
		return nil, nil, err
	}
	//  Get the list of all the involved aggregated measures
	query, inargs, err := sqlx.In(getaggrMeasuresMapByColumnName, aggrMeasures)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
		return nil, nil, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, inargs...)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
		return nil, nil, err
	}

	aggrMeasuresMap := map[string]*AggrMeasureForMap{}
	measuresMap := map[int]*MeasureForMap{}
	aggrMeasureIDtoName := map[int]string{}

	aggMeasuresFormulas := []int{}
	measuresInvolved := []int{}

	found := 0
	onlyDataExtension := true
	for rows.Next() {
		found++
		var measureStr *string
		measureAggregation := AggrMeasureForMap{}
		err = rows.Scan(
			&measureAggregation.MAggregationID,
			&measureAggregation.MAggregationColumnName,
			&measureAggregation.MAggregationType,
			&measureStr,
			&measureAggregation.MAggregationFormula,
			&measureAggregation.MAggregationCastType,
			&measureAggregation.MAggregationExtensionID,
		)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}
		if measureStr != nil {
			measure_id, _ := strconv.Atoi(*measureStr)
			measureAggregation.MeasureID = &measure_id
		}
		aggrMeasureIDtoName[measureAggregation.MAggregationID] = measureAggregation.MAggregationColumnName

		aggrMeasuresMap[measureAggregation.MAggregationColumnName] = &measureAggregation

		// From this we will detect if there is any formula column
		// (if measure_id = NULL and measure_formula != NULL), in that case we should collect the agg_measures used by the formulas:
		if measureAggregation.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA {
			if measureAggregation.MAggregationFormula == nil {
				err := errors.New("Measure type is formula, but measure_formula is empty")
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
				return nil, nil, err
			}
			aggMeasuresFormulas = append(aggMeasuresFormulas, measureAggregation.MAggregationID)
			onlyDataExtension = false
		} else if measureAggregation.MAggregationType == config.MEASURE_AGGREGATION_TYPE_EXTENSION {
			if measureAggregation.MAggregationExtensionID == nil {
				err := errors.New("Measure type is extension, but dataextension_id is empty")
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
				return nil, nil, err
			}
			aggMeasuresFormulas = append(aggMeasuresFormulas, measureAggregation.MAggregationID)
		} else {
			if measureAggregation.MeasureID == nil {
				err := errors.New("Measure type NOT is formula, but measure_id is empty")
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s [%s]",
					err, measureAggregation.MAggregationColumnName)
				return nil, nil, err
			}
			onlyDataExtension = false
			measuresInvolved = append(measuresInvolved, *measureAggregation.MeasureID)
		}
	}
	if len(aggrMeasures) > found {
		err = fmt.Errorf("aggregated measures not found in database (%d required, %d found)", len(aggrMeasures), found)
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
		return nil, nil, err
	}

	// Collect formula dependencies (if any)
	if len(aggMeasuresFormulas) > 0 {
		query, inargs, err = sqlx.In(getAggrMeasuresByFormulaDependencies, aggMeasuresFormulas)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}
		query = db.Rebind(query)
		rows, err = db.QueryxContext(ctx, query, inargs...)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}
		for rows.Next() {
			var measureStr *string
			var targetMAggregationID int
			measureAggregation := AggrMeasureForMap{}
			err = rows.Scan(
				&targetMAggregationID,
				&measureAggregation.MAggregationID,
				&measureAggregation.MAggregationColumnName,
				&measureAggregation.MAggregationType,
				&measureStr,
				&measureAggregation.MAggregationCastType,
				&measureAggregation.MAggregationExtensionID,
			)
			if err != nil {
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
				return nil, nil, err
			}
			if measureStr != nil {
				measure_id, _ := strconv.Atoi(*measureStr)
				measureAggregation.MeasureID = &measure_id
			}

			if measureAggregation.MAggregationType == config.MEASURE_AGGREGATION_TYPE_FORMULA && firstLevel {
				err := errors.New("Formulas in formulas are not suppported")
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
				return nil, nil, err
			}

			if measureAggregation.MeasureID == nil && firstLevel {
				err := errors.New("Measure type NOT is formula, but measure_id is empty")
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s [%s]",
					err, measureAggregation.MAggregationColumnName)
				return nil, nil, err
			}

			// Update parent dependencies
			targetMAggregationName := aggrMeasureIDtoName[targetMAggregationID]
			if aggrMeasuresMap[targetMAggregationName].MADependencies == nil {
				aggrMeasuresMap[targetMAggregationName].MADependencies = []string{}
			}
			aggrMeasuresMap[targetMAggregationName].MADependencies = append(
				aggrMeasuresMap[targetMAggregationName].MADependencies, measureAggregation.MAggregationColumnName)

			// Add the dependency into the list
			aggrMeasuresMap[measureAggregation.MAggregationColumnName] = &measureAggregation
			if measureAggregation.MeasureID != nil {
				measuresInvolved = append(measuresInvolved, *measureAggregation.MeasureID)
			}
		}
	}

	if firstLevel && !onlyDataExtension {
		if len(measuresInvolved) == 0 {
			err := errors.New("No measures could be found for the requested names")
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}

		// Define the list of all the required measures and collect required info
		query, inargs, err = sqlx.In(getMeasureDetails, measuresInvolved)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}
		query = db.Rebind(query)
		rows, err = db.QueryxContext(ctx, query, inargs...)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
			return nil, nil, err
		}
		for rows.Next() {
			measure := MeasureForMap{}
			err = rows.Scan(
				&measure.MeasureID,
				&measure.MeasureColumnName,
				&measure.DatatableID,
				&measure.MeasureDataType,
				&measure.MeasureCastType,
				&measure.MeasureDataTypeID,
			)
			if err != nil {
				log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnName: %s", err)
				return nil, nil, err
			}
			measuresMap[measure.MeasureID] = &measure
		}
	}
	return aggrMeasuresMap, measuresMap, nil
}

func (q *Queries) GetMapAggrMeasuresByColumnNameBasic(ctx context.Context, datasetID int, aggrMeasures []string) (map[string]*AggrMeasureForMap, error) {
	if len(aggrMeasures) == 0 {
		err := errors.New("Aggregated measures are empty")
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnNameBasic: %s", err)
		return nil, err
	}

	//  Get the list of all the involved aggregated measures
	query, inargs, err := sqlx.In(getaggrMeasuresMapByColumnName, aggrMeasures)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnNameBasic: %s", err)
		return nil, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, inargs...)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnNameBasic: %s", err)
		return nil, err
	}

	aggrMeasuresMap := map[string]*AggrMeasureForMap{}
	for rows.Next() {
		var measureStr *string
		measureAggregation := AggrMeasureForMap{}
		err = rows.Scan(
			&measureAggregation.MAggregationID,
			&measureAggregation.MAggregationColumnName,
			&measureAggregation.MAggregationType,
			&measureStr,
			&measureAggregation.MAggregationFormula,
			&measureAggregation.MAggregationCastType,
			&measureAggregation.MAggregationExtensionID,
		)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetMapAggrMeasuresByColumnNameBasic: %s", err)
			return nil, err
		}
		if measureStr != nil {
			measure_id, _ := strconv.Atoi(*measureStr)
			measureAggregation.MeasureID = &measure_id
		}

		aggrMeasuresMap[measureAggregation.MAggregationColumnName] = &measureAggregation
	}

	return aggrMeasuresMap, nil
}

type CacheLayerMeasure struct {
	AggrMeasureId   int
	AggrMeasureType string // SUM, MAX ...
	MeasureID       int
	MeasureCastType string
}

func (q *Queries) GetDTCacheLayerMeasuresByType(ctx context.Context, datasetId int, datatableId int, measuresIDs []int) (map[string][]*CacheLayerMeasure, error) {
	db := q.GetDatasetDB(datasetId)

	query := `SELECT  measure_aggregation_id, measure_aggregation_type, measure_id, measure_cast_type, measure_type_column_name
			FROM datatables
				NATURAL JOIN measures
				NATURAL JOIN measure_aggregations
				NATURAL JOIN measure_types
			WHERE datatable_id = ? AND
				  measure_aggregation_type NOT IN ("BASE_ONLY", "FORMULA")`

	var argsQuery []interface{}
	argsQuery = append(argsQuery, datatableId)

	if measuresIDs != nil && len(measuresIDs) > 0 {
		queryTmp := " AND measure_id IN (?)"
		queryTmp, inargs, err := sqlx.In(queryTmp, measuresIDs)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDTCacheLayerMeasuresByType: %s", err)
			return nil, err
		}
		query += queryTmp
		argsQuery = append(argsQuery, inargs...)
	}

	rows, err := db.QueryxContext(ctx, query, argsQuery...)
	if err != nil {
		log.Printf("ERROR: datamodels/measures.go/GetDTCacheLayerMeasuresByType: %s", err)
		return nil, err
	}

	result := map[string][]*CacheLayerMeasure{}
	for rows.Next() {
		var cacheLayerMeasure CacheLayerMeasure
		var measureType string

		err = rows.Scan(
			&cacheLayerMeasure.AggrMeasureId,
			&cacheLayerMeasure.AggrMeasureType,
			&cacheLayerMeasure.MeasureID,
			&cacheLayerMeasure.MeasureCastType,
			&measureType,
		)
		if err != nil {
			log.Printf("ERROR: datamodels/measures.go/GetDTCacheLayerMeasuresByType: %s", err)
			return nil, err
		}

		if _, ok := result[measureType]; !ok {
			result[measureType] = []*CacheLayerMeasure{}
		}
		result[measureType] = append(result[measureType], &cacheLayerMeasure)
	}

	return result, nil
}

// private methods ------------------------------------------------------------

func (q *Queries) getDAMeasureTypeByID(ctx context.Context, datasetId int, id string) (*DAMeasureType, error) {
	query := `SELECT *
			  FROM measure_types
			  WHERE measure_type_id = ?`

	db := q.GetDatasetDB(datasetId)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToMeasureType(row)
}

func rowToMeasureType(row *sqlx.Row) (*DAMeasureType, error) {
	measureType := DAMeasureType{}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/measures.go/rowToMeasureType: %s", err)
		return nil, err
	}
	err = row.StructScan(&measureType)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/measures.go/rowToMeasureType: %s", err)
		}
		return nil, err
	}
	return &measureType, nil
}

func (q *Queries) getDAMeasureByColumnName(ctx context.Context, datasetID int, column_name *string) (*DAMeasure, error) {
	query := `SELECT m.*
				FROM measures m
				WHERE m.measure_column_name = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, column_name)

	return rowToMeasure(row, datasetID)
}

func rowToMeasure(row *sqlx.Row, datasetID int) (*DAMeasure, error) {
	measure := DAMeasure{}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/measures.go/rowToMeasure: %s", err)
		return nil, err
	}
	err = row.StructScan(&measure)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/measures.go/rowToMeasure: %s", err)
		}
		return nil, err
	}

	measure.DatasetID = strconv.Itoa(datasetID)
	measure.id, _ = strconv.Atoi(measure.ID)
	return &measure, nil
}

const listNonformulaMeasureAggregationsAndDependenciesByIDs = `
WITH RECURSIVE measure_deps AS (
	SELECT source_measure_aggregation_id AS id FROM measure_aggregation_dependencies
		WHERE target_measure_aggregation_id IN (?)
	UNION
		SELECT measure_aggregation_dependencies.source_measure_aggregation_id FROM measure_aggregation_dependencies, measure_deps
			WHERE measure_deps.id = measure_aggregation_dependencies.target_measure_aggregation_id
)
SELECT measure_aggregations.* FROM measure_deps
	INNER JOIN measure_aggregations ON measure_aggregations.measure_aggregation_id = measure_deps.id
	WHERE measure_aggregations.measure_aggregation_type != 'FORMULA'
`

//TODO: call this during startup, cache result in redis with TTL 0
func (q *Queries) listNonformulaMeasureAggregationsAndDependenciesByIDs(ctx context.Context, datasetId int, ids []int) ([]*DAAggregatedMeasure, error) {
	query, inargs, err := sqlx.In(listNonformulaMeasureAggregationsAndDependenciesByIDs, ids)
	if err != nil {
		return []*DAAggregatedMeasure{}, err
	}
	db := q.GetDatasetDB(datasetId)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, inargs)
	if err != nil {
		return []*DAAggregatedMeasure{}, err
	}

	measureAggregations := []*DAAggregatedMeasure{}
	for rows.Next() {
		measureAggregation := DAAggregatedMeasure{}
		err := rows.StructScan(&measureAggregation)
		if err != nil {
			return measureAggregations, err
		}
		measureAggregations = append(measureAggregations, &measureAggregation)
	}
	return measureAggregations, nil
}

func (q *Queries) getDAAggregatedMeasureByColumnName(ctx context.Context, datasetID int, column_name *string) (*DAAggregatedMeasure, error) {
	query := `
	SELECT ma.measure_aggregation_id,
			ma.measure_aggregation_type,
			ma.measure_formula,
			ma.measure_aggregation_column_name,
			ma.dataextension_id,
			m.measure_id,
			m.datatable_id,
			m.measure_column_name
		FROM measure_aggregations ma
		INNER JOIN measures m ON ma.measure_id = m.measure_id
		WHERE ma.measure_aggregation_column_name = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, column_name)
	return rowToAggregatedMeasure(row, datasetID)
}

func rowToAggregatedMeasure(row *sqlx.Row, datasetID int) (*DAAggregatedMeasure, error) {
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/measures.go/rowToAggregatedMeasure: %s", err)
		return nil, err
	}

	measureAgg := &DAAggregatedMeasure{}
	measure := &DAMeasure{}

	err = row.Scan(
		&measureAgg.ID,
		&measureAgg.AggregationType,
		&measureAgg.Formula,
		&measureAgg.ColumnName,
		&measureAgg.DataExtensionID,
		&measure.ID,
		&measure.DatatableID,
		&measure.ColumnName,
	)
	if err != nil {
		log.Printf("ERROR datamodels/measures.go/rowToAggregatedMeasure: %s", err)
		return nil, err
	}

	measure.DatasetID = strconv.Itoa(datasetID)
	measure.id, _ = strconv.Atoi(measure.ID)
	measureAgg.Measure = measure
	measureAgg.DatasetID = strconv.Itoa(datasetID)
	return measureAgg, nil
}

type measureAggInfo struct {
	MeasureAggregation   string            `db:"measure_aggregation_column_name"`
	MeasureAggregationID string            `db:"measure_aggregation_id"`
	AggregationType      DAAggregationType `db:"measure_aggregation_type"`
	MeasureID            sql.NullString    `db:"measure_id"`
	MeasureType          string            `db:"measure_type_column_name"`
	MeasureCastType      DAMeasureCastType `db:"measure_cast_type"`
	AggregationDataExtID *string           `db:"dataextension_id"`
}

const measureAggregationForUpdateQuery = `
SELECT measure_aggregation_column_name, measure_aggregation_id, measure_id, measure_aggregation_type, measure_type_column_name, measure_cast_type, dataextension_id
FROM measure_aggregations ma 
	NATURAL JOIN measures m
	NATURAL JOIN measure_types mt
WHERE ma.measure_aggregation_column_name IN (?)
`

func (q *Queries) getMeasureAggregationInfo(ctx context.Context, datasetID int, measureAggregations []string) (map[string]measureAggInfo, error) {
	result := map[string]measureAggInfo{}
	query, args, err := sqlx.In(measureAggregationForUpdateQuery, measureAggregations)
	if err != nil {
		log.Printf("ERROR datamodels/dataupdates.go/getMeasureAggregationInfo: %s", err)
		return result, err
	}
	db := q.GetDatasetDB(datasetID)
	query = db.Rebind(query)
	rows, err := db.QueryxContext(ctx, query, args...)
	if err != nil {
		log.Printf("ERROR datamodels/dataupdates.go/getMeasureAggregationInfo: %s", err)
		return result, err
	}
	var magg measureAggInfo
	for rows.Next() {
		err := rows.StructScan(&magg)
		if err != nil {
			log.Printf("ERROR datamodels/dataupdates.go/getMeasureAggregationInfo: %s", err)
			return result, err
		}
		result[magg.MeasureAggregation] = magg
	}
	return result, nil
}
