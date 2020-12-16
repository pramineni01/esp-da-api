package datamodels

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"github.com/jmoiron/sqlx"
)

// Models

type MapScopeJson map[string]map[string][]map[string]map[string][]int
type MapScope map[string]DimensionFilter // string (DimensionColumnName)

type DimensionFilter struct {
	DimensionColumnName string
	AND                 []*DimensionLevelFilter
	OR                  []*DimensionLevelFilter
}

type DimensionLevelFilter struct {
	DimLevelColumnName string
	CmpOperator        DAComparisonOperator
	Values             []int
}

type FullScopeJson struct {
	AccessViewScope MapScopeJson
	RequestedScope  MapScopeJson
}

// ------------------------------------------------------------------------------------
// sort helper
type byDimLvlFltrCol []*DimensionLevelFilter

func (col byDimLvlFltrCol) Len() int { return len(col) }
func (col byDimLvlFltrCol) Less(i, j int) bool {
	return strings.Compare(col[i].DimLevelColumnName, col[j].DimLevelColumnName) <= 0
}
func (col byDimLvlFltrCol) Swap(i, j int) { col[i], col[j] = col[j], col[i] }

// ------------------------------------------------------------------------------------

// Private methods ------------------------------------------------------------

func (q *Queries) queryScopeToWhereConditions(ctx context.Context, datasetID int, datatableID int, userID *string, scopeInput *DAQueryScopeInput, writeRequired bool) (*string, error) {
	whereInput, err := q.queryScopeOnlyToWhereConditions(ctx, scopeInput)
	if err != nil {
		log.Printf("ERROR datamodels/access_views.go/queryScopeOnlyToWhereConditions: %s", err)
		return nil, err
	}

	scopeMapDB, err := q.accessViewScopeToMapScope(ctx, datasetID, userID, datatableID, writeRequired)
	if err != nil {
		log.Printf("ERROR datamodels/access_views.go/queryScopeToWhereConditions: %s", err)
		return nil, err
	}

	whereBD := ""
	if len(scopeMapDB) > 0 {
		whereBD, err = mapScopeToWhereConditions(scopeMapDB)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/queryScopeToWhereConditions: %s", err)
			return nil, err
		}
	}

	var whereMerged string
	if whereInput != nil && len(*whereInput) > 0 && len(whereBD) > 0 {
		whereMerged = fmt.Sprintf("(%s) AND (%s)", *whereInput, whereBD)
	} else if whereInput != nil && len(*whereInput) > 0 {
		whereMerged = *whereInput
	} else if len(whereBD) > 0 {
		whereMerged = whereBD
	} else {
		return nil, nil
	}

	return &whereMerged, nil
}

func (q *Queries) queryScopeOnlyToWhereConditions(ctx context.Context, scopeInput *DAQueryScopeInput) (*string, error) {
	whereInput := ""
	if scopeInput != nil && len(scopeInput.DimensionFilters) > 0 {
		scopeMapInput, err := loadScopeToMapScope(*scopeInput)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/queryScopeOnlyToWhereConditions: %s", err)
			return nil, err
		}
		whereInput, err = mapScopeToWhereConditions(scopeMapInput)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/queryScopeOnlyToWhereConditions: %s", err)
			return nil, err
		}
	}

	if len(whereInput) == 0 {
		return nil, nil
	}
	return &whereInput, nil
}

func fullScopeJsonToWhereConditions(scopeJsonIngestDB []byte) (string, error) {
	scopeJsonIngest := FullScopeJson{}
	err := json.Unmarshal(scopeJsonIngestDB, &scopeJsonIngest)
	if err != nil {
		errS := fmt.Sprintf("[JSON Scope Invalid] %s", err.Error())
		log.Printf("ERROR: datamodels/access_views.go/fullScopeJsonToWhereConditions: %s", errS)
		return "", err
	}

	scopeMapInput, err := loadMapScopeJsonToMapScope(scopeJsonIngest.RequestedScope)
	if err != nil {
		log.Printf("ERROR datamodels/access_views.go/fullScopeJsonToWhereConditions: %s", err)
		return "", err
	}

	scopeMapDB, err := loadMapScopeJsonToMapScope(scopeJsonIngest.AccessViewScope)
	if err != nil {
		log.Printf("ERROR datamodels/access_views.go/fullScopeJsonToWhereConditions: %s", err)
		return "", err
	}

	whereInput := ""
	if len(scopeMapDB) > 0 {
		whereInput, err = mapScopeToWhereConditions(scopeMapInput)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/fullScopeJsonToWhereConditions: %s", err)
			return "", err
		}
	}

	whereBD := ""
	if len(scopeMapDB) > 0 {
		whereBD, err = mapScopeToWhereConditions(scopeMapDB)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/fullScopeJsonToWhereConditions: %s", err)
			return "", err
		}
	}

	whereMerged := ""
	if len(whereInput) > 0 && len(whereBD) > 0 {
		whereMerged = fmt.Sprintf("(%s) AND (%s)", whereInput, whereBD)
	} else if len(whereInput) > 0 {
		whereMerged = whereInput
	} else if len(whereBD) > 0 {
		whereMerged = whereBD
	}

	return whereMerged, nil
}

func mapScopeToWhereConditions(scopeMap MapScope) (string, error) {
	results := ""

	// we need output query in a consistent manner, as we are forming key based on it and caching the data in redis.
	// hence sort input map data based on key
	dimFltrKeys := []string{}
	for key := range scopeMap {
		dimFltrKeys = append(dimFltrKeys, key)
	}

	sort.Strings(dimFltrKeys)

	for _, dimensionColumnName := range dimFltrKeys {
		conditionWhere := ""
		dimensionFilter := scopeMap[dimensionColumnName] // want it in a sorted order

		// AND CONDITION
		sort.Sort(byDimLvlFltrCol(dimensionFilter.AND)) // sort before using
		for _, dimLevelFilter := range dimensionFilter.AND {
			if len(dimLevelFilter.DimLevelColumnName) == 0 {
				errS := fmt.Sprintf("Scope dimension level column name for dimension %s is empty (AND condition)", dimensionColumnName)
				log.Printf("ERROR: datamodels/access_views.go/queryScopeToWhereConditions: %s", errS)
				return "", errors.New(errS)
			}
			if len(dimLevelFilter.Values) == 0 {
				errS := fmt.Sprintf("Scope dimension level values for dimension %s is empty (AND condition)", dimensionColumnName)
				log.Printf("ERROR: datamodels/access_views.go/queryScopeToWhereConditions: %s", errS)
				return "", errors.New(errS)
			}
			sort.Ints(dimLevelFilter.Values)
			idsValues := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(dimLevelFilter.Values)), ", "), "[]")
			conditionWhere += fmt.Sprintf("%s_id %s (%s) %s ", dimLevelFilter.DimLevelColumnName, dimLevelFilter.CmpOperator.String(), idsValues, "AND")
		}
		if len(conditionWhere) > 0 {
			conditionWhere = conditionWhere[:len(conditionWhere)-4]
			results += fmt.Sprintf("(%s) AND ", conditionWhere)
		}

		// OR CONDITION
		conditionWhere = ""
		sort.Sort(byDimLvlFltrCol(dimensionFilter.OR)) // sort before using
		for _, dimLevelFilter := range dimensionFilter.OR {
			if len(dimLevelFilter.DimLevelColumnName) == 0 {
				errS := fmt.Sprintf("Scope dimension level column name for dimension %s is empty (OR condition)", dimensionColumnName)
				log.Printf("ERROR: datamodels/access_views.go/queryScopeToWhereConditions: %s", errS)
				return "", errors.New(errS)
			}
			if len(dimLevelFilter.Values) == 0 {
				errS := fmt.Sprintf("Scope dimension level values for dimension %s is empty (OR condition)", dimensionColumnName)
				log.Printf("ERROR: datamodels/access_views.go/queryScopeToWhereConditions: %s", errS)
				return "", errors.New(errS)
			}
			sort.Ints(dimLevelFilter.Values)
			idsValues := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(dimLevelFilter.Values)), ", "), "[]")
			conditionWhere += fmt.Sprintf("%s_id %s (%s) %s ", dimLevelFilter.DimLevelColumnName, dimLevelFilter.CmpOperator.String(), idsValues, "OR")
		}
		if len(conditionWhere) > 0 {
			conditionWhere = conditionWhere[:len(conditionWhere)-3]
			results += fmt.Sprintf("(%s) AND ", conditionWhere)
		}
	}
	if len(results) > 0 {
		results = results[:len(results)-5]
	}
	return results, nil
}

func (q *Queries) accessViewScopeToMapScope(ctx context.Context, datasetID int, userId *string, datatableID int, writeRequired bool) (MapScope, error) {
	accessViewScopeDB := MapScope{}

	if userId != nil {
		accessViewScopeDBBytes, err := q.queryJsonAccessViewScopeDB(ctx, datasetID, userId, writeRequired)
		if err != nil {
			log.Printf("ERROR datamodels/access_views.go/accessViewScopeToMapScope: %s", err)
			return nil, err
		}
		if accessViewScopeDBBytes != nil {
			mapScopeDB, err := loadJsonDBToMapScope(accessViewScopeDBBytes)
			if err != nil {
				log.Printf("ERROR datamodels/access_views.go/accessViewScopeToMapScope: %s", err)
				return nil, err
			}

			// Get dim and dim levels by datatable
			mapDimAndDimLevelByDT, err := q.GetDADimensionAndDimLevelsByDatatable(ctx, datasetID, datatableID)
			if err != nil {
				log.Printf("ERROR datamodels/access_views.go/accessViewScopeToMapScope: %s", err)
				return nil, err
			}

			// Filter by dim and dim levels from datatable
			for dimensionColumnName, dimensionFilter := range mapScopeDB {
				listDimensionLevel, ok := mapDimAndDimLevelByDT[dimensionColumnName]
				if !ok {
					continue
				}
				dimFilter := DimensionFilter{}
				dimFilter.DimensionColumnName = dimensionColumnName

				for _, dimLevelFilter := range dimensionFilter.AND {
					if DimFilterContainsColumnName(listDimensionLevel, dimLevelFilter.DimLevelColumnName) {
						dimFilter.AND = append(dimFilter.AND, dimLevelFilter)
					}
				}
				for _, dimLevelFilter := range dimensionFilter.OR {
					if DimFilterContainsColumnName(listDimensionLevel, dimLevelFilter.DimLevelColumnName) {
						dimFilter.OR = append(dimFilter.OR, dimLevelFilter)
					}
				}
				accessViewScopeDB[dimensionColumnName] = dimFilter
			}

		}
	}
	return accessViewScopeDB, nil
}

func (q *Queries) queryScopeToFullScopeJson(ctx context.Context, datasetID int, userID *string, scopeInput *DAQueryScopeInput, writeRequired bool) (string, error) {
	scopeMapJsonInput, err := scopeToMapScopeJson(scopeInput)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/queryScopeToFullScopeJson: %s", err)
		return "", err
	}

	scopeMapJsonDB, err := q.queryJsonAccessViewScopeDB(ctx, datasetID, userID, writeRequired)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/queryScopeToFullScopeJson: %s", err)
		return "", err
	}

	accessViewScopeDBJson := MapScopeJson{}
	if scopeMapJsonDB != nil {
		err = json.Unmarshal(scopeMapJsonDB, &accessViewScopeDBJson)
		if err != nil {
			errS := fmt.Sprintf("[JSON Scope Invalid] %s", err.Error())
			log.Printf("ERROR: datamodels/access_views.go/queryScopeToFullScopeJson: %s", errS)
			return "", err
		}
	}

	scopeJsonIngest := FullScopeJson{}
	scopeJsonIngest.AccessViewScope = accessViewScopeDBJson
	scopeJsonIngest.RequestedScope = scopeMapJsonInput
	jsonScopeBytes, err := json.Marshal(scopeJsonIngest)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/queryScopeToFullScopeJson: %s [json.Marshal]", err)
	}

	return string(jsonScopeBytes), nil
}

func scopeToMapScopeJson(scopeInput *DAQueryScopeInput) (MapScopeJson, error) {
	scopeJson := MapScopeJson{} //map[string]map[string][]map[string]map[string][]int
	if scopeInput == nil {
		return scopeJson, nil
	}

	for _, dimensionFilter := range scopeInput.DimensionFilters {
		dimFilterJson := map[string][]map[string]map[string][]int{}
		if len(dimensionFilter.DimensionColumnName) == 0 {
			errS := "Dimension column name is empty"
			log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s", errS)
			return nil, errors.New(errS)
		}
		if len(dimensionFilter.AND) == 0 && len(dimensionFilter.OR) == 0 {
			errS := fmt.Sprintf("Dimension filter conditions is empty in dimension %s", dimensionFilter.DimensionColumnName)
			log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s", errS)
			return nil, errors.New(errS)
		}

		// CONDITION AND
		dimFilterJsonAND := []map[string]map[string][]int{}
		for _, dimensionLevelFilterInput := range dimensionFilter.AND {
			dimLevelFilter := map[string]map[string][]int{}
			if len(dimensionLevelFilterInput.DimensionLevelColumnName) == 0 {
				errS := fmt.Sprintf("Dimension level column name in dimension %s is empty [condition AND]", dimensionFilter.DimensionColumnName)
				log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s [condition AND]", errS)
				return nil, errors.New(errS)
			}
			dimFilterCmpValues := map[string][]int{}
			values := []int{}
			for _, val := range dimensionLevelFilterInput.Values {
				intVal, err := strconv.Atoi(val)
				if err != nil {
					log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s [condition AND]", err)
					return nil, err
				}
				values = append(values, intVal)
			}
			dimFilterCmpValues[dimensionLevelFilterInput.CmpOperator.String()] = values
			dimLevelFilter[dimensionLevelFilterInput.DimensionLevelColumnName] = dimFilterCmpValues
			dimFilterJsonAND = append(dimFilterJsonAND, dimLevelFilter)
		}

		// CONDITION OR
		dimFilterJsonOR := []map[string]map[string][]int{}
		for _, dimensionLevelFilterInput := range dimensionFilter.OR {
			dimLevelFilter := map[string]map[string][]int{}
			if len(dimensionLevelFilterInput.DimensionLevelColumnName) == 0 {
				errS := fmt.Sprintf("Dimension level column name in dimension %s is empty", dimensionFilter.DimensionColumnName)
				log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s [condition OR]", errS)
				return nil, errors.New(errS)
			}
			dimFilterCmpValues := map[string][]int{}
			values := []int{}
			for _, val := range dimensionLevelFilterInput.Values {
				intVal, err := strconv.Atoi(val)
				if err != nil {
					log.Printf("ERROR datamodels/access_views.go/scopeToMapScopeJson: %s [condition OR]", err)
					return nil, err
				}
				values = append(values, intVal)
			}
			dimFilterCmpValues[dimensionLevelFilterInput.CmpOperator.String()] = values
			dimLevelFilter[dimensionLevelFilterInput.DimensionLevelColumnName] = dimFilterCmpValues
			dimFilterJsonOR = append(dimFilterJsonOR, dimLevelFilter)
		}
		dimFilterJson["and"] = dimFilterJsonAND
		dimFilterJson["or"] = dimFilterJsonOR
		scopeJson[dimensionFilter.DimensionColumnName] = dimFilterJson
	}
	return scopeJson, nil
}

func (q *Queries) queryJsonAccessViewScopeDB(ctx context.Context, datasetID int, userId *string, writeRequired bool) ([]byte, error) {
	if userId == nil {
		return nil, nil
	}

	scopeField := "read"
	if writeRequired {
		scopeField = "write"
	}
	query := fmt.Sprintf(`
		SELECT access_view_%s_scope FROM access_views av
			NATURAL JOIN user_access_views uav
			WHERE uav.user_id = ?
	`, scopeField)
	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, *userId)

	var accessViewScopeDBBytes []byte
	err := row.Scan(&accessViewScopeDBBytes)
	if err != nil && err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		log.Printf("ERROR datamodels/access_views.go/QueryJsonAccessViewScopeDB: %s", err)
		return nil, err
	}
	return accessViewScopeDBBytes, nil
}

func loadScopeToMapScope(scopeInput DAQueryScopeInput) (MapScope, error) {
	mapScope := MapScope{}
	for _, dimensionFilter := range scopeInput.DimensionFilters {
		if dimensionFilter == nil {
			continue
		}

		if len(dimensionFilter.DimensionColumnName) == 0 {
			errS := "Dimension column name is empty"
			log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s", errS)
			return nil, errors.New(errS)
		}
		if len(dimensionFilter.AND) == 0 && len(dimensionFilter.OR) == 0 {
			errS := fmt.Sprintf("Dimension filter conditions is empty in dimension %s", dimensionFilter.DimensionColumnName)
			log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s", errS)
			return nil, errors.New(errS)
		}

		dimFilterStruct := DimensionFilter{}
		dimFilterStruct.DimensionColumnName = dimensionFilter.DimensionColumnName

		// CONDITION AND
		for _, dimensionLevelFilterInput := range dimensionFilter.AND {
			if len(dimensionLevelFilterInput.DimensionLevelColumnName) == 0 {
				errS := fmt.Sprintf("Dimension level column name in dimension %s is empty [condition AND]", dimensionFilter.DimensionColumnName)
				log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s [condition AND]", errS)
				return nil, errors.New(errS)
			}
			dimensionLevelFilter := DimensionLevelFilter{}
			dimensionLevelFilter.DimLevelColumnName = dimensionLevelFilterInput.DimensionLevelColumnName
			dimensionLevelFilter.CmpOperator = dimensionLevelFilterInput.CmpOperator
			for _, val := range dimensionLevelFilterInput.Values {
				intVal, err := strconv.Atoi(val)
				if err != nil {
					log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s [condition AND]", err)
					return nil, err
				}
				dimensionLevelFilter.Values = append(dimensionLevelFilter.Values, intVal)
				sort.Ints(dimensionLevelFilter.Values)
			}
			dimFilterStruct.AND = append(dimFilterStruct.AND, &dimensionLevelFilter)
		}

		// CONDITION OR
		for _, dimensionLevelFilterInput := range dimensionFilter.OR {
			if len(dimensionLevelFilterInput.DimensionLevelColumnName) == 0 {
				errS := fmt.Sprintf("Dimension level column name in dimension %s is empty", dimensionFilter.DimensionColumnName)
				log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s [condition OR]", errS)
				return nil, errors.New(errS)
			}
			dimensionLevelFilter := DimensionLevelFilter{}
			dimensionLevelFilter.DimLevelColumnName = dimensionLevelFilterInput.DimensionLevelColumnName
			dimensionLevelFilter.CmpOperator = dimensionLevelFilterInput.CmpOperator
			for _, val := range dimensionLevelFilterInput.Values {
				intVal, err := strconv.Atoi(val)
				if err != nil {
					log.Printf("ERROR datamodels/access_views.go/loadScopeToMapScope: %s [condition OR]", err)
					return nil, err
				}
				dimensionLevelFilter.Values = append(dimensionLevelFilter.Values, intVal)
				sort.Ints(dimensionLevelFilter.Values)
			}
			dimFilterStruct.OR = append(dimFilterStruct.OR, &dimensionLevelFilter)
		}

		mapScope[dimensionFilter.DimensionColumnName] = dimFilterStruct
	}
	return mapScope, nil
}

func loadJsonDBToMapScope(jsonDB []byte) (MapScope, error) {
	var accessViewScopeDBJson MapScopeJson
	err := json.Unmarshal(jsonDB, &accessViewScopeDBJson)
	if err != nil {
		errS := fmt.Sprintf("[JSON Scope Invalid] %s", err.Error())
		log.Printf("ERROR datamodels/access_views.go/LoadJsonDBToMapScope: %s", errS)
		return nil, err
	}
	return loadMapScopeJsonToMapScope(accessViewScopeDBJson)
}

func loadMapScopeJsonToMapScope(json MapScopeJson) (MapScope, error) {
	mapScope := MapScope{}

	for dimColumnName, logOpers := range json {
		if len(dimColumnName) == 0 {
			errS := "Dimension column name is empty"
			log.Printf("ERROR datamodels/access_views.go/loadMapScopeJsonToMapScope: %s", errS)
			return nil, errors.New(errS)
		}

		dimFilterStruct := DimensionFilter{}
		dimFilterStruct.DimensionColumnName = dimColumnName

		for logOpS, conditions := range logOpers {
			isAND := false
			isOR := false
			if strings.ToUpper(logOpS) == "AND" {
				isAND = true
			} else if strings.ToUpper(logOpS) == "OR" {
				isOR = true
			} else {
				errS := fmt.Sprintf("Logical operator in dimension %s is not valid (%s) [expected AND or OR]", dimColumnName, logOpS)
				log.Printf("ERROR datamodels/access_views.go/loadMapScopeJsonToMapScope: %s", errS)
				return nil, errors.New(errS)
			}

			for _, condition := range conditions {
				for dimLevel, valuesIn := range condition {
					if len(dimLevel) == 0 {
						errS := fmt.Sprintf("Dimension level column name in dimension %s is empty", dimColumnName)
						log.Printf("ERROR datamodels/access_views.go/loadMapScopeJsonToMapScope: %s", errS)
						return nil, errors.New(errS)
					}

					dimensionLevelFilter := DimensionLevelFilter{}
					dimensionLevelFilter.DimLevelColumnName = dimLevel

					for cmpOp, values := range valuesIn {
						if strings.ToUpper(cmpOp) == "IN" {
							dimensionLevelFilter.CmpOperator = DAComparisonOperatorIn
						} else {
							errS := fmt.Sprintf("Compare operator in dimension level %s is not valid (%s) [expected IN]", dimLevel, cmpOp)
							log.Printf("ERROR datamodels/access_views.go/loadMapScopeJsonToMapScope: %s", errS)
							return nil, errors.New(errS)
						}

						dimensionLevelFilter.Values = append(dimensionLevelFilter.Values, values...)
					}
					if isAND {
						dimFilterStruct.AND = append(dimFilterStruct.AND, &dimensionLevelFilter)
					} else if isOR {
						dimFilterStruct.OR = append(dimFilterStruct.OR, &dimensionLevelFilter)
					}
				}
			}
		}
		mapScope[dimColumnName] = dimFilterStruct
	}

	return mapScope, nil
}

type validateQueryParams struct {
	datasetId       int
	datatableName   string
	dimLevels       []string
	aggrMeasures    []string
	scope           *DAQueryScopeInput
	postAggFilter   *DAQueryPostAggFilterInput
	sort            *DAQuerySortInput
	postAggGrouping *DAQueryPostAggGroupingInput
	branchID        *string
	version         *string
	userID          *string
}

type validateQueryOutput struct {
	datatable       *DADatatable
	version         *DAVersion
	branch          *DABranch
	aggrMeasuresMap map[string]*AggrMeasureForMap
	measuresMap     map[int]*MeasureForMap
}

func (q *Queries) validateQuery(ctx context.Context, params validateQueryParams) (*validateQueryOutput, error) {
	result := validateQueryOutput{}

	// Validate datatable exists
	datatable, err := q.validateDatatable(ctx, params.datasetId, params.datatableName)
	if err != nil {
		return nil, err
	}
	result.datatable = datatable

	// Validate the version exists
	version, err := q.validateVersion(ctx, params.datasetId, params.version, params.branchID)
	if err != nil {
		return nil, err
	}
	if version.Status != DAVersionStatusApplied || version.AppliedTimestamp == nil {
		err := errors.New("The version is not valid (status != applied or applied timestamp is null)")
		log.Printf("ERROR: datamodels/access_views.go/validateQuery: %s", err)
		return nil, err
	}
	result.version = version
	if version.BranchID != nil {
		params.branchID = version.BranchID
	}

	// Validate the branch exists
	branch, _, err := q.validateBranch(ctx, params.datasetId, params.branchID)
	if err != nil {
		return nil, err
	}
	result.branch = branch

	// Validate all dimension levels are in datatable
	err = q.validateDimensionLevels(ctx, params.datasetId, datatable.id, params.dimLevels)
	if err != nil {
		return nil, err
	}

	// Validate that the measure aggregations exist and the user has proper access to them
	aggrMeasuresMap, measureMap, err := q.validateMeasuresAggregations(ctx, params.datasetId, datatable, params.userID, params.aggrMeasures)
	if err != nil {
		return nil, err
	}
	err = q.validateMeasuresAggrBaseOnly(ctx, params.datasetId, datatable, aggrMeasuresMap, params.dimLevels)
	if err != nil {
		return nil, err
	}
	result.aggrMeasuresMap = aggrMeasuresMap
	result.measuresMap = measureMap

	// Validate that scope's dimension members are correctly defined
	err = q.validateScope(ctx, params.datasetId, params.scope)
	if err != nil {
		return nil, err
	}

	// Validate post aggr grouping
	err = q.validatePostAggrGrouping(ctx, params.datasetId, params.userID, params.postAggGrouping, params.dimLevels, params.aggrMeasures)
	if err != nil {
		return nil, err
	}

	// Validate sort
	err = q.validateSort(ctx, params.datasetId, params.sort, params.postAggGrouping, params.dimLevels, params.aggrMeasures)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

type validateUpdateParams struct {
	datasetId         int
	datatableName     string
	measures          []string // measures or measureIDs
	measureIDs        []int
	dataExtMeasureIDs []int
	scope             *DAQueryScopeInput
	version           *string
	userID            *string
}

type validateUpdateOutput struct {
	datatable    *DADatatable
	measuresData []*DAMeasure // Only filled if measures is passed
}

func (q *Queries) validateUpdate(ctx context.Context, params validateUpdateParams) (*validateUpdateOutput, error) {
	result := validateUpdateOutput{}

	// Validate datatable exists
	datatable, err := q.validateDatatable(ctx, params.datasetId, params.datatableName)
	if err != nil {
		return nil, err
	}
	result.datatable = datatable

	// Validate the version exists
	_, err = q.validateVersion(ctx, params.datasetId, params.version, nil)
	if err != nil {
		return nil, err
	}

	// Validate that the measure exist and the user has proper access to them
	if len(params.measures) > 0 {
		measuresData, err := q.validateMeasures(ctx, params.datasetId, datatable, params.measures)
		if err != nil {
			return nil, err
		}
		result.measuresData = measuresData

		// Collect measure IDs
		params.measureIDs = []int{}
		for _, measure := range measuresData {
			params.measureIDs = append(params.measureIDs, measure.id)
		}
	}

	// Validate that the measure ids exist, data extension ids exists and the user has proper access to them
	if len(params.measureIDs) > 0 || len(params.dataExtMeasureIDs) > 0 {
		err := q.validateUserAccessToMeasures(ctx, params.datasetId, params.userID, true, params.measureIDs, params.dataExtMeasureIDs)
		if err != nil {
			return nil, err
		}
	}

	// Validate that scope's dimension members are correctly defined
	err = q.validateScope(ctx, params.datasetId, params.scope)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (q *Queries) validateDatatable(ctx context.Context, datasetID int, tableName string) (*DADatatable, error) {
	datatable, err := q.GetDADatatable(ctx, datasetID, nil, nil, &tableName)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("ERROR: datamodels/access_views.go/validateDatatable: %s", err)
		return nil, err
	} else if datatable == nil || err == sql.ErrNoRows {
		err = fmt.Errorf("datatable %s not found", tableName)
		log.Printf("ERROR: datamodels/access_views.go/validateDatatable: %s", err)
		return nil, err
	}
	return datatable, nil
}

func (q *Queries) validateVersion(ctx context.Context, datasetID int, versionID *string, branchID *string) (*DAVersion, error) {
	version, err := q.GetDAVersion(ctx, datasetID, versionID, branchID)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateVersion: %s", err)
		return nil, err
	}
	return version, nil
}

func (q *Queries) validateBranch(ctx context.Context, datasetID int, branchID *string) (*DABranch, bool, error) {
	if branchID == nil {
		return nil, false, nil
	}
	branchId, err := strconv.Atoi(*branchID)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateBranch: %s", err)
		return nil, false, err
	}
	if branchId == 0 {
		// This is the main branch
		return nil, true, nil
	}

	branch, err := q.getDABranchByID(ctx, datasetID, branchId)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateBranch: %s", err)
		return nil, false, err
	}
	if branch == nil {
		err := fmt.Errorf("The branch=%d does not exist", branchId)
		log.Printf("ERROR: datamodels/access_views.go/validateBranch: %s", err)
		return nil, false, err
	}
	return branch, false, nil
}

func (q *Queries) validateDimensionLevels(ctx context.Context, datasetID int, datatableID int, dimLevels []string) error {
	if len(dimLevels) == 0 {
		return nil
	}
	db := q.GetDatasetDB(datasetID)

	query, args, err := sqlx.In(`
			SELECT COUNT(*) FROM datatable_dimension_levels ddl
				NATURAL JOIN dimension_levels dl
				WHERE ddl.datatable_id = ? AND dl.dimension_level_column_name IN (?)
		`, datatableID, dimLevels)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateDimensionLevels: %s", err)
		return err
	}
	query = db.Rebind(query)
	foundDimensionLevelCount := 0
	row := db.QueryRowxContext(ctx, query, args...)
	err = row.Scan(&foundDimensionLevelCount)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateDimensionLevels: %s", err)
		return err
	}
	requestedDimensionLevelCount := len(dimLevels)
	if foundDimensionLevelCount != requestedDimensionLevelCount {
		err = fmt.Errorf("dimension levels not found in database (%d requested, %d found)", requestedDimensionLevelCount, foundDimensionLevelCount)
		log.Printf("ERROR: datamodels/access_views.go/validateDimensionLevels: %s", err)
		return err
	}

	return nil
}

func (q *Queries) validateUserAccessToMeasures(ctx context.Context, datasetID int, userID *string, writeRequired bool, measuresRequired []int, dataExtMeasureIDs []int) error {
	if userID == nil {
		return nil
	}
	db := q.GetDatasetDB(datasetID)

	// Get the user's access view
	var args []interface{}

	query := `
	SELECT access_view_id, access_view_read_only
	FROM user_access_views uav
		NATURAL JOIN access_views
	WHERE uav.user_id = ?
	`
	row := db.QueryRowxContext(ctx, query, *userID)
	err := row.Err()
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [get user_access_views]", err)
		return err
	}

	var accessViewId int
	var readOnly bool
	err = row.Scan(&accessViewId, &readOnly)
	if err != nil {
		if err == sql.ErrNoRows {
			err = errors.New(fmt.Sprintf("Couldn't find access view for user=%s", *userID))
		}
		log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [scan row user_access_views]", err)
		return err
	}
	if writeRequired && readOnly {
		err = errors.New(fmt.Sprintf("The user=%s has read-only access", *userID))
		log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s", err)
		return err
	}

	// Validate user access to the measures
	var accessType string
	if writeRequired {
		accessType = "write"
	} else {
		accessType = "read"
	}

	if len(measuresRequired) > 0 {
		query = fmt.Sprintf(`
		SELECT COUNT(*)
		FROM access_view_%s_measures
		WHERE access_view_id = ?
		`, accessType)

		row = db.QueryRowxContext(ctx, query, accessViewId)
		err = row.Err()
		if err != nil {
			log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [get access_view_*_measures by access view id]", err)
			return err
		}
		measuresCount := 0
		err = row.Scan(&measuresCount)
		if err != nil {
			log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [scan row access_view_*_measures by access view id]", err)
			return err
		}

		if measuresCount > 0 {
			// The user is restricted to a subset of measures

			query = fmt.Sprintf(`
			SELECT COUNT(*)
			FROM access_view_%s_measures
			WHERE measure_id IN (?) AND access_view_id = ?
			`, accessType)

			query, args, err = sqlx.In(query, measuresRequired, accessViewId)
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [In measure_column_name]", err)
				return err
			}

			query = db.Rebind(query)
			row = db.QueryRowxContext(ctx, query, args...)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [get access_view_*_measures]", err)
				return err
			}

			foundMeasuresCount := 0
			err = row.Scan(&foundMeasuresCount)
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [scan row access_view_*_measures]", err)
				return err
			}

			requiredMeasuresCount := len(measuresRequired)
			if foundMeasuresCount != requiredMeasuresCount {
				err = errors.New(fmt.Sprintf("The user=%s doesn't have access to all the requested measures (%d required, %d found)",
					*userID, requiredMeasuresCount, foundMeasuresCount))
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s", err)
				return err
			}
		}
	}

	if len(dataExtMeasureIDs) > 0 {
		query = fmt.Sprintf(`
		SELECT COUNT(*)
		FROM access_view_%s_dataextensions
		WHERE access_view_id = ?
		`, accessType)

		row = db.QueryRowxContext(ctx, query, accessViewId)
		err = row.Err()
		if err != nil {
			log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [get access_view_*_dataextensions by access view id]", err)
			return err
		}
		measuresCount := 0
		err = row.Scan(&measuresCount)
		if err != nil {
			log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [scan row access_view_*_dataextensions by access view id]", err)
			return err
		}

		if measuresCount > 0 {
			// The user is restricted to a subset of measures

			query = fmt.Sprintf(`
			SELECT COUNT(*)
			FROM access_view_%s_dataextensions
			WHERE measure_aggregation_id IN (?) AND access_view_id = ?
			`, accessType)

			query, args, err = sqlx.In(query, dataExtMeasureIDs, accessViewId)
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [In measure_column_name]", err)
				return err
			}

			query = db.Rebind(query)
			row = db.QueryRowxContext(ctx, query, args...)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [get access_view_*_dataextensions]", err)
				return err
			}

			foundMeasuresCount := 0
			err = row.Scan(&foundMeasuresCount)
			if err != nil {
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s [scan row access_view_*_dataextensions]", err)
				return err
			}

			requiredMeasuresCount := len(dataExtMeasureIDs)
			if foundMeasuresCount != requiredMeasuresCount {
				err = errors.New(fmt.Sprintf("The user=%s doesn't have access to all the requested measures aggregation for data extensions (%d required, %d found)",
					*userID, requiredMeasuresCount, foundMeasuresCount))
				log.Printf("ERROR: datamodels/access_views.go/validateUserAccessToMeasures: %s", err)
				return err
			}
		}
	}

	return nil
}

func (q *Queries) validateMeasures(ctx context.Context, datasetID int, datatable *DADatatable, measures []string) ([]*DAMeasure, error) {
	measuresData := []*DAMeasure{}
	if len(measures) == 0 {
		return measuresData, nil
	}

	// Validate measures
	var err error
	measuresData, err = q.getDAMeasuresByColumnNameList(ctx, datasetID, &datatable.Id, &measures)
	if err != nil {
		log.Printf("ERROR datamodels/access_views.go/validateMeasures: %s [getDAMeasuresByColumnNameList]", err)
		return nil, err
	}
	if len(measuresData) != len(measures) {
		err = fmt.Errorf("len(measuresData) != len(measures)")
		log.Printf("ERROR: datamodels/access_views.go/validateMeasures: %s", err)
		return nil, err
	}

	return measuresData, nil
}

func (q *Queries) validateMeasuresAggregations(ctx context.Context, datasetID int, datatable *DADatatable, userID *string, aggrMeasures []string) (map[string]*AggrMeasureForMap, map[int]*MeasureForMap, error) {
	if len(aggrMeasures) == 0 {
		return nil, nil, nil
	}

	measuresRequired := []int{}
	measuresFound := map[string]bool{}
	aggrMeasuresMap, measureMap, err := q.GetMapAggrMeasuresByColumnName(ctx, datasetID, aggrMeasures, true)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateMeasuresAggregations: %s", err)
		return nil, nil, err
	}

	for _, data := range measureMap {
		if data.DatatableID != datatable.id {
			err = fmt.Errorf("The measure=%s does not belongs to datatable=%s", data.MeasureColumnName, datatable.TableName)
			log.Printf("ERROR: datamodels/access_views.go/validateMeasuresAggregations: %s", err)
			return nil, nil, err
		}
		if _, ok := measuresFound[data.MeasureColumnName]; !ok {
			measuresRequired = append(measuresRequired, data.MeasureID)
			measuresFound[data.MeasureColumnName] = true
		}
	}
	if len(measuresRequired) > 0 && userID != nil {
		// Get the user's access view
		err := q.validateUserAccessToMeasures(ctx, datasetID, userID, false, measuresRequired, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	return aggrMeasuresMap, measureMap, nil
}

func (q *Queries) validateMeasuresAggrBaseOnly(ctx context.Context, datasetID int, datatable *DADatatable, aggrMeasuresMap map[string]*AggrMeasureForMap, dimLevels []string) error {
	if len(aggrMeasuresMap) == 0 || len(dimLevels) == 0 {
		return nil
	}
	validateBaseOnly := false
	colsName := []string{}
	for columnName, measureAggr := range aggrMeasuresMap {
		if measureAggr.MAggregationType == config.MEASURE_AGGREGATION_TYPE_BASE_ONLY {
			validateBaseOnly = true
			colsName = append(colsName, columnName)
		}
	}
	if !validateBaseOnly {
		return nil
	}

	dimLevelsByDim, err := q.getDimLevelsByDim(ctx, datatable, dimLevels)
	if err != nil {
		return err
	}
	lowestLevel, err := q.areDimLevelsAtLowestLevel(ctx, datasetID, datatable.id, dimLevelsByDim)
	if err != nil {
		return err
	}
	if !lowestLevel {
		err := errors.New(fmt.Sprintf("There are aggregated measures that are of type BASE_ONLY (%s) and the request is not at the lowest level", strings.Join(colsName, ",")))
		return err
	}
	return nil
}

func (q *Queries) validateScope(ctx context.Context, datasetID int, scope *DAQueryScopeInput) error {
	if scope == nil || len(scope.DimensionFilters) == 0 {
		return nil
	}
	db := q.GetDatasetDB(datasetID)

	queryWhere := ""
	args := []interface{}{}
	membersIdMap := map[int]bool{}

	for _, dimensionFilter := range scope.DimensionFilters {
		if len(dimensionFilter.DimensionColumnName) == 0 {
			errS := "Dimension column name is empty"
			log.Printf("ERROR datamodels/access_views.go/validateScope: %s", errS)
			return errors.New(errS)
		}

		dimfilters := map[string][]*DADimensionLevelFilterInput{}
		if len(dimensionFilter.AND) > 0 {
			dimfilters["AND"] = dimensionFilter.AND
		}
		if len(dimensionFilter.OR) > 0 {
			dimfilters["OR"] = dimensionFilter.OR
		}
		if len(dimfilters) == 0 {
			errS := fmt.Sprintf("Dimension filter conditions is empty in dimension %s", dimensionFilter.DimensionColumnName)
			log.Printf("ERROR datamodels/access_views.go/validateScope: %s", errS)
			return errors.New(errS)
		}

		dimensionMemberConditions := []string{}
		for condition, levelFilter := range dimfilters {
			for _, dimensionLevelFilterInput := range levelFilter {
				if len(dimensionLevelFilterInput.DimensionLevelColumnName) == 0 {
					errS := fmt.Sprintf("Dimension level column name in dimension %s is empty [condition %s]", dimensionFilter.DimensionColumnName, condition)
					log.Printf("ERROR datamodels/access_views.go/validateScope: %s", errS)
					return errors.New(errS)
				}

				values := []int{}
				for _, val := range dimensionLevelFilterInput.Values {
					intVal, err := strconv.Atoi(val)
					if err != nil {
						log.Printf("ERROR datamodels/access_views.go/validateScope: %s [condition %s]", err, condition)
						return err
					}
					values = append(values, intVal)
					membersIdMap[intVal] = true
				}

				dimensionMemberConditions = append(dimensionMemberConditions, "(d.dimension_column_name = ? AND dl.dimension_level_column_name = ? AND dm.dimension_member_id IN (?))")
				args = append(args, dimensionFilter.DimensionColumnName, dimensionLevelFilterInput.DimensionLevelColumnName, values)
			}
		}
		queryWhere += fmt.Sprintf("%s OR ", strings.Join(dimensionMemberConditions, " OR "))
	}
	queryWhere = queryWhere[:len(queryWhere)-4]

	query := `
			SELECT COUNT(*) FROM dimension_members dm
				NATURAL JOIN dimensions d
				NATURAL JOIN dimension_levels dl
				WHERE (
					%s
				)
		`
	query = fmt.Sprintf(query, queryWhere)
	query, args, err := sqlx.In(query, args...)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateScope: %s", err)
		return err
	}

	query = db.Rebind(query)
	row := db.QueryRowxContext(ctx, query, args...)
	err = row.Err()
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateScope: %s", err)
		return err
	}

	foundDimensionMembersCount := 0
	err = row.Scan(&foundDimensionMembersCount)
	if err != nil {
		log.Printf("ERROR: datamodels/access_views.go/validateScope: %s", err)
		return err
	}
	if foundDimensionMembersCount != len(membersIdMap) {
		err = fmt.Errorf("Invalid scope")
		log.Printf("ERROR: datamodels/access_views.go/validateScope: %s", err)
		return err
	}
	return nil
}

func (q *Queries) validatePostAggrGrouping(ctx context.Context, datasetID int, userID *string, postAggGrouping *DAQueryPostAggGroupingInput, dimLevels []string, aggrMeasures []string) error {
	if postAggGrouping == nil {
		return nil
	}

	// Validate groupByColumns
	if len(postAggGrouping.GroupByColumns) > 0 {
		for _, columnName := range postAggGrouping.GroupByColumns {
			// One of (dimensionLevels U aggregatedMeasures)
			if (len(dimLevels) > 0 && DimFilterContainsColumnName(dimLevels, columnName)) ||
				(len(aggrMeasures) > 0 && DimFilterContainsColumnName(aggrMeasures, columnName)) {
				continue
			} else {
				err := errors.New(fmt.Sprintf("The post aggregated grouping (GroupByColumns) is invalid, column name=%s doesn't exists in dimensionLevels or aggregatedMeasures", columnName))
				log.Printf("ERROR: datamodels/access_views.go/validatePostAggrGrouping: %s", err)
				return err
			}
		}
	}

	// Validate aggregatedMeasures
	if len(postAggGrouping.AggregatedMeasures) > 0 {
		measuresRequired := []int{}
		measuresFound := map[string]bool{}
		_, measureMap, err := q.GetMapAggrMeasuresByColumnName(ctx, datasetID, postAggGrouping.AggregatedMeasures, false)
		if err != nil {
			log.Printf("ERROR: datamodels/access_views.go/validatePostAggrGrouping: %s", err)
			return err
		}

		for _, data := range measureMap {
			// TODO: Control datatable
			if _, ok := measuresFound[data.MeasureColumnName]; !ok {
				measuresRequired = append(measuresRequired, data.MeasureID)
				measuresFound[data.MeasureColumnName] = true
			}
		}
		if len(measuresRequired) > 0 && userID != nil {
			// Validate the user's access view
			err := q.validateUserAccessToMeasures(ctx, datasetID, userID, false, measuresRequired, nil)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (q *Queries) validateSort(ctx context.Context, datasetID int, sort *DAQuerySortInput, postAggGrouping *DAQueryPostAggGroupingInput, dimLevels []string, aggrMeasures []string) error {
	if sort == nil {
		return nil
	}

	for _, each := range (*sort).Entries {
		if postAggGrouping == nil {
			// One of (dimensionLevels U aggregatedMeasures)
			if (len(dimLevels) > 0 && DimFilterContainsColumnName(dimLevels, each.ColumnName)) ||
				(len(aggrMeasures) > 0 && DimFilterContainsColumnName(aggrMeasures, each.ColumnName)) {
				continue
			} else {
				err := errors.New(fmt.Sprintf("The sort is invalid, column name=%s doesn't exists in dimensionLevels or aggregatedMeasures", each.ColumnName))
				log.Printf("ERROR: datamodels/access_views.go/validateSort: %s", err)
				return err
			}
		} else {
			// One of (postAggGrouping.groupByColumns U postAggGrouping.aggregatedMeasures)
			if (len(postAggGrouping.GroupByColumns) > 0 && DimFilterContainsColumnName(postAggGrouping.GroupByColumns, each.ColumnName)) ||
				(len(postAggGrouping.AggregatedMeasures) > 0 && DimFilterContainsColumnName(postAggGrouping.AggregatedMeasures, each.ColumnName)) {
				continue
			} else {
				err := errors.New(fmt.Sprintf("The sort is invalid, column name=%s doesn't exists in postAggGrouping.groupByColumns or postAggGrouping.aggregatedMeasures", each.ColumnName))
				log.Printf("ERROR: datamodels/access_views.go/validateSort: %s", err)
				return err
			}
		}
	}
	return nil
}
