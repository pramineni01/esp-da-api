package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"golang.org/x/sync/errgroup"
)

type UpdateDataInput struct {
	DatasetID string                `json:"datasetID"`
	BranchID  *string               `json:"branchID"`
	Changes   []*UpdateMeasureInput `json:"changes"`
}

type DADataUpdateType string

const (
	DAUpdateTypePlugging   DADataUpdateType = "PLUGGING"
	DAUpdateTypeEqualSplit DADataUpdateType = "EQUAL_SPLIT"
	DAUpdateTypeWeighted   DADataUpdateType = "WEIGHTED"
)

type UpdateMeasureInput struct {
	Datatable         string             `json:"datatable"`
	Scope             *DAQueryScopeInput `json:"scope"`
	AggregatedMeasure string             `json:"aggregatedMeasure"`
	Value             string             `json:"value"`
	Type              DADataUpdateType   `json:"type"`
	WeightingMeasure  *string            `json:"weightingMeasure"`
}

func (q *Queries) ApplyDataUpdates(ctx context.Context, updates UpdateDataInput) (string, error) {
	datasetId, err := strconv.Atoi(updates.DatasetID)
	if err != nil || q.dbs[datasetId] == nil {
		error := errors.New(fmt.Sprintf("datasetID %s not found", updates.DatasetID))
		log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdate: %s", error)
		return "", error
	}

	// Get user id from context
	userID, ok := getUserID(ctx)
	if !ok {
		log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates [getUserID]: No found userID in context")
		return "", errors.New("No found userID in context")
	}

	// Validate branch
	branch, isLive, err := q.validateBranch(ctx, datasetId, updates.BranchID)
	if err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s", err)
		return "", err
	}

	// Collect info about aggregated-measures and measures
	measureAggregations := []string{}
	keys := make(map[string]bool)
	for _, update := range updates.Changes {
		if _, ok := keys[update.AggregatedMeasure]; !ok {
			keys[update.AggregatedMeasure] = true
			measureAggregations = append(measureAggregations, update.AggregatedMeasure)
		}
	}
	aggrMeasuresMap, measureMap, err := q.GetMapAggrMeasuresByColumnName(ctx, datasetId, measureAggregations, true)
	if err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s", err)
		return "", err
	}
	maggs, err := q.getMeasureAggregationInfo(ctx, datasetId, measureAggregations)
	if err != nil {
		log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [Get Measures]", err)
		return "", err
	}

	// Validate the updates
	validations := []*validateUpdateOutput{}
	for pos, update := range updates.Changes {
		magg, ok := aggrMeasuresMap[update.AggregatedMeasure]
		if !ok {
			err = fmt.Errorf("The update=%d is invalid: measure-agg=%s does not exist", pos, update.AggregatedMeasure)
			log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s", err)
			return "", err
		}

		dataExtMeasureIDs := []int{}
		measureIDs := []int{}
		if magg.MAggregationType == DAAggregationTypeExtension {
			dataExtMeasureIDs = append(dataExtMeasureIDs, magg.MAggregationID)
		} else if magg.MAggregationType != DAAggregationTypeBaseOnly && magg.MAggregationType != DAAggregationTypeSum {
			err = fmt.Errorf("The update=%d is invalid: measure-agg=%s has type=%s", pos, update.AggregatedMeasure, magg.MAggregationType)
			log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s", err)
			return "", err
		} else {
			measureIDs = append(measureIDs, *magg.MeasureID)
		}

		validation, err := q.validateUpdate(ctx, validateUpdateParams{
			datasetId:         datasetId,
			datatableName:     update.Datatable,
			measureIDs:        measureIDs,
			dataExtMeasureIDs: dataExtMeasureIDs,
			scope:             update.Scope,
			userID:            &userID,
		})
		if err != nil {
			err = fmt.Errorf("The update=%d is invalid [%s]", pos, err)
			log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s", err)
			return "", err
		}
		validations = append(validations, validation)
	}

	// Create a branch if it doesn't exist
	var branchId *int32 = nil
	if branch == nil && !isLive {
		branch, err = q.createBranch(ctx, datasetId)
		if err != nil {
			log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [Create Branch]", err)
			return "", err
		}
		branchId = &branch.id
	}

	// Get a write lock
	mutex := q.GetUpdateLock(ctx, datasetId, branchId, LockSettings{
		Expiry:     time.Duration(2) * time.Minute,
		Tries:      200,
		RetryDelay: 100 * time.Millisecond,
	})
	if err := mutex.Lock(ctx); err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s [Acquire Lock]", err)
		return "", err
	}
	defer func() {
		// Release lock
		ok, err := mutex.Unlock(ctx)
		if err != nil {
			log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates: %s [Unlock Failed]", err)
		} else if !ok {
			log.Printf("ERROR: datamodels/dataupdates.go/ApplyDataUpdates [Unlock Failed]")
		}
	}()

	// Construct the needed queries and the different version updates
	datatableUpdateQueries := [][]string{}
	cacheLayerUpdateQueries := []string{}
	newVersionParams := CreateVersionParams{
		BranchID:       nil,
		UserID:         &userID,
		Started:        true,
		VersionUpdates: []CreateVersionUpdateParams{},
	}

	for pos, update := range updates.Changes {
		magg, _ := aggrMeasuresMap[update.AggregatedMeasure]
		var measure *MeasureForMap
		if magg.MAggregationType != DAAggregationTypeExtension {
			measure = measureMap[*magg.MeasureID]
		}
		datatable := validations[pos].datatable
		var ok bool
		var maggInfo measureAggInfo

		if magg.MAggregationType != DAAggregationTypeExtension {
			maggInfo, ok = maggs[update.AggregatedMeasure]
			if !ok {
				err = fmt.Errorf("Measure aggregation %s not found in database", update.AggregatedMeasure)
				log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [Check Measure]", err)
				return "", err
			}
		} else {
			maggInfo.AggregationDataExtID = magg.MAggregationExtensionID
			maggInfo.MeasureAggregation = magg.MAggregationColumnName
			maggInfo.MeasureAggregationID = strconv.Itoa(magg.MAggregationID)
			maggInfo.AggregationType = magg.MAggregationType
		}

		// Get the datatable update queries
		queryBlocks, scopeSql, err := q.getDatatableUpdateQueries(ctx, datasetId, datatable, branch, update, userID, maggInfo)
		if err != nil {
			log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [getDatatableUpdateQueries]", err)
			return "", err
		}
		datatableUpdateQueries = append(datatableUpdateQueries, queryBlocks...)

		if magg.MAggregationType != DAAggregationTypeExtension {
			// Get the cache layer update queries
			queries, err := q.getCacheLayerUpdateQueries(ctx, datatable, branch, measure.MeasureID, scopeSql)
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [getCacheLayerUpdateQueries]", err)
				return "", err
			}
			cacheLayerUpdateQueries = append(cacheLayerUpdateQueries, queries...)
		}

		// Add the version update
		var versionUpdateOp VersionUpdateOperation
		switch update.Type {
		case DAUpdateTypePlugging:
			versionUpdateOp = VersionUpdateOpPLUGGING
		case DAUpdateTypeEqualSplit:
			versionUpdateOp = VersionUpdateOpEQUALSPLIT
		case DAUpdateTypeWeighted:
			versionUpdateOp = VersionUpdateOpWEIGHTED
		}
		jsonScope, err := q.queryScopeToFullScopeJson(ctx, datasetId, &userID, update.Scope, true)
		if err != nil {
			log.Printf("ERROR datamodels/dataupdates.go/ApplyDataUpdates: %s [queryScopeToFullScopeJson]", err)
			return "", err
		}
		newVersionParams.VersionUpdates = append(newVersionParams.VersionUpdates, CreateVersionUpdateParams{
			VersionUpdateScope: &jsonScope,
			Operation:          versionUpdateOp,
		})

		if branch != nil {
			if magg.MAggregationType != DAAggregationTypeExtension {
				// Update branch-datatable relationship
				err = q.addBranchDatatable(ctx, datasetId, branch.id, datatable.id, measure.MeasureDataTypeID)
				if err != nil {
					return "", err
				}
			} else {
				// Update branch-dataextension relationship
				dataExtID, _ := strconv.Atoi(*magg.MAggregationExtensionID)
				err = q.addBranchDataExtension(ctx, datasetId, branch.id, dataExtID)
				if err != nil {
					return "", err
				}
			}
		}
	}

	// Create a version
	if branch != nil {
		newVersionParams.BranchID = &branch.id
	}
	version, err := q.CreateVersion(ctx, datasetId, newVersionParams)
	if err != nil {
		return "", err
	}
	log.Printf("INFO datamodels/dataupdates.go/ApplyDataUpdates: The version=%d has been created", version.VersionID)

	for _, updateQueries := range datatableUpdateQueries {
		// Run the datatable updates in bulks
		err = q.RunQueryBulk(ctx, datasetId, updateQueries, config.DATA_UPDATE_BULK_SIZE, false)
		if err != nil {
			return "", err
		}
	}

	if len(cacheLayerUpdateQueries) > 0 {
		// Run the cache layer updates in bulks
		err = q.RunQueryBulk(ctx, datasetId, cacheLayerUpdateQueries, config.DATA_UPDATE_BULK_SIZE, false)
		if err != nil {
			return "", err
		}
	}

	// Mark version as applied
	err = q.applyVersion(ctx, datasetId, version.VersionID)
	if err != nil {
		return "", err
	}
	log.Printf("INFO datamodels/dataupdates.go/ApplyDataUpdates: The version=%d has been marked as applied", version.VersionID)

	if branch != nil {
		return branch.ID, nil
	}
	return "0", nil
}

// Private methods ------------------------------------------------------------

func (q *Queries) getDatatableUpdateQueries(ctx context.Context, datasetId int, datatable *DADatatable, branch *DABranch,
	update *UpdateMeasureInput, userID string, maggInfo measureAggInfo) ([][]string, string, error) {

	var scopeSql *string
	var err error

	if maggInfo.AggregationType == DAAggregationTypeExtension {
		scopeSql, err = q.queryScopeOnlyToWhereConditions(ctx, update.Scope)
		if err != nil {
			log.Printf("ERROR: datamodels/dataupdates.go/getDatatableUpdateQueries: %s [queryScopeOnlyToWhereConditions]", err)
			return nil, "", err
		}
	} else {
		scopeSql, err = q.queryScopeToWhereConditions(ctx, datasetId, datatable.id, &userID, update.Scope, true)
		if err != nil {
			log.Printf("ERROR: datamodels/dataupdates.go/getDatatableUpdateQueries: %s [queryScopeToWhereConditions]", err)
			return nil, "", err
		}
	}

	where := ""
	if scopeSql != nil && len(*scopeSql) > 0 {
		where += "AND " + *scopeSql + " "
	}

	if maggInfo.AggregationType == DAAggregationTypeExtension {
		// DATA EXTENSION
		updateDataExtensionQueries, err := q.UpdateDataExtension(ctx, datasetId, datatable, branch, *update, userID, maggInfo, where)
		if err != nil {
			return nil, "", err
		}
		return updateDataExtensionQueries, where, nil
	}

	switch update.Type {
	case DAUpdateTypePlugging:
		// type PLUGGING
		updatePluggingQueries, err := q.UpdatePlugging(ctx, datasetId, datatable, branch, *update, userID, maggInfo, where)
		if err != nil {
			return nil, "", err
		}
		return updatePluggingQueries, where, nil
	case DAUpdateTypeEqualSplit:
		// type EQUAL_SPLIT
		updateEqualSplitQueries, err := q.UpdateEqualSplit(ctx, datasetId, datatable, branch, *update, userID, maggInfo, where)
		if err != nil {
			return nil, "", err
		}
		return updateEqualSplitQueries, where, nil
	case DAUpdateTypeWeighted:
		// Type WEIGHTED
		updateWeightedQueries, err := q.UpdateWeighted(ctx, datasetId, datatable, branch, *update, userID, maggInfo, where)
		if err != nil {
			return nil, "", err
		}
		return updateWeightedQueries, where, nil
	default:
		return nil, "", errors.New(fmt.Sprintf("update type %s is not allowed", update.Type))
	}
}

func (q *Queries) UpdatePlugging(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userID string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	queriesUpdate := []string{}
	var err error

	if len(update.Value) == 0 {
		// Value is NULL

		// Execute updates for each partition
		for partition := 0; partition < datatable.Partitions; partition++ {
			var query string
			if branch != nil {
				// Update on a branch
				query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
					SELECT
						base_id,
						measure_id,
						NULL AS value,
						%s AS branch_id
					FROM dt%d_dimension_data PARTITION (p%d)
					NATURAL JOIN dt%d_base_measures_%s_branches PARTITION (p%d)
					WHERE branch_id = %s %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
					datatable.id, magg.MeasureType, partition,
					branch.ID,
					datatable.id, partition,
					datatable.id, magg.MeasureType, partition,
					branch.ID, whereConditionsString,
				)
			} else {
				// Update the live branch
				query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
					SELECT
						base_id,
						measure_id,
						NULL AS value
					FROM dt%d_dimension_data PARTITION (p%d)
					NATURAL JOIN dt%d_base_measures_%s PARTITION (p%d)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
					datatable.id, magg.MeasureType, partition,
					datatable.id, partition,
					datatable.id, magg.MeasureType, partition,
					whereConditionsString,
				)
			}
			queriesUpdate = append(queriesUpdate, query)
		}

		return [][]string{queriesUpdate}, nil
	}

	// Value not NULL
	pluggingValue := ""

	switch magg.MeasureCastType {
	case DAMeasureCastTypeSignedInteger, DAMeasureCastTypeUnsignedInteger:
		valueInt, err := strconv.Atoi(update.Value)
		if err != nil {
			err = errors.New("Value not is a valid integer number")
			log.Printf("ERROR datamodels/dataupdates.go/UpdatePlugging: %s [Read Value Param]", err)
			return nil, err
		}
		pluggingValue = fmt.Sprintf("%d", valueInt)
	case DAMeasureCastTypeFloat, DAMeasureCastTypeDouble:
		valueFloat, err := strconv.ParseFloat(update.Value, 64)
		if err != nil {
			err = errors.New("Value not is a valid float number")
			log.Printf("ERROR datamodels/dataupdates.go/UpdatePlugging: %s [Read Value Param]", err)
			return nil, err
		}
		pluggingValue = fmt.Sprintf("%f", valueFloat)
	default:
		err = errors.New("Measure type not is a integer or float, this operation is not allowed")
		log.Printf("ERROR datamodels/dataupdates.go/UpdatePlugging: %s [Check Value]", err)
		return nil, err
	}

	// Execute updates for each partition
	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
					INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
						SELECT
							base_id,
							%s AS measure_id,
							%s AS value,
							%s AS branch_id
						FROM dt%d_dimension_data PARTITION (p%d)
						WHERE 1=1 %s
					ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				pluggingValue,
				branch.ID,
				datatable.id, partition,
				whereConditionsString,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
					INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
						SELECT
							base_id,
							%s AS measure_id,
							%s AS value
						FROM dt%d_dimension_data PARTITION (p%d)
						WHERE 1=1 %s
					ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				pluggingValue,
				datatable.id, partition,
				whereConditionsString,
			)
		}
		queriesUpdate = append(queriesUpdate, query)
	}

	return [][]string{queriesUpdate}, nil

}

// UpdateEqualSplit distributes the difference between the last value and the new value as evenly as it can among the partitions of the datatable.
// Each partition with rows in scope is given an equal share of the value to be distributed, and its rows are updated as evenly as possible.
func (q *Queries) UpdateEqualSplit(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userID string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	var err error

	if len(update.Value) == 0 {
		err = errors.New("Value cannot be null for an EVEN_SPLIT update; use PLUGGING instead")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Check Value]", err)
		return nil, err
	}

	if magg.AggregationType != DAAggregationTypeSum {
		err = errors.New("For EQUAL_SPLIT updates, the aggregated measure must be of type SUM")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Check SUM]", err)
		return nil, err
	}

	switch magg.MeasureCastType {
	case DAMeasureCastTypeSignedInteger, DAMeasureCastTypeUnsignedInteger:
		updateEqualSplitQueries, err := q.UpdateEqualSplitInteger(ctx, datasetID, datatable, branch, update, userID, magg, whereConditionsString)
		if err != nil {
			return nil, err
		}
		return updateEqualSplitQueries, nil
	case DAMeasureCastTypeFloat, DAMeasureCastTypeDouble:
		updateEqualSplitQueries, err := q.UpdateEqualSplitFloat(ctx, datasetID, datatable, branch, update, userID, magg, whereConditionsString)
		if err != nil {
			return nil, err
		}
		return updateEqualSplitQueries, nil
	default:
		err = errors.New("Measure type not is a integer or float, this operation is not allowed")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Check Value]", err)
		return nil, err
	}

}

func (q *Queries) UpdateEqualSplitFloat(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userId string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	queriesUpdate := []string{}

	value, err := strconv.ParseFloat(update.Value, 64)
	if err != nil {
		err = errors.New("Value not is a valid float number")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Read Value Param Float]", err)
		return nil, err
	}

	// Collect row count from each partition in parallel
	counts := make([]int, datatable.Partitions, datatable.Partitions)
	g, _ := errgroup.WithContext(context.Background())
	for partition := 0; partition < datatable.Partitions; partition++ {
		partition := partition
		g.Go(func() error {
			query := fmt.Sprintf(`
					SELECT COUNT(*) FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				`, datatable.id, partition, whereConditionsString,
			)
			var count int
			db := q.GetDatasetDB(datasetID)
			row := db.QueryRowxContext(ctx, query)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Query Row Count Float]", err)
				return err
			}
			if err = row.Scan(&count); err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Query Row Count Float]", err)
				return err
			}
			counts[partition] = count
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	rows := 0
	for _, cTmp := range counts {
		rows += cTmp
	}
	equalSplitValue := value / float64(rows)

	// Execute updates for each partition
	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
					SELECT
						base_id,
						%s AS measure_id,
						%f AS value,
						%s AS branch_id
					FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				equalSplitValue,
				branch.ID,
				datatable.id, partition,
				whereConditionsString,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
					SELECT
						base_id,
						%s AS measure_id,
						%f AS value
					FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				equalSplitValue,
				datatable.id, partition,
				whereConditionsString,
			)
		}

		queriesUpdate = append(queriesUpdate, query)
	}

	return [][]string{queriesUpdate}, nil
}

func (q *Queries) UpdateEqualSplitInteger(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userId string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	queriesUpdate := []string{}

	value, err := strconv.Atoi(update.Value)
	if err != nil {
		err = errors.New("Value not is a valid integer number")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Read Value Param Integer]", err)
		return nil, err
	}

	// Collect row count from each partition in parallel
	counts := make([]int, datatable.Partitions, datatable.Partitions)
	g, _ := errgroup.WithContext(context.Background())
	for partition := 0; partition < datatable.Partitions; partition++ {
		partition := partition
		g.Go(func() error {
			query := fmt.Sprintf(`
					SELECT COUNT(*) FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				`, datatable.id, partition, whereConditionsString,
			)
			var count int
			db := q.GetDatasetDB(datasetID)
			row := db.QueryRowxContext(ctx, query)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Query Row Count Integer]", err)
				return err
			}
			if err = row.Scan(&count); err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateEqualSplit: %s [Query Row Count Integer]", err)
				return err
			}
			counts[partition] = count
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	rows := 0
	for _, cTmp := range counts {
		rows += cTmp
	}
	valueDist := int(math.Round(float64(value) / float64(rows)))
	difference := value - rows*valueDist
	valueRest := 0
	countDist := 0
	if difference < 0 {
		countDist = rows - int(math.Abs(float64(difference)))
		valueRest = valueDist - 1
	} else {
		countDist = rows - difference
		valueRest = valueDist + 1
	}

	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		var valueCalc string

		if countDist <= 0 {
			valueCalc = fmt.Sprintf("%d AS value", valueRest)
		} else if countDist >= counts[partition] {
			valueCalc = fmt.Sprintf("%d AS value", valueDist)
			countDist -= counts[partition]
		} else {
			valueCalc = fmt.Sprintf("IF((ROW_NUMBER() OVER ()) <= %d, %d, %d) AS value", countDist, valueDist, valueRest)
			countDist = 0
		}

		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
					SELECT
						base_id,
						%s AS measure_id,
						%s,
						%s AS branch_id
					FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				valueCalc,
				branch.ID,
				datatable.id, partition,
				whereConditionsString,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
					SELECT
						base_id,
						%s AS measure_id,
						%s
					FROM dt%d_dimension_data PARTITION (p%d)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				valueCalc,
				datatable.id, partition,
				whereConditionsString,
			)
		}

		queriesUpdate = append(queriesUpdate, query)
	}
	return [][]string{queriesUpdate}, nil
}

func (q *Queries) UpdateWeighted(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userID string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	var err error

	if len(update.Value) == 0 {
		err = errors.New("Value cannot be null for a WEIGHTED update")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Check Value]", err)
		return nil, err
	}
	if update.WeightingMeasure == nil || len(*update.WeightingMeasure) == 0 {
		err = errors.New("WeightingMeasure cannot be null for a WEIGHTED update")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Check WeightingMeasure]", err)
		return nil, err
	}

	if magg.AggregationType != DAAggregationTypeSum {
		err = errors.New("For UpdateWeighted updates, the aggregated measure must be of type SUM")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Check SUM]", err)
		return nil, err
	}

	maggsWeighted, err := q.getMeasureAggregationInfo(ctx, datasetID, []string{*update.WeightingMeasure})
	if err != nil {
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Get WeightingMeasureInfo]", err)
		return nil, err
	}

	maggWeighted, ok := maggsWeighted[*update.WeightingMeasure]
	if !ok {
		err = fmt.Errorf("Weighting measure aggregation %s not found in database", update.AggregatedMeasure)
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Check Measure]", err)
		return nil, err
	}

	switch magg.MeasureCastType {
	case DAMeasureCastTypeSignedInteger, DAMeasureCastTypeUnsignedInteger:
		updateWeightedQueries, err := q.UpdateWeightedInteger(ctx, datasetID, datatable, branch, update, userID, magg, maggWeighted, whereConditionsString)
		if err != nil {
			return nil, err
		}
		return updateWeightedQueries, nil
	case DAMeasureCastTypeFloat, DAMeasureCastTypeDouble:
		updateWeightedQueries, err := q.UpdateWeightedFloat(ctx, datasetID, datatable, branch, update, userID, magg, maggWeighted, whereConditionsString)
		if err != nil {
			return nil, err
		}
		return updateWeightedQueries, nil
	default:
		err = errors.New("Measure type not is a integer or float, this operation is not allowed")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeighted: %s [Check Value]", err)
		return nil, err
	}

}

func (q *Queries) UpdateWeightedFloat(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userId string, magg measureAggInfo, maggWeighted measureAggInfo, whereConditionsString string) ([][]string, error) {
	queriesUpdate := []string{}

	value, err := strconv.ParseFloat(update.Value, 64)
	if err != nil {
		err = errors.New("Value not is a valid float number")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedFloat: %s [Read Value Param Float]", err)
		return nil, err
	}

	branchTime := time.Now().Format(config.DEFAULT_TIME_FORMAT)
	if branch != nil && branch.FromTimestamp.Valid {
		branchTime = branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)
	}

	// Collect total weight value from each partition in parallel
	countsTotalWeight := make([]float64, datatable.Partitions, datatable.Partitions)
	g, _ := errgroup.WithContext(context.Background())
	for partition := 0; partition < datatable.Partitions; partition++ {
		partition := partition
		g.Go(func() error {
			var query string

			if branch != nil {
				// Update on a branch
				query = fmt.Sprintf(`
					SELECT IFNULL(SUM(CAST( COALESCE(b.value, m.value) AS %s )),0) 
					FROM dt%d_dimension_data PARTITION (p%d) dd
					LEFT JOIN dt%d_base_measures_%s PARTITION (p%d) FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
					ON (dd.base_id = m.base_id AND m.measure_id = %s)
					LEFT JOIN dt%d_base_measures_%s_branches PARTITION (p%d) b
					ON (dd.base_id = b.base_id AND b.measure_id = %s AND b.branch_id = %s)
					WHERE 1=1 %s;`,
					maggWeighted.MeasureCastType,
					datatable.id, partition,
					datatable.id, maggWeighted.MeasureType, partition, branchTime,
					maggWeighted.MeasureID.String,
					datatable.id, maggWeighted.MeasureType, partition,
					maggWeighted.MeasureID.String, branch.ID,
					whereConditionsString,
				)
			} else {
				// Update the live branch
				query = fmt.Sprintf(`
					SELECT IFNULL(SUM(value),0) 
					FROM dt%d_dimension_data PARTITION (p%d)
					NATURAL JOIN dt%d_base_measures_%s PARTITION (p%d)
					WHERE measure_id = %s %s;`,
					datatable.id, partition,
					datatable.id, maggWeighted.MeasureType, partition,
					maggWeighted.MeasureID.String, whereConditionsString,
				)
			}

			var count float64
			db := q.GetDatasetDB(datasetID)
			row := db.QueryRowxContext(ctx, query)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedFloat: %s [Query Row Count Float]", err)
				return err
			}
			if err = row.Scan(&count); err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedFloat: %s [Query Row Count Float]", err)
				return err
			}
			if err != nil && err != sql.ErrNoRows {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedFloat: %s [Query Row Count Float]", err)
				return err
			} else if err != nil && err == sql.ErrNoRows {
				countsTotalWeight[partition] = 0
			} else {
				countsTotalWeight[partition] = count
			}
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	totalWeight := float64(0)
	for _, cTmp := range countsTotalWeight {
		totalWeight += cTmp
	}
	if totalWeight == 0 {
		err = errors.New("Total weight is zero!")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedFloat: %s [Calculate total weight]", err)
		return nil, err
	}

	// Execute updates for each partition
	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
					SELECT
						COALESCE(b.base_id, m.base_id) AS base_id,
						%s AS measure_id,
						%f * CAST(IFNULL(CAST( COALESCE(b.value, m.value) AS %s ), 0) AS DOUBLE)/%f AS value,
						%s AS branch_id
					FROM dt%d_dimension_data PARTITION (p%d) dd
					LEFT JOIN dt%d_base_measures_%s PARTITION (p%d) FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
					ON (dd.base_id = m.base_id AND m.measure_id = %s)
					LEFT JOIN dt%d_base_measures_%s_branches PARTITION (p%d) b
					ON (dd.base_id = b.base_id AND b.measure_id = %s AND b.branch_id = %s)
					WHERE 1=1 %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				value, maggWeighted.MeasureCastType, totalWeight,
				branch.ID,
				datatable.id, partition,
				datatable.id, maggWeighted.MeasureType, partition, branchTime,
				maggWeighted.MeasureID.String,
				datatable.id, maggWeighted.MeasureType, partition,
				maggWeighted.MeasureID.String, branch.ID,
				whereConditionsString,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
				INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
					SELECT
						base_id,
						%s AS measure_id,
						%f * CAST(IFNULL(value, 0) AS DOUBLE)/%f AS value
					FROM dt%d_dimension_data PARTITION (p%d)
					NATURAL LEFT JOIN dt%d_base_measures_%s PARTITION (p%d)
					WHERE measure_id = %s %s
				ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				magg.MeasureID.String,
				value, totalWeight,
				datatable.id, partition,
				datatable.id, maggWeighted.MeasureType, partition,
				maggWeighted.MeasureID.String, whereConditionsString,
			)
		}
		queriesUpdate = append(queriesUpdate, query)
	}

	return [][]string{queriesUpdate}, nil
}

func (q *Queries) UpdateWeightedInteger(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userId string, magg measureAggInfo, maggWeighted measureAggInfo, whereConditionsString string) ([][]string, error) {
	queriesUpdate := []string{}

	value, err := strconv.Atoi(update.Value)
	if err != nil {
		err = errors.New("Value not is a valid integer number")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Read Value Param Integer]", err)
		return nil, err
	}

	branchTime := time.Now().Format(config.DEFAULT_TIME_FORMAT)
	if branch != nil && branch.FromTimestamp.Valid {
		branchTime = branch.FromTimestamp.Time.Format(config.DEFAULT_TIME_FORMAT)
	}

	// Collect total weight value from each partition in parallel
	counts := make([]int, datatable.Partitions, datatable.Partitions)
	countsTotalWeight := make([]float64, datatable.Partitions, datatable.Partitions)
	g, _ := errgroup.WithContext(context.Background())
	for partition := 0; partition < datatable.Partitions; partition++ {
		partition := partition
		g.Go(func() error {
			var query string

			if branch != nil {
				// Update on a branch
				query = fmt.Sprintf(`
					SELECT IFNULL(SUM(CAST( COALESCE(b.value, m.value) AS %s )),0), COUNT(*)
					FROM dt%d_dimension_data PARTITION (p%d) dd
					LEFT JOIN dt%d_base_measures_%s PARTITION (p%d) FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
					ON (dd.base_id = m.base_id AND m.measure_id = %s)
					LEFT JOIN dt%d_base_measures_%s_branches PARTITION (p%d) b
					ON (dd.base_id = b.base_id AND b.measure_id = %s AND b.branch_id = %s)
					WHERE 1=1 %s;`,
					maggWeighted.MeasureCastType,
					datatable.id, partition,
					datatable.id, maggWeighted.MeasureType, partition, branchTime,
					maggWeighted.MeasureID.String,
					datatable.id, maggWeighted.MeasureType, partition,
					maggWeighted.MeasureID.String, branch.ID,
					whereConditionsString,
				)
			} else {
				// Update the live branch
				query = fmt.Sprintf(`
					SELECT IFNULL(SUM(value),0), COUNT(*)
					FROM dt%d_dimension_data PARTITION (p%d)
					NATURAL JOIN dt%d_base_measures_%s PARTITION (p%d)
					WHERE measure_id = %s %s;`,
					datatable.id, partition,
					datatable.id, maggWeighted.MeasureType, partition,
					maggWeighted.MeasureID.String, whereConditionsString,
				)
			}

			var count int
			var countSUM float64
			db := q.GetDatasetDB(datasetID)
			row := db.QueryRowxContext(ctx, query)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			}
			if err = row.Scan(&countSUM, &count); err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			}
			if err != nil && err != sql.ErrNoRows {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			} else if err != nil && err == sql.ErrNoRows {
				countsTotalWeight[partition] = 0
				counts[partition] = 0
			} else {
				countsTotalWeight[partition] = countSUM
				counts[partition] = count
			}
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
	}

	totalWeightF := float64(0)
	for _, cTmp := range countsTotalWeight {
		totalWeightF += cTmp
	}
	totalWeight := math.Round(totalWeightF)
	if totalWeight == 0 {
		err = errors.New("Total weight is zero!")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Calculate total weight]", err)
		return nil, err
	}

	// Create temporal tables
	createTemporalTables := []string{}
	deleteTemporalTables := []string{}
	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
				CREATE OR REPLACE TABLE update_%d_weighted_allocation_p%d ENGINE=MEMORY AS
					SELECT
						COALESCE(b.base_id, m.base_id) AS base_id,
						%s as measure_id,
						ROUND(%d * CAST(IFNULL(CAST( COALESCE(b.value, m.value) AS %s), 0) AS DOUBLE)/%f) as value
					FROM dt%d_dimension_data PARTITION (p%d) dd
					LEFT JOIN dt%d_base_measures_%s PARTITION (p%d) FOR SYSTEM_TIME AS OF TIMESTAMP'%s' m
					ON (dd.base_id = m.base_id AND m.measure_id = %s)
					LEFT JOIN dt%d_base_measures_%s_branches PARTITION (p%d) b
					ON (dd.base_id = b.base_id AND b.measure_id = %s AND b.branch_id = %s)
					WHERE 1=1 %s;`,
				datatable.id, partition,
				magg.MeasureID.String, value, maggWeighted.MeasureCastType, totalWeight,
				datatable.id, partition,
				datatable.id, maggWeighted.MeasureType, partition, branchTime,
				maggWeighted.MeasureID.String,
				datatable.id, maggWeighted.MeasureType, partition,
				maggWeighted.MeasureID.String, branch.ID,
				whereConditionsString,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
				CREATE OR REPLACE TABLE update_%d_weighted_allocation_p%d ENGINE=MEMORY AS
					SELECT base_id, %s as measure_id, ROUND(%d * CAST(IFNULL(value, 0) AS DOUBLE)/%f) as value
		  			FROM dt%d_dimension_data PARTITION (p%d) 
		  			NATURAL LEFT JOIN dt%d_base_measures_%s PARTITION (p%d)
		   			WHERE measure_id = %s %s;`,
				datatable.id, partition,
				magg.MeasureID.String, value, totalWeight,
				datatable.id, partition,
				datatable.id, maggWeighted.MeasureType, partition,
				maggWeighted.MeasureID.String, whereConditionsString,
			)
		}

		createTemporalTables = append(createTemporalTables, query)
		deleteTemporalTables = append(deleteTemporalTables, fmt.Sprintf(`DROP TABLE update_%d_weighted_allocation_p%d`, datatable.id, partition))
	}

	// Run the create temporal tables in bulks
	err = q.RunQueryBulk(ctx, datasetID, createTemporalTables, config.DATA_UPDATE_BULK_SIZE, false)
	if err != nil {
		return nil, err
	}

	// Collect total sum value from each partition in parallel
	countsSumRounded := make([]int, datatable.Partitions, datatable.Partitions)
	g2, _ := errgroup.WithContext(context.Background())
	for partition := 0; partition < datatable.Partitions; partition++ {
		partition := partition
		g2.Go(func() error {
			query := fmt.Sprintf(`
						SELECT IFNULL(SUM(value),0) as rounded_value 
						FROM update_%d_weighted_allocation_p%d;`,
				datatable.id, partition,
			)

			var count int
			db := q.GetDatasetDB(datasetID)
			row := db.QueryRowxContext(ctx, query)
			err = row.Err()
			if err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			}
			if err = row.Scan(&count); err != nil {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			}
			if err != nil && err != sql.ErrNoRows {
				log.Printf("ERROR datamodels/dataupdates.go/UpdateWeightedInteger: %s [Query Row Count Integer]", err)
				return err
			} else if err != nil && err == sql.ErrNoRows {
				countsSumRounded[partition] = 0
			} else {
				countsSumRounded[partition] = count
			}
			return nil
		})
	}
	err = g2.Wait()
	if err != nil {
		return nil, err
	}

	roundedValue := 0
	for _, cTmp := range countsSumRounded {
		roundedValue += cTmp
	}

	difference := value - roundedValue
	valueRest := ""
	countDist := difference
	if difference < 0 {
		countDist = int(math.Abs(float64(difference)))
		valueRest = "- 1"
	} else {
		valueRest = "+ 1"
	}

	for partition := 0; partition < datatable.Partitions; partition++ {
		var query string
		var valueCalc string

		if countDist <= 0 {
			valueCalc = fmt.Sprintf("d.value AS value")
		} else if countDist >= counts[partition] {
			valueCalc = fmt.Sprintf("d.value %s AS value", valueRest)
			countDist -= counts[partition]
		} else {
			valueCalc = fmt.Sprintf("IF(ROW_NUMBER() OVER (ORDER BY d.value DESC) <= %d, d.value %s, d.value) as value", countDist, valueRest)
			countDist = 0
		}
		if branch != nil {
			// Update on a branch
			query = fmt.Sprintf(`
						INSERT INTO dt%d_base_measures_%s_branches PARTITION (p%d)
							SELECT base_id, measure_id, %s, %s AS branch_id
							FROM update_%d_weighted_allocation_p%d d
						ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				valueCalc, branch.ID,
				datatable.id, partition,
			)
		} else {
			// Update the live branch
			query = fmt.Sprintf(`
						INSERT INTO dt%d_base_measures_%s PARTITION (p%d)
							SELECT base_id, measure_id, %s
							FROM update_%d_weighted_allocation_p%d d
						ON DUPLICATE KEY UPDATE value=VALUE(value);`,
				datatable.id, magg.MeasureType, partition,
				valueCalc,
				datatable.id, partition,
			)
		}

		queriesUpdate = append(queriesUpdate, query)
	}

	return [][]string{queriesUpdate, deleteTemporalTables}, nil
}

func (q *Queries) UpdateDataExtension(ctx context.Context, datasetID int, datatable *DADatatable, branch *DABranch, update UpdateMeasureInput, userID string, magg measureAggInfo, whereConditionsString string) ([][]string, error) {
	var err error

	if len(update.Value) == 0 {
		err = errors.New("Value cannot be null for a DataExtension update")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check Value]", err)
		return nil, err
	}

	if magg.AggregationDataExtID == nil {
		err = errors.New("Dataextension ID for measure aggregation cannot be null")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check DataExtension ID]", err)
		return nil, err
	}

	if update.Scope == nil || len(update.Scope.DimensionFilters) == 0 {
		err := errors.New("Scope empty not allowed for update data extension")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s", err)
		return nil, err
	}

	dataExtID, _ := strconv.Atoi(*magg.AggregationDataExtID)
	mAggrID, _ := strconv.Atoi(magg.MeasureAggregationID)

	aggrMeasureForMap := AggrMeasureForMap{
		MAggregationID:          mAggrID,
		MAggregationExtensionID: magg.AggregationDataExtID,
		MAggregationColumnName:  magg.MeasureAggregation,
	}

	// Get dim levels from scope
	dimLevels, err := q.getDimLevelsByScope(ctx, datasetID, update.Scope)
	if err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/UpdateDataExtension: %s", err)
		return nil, err
	}
	if dimLevels == nil || len(dimLevels) == 0 {
		err = errors.New("Not found dimensions levels in scope")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check Dimension levels]", err)
		return nil, err
	}

	// Get the matching data extensions
	aggrMeasureExtensionMap, err := q.getMatchingDataExtension(ctx, getMatchingDataExtensionParams{
		DatasetID:            datasetID,
		AggrMeasureExtension: []*AggrMeasureForMap{&aggrMeasureForMap},
		DimensionLevels:      dimLevels,
	})
	if err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/UpdateDataExtension: %s", err)
		return nil, err
	}

	if aggrMeasureExtensionMap == nil || len(aggrMeasureExtensionMap) == 0 {
		err = errors.New("Dataextension ID and measure aggregation not be matching to dimensions levels")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check Matching DataExtension]", err)
		return nil, err
	}

	dataextensionId, ok := aggrMeasureExtensionMap[magg.MeasureAggregation]
	if !ok {
		err = errors.New("Dataextension ID not found in matching for this measure aggregation")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check Matching DataExtension ID]", err)
		return nil, err
	}
	if dataExtID != dataextensionId {
		err = errors.New("Dataextension ID not matching for this measure aggregation")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Check DataExtension ID]", err)
		return nil, err
	}

	queryDimLevel, queryDimLevelValues, err := q.getQueryDimLevelsDataExtByScope(ctx, datasetID, update.Scope)
	if err != nil {
		log.Printf("ERROR: datamodels/dataupdates.go/UpdateDataExtension: %s", err)
		return nil, err
	}
	if queryDimLevel == nil || len(*queryDimLevel) == 0 || queryDimLevelValues == nil || len(*queryDimLevelValues) == 0 {
		err = errors.New("Not found dimensions levels or values in scope")
		log.Printf("ERROR datamodels/dataupdates.go/UpdateDataExtension: %s [Get Query Dimension levels By Scope]", err)
		return nil, err
	}

	var query string
	if branch != nil {
		// Update on a branch
		query = fmt.Sprintf(`
				INSERT INTO dataextensions_%d_branches (%s, %s, branch_id)
					VALUE (%s, %s, %s)
				ON DUPLICATE KEY UPDATE %s=%s;`,
			dataExtID, *queryDimLevel, magg.MeasureAggregation,
			*queryDimLevelValues, update.Value, branch.ID,
			magg.MeasureAggregation, update.Value,
		)
	} else {
		// Update the live branch
		query = fmt.Sprintf(`
				INSERT INTO dataextensions_%d (%s, %s)
					VALUE (%s, %s)
				ON DUPLICATE KEY UPDATE %s=%s;`,
			dataExtID, *queryDimLevel, magg.MeasureAggregation,
			*queryDimLevelValues, update.Value,
			magg.MeasureAggregation, update.Value,
		)
	}

	return [][]string{[]string{query}}, nil

}
