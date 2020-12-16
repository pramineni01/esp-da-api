package datamodels

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
)

// This file contains some logic required for retrieving dataset data that isn't associated with any particular models.

func (q *Queries) GetLatestUpdateTimestamp(ctx context.Context, datasetId int, branchId *int) (*time.Time, error) {
	var key string
	if branchId != nil {
		key = fmt.Sprintf("dataset/%d/branch/%d/version-applied-timestamp", datasetId, *branchId)
	} else {
		key = fmt.Sprintf("dataset/%d/live/version-applied-timestamp", datasetId)
	}
	val, err := q.rdb.ZRevRange(ctx, key, 0, 0).Result()
	if err == redis.Nil || len(val) == 0 {
		timestamp, err := q.queryLatestUpdateTimestamp(ctx, datasetId, branchId)
		if err != nil {
			log.Printf("ERROR datamodels/get_data.go/GetLatestUpdateTimestamp: %s", err)
			return nil, err
		}
		if timestamp == nil {
			// have to fall back to live data
			key = fmt.Sprintf("dataset/%d/live/version-applied-timestamp", datasetId)
			val, err = q.rdb.ZRevRange(ctx, key, 0, 0).Result()
			if err != nil {
				log.Printf("ERROR datamodels/get_data.go/GetLatestUpdateTimestamp: %s", err)
				return nil, err
			}
			res, err := time.Parse(time.RFC3339, val[0])
			if err != nil {
				log.Printf("ERROR datamodels/get_data.go/GetLatestUpdateTimestamp: %s", err)
			}
			return &res, err
		} else {
			// update the cache
			err = q.updateLatestUpdateTimestamp(ctx, *timestamp, datasetId, branchId)
			return timestamp, err
		}
	} else if err != nil {
		log.Printf("ERROR datamodels/get_data.go/GetLatestUpdateTimestamp: %s", err)
		return nil, err
	}
	res, err := time.Parse(time.RFC3339, val[0])
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/GetLatestUpdateTimestamp: %s", err)
	}
	return &res, err
}

const getLatestBranchUpdateTimestamp = `
SELECT GREATEST(MAX(v.applied_timestamp, b.branch_from_timestamp)) FROM branches b
INNER JOIN versions v ON v.branch_id = b.branch_id
WHERE b.branch_id = ?
`

const getLatestLiveUpdateTimestamp = `
SELECT MAX(v.applied_timestamp) FROM versions v WHERE v.branch_id IS NULL
`

func (q *Queries) queryLatestUpdateTimestamp(ctx context.Context, datasetId int, branchId *int) (*time.Time, error) {
	var row *sqlx.Row
	db := q.GetDatasetDB(datasetId)
	if branchId != nil {
		row = db.QueryRowxContext(ctx, getLatestBranchUpdateTimestamp, branchId)
	} else {
		// Query the live data
		row = db.QueryRowxContext(ctx, getLatestLiveUpdateTimestamp)
	}
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/queryLatestUpdateTimestamp: %s", err)
		return nil, err
	}
	var timestamp sql.NullTime
	row.Scan(&timestamp)
	if !timestamp.Valid {
		log.Printf("timestamp invalid")
		return nil, nil
	}
	return &timestamp.Time, nil
}

// Update the last timestamp in redis
func (q *Queries) updateLatestUpdateTimestamp(ctx context.Context, timestamp time.Time, datasetId int, branchId *int) error {
	var key string
	if branchId == nil {
		key = fmt.Sprintf("dataset/%d/live/version-applied-timestamp", datasetId)
	} else {
		key = fmt.Sprintf("dataset/%d/branch/%d/version-applied-timestamp", datasetId, *branchId)
	}

	_, err := q.rdb.ZAdd(ctx, key, &redis.Z{
		Score:  float64(timestamp.Unix()),
		Member: timestamp.Format(time.RFC3339),
	}).Result()
	if err != nil {
		return err
	}
	_, err = q.rdb.ZRemRangeByRank(ctx, key, 0, -2).Result()
	return err
}

func (q *Queries) getQueryID(ctx context.Context, datasetId int) (int64, error) {
	return q.rdb.Incr(ctx, fmt.Sprintf("dataset/%d/query_id", datasetId)).Result()
}

// Retrieves information from redis about the columns specified in query
func (q *Queries) FindMeasureOrDimensionLevelInfoByColumnName(ctx context.Context, datasetId int, columnName string) (id int, isMeasure bool, isAggregated bool, measureType string, err error) {
	val, err := q.rdb.Get(ctx, fmt.Sprintf("column-names/%s", columnName)).Result()
	if err == redis.Nil {
		return q.loadDimensionLevelOrMeasureByColumnName(ctx, datasetId, columnName)
	}
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/findMeasureOrDimensionLevelInfoByColumnName: %s", err)
		return
	}
	vals := strings.Split(val, ":")
	switch vals[0] {
	case "measure":
		isMeasure = true
		isAggregated = false
		measureType = vals[2]
	case "measure-aggregation":
		isMeasure = true
		isAggregated = true
		measureType = vals[2]
	case "dimension-level":
		isMeasure = false
	default:
		err = fmt.Errorf("Malformed column name value: %s", columnName)
		log.Printf("ERROR datamodels/get_data.go/findMeasureOrDimensionLevelInfoByColumnName: %s", err)
		return
	}
	id, err = strconv.Atoi(vals[1])
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/findMeasureOrDimensionLevelInfoByColumnName: %s", err)
		return
	}
	return
}

// load info into redis if not present
func (q *Queries) loadDimensionLevelOrMeasureByColumnName(ctx context.Context, datasetId int, columnName string) (id int, isMeasure bool, isAggregated bool, measureType string, err error) {
	dimensionLevel, err := q.getDADimensionLevelByColumnName(ctx, datasetId, columnName)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
		return
	}
	if dimensionLevel != nil {
		id, err = strconv.Atoi(dimensionLevel.ID)
		if err != nil {
			log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
			return
		}
		isMeasure = false
		isAggregated = false
		storeVal := fmt.Sprintf("dimension-level:%d", id)
		q.rdb.Set(ctx, fmt.Sprintf("column-names/%s", columnName), storeVal, 0)
		return
	}
	measure, err := q.getDAMeasureByColumnName(ctx, datasetId, &columnName)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
		return
	}
	if measure != nil {
		id, err = strconv.Atoi(measure.ID)
		if err != nil {
			log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
			return
		}
		isMeasure = true
		isAggregated = false
		_, measureTypeColumnName, measureTypeErr := q.findMeasureTypeByID(ctx, datasetId, measure.MeasureTypeID)
		if measureTypeErr != nil {
			err = measureTypeErr
			return
		}
		measureType = measureTypeColumnName
		storeVal := fmt.Sprintf("measure:%d:%s", id, measureType)
		q.rdb.Set(ctx, fmt.Sprintf("column-names/%s", columnName), storeVal, 0)
		return
	}

	aggregatedMeasure, err := q.getDAAggregatedMeasureByColumnName(ctx, datasetId, &columnName)
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
		return
	}
	id, err = strconv.Atoi(aggregatedMeasure.ID)
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/loadDimensionOrMeasureByColumnName: %s", err)
		return
	}
	isMeasure = true
	isAggregated = true
	storeVal := fmt.Sprintf("measure-aggregation:%d:%s", id, aggregatedMeasure.AggregationType)
	q.rdb.Set(ctx, fmt.Sprintf("column-names/%s", columnName), storeVal, 0)
	return
}

func (q *Queries) findMeasureTypeByID(ctx context.Context, datasetId int, id string) (columnType string, columnName string, err error) {
	val, err := q.rdb.Get(ctx, fmt.Sprintf("measure-types/%s", id)).Result()
	if err == redis.Nil {
		return q.loadMeasureTypeByID(ctx, datasetId, id)
	}
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/findMeasureTypeByID: %s", err)
		return
	}
	vals := strings.Split(val, ":")
	if len(vals) != 2 {
		err = fmt.Errorf("malformed measure type value %s", val)
		log.Printf("ERROR datamodels/get_data.go/findMeasureTypeByID: %s", err)
		return
	}
	columnType = vals[0]
	columnName = vals[1]
	return
}

func (q *Queries) loadMeasureTypeByID(ctx context.Context, datasetId int, id string) (columnType string, columnName string, err error) {
	measureType, err := q.getDAMeasureTypeByID(ctx, datasetId, id)
	if err != nil {
		log.Printf("ERROR datamodels/get_data.go/loadMeasureTypeByID: %s", err)
		return
	}
	columnType = measureType.ColumnType
	columnName = measureType.ColumnName
	storeVal := fmt.Sprintf("%s:%s", columnType, columnName)
	q.rdb.Set(ctx, fmt.Sprintf("measure-types/%s", id), storeVal, 0)
	return
}
