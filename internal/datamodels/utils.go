package datamodels

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/config"
	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"
)

func (q *Queries) GetTimeApplied(ctx context.Context, datasetID int, version *string, branchID *string) (*time.Time, error) {
	var timeApplied time.Time
	if version != nil {
		// applied_timestamp (VERSION_TIME) -> getted from version
		ver, err := q.GetVersion(ctx, datasetID, GetVersionParams{VersionID: version})
		if err != nil {
			return nil, err
		}
		timeApplied = ver.AppliedTimestamp.Time
	} else {
		// applied_timestamp (VERSION_TIME) ->  greatest at that time
		timeApp, err := q.GetLatestAppliedTimestamp(ctx, datasetID, branchID)
		if err != nil {
			return nil, err
		}
		timeApplied = *timeApp
	}
	return &timeApplied, nil
}

func getUserID(ctx context.Context) (string, bool) {
	// return "d2aeaa1c-1d0c-4edf-bcd7-87c9435337bc", true
	val, ok := ctx.Value("userId").(string)
	if ok && len(strings.TrimSpace(val)) == 0 {
		return "", false
	}
	return val, ok
}

func getLocaleId(ctx context.Context) (int, error) {
	var localeID int
	var err error

	if ctx.Value("userLocale") == nil || ctx.Value("userLocale") == "" {
		localeID = config.DEFAULT_LOCALE_ID
	} else {
		localeID, err = strconv.Atoi(ctx.Value("userLocale").(string))
		if err != nil {
			log.Printf("ERROR: datamodels/dataqueries.go/getLocaleId: %s", err)
			return localeID, err
		}
	}

	return localeID, nil
}

func ValueToString(input interface{}, value string, castType *DAMeasureCastType) string {
	switch val := input.(type) {
	case []uint8:
		value = string(val)
	case int32:
	case int64:
		value = fmt.Sprintf("%d", val)
	case float32:
	case float64:
		value = fmt.Sprintf("%f", val)
	case bool:
		if val {
			value = "true"
		} else {
			value = "false"
		}
	case time.Time:
		if castType != nil {
			switch *castType {
			case DAMeasureCastTypeDate:
				value = val.Format(config.CASTYYPE_DATE_FORMAT)
			case DAMeasureCastTypeDatetime:
				value = val.Format(config.CASTYYPE_DATETIME_FORMAT)
			case DAMeasureCastTypeTime:
				value = val.Format(config.CASTYYPE_TIME_FORMAT)
			default:
				value = val.String()
			}
		} else {
			value = val.String()
		}
	}
	return value
}

func SqlQueryTrim(query string) string {
	query = strings.ReplaceAll(query, "\n\t", " ")
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.TrimSpace(query)
	return query
}

func SqlQuery2String(query string, args ...interface{}) string {
	var buffer bytes.Buffer
	nArgs := len(args)
	for i, part := range strings.Split(query, "?") {
		buffer.WriteString(part)
		if i < nArgs {
			switch a := args[i].(type) {
			case int64:
			case int32:
			case int:
				buffer.WriteString(fmt.Sprintf("%d", a))
			default:
				buffer.WriteString(fmt.Sprintf("%s", a))
			}
		}
	}
	return buffer.String()
}

func SqlQuery2StringTypes(query string, args ...interface{}) string {
	var buffer bytes.Buffer
	nArgs := len(args)
	for i, part := range strings.Split(query, "?") {
		buffer.WriteString(part)
		if i < nArgs {
			switch a := args[i].(type) {
			case int64:
			case int:
				buffer.WriteString(fmt.Sprintf("%d", a))
			case bool:
				buffer.WriteString(fmt.Sprintf("%t", a))
			case sql.NullBool:
				if a.Valid {
					buffer.WriteString(fmt.Sprintf("%t", a.Bool))
				} else {
					buffer.WriteString("NULL")
				}
			case sql.NullInt64:
				if a.Valid {
					buffer.WriteString(fmt.Sprintf("%d", a.Int64))
				} else {
					buffer.WriteString("NULL")
				}
			case sql.NullString:
				if a.Valid {
					buffer.WriteString(fmt.Sprintf("%q", a.String))
				} else {
					buffer.WriteString("NULL")
				}
			case sql.NullFloat64:
				if a.Valid {
					buffer.WriteString(fmt.Sprintf("%f", a.Float64))
				} else {
					buffer.WriteString("NULL")
				}
			default:
				buffer.WriteString(fmt.Sprintf("%q", a))
			}
		}
	}
	return buffer.String()
}

func DumpMapScope(mapScope MapScope) string {
	result := "\n\n***********************************************************\n"
	for dimensionColumnName, dimensionFilter := range mapScope {
		result += fmt.Sprintf("\tDimensionColumnName: %s\n", dimensionColumnName)
		// AND CONDITION
		result += fmt.Sprintf("\t\tCondition AND: [ \n")
		for _, dimLevelFilter := range dimensionFilter.AND {
			result += fmt.Sprintf("\t\t\t{\n")
			result += fmt.Sprintf("\t\t\t\tDimensionLevelColumnName: %s\n", dimLevelFilter.DimLevelColumnName)
			result += fmt.Sprintf("\t\t\t\tCompare operator: %s\n", dimLevelFilter.CmpOperator.String())
			idsValues := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(dimLevelFilter.Values)), ", "), "[]")
			result += fmt.Sprintf("\t\t\t\tValues: %s\n", idsValues)
			result += fmt.Sprintf("\t\t\t}\n")
		}
		result += fmt.Sprintf("\t\t] \n")

		result += fmt.Sprintf("\t\tCondition OR: [ \n")
		// OR CONDITION
		for _, dimLevelFilter := range dimensionFilter.OR {
			result += fmt.Sprintf("\t\t\t{\n")
			result += fmt.Sprintf("\t\t\t\tDimensionLevelColumnName: %s\n", dimLevelFilter.DimLevelColumnName)
			result += fmt.Sprintf("\t\t\t\tCompare operator: %s\n", dimLevelFilter.CmpOperator.String())
			idsValues := strings.Trim(strings.Join(strings.Fields(fmt.Sprint(dimLevelFilter.Values)), ", "), "[]")
			result += fmt.Sprintf("\t\t\t\tValues: %s\n", idsValues)
			result += fmt.Sprintf("\t\t\t}\n")
		}
		result += fmt.Sprintf("\t\t}] \n")
	}
	result += "***********************************************************\n"
	return result
}

func DimFilterContainsColumnName(dimLevelString []string, dimensionLevelColumnName string) bool {
	for _, dimLevelColumnName := range dimLevelString {
		if dimLevelColumnName == dimensionLevelColumnName {
			return true
		}
	}
	return false
}

func UnOrderedEqual(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	exists := make(map[string]bool)
	for _, value := range first {
		exists[value] = true
	}
	for _, value := range second {
		if !exists[value] {
			return false
		}
	}
	return true
}

func DimLevelGroupCheck(dimLevelGroup string, dimLevels []string) bool {
	sort.Strings(dimLevels)
	if strings.Join(dimLevels, ",") == dimLevelGroup {
		return true
	}
	return false
}

func ReleaseMutex(ctx context.Context, pool redis.Pool, key string) (bool, error) {
	var deleteScript = redis.NewScript(1, `
    			if redis.call("EXISTS", KEYS[1]) == 1 then
        			return redis.call("DEL", KEYS[1])
				else
					return 0
				end
			`)
	conn := pool.Get()
	defer conn.Close()
	status, err := conn.Eval(ctx, deleteScript, key)
	if err != nil {
		return false, err
	}
	return status != 0, nil
}

type CacheLayerConstraint struct {
	PrimaryKey    []string
	Indexes       map[string][]string
	Autoincrement *AutoIncrement
}

type Index struct {
	ColumnName string `db:"COLUMN_NAME"`
	KeyName    string `db:"INDEX_NAME"`
}

type AutoIncrement struct {
	ColumnName string `db:"COLUMN_NAME"`
	ColumnType string `db:"COLUMN_TYPE"`
}

func (q *Queries) CacheLayerConstraints(ctx context.Context, datasetId int, tableNames []string) (map[string]*CacheLayerConstraint, error) {
	result := map[string]*CacheLayerConstraint{}
	db := q.GetDatasetDB(datasetId)
	for _, tableName := range tableNames {
		rows, err := db.QueryxContext(ctx, fmt.Sprintf("SELECT COLUMN_NAME, INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA='dadataset%d' AND TABLE_NAME='%s'", datasetId, tableName))
		if err != nil {
			log.Printf("ERROR: datamodels/utils.go/CacheLayerConstraints: [getting indexes from table %s]=%s", tableName, err)
			return nil, err
		}

		var cacheLayerConstraint CacheLayerConstraint
		var primaryKey []string
		indexes := map[string][]string{}
		for rows.Next() {
			var index Index
			err := rows.StructScan(&index)
			if err != nil {
				log.Printf("ERROR: datamodels/utils.go/CacheLayerConstraints: [getting rows] %s", err)
				return nil, err
			}
			if index.KeyName == "PRIMARY" {
				primaryKey = append(primaryKey, index.ColumnName)
			} else {
				indexes[index.KeyName] = append(indexes[index.KeyName], index.ColumnName)
			}
		}

		query := fmt.Sprintf("SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='dadataset%d' AND TABLE_NAME='%s' AND EXTRA LIKE '%s'", datasetId, tableName, "%auto_increment%")
		row := db.QueryRowxContext(ctx, query)
		err = row.Err()
		if err != nil {
			log.Printf("ERROR: datamodels/utils.go/CacheLayerConstraints: %s [getting autoincrement]", err)
			return nil, err
		}

		autoIncrement := AutoIncrement{}
		err = row.StructScan(&autoIncrement)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("ERROR: datamodels/utils.go/CacheLayerConstraints: %s [scan autoincrement]", err)
			return nil, err
		} else if err == sql.ErrNoRows {
			cacheLayerConstraint.Autoincrement = nil
		} else {
			cacheLayerConstraint.Autoincrement = &autoIncrement
		}

		cacheLayerConstraint.PrimaryKey = primaryKey
		cacheLayerConstraint.Indexes = indexes

		result[tableName] = &cacheLayerConstraint
	}

	return result, nil
}
