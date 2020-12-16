package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync"
	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"
	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis/goredis"

	"github.com/jmoiron/sqlx"
)

type DAVersion struct {
	ID                 string          `json:"id"`
	BranchID           *string         `json:"branchID"`
	UserID             *string         `json:"userID"`
	AppliedTimestamp   *time.Time      `json:"appliedTimestamp"`
	KeepAliveTimestamp *time.Time      `json:"keepAliveTimestamp"`
	Status             DAVersionStatus `json:"status"`
	StartedTimestamp   *time.Time      `json:"startedTimestamp"`
}

type Version struct {
	VersionID          int64          `db:"version_id"`
	BranchID           sql.NullInt64  `db:"branch_id"`         // The branch to update.  If null then update base data
	UserID             sql.NullString `db:"user_id"`           // The User who requested the update
	AppliedTimestamp   sql.NullTime   `db:"applied_timestamp"` // Timestamp when the update finished applying before the next one starts
	KeepAliveTimestamp sql.NullTime   `db:"keepalive_timestamp"`
	Status             sql.NullString `db:"status"`
	StartedTimestamp   sql.NullTime   `db:"started_timestamp"`
}

type VersionUpdateOperation string

const (
	VersionUpdateOpWEIGHTED    VersionUpdateOperation = "WEIGHTED"
	VersionUpdateOpPLUGGING    VersionUpdateOperation = "PLUGGING"
	VersionUpdateOpEQUALSPLIT  VersionUpdateOperation = "EQUALSPLIT"
	VersionUpdateOpBATCHLOAD   VersionUpdateOperation = "BATCHLOAD"
	VersionUpdateOpBRANCHMERGE VersionUpdateOperation = "BRANCHMERGE"
)

type DAVersionStatus string

const (
	DAVersionStatusPending    DAVersionStatus = "PENDING"
	DAVersionStatusInProgress DAVersionStatus = "IN_PROGRESS"
	DAVersionStatusError      DAVersionStatus = "ERROR"
	DAVersionStatusApplied    DAVersionStatus = "APPLIED"
)

var AllDAVersionStatus = []DAVersionStatus{
	DAVersionStatusPending,
	DAVersionStatusInProgress,
	DAVersionStatusError,
	DAVersionStatusApplied,
}

func (e DAVersionStatus) IsValid() bool {
	switch e {
	case DAVersionStatusPending, DAVersionStatusInProgress, DAVersionStatusError, DAVersionStatusApplied:
		return true
	}
	return false
}

func (e DAVersionStatus) String() string {
	return string(e)
}

func (e *DAVersionStatus) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = DAVersionStatus(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid DAVersionStatus", str)
	}
	return nil
}

func (e DAVersionStatus) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

func NewDAVersionStatus(status string) *DAVersionStatus {
	var result DAVersionStatus
	switch strings.ToUpper(status) {
	case "PENDING":
		result = DAVersionStatusPending
	case "IN_PROGRESS":
		result = DAVersionStatusInProgress
	case "ERROR":
		result = DAVersionStatusError
	case "APPLIED":
		result = DAVersionStatusApplied
	default:
		return nil
	}
	return &result
}

type GetVersionParams struct {
	VersionID *string
	BranchID  *string
}

func (q *Queries) GetVersion(ctx context.Context, datasetID int, args GetVersionParams) (*Version, error) {

	if args.VersionID == nil {
		log.Printf("ERROR datamodels/versions.go/GetVersion: %s", "VersionID is empty")
		return nil, errors.New("VersionID is empty")
	}
	return q.getVersionByID(ctx, datasetID, args.VersionID)
}

func (q *Queries) GetDAVersion(ctx context.Context, datasetID int, versionID, branchID *string) (*DAVersion, error) {
	db := q.GetDatasetDB(datasetID)

	if versionID == nil {
		q_select := `SELECT v.version_id,
						 v.branch_id,
						 v.user_id,
						 v.applied_timestamp,
						 v.keepalive_timestamp,
						 v.status,
						 v.started_timestamp
				     FROM versions v
				     WHERE 1=1 `
		q_where := ` AND v.applied_timestamp IS NOT NULL`
		q_order := ` ORDER BY v.applied_timestamp DESC LIMIT 1`

		var row *sqlx.Row
		version := &Version{}
		var err error
		if branchID == nil {
			q_where += ` AND v.branch_id IS NULL `
			row = db.QueryRowxContext(ctx, q_select+q_where+q_order)
			version, err = rowToVersion(row, datasetID)
		} else {
			q_whereB := q_where + ` AND v.branch_id = ? `

			row = db.QueryRowxContext(ctx, q_select+q_whereB+q_order, branchID)
			version, err = rowToVersion(row, datasetID)
			if err != nil && err != sql.ErrNoRows {
				log.Printf("ERROR: datamodels/versions.go/GetDAVersion: %s", err)
				return nil, err
			} else if err != nil && err == sql.ErrNoRows {
				q_where += ` AND v.branch_id IS NULL `
				q_select = fmt.Sprintf("%s %s AND v.applied_timestamp < (SELECT branch_from_timestamp FROM branches WHERE branch_id=?) %s", q_select, q_where, q_order)
				row = db.QueryRowxContext(ctx, q_select, branchID)
				version, err = rowToVersion(row, datasetID)
			}
		}

		if err != nil && err == sql.ErrNoRows {
			log.Printf("ERROR: datamodels/versions.go/GetDAVersion: %s", "There is no version that satisfies the required parameters")
			return nil, errors.New("There is no version that satisfies the required parameters")
		}
		if err != nil {
			log.Printf("ERROR: datamodels/versions.go/GetDAVersion: %s", err)
			return nil, err
		}
		return version2DAVersion(version), nil
	}

	version, err := q.getVersionByID(ctx, datasetID, versionID)
	if err != nil && err == sql.ErrNoRows {
		log.Printf("ERROR: datamodels/versions.go/GetDAVersion: %s", "There is no version that satisfies the required parameters")
		return nil, errors.New("There is no version that satisfies the required parameters")
	}
	if err != nil {
		log.Printf("ERROR: datamodels/versions.go/GetDAVersion: %s", err)
		return nil, err
	}
	return version2DAVersion(version), nil
}

type CreateVersionParams struct {
	BranchID       *int32
	UserID         *string
	Started        bool
	VersionUpdates []CreateVersionUpdateParams
}

const insertVersion = `
INSERT INTO versions (
	branch_id,
	user_id,
	status,
	applied_timestamp,
	keepalive_timestamp,
	started_timestamp
) VALUES (
	?, ?, "PENDING", NULL, NULL, NULL
) RETURNING version_id, branch_id, user_id, applied_timestamp, keepalive_timestamp, status, started_timestamp
`

const insertVersionStarted = `
INSERT INTO versions (
	branch_id,
	user_id,
	status,
	applied_timestamp,
	keepalive_timestamp,
	started_timestamp
) VALUES (
	?, ?, "IN_PROGRESS", NULL, NOW(6), NOW(6)
) RETURNING version_id, branch_id, user_id, applied_timestamp, keepalive_timestamp, status, started_timestamp
`

func (q *Queries) CreateVersion(ctx context.Context, datasetID int, args CreateVersionParams) (*Version, error) {
	insertedVersion := Version{}

	// Apply changes
	err := q.RunTransaction(ctx, datasetID, func(tx *sqlx.Tx) error {
		var insertSql string
		if args.Started {
			insertSql = insertVersionStarted
		} else {
			insertSql = insertVersion
		}
		rowv := tx.QueryRowxContext(ctx, insertSql,
			args.BranchID,
			args.UserID,
		)
		if err := rowv.Err(); err != nil {
			return err
		}

		if err := rowv.StructScan(&insertedVersion); err != nil {
			return err
		}

		for _, versionUpdate := range args.VersionUpdates {
			err := q.createVersionUpdate(ctx, tx, insertedVersion.VersionID, versionUpdate)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("ERROR: datamodels/versions.go/CreateVersion: %s", err)
		return nil, err
	}

	return &insertedVersion, nil
}

const getLatestBranchAppliedTimestamp = `
SELECT GREATEST(MAX(v.applied_timestamp), b.branch_from_timestamp) FROM branches b
INNER JOIN versions v ON v.branch_id = b.branch_id
WHERE b.branch_id = ?
`
const getLatestLiveAppliedTimestamp = `
SELECT MAX(v.applied_timestamp)
		FROM versions v
		WHERE v.branch_id IS NULL
`

func (q *Queries) GetLatestAppliedTimestamp(ctx context.Context, datasetID int, branchId *string) (*time.Time, error) {
	db := q.GetDatasetDB(datasetID)

	if branchId == nil {
		// Branch is empty -> live
		row := db.QueryRowxContext(ctx, getLatestLiveAppliedTimestamp)
		err := row.Err()
		if err != nil {
			log.Printf("ERROR datamodels/versions.go/GetLatestAppliedTimestamp: %s", err)
			return nil, err
		}
		var timestamp sql.NullTime
		row.Scan(&timestamp)
		if !timestamp.Valid {
			log.Printf("ERROR datamodels/versions.go/GetLatestAppliedTimestamp: %s", "Timestamp not valid")
			return nil, errors.New("Timestamp not valid")
		}
		return &timestamp.Time, nil
	}
	row := db.QueryRowxContext(ctx, getLatestBranchAppliedTimestamp, branchId)
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/versions.go/GetLatestAppliedTimestamp: %s", err)
		return nil, err
	}
	var timestamp sql.NullTime
	row.Scan(&timestamp)
	if !timestamp.Valid {
		log.Printf("ERROR datamodels/versions.go/GetLatestAppliedTimestamp: %s", "Timestamp not valid")
		return nil, errors.New("Timestamp not valid")
	}
	return &timestamp.Time, nil
}

type LockSettings struct {
	Expiry     time.Duration
	Tries      int
	RetryDelay time.Duration
}

func (q *Queries) GetUpdateLock(ctx context.Context, datasetID int, branchID *int32, lockConf LockSettings) *redsync.Mutex {
	pool := goredis.NewGoredisPool(&(q.rdb))
	rs := redsync.New([]redis.Pool{pool})

	var lock string
	if branchID != nil {
		lock = fmt.Sprintf("dataset/%d/branch/%d/update-lock", datasetID, *branchID)
	} else {
		lock = fmt.Sprintf("dataset/%d/live/update-lock", datasetID)
	}
	mutex := rs.NewMutex(lock, redsync.SetExpiry(lockConf.Expiry), redsync.SetTries(lockConf.Tries),
		redsync.SetRetryDelay(lockConf.RetryDelay))
	return mutex
}

// Private methods ------------------------------------------------------------

func (q *Queries) getVersionByID(ctx context.Context, datasetID int, id *string) (*Version, error) {
	query := `SELECT v.version_id,
					v.branch_id,
					v.user_id,
					v.applied_timestamp,
					v.keepalive_timestamp,
					v.status,
					v.started_timestamp
			  FROM versions v
			  WHERE v.version_id = ?`

	db := q.GetDatasetDB(datasetID)
	row := db.QueryRowxContext(ctx, query, id)

	return rowToVersion(row, datasetID)
}

func rowToVersion(row *sqlx.Row, datasetID int) (*Version, error) {
	version := Version{}
	err := row.Err()
	if err != nil {
		return nil, err
	}
	err = row.StructScan(&version)
	if err != nil {
		return nil, err
	}

	return &version, nil
}

func version2DAVersion(version *Version) *DAVersion {
	daVersion := DAVersion{ID: strconv.FormatInt(version.VersionID, 10)}
	if version.BranchID.Valid {
		str_branchID := strconv.FormatInt(version.BranchID.Int64, 10)
		daVersion.BranchID = &str_branchID
	}
	if version.UserID.Valid {
		daVersion.UserID = &version.UserID.String
	}
	if version.AppliedTimestamp.Valid {
		daVersion.AppliedTimestamp = &version.AppliedTimestamp.Time
	}
	if version.KeepAliveTimestamp.Valid {
		daVersion.KeepAliveTimestamp = &version.KeepAliveTimestamp.Time
	}
	if version.Status.Valid {
		daStatus := NewDAVersionStatus(version.Status.String)
		if daStatus != nil {
			daVersion.Status = *daStatus
		}
	}
	if version.StartedTimestamp.Valid {
		daVersion.StartedTimestamp = &version.StartedTimestamp.Time
	}
	return &daVersion
}

type CreateVersionUpdateParams struct {
	VersionUpdateScope *string
	Operation          VersionUpdateOperation
	Parameters         *string
}

const insertVersionUpdate = `
INSERT INTO version_updates (
	version_id,
	version_update_scope,
	operation,
	parameters
) VALUES (
	?, ?, ?, ?
)
`

func (q *Queries) createVersionUpdate(ctx context.Context, tx *sqlx.Tx, versionID int64, args CreateVersionUpdateParams) error {
	_, err := tx.ExecContext(ctx, insertVersionUpdate,
		versionID,
		args.VersionUpdateScope,
		args.Operation,
		args.Parameters,
	)
	if err != nil {
		log.Printf("ERROR: datamodels/versions.go/CreateVersionUpdate: %s", err)
		return err
	}
	return nil
}

const updateVersionAppliedTimeByID = `
UPDATE versions
SET applied_timestamp = NOW(6),
	status = "APPLIED" 
WHERE version_id = ?
`

func (q *Queries) applyVersion(ctx context.Context, datasetId int, versionID int64) error {
	_, err := q.GetDatasetDB(datasetId).ExecContext(ctx, updateVersionAppliedTimeByID, versionID)
	if err != nil {
		log.Printf("ERROR datamodels/versions.go/applyVersion: %s", err)
		return err
	}
	return nil
}

const updateStartVersionByID = `
UPDATE versions
SET started_timestamp = NOW(6), keepalive_timestamp = NOW(6), status = "IN_PROGRESS"
WHERE version_id = ?
`

func (q *Queries) startVersion(ctx context.Context, datasetId int, versionID int64) error {
	_, err := q.GetDatasetDB(datasetId).ExecContext(ctx, updateStartVersionByID, versionID)
	if err != nil {
		log.Printf("ERROR datamodels/versions.go/startVersion: %s", err)
		return err
	}
	return nil
}

const updateVersionErrorByID = `
UPDATE versions
SET status = "ERROR"
WHERE version_id = ?
`

func (q *Queries) ApplyVersionError(ctx context.Context, datasetId int, versionID int64) error {
	_, err := q.GetDatasetDB(datasetId).ExecContext(ctx, updateVersionErrorByID, versionID)
	if err != nil {
		log.Printf("ERROR datamodels/versions.go/ApplyVersionError: %s", err)
		return err
	}
	return nil
}

const updateVersionKeepAliveByID = `
UPDATE versions
SET keepalive_timestamp = NOW(6)
WHERE version_id = ?
`

func (q *Queries) keepAliveVersion(ctx context.Context, datasetId int, versionID int64) error {
	_, err := q.GetDatasetDB(datasetId).ExecContext(ctx, updateVersionKeepAliveByID, versionID)
	if err != nil {
		log.Printf("ERROR datamodels/versions.go/updateVersionKeepAliveByID: %s", err)
		return err
	}
	return nil
}

func (q *Queries) ProcessErroneousVersions(ctx context.Context, datasets []*Dataset) error {
	for _, dataset := range datasets {
		datasetId, err := strconv.Atoi(dataset.ID)
		if err != nil {
			log.Printf("ERROR datamodels/versions.go/ProcessErroneousVersions: %s", err)
			return err
		}
		erroneousVersions, err := q.getErroneousVersions(ctx, datasetId)
		if err != nil {
			log.Printf("ERROR: datamodels/versions.go/ProcessErroneousVersions: %s", err)
			return err
		}
		if len(erroneousVersions) < 1 {
			log.Printf("INFO: datamodels/versions.go/ProcessErroneousVersions: No versions to process")
			return nil
		}

		unlockLive := false
		redisPool := goredis.NewGoredisPool(&(q.rdb))
		for _, v := range erroneousVersions {
			log.Printf("WARNING: datamodels/versions.go/ProcessErroneousVersions: Marking version=%d as error", v.VersionID)
			if err := q.ApplyVersionError(ctx, datasetId, v.VersionID); err != nil {
				log.Printf("ERROR: datamodels/versions.go/ProcessErroneousVersions: %s [applying version error]", err)
			}

			if !v.BranchID.Valid {
				// This version belong to the live branch
				unlockLive = true
				continue
			}

			log.Printf("INFO: datamodels/versions.go/ProcessErroneousVersions: Releasing mutex for branch=%d", v.BranchID.Int64)
			if _, err := ReleaseMutex(ctx, redisPool, fmt.Sprintf("dataset/%d/branch/%d/update-lock", datasetId, v.BranchID.Int64)); err != nil {
				log.Printf("ERROR: datamodels/versions.go/ProcessErroneousVersions: %s [releasing mutex]", err)
			}
		}

		if unlockLive {
			log.Printf("INFO: datamodels/versions.go/ProcessErroneousVersions: Releasing mutex for branch=live")
			if _, err := ReleaseMutex(ctx, redisPool, fmt.Sprintf("dataset/%d/live/update-lock", datasetId)); err != nil {
				log.Printf("ERROR: datamodels/versions.go/ProcessErroneousVersions: [releasing mutex]=%s", err)
			}
		}
	}

	return nil
}

const getErroneousVersionsSql = `
SELECT version_id,
		branch_id,
		status
FROM versions
WHERE status = "IN_PROGRESS" 
AND   keepalive_timestamp < DATE_SUB(NOW(), INTERVAL 1 HOUR)
`

func (q *Queries) getErroneousVersions(ctx context.Context, datasetId int) ([]*Version, error) {
	rows, err := q.GetDatasetDB(datasetId).QueryxContext(ctx, getErroneousVersionsSql)
	if err != nil {
		log.Printf("ERROR: datamodels/versions.go/getErroneousVersions: %s", err)
		return nil, err
	}

	var result []*Version
	for rows.Next() {
		var version Version
		err := rows.StructScan(&version)
		if err != nil {
			log.Printf("ERROR: datamodels/cache_layers.go/getErroneousVersions: %s", err)
			return nil, err
		}
		result = append(result, &version)

	}
	return result, nil
}
