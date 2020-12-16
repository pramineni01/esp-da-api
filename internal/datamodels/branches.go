package datamodels

import (
	"context"
	"database/sql"
	"log"
	"strconv"

	"github.com/jmoiron/sqlx"
)

type DABranch struct {
	DatasetID     string
	ID            string       `db:"branch_id"`
	FromTimestamp sql.NullTime `db:"branch_from_timestamp"`

	// internal usage
	id int32
}

func (DABranch) IsEntity() {}

type FindDABranchByDatasetIDAndIDParams struct {
	DatasetID int
	BranchID  *string
}

func (q *Queries) FindDABranchByDatasetIDAndID(ctx context.Context, args FindDABranchByDatasetIDAndIDParams) (*DABranch, error) {
	if args.BranchID == nil {
		return nil, nil
	}
	return q.getDABranchByID(ctx, args.DatasetID, *args.BranchID)
}

// Private methods ------------------------------------------------------------

func (q *Queries) createBranch(ctx context.Context, datasetID int) (*DABranch, error) {
	db := q.GetDatasetDB(datasetID)

	query := `INSERT INTO branches (branch_from_timestamp)
			  VALUES (NOW(6))
			  RETURNING branch_id, branch_from_timestamp`

	row := db.QueryRowxContext(ctx, query)
	return rowToBranch(row, datasetID)
}

func (q *Queries) getDABranchByID(ctx context.Context, datasetID int, id interface{}) (*DABranch, error) {
	db := q.GetDatasetDB(datasetID)

	query := `SELECT b.branch_id,
                     b.branch_from_timestamp
				FROM branches b
				WHERE b.branch_id = ?`

	row := db.QueryRowxContext(ctx, query, id)
	return rowToBranch(row, datasetID)
}

func rowToBranch(row *sqlx.Row, datasetID int) (*DABranch, error) {
	err := row.Err()
	if err != nil {
		log.Printf("ERROR datamodels/branches.go/rowToBranch: %s", err)
		return nil, err
	}

	branch := DABranch{}
	err = row.StructScan(&branch)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Printf("ERROR datamodels/branches.go/rowToBranch: %s", err)
		}
		return nil, err
	}

	branch.DatasetID = strconv.Itoa(datasetID)
	id, _ := strconv.Atoi(branch.ID)
	branch.id = int32(id)
	return &branch, nil
}

func (q *Queries) addBranchDatatable(ctx context.Context, datasetId int, branchId int32, datatableId, measureTypeId int) error {
	db := q.GetDatasetDB(datasetId)

	query := `INSERT INTO branch_datatable
			  SET branch_id       = ?,
			      datatable_id    = ?,
				  measure_type_id = ?
			  ON DUPLICATE KEY UPDATE datatable_id=datatable_id`

	_, err := db.ExecContext(ctx, query, branchId, datatableId, measureTypeId)
	if err != nil {
		log.Printf("ERROR datamodels/branches.go/addBranchDatatable: %s", err)
		return err
	}
	return nil
}

func (q *Queries) addBranchDataExtension(ctx context.Context, datasetId int, branchId int32, dataExtensionId int) error {
	db := q.GetDatasetDB(datasetId)

	query := `INSERT INTO branch_dataextension
			  SET dataextension_id = ?,
			 	  branch_id = ?
			  ON DUPLICATE KEY UPDATE dataextension_id=dataextension_id`

	_, err := db.ExecContext(ctx, query, dataExtensionId, branchId)
	if err != nil {
		log.Printf("ERROR datamodels/branches.go/addBranchDataExtension: %s", err)
		return err
	}
	return nil
}
