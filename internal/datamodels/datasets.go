package datamodels

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

type DADataset struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	Version      string `json:"version"`
	DatasourceID string `json:"datasourceID"`
}

func (DADataset) IsEntity() {}

type Dataset struct {
	ID           string         `db:"dataset_id"`
	DatasourceID string         `db:"datasource_id"`
	Name         sql.NullString `db:"dataset_name"`
	Description  sql.NullString `db:"dataset_description"`
	Version      sql.NullString `db:"dataset_version"`
}

var all_datasets = `
SELECT dataset_id, datasource_id, dataset_name, dataset_description, dataset_version
FROM datasets`

func (q_df *QueriesDA) GetDatasets(ctx context.Context) ([]*Dataset, map[string]string, error) {
	rows, err := q_df.db.QueryxContext(ctx, all_datasets)
	if err != nil {
		return nil, nil, err
	}

	dataset_map := make(map[string]string)
	var datasets []*Dataset
	for rows.Next() {
		var dataset Dataset
		err := rows.StructScan(&dataset)
		if err != nil {
			return datasets, dataset_map, err
		}
		datasets = append(datasets, &dataset)
		dataset_map[dataset.ID] = dataset.DatasourceID
	}

	return datasets, dataset_map, nil
}

func (q_df *QueriesDA) LoadDatasets(datasets []*Dataset) map[int]*DADataset {
	map_datasets := make(map[int]*DADataset)
	for _, dataset := range datasets {

		// Create dataset
		datasetID, _ := strconv.Atoi(dataset.ID)
		da := &DADataset{
			ID:           dataset.ID,
			Name:         dataset.Name.String,
			Description:  dataset.Description.String,
			Version:      dataset.Version.String,
			DatasourceID: dataset.DatasourceID,
		}
		map_datasets[datasetID] = da
	}
	return map_datasets
}

func (q *Queries) GetDADataset(ctx context.Context, id int) (*DADataset, error) {
	ds, ok := q.datasets[id]
	if ok {
		return ds, nil
	}
	return nil, errors.New("No found dataset")
}

// Entity Resolver

type FindDADatasetByIDParams struct {
	DatasetID int
}

func (q *Queries) FindDADatasetByID(ctx context.Context, args FindDADatasetByIDParams) (*DADataset, error) {
	return q.GetDADataset(ctx, args.DatasetID)
}
