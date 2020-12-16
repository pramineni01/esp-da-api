package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"strconv"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api-internal/generated"
)

func (r *entityResolver) FindDAAggregatedMeasureByDatasetIDAndColumnName(ctx context.Context, datasetID string, columnName string) (*datamodels.DAAggregatedMeasure, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDAAggregatedMeasureByDatasetIDAndColumnName(ctx, datamodels.FindDAAggregatedMeasureByDatasetIDAndColumnNameParams{
		DatasetID:                   dataset_id,
		AggregatedMeasureColumnName: &columnName,
	})
}

func (r *entityResolver) FindDABranchByDatasetIDAndID(ctx context.Context, datasetID string, id string) (*datamodels.DABranch, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDABranchByDatasetIDAndID(ctx, datamodels.FindDABranchByDatasetIDAndIDParams{
		DatasetID: dataset_id,
		BranchID:  &id,
	})
}

func (r *entityResolver) FindDADatasetByID(ctx context.Context, id string) (*datamodels.DADataset, error) {
	dataset_id, _ := strconv.Atoi(id)
	return r.DBRepo.FindDADatasetByID(ctx, datamodels.FindDADatasetByIDParams{
		DatasetID: dataset_id,
	})
}

func (r *entityResolver) FindDADatatableByDatasetIDAndID(ctx context.Context, datasetID string, id string) (*datamodels.DADatatable, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDADatatableByDatasetIDAndID(ctx, datamodels.FindDADatatableByDatasetIDAndIDParams{
		DatasetID:   dataset_id,
		DatatableID: &id,
	})
}

func (r *entityResolver) FindDADimensionByDatasetIDAndColumnName(ctx context.Context, datasetID string, columnName string) (*datamodels.DADimension, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDADimensionByDatasetIDAndColumnName(ctx, datamodels.FindDADimensionByDatasetIDAndColumnNameParams{
		DatasetID:           dataset_id,
		DimensionColumnName: &columnName,
	})
}

func (r *entityResolver) FindDADimensionLevelByDatasetIDAndColumnName(ctx context.Context, datasetID string, columnName string) (*datamodels.DADimensionLevel, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDADimensionLevelByDatasetIDAndColumnName(ctx, datamodels.FindDADimensionLevelByDatasetIDAndColumnNameParams{
		DatasetID:                dataset_id,
		DimensionLevelColumnName: &columnName,
	})
}

func (r *entityResolver) FindDADimensionMemberByDatasetIDAndID(ctx context.Context, datasetID string, id string) (*datamodels.DADimensionMember, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDADimensionMemberByDatasetIDAndID(ctx, datamodels.FindDADimensionMemberByDatasetIDAndIDParams{
		DatasetID:         dataset_id,
		DimensionMemberID: &id,
	})
}

func (r *entityResolver) FindDAMeasureByDatasetIDAndColumnName(ctx context.Context, datasetID string, columnName string) (*datamodels.DAMeasure, error) {
	dataset_id, _ := strconv.Atoi(datasetID)
	return r.DBRepo.FindDAMeasureByDatasetIDAndColumnName(ctx, datamodels.FindDAMeasureByDatasetIDAndColumnNameParams{
		DatasetID:         dataset_id,
		MeasureColumnName: &columnName,
	})
}

// Entity returns generated.EntityResolver implementation.
func (r *Resolver) Entity() generated.EntityResolver { return &entityResolver{r} }

type entityResolver struct{ *Resolver }
