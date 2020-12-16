package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api-internal/generated"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

func (r *dADatasetResolver) DataVersion(ctx context.Context, obj *datamodels.DADataset, version *string, branchID *string) (*datamodels.DAVersion, error) {
	datasetID, _ := strconv.Atoi(obj.ID)
	daVersion, err := r.DBRepo.GetDAVersion(ctx, datasetID, version, branchID)
	if err != nil {
		return nil, err
	}

	return daVersion, nil
}

func (r *dADatasetResolver) Datatable(ctx context.Context, obj *datamodels.DADataset, id *string, tableName *string, measureColumnName *string) (*datamodels.DADatatable, error) {
	datasetID, _ := strconv.Atoi(obj.ID)
	if (id == nil || *id == "") && (measureColumnName == nil || *measureColumnName == "") && (tableName == nil || *tableName == "") {
		return nil, gqlerror.Errorf("DADatatable: ID, tableName or measureColumnName must be provided")
	}
	if id != nil {
		if _, err := strconv.Atoi(*id); err != nil {
			return nil, gqlerror.Errorf("DADatatableID must be a number")
		}
	}

	datatable, err := r.DBRepo.GetDADatatable(ctx, datasetID, id, measureColumnName, tableName)
	if err != nil {
		return nil, err
	}
	return datatable, nil
}

func (r *dADatasetResolver) DimensionLevels(ctx context.Context, obj *datamodels.DADataset, dimensionID *string, dimensionColumnName *string) ([]*datamodels.DADimensionLevel, error) {
	datasetID, _ := strconv.Atoi(obj.ID)
	dimensionLevels, err := r.DBRepo.GetDADimensionLevels(ctx, datasetID, dimensionID, dimensionColumnName, nil, false)
	if err != nil {
		return nil, err
	}

	return dimensionLevels, nil
}

func (r *dADatasetResolver) Measures(ctx context.Context, obj *datamodels.DADataset, aggregated *bool) ([]datamodels.DADatasetMeasure, error) {
	datasetID, _ := strconv.Atoi(obj.ID)
	measures, err := r.DBRepo.GetDAMeasures(ctx, datasetID, aggregated)
	if err != nil {
		return nil, err
	}

	return measures, nil
}

func (r *dADatatableResolver) DimensionLevels(ctx context.Context, obj *datamodels.DADatatable, dimensionID *string, dimensionColumnName *string) ([]*datamodels.DADimensionLevel, error) {
	datasetID, _ := strconv.Atoi(obj.DatasetID)
	dimensionLevels, err := r.DBRepo.GetDADimensionLevels(ctx, datasetID, dimensionID, dimensionColumnName, &obj.Id, false)
	if err != nil {
		return nil, err
	}

	return dimensionLevels, nil
}

func (r *dADatatableResolver) Measures(ctx context.Context, obj *datamodels.DADatatable) ([]*datamodels.DAMeasure, error) {
	datasetID, _ := strconv.Atoi(obj.DatasetID)
	measures, err := r.DBRepo.GetDADatatableMeasures(ctx, datasetID, &obj.Id)
	if err != nil {
		return nil, err
	}

	return measures, nil
}

func (r *dADatatableResolver) DataView(ctx context.Context, obj *datamodels.DADatatable, branchID *string, version *string, scope *datamodels.DAQueryScopeInput, dimensionLevels []string, aggregatedMeasures []string, userID *string, allData bool, localeID *string, dimMemberAttributes []datamodels.DADimensionMemberAttribute, aliases []*datamodels.DADataViewAlias, partitioned bool) (*datamodels.DASQLQuery, error) {
	datasetID, _ := strconv.Atoi(obj.DatasetID)
	querySQL, err := r.DBRepo.CreateView(ctx, datasetID, datamodels.CreateDataviewParams{
		Datatable:           obj,
		DimensionLevels:     dimensionLevels,
		AggregatedMeasures:  aggregatedMeasures,
		Version:             version,
		BranchID:            branchID,
		UserID:              userID,
		Scope:               scope,
		AllData:             allData,
		LocaleID:            localeID,
		DimMemberAttributes: dimMemberAttributes,
		Aliases:             aliases,
		Partitioned:         partitioned,
	})
	if err != nil {
		log.Printf("ERROR schema.resolvers.go/DataView: %s", err)
		return nil, err
	}

	return querySQL, nil
}

func (r *dADatatableResolver) DataframeQueries(ctx context.Context, obj *datamodels.DADatatable, branchID *string, version *string, scope *datamodels.DAQueryScopeInput, dimensionLevels []string, aggregatedMeasures []string, userID *string, allData bool, localeID *string, dimMemberAttributes []datamodels.DADimensionMemberAttribute, aliases []*datamodels.DADataViewAlias) (*datamodels.DADataframeQueries, error) {
	dataframeQueries, err := r.DBRepo.GetDataframeQueries(ctx, datamodels.GetDataframeQueriesParams{
		Datatable:           obj,
		DimensionLevels:     dimensionLevels,
		AggregatedMeasures:  aggregatedMeasures,
		Version:             version,
		BranchID:            branchID,
		UserID:              userID,
		Scope:               scope,
		AllData:             allData,
		LocaleID:            localeID,
		DimMemberAttributes: dimMemberAttributes,
		Aliases:             aliases,
	})
	if err != nil {
		log.Printf("ERROR schema.resolvers.go/DataframeQueries: %s", err)
		return nil, err
	}

	return dataframeQueries, nil
}

func (r *dADatatableResolver) StagingTable(ctx context.Context, obj *datamodels.DADatatable, branchID *string, scope *datamodels.DAQueryScopeInput, measures []string, userID *string, allData bool, numPartitions *int) (*datamodels.DAStagingTable, error) {
	datasetID, _ := strconv.Atoi(obj.DatasetID)
	daStagingTable, err := r.DBRepo.StagingTable(ctx, datasetID, obj, branchID, scope, measures, userID, allData, *numPartitions)
	if err != nil {
		return nil, err
	}

	return daStagingTable, nil
}

func (r *mutationResolver) DaIngestStagingTables(ctx context.Context, datasetID string, stagingTableIDs []string) (*string, error) {
	// Validations
	dataset_id, err := strconv.Atoi(datasetID)
	if err != nil {
		log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables: %s", err)
		return nil, err
	}
	ingestIds := []int64{}
	for _, id := range stagingTableIDs {
		numId, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables: %s", err)
			return nil, err
		}
		ingestIds = append(ingestIds, numId)
	}

	// Get the ingests map and define branch
	var branchID int32 = -1
	ingestsMap, err := r.DBRepo.GetIngestsbyID(ctx, dataset_id, ingestIds)
	if err != nil {
		return nil, err
	}
	if len(ingestsMap) == 0 {
		err = errors.New("Invalid stagingTableIDs")
		log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables: %s [Get ingests map]", err)
		return nil, err
	}
	var userID *string
	for _, ingest := range ingestsMap {
		if !ingest.BranchID.Valid {
			if branchID == -1 {
				branchID = 0
			} else if branchID != 0 {
				err = errors.New(fmt.Sprintf("Mixing ingest tables from different branches is not allowed"))
			}
		} else {
			if branchID == -1 {
				branchID = ingest.BranchID.Int32
			} else if branchID != ingest.BranchID.Int32 {
				err = errors.New(fmt.Sprintf("Mixing ingest tables from different branches is not allowed"))
			}
		}
		if ingest.IngestMerged {
			err = errors.New(fmt.Sprintf("The ingest=%d has already been merged", ingest.IngestID))
		} else if ingest.IngestDeleted {
			err = errors.New(fmt.Sprintf("The ingest=%d has already been deleted", ingest.IngestID))
		}
		if err != nil {
			log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables: %s [Get ingests map]", err)
			return nil, err
		}

		// Get user from ingest table
		if ingest.UserID.Valid {
			if userID == nil {
				userID = &ingest.UserID.String
			} else if *userID != ingest.UserID.String {
				log.Printf("ERROR: schema.resolvers.go/DaIngestStagingTables: Distinct users not allowed")
				return nil, errors.New("Distinct users not allowed")
			}
		}
	}

	// Create a version
	newVersionParams := datamodels.CreateVersionParams{
		BranchID: nil,
		UserID:   userID,
		Started:  false,
		VersionUpdates: []datamodels.CreateVersionUpdateParams{
			datamodels.CreateVersionUpdateParams{
				Operation: datamodels.VersionUpdateOpBATCHLOAD,
			},
		},
	}
	if branchID != 0 {
		newVersionParams.BranchID = &branchID
	}
	version, err := r.DBRepo.CreateVersion(ctx, dataset_id, newVersionParams)
	if err != nil {
		return nil, err
	}
	log.Printf("The version=%d has been create", version.VersionID)

	// Process the changes in background
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(6)*time.Hour)
		defer cancel()

		var b *int32 = nil
		if branchID != 0 {
			b = &branchID
		}
		err := r.DBRepo.ProcessIngestTables(ctx, dataset_id, b, userID, ingestsMap, version)
		if err != nil {
			log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables/ProcessIngestTables: %s", err)
			err = r.DBRepo.ApplyVersionError(ctx, dataset_id, version.VersionID)
			if err != nil {
				log.Printf("ERROR schema.resolvers.go/DaIngestStagingTables/ProcessIngestTables: ApplyVersionError=%s", err)
				return
			}
			log.Printf("The version=%d has been marked in error", version.VersionID)
		}
	}()

	// Return the version
	vID := strconv.FormatInt(version.VersionID, 10)
	return &vID, nil
}

func (r *queryResolver) DaDataset(ctx context.Context, id string) (*datamodels.DADataset, error) {
	datasetID, _ := strconv.Atoi(id)
	return r.DBRepo.GetDADataset(ctx, datasetID)
}

// DADataset returns generated.DADatasetResolver implementation.
func (r *Resolver) DADataset() generated.DADatasetResolver { return &dADatasetResolver{r} }

// DADatatable returns generated.DADatatableResolver implementation.
func (r *Resolver) DADatatable() generated.DADatatableResolver { return &dADatatableResolver{r} }

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type dADatasetResolver struct{ *Resolver }
type dADatatableResolver struct{ *Resolver }
type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
