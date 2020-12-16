package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"sync"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api/generated"
)

func (r *mutationResolver) DaUpdateData(ctx context.Context, updates datamodels.UpdateDataInput) (string, error) {
	return r.DBRepo.ApplyDataUpdates(ctx, updates)
}

func (r *queryResolver) DaDataQuery(ctx context.Context, queries []*datamodels.DAQueryInput) ([]*datamodels.DAQueryResultConnection, error) {
	// Run all queries in parallel
	var wg sync.WaitGroup
	results := make([]*datamodels.DAQueryResultConnection, len(queries))
	for queryPos, query := range queries {
		wg.Add(1)
		go func(queryPos int, query *datamodels.DAQueryInput) {
			defer wg.Done()

			queryConn, err := r.DBRepo.PeformDataQuery(ctx, query)
			if err != nil {
				queryConn = &datamodels.DAQueryResultConnection{
					Edges: []*datamodels.DAQueryResultEdge{},
					PageInfo: &datamodels.DAPageInfo{
						HasNextPage: false,
					},
					Error: err.Error(),
				}
			}
			results[queryPos] = queryConn
		}(queryPos, query)
	}

	// Wait until all queries have finished
	wg.Wait()

	// Return results
	return results, nil
}

func (r *queryResolver) DaDimMembersQuery(ctx context.Context, queries []*datamodels.DADimMembersQueryInput) ([]*datamodels.DAQueryResultConnection, error) {
	// Run all queries in parallel
	var wg sync.WaitGroup
	results := make([]*datamodels.DAQueryResultConnection, len(queries))
	for queryPos, query := range queries {
		wg.Add(1)
		go func(queryPos int, query *datamodels.DADimMembersQueryInput) {
			defer wg.Done()

			queryConn, err := r.DBRepo.PeformDataSearchQuery(ctx, query)
			if err != nil {
				queryConn = &datamodels.DAQueryResultConnection{
					Edges: []*datamodels.DAQueryResultEdge{},
					PageInfo: &datamodels.DAPageInfo{
						HasNextPage: false,
					},
					Error: err.Error(),
				}
			}
			results[queryPos] = queryConn
		}(queryPos, query)
	}

	// Wait until all queries have finished
	wg.Wait()

	// Return results
	return results, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type mutationResolver struct{ *Resolver }
type queryResolver struct{ *Resolver }
