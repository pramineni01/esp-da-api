package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"errors"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api/generated"
)

func (r *dAPageInfoResolver) TotalRows(ctx context.Context, obj *datamodels.DAPageInfo) (*int, error) {
	if obj == nil {
		return nil, errors.New("ERROR: common.resolvers.go/TotalRows: DAPageInfo is empty")
	}
	return r.DBRepo.GetTotalRows(ctx, obj)
}

// DAPageInfo returns generated.DAPageInfoResolver implementation.
func (r *Resolver) DAPageInfo() generated.DAPageInfoResolver { return &dAPageInfoResolver{r} }

type dAPageInfoResolver struct{ *Resolver }
