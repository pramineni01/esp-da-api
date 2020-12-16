package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"fmt"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api-internal/generated"
)

func (r *dAPageInfoResolver) TotalRows(ctx context.Context, obj *datamodels.DAPageInfo) (*int, error) {
	panic(fmt.Errorf("not implemented"))
}

// DAPageInfo returns generated.DAPageInfoResolver implementation.
func (r *Resolver) DAPageInfo() generated.DAPageInfoResolver { return &dAPageInfoResolver{r} }

type dAPageInfoResolver struct{ *Resolver }
