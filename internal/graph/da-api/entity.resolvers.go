package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"fmt"

	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api/generated"
)

func (r *entityResolver) FindDADimensionMemberByDatasetIDAndID(ctx context.Context, datasetID string, id string) (*datamodels.DADimensionMember, error) {
	panic(fmt.Errorf("not implemented"))
}

// Entity returns generated.EntityResolver implementation.
func (r *Resolver) Entity() generated.EntityResolver { return &entityResolver{r} }

type entityResolver struct{ *Resolver }
