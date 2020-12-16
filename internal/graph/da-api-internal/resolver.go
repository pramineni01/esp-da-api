package graph

import (
	//"bitbucket.org/antuitinc/esp-da-api/dataloaders"
	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
)

// Resolver allows for dependency injection (into generated resolver methods)
type Resolver struct {
	DBRepo datamodels.DBRepo
	//	Dataloaders dataloaders.Retriever
}
