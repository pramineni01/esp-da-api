package main

import (
	"context"
	"log"
	"time"

	"bitbucket.org/antuitinc/esp-da-api/internal/dataaccess"
	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
)

func main() {
	// Setup DA connection
	conf := dataaccess.ReadConfig()
	_, db_da := dataaccess.InitDA(conf)
	defer db_da.Close()
	// Get all datasets from DA
	dbRepo := datamodels.NewRepo(db_da)
	datasets, dataset_map, err := dbRepo.QueriesDA.GetDatasets(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	datasets_def := dbRepo.QueriesDA.LoadDatasets(datasets)
	// Setup all Datasets DB connections
	map_dbs := dataaccess.InitDatasets(conf, dataset_map)
	for _, db := range map_dbs {
		defer db.Close()
	}

	// Setup Redis connection
	rdb := dataaccess.InitRedis()

	// Get dbRepo
	dbRepo.NewQueries(map_dbs, datasets_def, rdb)

	// Process the erroneous versions
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(6)*time.Hour)
	defer cancel()
	log.Printf("ProcessErroneousVersions running...")
	dbRepo.ProcessErroneousVersions(ctx, datasets)
}
