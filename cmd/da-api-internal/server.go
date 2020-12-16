package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"bitbucket.org/antuitinc/esp-da-api/internal/dataaccess"
	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
	graph "bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api-internal"
	"bitbucket.org/antuitinc/esp-da-api/internal/graph/da-api-internal/generated"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
)

const defaultPort = "8080"

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

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

	// Update dbRepo
	dbRepo.NewQueries(map_dbs, datasets_def, rdb)

	//dataloaders := dataloaders.NewRetriever()
	srv := handler.NewDefaultServer(generated.NewExecutableSchema(generated.Config{Resolvers: &graph.Resolver{
		DBRepo: dbRepo,
		//Dataloaders: dataloaders,
	}}))

	http.Handle("/", playground.Handler("GraphQL playground", "/query"))
	http.Handle("/query", srv)

	log.Printf("connect to http://localhost:%s/ for GraphQL playground", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
