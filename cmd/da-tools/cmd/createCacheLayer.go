package cmd

import (
	"context"
	"log"

	"github.com/spf13/cobra"

	"bitbucket.org/antuitinc/esp-da-api/internal/dataaccess"
	"bitbucket.org/antuitinc/esp-da-api/internal/datamodels"
)

// createCacheLayerCmd represents the createCacheLayer command
var createCacheLayerCmd = &cobra.Command{
	Use:   "createCacheLayer",
	Short: "Creates a cache layer",
	Long:  `Creates a cache layer`,

	Run: func(cmd *cobra.Command, args []string) {
		createCacheLayer(cmd, args)
	},
}

var datasetId, datatableId int
var dimLevelIds []int

func init() {
	rootCmd.AddCommand(createCacheLayerCmd)

	createCacheLayerCmd.Flags().IntVar(&datasetId, "datasetId", -1, "--datasetId XXX")
	createCacheLayerCmd.MarkFlagRequired("datasetId")

	createCacheLayerCmd.Flags().IntVar(&datatableId, "datatableId", -1, "--datatableId XXX")
	createCacheLayerCmd.MarkFlagRequired("datatableId")

	createCacheLayerCmd.Flags().IntSliceVar(&dimLevelIds, "dimLevelIds", []int{}, "--dimLevelIds XXX,XXX,XXX")
	createCacheLayerCmd.MarkFlagRequired("dimLevelIds")
	// createCacheLayerCmd.Flags().Parse()
}

func createCacheLayer(cmd *cobra.Command, args []string) {

	// get commandline inputs
	dsId, _ := cmd.Flags().GetInt("datasetId")
	dtId, _ := cmd.Flags().GetInt("datatableId")
	dimLvlIds, err := cmd.Flags().GetIntSlice("dimLevelIds")
	if err != nil {
		panic(err)
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

	// Update dbRepo
	dbRepo.NewQueries(map_dbs, datasets_def, nil)

	// add new cache layer
	if err := dbRepo.AddCacheLayerForDTAndDimLvls(context.Background(), dsId, dtId, dimLvlIds); err != nil {
		log.Println("Error while creating cache layer: ", err)
	}

	// for now not needed
	// Rebuild the cache layers
	// ctx, cancel := context.WithTimeout(context.Background(), time.Duration(6)*time.Hour)
	// defer cancel()
	// dbRepo.RebuildCacheLayers(ctx, dsId)
}
