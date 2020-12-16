package dataaccess

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
)

type DSs struct {
	DA          DAItem   `yaml:"da"`
	Datasources []DSItem `yaml:",flow"`
}

type DAItem struct {
	Env      string `yaml:"env"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Hostname string `yaml:"hostname"`
	Port     string `yaml:"port"`
	DBName   string `yaml:"db_name"`
	Type     string `yaml:"type"`
}

type DSItem struct {
	DatasourceID int    `yaml:"datasource_id"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Hostname     string `yaml:"hostname"`
	Port         string `yaml:"port"`
	Type         string `yaml:"type"`
}

const (
	CONFIG_NET  = "tcp"
	DATASET_NOM = "dadataset"
)

func ReadConfig() *DSs {
	data, err := ioutil.ReadFile("./config/datasources.yml")
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	dbs := DSs{}
	err = yaml.Unmarshal(data, &dbs)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	return &dbs
}

func InitDA(conf *DSs) (*mysql.Config, *sqlx.DB) {
	// Resolve DA DB name
	dbName := ""
	if len(conf.DA.Env) != 0 {
		dbName = conf.DA.Env + "_"
	}
	dbName += conf.DA.DBName

	db_da_config := mysql.NewConfig()
	db_da_config.User = conf.DA.User
	db_da_config.Passwd = conf.DA.Password
	db_da_config.DBName = dbName
	db_da_config.Addr = conf.DA.Hostname + ":" + conf.DA.Port
	db_da_config.Net = CONFIG_NET
	db_da_config.ParseTime = true

	db_da, err := sqlx.Open(conf.DA.Type, db_da_config.FormatDSN())
	if err != nil {
		log.Fatalln(err)
	}

	return db_da_config, db_da
}

func GetDBConfig(conf *DSs, datasets map[string]string) (map[int]*mysql.Config, map[int]string) {
	map_config := make(map[int]*mysql.Config)
	map_types := make(map[int]string)

	for datasetID, datasourceID := range datasets {
		datasource_id, _ := strconv.Atoi(datasourceID)
		dataset_id, _ := strconv.Atoi(datasetID)

		// Create connection
		user, pass, addr, type_db, err := GetDSConf(conf, datasource_id)
		if err != nil {
			log.Fatalln(err)
		}
		// Resolve dataset DB name
		dbName := ""
		if len(conf.DA.Env) != 0 {
			dbName = conf.DA.Env + "_"
		}
		dbName += fmt.Sprintf("%s%d", DATASET_NOM, dataset_id)

		db_config := mysql.NewConfig()
		db_config.User = user
		db_config.Passwd = pass
		db_config.DBName = dbName
		db_config.Addr = addr
		db_config.Net = CONFIG_NET
		db_config.ParseTime = true
		map_config[dataset_id] = db_config
		map_types[dataset_id] = type_db
	}

	return map_config, map_types
}

func GetDSConf(conf *DSs, datasource_id int) (string, string, string, string, error) {
	for _, db := range conf.Datasources {
		if db.DatasourceID == datasource_id {
			return db.User, db.Password, db.Hostname + ":" + db.Port, db.Type, nil
		}
	}
	return "", "", "", "", errors.New("No configuration found for datasource")
}

func InitDatasets(conf *DSs, datasets map[string]string) map[int]*sqlx.DB {
	map_config, map_types := GetDBConfig(conf, datasets)
	map_dbs := make(map[int]*sqlx.DB)
	for dataset_id, db_config := range map_config {
		db, err := sqlx.Open(map_types[dataset_id], db_config.FormatDSN())
		if err != nil {
			log.Fatalln(err)
		}
		map_dbs[dataset_id] = db
	}
	return map_dbs
}
