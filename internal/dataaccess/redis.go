package dataaccess

import (
	"fmt"
	"io/ioutil"
	"log"

	"github.com/go-redis/redis/v8"
	"gopkg.in/yaml.v2"
)

type RedisConf struct {
	Redis RedisItem `yaml:"redis"`
}

type RedisItem struct {
	Hostname string `yaml:"hostname"`
	Port     int    `yaml:"port"`
	Type     string `yaml:"type"`
	Password string `yaml:"password"`
}

type RedisDeployment int

const (
	RedisServer RedisDeployment = iota + 1
	RedisCluster
)

const (
	DEFAULT_REDIS_HOST       = "127.0.0.1"
	DEFAULT_REDIS_PORT       = 6379
	DEFAULT_REDIS_DEPLOYMENT = RedisServer
)

const (
	LockTimeout = 60000 // msec
	LockLimit   = 20000 // msec
)

func REDIS_DEPLOYMENT(type_redis string) RedisDeployment {
	if type_redis == "server" {
		return RedisServer
	} else if type_redis == "cluster" {
		return RedisCluster
	}
	return DEFAULT_REDIS_DEPLOYMENT
}

func InitRedis() *redis.UniversalClient {
	data, err := ioutil.ReadFile("./config/redis.yml")
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	redis_conf := RedisConf{}
	err = yaml.Unmarshal(data, &redis_conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	var rdb redis.UniversalClient
	if REDIS_DEPLOYMENT(redis_conf.Redis.Type) == RedisCluster {
		// We are connecting a cluster
		options := redis.ClusterOptions{
			Addrs: []string{
				fmt.Sprintf("%s:%d", redis_conf.Redis.Hostname, redis_conf.Redis.Port),
			},
		}
		if redis_conf.Redis.Password != "" {
			options.Password = redis_conf.Redis.Password
		}
		rdb = redis.NewClusterClient(&options)
	} else {
		// We are connecting a server
		options := redis.Options{
			Addr: fmt.Sprintf("%s:%d", redis_conf.Redis.Hostname, redis_conf.Redis.Port),
		}
		if redis_conf.Redis.Password != "" {
			options.Password = redis_conf.Redis.Password
		}
		rdb = redis.NewClient(&options)
	}
	return &rdb
}
