package main

import (
	"time"

	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync"
	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"
	"bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis/redigo"
	redigolib "github.com/gomodule/redigo/redis"
	"github.com/stvp/tempredis"
)

func main() {

	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	defer server.Term()

	pool := redigo.NewRedigoPool(&redigolib.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redigolib.Conn, error) {
			return redigolib.Dial("unix", server.Socket())
		},
		TestOnBorrow: func(c redigolib.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	})

	rs := redsync.New([]redis.Pool{pool})

	mutex := rs.NewMutex("test-redsync")
	err = mutex.Lock()

	if err != nil {
		panic(err)
	}

	mutex.Unlock()
}
