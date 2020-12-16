package goredis

import "bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"

var _ (redis.Conn) = (*GoredisConn)(nil)

var _ (redis.Pool) = (*GoredisPool)(nil)
