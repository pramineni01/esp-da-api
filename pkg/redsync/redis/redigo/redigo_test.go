package redigo

import "bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"

var _ (redis.Conn) = (*RedigoConn)(nil)

var _ (redis.Pool) = (*RedigoPool)(nil)
