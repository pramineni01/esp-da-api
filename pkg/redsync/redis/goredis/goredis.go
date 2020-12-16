package goredis

import (
	"context"
	"strings"
	"time"

	redsyncredis "bitbucket.org/antuitinc/esp-da-api/pkg/redsync/redis"
	"github.com/go-redis/redis/v8"
)

type GoredisPool struct {
	delegate *redis.UniversalClient
}

func (self *GoredisPool) Get() redsyncredis.Conn {
	return &GoredisConn{self.delegate}
}

func NewGoredisPool(delegate *redis.UniversalClient) *GoredisPool {
	return &GoredisPool{delegate}
}

type GoredisConn struct {
	delegate *redis.UniversalClient
}

func (self *GoredisConn) Get(ctx context.Context, name string) (string, error) {
	value, err := (*self.delegate).Get(ctx, name).Result()
	err = noErrNil(err)
	return value, err
}

func (self *GoredisConn) Set(ctx context.Context, name string, value string) (bool, error) {
	reply, err := (*self.delegate).Set(ctx, name, value, 0).Result()
	return err == nil && reply == "OK", nil
}

func (self *GoredisConn) SetNX(ctx context.Context, name string, value string, expiry time.Duration) (bool, error) {
	return (*self.delegate).SetNX(ctx, name, value, expiry).Result()
}

func (self *GoredisConn) PTTL(ctx context.Context, name string) (time.Duration, error) {
	return (*self.delegate).PTTL(ctx, name).Result()
}

func (self *GoredisConn) Eval(ctx context.Context, script *redsyncredis.Script, keysAndArgs ...interface{}) (interface{}, error) {
	var keys []string
	var args []interface{}

	if script.KeyCount > 0 {

		keys = []string{}

		for i := 0; i < script.KeyCount; i++ {
			keys = append(keys, keysAndArgs[i].(string))
		}

		args = keysAndArgs[script.KeyCount:]

	} else {
		keys = []string{}
		args = keysAndArgs
	}

	v, err := (*self.delegate).EvalSha(ctx, script.Hash, keys, args...).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		v, err = (*self.delegate).Eval(ctx, script.Src, keys, args...).Result()
	}
	err = noErrNil(err)
	return v, err
}

func (self *GoredisConn) Close() error {
	// Not needed for this library
	return nil
}

func noErrNil(err error) error {

	if err != nil && err.Error() == "redis: nil" {
		return nil
	} else {
		return err
	}

}
