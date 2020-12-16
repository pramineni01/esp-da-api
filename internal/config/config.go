package config

import (
	"math/rand"
	"time"
)

// Cache expiration
// ----------------------------------------------

func BASE_CACHE_EXPIRATION() time.Duration {
	rand.Seed(time.Now().UnixNano())
	random := rand.Intn(DEFAULT_MAXRAND_CACHE_EXPIRATION-DEFAULT_MINRAND_CACHE_EXPIRATION+1) + DEFAULT_MINRAND_CACHE_EXPIRATION
	return time.Duration(DEFAULT_BASE_CACHE_EXPIRATION+random) * time.Second
}
