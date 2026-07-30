// Package redisclient builds a shared redis.Client from environment config.
package redisclient

import (
	"os"

	"github.com/redis/go-redis/v9"
)

// New builds a redis client from the REDIS_ADDR / REDIS_PASSWORD env vars.
// Addr falls back to the docker-compose default since it's not sensitive;
// the password has no fallback and must be set (see .env.example).
func New() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})
}
