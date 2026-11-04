package utils

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
)

// Pings rdb until it replies or 10s pass.
func WaitForRedis(ctx context.Context, rdb *redis.Client, timeout int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	for {
		if err := rdb.Ping(ctx).Err(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("redis not ready: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}