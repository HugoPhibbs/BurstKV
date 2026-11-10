package utils

import (
	"context"
	"fmt"
	"math/rand/v2"
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

// Generates a random string of length n from lower/uppercase letters and digits.
func RandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, n)
	for i := range n {
		result[i] = letters[rand.IntN(len(letters))]
	}
	return string(result)
}

func PortToLocalAddr(port int) string {
	return fmt.Sprintf(":%d", port)
}

func FormatAddress(IP string, port int) string {
	return fmt.Sprintf("%s:%d", IP, port)
}
