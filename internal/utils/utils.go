package utils

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/redis/go-redis/v9"
)

// Pings rdb until it replies or 10s pass. Timeout in seconds
func WaitForRedis(ctx context.Context, rdb *redis.Client, timeout int) error {
	check := func(ctx context.Context) error {return rdb.Ping(ctx).Err()}
	return WaitUntilReady(ctx, check, time.Duration(timeout) * time.Second, 100 * time.Millisecond)
}

func WaitUntilReady(ctx context.Context, check func(ctx context.Context) error, timeout time.Duration, interval time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		err := check(ctx)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("not ready after %s: %w", timeout, err)
		case <-ticker.C:
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
