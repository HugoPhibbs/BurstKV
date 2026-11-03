package redis_db

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9/logging"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	logging.Disable()
	m.Run()
}

func TestRedisDB_TestUpdateLastRequest(t *testing.T) {
	// Create a new RedisDB instance
	rdb := NewRedisDB("localhost:6379", "password")

	// Test that the last request time is initially nil
	assert.Nil(t, rdb.LastRequestTime(), "expected LastRequestTime to be nil initially")

	// Update the last request time
	rdb.RecordActivity()

	// Test that the last request time is now set
	rdb.RecordActivity()
	newLastRequestTime := rdb.LastRequestTime()
	assert.NotNil(t, newLastRequestTime, "expected LastRequestTime to be set after update")

	time.Sleep(1 * time.Second) // Sleep for a second to ensure the time changes

	rdb.RecordActivity()
	newLastRequestTime2 := rdb.LastRequestTime()
	assert.Greater(t, *newLastRequestTime2, *newLastRequestTime, "expected LastRequestTime to be updated to a later time after second update")
}
