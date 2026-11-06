package database

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
	rdb := NewDatabase("test_db", "worker_1")

	// Test that the last request time is initially nil
	assert.Nil(t, rdb.LatestActivity(), "expected LastRequestTime to be nil initially")

	// Update the last request time
	rdb.RecordActivity()

	// Test that the last request time is now set
	rdb.RecordActivity()
	updatedLatestActivity := rdb.LatestActivity()
	assert.NotNil(t, updatedLatestActivity, "expected LastRequestTime to be set after update")

	time.Sleep(1 * time.Second) // Sleep for a second to ensure the time changes

	rdb.RecordActivity()
	newLastRequestTime2 := rdb.LatestActivity()
	assert.Greater(t, *newLastRequestTime2, *updatedLatestActivity, "expected LastRequestTime to be updated to a later time after second update")
}
