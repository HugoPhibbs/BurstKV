package worker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logging.Disable()
	if err := os.MkdirAll("snapshots", 0o755); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll("snapshots")
	os.Exit(code)
}

func newTestWorker(t *testing.T, minPort, poolSize int) *Worker {
	t.Helper()
	worker, err := NewWorker("test", minPort, poolSize)
	require.NoError(t, err)
	t.Cleanup(worker.Close)
	return worker
}

// Connects to an assigned database, waiting for Redis to start.
func redisConnect(t *testing.T, addr, databaseID string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr, Password: databaseID})
	t.Cleanup(func() { client.Close() })
	require.Eventually(t, func() bool {
		return client.Ping(context.Background()).Err() == nil
	}, 10*time.Second, 100*time.Millisecond, "redis did not become ready")
	return client
}

func TestWorkerManager_CreateAndDeleteDatabase(t *testing.T) {
	worker := newTestWorker(t, 31000, 1)
	id := "assign-delete-test"

	err := worker.CreateDatabase(id)
	require.NoError(t, err)
	addr, err := worker.DatabaseAddress(id)
	require.NoError(t, err)
	redisConnect(t, addr, id)

	assert.True(t, worker.IsAssigned(id), "database should be assigned after creation")
	assert.Equal(t, 0, worker.AvailableContainerCount())

	require.NoError(t, worker.DeleteDatabase(id))
	assert.False(t, worker.IsAssigned(id), "database should not be assigned after deletion")

	assert.Equal(t, 1, worker.AvailableContainerCount())
}

func TestWorkerManager_PauseAndResumeDatabase(t *testing.T) {
	worker := newTestWorker(t, 31010, 1)
	id := "pause-resume-test"
	ctx := context.Background()

	err := worker.CreateDatabase(id)
	require.NoError(t, err)
	addr, err := worker.DatabaseAddress(id)
	require.NoError(t, err)
	require.NoError(t, redisConnect(t, addr, id).Set(ctx, "key", "value", 0).Err())

	require.NoError(t, worker.PauseDatabase(id))
	assert.False(t, worker.IsAssigned(id), "database should not be assigned after pause")

	err = worker.ResumeDatabase(id)
	require.NoError(t, err)
	addr, err = worker.DatabaseAddress(id)
	require.NoError(t, err)
	val, err := redisConnect(t, addr, id).Get(ctx, "key").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", val, "data should survive pause/resume")

	require.NoError(t, worker.DeleteDatabase(id))
}

func TestWorkerManager_NoAvailableContainers(t *testing.T) {
	worker := newTestWorker(t, 31020, 1)

	err := worker.CreateDatabase("first")
	require.NoError(t, err)
	assert.False(t, worker.CanAssignDatabase())

	err = worker.CreateDatabase("second")
	assert.ErrorIs(t, err, ErrNoAvailableContainers)

	require.NoError(t, worker.DeleteDatabase("first"))
}

func TestWorkerManager_PauseUnassignedDatabase(t *testing.T) {
	worker := newTestWorker(t, 31030, 1)
	assert.ErrorIs(t, worker.PauseDatabase("missing"), ErrDatabaseNotAssigned)
}
