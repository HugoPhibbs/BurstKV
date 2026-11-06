package worker_test

import (
	"context"
	"os"
	"testing"
	"time"

	"burstkv/internal/worker"

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

func TestWorker_CreateAndDeleteDatabase(t *testing.T) {
	ctx := context.Background()

	worker, _ := NewMockWorker(t, 31000, 1)
	id := "assign-delete-test"

	err := worker.CreateDatabase(id)
	require.NoError(t, err)
	addr, err := worker.DatabaseAddress(id)
	require.NoError(t, err)
	redisConnect(t, addr, id)

	assert.True(t, worker.IsAssigned(id), "database should be assigned after creation")
	assert.Equal(t, 0, worker.AvailableContainerCount())

	require.NoError(t, worker.DeleteDatabase(ctx, id))
	assert.False(t, worker.IsAssigned(id), "database should not be assigned after deletion")

	assert.Equal(t, 1, worker.AvailableContainerCount())
}

func TestWorker_PauseAndResumeDatabase(t *testing.T) {
	worker, snapshotStorage := NewMockWorker(t, 31010, 1)
	id := "pause-resume-test"
	ctx := context.Background()

	err := worker.CreateDatabase(id)
	require.NoError(t, err)
	addr, err := worker.DatabaseAddress(id)
	require.NoError(t, err)
	require.NoError(t, redisConnect(t, addr, id).Set(ctx, "key", "value", 0).Err())

	require.NoError(t, worker.PauseDatabase(ctx, id))
	assert.False(t, worker.IsAssigned(id), "database should not be assigned after pause")
	assert.Equal(t, worker.AvailableContainerCount(), 1)
	assert.True(t, worker.CanAssignDatabase())
	exists, err := snapshotStorage.SnapshotExists(ctx, id)
	require.NoError(t, err)
	assert.True(t, exists, "snapshot should exist after pausing database")

	err = worker.ResumeDatabase(id)
	require.NoError(t, err)
	addr, err = worker.DatabaseAddress(id)
	require.NoError(t, err)
	val, err := redisConnect(t, addr, id).Get(ctx, "key").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", val, "data should survive pause/resume")

	require.NoError(t, worker.DeleteDatabase(ctx, id))
}

func TestWorker_TestRepeatedPauseResume(t *testing.T) {
	worker, snapshotStorage := NewMockWorker(t, 31020, 1)
	id := "repeated-pause-resume-test"
	ctx := context.Background()

	err := worker.CreateDatabase(id)
	require.NoError(t, err)
	addr, err := worker.DatabaseAddress(id)
	require.NoError(t, err)

	newVals := []string{"value1", "value2", "value3"}
	nRepeats := len(newVals)

	for i := 0; i < nRepeats; i++ {
		require.NoError(t, redisConnect(t, addr, id).Set(ctx, "key", newVals[i], 0).Err())

		// Sleep a bit to ensure update times are slightly different
		time.Sleep(10 * time.Millisecond)

		require.NoError(t, worker.PauseDatabase(ctx, id))
		assert.False(t, worker.IsAssigned(id), "database should not be assigned after pause")
		exists, err := snapshotStorage.SnapshotExists(ctx, id)
		require.NoError(t, err)
		assert.True(t, exists, "snapshot should exist after pausing database")

		err = worker.ResumeDatabase(id)
		require.NoError(t, err)
		addr, err = worker.DatabaseAddress(id)
		require.NoError(t, err)
		val, err := redisConnect(t, addr, id).Get(ctx, "key").Result()
		require.NoError(t, err)
		assert.Equal(t, newVals[i], val, "data should survive pause/resume")
	}
}

func TestWorker_NoAvailableContainers(t *testing.T) {
	ctx := context.Background()
	mockWorker, _ := NewMockWorker(t, 31020, 1)

	err := mockWorker.CreateDatabase("first")
	require.NoError(t, err)
	assert.False(t, mockWorker.CanAssignDatabase())

	err = mockWorker.CreateDatabase("second")
	assert.ErrorIs(t, err, worker.ErrNoAvailableContainers)

	require.NoError(t, mockWorker.DeleteDatabase(ctx, "first"))
}

func TestWorker_PauseUnassignedDatabase(t *testing.T) {
	ctx := context.Background()
	mockWorker, _ := NewMockWorker(t, 31030, 1)

	assert.ErrorIs(t, mockWorker.PauseDatabase(ctx, "missing"), worker.ErrDatabaseNotAssigned)
}

func TestWorker_TestStoppingAllContainers(t *testing.T) {
	ctx := context.Background()

	worker, snapshotStorage := NewMockWorker(t, 31040, 2)

	err := worker.CreateDatabase("first")
	require.NoError(t, err)
	err = worker.CreateDatabase("second")
	require.NoError(t, err)

	numSnapshots, _ := snapshotStorage.NumSnapshots(ctx)
	assert.Equal(t, 0, numSnapshots, "expected no snapshots before stopping containers")

	success, err := worker.StopAllContainers()
	require.NoError(t, err)
	assert.True(t, success, "expected all containers to stop successfully")
	numSnapshots, _ = snapshotStorage.NumSnapshots(ctx)
	assert.Equal(t, 2, numSnapshots, "expected snapshots to be saved after stopping all containers")

	assert.Equal(t, 0, worker.AvailableContainerCount())
	assert.False(t, worker.IsAssigned("first"))
	assert.False(t, worker.IsAssigned("second"))
}

func TestWorker_TestPausingDatabaseSavesSnapshot(t *testing.T) {
	mockWorker, snapshotStorage := NewMockWorker(t, 31050, 2)

	// Snapshot doesn't exist yet
	exists, err := snapshotStorage.SnapshotExists(context.Background(), "first")
	require.NoError(t, err)
	assert.False(t, exists, "expected no snapshot for database 'first'")

	// Create and pause the databasee
	require.NoError(t, mockWorker.CreateDatabase("first"))
	require.NoError(t, mockWorker.PauseDatabase(context.Background(), "first"))

	// Snapshot should now exist
	exists, err = snapshotStorage.SnapshotExists(context.Background(), "first")
	require.NoError(t, err)
	assert.True(t, exists, "expected snapshot for database 'first' after pausing")
}

func TestWorker_TestResumingDatabaseLoadsSnapshot(t *testing.T) {
}
