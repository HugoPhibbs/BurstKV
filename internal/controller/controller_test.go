package controller

import (
	"burstkv/internal/controller/domain/database"
	"burstkv/internal/utils"
	"context"
	"fmt"
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

// Starts a worker with a gRPC server on grpcPort and returns a controller connected to it,
// along with the worker itself so tests can look up database addresses.
func newTestController(t *testing.T, idleLimit time.Duration, grpcPort, minPort, poolSize int) (*Controller, *worker.Worker) {
	ctx := t.Context()
	t.Helper()
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)

	controlledWorker, err := controlled_worker.NewControlledWorker("test", grpcAddr, "none")
	require.NoError(t, err)

	remoteWorker, snapshotStorage := worker_test.NewMockWorker(t, minPort, poolSize)
	t.Cleanup(func() {
		if remoteWorker.StopAllContainers(ctx) != nil {
			t.Errorf("stopping containers: %v", err)
		}
	})

	go workergrpc.New(remoteWorker).ListenAndServe(grpcAddr)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, controlledWorker.WaitUntilRPCIsReady(waitCtx))

	require.NoError(t, err)
	t.Cleanup(func() { controlledWorker.RPCConnection.Close() })

	c := NewController(idleLimit, 60*time.Second, snapshotStorage)
	c.AddWorker(ctx, controlledWorker)
	return c, remoteWorker
}

// Connects to a running database, waiting for Redis to start.
func redisConnect(t *testing.T, w *worker.Worker, databaseID string) *redis.Client {
	t.Helper()
	addr, err := w.DatabaseAddress(databaseID)
	require.NoError(t, err)
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: databaseID})
	t.Cleanup(func() { rdb.Close() })
	require.NoError(t, utils.WaitForRedis(t.Context(), rdb, 5))
	return rdb
}

func TestController_TestPauseAndResumeDatabase(t *testing.T) {
	ctx := t.Context()
	c, w := newTestController(t, 300*time.Second, 33000, 32000, 1)
	defer c.DeleteAllDatabases(ctx)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database")

	rdb := redisConnect(t, w, id)
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	require.NoError(t, c.PauseDatabase(ctx, id), "failed to pause database")
	assert.Error(t, rdb.Ping(ctx).Err(), "expected error when pinging paused database")

	require.NoError(t, c.ResumeDatabase(ctx, id), "failed to resume database")

	rdb = redisConnect(t, w, id)
	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err, "failed to get key from resumed database")
	assert.Equal(t, "test_value", val)
}

func TestController_TestDeleteDatabase(t *testing.T) {
	ctx := t.Context()
	c, w := newTestController(t, 300*time.Second, 33001, 32010, 1)
	defer c.DeleteAllDatabases(ctx)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database")

	rdb := redisConnect(t, w, id)
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	require.NoError(t, c.DeleteDatabase(ctx, id), "failed to delete database")
	assert.Error(t, rdb.Ping(ctx).Err(), "expected error when pinging deleted database")
}

func TestController_TestCreateDatabase(t *testing.T) {
	ctx := t.Context()
	c, w := newTestController(t, 300*time.Second, 33002, 32020, 1)
	defer c.DeleteAllDatabases(ctx)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database")

	rdb := redisConnect(t, w, id)
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err, "failed to get key from new database")
	assert.Equal(t, "test_value", val)
}

func TestController_TestGetAllDatabaseIDs(t *testing.T) {
	ctx := t.Context()
	c, _ := newTestController(t, 300*time.Second, 33003, 32030, 2)
	defer c.DeleteAllDatabases(ctx)

	id1, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database 1")

	id2, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database 2")

	all_ids := c.GetAllDatabaseIDs()
	assert.True(t, len(all_ids) == 2, "expected just 2 database IDs, got %d", len(all_ids))
	assert.Contains(t, all_ids, id1, "database 1 ID not found in all IDs")
	assert.Contains(t, all_ids, id2, "database 2 ID not found in all IDs")
}

func TestController_TestGetDatabaseState(t *testing.T) {
	ctx := t.Context()
	c, _ := newTestController(t, 300*time.Second, 33004, 32040, 1)
	defer c.DeleteAllDatabases(ctx)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database")

	state, err := c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.Running, state, "expected database state to be Active")

	require.NoError(t, c.PauseDatabase(ctx, id), "failed to pause database")
	state, err = c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.Paused, state, "expected database state to be Paused")

	require.NoError(t, c.ResumeDatabase(ctx, id), "failed to resume database")
	state, err = c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.Running, state, "expected database state to be Active after resuming")

	require.NoError(t, c.DeleteDatabase(ctx, id), "failed to delete database")
	state, err = c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.NotFound, state, "expected database state to be NotFound after deletion")
}

func TestController_TestIdleDatabaseReaping(t *testing.T) {
	ctx := t.Context()
	timeout := 1 * time.Second
	c, _ := newTestController(t, timeout, 33005, 32050, 1) // Set idle timeout to 1 second for testing
	c.ReaperTimeInterval = 500 * time.Millisecond
	c.StartIdleDatabaseReaper(ctx)
	defer c.DeleteAllDatabases(ctx)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err, "failed to create database")
	require.NoError(t, c.RecordDatabaseActivity(id))

	// Check if the database has been reaped
	require.Eventually(t, func() bool {
		s, _ := c.GetDatabaseState(id)
		return s == database.Paused
	}, 10*time.Second, 100*time.Millisecond)

	state, err := c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.Paused, state, "expected database state to be Paused after idle timeout")
}
