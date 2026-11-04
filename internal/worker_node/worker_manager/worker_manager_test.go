package worker_manager

import (
	"context"
	"os"
	"testing"
	"time"

	"burstkv/internal/utils"
	"burstkv/internal/worker_node"

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

func newTestManager(t *testing.T, minPort, poolSize int) *WorkerManager {
	t.Helper()
	wm, err := NewWorkerManager("test", minPort, poolSize)
	require.NoError(t, err)
	t.Cleanup(wm.Close)
	return wm
}

// Connects to an assigned database, waiting for Redis to start.
func connect(t *testing.T, addr, databaseID string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr, Password: databaseID})
	t.Cleanup(func() { client.Close() })
	require.Eventually(t, func() bool {
		return client.Ping(context.Background()).Err() == nil
	}, 10*time.Second, 100*time.Millisecond, "redis did not become ready")
	return client
}

func TestWorkerManager_AssignAndDeleteDatabase(t *testing.T) {
	wm := newTestManager(t, 31000, 1)
	id := "assign-delete-test"

	addr, err := wm.AssignDatabase(id)
	require.NoError(t, err)
	connect(t, addr, id)

	state, err := wm.DatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, utils.Active, state)
	assert.Contains(t, wm.AssignedDatabases(), id)
	assert.Equal(t, 0, wm.AvailableContainerCount())

	require.NoError(t, wm.DeleteDatabase(id))

	state, err = wm.DatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, utils.NotFound, state)
	assert.Equal(t, 1, wm.AvailableContainerCount())
}

func TestWorkerManager_PauseAndResumeDatabase(t *testing.T) {
	wm := newTestManager(t, 31010, 1)
	id := "pause-resume-test"
	ctx := context.Background()

	addr, err := wm.AssignDatabase(id)
	require.NoError(t, err)
	require.NoError(t, connect(t, addr, id).Set(ctx, "key", "value", 0).Err())

	require.NoError(t, wm.PauseDatabase(id))
	state, err := wm.DatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, utils.Paused, state)

	addr, err = wm.AssignDatabase(id)
	require.NoError(t, err)
	val, err := connect(t, addr, id).Get(ctx, "key").Result()
	require.NoError(t, err)
	assert.Equal(t, "value", val, "data should survive pause/resume")

	require.NoError(t, wm.DeleteDatabase(id))
}

func TestWorkerManager_NoAvailableContainers(t *testing.T) {
	wm := newTestManager(t, 31020, 1)

	_, err := wm.AssignDatabase("first")
	require.NoError(t, err)
	assert.False(t, wm.CanAssignDatabase())

	_, err = wm.AssignDatabase("second")
	assert.ErrorIs(t, err, worker_node.ErrNoAvailableContainers)

	require.NoError(t, wm.DeleteDatabase("first"))
}

func TestWorkerManager_PauseUnassignedDatabase(t *testing.T) {
	wm := newTestManager(t, 31030, 1)
	assert.ErrorIs(t, wm.PauseDatabase("missing"), worker_node.ErrDatabaseNotAssigned)
}
