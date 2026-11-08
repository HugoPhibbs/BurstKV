package redis_proxy

import (
	"burstkv/internal/utils"
	"burstkv/internal/worker"
	"burstkv/internal/worker/worker_test"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logging.Disable()
	if err := os.MkdirAll("snapshots", 0o755); err != nil { // WorkerManager stores paused snapshots here
		panic(err)
	}
	code := m.Run()
	os.RemoveAll("snapshots")
	os.Exit(code)
}

func startProxy(wm *worker.Worker, proxyAddr string) {
	go NewRedisProxy(proxyAddr, wm).ListenAndServe()
}

func TestRedisProxy_TestSetAndGet(t *testing.T) {
	ctx := t.Context()
	mockWorker, _ := worker_test.NewMockWorker(t, 31100, 1)

	const databaseID = "proxy-set-get"
	err := mockWorker.CreateDatabase(ctx, databaseID)
	require.NoError(t, err, "failed to create database")
	defer mockWorker.DeleteDatabase(ctx, databaseID)

	const proxyAddr = "127.0.0.1:6400"
	startProxy(mockWorker, proxyAddr)

	rdb := redis.NewClient(&redis.Options{
		Addr:     proxyAddr,
		Password: databaseID,
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))

	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

func NewMockWorker(t *testing.T, i1, i2 int) any {
	panic("unimplemented")
}

func TestRedisProxy_TestDynamicRouting(t *testing.T) {
	ctx := t.Context()
	mockWorker, _ := worker_test.NewMockWorker(t, 31130, 5)

	const proxyAddr = "127.0.0.1:6430"
	startProxy(mockWorker, proxyAddr)

	databaseIDs := []string{"routing-a", "routing-b", "routing-c"}
	directAddrs := map[string]string{}
	for _, id := range databaseIDs {
		err := mockWorker.CreateDatabase(ctx, id)
		require.NoError(t, err, "failed to create database %s", id)
		defer mockWorker.DeleteDatabase(ctx, id)
		addr, err := mockWorker.DatabaseAddress(id)
		require.NoError(t, err, "failed to get database address %s", id)
		directAddrs[id] = addr
	}

	// Same key, a different value per database, all through the one proxy
	for _, id := range databaseIDs {
		rdb := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: id})
		defer rdb.Close()
		require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))
		require.NoError(t, rdb.Set(ctx, "shared_key", "value-"+id, 0).Err())
	}

	for _, id := range databaseIDs {
		// Read back through the proxy
		viaProxy := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: id})
		defer viaProxy.Close()
		val, err := viaProxy.Get(ctx, "shared_key").Result()
		require.NoError(t, err)
		assert.Equal(t, "value-"+id, val, "proxy returned the wrong value for %s", id)

		// Read straight from the database's container, to check the write landed there
		direct := redis.NewClient(&redis.Options{Addr: directAddrs[id], Password: id})
		defer direct.Close()
		val, err = direct.Get(ctx, "shared_key").Result()
		require.NoError(t, err)
		assert.Equal(t, "value-"+id, val, "write for %s did not land in its own container", id)

		size, err := direct.DBSize(ctx).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), size, "container for %s should only hold its own key", id)
	}

	assert.Equal(t, 2, mockWorker.AvailableContainerCount(), "expected 2 of 5 containers left unassigned")
}

func TestRedisProxy_TestNoAuth(t *testing.T) {
	ctx := t.Context()
	mockWorker, _ := worker_test.NewMockWorker(t, 31120, 1)

	const databaseID = "proxy-no-auth"
	err := mockWorker.CreateDatabase(ctx, databaseID)
	require.NoError(t, err, "failed to create database")
	defer mockWorker.DeleteDatabase(ctx, databaseID)

	const proxyAddr = "127.0.0.1:6420"
	startProxy(mockWorker, proxyAddr)

	rdb := redis.NewClient(&redis.Options{Addr: proxyAddr})
	defer rdb.Close()

	assert.Error(t, utils.WaitForRedis(ctx, rdb, 2), "expected connection without a database ID to fail")
}

func TestRedisProxy_TestResumesPausedDatabase(t *testing.T) {
	ctx := t.Context()
	mockWorker, _ := worker_test.NewMockWorker(t, 31140, 1)

	const databaseID = "proxy-resume"
	err := mockWorker.CreateDatabase(ctx, databaseID)
	require.NoError(t, err, "failed to create database")
	defer mockWorker.DeleteDatabase(ctx, databaseID)

	const proxyAddr = "127.0.0.1:6440"
	startProxy(mockWorker, proxyAddr)

	rdb := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: databaseID})
	defer rdb.Close()
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	// Now pause the database
	require.NoError(t, mockWorker.PauseDatabase(ctx, databaseID), "failed to pause database")
	require.Error(t, utils.WaitForRedis(ctx, rdb, 2)) // Should not reconnect

	// Now resume the database again
	require.NoError(t, mockWorker.ResumeDatabase(ctx, databaseID))

	// A new connection to the paused database should resume it with its data intact
	resumed := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: databaseID})
	defer resumed.Close()
	require.NoError(t, utils.WaitForRedis(ctx, resumed, 10))

	val, err := resumed.Get(ctx, "test_key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)
	assert.Contains(t, mockWorker.AssignedDatabases(), databaseID)
}
