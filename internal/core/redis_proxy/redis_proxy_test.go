package redis_proxy

import (
	"burstkv/internal/core/snapshot_storage/snapshot_storage_test"
	"burstkv/internal/utils"
	"burstkv/internal/worker"
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

func newTestWorker(t *testing.T, minPort, poolSize int) *worker.Worker {
	t.Helper()
	wm, err := worker.NewWorker("proxy-test", minPort, poolSize, &snapshot_storage_test.NoopSnapshotStorage{})
	require.NoError(t, err)
	t.Cleanup(wm.Close)
	return wm
}

func startProxy(wm *worker.Worker, proxyAddr string) {
	go NewRedisProxy(proxyAddr, wm).ListenAndServe()
}

func TestRedisProxy_WorkerManager_TestSetAndGet(t *testing.T) {
	ctx := t.Context()
	wm := newTestWorker(t, 31100, 1)

	const databaseId = "proxy-set-get"
	err := wm.CreateDatabase(databaseId)
	require.NoError(t, err, "failed to create database")
	defer wm.DeleteDatabase(ctx ,databaseId)

	const proxyAddr = "127.0.0.1:6400"
	startProxy(wm, proxyAddr)

	rdb := redis.NewClient(&redis.Options{
		Addr:     proxyAddr,
		Password: databaseId,
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))

	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

func TestRedisProxy_WorkerManager_TestDynamicRouting(t *testing.T) {
	ctx := t.Context()
	wm := newTestWorker(t, 31130, 5)

	const proxyAddr = "127.0.0.1:6430"
	startProxy(wm, proxyAddr)

	databaseIds := []string{"routing-a", "routing-b", "routing-c"}
	directAddrs := map[string]string{}
	for _, id := range databaseIds {
		err := wm.CreateDatabase(id)
		require.NoError(t, err, "failed to create database %s", id)
		defer wm.DeleteDatabase(ctx ,id)
		addr, err := wm.DatabaseAddress(id)
		require.NoError(t, err, "failed to get database address %s", id)
		directAddrs[id] = addr
	}

	// Same key, a different value per database, all through the one proxy
	for _, id := range databaseIds {
		rdb := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: id})
		defer rdb.Close()
		require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))
		require.NoError(t, rdb.Set(ctx, "shared_key", "value-"+id, 0).Err())
	}

	for _, id := range databaseIds {
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

	assert.Equal(t, 2, wm.AvailableContainerCount(), "expected 2 of 5 containers left unassigned")
}

func TestRedisProxy_WorkerManager_TestNoAuth(t *testing.T) {
	ctx := t.Context()
	wm := newTestWorker(t, 31120, 1)

	const databaseId = "proxy-no-auth"
	err := wm.CreateDatabase(databaseId)
	require.NoError(t, err, "failed to create database")
	defer wm.DeleteDatabase(ctx ,databaseId)

	const proxyAddr = "127.0.0.1:6420"
	startProxy(wm, proxyAddr)

	rdb := redis.NewClient(&redis.Options{Addr: proxyAddr})
	defer rdb.Close()

	assert.Error(t, utils.WaitForRedis(ctx, rdb, 2), "expected connection without a database ID to fail")
}

func TestRedisProxy_WorkerManager_TestResumesPausedDatabase(t *testing.T) {
	ctx := t.Context()
	wm := newTestWorker(t, 31140, 1)

	const databaseId = "proxy-resume"
	err := wm.CreateDatabase(databaseId)
	require.NoError(t, err, "failed to create database")
	defer wm.DeleteDatabase(ctx ,databaseId)

	const proxyAddr = "127.0.0.1:6440"
	startProxy(wm, proxyAddr)

	rdb := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: databaseId})
	defer rdb.Close()
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 10))
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	require.NoError(t, wm.PauseDatabase(ctx, databaseId), "failed to pause database")

	// A new connection to the paused database should resume it with its data intact
	resumed := redis.NewClient(&redis.Options{Addr: proxyAddr, Password: databaseId})
	defer resumed.Close()
	require.NoError(t, utils.WaitForRedis(ctx, resumed, 10))

	val, err := resumed.Get(ctx, "test_key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)
	assert.Contains(t, wm.AssignedDatabases(), databaseId)
}
