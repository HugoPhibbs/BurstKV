package db_proxy

import (
	"burstkv/internal/db_manager"
	"burstkv/internal/local_db_runtime"
	"burstkv/internal/utils"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logging.Disable()
	m.Run()
}

func TestRedisProxy_TestSetAndGet(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6390, 6399)
	db_manager := db_manager.NewDBManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	const proxyAddr = "127.0.0.1:6400"
	proxy := NewRedisProxy(proxyAddr, db_manager)
	go proxy.ListenAndServe()

	rdb := redis.NewClient(&redis.Options{
		Addr:     proxyAddr,
		Password: new_db.Id(),
	})
	defer rdb.Close()
	
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err)
	assert.Equal(t, "test_value", val)
}

func TestRedisProxy_TestUpdatingLastRequestTime(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6410, 6419)
	db_manager := db_manager.NewDBManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	assert.Nil(t, new_db.LastRequestTime(), "expected LastRequestTime to be nil initially")

	const proxyAddr = "127.0.0.1:6420"
	proxy := NewRedisProxy(proxyAddr, db_manager)
	go proxy.ListenAndServe()

	rdb := redis.NewClient(&redis.Options{
		Addr:     proxyAddr,
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	assert.NotNil(t, new_db.LastRequestTime(), "expected LastRequestTime to be set after a connection")
}

func TestRedisProxy_TestDatabaseReaping(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6430, 6439)
	manager := db_manager.NewDBManager(local_runtime, 1)

	manager.ReaperTimeInterval = 500 * time.Millisecond
	manager.StartIdleDatabaseReaper()

	manager.DeleteAllDatabases()
	defer manager.DeleteAllDatabases()

	new_db, err := manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	const proxyAddr = "127.0.0.1:6440"
	proxy := NewRedisProxy(proxyAddr, manager)
	go proxy.ListenAndServe()

	rdb := redis.NewClient(&redis.Options{
		Addr:     proxyAddr,
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	time.Sleep(2 * time.Second)

	state := manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, db_manager.Paused, state, "expected database to be reaped after idle timeout")	
}