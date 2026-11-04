package db_manager
//go:build ignore

import (
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

func TestDBManager_TestPauseAndResumeDatabase(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	db_manager := NewCentralManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	rdb := redis.NewClient(&redis.Options{
		Addr:     new_db.Address(),
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	require.NoError(t, db_manager.PauseDatabase(new_db.Id()), "failed to pause database")
	assert.Error(t, rdb.Ping(ctx).Err(), "expected error when pinging paused database")

	new_address, err := db_manager.ResumeDatabase(new_db.Id())
	require.NoError(t, err, "failed to resume database")

	rdb = redis.NewClient(&redis.Options{
		Addr:     new_address,
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err, "failed to get key from resumed database")
	assert.Equal(t, "test_value", val)
}

func TestDBManager_TestDeleteDatabase(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	db_manager := NewCentralManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	rdb := redis.NewClient(&redis.Options{
		Addr:     new_db.Address(),
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	require.NoError(t, db_manager.DeleteDatabase(new_db.Id()), "failed to delete database")
	assert.Error(t, rdb.Ping(ctx).Err(), "expected error when pinging deleted database")
}

func TestDBManager_TestCreateDatabase(t *testing.T) {
	ctx := t.Context()
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	db_manager := NewCentralManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	rdb := redis.NewClient(&redis.Options{
		Addr:     new_db.Address(),
		Password: new_db.Id(),
	})
	defer rdb.Close()

	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))
	require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())

	val, err := rdb.Get(ctx, "test_key").Result()
	require.NoError(t, err, "failed to get key from new database")
	assert.Equal(t, "test_value", val)
}

func TestDBManager_TestGetAllDatabaseIDs(t *testing.T) {
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	db_manager := NewCentralManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db1, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database 1")

	new_db2, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database 2")

	all_ids := db_manager.GetAllDatabaseIDs()
	assert.True(t, len(all_ids) == 2, "expected just 2 database IDs, got %d", len(all_ids))
	assert.Contains(t, all_ids, new_db1.Id(), "database 1 ID not found in all IDs")
	assert.Contains(t, all_ids, new_db2.Id(), "database 2 ID not found in all IDs")
}

func TestDBManager_TestGetDatabaseState(t *testing.T) {
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	db_manager := NewCentralManager(local_runtime, 300)
	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")

	state := db_manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, Active, state, "expected database state to be Active")

	require.NoError(t, db_manager.PauseDatabase(new_db.Id()), "failed to pause database")
	state = db_manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, Paused, state, "expected database state to be Paused")

	_, err = db_manager.ResumeDatabase(new_db.Id())
	require.NoError(t, err, "failed to resume database")
	state = db_manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, Active, state, "expected database state to be Active after resuming")

	require.NoError(t, db_manager.DeleteDatabase(new_db.Id()), "failed to delete database")
	state = db_manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, NotFound, state, "expected database state to be NotFound after deletion")
}

func TestDBManager_TestIdleDatabaseReaping(t *testing.T) {
	local_runtime := local_db_runtime.NewLocalDBRuntime(6380, 6389)
	timeout := 1 * time.Second
	db_manager := NewCentralManager(local_runtime, timeout) // Set idle timeout to 1 second for testing
	db_manager.ReaperTimeInterval = 500 * time.Millisecond
	db_manager.StartIdleDatabaseReaper()

	db_manager.DeleteAllDatabases()
	defer db_manager.DeleteAllDatabases()

	new_db, err := db_manager.CreateDatabase()
	require.NoError(t, err, "failed to create database")
	db_manager.RecordDatabaseActivity(new_db.Id())
	assert.NotNil(t, *new_db.LastRequestTime(), "expected LastRequestTime to be set after activity recording")

	// Wait for more than the idle timeout
	time.Sleep(2 * timeout)

	// Check if the database has been reaped
	state := db_manager.GetDatabaseState(new_db.Id())
	assert.Equal(t, Paused, state, "expected database state to be Paused after idle timeout")
}
