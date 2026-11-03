package local_db_runtime

import (
	"os/exec"
	"testing"

	"github.com/redis/go-redis/v9/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	m.Run()
	logging.Disable()
	
}

func volumeExists(volumeName string) bool {
	cmd := exec.Command("docker", "volume", "inspect", volumeName)
	err := cmd.Run()
	return err == nil
}

func TestLocalDBRuntime_CreateAndDeleteDatabase(t *testing.T) {
	local_runtime := NewLocalDBRuntime(6379, 6389)
	id := "create-delete-test"
	address, err := local_runtime.CreateDatabase(id)
	require.NoError(t, err, "failed to create database")
	require.NotEmpty(t, address, "created database address is empty")

	err = local_runtime.DeleteDatabase(id)
	require.NoError(t, err, "failed to delete database")

	volumeName := local_runtime.databaseVolumeName(id)
	assert.False(t, volumeExists(volumeName), "expected volume to be deleted after database deletion")
}

func TestLocalDBRuntime_PauseAndResumeDatabase(t *testing.T) {
	local_runtime := NewLocalDBRuntime(6379, 6389)
	id := "pause-resume-test"
	_, err := local_runtime.CreateDatabase(id)
	require.NoError(t, err, "failed to create database")

	err = local_runtime.PauseDatabase(id)
	require.NoError(t, err, "failed to pause database")

	_, err = local_runtime.ResumeDatabase(id)
	require.NoError(t, err, "failed to resume database")

	err = local_runtime.DeleteDatabase(id)
	require.NoError(t, err, "failed to delete database")
}