package worker_test

import (
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/core/snapshot_storage/snapshot_storage_test"
	"burstkv/internal/worker"
	"testing"

	"github.com/stretchr/testify/require"
)

func NewMockWorker(t *testing.T, minPort, poolSize int) (*worker.Worker, *snapshot_storage.SnapshotStorage) {
	t.Helper()
	mockSnapshotStorage := snapshot_storage_test.NewMockSnapshotStorage(t)
	worker, err := worker.NewWorker(t.Context(), "test", minPort, poolSize, mockSnapshotStorage)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := worker.StopAllContainers(t.Context()); err != nil {
			t.Errorf("stopping containers failed")
		}
	})
	return worker, mockSnapshotStorage
}
