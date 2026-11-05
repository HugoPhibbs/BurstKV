package snapshot_storage_test

import (
	"archive/tar"
	"context"
	"io"
)

type NoopSnapshotStorage struct{}

func (NoopSnapshotStorage) SaveSnapshot(ctx context.Context, databaseID string, tarStream io.Reader) error {
	_, err := io.Copy(io.Discard, tarStream) // must drain, or docker cp blocks on a full pipe
	return err
}
	
func (NoopSnapshotStorage) LoadSnapshot(ctx context.Context, databaseID string, w io.Writer) error {
	return tar.NewWriter(w).Close() // empty but valid tar, so docker cp succeeds
}

func (NoopSnapshotStorage) SnapshotExists(ctx context.Context, databaseID string) (bool, error) {
	return true, nil
}

func (NoopSnapshotStorage) DeleteSnapshot(ctx context.Context, databaseID string) (bool, error) {
	return true, nil
}