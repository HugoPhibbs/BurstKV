package snapshot_storage_test

import (
	"burstkv/internal/core/snapshot_storage"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7/pkg/credentials"
)


func NewMockSnapshotStorage(t *testing.T) *snapshot_storage.SnapshotStorage {
	bucketName := "snapshots-mock"
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	mockServer := httptest.NewServer(faker.Server())
	t.Cleanup(func() { mockServer.Close() })

	backend.CreateBucket(bucketName)

	endpoint := strings.TrimPrefix(mockServer.URL, "http://")
	minIoCreds := credentials.NewStaticV4("test-access-key", "test-secret-key", "")
	
	return snapshot_storage.NewSnapshotStorage(
		endpoint,
		bucketName,
		minIoCreds,
		false, 
	)
}