package snapshot_storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type SnapshotStorage struct {
	storageURL  string
	bucketName  string
	minioClient *minio.Client
}

func NewSnapshotStorage(storageURL string, bucketName string, minioCreds *credentials.Credentials, useHttps bool) *SnapshotStorage {
	minioClient, err := minio.New(storageURL, &minio.Options{
		Creds:  minioCreds,
		Secure: useHttps,
	})

	if err != nil {
		panic(err)
	}

	return &SnapshotStorage{
		storageURL:  storageURL,
		bucketName:  bucketName,
		minioClient: minioClient,
	}
}

func (s *SnapshotStorage) StorageURL() string {
	return s.storageURL
}

func (s *SnapshotStorage) MinioClient() *minio.Client {
	return s.minioClient
}

func (s *SnapshotStorage) BucketName() string {
	return s.bucketName
}

func (s *SnapshotStorage) snapshotObjectName(databaseID string) string {
	return fmt.Sprintf("%s/dump.rdb", databaseID)
}

func (s *SnapshotStorage) SnapshotExists(ctx context.Context, databaseID string) (bool, error) {
	objectName := s.snapshotObjectName(databaseID)
	_, err := s.minioClient.StatObject(ctx, s.bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *SnapshotStorage) DeleteSnapshot(ctx context.Context, databaseID string) error {
	objectName := s.snapshotObjectName(databaseID)
	err := s.minioClient.RemoveObject(ctx, s.bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return ErrSnapshotNotFound
		}
		return err
	}
	return nil
}

func (s *SnapshotStorage) PresignedPutURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error) {
	putURL, err := s.minioClient.PresignedPutObject(ctx, s.bucketName, s.snapshotObjectName(databaseID), expiry)
	if err != nil {
		return "", err
	}
	return putURL.String(), nil
}

func (s *SnapshotStorage) PresignedGetURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error) {
	getURL, err := s.minioClient.PresignedGetObject(ctx, s.bucketName, s.snapshotObjectName(databaseID), expiry, nil)
	if err != nil {
		return "", err
	}
	return getURL.String(), nil
}

func (s *SnapshotStorage) NumSnapshots(ctx context.Context) (int, error) {
	count := 0
	for obj := range s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			return 0, obj.Err
		}
		count++
	}
	return count, nil
}

// Uploads content from a reader using the given PUT URL
func UploadSnapshot(ctx context.Context, reader io.Reader, putURL string, contentLength int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL, reader)
	if err != nil {
		return err
	}

	req.ContentLength = contentLength

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload snapshot: %s", resp.Status)
	}
	return nil
}
