package snapshot_storage

import (
	"archive/tar"
	"context"
	"fmt"
	"io"

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

func (s *SnapshotStorage) snapshotObjectName(databaseID string) string {
	return fmt.Sprintf("%s/dump.rdb", databaseID)
}

func (s *SnapshotStorage) LoadSnapshot(ctx context.Context, databaseID string, writer io.Writer) error {
	objectName := s.snapshotObjectName(databaseID)
	obj, err := s.minioClient.GetObject(ctx, s.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return err
	}

	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		return err
	}
	tarWriter := tar.NewWriter(writer)
	if err := tarWriter.WriteHeader(&tar.Header{
		Name: "dump.rdb",
		Size: info.Size,
		Mode: 0644,
	}); err != nil {
		return err
	}

	if _, err := io.Copy(tarWriter, obj); err != nil {
		return err
	}

	return tarWriter.Close()
}

func (s *SnapshotStorage) SaveSnapshot(ctx context.Context, databaseID string, tarStream io.Reader) error {
	tarReader := tar.NewReader(tarStream)
	header, err := tarReader.Next()
	if err != nil {
		return err
	}
	objectName := s.snapshotObjectName(databaseID)
	_, err = s.minioClient.PutObject(ctx, s.bucketName, objectName, tarReader, header.Size, minio.PutObjectOptions{})
	return err
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

func (s *SnapshotStorage) DeleteSnapshot(ctx context.Context, databaseID string) (bool, error) {
	objectName := s.snapshotObjectName(databaseID)
	err := s.minioClient.RemoveObject(ctx, s.bucketName, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return false, nil
		}
		return false, err
	}
	return true, nil
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

func (s *SnapshotStorage) SnapshotLastUpdateTime(ctx context.Context, databaseID string) (int64, error) {
	objectName := s.snapshotObjectName(databaseID)
	info, err := s.minioClient.StatObject(ctx, s.bucketName, objectName, minio.StatObjectOptions{})
	if err != nil {
		return 0, err
	}
	return info.LastModified.Unix(), nil
}
