package snapshot_storage_test

import (
	"burstkv/internal/core/snapshot_storage"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readSnapshot(getURL string) (string, error) {
	resp, err := http.Get(getURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %s", resp.Status)
	}

	data, err := io.ReadAll(resp.Body) // []byte
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func TestSnapshotStorage_TestSnapshotLifecycle(t *testing.T) {
	ctx := t.Context()
	snapshotStorage := CreateNewTestServer(t, 9000)

	testDBID := "test-db"
	expiry := 30 * time.Second
	putURL, err := snapshotStorage.PresignedPutURL(ctx, testDBID, expiry)

	require.NoError(t, err)
	require.NotEqual(t, "", putURL)

	content := "I am a snapshot"
	snapshotReader := strings.NewReader(content)
	snapshot_storage.UploadSnapshot(ctx, snapshotReader, putURL, snapshotReader.Size())

	exists, err := snapshotStorage.SnapshotExists(ctx, testDBID)
	assert.NoError(t, err)
	assert.True(t, exists)

	getURL, err := snapshotStorage.PresignedGetURL(ctx, testDBID, expiry)

	foundContent, err := readSnapshot(getURL)
	assert.Equal(t, content, foundContent)

	assert.NoError(t, snapshotStorage.DeleteSnapshot(ctx, testDBID))

	exists, err = snapshotStorage.SnapshotExists(ctx, testDBID)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestSnapshotStorage_TestManagingMultipleSnapshots(t *testing.T) {
	ctx := t.Context()
	snapshotStorage := CreateNewTestServer(t, 9004)

	testIDs := []string{"db-1", "db-2", "db-3"}
	snapshotContent := []string{"content-1", "content-2", "content-3"}
	expiry := 30 * time.Second

	for i, id := range testIDs {
		putURL, _ := snapshotStorage.PresignedPutURL(ctx, id, expiry)

		reader := strings.NewReader(snapshotContent[i])
		snapshot_storage.UploadSnapshot(ctx, reader, putURL, reader.Size())

		exists, err := snapshotStorage.SnapshotExists(ctx, id)
		assert.NoError(t, err)
		assert.True(t, exists)
	}

	nSnapshots, err := snapshotStorage.NumSnapshots(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, nSnapshots)

	for i, id := range testIDs {
		getURL, _ := snapshotStorage.PresignedGetURL(ctx, id, expiry)

		foundContent, _ := readSnapshot(getURL)
		assert.Equal(t, snapshotContent[i], foundContent)
	}

	err = snapshotStorage.DeleteSnapshot(ctx, "db-1")
	assert.NoError(t, err)

	nSnapshots, err = snapshotStorage.NumSnapshots(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, nSnapshots)

	exists, err := snapshotStorage.SnapshotExists(ctx, "db-1")
	assert.NoError(t, err)
	assert.False(t, exists)

	exists, err = snapshotStorage.SnapshotExists(ctx, "db-2")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = snapshotStorage.SnapshotExists(ctx, "db-3")
	assert.NoError(t, err)
	assert.True(t, exists)
}