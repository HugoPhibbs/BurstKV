package snapshot_storage_test

import (
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/utils"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

func repoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..")
}

func startDevSnapshotServer(t *testing.T, port int, ak string, sk string) error {
	name := "snapshot-server-test"
	cmd := exec.Command("python", "./scripts/start_dev_snapshot_server.py", "-n", name, "-nv", "-d", "-p", strconv.Itoa(port), "-ak", ak, "-sk", sk)
	cmd.Dir = repoRoot()
	out, err := cmd.CombinedOutput() // waits for the script, captures stdout+stderr
	fmt.Printf("output from starting snapshot server: \n%s\n", out)
	if err != nil {
		return fmt.Errorf("starting snapshot server: \n%w", err)
	}

	t.Cleanup(func() {
		exec.Command("docker", "rm", "-f", name).Run()
	})
	return nil
}

// Gets the Docker bridge gateway.
// Used so the Snapshot server is accessable locally, and within containers seemlessly, as if it was a remote IP
func DockerGatewayIP() (string, error) {
	out, err := exec.Command("docker", "network", "inspect", "bridge",
		"-f", "{{(index .IPAM.Config 0).Gateway}}").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("inspecting docker bridge: %w: %s", err, out)
	}
	gw := strings.TrimSpace(string(out))
	if gw == "" {
		return "", errors.New("docker bridge network has no gateway")
	}
	return gw, nil
}

func WaitForSnapshotServerReady(ctx context.Context, storage *snapshot_storage.SnapshotStorage) error {
	return utils.WaitUntilReady(ctx, func(ctx context.Context) error {
		_, err := storage.MinioClient().ListBuckets(ctx)
		return err
	}, 15*time.Second, 100*time.Millisecond)
}

func CreateNewTestServer(t *testing.T, port int) *snapshot_storage.SnapshotStorage {
	ctx := t.Context()
	ak := "ak-dev"
	sk := "sk-dev"
	creds := credentials.NewStaticV4(ak, sk, "")

	require.NoError(t, startDevSnapshotServer(t, port, ak, sk))

	gatewayIP, err := DockerGatewayIP()
	require.NoError(t, err)

	snapshotAddress := net.JoinHostPort(gatewayIP, strconv.Itoa(port))
	snapshotStorage := snapshot_storage.NewSnapshotStorage(snapshotAddress, "snapshots", creds, false)

	err = WaitForSnapshotServerReady(ctx, snapshotStorage)
	require.NoError(t, err, "snapshot server never became ready")

	err = snapshotStorage.MinioClient().MakeBucket(ctx, snapshotStorage.BucketName(), minio.MakeBucketOptions{})
	require.NoError(t, err)

	fmt.Printf("Local Snapshot server ready on port: %d", port)

	return snapshotStorage
}
