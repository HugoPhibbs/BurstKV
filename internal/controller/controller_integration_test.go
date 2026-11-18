package controller_test

import (
	"burstkv/internal/controller"
	"burstkv/internal/controller/controller_grpc"
	"burstkv/internal/controller/pod"
	"burstkv/internal/controller/pod_grpc_client_factory"
	"burstkv/internal/core"
	snapshot_storage_test "burstkv/internal/core/snapshot_storage/snapshot_storage_test"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dbImageName = "burstkv-db:test"

// A PodManager backed by real docker containers instead of Kubernetes.
type dockerPodManager struct {
	mu      sync.Mutex
	pods    []*pod.Pod
	claimed map[string]bool // podName -> claimed
}

func TestMain(m *testing.M) {
	if err := buildDBPodImage(); err != nil {
		log.Fatalf("building db_pod image: %v", err)
	}

	code := m.Run() // Run all tests

	exec.Command("docker", "image", "rm", "-f", dbImageName).Run()
	os.Exit(code)
}

func buildDBPodImage() error {
	cmd := exec.Command("docker", "build", "-f", "docker/Dockerfile.db_pod", "-t", dbImageName, ".")
	cmd.Dir = "../.."

	out, err := cmd.CombinedOutput()

	if err != nil {
		log.Print(string(out))
		return err
	}
	return nil
}

func newDockerPodManager(t *testing.T, numPods int, controllerGRPCAddress string) *dockerPodManager {
	t.Helper()
	pm := &dockerPodManager{claimed: map[string]bool{}}

	env := map[string]string{
		"GRPC_PORT":               "6380",
		"REDIS_PORT":              "6379",
		"CONTROLLER_GRPC_ADDRESS": controllerGRPCAddress,
	}

	for i := range numPods {
		name := fmt.Sprintf("burstkv-test-pod-%d", i+1)

		args := []string{"run",
			"-d",
			"--name", name,
			"--add-host=host.docker.internal:host-gateway", // Make host.docker.internal resolve to the dev container
		}
		for k, v := range env {
			args = append(args, "-e", k+"="+v)
		}
		args = append(args, dbImageName)

		cmd := exec.Command("docker", args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println(string(out))
			t.Fail()
		}

		t.Cleanup(func() {
			if t.Failed() {
				// Get logs from the container (useful for debugging)
				out, _ := exec.Command("docker", "logs", name).CombinedOutput()
				t.Logf("logs for %s:\n%s", name, out)
			}
			exec.Command("docker", "rm", "-f", name).Run()
		})

		ip, err := containerIP(name)
		require.NoError(t, err)

		pm.pods = append(pm.pods, pod.NewPod(name, ip))
	}
	return pm
}

func containerIP(name string) (string, error) {
	out, err := exec.Command("docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name).CombinedOutput()

	if err != nil {
		log.Print(string(out))
		return "", err
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" {
		return "", fmt.Errorf("container %s has no IP", name)
	}
	return ip, nil
}

func (pm *dockerPodManager) ClaimWarmPod(ctx context.Context) (*pod.Pod, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, p := range pm.pods {
		if !pm.claimed[p.Name()] {
			pm.claimed[p.Name()] = true
			return p, nil
		}
	}
	return nil, core.ErrNoAvailablePods
}

func (pm *dockerPodManager) DeletePod(ctx context.Context, podName string) error {
	return nil
}

func startControllerGRPCServer(controller *controller.Controller, port int) {
	grpcAddr := utils.PortToLocalAddr(port)
	log.Default().Println("Controller gRPC listening on " + grpcAddr)
	if err := controller_grpc.NewControllerGRPCServer(controller).ListenAndServe(grpcAddr); err != nil {
		log.Println(err)
	}
}

func TestController_TestDBLifecycle(t *testing.T) {
	ctx := t.Context()
	controllerGRPCPort := 8000
	pm := newDockerPodManager(t, 5, fmt.Sprintf("host.docker.internal:%d", controllerGRPCPort))

	cf := pod_grpc_client_factory.NewPodGRPCClientFactory(6380)

	snapshortPort := 9000
	snapshotStorage := snapshot_storage_test.CreateNewTestServer(t, snapshortPort)

	redisPort := 6379

	c := controller.NewController(30*time.Second, 10*time.Second, snapshotStorage, redisPort, pm, cf)

	go startControllerGRPCServer(c, controllerGRPCPort)

	// Create
	dbID, pw, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	require.NotEqual(t, dbID, "")

	dbAddress, err := c.RouteDatabase(ctx, dbID, pw)
	assert.NoError(t, err)

	initialRDB := redis.NewClient(&redis.Options{Addr: dbAddress, Password: pw})

	// Check we can set some data
	assert.NoError(t, initialRDB.Ping(ctx).Err())
	assert.NoError(t, initialRDB.Set(ctx, "test-k", "test-v", 0).Err())

	// Pausing
	assert.NoError(t, c.PauseDatabase(ctx, dbID), "Should be no error when pausing")
	assert.Error(t, initialRDB.Ping(ctx).Err()) // Should now no longer be reachable
	assert.Error(t, c.PauseDatabase(ctx, dbID), "Should be an error when pausing a paused DB")
	assert.NoError(t, err)
	assert.Error(t, initialRDB.Ping(ctx).Err()) // Should now no longer be reachable

	// Resume
	assert.NoError(t, c.ResumeDatabase(ctx, dbID))
	resumedDBAddress, err := c.RouteDatabase(ctx, dbID, pw) // Resume gives a new address
	assert.NotEqual(t, resumedDBAddress, dbAddress)
	resumedRDB := redis.NewClient(&redis.Options{Addr: resumedDBAddress, Password: pw})
	assert.NoError(t, resumedRDB.Ping(ctx).Err())
	val, err := resumedRDB.Get(ctx, "test-k").Result() // Check that we recover the old data
	assert.NoError(t, err)
	assert.Equal(t, "test-v", val)

	// Migrate
	assert.NoError(t, c.MigrateDatabase(ctx, dbID))
	migatedDBAddress, err := c.RouteDatabase(ctx, dbID, pw)
	assert.NotEqual(t, resumedDBAddress, migatedDBAddress)
	migratedDB := redis.NewClient(&redis.Options{Addr: migatedDBAddress, Password: pw})
	assert.NoError(t, migratedDB.Ping(ctx).Err())
	assert.Error(t, resumedRDB.Ping(ctx).Err())       // Check that the old primary is shutdown
	val, err = migratedDB.Get(ctx, "test-k").Result() // Check that the data is migrated
	assert.NoError(t, err)
	assert.Equal(t, "test-v", val)

	// Delete
	assert.NoError(t, c.DeleteDatabase(ctx, dbID))
	deletedAddress, err := c.RouteDatabase(ctx, dbID, pw)
	assert.Equal(t, "", deletedAddress)
	assert.Error(t, err, core.ErrDatabaseNotFound)
	assert.Error(t, migratedDB.Ping(ctx).Err())                      // Check the deleted DB is shutdown
	snapshotExists, err := snapshotStorage.SnapshotExists(ctx, dbID) // Check the snapshot was deletedd
	assert.NoError(t, err)
	assert.False(t, snapshotExists)
}
