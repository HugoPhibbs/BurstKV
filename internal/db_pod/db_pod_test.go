package db_pod

import (
	"burstkv/internal/controller/controller_pb"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type mockClientFactory struct {
	client controller_pb.ControllerClient
	err    error
}

func (f *mockClientFactory) NewConnectedClient() (controller_pb.ControllerClient, func() error, error) {
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.client, func() error { return nil }, nil
}

type MockControllerClient struct {
	url    string
	err    error
	gotReq *controller_pb.GetSnapshotUploadURLRequest
}

func (mc *MockControllerClient) GetSnapshotUploadURL(
	ctx context.Context,
	req *controller_pb.GetSnapshotUploadURLRequest,
	opts ...grpc.CallOption) (
	*controller_pb.GetSnapshotUploadURLResponse,
	error) {
	mc.gotReq = req
	if mc.err != nil {
		return nil, mc.err
	}
	return &controller_pb.GetSnapshotUploadURLResponse{Url: mc.url}, nil
}

func (mc *MockControllerClient) RequestMigration(
	ctx context.Context,
	req *controller_pb.RequestMigrationRequest,
	opts ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, nil // TODO
}

// fakeSnapshotStorage stands in for S3 presigned URLs: PUT stores the body, GET returns it (404 if empty).
type fakeSnapshotStorage struct {
	mu     sync.Mutex
	stored []byte
}

func (s *fakeSnapshotStorage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		s.stored = body
	case http.MethodGet:
		if s.stored == nil {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write(s.stored)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func requireRedis(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"redis-server", "redis-cli"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not on PATH, install redis-server/redis-cli to run this test properly", bin)
		}
	}
}

func newRedisClient(databaseID string, port int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("localhost:%d", port),
		Password: databaseID,
	})
}

func TestLoadSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantErr  error // checked with ErrorIs, only when set
		wantFile bool
	}{
		{"ok", http.StatusOK, "rdb-bytes", nil, true},
		{"not found", http.StatusNotFound, "", ErrSnapshotNotFound, false},
		{"server error", http.StatusInternalServerError, "", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			d := &DBPod{dataDir: t.TempDir()}
			err := d.loadSnapshot(t.Context(), srv.URL)

			switch {
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
			case tc.status != http.StatusOK:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}

			got, readErr := os.ReadFile(d.dumpPath())
			if tc.wantFile {
				require.NoError(t, readErr)
				require.Equal(t, tc.body, string(got))
			} else {
				require.ErrorIs(t, readErr, os.ErrNotExist)
			}
		})
	}
}

func TestAssign_SnapshotNotFound(t *testing.T) {
	// loadSnapshot fails before redis is started, so no redis binary is needed.
	srv := httptest.NewServer(&fakeSnapshotStorage{})
	defer srv.Close()
	
	port := 6390
	d := &DBPod{dataDir: t.TempDir(), redisPort: port}
	err := d.ResumeDatabase(t.Context(), "db-1", srv.URL)

	require.ErrorIs(t, err, ErrSnapshotNotFound)
	require.Nil(t, d.redisServerStartCmd, "redis should not have been started")
}

func TestStop_NotRunning(t *testing.T) {
	d := &DBPod{dataDir: t.TempDir()}
	require.ErrorIs(t, d.Stop(t.Context(), true), ErrNoDatabaseRunningOnPod)
}

// --- Integration tests (need redis-server and redis-cli on PATH) ---

func TestDBPod_TestPausingAndResumingWithSnapshot(t *testing.T) {
	requireRedis(t)
	ctx := t.Context()

	store := &fakeSnapshotStorage{}
	srv := httptest.NewServer(store)
	defer srv.Close()

	controllerClient := &MockControllerClient{url: srv.URL}
	factory := &mockClientFactory{client: controllerClient}

	port := 6390
	pod := &DBPod{dataDir: t.TempDir(), redisPort: port, controllerClientFactory: factory}
	require.NoError(t, pod.CreateDatabase(ctx, "db-1"))
	t.Cleanup(func() { pod.Stop(context.Background(), false) })

	rdb := newRedisClient("db-1", port)
	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())
	rdb.Close()

	require.NoError(t, pod.Stop(ctx, true))
	require.Nil(t, pod.redisServerStartCmd)
	require.Equal(t, "db-1", controllerClient.gotReq.GetDatabaseId())
	require.NotEmpty(t, store.stored, "snapshot should have been uploaded")

	pod2 := &DBPod{dataDir: t.TempDir(), redisPort: port, controllerClientFactory: factory}
	require.NoError(t, pod2.ResumeDatabase(ctx, "db-1", srv.URL))
	t.Cleanup(func() { pod2.Stop(context.Background(), false) })

	rdb2 := newRedisClient("db-1", port)
	defer rdb2.Close()
	got, err := rdb2.Get(ctx, "k").Result()
	require.NoError(t, err)
	require.Equal(t, "v", got)
}

func TestDBPod_RequiresPassword(t *testing.T) {
	requireRedis(t)
	ctx := t.Context()

	port := 6390
	pod := &DBPod{dataDir: t.TempDir(), redisPort: port}
	require.NoError(t, pod.CreateDatabase(ctx, "db-1"))
	t.Cleanup(func() { pod.Stop(context.Background(), false) })

	wrongClient := newRedisClient("not-the-password", port)
	defer wrongClient.Close()
	require.Error(t, wrongClient.Ping(ctx).Err())
}

func TestDBPod_StopWithSave_UploadFails(t *testing.T) {
	requireRedis(t)
	ctx := t.Context()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	port := 6390
	factory := &mockClientFactory{client: &MockControllerClient{url: srv.URL}}
	pod := &DBPod{dataDir: t.TempDir(), redisPort: port, controllerClientFactory: factory}
	require.NoError(t, pod.CreateDatabase(ctx, "db-1"))
	t.Cleanup(func() { pod.Stop(context.Background(), false) })

	err := pod.Stop(ctx, true)
	require.ErrorContains(t, err, "upload snapshot")
	require.NotNil(t, pod.redisServerStartCmd, "redis should keep running if the save failed")
}

func TestDBPod_TestReplicating(t *testing.T) {
	requireRedis(t)
	ctx := t.Context()

	port1 := 6390
	pod1 := &DBPod{dataDir: t.TempDir(), redisPort: port1}
	require.NoError(t, pod1.CreateDatabase(ctx, "db-1"))
	t.Cleanup(func() { pod1.Stop(context.Background(), false) })

	port2 := 6391
	pod2 := &DBPod{dataDir: t.TempDir(), redisPort: port2}
	t.Cleanup(func() { pod2.Stop(context.Background(), false) })

	client1 := newRedisClient("db-1", port1)
	defer client1.Close()

	valKeys := map[string]string{"a": "1", "b": "2", "c": "3"}

	for k, v := range valKeys {
		require.NoError(t, client1.Set(ctx, k, v, 0).Err())
	}

	require.NoError(t, pod2.ReplicateDatabase(ctx, "db-1", fmt.Sprintf("localhost:%d", port1)))

	client2 := newRedisClient("db-1", port2)
	defer client2.Close()

	for k, v := range valKeys {
		got, err := client2.Get(ctx, k).Result()
		require.NoError(t, err)
		require.Equal(t, v, got)
	}

	require.Equal(t, "db-1", pod2.DatabaseID)
}

func TestDBPod_TestMigrating(t *testing.T) {
	// TODO
}