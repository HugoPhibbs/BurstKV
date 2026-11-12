package controller

import (
	"burstkv/internal/controller/domain/database"
	"burstkv/internal/controller/pod"
	"burstkv/internal/core"
	"burstkv/internal/db_pod/db_pod_pb"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// Hands out pods with unique IPs until `available` runs out.
type fakePodManager struct {
	available int
	claimed   int
	deleted   int
}

func (m *fakePodManager) ClaimWarmPod(ctx context.Context) (*pod.Pod, error) {
	if m.claimed >= m.available {
		return nil, core.ErrNoAvailablePods
	}
	m.claimed++
	return &pod.Pod{PodName: fmt.Sprintf("pod-%d", m.claimed), PodIP: fmt.Sprintf("10.0.0.%d", m.claimed)}, nil
}

func (m *fakePodManager) DeletePod(ctx context.Context, podName string) error {
	m.deleted++
	return nil
}

// Records every RPC sent, keyed by pod IP.
type fakePodClient struct {
	creates   []*db_pod_pb.CreateDatabaseRequest
	resumes   []*db_pod_pb.ResumeDatabaseRequest
	repliates []*db_pod_pb.ReplicateDatabaseRequest
	migrates  []*db_pod_pb.MigrateDatabaseRequest
	stops     []*db_pod_pb.StopRequest
}

func (c *fakePodClient) CreateDatabase(ctx context.Context, in *db_pod_pb.CreateDatabaseRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.creates = append(c.creates, in)
	return &db_pod_pb.Empty{}, nil
}

func (c *fakePodClient) MigrateDatabase(ctx context.Context, in *db_pod_pb.MigrateDatabaseRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.migrates = append(c.migrates, in)
	return &db_pod_pb.Empty{}, nil
}

func (c *fakePodClient) ReplicateDatabase(ctx context.Context, in *db_pod_pb.ReplicateDatabaseRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.repliates = append(c.repliates, in)
	return &db_pod_pb.Empty{}, nil
}

func (c *fakePodClient) ResumeDatabase(ctx context.Context, in *db_pod_pb.ResumeDatabaseRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.resumes = append(c.resumes, in)
	return &db_pod_pb.Empty{}, nil
}

func (c *fakePodClient) Stop(ctx context.Context, in *db_pod_pb.StopRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.stops = append(c.stops, in)
	return &db_pod_pb.Empty{}, nil
}

type fakeClientFactory struct {
	clients map[string]*fakePodClient // podIP -> client
	opened  int
	closed  int
}

func (f *fakeClientFactory) NewConnectedClient(podIP string) (db_pod_pb.DBPodClient, func() error, error) {
	if f.clients[podIP] == nil {
		f.clients[podIP] = &fakePodClient{}
	}
	f.opened++
	return f.clients[podIP], func() error { f.closed++; return nil }, nil
}

type fakeSnapshotStorage struct {
	deleted []string
}

func (s *fakeSnapshotStorage) DeleteSnapshot(ctx context.Context, databaseID string) error {
	s.deleted = append(s.deleted, databaseID)
	return nil
}

func (s *fakeSnapshotStorage) PresignedPutURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error) {
	return "http://snapshots.test/put" + databaseID, nil
}

func (s *fakeSnapshotStorage) PresignedGetURL(ctx context.Context, databaseID string, expiry time.Duration) (string, error) {
	return "http://snapshots.test/get" + databaseID, nil
}

type testDeps struct {
	podManager      *fakePodManager
	clientFactory   *fakeClientFactory
	snapshotStorage *fakeSnapshotStorage
}

func newTestController(idleLimit time.Duration, availablePods int) (*Controller, *testDeps) {
	deps := &testDeps{
		podManager:      &fakePodManager{available: availablePods},
		clientFactory:   &fakeClientFactory{clients: map[string]*fakePodClient{}},
		snapshotStorage: &fakeSnapshotStorage{},
	}
	c := NewController(idleLimit, time.Minute, deps.snapshotStorage, 6379, deps.podManager, deps.clientFactory)
	return c, deps
}

// Returns the fake client for the pod the database is (or was last) running on.
func podClientFor(t *testing.T, c *Controller, d *testDeps, id string) *fakePodClient {
	t.Helper()
	db, ok := c.databases.Load(id)
	require.True(t, ok, "database doesn't exist for the controller")
	require.NotNil(t, db.Pod, "database %s has no pod", id)
	return d.clientFactory.clients[db.Pod.PodIP]
}

func TestController_CreateDatabase(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(time.Minute, 1)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)

	state, err := c.GetDatabaseState(id)
	require.NoError(t, err)
	assert.Equal(t, database.Running, state)

	client := podClientFor(t, c, d, id)
	require.Len(t, client.creates, 1)
	assert.Equal(t, id, client.creates[0].DatabaseId)
	assert.Equal(t, d.clientFactory.opened, d.clientFactory.closed, "every connection should be closed")
}

func TestController_CreateDatabase_NoPods(t *testing.T) {
	c, _ := newTestController(time.Minute, 0)

	_, err := c.CreateDatabase(t.Context())
	assert.ErrorIs(t, err, core.ErrNoAvailablePods)
	assert.Empty(t, c.GetAllDatabaseIDs())
}

func TestController_PauseAndResumeDatabase(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(time.Minute, 2)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	firstClient := podClientFor(t, c, d, id)

	assert.Equal(t, d.podManager.deleted, 0)
	require.NoError(t, c.PauseDatabase(ctx, id))
	require.Len(t, firstClient.stops, 1)
	assert.True(t, firstClient.stops[0].Save, "pause should save a snapshot")
	assert.Equal(t, d.podManager.deleted, 1, "Pausing should increase the number of deleted pods")

	state, _ := c.GetDatabaseState(id)
	assert.Equal(t, database.Paused, state)
	assert.ErrorIs(t, c.PauseDatabase(ctx, id), core.ErrDatabaseAlreadyPaused)
	assert.Equal(t, d.podManager.deleted, 1, "Pausing a paused db shouldn't increasing the number of deleted pods")

	require.NoError(t, c.ResumeDatabase(ctx, id))
	secondClient := podClientFor(t, c, d, id)
	require.Len(t, secondClient.resumes, 1)
	assert.Equal(t, id, secondClient.resumes[0].DatabaseId)

	state, _ = c.GetDatabaseState(id)
	assert.Equal(t, database.Running, state)
	assert.ErrorIs(t, c.ResumeDatabase(ctx, id), core.ErrDatabaseAlreadyActive)
	assert.Equal(t, d.clientFactory.opened, d.clientFactory.closed, "every connection should be closed")
}

func TestController_DeleteRunningDatabase(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(time.Minute, 1)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	client := podClientFor(t, c, d, id)

	assert.Equal(t, d.podManager.deleted, 0)
	require.NoError(t, c.DeleteDatabase(ctx, id))

	require.Len(t, client.stops, 1)
	assert.False(t, client.stops[0].Save, "delete should not save a snapshot")
	assert.Equal(t, []string{id}, d.snapshotStorage.deleted)
	assert.Equal(t, d.podManager.deleted, 1)

	state, _ := c.GetDatabaseState(id)
	assert.Equal(t, database.NotFound, state)
}

func TestController_DeletePausedDatabase(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(time.Minute, 1)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	client := podClientFor(t, c, d, id)
	require.NoError(t, c.PauseDatabase(ctx, id))

	require.NoError(t, c.DeleteDatabase(ctx, id))

	assert.Len(t, client.stops, 1, "a paused database should not be stopped again")
	assert.Equal(t, []string{id}, d.snapshotStorage.deleted)
}

func TestController_UnknownDatabase(t *testing.T) {
	ctx := t.Context()
	c, _ := newTestController(time.Minute, 1)

	state, err := c.GetDatabaseState("missing")
	require.NoError(t, err)
	assert.Equal(t, database.NotFound, state)

	assert.ErrorIs(t, c.PauseDatabase(ctx, "missing"), core.ErrDatabaseNotFound)
	assert.ErrorIs(t, c.ResumeDatabase(ctx, "missing"), core.ErrDatabaseNotFound)
	assert.ErrorIs(t, c.DeleteDatabase(ctx, "missing"), core.ErrDatabaseNotFound)
	assert.ErrorIs(t, c.RecordDatabaseActivity("missing"), core.ErrDatabaseNotFound)
	_, err = c.RouteDatabase(ctx, "missing")
	assert.ErrorIs(t, err, core.ErrDatabaseNotFound)
}

func TestController_GetAllDatabaseIDs(t *testing.T) {
	ctx := t.Context()
	c, _ := newTestController(time.Minute, 2)

	id1, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	id2, err := c.CreateDatabase(ctx)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{id1, id2}, c.GetAllDatabaseIDs())
}

func TestController_RouteDatabase(t *testing.T) {
	ctx := t.Context()
	c, _ := newTestController(time.Minute, 2)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)

	addr, err := c.RouteDatabase(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:6379", addr)

	// Routing to a paused database resumes it on a new pod.
	require.NoError(t, c.PauseDatabase(ctx, id))
	addr, err = c.RouteDatabase(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:6379", addr)

	state, _ := c.GetDatabaseState(id)
	assert.Equal(t, database.Running, state)
}

func TestController_ReapIdleDatabases(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(-time.Second, 2)

	idleDB, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	require.NoError(t, c.RecordDatabaseActivity(idleDB))
	idleClient := podClientFor(t, c, d, idleDB)
	state, _ := c.GetDatabaseState(idleDB)
	assert.Equal(t, database.Running, state)

	unusedDB, err := c.CreateDatabase(ctx)
	require.NoError(t, err)

	require.NoError(t, c.PauseDatabase(ctx, unusedDB))

	c.reapIdleDatabases(ctx)

	state, _ = c.GetDatabaseState(idleDB)
	assert.Equal(t, database.Paused, state) // Check that its reaped
	require.Len(t, idleClient.stops, 1)
	assert.True(t, idleClient.stops[0].Save)

	state, _ = c.GetDatabaseState(unusedDB)
	assert.Equal(t, database.Paused, state) // Already paused
}

func TestController_TestClaimWarmPodClient(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(-time.Second, 2)

	pod, client, close, err := c.ClaimWarmPodClient(ctx)
	require.NoError(t, err)
	defer close()

	require.Equal(t, "pod-1", pod.PodName)
	require.Equal(t, d.clientFactory.clients[pod.PodIP], client, "client should be connected to the pods IP")
	assert.Equal(t, 1, d.clientFactory.opened)
}
