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
}

func (m *fakePodManager) ClaimWarmPod(ctx context.Context) (*pod.Pod, error) {
	if m.claimed >= m.available {
		return nil, core.ErrNoAvailablePods
	}
	m.claimed++
	return &pod.Pod{PodName: fmt.Sprintf("pod-%d", m.claimed), PodIP: fmt.Sprintf("10.0.0.%d", m.claimed)}, nil
}

func (m *fakePodManager) DeletePod(ctx context.Context) error { return nil }

// Records every RPC sent, keyed by pod IP.
type fakePodClient struct {
	assigns []*db_pod_pb.AssignDatabaseRequest
	stops   []*db_pod_pb.StopRequest
}

func (c *fakePodClient) Assign(ctx context.Context, in *db_pod_pb.AssignDatabaseRequest, opts ...grpc.CallOption) (*db_pod_pb.Empty, error) {
	c.assigns = append(c.assigns, in)
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

type testDeps struct {
	pods      *fakePodManager
	clients   *fakeClientFactory
	snapshots *fakeSnapshotStorage
}

func newTestController(idleLimit time.Duration, availablePods int) (*Controller, *testDeps) {
	deps := &testDeps{
		pods:      &fakePodManager{available: availablePods},
		clients:   &fakeClientFactory{clients: map[string]*fakePodClient{}},
		snapshots: &fakeSnapshotStorage{},
	}
	c := NewController(idleLimit, time.Minute, deps.snapshots, "6379", deps.pods, deps.clients)
	return c, deps
}

// Returns the fake client for the pod the database is (or was last) running on.
func podClientFor(t *testing.T, c *Controller, d *testDeps, id string) *fakePodClient {
	t.Helper()
	db := c.databases[id]
	require.NotNil(t, db.Pod, "database %s has no pod", id)
	return d.clients.clients[db.Pod.PodIP]
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
	require.Len(t, client.assigns, 1)
	assert.Equal(t, id, client.assigns[0].DatabaseId)
	assert.True(t, client.assigns[0].Create)
	assert.Equal(t, d.clients.opened, d.clients.closed, "every connection should be closed")
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

	require.NoError(t, c.PauseDatabase(ctx, id))
	require.Len(t, firstClient.stops, 1)
	assert.True(t, firstClient.stops[0].Save, "pause should save a snapshot")

	state, _ := c.GetDatabaseState(id)
	assert.Equal(t, database.Paused, state)
	assert.ErrorIs(t, c.PauseDatabase(ctx, id), core.ErrDatabaseAlreadyPaused)

	require.NoError(t, c.ResumeDatabase(ctx, id))
	secondClient := podClientFor(t, c, d, id)
	require.Len(t, secondClient.assigns, 1)
	assert.Equal(t, id, secondClient.assigns[0].DatabaseId)
	assert.False(t, secondClient.assigns[0].Create, "resume should not create a new database")

	state, _ = c.GetDatabaseState(id)
	assert.Equal(t, database.Running, state)
	assert.ErrorIs(t, c.ResumeDatabase(ctx, id), core.ErrDatabaseAlreadyActive)
	assert.Equal(t, d.clients.opened, d.clients.closed, "every connection should be closed")
}

func TestController_DeleteRunningDatabase(t *testing.T) {
	ctx := t.Context()
	c, d := newTestController(time.Minute, 1)

	id, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	client := podClientFor(t, c, d, id)

	require.NoError(t, c.DeleteDatabase(ctx, id))

	require.Len(t, client.stops, 1)
	assert.False(t, client.stops[0].Save, "delete should not save a snapshot")
	assert.Equal(t, []string{id}, d.snapshots.deleted)

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
	assert.Equal(t, []string{id}, d.snapshots.deleted)
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
	c, d := newTestController(-time.Second, 2) // any recorded activity counts as idle

	idle, err := c.CreateDatabase(ctx)
	require.NoError(t, err)
	require.NoError(t, c.RecordDatabaseActivity(idle))
	idleClient := podClientFor(t, c, d, idle)

	neverUsed, err := c.CreateDatabase(ctx) // no recorded activity, so never reaped
	require.NoError(t, err)

	c.reapIdleDatabases(ctx)

	state, _ := c.GetDatabaseState(idle)
	assert.Equal(t, database.Paused, state)
	require.Len(t, idleClient.stops, 1)
	assert.True(t, idleClient.stops[0].Save)

	state, _ = c.GetDatabaseState(neverUsed)
	assert.Equal(t, database.Running, state)
}
