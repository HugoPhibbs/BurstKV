package redis_proxy

import (
	"burstkv/internal/utils"
	"context"
	"fmt"
	"net"
	"os/exec"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRouter struct {
	addrs    map[string]string // read-only after construction, so a plain map is fine
	activity *xsync.Map[string, int]
}

func newFakeRouter(addrs map[string]string) *fakeRouter {
	return &fakeRouter{addrs: addrs, activity: xsync.NewMap[string, int]()}
}

func (r *fakeRouter) RouteDatabase(_ context.Context, id string) (string, error) {
	addr, ok := r.addrs[id]
	if !ok {
		return "", fmt.Errorf("unknown database %q", id)
	}
	return addr, nil
}

func (r *fakeRouter) RecordDatabaseActivity(id string) error {
	r.activity.Compute(id, func(n int, _ bool) (int, xsync.ComputeOp) {
		return n + 1, xsync.UpdateOp
	})
	return nil
}

func (r *fakeRouter) activityFor(id string) int {
	n, _ := r.activity.Load(id)
	return n
}

func localAddr(port string) string {
	return net.JoinHostPort("localhost", port)
}

func startNewRedisServer(t *testing.T, port string, password string) {
	err := exec.CommandContext(t.Context(), "redis-server", "--port", port, "--requirepass", password).Start()
	require.NoError(t, err)
}

func startProxy(t *testing.T, port string, router Router) string {
	addr := localAddr(port)
	go NewRedisProxy(addr, router).ListenAndServe(t.Context())
	return addr
}

func newClient(t *testing.T, addr, password string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestRedisProxy_TestSetAndGet(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6840", "db-a")
	router := newFakeRouter(map[string]string{"db-a": localAddr("6840")})
	proxyAddr := startProxy(t, "6839", router)

	rdb := newClient(t, proxyAddr, "db-a")
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())
	val, err := rdb.Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	// Look directly into the DB to check the write landed
	direct, err := newClient(t, localAddr("6840"), "db-a").Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", direct)

	assert.Positive(t, router.activityFor("db-a"))
}

func TestRedisProxy_TestRoutesByDatabaseID(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6842", "db-a")
	startNewRedisServer(t, "6843", "db-b")
	proxyAddr := startProxy(t, "6841", newFakeRouter(map[string]string{
		"db-a": localAddr("6842"),
		"db-b": localAddr("6843"),
	}))

	clientA, clientB := newClient(t, proxyAddr, "db-a"), newClient(t, proxyAddr, "db-b")
	require.NoError(t, utils.WaitForRedis(ctx, clientA, 5))
	require.NoError(t, utils.WaitForRedis(ctx, clientB, 5))

	require.NoError(t, clientA.Set(ctx, "k", "from-a", 0).Err())
	require.NoError(t, clientB.Set(ctx, "k", "from-b", 0).Err())

	clientADirect, err := newClient(t, localAddr("6842"), "db-a").Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "from-a", clientADirect)

	clientBDirect, err := newClient(t, localAddr("6843"), "db-b").Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "from-b", clientBDirect)
}

func TestRedisProxy_TestNoAuth(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6845", "db-a")
	proxyAddr := startProxy(t, "6844", newFakeRouter(map[string]string{"db-a": localAddr("6845")}))
	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, "db-a"), 5))

	require.Error(t, newClient(t, proxyAddr, "").Ping(ctx).Err()) // No password
}

func TestRedisProxy_TestUnknownServer(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6847", "db-a")
	proxyAddr := startProxy(t, "6846", newFakeRouter(map[string]string{"db-a": localAddr("6847")}))
	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, "db-a"), 5))

	assert.Error(t, newClient(t, proxyAddr, "unknown").Ping(ctx).Err())
}

func TestRedisProxy_TestUnreachableServer(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6849", "db-a")
	proxyAddr := startProxy(t, "6848", newFakeRouter(map[string]string{
		"db-a": localAddr("6849"),
		"db-b": "127.0.0.1:1", // Nothing listening here
	}))

	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, "db-a"), 5))
	assert.Error(t, newClient(t, proxyAddr, "db-b").Ping(ctx).Err())
}
