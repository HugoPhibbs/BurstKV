package redis_proxy

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/alicebob/miniredis/v2"
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

func newDatabase(t *testing.T, databaseID string) *miniredis.Miniredis {
	s := miniredis.RunT(t)
	s.RequireAuth(databaseID)
	return s
}

func newClient(t *testing.T, p *RedisProxy, password string) *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "proxy", // unused, Dialer takes over
		Password: password,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			client, server := net.Pipe()
			go p.handle(t.Context(), server)
			return client, nil
		},
	})
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

func TestRedisProxy_SetAndGet(t *testing.T) {
	dbA := newDatabase(t, "db-a")
	router := newFakeRouter(map[string]string{"db-a": dbA.Addr()})
	rdb := newClient(t, NewRedisProxy("", router), "db-a")

	require.NoError(t, rdb.Set(t.Context(), "k", "v", 0).Err())
	val, err := rdb.Get(t.Context(), "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	dbA.CheckGet(t, "k", "v") // the write really landed upstream
	assert.Positive(t, router.activityFor("db-a"))
}

func TestRedisProxy_RoutesByDatabaseID(t *testing.T) {
	dbA, dbB := newDatabase(t, "db-a"), newDatabase(t, "db-b")
	p := NewRedisProxy("", newFakeRouter(map[string]string{"db-a": dbA.Addr(), "db-b": dbB.Addr()}))

	require.NoError(t, newClient(t, p, "db-a").Set(t.Context(),
		"k", "from-a", 0).Err())
	require.NoError(t, newClient(t, p, "db-b").Set(t.Context(),
		"k", "from-b", 0).Err())

	dbA.CheckGet(t, "k", "from-a")
	dbB.CheckGet(t, "k", "from-b")
}

func TestRedisProxy_NoAuth(t *testing.T) {
	p := NewRedisProxy("", newFakeRouter(nil))
	assert.Error(t, newClient(t, p, "").Ping(t.Context()).Err())
}

func TestRedisProxy_UnknownDatabase(t *testing.T) {
	p := NewRedisProxy("", newFakeRouter(nil))
	assert.Error(t, newClient(t, p, "nope").Ping(t.Context()).Err())
}

func TestRedisProxy_UpstreamUnreachable(t *testing.T) {
	// A made-up address that nothing listens on
	p := NewRedisProxy("", newFakeRouter(map[string]string{"db-a": "127.0.0.1:1"}))
	assert.Error(t, newClient(t, p, "db-a").Ping(t.Context()).Err())
}
