package redis_proxy

import (
	"bufio"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRouter struct {
	addrs    map[string]string // ID to address
	activity *xsync.Map[string, int]
}

func newFakeRouter(addrs map[string]string) *fakeRouter {
	return &fakeRouter{addrs: addrs, activity: xsync.NewMap[string, int]()}
}

func (r *fakeRouter) RouteDatabase(_ context.Context, id string, password string) (string, error) {
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

func contructToken(id string, pw string) string {
	return fmt.Sprintf("%s_%s", id, pw)
}

// Password used for every test Redis server, unless a test needs something different
const testPW = "pw"

func TestRedisProxy_TestSetAndGet(t *testing.T) {
	ctx := t.Context()
	id := "db-a"
	startNewRedisServer(t, "6840", testPW)
	router := newFakeRouter(map[string]string{id: localAddr("6840")})
	proxyAddr := startProxy(t, "6839", router)

	token := contructToken(id, testPW)
	rdb := newClient(t, proxyAddr, token)
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())
	val, err := rdb.Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	// Look directly into the DB to check the write landed
	direct, err := newClient(t, localAddr("6840"), testPW).Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", direct)

	assert.Positive(t, router.activityFor("db-a"))
}

func TestRedisProxy_TestRoutesByDatabaseID(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6842", testPW)
	startNewRedisServer(t, "6843", testPW)
	proxyAddr := startProxy(t, "6841", newFakeRouter(map[string]string{
		"db-a": localAddr("6842"),
		"db-b": localAddr("6843"),
	}))

	clientA := newClient(t, proxyAddr, contructToken("db-a", testPW))
	clientB := newClient(t, proxyAddr, contructToken("db-b", testPW))
	require.NoError(t, utils.WaitForRedis(ctx, clientA, 5))
	require.NoError(t, utils.WaitForRedis(ctx, clientB, 5))

	require.NoError(t, clientA.Set(ctx, "k", "from-a", 0).Err())
	require.NoError(t, clientB.Set(ctx, "k", "from-b", 0).Err())

	clientADirect, err := newClient(t, localAddr("6842"), testPW).Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "from-a", clientADirect)

	clientBDirect, err := newClient(t, localAddr("6843"), testPW).Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "from-b", clientBDirect)
}

func TestRedisProxy_TestNoAuth(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6845", testPW)
	proxyAddr := startProxy(t, "6844", newFakeRouter(map[string]string{"db-a": localAddr("6845")}))
	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, contructToken("db-a", testPW)), 5))

	require.Error(t, newClient(t, proxyAddr, "").Ping(ctx).Err()) // No password
}

func TestRedisProxy_TestWrongAuth(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6853", testPW)
	proxyAddr := startProxy(t, "6852", newFakeRouter(map[string]string{"db-a": localAddr("6853")}))
	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, contructToken("db-a", testPW)), 5))

	require.Error(t, newClient(t, proxyAddr, contructToken("db-a", "wrong")).Ping(ctx).Err())
}

func TestRedisProxy_TestUnknownServer(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6847", testPW)
	proxyAddr := startProxy(t, "6846", newFakeRouter(map[string]string{"db-a": localAddr("6847")}))
	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, contructToken("db-a", testPW)), 5))

	assert.Error(t, newClient(t, proxyAddr, contructToken("unknown", testPW)).Ping(ctx).Err())
}

func TestRedisProxy_TestUnreachableServer(t *testing.T) {
	ctx := t.Context()
	startNewRedisServer(t, "6849", testPW)
	proxyAddr := startProxy(t, "6848", newFakeRouter(map[string]string{
		"db-a": localAddr("6849"),
		"db-b": "127.0.0.1:1", // Nothing listening here
	}))

	require.NoError(t, utils.WaitForRedis(ctx, newClient(t, proxyAddr, contructToken("db-a", testPW)), 5))
	assert.Error(t, newClient(t, proxyAddr, contructToken("db-b", testPW)).Ping(ctx).Err())
}

func TestRedisProxy_TestUpdatingActivity(t *testing.T) {
	ctx := t.Context()

	dbID := "db-a"
	startNewRedisServer(t, "6850", testPW)
	router := newFakeRouter(map[string]string{dbID: localAddr("6850")})
	proxyAddr := startProxy(t, "6851", router)

	rdb := newClient(t, proxyAddr, contructToken(dbID, testPW))
	require.NoError(t, utils.WaitForRedis(ctx, rdb, 5))

	pings, _ := router.activity.Load(dbID)

	require.NoError(t, rdb.Set(ctx, "k", "v", 0).Err())

	a, _ := router.activity.Load(dbID)
	require.Equal(t, a, pings+1)

	val, err := rdb.Get(ctx, "k").Result()
	require.NoError(t, err)
	assert.Equal(t, "v", val)

	a, _ = router.activity.Load(dbID)
	require.Equal(t, a, pings+2)
}

func TestRedisProxy_TestAuthFromCmd(t *testing.T) {
	id, pw, err := splitToken("id_pw")
	assert.NoError(t, err)
	assert.Equal(t, "id", id)
	assert.Equal(t, "pw", pw)

	id, pw, err = splitToken("id_pw_d")
	assert.ErrorIs(t, err, ErrAuthCommandIncorrect)
}

func TestRedisProxy_TestReadCommand(t *testing.T) {
	// AUTH <token>
	r := bufio.NewReader(strings.NewReader("*2\r\n$4\r\nAUTH\r\n$10\r\nabc_s3cret\r\n"))
	cmd, err := readCommand(r)
	require.NoError(t, err)
	assert.Equal(t, []string{"AUTH", "abc_s3cret"}, cmd)

	// Exactly what go-redis v9 sends first
	r = bufio.NewReader(strings.NewReader("*5\r\n$5\r\nhello\r\n$1\r\n3\r\n$4\r\nauth\r\n$7\r\ndefault\r\n$7\r\ndb-a_pw\r\n"))
	cmd, err = readCommand(r)
	require.NoError(t, err)
	assert.Equal(t, []string{"hello", "3", "auth", "default", "db-a_pw"}, cmd)

	// A pipelined SET after the AUTH is left unread in r
	r = bufio.NewReader(strings.NewReader("*2\r\n$4\r\nAUTH\r\n$10\r\nabc_s3cret\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	_, err = readCommand(r)
	require.NoError(t, err)
	rest, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", string(rest))

	// Malformed input
	for _, input := range []string{
		"",                     // nothing sent
		"AUTH abc_s3cret\r\n",  // inline command, not RESP
		"*abc\r\n",             // count isn't a number
		"*0\r\n",               // empty array
		"*17\r\n",              // too many args
		"*2\r\n$4\r\nAUTH\r\n", // fewer args than promised
	} {
		_, err := readCommand(bufio.NewReader(strings.NewReader(input)))
		assert.ErrorIs(t, err, ErrAuthCommandIncorrect, "input %q", input)
	}
}

func TestRedisProxy_TestExtractTokenFromAuth(t *testing.T) {
	// Commands that carry a token
	for _, tc := range []struct {
		cmd   []string
		token string
	}{
		{[]string{"AUTH", "abc_pw"}, "abc_pw"},
		{[]string{"auth", "abc_pw"}, "abc_pw"},
		{[]string{"AUTH", "default", "abc_pw"}, "abc_pw"},
		{[]string{"HELLO", "3", "AUTH", "default", "abc_pw"}, "abc_pw"},
		{[]string{"hello", "2", "auth", "default", "abc_pw"}, "abc_pw"},
		{[]string{"HELLO", "3", "AUTH", "default", "abc_pw", "SETNAME", "myclient"}, "abc_pw"},
	} {
		i, err := tokenIndex(tc.cmd)
		require.NoError(t, err, "cmd %v", tc.cmd)
		assert.Equal(t, tc.token, tc.cmd[i], "cmd %v", tc.cmd)
	}

	// Commands without a token
	for _, cmd := range [][]string{
		{"PING"},
		{"SET", "foo", "bar"},
		{"AUTH"},                          // no token
		{"AUTH", "a", "b", "c"},           // too many args
		{"HELLO", "3"},                    // HELLO without AUTH
		{"HELLO", "3", "AUTH", "default"}, // AUTH missing the token
	} {
		_, err := tokenIndex(cmd)
		assert.ErrorIs(t, err, ErrAuthCommandIncorrect, "cmd %v", cmd)
	}
}
