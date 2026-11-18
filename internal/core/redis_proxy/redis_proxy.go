package redis_proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type activityReader struct {
	r              io.Reader
	recordActivity func()
}

func (ar *activityReader) Read(p []byte) (int, error) {
	n, err := ar.r.Read(p)
	if n > 0 {
		ar.recordActivity()
	}
	return n, err
}

type Router interface {
	RouteDatabase(ctx context.Context, databaseID string, password string) (string, error)
	RecordDatabaseActivity(databaseID string) error
}

type RedisProxy struct {
	listenAddr string
	router     Router
}

func NewRedisProxy(listenAddr string, router Router) *RedisProxy {
	return &RedisProxy{listenAddr: listenAddr, router: router}
}

// Listens on listenAddr and proxies every connection. Blocks.
func (p *RedisProxy) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		clientConn, err := ln.Accept()
		if err != nil {
			return err
		}
		go p.handle(ctx, clientConn)
	}
}

func (p *RedisProxy) handle(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()

	clientReader := bufio.NewReader(clientConn)

	authCmd, err := readCommand(clientReader) // Read the first one
	if err != nil {
		return
	}

	tokenIdx, err := tokenIndex(authCmd)
	if err != nil {
		return
	}

	databaseID, password, err := splitToken(authCmd[tokenIdx])
	if err != nil {
		io.WriteString(clientConn, "-NOAUTH connect using the supplied token as the password")
		return
	}

	routeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	addr, err := p.router.RouteDatabase(routeCtx, databaseID, password)

	if err != nil {
		io.WriteString(clientConn, "-ERR "+err.Error()+"\r\n")
		return
	}

	upstream, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("proxy: dial %s for database %s: %v", addr, databaseID, err)
		io.WriteString(clientConn, "-ERR database unavailable\r\n")
		return
	}
	defer upstream.Close()

	authCmd[tokenIdx] = password
	if _, err := io.WriteString(upstream, encodeCommand(authCmd)); err != nil {
		log.Printf("proxy: auth upstream %s for database %s: %v", addr, databaseID, err)
		io.WriteString(clientConn, "-ERR database unavailable\r\n")
		return
	}

	recordActivity := func() {
		if err := p.router.RecordDatabaseActivity(databaseID); err != nil {
			log.Println(err)
		}
	}
	done := make(chan struct{}, 2) // One chan spot for client and upstream
	pipe := func(dst io.Writer, src io.Reader) {
		io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(upstream, &activityReader{r: clientReader, recordActivity: recordActivity})
	go pipe(clientConn, upstream)
	<-done
}

// Reads one RESP command, e.g. "*2\r\n$4\r\nAUTH\r\n$3\r\nabc\r\n" -> ["AUTH", "abc"].
// Each arg is a "$<n>" line followed by the value line. We ignore <n> and read the value up
// to "\r\n", which is fine for auth commands (tokens are alphanumeric + "_").
func readCommand(r *bufio.Reader) ([]string, error) {
	const MAX_FIRST_CMD_ARGS = 16

	header, err := readLine(r)
	if err != nil || !strings.HasPrefix(header, "*") {
		return nil, ErrAuthCommandIncorrect
	}
	n, err := strconv.Atoi(header[1:])
	if err != nil || n < 1 || n > MAX_FIRST_CMD_ARGS {
		return nil, ErrAuthCommandIncorrect
	}
	cmd := make([]string, n)
	for i := range cmd {
		if _, err := readLine(r); err != nil { // "$<n>", not needed
			return nil, ErrAuthCommandIncorrect
		}
		if cmd[i], err = readLine(r); err != nil {
			return nil, ErrAuthCommandIncorrect
		}
	}
	return cmd, nil
}

// Returns the index of the token in an auth command, or -1 if it isn't one. Supports:
//
//	AUTH <token>
//	AUTH <user> <token>
//	HELLO <ver> AUTH <user> <token> [...]   (what go-redis v9 sends)
func tokenIndex(cmd []string) (int, error) {
	switch strings.ToUpper(cmd[0]) {
	case "AUTH":
		if len(cmd) == 2 || len(cmd) == 3 {
			return len(cmd) - 1, nil
		}
	case "HELLO":
		for i := 1; i+2 < len(cmd); i++ {
			if strings.EqualFold(cmd[i], "AUTH") {
				return i + 2, nil
			}
		}
	}
	return -1, ErrAuthCommandIncorrect
}

// Encodes a command as RESP, e.$4\r\nAUTH\r\n$2\r\npw\r\n".
func encodeCommand(cmd []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(cmd))
	for _, arg := range cmd {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return b.String()
}

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(line), "\r\n"), nil
}

func splitToken(token string) (string, string, error) {
	parts := strings.Split(token, "_")

	if len(parts) != 2 {
		return "", "", ErrAuthCommandIncorrect
	}

	dbID, pw := parts[0], parts[1]

	return dbID, pw, nil
}
