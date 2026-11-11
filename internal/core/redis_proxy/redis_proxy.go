package redis_proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

// Max args / bytes per arg accepted in the first command, so a bad client can't make us allocate huge buffers.
const maxFirstCommandSize = 1024

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
	RouteDatabase(ctx context.Context, databaseID string) (string, error)
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

	// Everything read while parsing is copied into `first`, so it can be replayed upstream as-is.
	var first bytes.Buffer
	cmd, err := readCommand(bufio.NewReader(io.TeeReader(clientConn, &first)))
	if err != nil {
		return
	}

	databaseID := databaseIDFromCommand(cmd)
	if databaseID == "" {
		io.WriteString(clientConn, "-NOAUTH connect with redis://:<db-id>@host:port\r\n")
		return
	}

	addr, err := p.router.RouteDatabase(ctx, databaseID)
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

	recordActivity := func() { 
		if err := p.router.RecordDatabaseActivity(databaseID); err != nil {
			log.Println(err)
		}
	}
	done := make(chan struct{}, 2)
	pipe := func(dst io.Writer, src io.Reader) {
		io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(upstream, &activityReader{r: io.MultiReader(&first, clientConn), recordActivity: recordActivity})
	go pipe(clientConn, upstream)
	<-done
}

// The database ID is the password in AUTH <pass>, AUTH <user> <pass>, or HELLO <ver> AUTH <user> <pass>.
func databaseIDFromCommand(cmd []string) string {
	if len(cmd) == 0 {
		return ""
	}
	switch strings.ToUpper(cmd[0]) {
	case "AUTH":
		return cmd[len(cmd)-1]
	case "HELLO":
		for i := 1; i+2 < len(cmd); i++ {
			if strings.EqualFold(cmd[i], "AUTH") {
				return cmd[i+2]
			}
		}
	}
	return ""
}

// Reads one RESP command, e.g. "*2\r\n$4\r\nAUTH\r\n$3\r\nabc\r\n" -> ["AUTH", "abc"].
func readCommand(r *bufio.Reader) ([]string, error) {
	// Reads a "<prefix><n>\r\n" header line and returns n.
	header := func(prefix byte) (int, error) {
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		if len(line) < 3 || line[0] != prefix {
			return 0, errors.New("malformed RESP")
		}
		n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || n < 0 || n > maxFirstCommandSize {
			return 0, errors.New("malformed RESP")
		}
		return n, nil
	}

	n, err := header('*')
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range n {
		size, err := header('$')
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2) // +2 for the trailing \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}
