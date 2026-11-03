package db_proxy

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"burstkv/internal/db_manager"
)

// Max args / bytes per arg accepted in the first command, so a bad client can't make us allocate huge buffers.
const maxFirstCommandSize = 1024

// A single TCP proxy in front of all databases. Clients connect with redis://:<db-id>@host:port,
// so the first command they send is AUTH or HELLO carrying the database ID as the password.
// The proxy reads that ID, then forwards the connection's bytes untouched to that database.
type DBProxy struct {
	listenAddr string
	db_manager *db_manager.DBManager
}

func NewDBProxy(listenAddr string, db_manager *db_manager.DBManager) *DBProxy {
	return &DBProxy{listenAddr: listenAddr, db_manager: db_manager}
}

// Listens on listenAddr and proxies every connection. Blocks.
func (p *DBProxy) ListenAndServe() error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return err
	}
	defer ln.Close()
	log.Printf("proxy: listening on %s", p.listenAddr)
	
	for {
		client, err := ln.Accept()
		if err != nil {
			return err
		}
		go p.handle(client)
	}
}

func (p *DBProxy) handle(client net.Conn) {
	defer client.Close()

	// Everything read while parsing is copied into `first`, so it can be replayed upstream as-is.
	var first bytes.Buffer
	cmd, err := readCommand(bufio.NewReader(io.TeeReader(client, &first)))
	if err != nil {
		return
	}

	databaseId := databaseIdFromCommand(cmd)
	if databaseId == "" {
		io.WriteString(client, "-NOAUTH connect with redis://:<db-id>@host:port\r\n")
		return
	}

	addr, err := p.db_manager.DatabaseAddress(databaseId)
	if err != nil {
		io.WriteString(client, "-ERR "+err.Error()+"\r\n")
		return
	}

	upstream, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("proxy: dial %s for database %s: %v", addr, databaseId, err)
		io.WriteString(client, "-ERR database unavailable\r\n")
		return
	}
	defer upstream.Close()

	// Copy both ways; when either side finishes, return so the deferred Closes end the other copy.
	done := make(chan struct{}, 2)
	pipe := func(dst io.Writer, src io.Reader) {
		io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(upstream, io.MultiReader(&first, client))
	go pipe(client, upstream)
	<-done
}

// The database ID is the password in AUTH <pass>, AUTH <user> <pass>, or HELLO <ver> AUTH <user> <pass>.
func databaseIdFromCommand(cmd []string) string {
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
