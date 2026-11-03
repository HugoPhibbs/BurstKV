package local_db_runtime

import (
	"burstkv/internal/db_runtime"
	"burstkv/internal/redis_db"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

const REDIS_IMAGE = "redis:8.10.3"
const CONTAINER_PREFIX = "burstkv-"

type LocalDBRuntime struct {
	portPool chan int
	minPort  int
	maxPort  int
}

// Creates a new LocalDBRuntime with a pool of ports from minPort to maxPort (inclusive).
func NewLocalDBRuntime(minPort, maxPort int) *LocalDBRuntime {
	portPool := make(chan int, maxPort-minPort+1)
	for port := minPort; port <= maxPort; port++ {
		portPool <- port
	}

	return &LocalDBRuntime{
		portPool: portPool,
		minPort:  minPort,
		maxPort:  maxPort,
	}
}

// Formats an address for a local Redis DB (no protocol prefix)
func (l *LocalDBRuntime) formatAddress(port int) string {
	return fmt.Sprintf("localhost:%d", port)
}

// Finds existing local databases and returns a list of them
func (l *LocalDBRuntime) FindExistingDatabases() ([]*redis_db.RedisDB, error) {
	docker_client, err := client.New(client.FromEnv)
	if err != nil {
		return nil, err
	}
	defer docker_client.Close()

	res, err := docker_client.ContainerList(context.Background(), client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("name", CONTAINER_PREFIX),
	})
	if err != nil {
		return nil, err
	}

	var dbs []*redis_db.RedisDB
	for _, c := range res.Items {
		name := strings.TrimPrefix(c.Names[0], "/")

		id := strings.TrimPrefix(name, CONTAINER_PREFIX)

		address := ""
		if c.State == container.StateRunning {
			for _, p := range c.Ports {
				if p.PrivatePort == 6379 && p.PublicPort != 0 {
					address = l.formatAddress(int(p.PublicPort))
					break
				}
			}
		}

		dbs = append(dbs, redis_db.NewRedisDB(address, id))
	}

	return dbs, nil
}

// The name of a database's data volume
func (l *LocalDBRuntime) databaseVolumeName(databaseId string) string {
	return fmt.Sprintf("%s%s-data", CONTAINER_PREFIX, databaseId)
}

// The name of a database's container
func (l *LocalDBRuntime) databaseContainerName(databaseId string) string {
	return fmt.Sprintf("%s%s", CONTAINER_PREFIX, databaseId)
}

// Creates a new local database with the given ID and returns its address
func (l *LocalDBRuntime) CreateDatabase(databaseId string) (string, error) {
	new_port, err := l.acquirePort()
	if err != nil {
		return "", err
	}

	start_cmd := exec.Command("docker", "run", "-d",
		"--name", l.databaseContainerName(databaseId),
		"-p", fmt.Sprintf("127.0.0.1:%d:6379", new_port),
		"-v", fmt.Sprintf("%s:/data", l.databaseVolumeName(databaseId)),
		REDIS_IMAGE,
		// The database ID doubles as the password, so the AUTH the proxy forwards succeeds.
		"redis-server", "--requirepass", databaseId,
	)

	if err := start_cmd.Run(); err != nil {
		l.releasePort(new_port)
		return "", err
	}

	address := l.formatAddress(new_port)

	return address, nil
}

// Deletes a local database with the given ID
func (l *LocalDBRuntime) DeleteDatabase(databaseId string) error {
	remove_cmd := exec.Command("docker", "rm", "-f", l.databaseContainerName(databaseId))
	if err := remove_cmd.Run(); err != nil {
		return err
	}
	remove_vols_cmd := exec.Command("docker", "volume", "rm", l.databaseVolumeName(databaseId))
	if err := remove_vols_cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Pauses a local database with the given ID
func (l *LocalDBRuntime) PauseDatabase(databaseId string) error {
	pause_cmd := exec.Command("docker", "stop", l.databaseContainerName(databaseId))
	if err := pause_cmd.Run(); err != nil {
		return err
	}
	remove_cmd := exec.Command("docker", "rm", l.databaseContainerName(databaseId))
	if err := remove_cmd.Run(); err != nil {
		return err
	}
	return nil
}

// Resumes a local database with the given ID
func (l *LocalDBRuntime) ResumeDatabase(databaseId string) (string, error) {
	return l.CreateDatabase(databaseId)
}

// Acquires a free port to use for a new local database
func (l *LocalDBRuntime) acquirePort() (int, error) {
	nextPort := func() (int, error) {
		select {
		case port := <-l.portPool:
			return port, nil
		default:
			return 0, errors.New("no available ports in the pool")
		}
	}
	portOccupied := func(port int) bool {
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			return true
		}
		ln.Close()
		return false
	}

	port, err := nextPort()
	for err == nil && portOccupied(port) {
		log.Printf("Warning: Port %d is occupied, trying next port", port)
		port, err = nextPort()
	}

	if err != nil {
		return 0, err
	}

	return port, nil
}

// Releases a port back to the pool
func (l *LocalDBRuntime) releasePort(port int) {
	l.portPool <- port
}

var _ db_runtime.DBRuntime = (*LocalDBRuntime)(nil)
