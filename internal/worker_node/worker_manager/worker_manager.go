package worker_manager

import (
	"burstkv/internal/utils"
	"burstkv/internal/worker_node"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
)

const redisImage = "redis:8.10.3"

type DatabaseContainer struct {
	containerID string
	port        int
}

type WorkerManager struct {
	ManagerID string
	containerPool chan *DatabaseContainer       // Containers not assigned to a database
	assigned      map[string]*DatabaseContainer // databaseID -> container
}

// Creates a WorkerManager and starts poolSize idle containers on ports minPort to minPort+poolSize-1.
func NewWorkerManager(managerID string, minPort int, poolSize int) (*WorkerManager, error) {
	wm := &WorkerManager{
		ManagerID: managerID,
		containerPool: make(chan *DatabaseContainer, poolSize),
		assigned:      make(map[string]*DatabaseContainer),
	}

	for port := minPort; port < minPort+poolSize; port++ {
		container, err := wm.createContainer(port)
		if err != nil {
			wm.Close()
			return nil, err
		}
		wm.containerPool <- container
	}

	return wm, nil
}

// Removes all containers, assigned or not.
func (wm *WorkerManager) Close() {
	for _, container := range wm.assigned {
		wm.deleteContainer(container)
	}
	for {
		select {
		case container := <-wm.containerPool:
			wm.deleteContainer(container)
		default:
			return
		}
	}
}

func (wm *WorkerManager) DatabaseState(databaseID string) (utils.DBState, error) {
	if _, ok := wm.assigned[databaseID]; ok {
		return utils.Active, nil
	}

	snapshotPath := wm.localSnapshotPath(databaseID)
	if _, err := os.Stat(snapshotPath); err == nil {
		return utils.Paused, nil
	} else if errors.Is(err, os.ErrNotExist) {
		return utils.NotFound, nil
	} else {
		return utils.Unknown, fmt.Errorf("checking snapshot existence: %w", err)
	}
}

func (wm *WorkerManager) CanAssignDatabase() bool {
	return len(wm.containerPool) > 0
}

func (wm *WorkerManager) AvailableContainerCount() int {
	return len(wm.containerPool)
}

func (wm *WorkerManager) AssignedDatabases() []string {
	return slices.Collect(maps.Keys(wm.assigned))
}

// Starts a new idle container on the given port.
func (wm *WorkerManager) createContainer(port int) (*DatabaseContainer, error) {
	container := &DatabaseContainer{containerID: utils.RandomString(8), port: port}

	startCmd := exec.Command("docker", "run", "-d",
		"--name", wm.containerName(container.containerID),
		"-p", fmt.Sprintf("127.0.0.1:%d:6379", port),
		redisImage,
		"sleep", "infinity", // Keep the container idle until a database is assigned
	)
	if err := startCmd.Run(); err != nil {
		return nil, fmt.Errorf("starting container on port %d: %w", port, err)
	}

	return container, nil
}

// Removes a container completely.
func (wm *WorkerManager) deleteContainer(container *DatabaseContainer) error {
	return exec.Command("docker", "rm", "-f", wm.containerName(container.containerID)).Run()
}

// Takes a container from the pool, loads the database's snapshot (if any) and starts Redis. Returns the address.
func (wm *WorkerManager) AssignDatabase(databaseID string) (string, error) {
	if _, exists := wm.assigned[databaseID]; exists {
		return "", worker_node.ErrDatabaseAlreadyAssigned
	}

	var container *DatabaseContainer
	select {
	case container = <-wm.containerPool:
	default:
		return "", worker_node.ErrNoAvailableContainers
	}

	if err := wm.startDatabase(databaseID, container); err != nil {
		releaseErr := wm.releaseContainer(databaseID, container)
		if releaseErr != nil {
			return "", fmt.Errorf("starting database and releasing container: %v, %v", err, releaseErr)
		}
		wm.containerPool <- container
		return "", err
	}

	wm.assigned[databaseID] = container
	return wm.formatAddress(container.port), nil
}

// Copies the database's snapshot into the container (if one exists) and starts Redis.
func (wm *WorkerManager) startDatabase(databaseID string, container *DatabaseContainer) error {
	name := wm.containerName(container.containerID)
	localSnapshotPath := wm.localSnapshotPath(databaseID)
	containerSnapshotPath := wm.containerSnapshotPath(container.containerID)

	if _, err := os.Stat(localSnapshotPath); err == nil {
		if err := exec.Command("docker", "cp", localSnapshotPath, containerSnapshotPath).Run(); err != nil {
			releaseErr := wm.releaseContainer(databaseID, container)
			if releaseErr != nil {
				return fmt.Errorf("copying snapshot and releasing container: %v, %v", err, releaseErr)
			}
			return err			
		}
	}

	return exec.Command("docker", "exec", "-d", name, "redis-server", "--requirepass", databaseID).Run()
}

// Saves the database's snapshot locally and returns its container to the pool.
func (wm *WorkerManager) PauseDatabase(databaseID string) error {
	container, ok := wm.assigned[databaseID]
	if !ok {
		return worker_node.ErrDatabaseNotAssigned
	}

	name := wm.containerName(container.containerID)
	if err := exec.Command("docker", "exec", name, "redis-cli", "-a", databaseID, "SAVE").Run(); err != nil {
		return err
	}
	if err := exec.Command("docker", "cp", name+":/data/dump.rdb", wm.localSnapshotPath(databaseID)).Run(); err != nil {
		return err
	}

	return wm.releaseContainer(databaseID, container)
}

// Deletes the database's snapshot and, if it is running, returns its container to the pool.
func (wm *WorkerManager) DeleteDatabase(databaseID string) error {
	if container, ok := wm.assigned[databaseID]; ok {
		if err := wm.releaseContainer(databaseID, container); err != nil {
			return err
		}
	}

	if err := os.Remove(wm.localSnapshotPath(databaseID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Deletes the database's container and puts a fresh one on the same port into the pool.
func (wm *WorkerManager) releaseContainer(databaseID string, container *DatabaseContainer) error {
	if err := wm.deleteContainer(container); err != nil {
		return err
	}
	delete(wm.assigned, databaseID)

	newContainer, err := wm.createContainer(container.port)
	if err != nil {
		return err
	}
	wm.containerPool <- newContainer
	return nil
}

func (wm *WorkerManager) DatabaseAddress(databaseID string) (string, error) {
	container, ok := wm.assigned[databaseID]
	if !ok {
		return "", worker_node.ErrDatabaseNotAssigned
	} 
	return wm.formatAddress(container.port), nil
}

// Formats an address for a local Redis DB (no protocol prefix)
func (wm *WorkerManager) formatAddress(port int) string {
	return fmt.Sprintf("localhost:%d", port)
}

func (wm *WorkerManager) containerName(containerID string) string {
	return fmt.Sprintf("burstkv-%s-%s", wm.ManagerID, containerID)
}

func (wm *WorkerManager) localSnapshotPath(databaseID string) string {
	return fmt.Sprintf("./snapshots/%s.rdb", databaseID)
}

func (wm *WorkerManager) containerSnapshotPath(containerID string) string {
	return fmt.Sprintf("%s:/data/dump.rdb", wm.containerName(containerID))
}
// Implements redis_proxy.Router: returns the address of a running database.
func (wm *WorkerManager) RouteDatabase(databaseID string) (string, error) {
	return wm.DatabaseAddress(databaseID)
}

// Implements redis_proxy.Router.
func (wm *WorkerManager) RecordDatabaseActivity(databaseID string) {}
