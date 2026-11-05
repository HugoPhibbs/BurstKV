package worker

import (
	"burstkv/internal/utils"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"slices"
)

var redisImage = "redis:8.10.3"

type DatabaseContainer struct {
	containerID string
	port        int
}

type Worker struct {
	WorkerID     string
	containerPool chan *DatabaseContainer       // Containers not assigned to a database
	assigned      map[string]*DatabaseContainer // databaseID -> container
}

// Creates a Worker and starts poolSize idle containers on ports minPort to minPort+poolSize-1.
func NewWorker(workerID string, minPort int, poolSize int) (*Worker, error) {
	wm := &Worker{
		WorkerID:     workerID,
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
func (w *Worker) Close() {
	for _, container := range w.assigned {
		w.deleteContainer(container)
	}
	for {
		select {
		case container := <-w.containerPool:
			w.deleteContainer(container)
		default:
			return
		}
	}
}

func (w *Worker) CanAssignDatabase() bool {
	return len(w.containerPool) > 0
}

func (w *Worker) AvailableContainerCount() int {
	return len(w.containerPool)
}

func (w *Worker) AssignedDatabases() []string {
	return slices.Collect(maps.Keys(w.assigned))
}

func (w *Worker) IsAssigned(databaseID string) bool {
	_, assigned := w.assigned[databaseID]
	return assigned
}

// Starts a new idle container on the given port.
func (w *Worker) createContainer(port int) (*DatabaseContainer, error) {
	container := &DatabaseContainer{containerID: utils.RandomString(8), port: port}

	startCmd := exec.Command("docker", "run", "-d",
		"--name", w.containerName(container.containerID),
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
func (w *Worker) deleteContainer(container *DatabaseContainer) error {
	return exec.Command("docker", "rm", "-f", w.containerName(container.containerID)).Run()
}

// Takes a container from the pool, loads the database's snapshot (if any) and starts Redis. Returns the address.
func (w *Worker) assignDatabase(databaseID string, createNew bool) error {
	if _, exists := w.assigned[databaseID]; exists {
		return ErrDatabaseAlreadyAssigned
	}

	var container *DatabaseContainer
	select {
	case container = <-w.containerPool:
	default:
		return ErrNoAvailableContainers
	}

	if err := w.startDatabase(databaseID, container, createNew); err != nil {
		releaseErr := w.releaseContainer(databaseID, container)
		if releaseErr != nil {
			return fmt.Errorf("starting database and releasing container: %v, %v", err, releaseErr)
		}
		w.containerPool <- container
		return err
	}

	w.assigned[databaseID] = container
	return nil
}

func (w *Worker) CreateDatabase(databaseID string) error {
	return w.assignDatabase(databaseID, true)
}

func (w *Worker) ResumeDatabase(databaseID string) error {
	return w.assignDatabase(databaseID, false)
}

// Copies the database's snapshot into the container (if one exists) and starts Redis.
func (w *Worker) startDatabase(databaseID string, container *DatabaseContainer, createNew bool) error {
	name := w.containerName(container.containerID)

	if !createNew {
		localSnapshotPath := w.localSnapshotPath(databaseID)
		containerSnapshotPath := w.containerSnapshotPath(container.containerID)

		if _, err := os.Stat(localSnapshotPath); err == nil {
			if err := exec.Command("docker", "cp", localSnapshotPath, containerSnapshotPath).Run(); err != nil {
				releaseErr := w.releaseContainer(databaseID, container)
				if releaseErr != nil {
					return fmt.Errorf("copying snapshot and releasing container: %v, %v", err, releaseErr)
				}
				return err
			}
		} else {
			return ErrSnapshotNotFound
		}
	}

	return exec.Command("docker", "exec", "-d", name, "redis-server", "--requirepass", databaseID).Run()
}

// Saves the database's snapshot locally and returns its container to the pool.
func (w *Worker) PauseDatabase(databaseID string) error {
	container, ok := w.assigned[databaseID]
	if !ok {
		return ErrDatabaseNotAssigned
	}

	name := w.containerName(container.containerID)
	if err := exec.Command("docker", "exec", name, "redis-cli", "-a", databaseID, "SAVE").Run(); err != nil {
		return err
	}
	if err := exec.Command("docker", "cp", name+":/data/dump.rdb", w.localSnapshotPath(databaseID)).Run(); err != nil {
		return err
	}

	return w.releaseContainer(databaseID, container)
}

// Deletes the database's snapshot and, if it is running, returns its container to the pool.
func (w *Worker) DeleteDatabase(databaseID string) error {
	if container, ok := w.assigned[databaseID]; ok {
		if err := w.releaseContainer(databaseID, container); err != nil {
			return err
		}
	}

	if err := os.Remove(w.localSnapshotPath(databaseID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Deletes the database's container and puts a fresh one on the same port into the pool.
func (w *Worker) releaseContainer(databaseID string, container *DatabaseContainer) error {
	if err := w.deleteContainer(container); err != nil {
		return err
	}
	delete(w.assigned, databaseID)

	newContainer, err := w.createContainer(container.port)
	if err != nil {
		return err
	}
	w.containerPool <- newContainer
	return nil
}

func (w *Worker) DatabaseAddress(databaseID string) (string, error) {
	container, ok := w.assigned[databaseID]
	if !ok {
		return "", ErrDatabaseNotAssigned
	}
	return w.formatAddress(container.port), nil
}

// Formats an address for a local Redis DB (no protocol prefix)
func (w *Worker) formatAddress(port int) string {
	return fmt.Sprintf("localhost:%d", port)
}

func (w *Worker) containerName(containerID string) string {
	return fmt.Sprintf("burstkv-%s-%s", w.WorkerID, containerID)
}

func (w *Worker) localSnapshotPath(databaseID string) string {
	return fmt.Sprintf("./snapshots/%s.rdb", databaseID)
}

func (w *Worker) containerSnapshotPath(containerID string) string {
	return fmt.Sprintf("%s:/data/dump.rdb", w.containerName(containerID))
}

// Implements redis_proxy.Router: returns the address of a running database.
func (w *Worker) RouteDatabase(databaseID string) (string, error) {
	return w.DatabaseAddress(databaseID)
}

// Implements redis_proxy.Router.
func (w *Worker) RecordDatabaseActivity(databaseID string) {}
