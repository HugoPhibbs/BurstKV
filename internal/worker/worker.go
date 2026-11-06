package worker

import (
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"maps"
	"os/exec"
	"slices"
)

var REDIS_IMAGE = "redis:8.10.3"


type DatabaseContainer struct {
	containerID string
	port        int
}

type Worker struct {
	WorkerID        string
	containerPool   chan *DatabaseContainer       // Containers not assigned to a database
	assigned        map[string]*DatabaseContainer // databaseID -> container
	snapshotStorage *snapshot_storage.SnapshotStorage
}

// Creates a Worker and starts poolSize idle containers on ports minPort to minPort+poolSize-1.
func NewWorker(workerID string, minPort int, poolSize int, snapshotStorage *snapshot_storage.SnapshotStorage) (*Worker, error) {
	wm := &Worker{
		WorkerID:        workerID,
		containerPool:   make(chan *DatabaseContainer, poolSize),
		assigned:        make(map[string]*DatabaseContainer),
		snapshotStorage: snapshotStorage,
	}

	for port := minPort; port < minPort+poolSize; port++ {
		container, err := wm.createContainer(port)
		if err != nil {
			wm.StopAllContainers()
			return nil, err
		}
		wm.containerPool <- container
	}

	return wm, nil
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
		REDIS_IMAGE,
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
		snapshotExists, err := w.snapshotStorage.SnapshotExists(context.Background(), databaseID)
		if err != nil {
			return err
		}
		if !snapshotExists {
			return snapshot_storage.ErrSnapshotNotFound
		}

		if err := w.loadSnapshot(context.Background(), databaseID, container.containerID); err != nil {
			return err
		}
	}

	return exec.Command("docker", "exec", "-d", name, "redis-server", "--requirepass", databaseID).Run()
}

func (w *Worker) loadSnapshot(ctx context.Context, databaseID string, containerID string) error {
	cmd := exec.CommandContext(ctx, "docker", "cp", "-", w.containerName(containerID)+":/data")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := w.snapshotStorage.LoadSnapshot(ctx, databaseID, stdin); err != nil {
		stdin.Close()
		cmd.Process.Kill()
		cmd.Wait()
		return err
	}
	stdin.Close() // signals EOF so docker cp finishes
	return cmd.Wait()
}

func (w *Worker) saveSnapshot(ctx context.Context, databaseID string, containerID string) error {
	if err := exec.CommandContext(ctx, "docker", "exec", w.containerName(containerID), "redis-cli", "-a", databaseID, "SAVE").Run(); err != nil {
		return err
	}

	cmd := exec.CommandContext(ctx, "docker", "cp", w.containerSnapshotPath(containerID), "-")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := w.snapshotStorage.SaveSnapshot(ctx, databaseID, stdout); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		return err
	}
	return cmd.Wait()
}

// Saves the database's snapshot locally and returns its container to the pool.
func (w *Worker) PauseDatabase(ctx context.Context, databaseID string) error {
	container, ok := w.assigned[databaseID]
	if !ok {
		return ErrDatabaseNotAssigned
	}

	if err := w.saveSnapshot(ctx, databaseID, container.containerID); err != nil {
		return err
	}

	return w.releaseContainer(databaseID, container)
}

// Deletes the database's snapshot and, if it is running, returns its container to the pool.
func (w *Worker) DeleteDatabase(ctx context.Context, databaseID string) error {
	if container, ok := w.assigned[databaseID]; ok {
		if err := w.releaseContainer(databaseID, container); err != nil {
			return err
		}
	}

	deleted, err := w.snapshotStorage.DeleteSnapshot(ctx, databaseID)
	if err != nil {
		return err
	}
	if !deleted {
		println(fmt.Sprintf("Warning: snapshot for database %s not deleted", databaseID))
	}

	return nil
}


// Removes all containers, assigned or not.
func (w *Worker) StopAllContainers() (bool, error) {
	success := true
	for databaseID, container := range w.assigned {
		ctx := context.Background()
		if err := w.saveSnapshot(ctx, databaseID, container.containerID); err != nil {
			fmt.Printf("Failed to save snapshot for database %s: %v\n", databaseID, err)
			success = false
		}
		w.deleteContainer(container)
		delete(w.assigned, databaseID)
	}
	for {
		select {
		case container := <-w.containerPool:
			w.deleteContainer(container)
		default:
			return success, nil
		}
	}
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

func (w *Worker) containerSnapshotPath(containerID string) string {
	return fmt.Sprintf("%s:/data/dump.rdb", w.containerName(containerID))
}

// Implements redis_proxy.Router: returns the address of a running database.
func (w *Worker) RouteDatabase(databaseID string) (string, error) {
	return w.DatabaseAddress(databaseID)
}

// Implements redis_proxy.Router.
func (w *Worker) RecordDatabaseActivity(databaseID string) {}
