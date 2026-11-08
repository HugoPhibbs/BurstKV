package worker

import (
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"os/exec"
	"github.com/puzpuzpuz/xsync/v4"
	"golang.org/x/sync/errgroup"
)

var REDIS_IMAGE = "redis:8.10.3"


type DatabaseContainer struct {
	containerID string
	port        int
}

type Worker struct {
	WorkerID        string
	containerPool   chan *DatabaseContainer       // Containers not assigned to a database
	assigned        *xsync.Map[string, *DatabaseContainer]
	snapshotStorage *snapshot_storage.SnapshotStorage
}

// Creates a Worker and starts poolSize idle containers on ports minPort to minPort+poolSize-1.
func NewWorker(ctx context.Context, workerID string, minPort int, poolSize int, snapshotStorage *snapshot_storage.SnapshotStorage) (*Worker, error) {
	w := &Worker{
		WorkerID:        workerID,
		containerPool:   make(chan *DatabaseContainer, poolSize),
		assigned:        xsync.NewMap[string, *DatabaseContainer](),
		snapshotStorage: snapshotStorage,
	}

	var eg errgroup.Group

	for port := minPort; port < minPort+poolSize; port++ {
		eg.Go(func() error {
			if _, err := w.createContainer(port); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		w.StopAllContainers(ctx)
		return nil, err
	}

	return w, nil
}

func (w *Worker) CanAssignDatabase() bool {
	return len(w.containerPool) > 0
}

func (w *Worker) AvailableContainerCount() int {
	return len(w.containerPool)
}

func (w *Worker) AssignedDatabases() []string {
    ids := make([]string, 0, w.assigned.Size())
    for id := range w.assigned.All() {
        ids = append(ids, id)
    }
    return ids
}

func (w *Worker) IsAssigned(databaseID string) bool {
	_, exists := w.assigned.Load(databaseID)
	return exists;
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

	w.containerPool <- container

	return container, nil
}

// Removes a container completely.
func (w *Worker) deleteContainer(container *DatabaseContainer) error {
	if err := exec.Command("docker", "rm", "-f", w.containerName(container.containerID)).Run(); err != nil {
		return err
	}
	return nil
}

// Takes a container from the pool, loads the database's snapshot (if any) and starts Redis. Returns the address.
func (w *Worker) assignDatabase(ctx context.Context, databaseID string, createNew bool) error {
	if w.IsAssigned(databaseID) {
		return ErrDatabaseAlreadyAssigned
	}

	var container *DatabaseContainer
	select {
	case container = <-w.containerPool:
	default:
		return ErrNoAvailableContainers
	}

	if err := w.startDatabase(ctx, databaseID, container, createNew); err != nil {
		releaseErr := w.releaseContainer(databaseID, container)
		if releaseErr != nil {
			return fmt.Errorf("starting database and releasing container: %v, %v", err, releaseErr)
		}
		return err
	}

	w.assigned.Store(databaseID, container)
	return nil
}

func (w *Worker) CreateDatabase(ctx context.Context, databaseID string) error {
	return w.assignDatabase(ctx, databaseID, true)
}

func (w *Worker) ResumeDatabase(ctx context.Context, databaseID string) error {
	return w.assignDatabase(ctx, databaseID, false)
}

// Copies the database's snapshot into the container (if one exists) and starts Redis.
func (w *Worker) startDatabase(ctx context.Context, databaseID string, container *DatabaseContainer, createNew bool) error {
	name := w.containerName(container.containerID)

	if !createNew {
		snapshotExists, err := w.snapshotStorage.SnapshotExists(ctx, databaseID)
		if err != nil {
			return err
		}
		if !snapshotExists {
			return snapshot_storage.ErrSnapshotNotFound
		}

		if err := w.loadSnapshot(ctx, databaseID, container.containerID); err != nil {
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
	container, exists := w.assigned.Load(databaseID)
	if !exists {
		return ErrDatabaseNotAssigned
	}

	if err := w.saveSnapshot(ctx, databaseID, container.containerID); err != nil {
		return err
	}

	return w.releaseContainer(databaseID, container)
}

// Deletes the database's snapshot and, if it is running, returns its container to the pool.
func (w *Worker) DeleteDatabase(ctx context.Context, databaseID string) error {
	container, exists := w.assigned.Load(databaseID)
	if exists {
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
func (w *Worker) StopAllContainers(ctx context.Context) error {
	var eg errgroup.Group
	
	// Delete assigned containers
	for databaseID, container := range w.assigned.All() {
		eg.Go(func() error {
			if err := w.saveSnapshot(ctx, databaseID, container.containerID); err != nil {
				fmt.Printf("Failed to save snapshot for database %s: %v\n", databaseID, err)
				return err
			}
			if err := w.deleteContainer(container); err != nil {
				return err
			}
			w.assigned.Delete(databaseID)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	// Delete un-assigned containers
	for {
		select {
		case container := <-w.containerPool:
			if err := w.deleteContainer(container); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// Deletes the database's container creates a fresh one
func (w *Worker) releaseContainer(databaseID string, container *DatabaseContainer) error {
	if err := w.deleteContainer(container); err != nil {
		return err
	}
	w.assigned.Delete(databaseID)

	_, err := w.createContainer(container.port)
	return err
}

func (w *Worker) DatabaseAddress(databaseID string) (string, error) {
	container, exists := w.assigned.Load(databaseID)
	if !exists {
		return "", ErrDatabaseNotAssigned
	}
	return w.formatAddress(container.port), nil
}

// Formats an address for a local Redis DB (no protocol prefix)
func (w *Worker) formatAddress(port int) string {
	return fmt.Sprintf("localhost:%d", port)
}

func (w *Worker) containerName(containerID string) string {
	return fmt.Sprintf("burstkv-worker-%s-%s", w.WorkerID, containerID)
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
