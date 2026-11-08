package controller

import (
	"burstkv/internal/controller/domain/controlled_worker"
	"burstkv/internal/controller/domain/database"
	"burstkv/internal/core/snapshot_storage"
	"burstkv/internal/utils"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
)

const DB_ID_LENGTH = 8

type Controller struct {
	idleLimit          time.Duration
	ReaperTimeInterval time.Duration
	workers            map[string]*controlled_worker.ControlledWorker // workerID -> Worker
	databases          map[string]*database.Database                  // databaseID -> Database
	snapShotstorage    *snapshot_storage.SnapshotStorage
}

// Creates a new DBManager with the given runtime and idle limit
func NewController(idleLimit, reaperInterval time.Duration, snapshotStorage *snapshot_storage.SnapshotStorage) *Controller {
	return &Controller{
		idleLimit:          idleLimit,
		ReaperTimeInterval: reaperInterval,
		workers:            make(map[string]*controlled_worker.ControlledWorker),
		databases:          make(map[string]*database.Database),
		snapShotstorage:    snapshotStorage,
	}
}

func (c *Controller) AddWorker(ctx context.Context, worker *controlled_worker.ControlledWorker) {
	c.workers[worker.WorkerID] = worker

	databaseIDs, err := worker.AssignedDatabases(ctx)
	if err != nil {
		fmt.Printf("Error getting assigned databases from worker %s: %v\n", worker.WorkerID, err)
		return
	}

	for _, id := range databaseIDs {
		database := database.NewDatabase(id, worker.WorkerID)
		c.databases[id] = database
		database.RecordActivity() // TODO update this from the worker
	}
}

// Returns the state of the database with the given ID.
func (c *Controller) GetDatabaseState(databaseID string) (database.DatabaseStatus, error) {
	foundDatabase, exists := c.databases[databaseID]
	if !exists {
		return database.NotFound, nil
	}
	if foundDatabase.IsRunning() {
		return database.Running, nil
	}
	return database.Paused, nil
}

// Returns a list of all database IDs managed by this DBManager.
func (c *Controller) GetAllDatabaseIDs() []string {
	return slices.Collect(maps.Keys(c.databases))
}

// Creates a new database and returns the new database ID
func (c *Controller) CreateDatabase(ctx context.Context) (string, error) {
	for workerID, worker := range c.workers {
		if worker.CanAssignDatabase(ctx) {
			new_database_id := utils.RandomString(DB_ID_LENGTH)
			err := worker.CreateDatabase(ctx, new_database_id)
			if err != nil {
				return "", err
			}
			new_database := database.NewDatabase(new_database_id, workerID)
			c.databases[new_database_id] = new_database
			return new_database_id, nil
		}
	}
	return "", ErrNoAvailableWorkers
}

// Deletes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist.
func (c *Controller) DeleteDatabase(ctx context.Context, databaseID string) error {
	if database, exists := c.databases[databaseID]; exists {
		isRunning := database.IsRunning()
		if isRunning {
			worker, ok := c.workers[database.WorkerID()]
			if !ok {
				panic("Invalid state: Database is assigned to a worker, yet the worker doesn't contain the database")
			}
			err := worker.DeleteDatabase(ctx, databaseID)
			if err != nil {
				return err
			}
		} else {
			c.snapShotstorage.DeleteSnapshot(ctx, databaseID)
		}

		delete(c.databases, databaseID)
		return nil
	}
	return ErrDatabaseNotFound
}

// Deletes all databases managed by this DBManager.
func (c *Controller) DeleteAllDatabases(ctx context.Context) error {
	for id := range c.databases {
		err := c.DeleteDatabase(ctx, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) workerForDatabase(databaseID string) *controlled_worker.ControlledWorker {
	database, exists := c.databases[databaseID]
	if !exists {
		return nil
	}
	return c.workers[database.WorkerID()]
}

// Pauses the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyPaused if the database is already paused.
func (c *Controller) PauseDatabase(ctx context.Context, databaseID string) error {
	database, exists := c.databases[databaseID]
	if !exists {
		return ErrDatabaseNotFound
	}
	if !database.IsRunning() {
		return ErrDatabaseAlreadyPaused
	}
	if err := c.workers[database.WorkerID()].PauseDatabase(ctx, databaseID); err != nil {
		return err
	}
	database.RemoveWorker()
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (c *Controller) ResumeDatabase(ctx context.Context, databaseID string) error {
	database, exists := c.databases[databaseID]
	if !exists {
		return ErrDatabaseNotFound
	}

	if database.IsRunning() {
		return ErrDatabaseAlreadyActive
	}

	worker := c.findAvailableWorker(ctx)
	if worker == nil {
		return ErrNoAvailableWorkers
	}
	err := worker.ResumeDatabase(ctx, databaseID)
	if err != nil {
		return err
	}
	database.AddWorker(worker.WorkerID)
	return nil
}

func (c *Controller) findAvailableWorker(ctx context.Context) *controlled_worker.ControlledWorker {
	for _, worker := range c.workers {
		if worker.CanAssignDatabase(ctx) {
			return worker
		}
	}
	return nil
}

func (c *Controller) reapIdleDatabases(ctx context.Context) {
	for _, db := range c.databases {
		if db.IsActiveAndIdle(c.idleLimit) {
			err := c.PauseDatabase(ctx, db.Id())
			if err != nil {
				fmt.Printf("Error pausing idle database %s: %v\n", db.Id(), err)
			}
		}
	}
}

func (c *Controller) StartIdleDatabaseReaper(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.ReaperTimeInterval)
		defer ticker.Stop()

		for range ticker.C {
			c.reapIdleDatabases(ctx)
		}
	}()
}

func (c *Controller) RouteDatabase(ctx context.Context, databaseID string) (string, error) {
    database, exists := c.databases[databaseID]
    if !exists {
        return "", ErrDatabaseNotFound
    }

    if !database.IsRunning() {
        if err := c.ResumeDatabase(ctx, databaseID); err != nil {
            return "", err
        }
    }

    worker, exists := c.workers[database.WorkerID()]
    if !exists {
        return "", fmt.Errorf("database %s linked to unknown worker %s", databaseID, database.WorkerID())
    }
    return worker.ProxyAddress, nil
}

func (c *Controller) RecordDatabaseActivity(databaseID string) error {
	database, exists := c.databases[databaseID]
	if !exists {
		return ErrDatabaseNotFound
	}
	database.RecordActivity()
	return nil
}

func (c *Controller) StopAllWorkers() error {
	return nil // Implement me
}