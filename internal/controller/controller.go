package controller

import (
	"burstkv/internal/controller/domain/database"
	"burstkv/internal/controller/domain/controlled_worker"
	"burstkv/internal/utils"
	"fmt"
	"maps"
	"slices"
	"time"
)

const DB_ID_LENGTH = 8
const DEFAULT_REAPER_TIME_INTERVAL = 1 * time.Minute

type Controller struct {
	idleLimit          time.Duration
	ReaperTimeInterval time.Duration
	workers            map[string]*controlled_worker.ControlledWorker   // workerID -> Worker
	databases          map[string]*database.Database // databaseID -> Database
}

// Creates a new DBManager with the given runtime and idle limit
func NewController(idleLimit time.Duration) *Controller {
	return &Controller{
		idleLimit:          idleLimit,
		ReaperTimeInterval: DEFAULT_REAPER_TIME_INTERVAL,
		workers:            make(map[string]*controlled_worker.ControlledWorker),
		databases:          make(map[string]*database.Database),
	}
}

func (c *Controller) AddWorker(worker *controlled_worker.ControlledWorker) {
	c.workers[worker.WorkerID] = worker

	databaseIDs, err := worker.AssignedDatabases()
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
func (c *Controller) GetDatabaseState(databaseId string) (database.DatabaseStatus, error) {
	return database.NotFound, nil
}

// Returns a list of all database IDs managed by this DBManager.
func (c *Controller) GetAllDatabaseIDs() []string {
	return slices.Collect(maps.Keys(c.databases))
}

// Creates a new database and returns the new database ID
func (c *Controller) CreateDatabase() (string, error) {
	for workerID, worker := range c.workers {
		if worker.CanAssignDatabase() {
			new_database_id := utils.RandomString(DB_ID_LENGTH)
			err := worker.CreateDatabase(new_database_id)
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
func (c *Controller) DeleteDatabase(databaseId string) error {
	if database, exists := c.databases[databaseId]; exists {
		worker := c.workers[database.WorkerID()]
		err := worker.DeleteDatabase(databaseId)
		if err != nil {
			return err
		}
		delete(c.databases, databaseId)
		return nil
	}
	return ErrDatabaseNotFound
}

// Deletes all databases managed by this DBManager.
func (c *Controller) DeleteAllDatabases() error {
	for id := range c.databases {
		err := c.DeleteDatabase(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) RecordDatabaseActivity(databaseId string) error {
	database, exists := c.databases[databaseId]
	if !exists {
		return ErrDatabaseNotFound
	}
	database.RecordActivity()
	return nil
}

func (c *Controller) workerForDatabase(databaseId string) *controlled_worker.ControlledWorker {
	database, exists := c.databases[databaseId]
	if !exists {
		return nil
	}
	return c.workers[database.WorkerID()]
}

// Pauses the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyPaused if the database is already paused.
func (c *Controller) PauseDatabase(databaseId string) error {
	database, exists := c.databases[databaseId]
	if !exists {
		return ErrDatabaseNotFound
	}
	if !database.IsActive() {
		return ErrDatabaseAlreadyPaused
	}
	database.RemoveWorker()
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (c *Controller) ResumeDatabase(databaseId string) error {
	database, exists := c.databases[databaseId]
	if !exists {
		return ErrDatabaseNotFound
	}

	if database.IsActive() {
		return ErrDatabaseAlreadyActive
	}

	worker := c.findAvailableWorker()
	if worker == nil {
		return ErrNoAvailableWorkers
	}
	err := worker.ResumeDatabase(databaseId)
	if err != nil {
		return err
	}
	database.AddWorker(worker.WorkerID)
	return nil
}

func (c *Controller) findAvailableWorker() *controlled_worker.ControlledWorker {
	for _, worker := range c.workers {
		if worker.CanAssignDatabase() {
			return worker
		}
	}
	return nil
}

func (c *Controller) reapIdleDatabases() {
	for _, db := range c.databases {
		if db.IsActiveAndIdle(c.idleLimit) {
			err := c.PauseDatabase(db.Id())
			if err != nil {
				fmt.Printf("Error pausing idle database %s: %v\n", db.Id(), err)
			}
		}
	}
}

func (c *Controller) StartIdleDatabaseReaper() {
	go func() {
		ticker := time.NewTicker(c.ReaperTimeInterval)
		defer ticker.Stop()

		for range ticker.C {
			c.reapIdleDatabases()
		}
	}()
}
