package db_manager
//go:build ignore

import (
	"burstkv/internal/core/redis_db"
	"burstkv/internal/db_runtime"
	"burstkv/internal/utils"
	"burstkv/internal/worker_node/worker_manager"
	"fmt"
	"maps"
	"slices"
	"time"
)

const DB_ID_LENGTH = 8
const DEFAULT_REAPER_TIME_INTERVAL = 1 * time.Minute

type RedisDBCentral struct {
	id          string
	lastRequest atomic.Int64 // thread-safe
}

type 

type CentralManager struct {
	idleLimit           time.Duration
	ReaperTimeInterval  time.Duration
	workerManagers      map[string]*worker_manager.WorkerManager // managerID -> WorkerManager
	workerAddresses     map[string]string                        // managerID -> address
	databaseIDToManagerID map[string]string                        // databaseID -> managerID
}

// Creates a new DBManager with the given runtime and idle limit
func NewCentralManager(idleLimit time.Duration) *CentralManager {
	return &CentralManager{
		idleLimit:          idleLimit,
		ReaperTimeInterval: DEFAULT_REAPER_TIME_INTERVAL,
	}
}

func (m *CentralManager) AddWorkerManager(workerManager *worker_manager.WorkerManager, address string) {
	m.workerManagers[workerManager.ManagerID] = workerManager
	m.workerAddresses[workerManager.ManagerID] = address

	for _, db := range workerManager.AssignedDatabases() {
		m.databaseIDToManagerID[db] = workerManager.ManagerID
	}
}

// func (m *CentralManager) DatabaseAddress(databaseId string) (string, error) {
// 	status := m.GetDatabaseState(databaseId)
// 	if status == NotFound {
// 		return "", ErrDatabaseNotFound
// 	}
// 	if status == Paused {
// 		return "", ErrDatabasePaused
// 	}

// 	database, _ := m.databases[databaseId]
// 	return database.Address(), nil
// }

// Returns the state of the database with the given ID.
func (m *CentralManager) GetDatabaseState(databaseId string) (utils.DBState, error) {
	if database, exists := m.databaseIDToManagerID[databaseId]; exists {
		workerManager := m.workerManagers[database]
		state, err := workerManager.DatabaseState(databaseId)
		if err != nil {
			return utils.Unknown, fmt.Errorf("getting database state from worker manager: %w", err)
		}
		return state, nil

	}
	return utils.NotFound, nil
}

// func (m *CentralManager) GetDatabase(databaseId string) (*redis_db.RedisDB, error) {
// 	database, exists := m.databases[databaseId]
// 	if !exists {
// 		return nil, ErrDatabaseNotFound
// 	}
// 	return database, nil
// }

// Returns a list of all database IDs managed by this DBManager.
func (m *CentralManager) GetAllDatabaseIDs() []string {
	return slices.Collect(maps.Keys(m.databaseIDToManagerID))
}

// Creates a new database and returns the corresponding RedisDB object.
func (m *CentralManager) CreateDatabase() (string, error) {
	for managerID, workerManager := range m.workerManagers {
		if workerManager.CanAssignDatabase() {
			new_database_id := utils.RandomString(DB_ID_LENGTH)
			workerManager.AssignDatabase(new_database_id)
			m.databaseIDToManagerID[new_database_id] = managerID
			return new_database_id, nil
		}
	}
	return "", fmt.Errorf("no worker manager available to create database")
}

// Deletes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist.
func (m *CentralManager) DeleteDatabase(databaseId string) error {
	if managerID, exists := m.databaseIDToManagerID[databaseId]; exists {
		workerManager := m.workerManagers[managerID]
		err := workerManager.DeleteDatabase(databaseId)
		if err != nil {
			return err
		}
		delete(m.databaseIDToManagerID, databaseId)
		return nil
	}
	return ErrDatabaseNotFound
}

func (m *CentralManager) DeleteAllDatabases() error {
	for id := range m.databaseIDToManagerID {
		err := m.DeleteDatabase(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *CentralManager) RecordDatabaseActivity(databaseId string) error {
	_, exists := m.databaseIDToManagerID[databaseId]
	if !exists {
		return ErrDatabaseNotFound
	}
	database.RecordActivity()
	return nil
}

// Pauses the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyPaused if the database is already paused.
func (m *CentralManager) PauseDatabase(databaseId string) error {
	dbState := m.GetDatabaseState(databaseId)
	if dbState == NotFound {
		return ErrDatabaseNotFound
	}
	if dbState == Paused {
		return ErrDatabaseAlreadyPaused
	}

	err := m.runtime.PauseDatabase(databaseId)
	if err != nil {
		return err
	}

	database := m.databases[databaseId]
	database.Pause()
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (m *CentralManager) ResumeDatabase(databaseId string) (string, error) {
	dbState := m.GetDatabaseState(databaseId)
	if dbState == NotFound {
		return "", ErrDatabaseNotFound
	}
	if dbState == Active {
		return "", ErrDatabaseAlreadyActive
	}

	address, err := m.runtime.ResumeDatabase(databaseId)
	if err != nil {
		return "", err
	}

	database := m.databases[databaseId]
	database.Resume(address)
	return address, nil
}

func (m *CentralManager) reapIdleDatabases() {
	for _, db := range m.databases {
		if db.IsActiveAndIdle(m.idleLimit) {
			err := m.PauseDatabase(db.Id())
			if err != nil {
				fmt.Printf("Error pausing idle database %s: %v\n", db.Id(), err)
			}
		}
	}
}

func (m *CentralManager) StartIdleDatabaseReaper() {
	go func() {
		ticker := time.NewTicker(m.ReaperTimeInterval)
		defer ticker.Stop()

		for range ticker.C {
			m.reapIdleDatabases()
		}
	}()
}
