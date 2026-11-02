package db_manager

import (
	"burstkv/internal/db_runtime"
	"burstkv/internal/redis_db"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"time"
)

const DB_ID_LENGTH = 8

type DBState int

const (
	Active DBState = iota
	Paused
	NotFound
)

func (s DBState) String() string {
	switch s {
	case Active:
		return "active"
	case Paused:
		return "paused"
	case NotFound:
		return "non-found"
	default:
		panic(fmt.Sprintf("unknown DBState: %d", s))
	}
}

type DBManager struct {
	runtime   db_runtime.DBRuntime
	databases map[string]*redis_db.RedisDB
	idleLimit time.Duration
}

// Creates a new DBManager with the given runtime and idle limit
func NewDBManager(runtime db_runtime.DBRuntime, idleLimit time.Duration) *DBManager {
	preexisting_dbs, err := runtime.FindExistingDatabases()
	if err != nil {
		panic(err)
	}

	databases := make(map[string]*redis_db.RedisDB)
	for _, db := range preexisting_dbs {
		databases[db.Id()] = db
	}

	return &DBManager{
		runtime:   runtime,
		databases: databases,
		idleLimit: idleLimit,
	}
}

// Generates a random string of length n from lower/uppercase letters and digits. 
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, n)
	for i := range n {
		result[i] = letters[rand.IntN(len(letters))]
	}
	return string(result)
}

// Returns the state of the database with the given ID.
func (m *DBManager) GetDatabaseState(databaseId string) DBState {
	if database, exists := m.databases[databaseId]; exists {
		if database.IsActive() {
			return Active
		} else {
			return Paused
		}
	}
	return NotFound
}

// Returns a list of all database IDs managed by this DBManager.
func (m *DBManager) GetAllDatabaseIDs() []string {
	return slices.Collect(maps.Keys(m.databases))
}

// Creates a new database and returns the corresponding RedisDB object.
func (m *DBManager) CreateDatabase() (*redis_db.RedisDB, error) {
	new_database_id := randomString(DB_ID_LENGTH)

	url, err := m.runtime.CreateDatabase(new_database_id)
	if err != nil {
		return nil, err
	}

	new_db := redis_db.NewRedisDB(url, new_database_id)
	m.databases[new_database_id] = new_db
	return new_db, nil
}

// Deletes the database with the given ID. 
// Returns ErrDatabaseNotFound if the database does not exist.
func (m *DBManager) DeleteDatabase(databaseId string) error {
	dbState := m.GetDatabaseState(databaseId)
	if dbState == NotFound {
		return ErrDatabaseNotFound
	}

	_, err := m.runtime.DeleteDatabase(databaseId)
	if err != nil {
		return err
	}
	delete(m.databases, databaseId)
	return nil
}

// Pauses the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyPaused if the database is already paused.
func (m *DBManager) PauseDatabase(databaseId string) error {
	dbState := m.GetDatabaseState(databaseId)
	if dbState == NotFound {
		return ErrDatabaseNotFound
	}
	if dbState == Paused {
		return ErrDatabaseAlreadyPaused
	}

	_, err := m.runtime.PauseDatabase(databaseId)
	if err != nil {
		return err
	}

	database := m.databases[databaseId]
	database.Pause()
	return nil
}

// Resumes the database with the given ID.
// Returns ErrDatabaseNotFound if the database does not exist or ErrDatabaseAlreadyActive if the database is already active.
func (m *DBManager) ResumeDatabase(databaseId string) (string, error) {
	dbState := m.GetDatabaseState(databaseId)
	if dbState == NotFound {
		return "", ErrDatabaseNotFound
	}
	if dbState == Active {
		return "", ErrDatabaseAlreadyActive
	}

	url, err := m.runtime.ResumeDatabase(databaseId)
	if err != nil {
		return "", err
	}

	database := m.databases[databaseId]
	database.Resume(url)
	return url, nil
}
