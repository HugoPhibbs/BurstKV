package db_runtime

import "burstkv/internal/redis_db"

type DBRuntime interface {
	CreateDatabase(databaseId string) (string, error)
	DeleteDatabase(databaseId string) error
	PauseDatabase(databaseId string) error
	ResumeDatabase(databaseId string) (string, error)
	FindExistingDatabases() ([]*redis_db.RedisDB, error)
}
