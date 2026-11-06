package database

import (
	"sync/atomic"
	"time"
)

type Database struct {
	id             string
	latestActivity atomic.Int64 // thread-safe
	workerID       string
}

func NewDatabase(id string, workerID string) *Database {
	return &Database{
		id:       id,
		workerID: workerID,
	}
}

func (db *Database) Id() string {
	return db.id
}

func (db *Database) RecordActivity() {
	db.latestActivity.Store(time.Now().Unix())
}

func (db *Database) LatestActivity() *time.Time {
	ts := db.latestActivity.Load()
	if ts == 0 {
		return nil
	}
	t := time.Unix(ts, 0)
	return &t
}

func (db *Database) RemoveWorker() {
	db.workerID = ""
}

func (db *Database) IsActiveAndIdle(idleLimit time.Duration) bool {
	if db.workerID == "" {
		return false
	}
	lastRequest := db.LatestActivity()
	if lastRequest == nil {
		return false
	}
	return time.Since(*lastRequest) > idleLimit
}

func (db *Database) AddWorker(workerID string) {
	db.workerID = workerID
}

func (db *Database) WorkerID() string {
	return db.workerID
}

func (db *Database) IsRunning() bool {
	return db.workerID != ""
}
