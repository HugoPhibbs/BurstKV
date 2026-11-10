package database

import (
	"burstkv/internal/controller/pod"
	"sync/atomic"
	"time"
)

type Database struct {
	id             string
	Pod            *pod.Pod
	latestActivity atomic.Int64 // thread-safe
}

func NewDatabase(id string) *Database {
	return &Database{
		id: id,
	}
}

func (db *Database) ID() string {
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

func (db *Database) IsActiveAndIdle(idleLimit time.Duration) bool {
	lastRequest := db.LatestActivity()
	if lastRequest == nil {
		return false
	}
	return time.Since(*lastRequest) > idleLimit
}

func (db *Database) IsRunning() bool {
	return db.Pod != nil
}
