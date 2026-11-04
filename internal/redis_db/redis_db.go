package redis_db

import (
	"sync/atomic"
	"time"
)

type RedisDB struct {
	id          string
	lastRequest atomic.Int64 // thread-safe
	address     string
}

func NewRedisDB(address string, id string) *RedisDB {
	return &RedisDB{
		address: address,
		id:      id,
	}
}

func (db *RedisDB) Address() string {
	return db.address
}

func (db *RedisDB) Id() string {
	return db.id
}

func (db *RedisDB) Resume(address string) {
	db.address = address
}

func (db *RedisDB) Pause() {
	db.address = ""
}

func (db *RedisDB) IsActive() bool {
	return db.address != ""
}

func (db *RedisDB) RecordActivity() {
	db.lastRequest.Store(time.Now().Unix())
}

func (db *RedisDB) LastRequestTime() *time.Time {
	if db.lastRequest.Load() == 0 {
		return nil
	}
	t := time.Unix(db.lastRequest.Load(), 0)
	return &t
}

func (db *RedisDB) IsActiveAndIdle(idleLimit time.Duration) bool {
	lastRequest := db.LastRequestTime()
	if lastRequest == nil {
		return false
	}
	return db.IsActive() && time.Since(*lastRequest) > idleLimit
}
