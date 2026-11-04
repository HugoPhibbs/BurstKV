package redis_db
//go:build ignore

import (
	"sync/atomic"
	"time"
)

type RedisDBCentral struct {
	id          string
	lastRequest atomic.Int64 // thread-safe
}

func NewRedisDB(port int, id string) *RedisDBCentral {
	return &RedisDBCentral{
		id:      id,
	}
}

func (db *RedisDBCentral) Id() string {
	return db.id
}

func (db *RedisDBCentral) RecordActivity() {
	db.lastRequest.Store(time.Now().Unix())
}

func (db *RedisDBCentral) LastRequestTime() *time.Time {
	if db.lastRequest.Load() == 0 {
		return nil
	}
	t := time.Unix(db.lastRequest.Load(), 0)
	return &t
}