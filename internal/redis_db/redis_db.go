package redis_db

import "time"

type RedisDB struct {
	id          string
	lastRequest time.Time
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

func (db *RedisDB) UpdateLastRequest() {
	db.lastRequest = time.Now()
}

func (db *RedisDB) LastRequest() time.Time {
	return db.lastRequest
}
