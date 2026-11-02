package redis_db

import "time"

type RedisDB struct {
	id string
	lastRequest time.Time
	url string
}

func NewRedisDB(url string, id string) *RedisDB {
	return &RedisDB{
		url: url,
		id: id,
	}
}

func (db *RedisDB) Url() string {
	return db.url
}

func (db *RedisDB) Id() string {
	return db.id
}

func (db *RedisDB) Resume(url string) {
	db.url = url
}

func (db *RedisDB) Pause() {
	db.url = ""
}

func (db *RedisDB) IsActive() bool {
	return db.url != ""
}

func (db *RedisDB) UpdateLastRequest() {
	db.lastRequest = time.Now()
}

func (db *RedisDB) LastRequest() time.Time {
	return db.lastRequest
}