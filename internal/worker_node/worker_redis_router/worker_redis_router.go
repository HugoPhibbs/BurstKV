package worker_redis_router

import (
	"burstkv/internal/worker_node/worker_manager"
)

type WorkerRedisRouter struct {
	wm *worker_manager.WorkerManager
}

func NewWorkerRedisRouter(wm *worker_manager.WorkerManager) *WorkerRedisRouter {
	return &WorkerRedisRouter{wm: wm}
}

// Returns the database's address, assigning it a container first if it isn't running.
func (p *WorkerRedisRouter) RouteDatabase(databaseID string) (string, error) {
	address, err := p.wm.DatabaseAddress(databaseID)
	if err != nil {
		return "", nil
	}
	return address, err
}

func (p *WorkerRedisRouter) RecordDatabaseActivity(databaseID string) {}
