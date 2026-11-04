package worker_node

import (
	"errors"
)

// No idle containers left in the pool
var ErrNoAvailableContainers = errors.New("no available containers in the pool")

// The database is already assigned to a container
var ErrDatabaseAlreadyAssigned = errors.New("database is already assigned to a container")

// The database is not currently assigned to a container
var ErrDatabaseNotAssigned = errors.New("database is not assigned to a container")