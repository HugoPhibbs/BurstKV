package core

import (
	"errors"
)

// A database was not found
var ErrDatabaseNotFound = errors.New("database not found")

// A database is inactive
var ErrDatabasePaused = errors.New("database is inactive")

// A database is already paused
var ErrDatabaseAlreadyPaused = errors.New("database is already paused")

// A database is already active
var ErrDatabaseAlreadyActive = errors.New("database is already active")

// No available pods to assign a database
var ErrNoAvailablePods = errors.New("no available pods to assign database")
