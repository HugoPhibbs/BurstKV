package db_pod

import (
	"errors"
)

var ErrNoDatabaseRunningOnPod = errors.New("No database currently running on this pod")

var ErrFailoverRejected = errors.New("Redis FAILOVER rejected")