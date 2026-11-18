package controller

import "errors"

var ErrCallerNotAssignedPod = errors.New("caller is not the pod assigned to this database")

var ErrDatabaseWrongAuth = errors.New("Cannot authenticate this request")