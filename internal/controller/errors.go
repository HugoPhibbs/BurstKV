package controller

import "errors"

var ErrCallerNotAssignedPod = errors.New("caller is not the pod assigned to this database")