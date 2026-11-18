package redis_proxy

import "errors"

var ErrAuthCommandIncorrect = errors.New("AUTH command is incorrect")