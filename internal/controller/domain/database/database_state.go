package database

import "fmt"

type DatabaseStatus int

const (
	Running DatabaseStatus = iota
	Paused
	NotFound
	Unknown
)

func (s DatabaseStatus) String() string {
	switch s {
	case Running:
		return "running"
	case NotFound:
		return "non-found"
	case Unknown:
		return "unknown"
	default:
		panic(fmt.Sprintf("invalid DatabaseStatus: %d", s))
	}
}
