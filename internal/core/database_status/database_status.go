package database_status

import "burstkv/internal/core/pod_resources.go"

type DatabaseStatus struct {
	Running          bool
	PodName            string
	MaxPodResources  *pod_resources.PodResources
	UsedPodResources *pod_resources.PodResources
}
