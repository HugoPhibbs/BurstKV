package pod_resources

type PodResources struct {
	Memory int64 // bytes
	Cpu    int64 // in millicores, e.g 50 <=> 0.05 <=> 5% of a core
}