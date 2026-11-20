package db_pod_manager

import (
	"burstkv/internal/controller/pod"
	"burstkv/internal/core"
	"burstkv/internal/core/pod_resources"
	"context"
	"os"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8srest "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

type PodManager struct {
	k8sInterface     kubernetes.Interface
	metricsInterface metricsv.Interface
	k8sNameSpace     string
}

func NewPodManager() (*PodManager, error) {
	k8s, err := newK8sInterface()
	if err != nil {
		return nil, err
	}

	metrics, err := newMetricsInterface()
	if err != nil {
		return nil, err
	}

	return &PodManager{
		k8sInterface: k8s,
		metricsInterface: metrics,
		k8sNameSpace: k8sNamespace(),
	}, nil
}

func k8sNamespace() string {
	n, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "default"
	}
	return strings.TrimSpace(string(n))
}

func newK8sInterface() (kubernetes.Interface, error) {
	cfg, err := k8sRestConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func newMetricsInterface() (metricsv.Interface, error) {
	cfg, err := k8sRestConfig()
	if err != nil {
		return nil, err
	}
	return metricsv.NewForConfig(cfg)
}

func k8sRestConfig() (*k8srest.Config, error) {
	cfg, err := k8srest.InClusterConfig()
	if err == k8srest.ErrNotInCluster {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func (p *PodManager) ClaimWarmPod(ctx context.Context) (*pod.Pod, error) {
	pods, err := p.k8sInterface.CoreV1().Pods(p.k8sNameSpace).List(ctx, metav1.ListOptions{
		LabelSelector: "burstkv/role=warm",
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return nil, err
	}
	for i := range pods.Items {
		foundPod := &pods.Items[i]
		foundPod.Labels["burstkv/role"] = "claimed"
		// Update sends the pod's resourceVersion, so if two claims race,
		// one gets a Conflict error and moves on to the next pod.
		claimed, err := p.k8sInterface.CoreV1().Pods(p.k8sNameSpace).Update(ctx, foundPod, metav1.UpdateOptions{})
		if err == nil {
			claimedPod := pod.NewPod(claimed.Name, claimed.Status.PodIP)
			return claimedPod, nil
		}
	}
	return nil, core.ErrNoAvailablePods
}

func (p *PodManager) DeletePod(ctx context.Context, podName string) error {
	err := p.k8sInterface.CoreV1().Pods(p.k8sNameSpace).Delete(ctx, podName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return core.ErrPodNotFound
	}
	return err
}

func (p *PodManager) PodMaxResources(ctx context.Context, podName string) (*pod_resources.PodResources, error) {
	pod, err := p.k8sInterface.CoreV1().Pods(p.k8sNameSpace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	res := pod.Spec.Containers[0].Resources
	memLimit := res.Limits.Memory().Value()
	cpuLimit := res.Limits.Cpu().MilliValue()
	return &pod_resources.PodResources{Memory: memLimit, Cpu: cpuLimit}, nil
}

func (p *PodManager) PodUsedResources(ctx context.Context, podName string) (*pod_resources.PodResources, error) {
	pm, err := p.metricsInterface.MetricsV1beta1().PodMetricses(p.k8sNameSpace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// log.Default().Printf("Warning: metrics for pod: %s, unavailable.\nThis may be because none yet exist (awaiting), or the pod couldn't not be found", podName)
			return nil, nil
		}
		return nil, err
	}
	usage := pm.Containers[0].Usage
	memUsage := usage.Memory().Value()
	cpuUsage := usage.Cpu().MilliValue()
	return &pod_resources.PodResources{Memory: memUsage, Cpu: cpuUsage}, nil
}
