package db_pod_manager

import (
	"burstkv/internal/controller/pod"
	"burstkv/internal/core"
	"context"
	"os"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type PodManager struct {
	k8s          kubernetes.Interface
	k8sNameSpace string
}

func NewPodManager() (*PodManager, error) {
	k8s, err := newK8sInterface()
	if err != nil {
		return nil, err
	}

	return &PodManager{
		k8s:          k8s,
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
	cfg, err := rest.InClusterConfig()
	if err == rest.ErrNotInCluster {
		cfg, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func (p *PodManager) ClaimWarmPod(ctx context.Context) (*pod.Pod, error) {
	pods, err := p.k8s.CoreV1().Pods(p.k8sNameSpace).List(ctx, metav1.ListOptions{
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
		claimed, err := p.k8s.CoreV1().Pods(p.k8sNameSpace).Update(ctx, foundPod, metav1.UpdateOptions{})
		if err == nil {
			claimedPod := pod.NewPod(claimed.Name, claimed.Status.PodIP)
			return claimedPod, nil
		}
	}
	return nil, core.ErrNoAvailablePods
}

func (p *PodManager) DeletePod(ctx context.Context, podName string) error {
	err := p.k8s.CoreV1().Pods(p.k8sNameSpace).Delete(ctx, podName, metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return core.ErrPodNotFound
	}
	return err
}
