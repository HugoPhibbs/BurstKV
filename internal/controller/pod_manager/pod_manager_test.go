package pod_manager

import (
	"burstkv/internal/core"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func newTestPodManager(k8s *fake.Clientset, namespace string) *PodManager {
	return &PodManager{k8s: k8s, k8sNameSpace: namespace}
}

func warmPod(name, ip string) *corev1.Pod {
	return &corev1.Pod{
		Name:      name,
		Namespace: "test",
		Labels:    map[string]string{"burstkv/role": "warm"},
		Status:    corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip},
	}
}

func podExists(t *testing.T, k8s kubernetes.Interface, podName string) bool {
    t.Helper()
    _, err := k8s.CoreV1().Pods("test").Get(t.Context(), podName, metav1.GetOptions{})
    if apierrors.IsNotFound(err) {
        return false
    }
    require.NoError(t, err)
    return true
}

func TestPodManager_TestClaimWarmPod(t *testing.T) {
	ctx := t.Context()
	k8s := fake.NewClientset(warmPod("pod-1", "10.0.0.1"))
	pm := newTestPodManager(k8s, "test")

	claimed, err := pm.ClaimWarmPod(ctx)
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", claimed.PodIP)

	got, err := k8s.CoreV1().Pods("test").Get(ctx, "pod-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "claimed", got.Labels["burstkv/role"])

	claimed, err = pm.ClaimWarmPod(ctx)
	require.ErrorIs(t, err, core.ErrNoAvailablePods, "No pods should be available")
	require.Nil(t, claimed, "Claimed should be empty")
}

func TestPodManager_TestDeletingPods(t *testing.T) {
	ctx := t.Context()
	k8s := fake.NewClientset(warmPod("pod-1", "10.0.0.1"), warmPod("pod-2", "10.0.0.2"))
	pm := newTestPodManager(k8s, "test")

	require.True(t, podExists(t, k8s, "pod-2"))
	require.NoError(t,  pm.DeletePod(ctx, "pod-2"), "Deleting pod-2 shouldn't return an error")
	require.False(t, podExists(t, k8s, "pod-2"))

	require.ErrorIs(t, pm.DeletePod(ctx, "pod-2"), core.ErrPodNotFound)

	require.True(t, podExists(t, k8s, "pod-1"))
}
