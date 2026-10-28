package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/metrics"
)

// p1 served 100000 requests at 0.05 s queue time, then slowed to 2 s. p2 steadily
// queues 0.5 s. Once least-latency stops sending p1 traffic, p1's 1-minute window
// empties. The slow pod must not become the lowest score again just because it
// stopped receiving requests.
func TestSlowPodDoesNotWinAgainWhenItsWindowEmpties(t *testing.T) {
	const model, metric = "m1", metrics.RequestQueueTimeSeconds
	labels := map[string]string{"model.aibrix.ai/name": model}
	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "default", Labels: labels}},
		{ObjectMeta: metav1.ObjectMeta{Name: "p2", Namespace: "default", Labels: labels}},
	}
	slow := &metrics.HistogramMetricValue{Sum: 100000*0.05 + 300*2, Count: 100300}
	c := NewWithPodsModelMetricsForTest(pods, model, map[string]map[string]metrics.MetricValue{
		"p1": {metric: slow},
		"p2": {metric: &metrics.HistogramMetricValue{Sum: 500, Count: 1000},
			RecentMeanMetricName(metric): &metrics.SimpleMetricValue{Value: 0.5}},
	})
	p1, ok := c.metaPods.Load("default/p1")
	require.True(t, ok)
	purgeRateHistory(t, p1)
	score := func() float64 { return RecentHistogramMean(c, "p1", "default", model, metric, slow) }

	// Minute 1: p1 slowed down; its recent mean puts it behind p2.
	seedMean1m(p1, model, metric, 100000*0.05, 100000, time.Minute)
	c.updateRecentMean(p1, model, metric, slow)
	require.InDelta(t, 2.0, score(), 1e-9)

	// Minute 2: p1 got no requests, so its histogram did not move.
	purgeRateHistoryNow(p1)
	seedMean1m(p1, model, metric, 100000*0.05, 100000, 2*time.Minute)
	seedMean1m(p1, model, metric, slow.Sum, slow.Count, time.Minute)
	c.updateRecentMean(p1, model, metric, slow)
	t.Logf("p1 score after its window emptied: %.4f (p2: 0.5)", score())
	require.Greater(t, score(), 0.5, "p1 is still the slow pod but scores below p2 and takes every request")
}
