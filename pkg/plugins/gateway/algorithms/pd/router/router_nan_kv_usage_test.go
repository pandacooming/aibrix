package router

import (
	"context"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/pd"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/pd/prefill"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/pd/selector"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
)

// A decode pod whose KV usage gauge is NaN must not be preferred over healthy
// pods that report real usage: with three equal pods it should get about a third.
func TestNaNKVUsagePodIsNotPreferred(t *testing.T) {
	prefillPod := burstPod("prefill-0", "prefill", "127.0.0.1")
	decodeNaN := burstPod("decode-nan", "decode", "127.0.0.2")
	decodeA := burstPod("decode-a", "decode", "127.0.0.3")
	decodeB := burstPod("decode-b", "decode", "127.0.0.4")
	readyPods := []*v1.Pod{prefillPod, decodeNaN, decodeA, decodeB}

	model := "nan-model"
	pod := func(kv float64) map[string]metrics.MetricValue {
		return map[string]metrics.MetricValue{
			metrics.RealtimeNumRequestsRunning:      &metrics.SimpleMetricValue{Value: 4},
			metrics.AvgGenerationThroughputToksPerS: &metrics.SimpleMetricValue{Value: 100},
			metrics.KVCacheUsagePerc:                &metrics.SimpleMetricValue{Value: kv},
		}
	}
	metricsMap := map[string]map[string]metrics.MetricValue{
		decodeNaN.Name: pod(math.NaN()),
		decodeA.Name:   pod(0.8),
		decodeB.Name:   pod(0.8),
	}

	gate := make(chan struct{})
	close(gate)
	client := &http.Client{Transport: &gatedTransport{gate: gate}}
	tracker := pd.NewPrefillRequestTracker()
	r := &pdRouter{
		cache:                 cache.NewWithPodsMetricsForTest(readyPods, model, metricsMap),
		prefillPolicy:         pd.NewLeastRequestPrefillPolicy(),
		prefillRequestTracker: tracker,
		pendingDecodeTracker:  pd.NewPendingDecodeTracker(),
		httpClient:            client,
		prefixUpdateCh:        make(chan prefixUpdateJob, 16),
		selectionCounts:       map[string]int64{},
	}
	r.podSelector = selector.NewDefaultSelector(r.filterPrefillDecodePods)
	r.prefillExecutor = prefill.NewDefaultExecutor(client, tracker)

	const n = 90
	picks := map[string]int{}
	for i := 0; i < n; i++ {
		ctx := types.NewRoutingContext(context.Background(), "pd", model, "hello", "nan-req", "user")
		ctx.Engine = "vllm"
		ctx.ReqPath = testChatCompletionsPath
		ctx.ReqBody = []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
		_, err := r.Route(ctx, &utils.PodArray{Pods: readyPods})
		require.NoError(t, err)
		picks[ctx.TargetPod().Name]++
		ctx.Delete()
	}
	t.Logf("decode picks over %d requests: %v", n, picks)
	require.Less(t, picks[decodeNaN.Name], 2*n/3, "the NaN-usage pod took most of the traffic")
}
