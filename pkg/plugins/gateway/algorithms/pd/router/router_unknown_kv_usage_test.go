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

// routeUnknownKV routes n requests over three decode pods that are equal except
// that decode-a and decode-b report KV usage 0.8 while decode-nan reports
// unknownKV (nil: no KV metric at all), and returns how often each pod was picked.
func routeUnknownKV(t *testing.T, unknownKV *float64, n int) map[string]int {
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
		decodeNaN.Name: pod(0),
		decodeA.Name:   pod(0.8),
		decodeB.Name:   pod(0.8),
	}

	if unknownKV == nil {
		delete(metricsMap[decodeNaN.Name], metrics.KVCacheUsagePerc)
	} else {
		metricsMap[decodeNaN.Name][metrics.KVCacheUsagePerc] = &metrics.SimpleMetricValue{Value: *unknownKV}
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
	return picks
}

// "Unknown KV usage = fully free" sends every decode request to the unknown pod,
// whether the gauge is NaN or missing. This documents the current behavior.
func TestUnknownKVUsagePodTakesAllDecodeTraffic(t *testing.T) {
	const n = 90
	nan := math.NaN()
	for name, kv := range map[string]*float64{"NaN KV usage": &nan, "no KV metric": nil} {
		t.Run(name, func(t *testing.T) {
			picks := routeUnknownKV(t, kv, n)
			t.Logf("decode picks over %d requests: %v", n, picks)
			require.Equal(t, n, picks["decode-nan"])
		})
	}
}
