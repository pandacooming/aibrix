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

// Two decode pods; decode-0 carries the odd gauge values. Each case routes 20
// requests and checks that routing never fails and, where one pod is clearly
// worse, that it is not the one picked.
func TestNonFiniteDecodeGauges(t *testing.T) {
	type gauges struct{ throughput, kv float64 }
	nan, inf := math.NaN(), math.Inf(1)
	cases := []struct {
		name       string
		d0, d1     gauges
		avoidPodD0 bool
	}{
		{"all throughputs NaN", gauges{nan, 0.3}, gauges{nan, 0.5}, false},
		{"-Inf throughput", gauges{-inf, 0.3}, gauges{100, 0.5}, false},
		{"-Inf KV usage", gauges{100, -inf}, gauges{100, 0.5}, false},
		{"all KV usage NaN", gauges{100, nan}, gauges{100, nan}, false},
		// A pod reporting KV usage above 1 is treated as full on main; +Inf is
		// the limit of that and should be avoided too, not scored as fully free.
		{"KV usage 1.5", gauges{100, 1.5}, gauges{100, 0.5}, true},
		{"+Inf KV usage", gauges{100, inf}, gauges{100, 0.5}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prefillPod := burstPod("prefill-0", "prefill", "127.0.0.1")
			d0 := burstPod("decode-0", "decode", "127.0.0.2")
			d1 := burstPod("decode-1", "decode", "127.0.0.3")
			readyPods := []*v1.Pod{prefillPod, d0, d1}
			m := func(g gauges) map[string]metrics.MetricValue {
				return map[string]metrics.MetricValue{
					metrics.RealtimeNumRequestsRunning:      &metrics.SimpleMetricValue{Value: 1},
					metrics.AvgGenerationThroughputToksPerS: &metrics.SimpleMetricValue{Value: g.throughput},
					metrics.KVCacheUsagePerc:                &metrics.SimpleMetricValue{Value: g.kv},
				}
			}
			metricsMap := map[string]map[string]metrics.MetricValue{d0.Name: m(c.d0), d1.Name: m(c.d1)}

			gate := make(chan struct{})
			close(gate)
			client := &http.Client{Transport: &gatedTransport{gate: gate}}
			tracker := pd.NewPrefillRequestTracker()
			r := &pdRouter{
				cache:                 cache.NewWithPodsMetricsForTest(readyPods, "m", metricsMap),
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
			for i := 0; i < 20; i++ {
				ctx := types.NewRoutingContext(context.Background(), "pd", "m", "hello", "req", "user")
				ctx.Engine = "vllm"
				ctx.ReqPath = testChatCompletionsPath
				ctx.ReqBody = []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
				_, err := r.Route(ctx, &utils.PodArray{Pods: readyPods})
				require.NoError(t, err)
				picks[ctx.TargetPod().Name]++
				ctx.Delete()
			}
			t.Logf("decode picks: %v", picks)
			if c.avoidPodD0 {
				require.Zero(t, picks[d0.Name], "decode-0 reports full KV but was picked")
			}
		})
	}
}
