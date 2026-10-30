/*
Copyright 2026 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package routingalgorithms

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/least"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

type staticCandidateQualityCache struct{ *budgetHintCache }

func (c *staticCandidateQualityCache) GetPodsRunningRequests(pods []*v1.Pod) (map[string]int64, error) {
	c.liveReads++
	out := make(map[string]int64, len(pods))
	for _, p := range pods {
		key := utils.GeneratePodKey(p.Namespace, p.Name)
		out[key] = c.hints[key]
	}
	return out, nil
}

// Reproducible static-objective probe, not an inference latency measurement.
// Fleet inputs have fixed seeds; production sampling remains stochastic. The
// deterministic objective tests guard correctness without statistical cutoffs.
func TestCandidateSelectionQualityProbe(t *testing.T) {
	if os.Getenv("AIBRIX_CANDIDATE_QUALITY_PROBE") != "1" {
		t.Skip("set AIBRIX_CANDIDATE_QUALITY_PROBE=1")
	}
	for _, seed := range []int64{3353, 3354, 3355} {
		for _, workload := range []string{"least-request", "load-balance", "kv-correlated", "kv-independent"} {
			for _, k := range []int{16, 32} {
				base, pods := newBudgetHintCache(t, 200)
				sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })
				c := &staticCandidateQualityCache{base}
				rng := rand.New(rand.NewSource(seed))
				values := make(map[string]map[string]metrics.MetricValue)
				for _, p := range pods {
					n := 1 + rng.Intn(100)
					c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = int64(n)
					kv := rng.Float64() * .9
					if workload == "kv-correlated" {
						kv = float64(n) / 110
					}
					values[p.Name] = map[string]metrics.MetricValue{
						metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: 500 + rng.Float64()*1500},
						metrics.KVCacheUsagePerc:            &metrics.SimpleMetricValue{Value: kv},
					}
				}
				cache.InitWithPodsMetrics(c.Cache.(*cache.Store), values)
				cache.InitWithPodsModelMetrics(c.Cache.(*cache.Store), values)
				strategy := workload
				var router types.Router
				var err error
				switch workload {
				case "least-request":
					router = least.NewLeastRequestTopKRouterWithCache(c)
				case "load-balance":
					router, err = NewLoadBalanceRouterWithCache(c)
				default:
					strategy = "least-kv-cache"
					router, err = least.NewLeastKvCacheRouterWithCache(c)
				}
				require.NoError(t, err)
				ctx := unifiedContext(t, strategy, k)
				input, err := json.Marshal(struct {
					Counts  map[string]int64
					Metrics map[string]map[string]metrics.MetricValue
				}{c.hints, values})
				require.NoError(t, err)
				inputHash := fmt.Sprintf("%x", sha256.Sum256(input))
				var scores []float64
				var valid []bool
				if workload == "least-request" {
					// Final least-request adds random epsilon jitter for routing
					// fairness. The static quality objective is the request count.
					for _, p := range pods {
						scores = append(scores, float64(c.hints[utils.GeneratePodKey(p.Namespace, p.Name)]))
						valid = append(valid, true)
					}
				} else {
					scores, valid, err = router.(types.PodScorer).ScoreAll(ctx, &utils.PodArray{Pods: pods})
					require.NoError(t, err)
				}
				objective := make(map[*v1.Pod]float64, len(pods))
				global := math.Inf(1)
				for i, p := range pods {
					require.True(t, valid[i])
					objective[p] = scores[i]
					global = math.Min(global, scores[i])
				}
				best := func(pool []*v1.Pod) float64 {
					v := math.Inf(1)
					for _, p := range pool {
						v = math.Min(v, objective[p])
					}
					return v
				}
				var randomSum, nativeSum float64
				liveBefore := c.liveReads
				for trial := 0; trial < 600; trial++ {
					randomSum += best(sampleCandidatePods(pods, k))
					nativeSum += best(SelectCandidates(ctx, router, pods))
				}
				require.Equal(t, liveBefore, c.liveReads, "candidate selection cannot read exact counts")
				randomMean, nativeMean := randomSum/600, nativeSum/600
				gap := randomMean - global
				captured := 0.0
				if gap > 0 {
					captured = (randomMean - nativeMean) / gap
				}
				data, err := json.Marshal(map[string]any{"seed": seed, "workload": workload, "fixture_sha256": inputHash, "pods": len(pods), "k": k, "trials": 600, "global_best": global, "random_best_mean": randomMean, "candidate_best_mean": nativeMean, "gap_captured": captured})
				require.NoError(t, err)
				t.Logf("CANDIDATE_QUALITY %s", data)
			}
		}
	}
}
