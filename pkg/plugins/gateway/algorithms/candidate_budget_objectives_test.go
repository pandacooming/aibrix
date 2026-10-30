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
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/least"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

// Each pair reverses the winner if the relevant term is removed. These are
// fixed inputs and unique rankings, independent of fleet sampling or timing.
func TestBudgetRecommendationsRespectObjectives(t *testing.T) {
	for _, tc := range []struct {
		name, strategy string
		counts         []int64
		capacity, kv   []float64
		cpu, match     []float64
		want           int
	}{
		{"incoming-request", "load-balance", []int64{0, 1}, []float64{1, 3}, []float64{0, 0}, nil, nil, 1},
		{"capacity", "load-balance", []int64{3, 4}, []float64{1, 10}, []float64{0, 0}, nil, nil, 1},
		{"kv-pressure", "load-balance", []int64{1, 2}, []float64{1, 1}, []float64{.9, 0}, nil, nil, 1},
		{"prefix-discount", "prefix-cache:5,load-balance:1", []int64{2, 3}, []float64{1, 1}, []float64{0, 0}, nil, []float64{0, 1}, 1},
		{"missing-capacity-mean", "load-balance", []int64{3, 0, 4}, []float64{1, 0, 10}, []float64{0, 0, 0}, nil, nil, 1},
		{"unknown-capacities", "load-balance", []int64{3, 0}, []float64{math.NaN(), math.Inf(1)}, []float64{0, 0}, nil, nil, 1},
		{"least-request-is-count", "least-request", []int64{1, 2}, []float64{1, 100}, []float64{.9, 0}, nil, nil, 0},
		{"kv-independent-of-count", "least-kv-cache", []int64{0, 100}, []float64{1, 1}, []float64{.9, .05}, nil, nil, 1},
		{"kv-includes-cpu", "least-kv-cache", []int64{0, 100}, []float64{1, 1}, []float64{.1, .6}, []float64{.8, 0}, nil, 1},
		{"kv-without-counts", "least-kv-cache", nil, []float64{1, 1}, []float64{.9, .05}, nil, nil, 1},
		{"kv-alias", "least-gpu-cache", nil, []float64{1, 1}, []float64{.9, .05}, nil, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, pods := newBudgetHintCache(t, len(tc.capacity))
			values := make(map[string]map[string]metrics.MetricValue)
			counts := make(map[string]int)
			view := &candidatePrefixView{fractions: make(map[*v1.Pod]float64)}
			for i, pod := range pods {
				values[pod.Name] = map[string]metrics.MetricValue{
					metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: tc.capacity[i]},
					metrics.KVCacheUsagePerc:            &metrics.SimpleMetricValue{Value: tc.kv[i]},
				}
				if tc.cpu != nil {
					values[pod.Name][metrics.CPUCacheUsagePerc] = &metrics.SimpleMetricValue{Value: tc.cpu[i]}
				}
				if tc.counts != nil {
					counts[utils.GeneratePodKey(pod.Namespace, pod.Name)] = int(tc.counts[i])
				}
				if tc.match != nil {
					view.fractions[pod] = tc.match[i]
				}
			}
			cache.InitWithPodsMetrics(c.Cache.(*cache.Store), values)
			cache.InitWithPodsModelMetrics(c.Cache.(*cache.Store), values)
			ctx := unifiedContext(t, tc.strategy, 16)
			ov := *ctx.RoutingOverrides()
			ov.LoadBalance.KVPressureAlpha = 2
			ctx.SetRoutingOverrides(&ov)
			src := candidateRoutingSources(ctx, &loadBalanceRouter{cache: c})
			got := budgetRecommendations(ctx, src, view, pods, counts, 1)
			require.Equal(t, []*v1.Pod{pods[tc.want]}, got)
			require.Zero(t, c.rawReads)
			require.Zero(t, c.liveReads)
		})
	}
}

func TestBudgetKVBlendHasItsOwnRecommendationShare(t *testing.T) {
	c, pods := newBudgetHintCache(t, 4)
	values := make(map[string]map[string]metrics.MetricValue)
	counts := make(map[string]int)
	for i, pod := range pods {
		values[pod.Name] = map[string]metrics.MetricValue{
			metrics.KVCacheUsagePerc: &metrics.SimpleMetricValue{Value: []float64{.9, .01, .8, .02}[i]},
		}
		counts[utils.GeneratePodKey(pod.Namespace, pod.Name)] = []int{0, 100, 1, 90}[i]
	}
	cache.InitWithPodsModelMetrics(c.Cache.(*cache.Store), values)
	for _, tc := range []struct {
		strategy string
		order    []int
	}{
		{"least-request:1,least-kv-cache:1", []int{1, 3, 0, 2}},
		{"least-request:1,least-kv-cache:3", []int{1, 3, 2, 0}},
	} {
		ctx := unifiedContext(t, tc.strategy, 16)
		src := candidateRoutingSources(ctx, &loadBalanceRouter{cache: c})
		got := budgetRecommendations(ctx, src, nil, pods, counts, 4)
		want := make([]*v1.Pod, len(tc.order))
		for i, n := range tc.order {
			want[i] = pods[n]
		}
		require.Equal(t, want, got, "KV must contribute even when request counts oppose it")
	}
}

func TestBudgetPureKVUsesMetricsWithoutCountHints(t *testing.T) {
	c, pods := newBudgetHintCache(t, 64)
	c.hints = nil
	values := make(map[string]map[string]metrics.MetricValue)
	for i, pod := range pods {
		values[pod.Name] = map[string]metrics.MetricValue{metrics.KVCacheUsagePerc: &metrics.SimpleMetricValue{Value: .1 + float64(i)/100}}
	}
	cache.InitWithPodsModelMetrics(c.Cache.(*cache.Store), values)
	router, err := least.NewLeastKvCacheRouterWithCache(c)
	require.NoError(t, err)
	ctx := unifiedContext(t, "least-kv-cache", 16)
	got := SelectCandidates(ctx, router, pods)
	require.Len(t, got, 16)
	require.Equal(t, pods[:4], got[:4])
	require.Zero(t, c.hintPods, "KV-only routing does not require running-count observations")
	require.Zero(t, c.rawReads)
	require.Zero(t, c.liveReads)
}

func TestBudgetExactSeedComposition(t *testing.T) {
	pods := candidatePods(80)
	for _, tc := range []struct{ k, r, holders, extra int }{
		{8, 2, 32, 0}, {16, 4, 32, 0}, {32, 4, 32, 0}, {64, 4, 32, 16},
		{64, 2, 32, 24}, {64, 4, 7, 3}, {64, 4, 0, 0},
	} {
		t.Run(fmt.Sprintf("K=%d/R=%d/holders=%d", tc.k, tc.r, tc.holders), func(t *testing.T) {
			view := &candidatePrefixView{pods: pods[:tc.holders]}
			for i := 0; i < tc.holders; i++ {
				view.scores = append(view.scores, float64(100-i))
				view.valid = append(view.valid, true)
			}
			seeds := budgetCandidateSeeds(tc.k, pods[:tc.r], view)
			require.Equal(t, pods[:tc.r+tc.extra], seeds, "deduplicating recommendations must not consume the prefix reserve")
			got := fillCandidatePods(pods, seeds, tc.k)
			require.Len(t, got, tc.k)
			requireDistinctSubset(t, got, pods)
		})
	}
}

func TestBudgetRecommendationCaps(t *testing.T) {
	for _, tc := range []struct{ k, want int }{{0, 0}, {1, 0}, {2, 0}, {3, 0}, {4, 1}, {8, 2}, {16, 4}, {32, 4}, {64, 4}, {1024, 4}} {
		require.Equal(t, tc.want, candidateRecommendationBudget(tc.k), "K=%d", tc.k)
	}
}

func TestBudgetSelectionPathsAreObservable(t *testing.T) {
	recordCandidateSelection("bypass")
	for _, path := range []string{"recommended", "no_hints", "small_k", "unsupported_strategy", "bypass", "legacy"} {
		t.Run(path, func(t *testing.T) {
			c, pods := newBudgetHintCache(t, 64)
			ctx := unifiedContext(t, "load-balance", 16)
			var router types.Router = &loadBalanceRouter{cache: c}
			ov := *ctx.RoutingOverrides()
			switch path {
			case "no_hints":
				c.hints = nil
			case "small_k":
				ov.Candidate.K = 2
			case "unsupported_strategy":
				router = nil
			case "bypass":
				ov.Candidate.K = len(pods)
			case "legacy":
				ov.Candidate.LegacyPrefixM = true
			}
			ctx.SetRoutingOverrides(&ov)
			counter := candidateSelectionPaths.WithLabelValues(path)
			before := testutil.ToFloat64(counter)
			SelectCandidates(ctx, router, pods)
			require.Equal(t, before+1, testutil.ToFloat64(counter))
		})
	}
}

func TestBudgetPrefixViewRejectsZeroAndInvalidMatches(t *testing.T) {
	pods := candidatePods(6)
	scorer := &budgetPrefixScorer{values: map[*v1.Pod]float64{pods[0]: 0, pods[1]: -1, pods[2]: math.NaN(), pods[3]: math.Inf(1), pods[4]: 50, pods[5]: 150}}
	ctx := unifiedContext(t, "prefix-cache", 16)
	view := readCandidatePrefix(ctx, scorer, pods)
	require.Equal(t, pods[4:], view.pods)
	require.Equal(t, .5, view.fractions[pods[4]])
	require.Equal(t, 1.0, view.fractions[pods[5]])
}

type candidateScoutObserver struct {
	*budgetHintCache
	requests   [][]*v1.Pod
	afterFirst func([]*v1.Pod)
}

func (c *candidateScoutObserver) GetCachedRunningRequestHints(pods []*v1.Pod) map[string]int64 {
	c.requests = append(c.requests, append([]*v1.Pod(nil), pods...))
	if len(c.requests) == 1 && c.afterFirst != nil {
		c.afterFirst(pods)
	}
	return c.budgetHintCache.GetCachedRunningRequestHints(pods)
}

func TestBudgetScoutsPrefixHoldersOutsideRandomSample(t *testing.T) {
	base, pods := newBudgetHintCache(t, 200)
	c := &candidateScoutObserver{budgetHintCache: base}
	prefix := &budgetPrefixScorer{}
	var holder *v1.Pod
	c.afterFirst = func(scout []*v1.Pod) {
		seen := map[*v1.Pod]bool{}
		for _, p := range scout {
			seen[p] = true
		}
		for _, p := range pods {
			if !seen[p] {
				holder = p
				break
			}
		}
		require.NotNil(t, holder)
		prefix.values = map[*v1.Pod]float64{holder: 100}
		base.hints[utils.GeneratePodKey(holder.Namespace, holder.Name)] = 0
	}
	ctx := unifiedContext(t, "prefix-cache:1,load-balance:1", 16)
	cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
	require.NoError(t, err)
	router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c}}}
	got := SelectCandidates(ctx, router, pods)
	require.Len(t, c.requests[0], 64, "K=16 must scout 64 ready pods")
	require.Len(t, c.requests, 2, "an outside prefix holder must get its own bounded hint read")
	require.Equal(t, []*v1.Pod{holder}, c.requests[1])
	require.Contains(t, got[:4], holder)
	require.LessOrEqual(t, base.hintPods, 5*16)
}

func TestBudgetNoValidRecommendationUsesReferenceFallback(t *testing.T) {
	c, pods := newBudgetHintCache(t, 64)
	values := map[string]map[string]metrics.MetricValue{}
	for _, p := range pods {
		values[p.Name] = map[string]metrics.MetricValue{metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: math.SmallestNonzeroFloat64}}
	}
	cache.InitWithPodsMetrics(c.Cache.(*cache.Store), values)
	prefix := &budgetPrefixScorer{values: map[*v1.Pod]float64{pods[0]: 100}}
	ctx := unifiedContext(t, "prefix-cache:1,load-balance:1", 16)
	cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
	require.NoError(t, err)
	router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c}}}
	recordCandidateSelection("bypass")
	counter := candidateSelectionPaths.WithLabelValues("no_valid_recommendation")
	before := testutil.ToFloat64(counter)
	got := SelectCandidates(ctx, router, pods)
	require.Len(t, got, 9, "reference fallback keeps eight random slots and the single available prefix seed")
	require.Equal(t, before+1, testutil.ToFloat64(counter))
}
