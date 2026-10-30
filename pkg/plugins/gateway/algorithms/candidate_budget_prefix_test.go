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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/prefixcache"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	"github.com/vllm-project/aibrix/pkg/utils/prefixcacheindexer"
	v1 "k8s.io/api/core/v1"
)

func TestBudgetPrefixFallbackFillsMissingSlots(t *testing.T) {
	for _, tc := range []struct{ k, holders int }{
		{1, 0}, {2, 0}, {3, 1}, {8, 0}, {8, 1}, {16, 1}, {16, 20},
	} {
		t.Run(fmt.Sprintf("K=%d/holders=%d", tc.k, tc.holders), func(t *testing.T) {
			c, pods := newBudgetHintCache(t, 64)
			c.hints = nil
			values := make(map[*v1.Pod]float64, tc.holders)
			for _, p := range pods[:tc.holders] {
				values[p] = 100
			}
			prefix := &budgetPrefixScorer{values: values}
			ctx := unifiedContext(t, "prefix-cache:3,load-balance:2", tc.k)
			cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
			require.NoError(t, err)
			router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{
				"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c},
			}}
			got := SelectCandidates(ctx, router, pods)
			require.Len(t, got, tc.k, "missing prefix holders must return their slots to random sampling")
			requireDistinctSubset(t, got, pods)
			if tc.holders == 1 && tc.k >= 2 {
				require.Contains(t, got, pods[0], "an available prefix holder must be retained")
			}
			require.Zero(t, c.liveReads, "fallback must not read live counts during candidate selection")
		})
	}
}

type prefixBudgetCounts struct {
	*budgetHintCache
	live map[string]int64
}

func (c *prefixBudgetCounts) GetPodsRunningRequests(pods []*v1.Pod) (map[string]int64, error) {
	c.liveReads++
	counts := make(map[string]int64, len(pods))
	for _, pod := range pods {
		key := utils.GeneratePodKey(pod.Namespace, pod.Name)
		counts[key] = c.live[key]
	}
	return counts, nil
}

func TestBudgetWeightedPrefixKeepsHolderButHonorsGate(t *testing.T) {
	t.Setenv(constants.EnvPrefixCacheUseRemoteTokenizer, "false")
	t.Setenv(constants.EnvPrefixCacheKVEventSyncEnabled, "false")
	for _, tc := range []struct {
		name         string
		hintRequests int64
		liveRequests int64
		wantWinner   bool
	}{
		{"normal load", 1, 1, true},
		{"overload gate", 8, 8, false},
		{"stale hint", 1, 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const n, k = 200, 16
			base, pods := newBudgetHintCache(t, n)
			c := &prefixBudgetCounts{budgetHintCache: base, live: make(map[string]int64, n)}
			values := make(map[string]map[string]metrics.MetricValue, n)
			for _, p := range pods {
				base.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 0
				values[p.Name] = map[string]metrics.MetricValue{
					metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: 1000},
					metrics.KVCacheUsagePerc:            &metrics.SimpleMetricValue{Value: 0},
				}
			}
			holder := pods[0]
			holderKey := utils.GeneratePodKey(holder.Namespace, holder.Name)
			base.hints[holderKey] = tc.hintRequests
			c.live[holderKey] = tc.liveRequests
			cache.InitWithPodsMetrics(base.Cache.(*cache.Store), values)
			cache.InitWithPodsModelMetrics(base.Cache.(*cache.Store), values)
			index := prefixcacheindexer.NewPrefixHashTable()
			tokens := []byte(strings.Repeat("shared conversation ", 64))
			index.AddPrefix(index.GetPrefixHashes(tokens), "m1", utils.GeneratePodKey(holder.Namespace, holder.Name))
			prefix, err := prefixcache.NewPrefixCacheRouterWithOptions(c, index)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, prefix.(interface{ Cleanup() error }).Cleanup()) })
			ctx := unifiedContext(t, "prefix-cache:3,load-balance:2", k)
			ctx.SetPrefixTokens(tokens)
			cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
			require.NoError(t, err)
			router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{
				"prefix-cache": prefix.(types.PodScorer), "load-balance": &loadBalanceRouter{cache: c},
			}}
			for i := 0; i < 20; i++ {
				ctx.SetSeededPrefixMatch(nil)
				c.liveReads = 0
				got := SelectCandidates(ctx, router, pods)
				require.Len(t, got, k)
				require.Contains(t, got, holder, "the sole prefix holder must compete even when its estimated load is higher")
				require.Zero(t, c.liveReads, "candidate selection must not read live counts")
				eligible := ApplyLoadImbalanceGate(ctx, c, got)
				require.Positive(t, c.liveReads, "the gate must read live counts rather than candidate hints")
				winner, _, err := router.scoreAndRank(ctx, &utils.PodArray{Pods: eligible})
				require.NoError(t, err)
				if tc.wantWinner {
					require.Equal(t, holder, winner, "final weighted scores should prefer the prefix hit")
				} else {
					require.NotContains(t, eligible, holder, "prefix reservation must not bypass overload protection")
					require.NotEqual(t, holder, winner)
				}
			}
		})
	}
}

func TestBudgetPrefixReserveRetainsRandomExploration(t *testing.T) {
	const n, k, trials = 64, 16, 3000
	c, pods := newBudgetHintCache(t, n)
	prefix := &budgetPrefixScorer{values: make(map[*v1.Pod]float64)}
	for i, p := range pods {
		count := int64(100)
		if i < 4 {
			count = 0
		}
		if i >= 4 && i < 36 {
			count = 1000
			prefix.values[p] = float64(100 - i)
		}
		c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = count
	}
	ctx := unifiedContext(t, "prefix-cache:1,load-balance:1", k)
	cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
	require.NoError(t, err)
	router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{
		"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c},
	}}
	counts := make(map[*v1.Pod]int, n)
	for i := 0; i < trials; i++ {
		got := SelectCandidates(ctx, router, pods)
		require.Len(t, got, k)
		require.ElementsMatch(t, pods[:4], got[:4], "the least-loaded recommendations must stay in the pool")
		require.Equal(t, pods[4:8], got[4:8], "the longest-prefix reserve must stay in the pool")
		for _, p := range got[8:] {
			counts[p]++
		}
	}
	// Eight random slots must be uniform over all 56 non-reserved pods.
	want := float64(trials * 8 / (n - 8))
	for _, p := range pods[8:] {
		require.InDelta(t, want, counts[p], 140, p.Name)
	}
}

// Benchmark the selector with locally supplied prefix scores and count hints.
// Prefix-index traversal, the load gate and final scoring are not included.
func BenchmarkBudgetPrefixSelection(b *testing.B) {
	silenceKlog(b)
	for _, n := range []int{500, 2000} {
		for _, holders := range []int{1, 16, 256} {
			b.Run(fmt.Sprintf("N=%d/holders=%d", n, holders), func(b *testing.B) {
				c, pods := newBudgetHintCache(b, n)
				prefix := &budgetPrefixScorer{values: make(map[*v1.Pod]float64, holders)}
				for i, p := range pods {
					c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 0
					if i < holders {
						prefix.values[p] = 100
						c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 1
					}
				}
				ctx := unifiedContext(b, "prefix-cache:3,load-balance:2", 16)
				cfg, err := ParseMultiRouterConfig(ctx.ResolvedStrategy)
				if err != nil {
					b.Fatal(err)
				}
				router := &multiStrategyRouter{config: cfg, scorers: map[string]types.PodScorer{
					"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c},
				}}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if got := SelectCandidates(ctx, router, pods); len(got) != 16 {
						b.Fatalf("got %d candidates", len(got))
					}
				}
			})
		}
	}
}
