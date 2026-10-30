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
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/prefixcache"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/routeutil"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	"github.com/vllm-project/aibrix/pkg/utils/prefixcacheindexer"
	v1 "k8s.io/api/core/v1"
)

type budgetHintCache struct {
	cache.Cache
	hints                                                map[string]int64
	hintPods, metricReads, rawReads, liveReads, livePods int
	allowRaw                                             bool
}

func (c *budgetHintCache) GetCachedRunningRequestHints(pods []*v1.Pod) map[string]int64 {
	c.hintPods += len(pods)
	out := make(map[string]int64)
	for _, p := range pods {
		key := utils.GeneratePodKey(p.Namespace, p.Name)
		if v, ok := c.hints[key]; ok {
			out[key] = v
		}
	}
	return out
}
func (c *budgetHintCache) GetMetricValueByPod(name, namespace, metric string) (metrics.MetricValue, error) {
	c.metricReads++
	if metric == metrics.RealtimeNumRequestsRunning && !c.allowRaw {
		c.rawReads++
		return nil, fmt.Errorf("unscoped count slot is forbidden")
	}
	return c.Cache.GetMetricValueByPod(name, namespace, metric)
}
func (c *budgetHintCache) GetMetricValueByPodModel(name, namespace, model, metric string) (metrics.MetricValue, error) {
	c.metricReads++
	return c.Cache.GetMetricValueByPodModel(name, namespace, model, metric)
}
func (c *budgetHintCache) GetPodsRunningRequests(pods []*v1.Pod) (map[string]int64, error) {
	c.liveReads++
	c.livePods += len(pods)
	return c.Cache.GetPodsRunningRequests(pods)
}

func newBudgetHintCache(t testing.TB, n int) (*budgetHintCache, []*v1.Pod) {
	t.Helper()
	base, pods := newFleetBenchCache(n)
	t.Cleanup(base.Close)
	c := &budgetHintCache{Cache: base, hints: make(map[string]int64, n)}
	for i, p := range pods {
		c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = int64(100 + i)
	}
	c.hints[utils.GeneratePodKey(pods[0].Namespace, pods[0].Name)] = 0
	return c, pods
}

func TestBudgetUsesObservedHintsAndLeavesLiveSnapshotEmpty(t *testing.T) {
	c, pods := newBudgetHintCache(t, 64)
	ctx := unifiedContext(t, "load-balance", 16)
	router := &loadBalanceRouter{cache: c}
	ctx.StartRunningRequestsSnapshot()
	defer ctx.StopRunningRequestsSnapshot()
	got := SelectCandidates(ctx, router, pods)
	require.Len(t, got, 16)
	requireDistinctSubset(t, got, pods)
	require.Contains(t, got, pods[0], "the known idle node must be recommended when the scout covers the fleet")
	require.Zero(t, c.rawReads)
	require.Zero(t, c.liveReads, "selection must not initialize final live-count state")
	require.LessOrEqual(t, c.hintPods, 64)
	counts, err := ctx.RunningRequests(got, c.GetPodsRunningRequests)
	require.NoError(t, err)
	require.Equal(t, 1, c.liveReads)
	require.Equal(t, 16, c.livePods)
	for _, n := range counts {
		require.LessOrEqual(t, n, int64(16), "cached recommendation counts must not contaminate live scoring")
	}
}

type budgetPrefixScorer struct {
	values         map[*v1.Pod]float64
	calls          int
	candidateCalls int
}

func (p *budgetPrefixScorer) ScoreCandidates(ctx *types.RoutingContext, ready types.PodList) ([]float64, []bool, error) {
	p.candidateCalls++
	return p.ScoreAll(ctx, ready)
}

func (p *budgetPrefixScorer) Polarity() types.Polarity { return types.PolarityMost }
func (p *budgetPrefixScorer) ScoreAll(_ *types.RoutingContext, ready types.PodList) ([]float64, []bool, error) {
	p.calls++
	all := ready.All()
	scores, valid := make([]float64, len(all)), make([]bool, len(all))
	for i, pod := range all {
		scores[i], valid[i] = p.values[pod], true
	}
	return scores, valid, nil
}
func (p *budgetPrefixScorer) SeedCandidates(ctx *types.RoutingContext, ready types.PodList, max int) []*v1.Pod {
	scores, valid, _ := p.ScoreAll(ctx, ready)
	for i := range valid {
		valid[i] = valid[i] && scores[i] > 0
	}
	return routeutil.RankCandidates(ready.All(), scores, valid, types.PolarityMost, max)
}

func TestBudgetPreservesAffinityAsTotalKGrows(t *testing.T) {
	c, pods := newBudgetHintCache(t, 200)
	prefix := &budgetPrefixScorer{values: make(map[*v1.Pod]float64)}
	for i, p := range pods {
		c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 0
		if i < 32 {
			prefix.values[p] = 100
			c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 10000
		}
	}
	ctx := unifiedContext(t, "prefix-cache:5,load-balance:4", 64)
	m := &multiStrategyRouter{config: &MultiRouterConfig{Items: []RouterItem{{Name: "prefix-cache", Coefficient: 5}, {Name: "load-balance", Coefficient: 4}}}, scorers: map[string]types.PodScorer{"prefix-cache": prefix, "load-balance": &loadBalanceRouter{cache: c}}}
	got := SelectCandidates(ctx, m, pods)
	require.Len(t, got, 64)
	requireDistinctSubset(t, got, pods)
	hits := 0
	for _, p := range got {
		if prefix.values[p] > 0 {
			hits++
		}
	}
	require.GreaterOrEqual(t, hits, 16, "larger budgets must retain an explicit prefix reserve")
	require.Equal(t, 1, prefix.calls, "one prefix lookup must serve scouting and reserved seeds")
	require.Equal(t, 1, prefix.candidateCalls, "the candidate interface must retain reusable raw observations")
	require.Zero(t, c.liveReads)
}

func TestBudgetReadScopeAndUnknownFallback(t *testing.T) {
	c, pods := newBudgetHintCache(t, 2000)
	ctx := unifiedContext(t, "load-balance", 16)
	router := &loadBalanceRouter{cache: c}
	got := SelectCandidates(ctx, router, pods)
	require.Len(t, got, 16)
	require.LessOrEqual(t, c.hintPods, 64)
	require.LessOrEqual(t, c.metricReads, 128)
	require.Zero(t, c.rawReads)
	require.Zero(t, c.liveReads)
	c.hints = nil
	c.metricReads = 0
	got = SelectCandidates(ctx, router, pods)
	require.Len(t, got, 16)
	requireDistinctSubset(t, got, pods)
	require.Zero(t, c.metricReads, "unknown counts must not trigger another hint source")
	require.Zero(t, c.liveReads)
	ov := *ctx.RoutingOverrides()
	ov.Candidate.LegacyPrefixM = true
	ctx.SetRoutingOverrides(&ov)
	c.hintPods = 0
	_ = SelectCandidates(ctx, router, pods)
	require.Zero(t, c.hintPods, "explicit legacy configuration must keep its old path")
}

func TestBudgetMissingCapacityDoesNotBecomeInfinite(t *testing.T) {
	c, pods := newBudgetHintCache(t, 64)
	values := make(map[string]map[string]metrics.MetricValue)
	for _, p := range pods {
		values[p.Name] = map[string]metrics.MetricValue{metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: math.MaxFloat64}}
		c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = 100
	}
	values[pods[0].Name][metrics.RealtimeOutputTokenRateEWMA] = &metrics.SimpleMetricValue{Value: 0}
	c.hints[utils.GeneratePodKey(pods[0].Namespace, pods[0].Name)] = 1000
	cache.InitWithPodsMetrics(c.Cache.(*cache.Store), values)
	ctx := unifiedContext(t, "load-balance", 16)
	router := &loadBalanceRouter{cache: c}
	chosen := 0
	for i := 0; i < 64; i++ {
		for _, p := range SelectCandidates(ctx, router, pods) {
			if p == pods[0] {
				chosen++
			}
		}
	}
	require.Less(t, chosen, 48, "overflow in the mean must not turn missing capacity into an infinite-speed recommendation")
}

func TestBudgetSelectionReusesRawPrefixForFinalSubset(t *testing.T) {
	t.Setenv(constants.EnvPrefixCacheUseRemoteTokenizer, "false")
	t.Setenv(constants.EnvPrefixCacheKVEventSyncEnabled, "false")
	c, pods := newBudgetHintCache(t, 64)
	index := prefixcacheindexer.NewPrefixHashTable()
	tokens := []byte(strings.Repeat("cache tokens ", 16))
	hashes := index.GetPrefixHashes(tokens)
	index.AddPrefix(hashes, "m1", utils.GeneratePodKey(pods[0].Namespace, pods[0].Name))
	router, err := prefixcache.NewPrefixCacheRouterWithOptions(c, index)
	require.NoError(t, err)
	ctx := unifiedContext(t, "prefix-cache", 16)
	ctx.SetPrefixTokens(tokens)
	selected := SelectCandidates(ctx, router, pods)
	require.Len(t, selected, 16)
	require.NotNil(t, ctx.SeededPrefixMatch(), "the actual selector must retain #3343's raw observation")
	var changed *v1.Pod
	for _, p := range selected {
		if p != pods[0] {
			changed = p
			break
		}
	}
	require.NotNil(t, changed)
	index.AddPrefix(hashes, "m1", utils.GeneratePodKey(changed.Namespace, changed.Name))
	subset := &utils.PodArray{Pods: []*v1.Pod{changed}}
	scores, _, err := router.(types.PodScorer).ScoreAll(ctx, subset)
	require.NoError(t, err)
	require.Zero(t, scores[0], "final scoring must use the candidate-stage raw result")
	ctx.SetSeededPrefixMatch(nil)
	scores, _, err = router.(types.PodScorer).ScoreAll(ctx, subset)
	require.NoError(t, err)
	require.Equal(t, float64(100), scores[0], "a new observation sees the changed index")
}

func BenchmarkNativeCandidateBudget(b *testing.B) {
	silenceKlog(b)
	for _, n := range []int{50, 500, 2000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			for _, mode := range []string{"random", "ranked-draft", "native"} {
				b.Run(mode, func(b *testing.B) {
					c, pods := newBudgetHintCache(b, n)
					c.allowRaw = true
					for i, p := range pods {
						c.hints[utils.GeneratePodKey(p.Namespace, p.Name)] = int64(i % 17)
					}
					ctx := unifiedContext(b, "load-balance", 16)
					ov := *ctx.RoutingOverrides()
					ov.Candidate.LegacyPrefixM = mode == "random"
					ctx.SetRoutingOverrides(&ov)
					router := &loadBalanceRouter{cache: c}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if mode == "ranked-draft" {
							_ = selectRankedCandidates(ctx, router, pods, 16)
						} else {
							_ = SelectCandidates(ctx, router, pods)
						}
					}
				})
			}
		})
	}
}

// selectRankedCandidates preserves the original draft rule for local comparison.
func selectRankedCandidates(ctx *types.RoutingContext, router types.Router, ready []*v1.Pod, k int) []*v1.Pod {
	if k <= 0 || len(ready) <= k || !candidateStageApplies(ctx) {
		return ready
	}
	candidates := make([]*v1.Pod, 0, k)
	picked := make(map[*v1.Pod]struct{}, k)
	seedBudget := k / 2
	if seeder, ok := router.(types.CandidateSeeder); ok && seedBudget > 0 {
		for _, pod := range seeder.SeedCandidates(ctx, &utils.PodArray{Pods: ready}, seedBudget) {
			if len(candidates) == seedBudget {
				break
			}
			if pod != nil {
				if _, exists := picked[pod]; !exists {
					picked[pod] = struct{}{}
					candidates = append(candidates, pod)
				}
			}
		}
	}
	if 2*k > len(ready) {
		for _, i := range rand.Perm(len(ready)) {
			if _, exists := picked[ready[i]]; !exists {
				picked[ready[i]] = struct{}{}
				candidates = append(candidates, ready[i])
				if len(candidates) == k {
					break
				}
			}
		}
	} else {
		for len(candidates) < k {
			pod := ready[rand.IntN(len(ready))]
			if _, exists := picked[pod]; !exists {
				picked[pod] = struct{}{}
				candidates = append(candidates, pod)
			}
		}
	}
	return candidates
}
