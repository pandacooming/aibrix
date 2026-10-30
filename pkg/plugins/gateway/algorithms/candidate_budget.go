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
	"math"
	"math/rand/v2"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/routeutil"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

type candidateSources struct {
	cache                     cache.Cache
	prefix                    types.PodScorer
	seeder                    types.CandidateSeeder
	prefixWeight, otherWeight float64
	supported                 bool
}

func candidateRoutingSources(ctx *types.RoutingContext, router types.Router) candidateSources {
	resolved := ctx.ResolvedStrategy
	if resolved == "" {
		resolved = string(ctx.Algorithm)
	}
	cfg, err := ParseMultiRouterConfig(resolved)
	if err != nil {
		return candidateSources{}
	}
	out := candidateSources{supported: true}
	for _, item := range cfg.Items {
		if item.Coefficient <= 0 {
			continue
		}
		var source any = router
		if blended, ok := router.(*multiStrategyRouter); ok {
			source = blended.scorers[item.Name]
		}
		if holder, ok := source.(requestCacheHolder); ok && out.cache == nil {
			out.cache = holder.Cache()
		}
		switch item.Name {
		case "prefix-cache":
			out.prefixWeight += float64(item.Coefficient)
			out.prefix, _ = source.(types.PodScorer)
			out.seeder, _ = source.(types.CandidateSeeder)
		case "load-balance", "least-request", "least-request-top-k", "least-kv-cache", "least-gpu-cache":
			out.otherWeight += float64(item.Coefficient)
		default:
			out.supported = false
		}
	}
	return out
}

type candidatePrefixView struct {
	pods      []*v1.Pod
	scores    []float64
	valid     []bool
	fractions map[*v1.Pod]float64
}

func readCandidatePrefix(ctx *types.RoutingContext, scorer types.PodScorer, ready []*v1.Pod) *candidatePrefixView {
	if scorer == nil {
		return nil
	}
	var scores []float64
	var valid []bool
	var err error
	if candidates, ok := scorer.(types.CandidateScorer); ok {
		scores, valid, err = candidates.ScoreCandidates(ctx, &utils.PodArray{Pods: ready})
	} else {
		scores, valid, err = scorer.ScoreAll(ctx, &utils.PodArray{Pods: ready})
	}
	if err != nil || len(scores) != len(ready) || len(valid) != len(ready) {
		return nil
	}
	out := &candidatePrefixView{fractions: make(map[*v1.Pod]float64)}
	for i, pod := range ready {
		if valid[i] && scores[i] > 0 && !math.IsNaN(scores[i]) && !math.IsInf(scores[i], 0) {
			out.pods = append(out.pods, pod)
			out.scores = append(out.scores, scores[i])
			out.valid = append(out.valid, true)
			out.fractions[pod] = math.Min(1, scores[i]/100)
		}
	}
	return out
}

func (p *candidatePrefixView) seeds(n int) []*v1.Pod {
	if p == nil || n <= 0 {
		return nil
	}
	return routeutil.RankCandidates(p.pods, p.scores, p.valid, types.PolarityMost, n)
}

// sampleCandidatePods samples without replacement. Large sampling fractions
// use a permutation so rejection sampling cannot spin near exhaustion.
func sampleCandidatePods(pods []*v1.Pod, n int) []*v1.Pod {
	n = min(n, len(pods))
	out := make([]*v1.Pod, 0, n)
	if n*2 > len(pods) {
		for _, i := range rand.Perm(len(pods))[:n] {
			out = append(out, pods[i])
		}
		return out
	}
	seen := make(map[*v1.Pod]bool, n)
	for len(out) < n {
		p := pods[rand.IntN(len(pods))]
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func fillCandidatePods(ready, seeds []*v1.Pod, wanted int) []*v1.Pod {
	wanted = min(wanted, len(ready))
	out := make([]*v1.Pod, 0, wanted)
	seen := make(map[*v1.Pod]bool, wanted)
	for _, p := range seeds {
		if p != nil && !seen[p] && len(out) < wanted {
			seen[p] = true
			out = append(out, p)
		}
	}
	if wanted*2 > len(ready) {
		for _, i := range rand.Perm(len(ready)) {
			if len(out) == wanted {
				break
			}
			p := ready[i]
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	} else {
		for len(out) < wanted {
			p := ready[rand.IntN(len(ready))]
			if !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out
}

type prefixOnlyCandidateSeeder struct {
	types.Router
	seeder types.CandidateSeeder
}

func (s prefixOnlyCandidateSeeder) SeedCandidates(ctx *types.RoutingContext, ready types.PodList, max int) []*v1.Pod {
	if s.seeder == nil || max <= 0 {
		return nil
	}
	allowed := make(map[*v1.Pod]bool, ready.Len())
	for _, p := range ready.All() {
		allowed[p] = true
	}
	seen := make(map[*v1.Pod]bool, max)
	var out []*v1.Pod
	for _, p := range s.seeder.SeedCandidates(ctx, ready, max) {
		if len(out) == max {
			break
		}
		if p != nil && allowed[p] && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func fallbackBudgetCandidates(ctx *types.RoutingContext, src candidateSources, ready []*v1.Pod, k int) []*v1.Pod {
	if src.prefixWeight == 0 {
		return selectCandidates(ctx, nil, ready, k, 0)
	}
	return selectCandidates(ctx, prefixOnlyCandidateSeeder{seeder: src.seeder}, ready, (k+1)/2, k/2)
}

func candidateCapacities(c cache.Cache, pods []*v1.Pod) []float64 {
	caps := make([]float64, len(pods))
	mean, known := 0.0, 0
	for i, p := range pods {
		v, err := c.GetMetricValueByPod(p.Name, p.Namespace, metrics.RealtimeOutputTokenRateEWMA)
		if err != nil || v == nil {
			continue
		}
		rate := v.GetSimpleValue()
		if rate > 0 && !math.IsInf(rate, 0) && !math.IsNaN(rate) {
			caps[i] = rate
			known++
			mean += (rate - mean) / float64(known)
		}
	}
	fallback := 1.0
	if known > 0 {
		fallback = mean
	}
	for i := range caps {
		if caps[i] == 0 {
			caps[i] = fallback
		}
	}
	return caps
}

func budgetRecommendations(ctx *types.RoutingContext, src candidateSources, view *candidatePrefixView, scout []*v1.Pod, counts map[string]int, budget int) []*v1.Pod {
	caps := candidateCapacities(src.cache, scout)
	lb := loadBalanceRouter{cache: src.cache}
	weight := src.otherWeight
	if weight == 0 {
		weight = src.prefixWeight
	}
	scores, valid := make([]float64, len(scout)), make([]bool, len(scout))
	for i, p := range scout {
		n, ok := counts[utils.GeneratePodKey(p.Namespace, p.Name)]
		if !ok {
			continue
		}
		used := 1 - lb.kvFreeFraction(ctx, p)
		cost := (float64(n) + 1) / caps[i] * (1 + ctx.RoutingOverrides().LoadBalance.KVPressureAlpha*used*used)
		if view != nil && weight > 0 {
			cost /= 1 + .5*src.prefixWeight/weight*view.fractions[p]
		}
		if math.IsNaN(cost) || math.IsInf(cost, 0) || cost < 0 {
			continue
		}
		scores[i], valid[i] = cost, true
	}
	return routeutil.RankCandidates(scout, scores, valid, types.PolarityLeast, budget)
}

// selectBudgetCandidates uses only bounded local hint reads. Final live counts
// and strategy scores are computed by the existing gate/router on its result.
func selectBudgetCandidates(ctx *types.RoutingContext, router types.Router, ready []*v1.Pod, k int) []*v1.Pod {
	if k <= 0 || len(ready) <= k || !candidateStageApplies(ctx) {
		return ready
	}
	src := candidateRoutingSources(ctx, router)
	if !src.supported || src.cache == nil || k < 4 {
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	if _, ok := src.cache.(cache.CachedRunningRequestHintProvider); !ok {
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	scout := sampleCandidatePods(ready, min(len(ready), 4*k))
	counts := routeutil.SnapshotRequestHints(src.cache, scout)
	if len(counts) == 0 {
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	view := readCandidatePrefix(ctx, src.prefix, ready)
	var extra []*v1.Pod
	seen := make(map[*v1.Pod]bool, len(scout)+k)
	for _, p := range scout {
		seen[p] = true
	}
	if view != nil {
		for _, p := range sampleCandidatePods(view.pods, k) {
			if !seen[p] {
				seen[p] = true
				scout = append(scout, p)
				extra = append(extra, p)
			}
		}
	}
	if len(extra) > 0 {
		for key, n := range routeutil.SnapshotRequestHints(src.cache, extra) {
			counts[key] = n
		}
	}
	recommendations := budgetRecommendations(ctx, src, view, scout, counts, min(k/4, 4))
	if len(recommendations) == 0 {
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	// Once the cost recommendation cap saturates, preserve prefix coverage as
	// K grows rather than giving every extra slot to uniform random sampling.
	prefixBudget := max(0, k/2-4*len(recommendations))
	seeds := append([]*v1.Pod(nil), recommendations...)
	if view != nil && prefixBudget > 0 {
		for _, p := range view.seeds(prefixBudget + len(recommendations)) {
			duplicate := false
			for _, r := range seeds {
				if p == r {
					duplicate = true
					break
				}
			}
			if !duplicate {
				seeds = append(seeds, p)
			}
			if len(seeds) == len(recommendations)+prefixBudget {
				break
			}
		}
	}
	return fillCandidatePods(ready, seeds, k)
}
