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
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/least"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/routeutil"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

const (
	candidateScoutFactor                  = 4
	candidateRecommendationDivisor        = 4
	candidateRecommendationCap            = 4
	candidatePrefixSlotsPerRecommendation = 4
)

type candidateSources struct {
	cache                               cache.Cache
	prefix                              types.PodScorer
	seeder                              types.CandidateSeeder
	prefixWeight, otherWeight           float64
	loadWeight, requestWeight, kvWeight float64
	supported                           bool
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
		case "load-balance":
			out.loadWeight += float64(item.Coefficient)
			out.otherWeight += float64(item.Coefficient)
		case "least-request", "least-request-top-k":
			out.requestWeight += float64(item.Coefficient)
			out.otherWeight += float64(item.Coefficient)
		case "least-kv-cache", "least-gpu-cache":
			out.kvWeight += float64(item.Coefficient)
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
	seeder := prefixOnlyCandidateSeeder{seeder: src.seeder}
	seeds := seeder.SeedCandidates(ctx, &utils.PodArray{Pods: ready}, k/2)
	// Unused prefix slots belong to the random pool. In particular, K=2 and a
	// cache miss must still leave two pods for the final scorer to compare.
	return fillCandidatePods(ready, seeds, k)
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
	if budget <= 0 {
		return nil
	}
	var recommendations []*v1.Pod
	if src.kvWeight > 0 {
		// KV occupancy need not correlate with request count or token rate.
		// Give its objective a share of the small recommendation budget.
		share := src.kvWeight / (src.kvWeight + src.loadWeight + src.requestWeight)
		kvBudget := min(budget, int(math.Ceil(float64(budget)*share)))
		scores, valid, err := least.NewLeastKvCacheScorer(src.cache).ScoreAll(ctx, &utils.PodArray{Pods: scout})
		if err == nil {
			for i := range scores {
				valid[i] = valid[i] && scores[i] >= 0
				if view != nil && src.otherWeight > 0 {
					scores[i] /= 1 + .5*src.prefixWeight/src.otherWeight*view.fractions[scout[i]]
				}
			}
			recommendations = routeutil.RankCandidates(scout, scores, valid, types.PolarityLeast, kvBudget)
		}
		if len(recommendations) == budget || src.loadWeight+src.requestWeight == 0 {
			return recommendations
		}
	}
	plainCounts := src.loadWeight == 0 && src.requestWeight > 0 && src.prefixWeight == 0
	var caps []float64
	if !plainCounts {
		caps = candidateCapacities(src.cache, scout)
	}
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
		cost := float64(n) + 1
		if !plainCounts {
			used := 1 - lb.kvFreeFraction(ctx, p)
			cost = cost / caps[i] * (1 + ctx.RoutingOverrides().LoadBalance.KVPressureAlpha*used*used)
		}
		if view != nil && weight > 0 {
			cost /= 1 + .5*src.prefixWeight/weight*view.fractions[p]
		}
		if math.IsNaN(cost) || math.IsInf(cost, 0) || cost < 0 {
			continue
		}
		scores[i], valid[i] = cost, true
	}
	for _, pod := range routeutil.RankCandidates(scout, scores, valid, types.PolarityLeast, budget+len(recommendations)) {
		duplicate := false
		for _, p := range recommendations {
			duplicate = duplicate || p == pod
		}
		if !duplicate {
			recommendations = append(recommendations, pod)
		}
		if len(recommendations) == budget {
			break
		}
	}
	return recommendations
}

// selectBudgetCandidates uses only bounded local hint reads. Final live counts
// and strategy scores are computed by the existing gate/router on its result.
func selectBudgetCandidates(ctx *types.RoutingContext, router types.Router, ready []*v1.Pod, k int) []*v1.Pod {
	if k <= 0 {
		return ready
	}
	if len(ready) <= k || !candidateStageApplies(ctx) {
		recordCandidateSelection("bypass")
		return ready
	}
	src := candidateRoutingSources(ctx, router)
	if k < candidateRecommendationDivisor {
		recordCandidateSelection("small_k")
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	if !src.supported || src.cache == nil {
		recordCandidateSelection("unsupported_strategy")
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	scout := sampleCandidatePods(ready, min(len(ready), candidateScoutFactor*k))
	needsCounts := src.kvWeight == 0 || src.loadWeight+src.requestWeight > 0
	var counts map[string]int
	if needsCounts {
		counts = routeutil.SnapshotRequestHints(src.cache, scout)
		if len(counts) == 0 {
			recordCandidateSelection("no_hints")
			return fallbackBudgetCandidates(ctx, src, ready, k)
		}
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
	if needsCounts && len(extra) > 0 {
		for key, n := range routeutil.SnapshotRequestHints(src.cache, extra) {
			counts[key] = n
		}
	}
	recommendations := budgetRecommendations(ctx, src, view, scout, counts, candidateRecommendationBudget(k))
	if len(recommendations) == 0 {
		recordCandidateSelection("no_valid_recommendation")
		return fallbackBudgetCandidates(ctx, src, ready, k)
	}
	recordCandidateSelection("recommended")
	return fillCandidatePods(ready, budgetCandidateSeeds(k, recommendations, view), k)
}

func candidateRecommendationBudget(k int) int {
	return min(k/candidateRecommendationDivisor, candidateRecommendationCap)
}

func budgetCandidateSeeds(k int, recommendations []*v1.Pod, view *candidatePrefixView) []*v1.Pod {
	// Reserve room for prefix holders even when the load recommendations fill
	// their cap. Keep the existing larger reserve as K grows, while leaving at
	// least half the budget for random exploration.
	r := len(recommendations)
	prefixBudget := max(min(k/2, r), k/2-candidatePrefixSlotsPerRecommendation*r)
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
	return seeds
}
