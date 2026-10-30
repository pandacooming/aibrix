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
	"math/rand/v2"
	"strconv"
	"sync"

	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/least"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// Candidate routing (RFC #3188) is off by default. K-only configuration uses a
// total K-pod budget: bounded cost recommendations, a prefix reserve when K
// grows, and uniform exploration. Live counts cover only these candidates;
// cached load-hint reads are bounded by K. Explicit M keeps
// the deprecated K random + M prefix-seed policy during migration.
var (
	routingCandidateK                 = utils.LoadEnvInt("AIBRIX_ROUTING_CANDIDATE_K", 0)
	routingCandidatePrefixM           = utils.LoadEnvInt("AIBRIX_ROUTING_CANDIDATE_PREFIX_M", 16)
	routingCandidatePrefixMConfigured = legacyPrefixMConfigured()
	legacyCandidateWarning            sync.Once
	unifiedCandidateInfo              sync.Once
)

func legacyPrefixMConfigured() bool {
	value, ok := utils.LookupEnv("AIBRIX_ROUTING_CANDIDATE_PREFIX_M")
	if !ok || value == "" {
		return false
	}
	_, err := strconv.Atoi(value)
	return err == nil
}

// candidateSafeStrategies are the strategies whose choice depends only on the pods they are
// given, so routing over a sample changes which pods compete but not the formulas that judge
// them. Checks that compare a pod with the rest of its pool (the gate's spread, prefix-cache's
// standard-deviation check, the blend's normalization) then compare it with the sample, not the
// fleet. A new strategy is kept off the candidate stage until it is added here.
// Strategies that keep a global view (vtc's fairness, the queue router, session pins, external
// and throughput routers, and the exclusive pd and slo routers) always see every ready pod.
var candidateSafeStrategies = map[string]bool{
	string(RouterLoadBalance):            true,
	string(RouterPrefixCache):            true,
	string(RouterRandom):                 true,
	string(RouterPowerOfTwo):             true,
	string(least.RouterLeastRequest):     true,
	string(least.RouterLeastRequestTopK): true,
	string(least.RouterLeastKvCache):     true,
	string(least.RouterLeastGpuCache):    true,
	string(least.RouterLeastBusyTime):    true,
	string(least.RouterLeastLatency):     true,
	string(least.RouterUtil):             true,
}

// candidateStageApplies reports whether every strategy in the request's resolved strategy, the
// one Select actually built including any auto-blend, is candidate-safe.
func candidateStageApplies(ctx *types.RoutingContext) bool {
	resolved := ctx.ResolvedStrategy
	if resolved == "" {
		resolved = string(ctx.Algorithm)
	}
	cfg, err := ParseMultiRouterConfig(resolved)
	if err != nil || len(cfg.Items) == 0 {
		return false
	}
	for _, item := range cfg.Items {
		if !candidateSafeStrategies[item.Name] {
			return false
		}
	}
	return true
}

// SelectCandidates returns the pods the gate and router should consider for this request: all of
// readyPods when the candidate stage is off, does not apply to the strategy, or would not shrink
// the set; otherwise a total K-pod set, or legacy K+M when M is explicit. It reads
// no per-pod state from Redis. router is the one Select returned for ctx.
func SelectCandidates(ctx *types.RoutingContext, router types.Router, readyPods []*v1.Pod) []*v1.Pod {
	knobs := ctx.RoutingOverrides().Candidate
	if !knobs.LegacyPrefixM {
		if knobs.K > 0 {
			unifiedCandidateInfo.Do(func() {
				klog.InfoS("K-only candidate routing enabled; K is the total budget, without the previous implicit M=16", "initial_k", knobs.K)
			})
		}
		return selectUnifiedCandidates(ctx, router, readyPods, knobs.K)
	}
	legacyCandidateWarning.Do(func() {
		klog.Warning("candidate.prefixM / AIBRIX_ROUTING_CANDIDATE_PREFIX_M is deprecated; explicit M preserves legacy K+M routing; omit M and configure a total K budget to migrate")
	})
	if knobs.K > 0 {
		recordCandidateSelection("legacy")
	}
	return selectCandidates(ctx, router, readyPods, knobs.K, knobs.PrefixM)
}

// selectUnifiedCandidates uses the observed-hint budget policy. Unknown hints
// retain the reference sampling distribution; final scoring still reads live
// counts, on these candidates only.
func selectUnifiedCandidates(ctx *types.RoutingContext, router types.Router, ready []*v1.Pod, k int) []*v1.Pod {
	return selectBudgetCandidates(ctx, router, ready, k)
}

func selectCandidates(ctx *types.RoutingContext, router types.Router, readyPods []*v1.Pod, k, m int) []*v1.Pod {
	seeder, _ := router.(types.CandidateSeeder)
	return selectCandidatesWithSeeder(ctx, seeder, readyPods, k, m)
}

func selectCandidatesWithSeeder(ctx *types.RoutingContext, seeder types.CandidateSeeder, readyPods []*v1.Pod, k, m int) []*v1.Pod {
	// A negative M only turns seeding off; it does not disable the stage.
	m = max(m, 0)
	if k <= 0 || len(readyPods) <= k || !candidateStageApplies(ctx) {
		return readyPods
	}

	var seeds []*v1.Pod
	if seeder != nil && m > 0 {
		seeds = seeder.SeedCandidates(ctx, &utils.PodArray{Pods: readyPods}, m)
		// Seeders are asked for at most m pods; enforce it so the sampling below never depends on
		// a seeder honoring that.
		if len(seeds) > m {
			seeds = seeds[:m]
		}
	}

	candidates := make([]*v1.Pod, 0, k+len(seeds))
	picked := make(map[*v1.Pod]struct{}, k+len(seeds))
	for _, pod := range seeds {
		if _, dup := picked[pod]; !dup {
			picked[pod] = struct{}{}
			candidates = append(candidates, pod)
		}
	}
	// The stage applies only when it leaves some ready pod out. M only caps the seeds, so it is
	// the seeds the router actually returned that count here: a router that seeds fewer than M,
	// or none, still gets a K-pod sample.
	want := len(candidates) + k
	if want >= len(readyPods) {
		return readyPods
	}
	// want < len(readyPods), so k random pods outside the seeds always exist. When the sample is
	// a small share of readyPods, rejection sampling keeps this O(k) instead of touching every pod.
	if 2*want > len(readyPods) {
		for _, i := range rand.Perm(len(readyPods)) {
			if len(candidates) == want {
				break
			}
			if _, dup := picked[readyPods[i]]; !dup {
				picked[readyPods[i]] = struct{}{}
				candidates = append(candidates, readyPods[i])
			}
		}
	} else {
		for len(candidates) < want {
			pod := readyPods[rand.IntN(len(readyPods))]
			if _, dup := picked[pod]; !dup {
				picked[pod] = struct{}{}
				candidates = append(candidates, pod)
			}
		}
	}

	if klog.V(5).Enabled() {
		klog.V(5).InfoS("routing candidate stage", "request_id", ctx.RequestID, "ready_pods", len(readyPods),
			"candidates", len(candidates), "seeded", len(seeds), "strategy", ctx.ResolvedStrategy)
	}
	return candidates
}
