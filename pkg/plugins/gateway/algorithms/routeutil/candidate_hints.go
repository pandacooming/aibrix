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

package routeutil

import (
	"math"
	"math/rand/v2"
	"sort"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

// SnapshotRequestHints uses an explicitly observed count source. It does not
// fall back to the metric slot: that slot may have lost remote contributions.
// Missing or expired observations stay unknown. Candidate observations remain
// separate from final live scoring and admission.
func SnapshotRequestHints(c cache.Cache, pods []*v1.Pod) map[string]int {
	provider, ok := c.(cache.CachedRunningRequestHintProvider)
	if !ok {
		return nil
	}
	raw := provider.GetCachedRunningRequestHints(pods)
	if len(raw) == 0 {
		return nil
	}
	counts := make(map[string]int, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		key := utils.GeneratePodKey(pod.Namespace, pod.Name)
		n, known := raw[key]
		if known && n >= 0 && n <= math.MaxInt32 {
			counts[key] = int(n)
		}
	}
	return counts
}

type rankedCandidate struct {
	pod   *v1.Pod
	score float64
	tie   uint64
}

func betterCandidate(a, b rankedCandidate) bool {
	return a.score < b.score || a.score == b.score && a.tie < b.tie
}

// RankCandidates keeps the best max finite, scored pods in O(N*max) time and
// O(max) extra memory. Independent random tie keys spread equal hints across
// the fleet; fixed input order must not pin every gateway to the same pods.
func RankCandidates(pods []*v1.Pod, scores []float64, scored []bool, polarity types.Polarity, max int) []*v1.Pod {
	if max <= 0 || len(scores) != len(pods) || len(scored) != len(pods) {
		return nil
	}
	top := make([]rankedCandidate, 0, min(max, len(pods)))
	for i, pod := range pods {
		if !scored[i] || math.IsNaN(scores[i]) || math.IsInf(scores[i], 0) {
			continue
		}
		score := scores[i]
		if polarity == types.PolarityMost {
			score = -score
		}
		c := rankedCandidate{pod: pod, score: score, tie: rand.Uint64()}
		if len(top) == max && !betterCandidate(c, top[len(top)-1]) {
			continue
		}
		pos := sort.Search(len(top), func(i int) bool { return betterCandidate(c, top[i]) })
		if len(top) < max {
			top = append(top, rankedCandidate{})
		}
		copy(top[pos+1:], top[pos:len(top)-1])
		top[pos] = c
	}
	picked := make([]*v1.Pod, len(top))
	for i := range top {
		picked[i] = top[i].pod
	}
	return picked
}
