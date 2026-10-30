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
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/types"
)

func TestRankCandidatesFiniteHintsAndPolarity(t *testing.T) {
	pods := make([]*v1.Pod, 6)
	for i := range pods {
		pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprint(i)}}
	}
	values := []float64{3, 1, 2, math.NaN(), math.Inf(1), 0}
	scored := []bool{true, true, true, true, true, false}
	require.Equal(t, []*v1.Pod{pods[1], pods[2]}, RankCandidates(pods, values, scored, types.PolarityLeast, 2))
	require.Equal(t, []*v1.Pod{pods[0], pods[2]}, RankCandidates(pods, values, scored, types.PolarityMost, 2))
	require.Len(t, RankCandidates(pods, values, scored, types.PolarityLeast, 100), 3)
	require.Empty(t, RankCandidates(pods, values, scored, types.PolarityLeast, 0))
	require.Empty(t, RankCandidates(pods, values, nil, types.PolarityLeast, 2))
}

func TestRankCandidatesSpreadEqualHints(t *testing.T) {
	pods := make([]*v1.Pod, 20)
	values := make([]float64, 20)
	scored := make([]bool, 20)
	for i := range pods {
		pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprint(i)}}
		scored[i] = true
	}
	counts := map[*v1.Pod]int{}
	for i := 0; i < 4000; i++ {
		for _, p := range RankCandidates(pods, values, scored, types.PolarityLeast, 2) {
			counts[p]++
		}
	}
	for _, p := range pods {
		assert.InDelta(t, 400, counts[p], 90, p.Name)
	}
}

type observedHintTestCache struct {
	cache.Cache
	counts map[string]int64
	calls  int
}

func (c *observedHintTestCache) GetCachedRunningRequestHints([]*v1.Pod) map[string]int64 {
	c.calls++
	return c.counts
}
func TestSnapshotRequestHintsDoNotGuessMissingObservations(t *testing.T) {
	c := &observedHintTestCache{counts: map[string]int64{"ns/valid": 7, "ns/negative": -1, "ns/overflow": math.MaxInt32 + 1, "unrequested": 99}}
	pods := []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "valid"}}, {ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "negative"}}, {ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "overflow"}}, nil}
	require.Equal(t, map[string]int{"ns/valid": 7}, SnapshotRequestHints(c, pods), "a provider's extra keys must not expand the requested scope")
	require.Equal(t, 1, c.calls)
	require.Empty(t, SnapshotRequestHints(c, nil))
	c.counts = nil
	require.Empty(t, SnapshotRequestHints(c, nil))
	require.Empty(t, SnapshotRequestHints(nil, nil), "unsupported cache must not supply a guessed local metric")
}
