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

package least

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
)

type hintRampCache struct {
	cache.Cache
	since  int64
	counts map[string]float64
}

func (c *hintRampCache) GetMetricValueByPod(name, ns, metric string) (metrics.MetricValue, error) {
	if metric != metrics.RealtimeNumRequestsRunning {
		return nil, fmt.Errorf("missing metric")
	}
	return &metrics.SimpleMetricValue{Value: c.counts[name]}, nil
}
func (c *hintRampCache) GetPodsReadySince(pods []*v1.Pod) (map[string]int64, error) {
	return map[string]int64{"ns/new": c.since}, nil
}
func (c *hintRampCache) LatestPodReadySince() int64 { return c.since }

func TestCandidateSeedingKeepsLeastRequestRamp(t *testing.T) {
	withRamp(t, 300*time.Second)
	now := time.Unix(1000, 0)
	c := &hintRampCache{since: now.UnixNano(), counts: map[string]float64{"new": 0, "warm": 10, "busy": 20}}
	r := newLeastRequestTopKRouterWithCacheAndClock(c, func() time.Time { return now })
	pods := []*v1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "new", Namespace: "ns"}}, {ObjectMeta: metav1.ObjectMeta{Name: "warm", Namespace: "ns"}}, {ObjectMeta: metav1.ObjectMeta{Name: "busy", Namespace: "ns"}}}
	ctx := types.NewRoutingContext(context.Background(), RouterLeastRequest, "m", "", "ramp", "")
	defer ctx.Delete()
	ov := *types.DefaultRoutingOverrides()
	ov.Candidate = types.CandidateOverrides{}
	ctx.SetRoutingOverrides(&ov)
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		seeds := r.SeedCandidates(ctx, &utils.PodArray{Pods: pods}, 1)
		require.Len(t, seeds, 1)
		require.NotEqual(t, "busy", seeds[0].Name)
		seen[seeds[0].Name] = true
	}
	assert.Len(t, seen, 2, "new zero-count pod must not be the sole seed while ramping")
	now = now.Add(301 * time.Second)
	require.Equal(t, []*v1.Pod{pods[0]}, r.SeedCandidates(ctx, &utils.PodArray{Pods: pods}, 1))
}
