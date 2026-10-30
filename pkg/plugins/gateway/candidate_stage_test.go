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

package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/metrics"
	routing "github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/configprofiles"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
)

// TestSelectTargetPod_CandidateStageBoundsRedisReads routes one least-request request over 200
// ready pods against a real Redis protocol server and counts the commands the routing path
// sends. Without the stage, every ready pod's running count is read once per decision (the
// gate, least-request and the load-balance blend share that read, #3329). With
// routingConfig.candidate.k set, only the candidates are read, so the command count stops
// depending on the fleet size.
func TestSelectTargetPod_CandidateStageBoundsRedisReads(t *testing.T) {
	const n = 200
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	require.NoError(t, client.Ping(context.Background()).Err())

	pods := make([]*v1.Pod, n)
	for i := range pods {
		pods[i] = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%03d", i), Namespace: "ns"},
			Status: v1.PodStatus{
				PodIP:      fmt.Sprintf("10.0.%d.%d", i/256, i%256),
				Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
			},
		}
	}
	store := cache.InitWithPods(cache.New(client, nil, nil), pods, "m")
	rm := routing.NewRouterManagerWithCache(store)
	rm.Init()
	s := &Server{cache: store, routerManager: rm}

	route := func(routingConfig string) int {
		t.Helper()
		ctx := types.NewRoutingContext(context.Background(), routing.RouterLeastRequest, "m", "hello", "req-"+routingConfig, "")
		t.Cleanup(ctx.Delete)
		if routingConfig != "" {
			raw := json.RawMessage(routingConfig)
			ctx.ConfigProfile = &types.ResolvedConfigProfile{RoutingConfig: raw, Routing: configprofiles.ParseRoutingConfig(raw)}
		}
		before := mr.CommandCount()
		addr, err := s.selectTargetPod(context.Background(), ctx, &utils.PodArray{Pods: pods}, "")
		require.NoError(t, err)
		require.NotEmpty(t, addr)
		return mr.CommandCount() - before
	}

	full := route("")
	sampled := route(`{"candidate":{"k":8}}`)
	t.Logf("Redis commands per request over %d pods: %d without the candidate stage, %d with k=8", n, full, sampled)
	assert.GreaterOrEqual(t, full, n, "without the stage every ready pod is read")
	assert.LessOrEqual(t, sampled, 8+8, "with the candidate stage only the 8 candidates are read")
}

// The real gateway keeps live Redis work bounded even while seeders inspect
// cached hints for the entire fleet. The inflight prefilter is the intentional
// exception: it must check every ready pod before sampling.
func TestUnifiedCandidateStageRedisBudget(t *testing.T) {
	const n = 200
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	pods := make([]*v1.Pod, n)
	hints := make(map[string]map[string]metrics.MetricValue, n)
	for i := range pods {
		name := fmt.Sprintf("unified-%03d", i)
		pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}, Status: v1.PodStatus{PodIP: fmt.Sprintf("10.0.0.%d", i+1), Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}}}}
		hints[name] = map[string]metrics.MetricValue{
			metrics.RealtimeNumRequestsRunning:  &metrics.SimpleMetricValue{Value: float64(i % 7)},
			metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: 1000 + float64(i)},
			metrics.KVCacheUsagePerc:            &metrics.SimpleMetricValue{Value: float64(i%5) / 10},
		}
	}
	store := cache.InitWithPods(cache.New(client, nil, nil), pods, "m")
	store = cache.InitWithPodsMetrics(store, hints)
	store = cache.InitWithPodsModelMetrics(store, hints)
	t.Cleanup(store.Close)
	rm := routing.NewRouterManagerWithCache(store)
	rm.Init()
	server := &Server{cache: store, routerManager: rm}
	for _, strategy := range []types.RoutingAlgorithm{"load-balance", "least-request", "least-kv-cache", "least-kv-cache:1,load-balance:2"} {
		for _, cap := range []int64{0, 1000} {
			t.Run(fmt.Sprintf("%s/inflight=%d", strategy, cap), func(t *testing.T) {
				ctx := types.NewRoutingContext(context.Background(), strategy, "m", "hello", "unified", "")
				defer ctx.Delete()
				raw := json.RawMessage(`{"candidate":{"k":8}}`)
				ctx.ConfigProfile = &types.ResolvedConfigProfile{RoutingConfig: raw, Routing: configprofiles.ParseRoutingConfig(raw), RequestsInflight: cap}
				before := mr.CommandCount()
				addr, err := server.selectTargetPod(context.Background(), ctx, &utils.PodArray{Pods: pods}, "")
				require.NoError(t, err)
				require.NotEmpty(t, addr)
				sent := mr.CommandCount() - before
				t.Logf("%d Redis commands", sent)
				if cap == 0 {
					assert.LessOrEqual(t, sent, 16, "no full-fleet Redis read in candidate seeding")
				} else {
					assert.GreaterOrEqual(t, sent, n, "inflight filter still checks every pod")
				}
			})
		}
	}
}
