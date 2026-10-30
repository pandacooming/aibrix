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

package candidateintegration

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/cache/discovery"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/metrics"
	routing "github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
)

type emptyDiscovery struct{}

func (emptyDiscovery) Type() string                                        { return "candidate-integration" }
func (emptyDiscovery) Watch(discovery.EventHandler, <-chan struct{}) error { return nil }

type observedStore struct {
	*cache.Store
	hintCount, liveReads, livePods int
}

func (c *observedStore) GetCachedRunningRequestHints(pods []*v1.Pod) map[string]int64 {
	got := c.Store.GetCachedRunningRequestHints(pods)
	c.hintCount = len(got)
	return got
}
func (c *observedStore) GetPodsRunningRequests(pods []*v1.Pod) (map[string]int64, error) {
	c.liveReads++
	c.livePods += len(pods)
	return c.Store.GetPodsRunningRequests(pods)
}

// This package has its own process, so cache.InitWithOptions uses real
// background synchronization without interfering with cache singleton tests.
func TestObservedSourceThroughCandidateSelectionAndFinalScoring(t *testing.T) {
	t.Setenv("PROMETHEUS_ENDPOINT", "")
	t.Setenv("PROMETHEUS_BASIC_AUTH_SECRET_NAME", "")
	t.Setenv(constants.EnvPrefixCacheUseRemoteTokenizer, "false")
	t.Setenv(constants.EnvPrefixCacheKVEventSyncEnabled, "false")
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	stop := make(chan struct{})
	base := cache.InitWithOptions(nil, stop, cache.InitOptions{RedisClient: client, DiscoveryProvider: emptyDiscovery{}})
	t.Cleanup(func() { close(stop); base.Close() })
	pods := make([]*v1.Pod, 64)
	values := make(map[string]map[string]metrics.MetricValue)
	for i := range pods {
		name := fmt.Sprintf("worker-%03d", i)
		pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "native", UID: ktypes.UID(name), Labels: map[string]string{constants.ModelLabelPort: "8000"}}, Status: v1.PodStatus{PodIP: "127.0.0.1"}}
		values[name] = map[string]metrics.MetricValue{
			metrics.RealtimeNumRequestsRunning:  &metrics.SimpleMetricValue{Value: 0},
			metrics.RealtimeOutputTokenRateEWMA: &metrics.SimpleMetricValue{Value: 1000},
			metrics.KVCacheUsagePerc:            &metrics.SimpleMetricValue{Value: 0},
		}
	}
	cache.InitWithPods(base, pods, "m")
	cache.InitWithPodsMetrics(base, values)
	cache.InitWithPodsModelMetrics(base, values)
	producerStop, producerDone := make(chan struct{}), make(chan struct{})
	var stopOnce sync.Once
	stopProducer := func() { stopOnce.Do(func() { close(producerStop) }); <-producerDone }
	t.Cleanup(stopProducer)
	go func() {
		defer close(producerDone)
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for generation := 0; ; generation++ {
			pipe := client.Pipeline()
			for i, p := range pods {
				// Deliberately no TTL: after freezing this writer, repeated
				// successful Redis reads must not keep the sample fresh.
				pipe.HSet(context.Background(), "aibrix:pod:remote-test:"+p.Namespace+":"+p.Name, map[string]any{
					"gateway_instance_id": "remote-test", "namespace": p.Namespace, "pod_name": p.Name, "pod_uid": string(p.UID),
					"requests_running": strconv.Itoa(i), "seq": "0", "update_time": strconv.Itoa(generation),
				})
			}
			if _, err := pipe.Exec(context.Background()); err != nil {
				return
			}
			select {
			case <-producerStop:
				return
			case <-tick.C:
			}
		}
	}()
	require.Eventually(t, func() bool { return len(base.GetCachedRunningRequestHints(pods)) == len(pods) }, 3*time.Second, 10*time.Millisecond)
	c := &observedStore{Store: base}
	rm := routing.NewRouterManagerWithCache(c)
	rm.Init()
	ctx := types.NewRoutingContext(context.Background(), "least-request", "m", "prompt", "native-integration", "")
	defer ctx.Delete()
	ov := *types.DefaultRoutingOverrides()
	ov.Candidate = types.CandidateOverrides{K: 16}
	ov.AutoBlend.LoadBalanceWeight, ov.AutoBlend.LeastRequestWeight, ov.AutoBlend.PrefixCacheLoadBalanceWeight = 0, 0, 0
	ctx.SetRoutingOverrides(&ov)
	router, err := rm.Select(ctx)
	require.NoError(t, err)
	ctx.StartRunningRequestsSnapshot()
	defer ctx.StopRunningRequestsSnapshot()
	var selected []*v1.Pod
	require.Eventually(t, func() bool { selected = routing.SelectCandidates(ctx, router, pods); return c.hintCount == 64 }, 3*time.Second, 10*time.Millisecond)
	require.Len(t, selected, 16)
	for _, p := range pods[:4] {
		require.Contains(t, selected, p, "observed remote counts must drive recommendations")
	}
	require.Zero(t, c.liveReads, "candidate selection cannot prefill final live-count state")
	eligible := routing.ApplyLoadImbalanceGate(ctx, c, selected)
	_, err = router.Route(ctx, &utils.PodArray{Pods: eligible})
	require.NoError(t, err)
	require.NotNil(t, ctx.TargetPod())
	require.Equal(t, 1, c.liveReads, "gate and final router should share one live read")
	require.Equal(t, 16, c.livePods)
	live, err := ctx.RunningRequests(selected, c.GetPodsRunningRequests)
	require.NoError(t, err)
	for _, n := range live {
		require.Zero(t, n, "approximate remote snapshot counts must not leak into live scoring")
	}
	stopProducer()
	require.True(t, mr.Exists("aibrix:pod:remote-test:native:"+pods[0].Name), "the stale row must remain readable for this regression")
	require.Eventually(t, func() bool { return len(base.GetCachedRunningRequestHints(pods)) == 0 }, 3*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return len(base.GetCachedRunningRequestHints(pods)) > 0 }, 250*time.Millisecond, 10*time.Millisecond)
	isolated := client.Pipeline()
	for _, p := range pods {
		isolated.HSet(context.Background(), "aibrix:pod:remote-test:"+p.Namespace+":"+p.Name, "update_time", "isolated-update")
	}
	_, err = isolated.Exec(context.Background())
	require.NoError(t, err)
	require.Never(t, func() bool { return len(base.GetCachedRunningRequestHints(pods)) > 0 }, 300*time.Millisecond, 10*time.Millisecond, "one fresh sample after a slow gap must not oscillate back to recommendations")
	seen := make(map[*v1.Pod]bool)
	for i := 0; i < 100; i++ {
		got := routing.SelectCandidates(ctx, router, pods)
		require.Len(t, got, 16)
		for _, p := range got {
			seen[p] = true
		}
	}
	require.Greater(t, len(seen), 16, "expired hints must fall back to exploration")
	require.Equal(t, 1, c.liveReads, "fallback must also avoid request-path live reads")
}
