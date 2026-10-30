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

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
)

// Opt-in source-timing probe. Run each N/gateway combination in a separate
// process against a fresh disposable Redis; it refuses a nonempty database.
// It measures source availability, not inference latency or production capacity.
func TestCandidateHintsRedisAvailability(t *testing.T) {
	addr := os.Getenv("AIBRIX_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set AIBRIX_TEST_REDIS_ADDR to a fresh disposable Redis")
	}
	setting := func(name string, fallback int) int {
		if value := os.Getenv(name); value != "" {
			n, err := strconv.Atoi(value)
			require.NoError(t, err)
			require.Positive(t, n)
			return n
		}
		return fallback
	}
	n := setting("AIBRIX_TEST_CANDIDATE_PODS", 500)
	gateways := setting("AIBRIX_TEST_CANDIDATE_GATEWAYS", 4)
	client := redis.NewClient(&redis.Options{Addr: addr, MaxRetries: -1})
	ctx, cancel := context.WithCancel(context.Background())
	size, err := client.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size, "use an empty disposable Redis, not a shared service")
	store := NewForTest()
	store.redisClient = client
	pods := make([]*v1.Pod, n)
	for i := range pods {
		pods[i] = &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "candidate-availability", Name: fmt.Sprintf("p-%04d", i), UID: ktypes.UID(fmt.Sprint(i))}}
	}
	InitWithPods(store, pods, "availability")
	stop := make(chan struct{})
	var writers sync.WaitGroup
	t.Cleanup(func() { cancel(); writers.Wait(); close(stop); store.Close(); _ = client.Close() })
	for gateway := 1; gateway < gateways; gateway++ {
		id := fmt.Sprintf("availability-remote-%d", gateway)
		writers.Add(1)
		go func() {
			defer writers.Done()
			tick := time.NewTicker(gatewaySnapshotSyncInterval)
			defer tick.Stop()
			for generation := 0; ; generation++ {
				pipe := client.Pipeline()
				for i, pod := range pods {
					key := gatewayPodSnapshotKey(id, pod.Namespace, pod.Name)
					pipe.HSet(ctx, key, map[string]any{
						"gateway_instance_id": id, "namespace": pod.Namespace, "pod_name": pod.Name,
						"pod_uid": string(pod.UID), "requests_running": i % 13,
						"seq": "0", "update_time": strconv.Itoa(generation),
					})
					pipe.PExpire(ctx, key, gatewayPodSnapshotTTL)
				}
				if _, err := pipe.Exec(ctx); err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
				}
			}
		}()
	}
	// Enable the existing background source, rather than installing a fixture snapshot.
	store.GetCachedRunningRequestHints(pods)
	initGatewaySnapshotSync(store, stop)
	rng := rand.New(rand.NewSource(3353))
	started := time.Now()
	warmup := 2 * time.Second
	finish := started.Add(warmup + 3*time.Second)
	var samples, full, available, scoutAny, scoutFour int
	var usableTotal int
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for now := range tick.C {
		if !now.Before(finish) {
			break
		}
		hints := store.GetCachedRunningRequestHints(pods)
		if now.Sub(started) < warmup {
			continue
		}
		samples++
		usableTotal += len(hints)
		if len(hints) > 0 {
			available++
		}
		if len(hints) == n {
			full++
		}
		known := 0
		for _, i := range rng.Perm(n)[:min(n, 64)] {
			if _, ok := hints[utils.GeneratePodKey(pods[i].Namespace, pods[i].Name)]; ok {
				known++
			}
		}
		if known > 0 {
			scoutAny++
		}
		if known >= 4 {
			scoutFour++
		}
	}
	require.Positive(t, samples)
	data, err := json.Marshal(map[string]any{
		"pods": n, "gateways": gateways, "samples": samples,
		"full_fleet_fraction":      float64(full) / float64(samples),
		"any_hint_fraction":        float64(available) / float64(samples),
		"mean_usable_pod_fraction": float64(usableTotal) / float64(n*samples),
		"scout64_any_fraction":     float64(scoutAny) / float64(samples),
		"scout64_four_fraction":    float64(scoutFour) / float64(samples),
	})
	require.NoError(t, err)
	t.Logf("CANDIDATE_HINT_AVAILABILITY %s", data)
}
