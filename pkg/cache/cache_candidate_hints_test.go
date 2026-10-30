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
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vllm-project/aibrix/pkg/metrics"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func candidateHintStore(t *testing.T) (*Store, *Pod, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	pod := &Pod{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "hint", Namespace: "ns", UID: "uid-hint"}}}
	store := &Store{redisClient: client}
	store.metaPods.Store("ns/hint", pod)
	atomic.StoreInt32(&pod.runningRequests, 3)
	return store, pod, mr
}
func candidateHintFields(gateway, count string) map[string]string {
	return map[string]string{"gateway_instance_id": gateway, "namespace": "ns", "pod_name": "hint", "pod_uid": "uid-hint", "requests_running": count}
}
func TestCachedRunningRequestHintsPreserveRemoteContribution(t *testing.T) {
	store, pod, mr := candidateHintStore(t)
	now := time.Unix(1000, 0)
	raw := map[string]map[string]string{"self": candidateHintFields(gatewayPodName, "999"), "remote-a": candidateHintFields("a", "5"), "remote-b": candidateHintFields("b", "7")}
	observed := map[string]time.Time{"self": now, "remote-a": now, "remote-b": now}
	store.candidateRequestSnapshot.Store(buildCandidateRequestSnapshot(raw, observed, now, gatewayPodName))
	// Same slot overwritten by local request updates in cache_trace.go.
	pod.Metrics.Store(metrics.RealtimeNumRequestsRunning, &metrics.SimpleMetricValue{Value: 3})
	before := mr.CommandCount()
	require.Equal(t, int64(15), store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now)["ns/hint"])
	atomic.StoreInt32(&pod.runningRequests, 6)
	pod.Metrics.Store(metrics.RealtimeNumRequestsRunning, &metrics.SimpleMetricValue{Value: 6})
	require.Equal(t, int64(18), store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now)["ns/hint"])
	require.Equal(t, before, mr.CommandCount(), "candidate hints must not call Redis")
	require.Equal(t, "5", raw["remote-a"]["requests_running"], "published inputs stay immutable")
}
func TestCachedRunningRequestHintsRejectUnknownOrStale(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	now := time.Unix(1000, 0)
	pods := []*v1.Pod{pod.Pod}
	require.Empty(t, store.cachedRunningRequestHintsAt(pods, now), "no first snapshot is unknown, not zero or local-only")
	for _, tc := range []struct {
		name, count, uid string
		age              time.Duration
	}{
		{"fresh", "5", "uid-hint", 100 * time.Millisecond},
		{"fresh-boundary", "5", "uid-hint", candidateHintMaxObservationAge - time.Nanosecond},
		{"observation-expired", "5", "uid-hint", candidateHintMaxObservationAge},
		{"expired", "5", "uid-hint", gatewayPodSnapshotTTL},
		{"clock-future", "5", "uid-hint", -time.Second},
		{"wrong-pod", "5", "different-uid", 0},
		{"malformed", "not-a-number", "uid-hint", 0},
		{"negative", "-1", "uid-hint", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := candidateHintFields("a", tc.count)
			fields["pod_uid"] = tc.uid
			snapshot := buildCandidateRequestSnapshot(map[string]map[string]string{"a": fields}, map[string]time.Time{"a": now.Add(-tc.age)}, now, gatewayPodName)
			store.candidateRequestSnapshot.Store(snapshot)
			got := store.cachedRunningRequestHintsAt(pods, now)
			if tc.name == "fresh" || tc.name == "fresh-boundary" {
				require.Equal(t, int64(8), got["ns/hint"])
			} else {
				require.Empty(t, got)
			}
		})
	}
	// Republishing a retained remote row does not renew its observation age.
	snapshot := buildCandidateRequestSnapshot(map[string]map[string]string{"a": candidateHintFields("a", "5")}, map[string]time.Time{"a": now}, now.Add(time.Second), gatewayPodName)
	store.candidateRequestSnapshot.Store(snapshot)
	require.Empty(t, store.cachedRunningRequestHintsAt(pods, now.Add(time.Second)))
}
func TestCachedRunningRequestHintsIdentityAndNoRedis(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	now := time.Unix(1000, 0)
	store.candidateRequestSnapshot.Store(buildCandidateRequestSnapshot(map[string]map[string]string{"a": candidateHintFields("a", "5")}, map[string]time.Time{"a": now}, now, gatewayPodName))
	require.Equal(t, map[string]int64{"ns/hint": 8}, store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now))
	replacement := pod.DeepCopy()
	replacement.UID = "replacement"
	require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{replacement, nil}, now))
	atomic.StoreInt32(&pod.runningRequests, -1)
	require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now))
	atomic.StoreInt32(&pod.runningRequests, 3)
	store.redisClient = nil
	require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now), "local-only scope must not masquerade as a cross-gateway hint")
}
func TestSnapshotObservationDoesNotRefreshFailedRead(t *testing.T) {
	store, _, _ := candidateHintStore(t)
	client := store.redisClient
	hook := &snapshotFaultHook{}
	client.AddHook(hook)
	ctx := context.Background()
	key := gatewayPodSnapshotKey("a", "ns", "hint")
	require.NoError(t, client.HSet(ctx, key, candidateHintFields("a", "5")).Err())
	observed := []string{}
	readBefore := time.Now()
	raw, unread, err := readGatewaySnapshotsObserved(ctx, client, []string{key}, nil, func(k string, at time.Time) {
		require.False(t, at.Before(readBefore))
		require.False(t, at.After(time.Now()))
		observed = append(observed, k)
	})
	require.NoError(t, err)
	require.False(t, unread)
	require.Equal(t, []string{key}, observed)
	hook.failReads.Store(true)
	observed = nil
	retained, unread, err := readGatewaySnapshotsObserved(ctx, client, []string{key}, raw, func(k string, _ time.Time) { observed = append(observed, k) })
	require.Error(t, err)
	require.False(t, unread)
	require.Empty(t, observed)
	require.Equal(t, raw, retained)
}
func TestCachedRunningRequestHintsBackgroundSource(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	ctx := context.Background()
	key := gatewayPodSnapshotKey("a", "ns", "hint")
	require.NoError(t, store.redisClient.HSet(ctx, key, candidateHintFields("a", "5")).Err())
	hook := &snapshotFaultHook{}
	store.redisClient.AddHook(hook)
	stop := make(chan struct{})
	initGatewaySnapshotSync(store, stop)
	t.Cleanup(func() { close(stop) })
	require.Eventually(t, func() bool {
		fields := candidateHintFields("a", "5")
		fields["update_time"] = time.Now().String()
		_ = store.redisClient.HSet(ctx, key, fields).Err()
		return store.GetCachedRunningRequestHints([]*v1.Pod{pod.Pod})["ns/hint"] == 8
	}, 2*time.Second, 10*time.Millisecond)
	atomic.StoreInt32(&pod.runningRequests, 7)
	require.Equal(t, int64(12), store.GetCachedRunningRequestHints([]*v1.Pod{pod.Pod})["ns/hint"])
	// Stop publication updates at the Redis read boundary; the existing metrics
	// snapshot can stay retained, but candidate hints must expire independently.
	hook.failScan.Store(true)
	require.Eventually(t, func() bool { return len(store.GetCachedRunningRequestHints([]*v1.Pod{pod.Pod})) == 0 }, 2*time.Second, 20*time.Millisecond)
}
func TestCandidateHintRemoteOverflow(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	now := time.Unix(1000, 0)
	raw := map[string]map[string]string{"a": candidateHintFields("a", strconv.FormatInt(1<<63-1, 10)), "b": candidateHintFields("b", "1")}
	store.candidateRequestSnapshot.Store(buildCandidateRequestSnapshot(raw, map[string]time.Time{"a": now, "b": now}, now, gatewayPodName))
	require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now))
}

func TestCandidateHintsRejectMixedGatewayPodIdentities(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	now := time.Unix(1000, 0)
	for _, missingGateway := range []bool{false, true} {
		name := "recreated-pod"
		if missingGateway {
			name = "missing-gateway"
		}
		t.Run(name, func(t *testing.T) {
			a, b := candidateHintFields("a", "5"), candidateHintFields("b", "7")
			if missingGateway {
				b["gateway_instance_id"] = ""
			} else {
				b["pod_uid"] = "replacement"
			}
			snapshot := buildCandidateRequestSnapshot(map[string]map[string]string{"a": a, "b": b}, map[string]time.Time{"a": now, "b": now}, now, gatewayPodName)
			require.False(t, snapshot.pods["ns/hint"].valid, "mixed identity cannot establish a scoped count")
			store.candidateRequestSnapshot.Store(snapshot)
			require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{pod.Pod}, now))
		})
	}
	// The remote observation already describes the replacement, but the local
	// counter still belongs to the old Pod with the same namespace/name.
	replacement := pod.DeepCopy()
	replacement.UID = "replacement"
	fields := candidateHintFields("a", "5")
	fields["pod_uid"] = string(replacement.UID)
	store.candidateRequestSnapshot.Store(buildCandidateRequestSnapshot(map[string]map[string]string{"a": fields}, map[string]time.Time{"a": now}, now, gatewayPodName))
	require.Empty(t, store.cachedRunningRequestHintsAt([]*v1.Pod{replacement}, now), "old local counts must not be combined with a new remote identity")
}

func BenchmarkCandidateHintSources(b *testing.B) {
	now := time.Unix(1000, 0)
	for _, n := range []int{50, 500, 2000} {
		c := &Store{redisClient: &redis.Client{}}
		snapshot := &candidateRequestSnapshot{at: now, pods: make(map[string]candidatePodRequests, n)}
		all := make([]*v1.Pod, 0, n)
		for i := 0; i < n; i++ {
			p := &Pod{Pod: &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("p-%d", i), Namespace: "ns", UID: "uid"}}}
			atomic.StoreInt32(&p.runningRequests, int32(i%7))
			key := "ns/" + p.Name
			p.Metrics.Store(metrics.RealtimeNumRequestsRunning, &metrics.SimpleMetricValue{Value: float64(i%7 + 3)})
			c.metaPods.Store(key, p)
			all = append(all, p.Pod)
			snapshot.pods[key] = candidatePodRequests{uid: "uid", remote: 3, observed: now, valid: true}
		}
		c.candidateRequestSnapshot.Store(snapshot)
		sample := all[:min(64, n)]
		for _, mode := range []string{"metric-slot", "observed-snapshot"} {
			b.Run(fmt.Sprintf("N=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for j := 0; j < b.N; j++ {
					var got map[string]int64
					if mode == "observed-snapshot" {
						got = c.cachedRunningRequestHintsAt(sample, now)
					} else {
						got = make(map[string]int64, len(sample))
						for _, p := range sample {
							v, err := c.GetMetricValueByPod(p.Name, p.Namespace, metrics.RealtimeNumRequestsRunning)
							if err != nil || math.IsNaN(v.GetSimpleValue()) {
								b.Fatal(err)
							}
							got["ns/"+p.Name] = int64(v.GetSimpleValue())
						}
					}
					if len(got) != len(sample) {
						b.Fatal("missing hint")
					}
				}
			})
		}
	}
}

func TestCandidateHintAggregationStartsOnDemand(t *testing.T) {
	store, pod, _ := candidateHintStore(t)
	key := gatewayPodSnapshotKey("a", "ns", "hint")
	require.NoError(t, store.redisClient.HSet(context.Background(), key, candidateHintFields("a", "5")).Err())
	stop := make(chan struct{})
	initGatewaySnapshotSync(store, stop)
	t.Cleanup(func() { close(stop) })
	require.Eventually(t, func() bool { return store.gatewaySnapshotCache.Load() != nil }, 2*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return store.candidateRequestSnapshot.Load() != nil }, 3*gatewaySnapshotSyncInterval, 20*time.Millisecond)
	require.Eventually(t, func() bool {
		fields := candidateHintFields("a", "5")
		fields["update_time"] = time.Now().String()
		_ = store.redisClient.HSet(context.Background(), key, fields).Err()
		return store.GetCachedRunningRequestHints([]*v1.Pod{pod.Pod})["ns/hint"] == 8
	}, 2*time.Second, 10*time.Millisecond)
}

func TestCandidateObservationRequiresSourceProgress(t *testing.T) {
	now := time.Unix(1000, 0)
	tracker := make(candidateObservationTracker)
	fields := candidateHintFields("remote", "5")
	fields["update_time"] = "remote-clock-can-be-anything"
	raw := map[string]map[string]string{"remote": fields}
	first := tracker.update(raw, map[string]time.Time{"remote": now}, "self")
	require.True(t, first["remote"].IsZero(), "first read cannot establish sample age")
	again := tracker.update(raw, map[string]time.Time{"remote": now.Add(100 * time.Millisecond)}, "self")
	require.True(t, again["remote"].IsZero(), "re-reading the same row must not freshen it")
	fields["update_time"] = "remote-clock-can-even-go-backward"
	progress := tracker.update(raw, map[string]time.Time{"remote": now.Add(150 * time.Millisecond)}, "self")
	require.Equal(t, now.Add(100*time.Millisecond), progress["remote"], "use the preceding read start as the conservative sample bound")
	retained := tracker.update(raw, nil, "self")
	require.Equal(t, progress, retained, "failed reads do not renew age")
	again = tracker.update(raw, map[string]time.Time{"remote": now.Add(time.Second)}, "self")
	require.Equal(t, progress, again, "successful reads of unchanged data also do not renew age")
	fields["pod_uid"] = "replacement"
	require.True(t, tracker.update(raw, map[string]time.Time{"remote": now.Add(2 * time.Second)}, "self")["remote"].IsZero(), "pod replacement must warm the source again")
}

func TestCandidateObservationRejectsIsolatedSlowUpdates(t *testing.T) {
	now := time.Unix(1000, 0)
	tracker := make(candidateObservationTracker)
	fields := candidateHintFields("remote", "5")
	raw := map[string]map[string]string{"remote": fields}
	update := func(at time.Time, generation string) time.Time {
		fields["update_time"] = generation
		return tracker.update(raw, map[string]time.Time{"remote": at}, "self")["remote"]
	}
	require.True(t, update(now, "0").IsZero())
	require.Equal(t, now, update(now.Add(100*time.Millisecond), "1"))
	require.True(t, update(now.Add(time.Second), "2").IsZero(), "an isolated fresh row after a slow gap must remain in fallback")
	require.True(t, update(now.Add(1050*time.Millisecond), "2").IsZero(), "reading it again cannot establish reliable cadence")
	require.Equal(t, now.Add(1050*time.Millisecond), update(now.Add(1100*time.Millisecond), "3"), "consecutive timely progress restores recommendations")
}
