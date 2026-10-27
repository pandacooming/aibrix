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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/vllm-project/aibrix/pkg/metrics"
)

const (
	testGatewaySelf = "gw-self"
	testPodName     = "pod-a"
	testNamespace   = "default"
)

func newGaugeTestPod(name string, running int32) *Pod {
	pod := &Pod{
		Pod: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
			Status: v1.PodStatus{
				PodIP:      "10.0.0.1",
				Conditions: []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionTrue}},
			},
		},
	}
	atomic.StoreInt32(&pod.runningRequests, running)
	return pod
}

// seedRemoteGateway records count running requests on a pod from another gateway, live
// (heartbeat now) or dead (heartbeat older than the liveness window).
func seedRemoteGateway(t *testing.T, client *redis.Client, gateway, podName string, count int, live bool) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, client.HSet(ctx, runningRequestsKey(testNamespace, podName), gateway, count).Err())
	beat := time.Now()
	if !live {
		beat = beat.Add(-10 * runningRequestsLivenessWindow)
	}
	require.NoError(t, client.ZAdd(ctx, runningRequestsGatewaysKey,
		redis.Z{Score: float64(beat.UnixMilli()), Member: gateway}).Err())
}

func gaugeValue(t *testing.T, store *Store, podName string) float64 {
	t.Helper()
	value, err := store.GetMetricValueByPod(podName, testNamespace, metrics.RealtimeNumRequestsRunning)
	require.NoError(t, err)
	return value.GetSimpleValue()
}

// The gauge reports this gateway's own running requests plus those of every live gateway,
// read from the running-request hashes, and leaves out dead gateways.
func TestRunningRequestsGauge_SumsLiveGateways(t *testing.T) {
	client := newTestRunningRequestsClient(t)
	store := &Store{redisClient: client}
	pod := newGaugeTestPod(testPodName, 4)
	store.metaPods.Store(testNamespace+"/"+testPodName, pod)

	seedRemoteGateway(t, client, "gw-remote", testPodName, 2, true)
	seedRemoteGateway(t, client, "gw-dead", testPodName, 7, false)

	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	require.EqualValues(t, 6, gaugeValue(t, store, testPodName))
}

// A pod no gateway has routed to has no hash; the gauge reports the local count.
func TestRunningRequestsGauge_NoHashReportsLocal(t *testing.T) {
	client := newTestRunningRequestsClient(t)
	store := &Store{redisClient: client}
	pod := newGaugeTestPod(testPodName, 3)
	store.metaPods.Store(testNamespace+"/"+testPodName, pod)

	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	require.EqualValues(t, 3, gaugeValue(t, store, testPodName))
}

// A failed read keeps the last counts read, so other gateways' requests do not drop out of the
// gauge, and the failure reaches the Error log.
func TestRunningRequestsGauge_FailedReadKeepsLastCounts(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	errLog, captured := newCapturingErrorLog(time.Hour)
	store := &Store{redisClient: client, runningRequestsGaugeErrorLog: errLog}
	pod := newGaugeTestPod(testPodName, 5)
	store.metaPods.Store(testNamespace+"/"+testPodName, pod)
	seedRemoteGateway(t, client, "gw-remote", testPodName, 2, true)

	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	require.EqualValues(t, 7, gaugeValue(t, store, testPodName))
	require.Empty(t, captured.snapshot(), "a successful read logs nothing at Error level")

	mr.Close()
	store.refreshRunningRequestsGaugeCounts()
	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	require.EqualValues(t, 7, gaugeValue(t, store, testPodName), "the remote gateway's 2 requests are kept")
	lines := captured.snapshot()
	require.Len(t, lines, 1, "repeated failures are logged once per interval")
	require.Contains(t, lines[0].msg, "keeping the last counts")
}

// Before any read has succeeded there is nothing to keep: nothing is published, and the metric
// is left alone rather than set to a local-only count that would look like a full picture.
func TestRunningRequestsGauge_FailedFirstReadPublishesNothing(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	errLog, captured := newCapturingErrorLog(time.Hour)
	store := &Store{redisClient: client, runningRequestsGaugeErrorLog: errLog}
	pod := newGaugeTestPod(testPodName, 5)
	store.metaPods.Store(testNamespace+"/"+testPodName, pod)

	mr.Close()
	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	_, err := store.GetMetricValueByPod(testPodName, testNamespace, metrics.RealtimeNumRequestsRunning)
	require.Error(t, err, "the metric is not set")
	lines := captured.snapshot()
	require.Len(t, lines, 1)
	require.Contains(t, lines[0].msg, "not publishing the first counts")
}

// syncRunningRequestsGlobally tells apart the two kinds of "no data": unset counts leave the
// metric alone, while a published read without a count for the pod is a real answer (no gateway
// has routed to it) and yields this gateway's local count.
func TestSyncRunningRequestsGlobally_GaugeCountsContract(t *testing.T) {
	podKey := testNamespace + "/" + testPodName
	const stale = 7.0 // what the metric held before the call
	tests := []struct {
		name   string
		counts map[string]int64 // nil leaves the counts unset
		want   float64
	}{
		{name: "unset counts leave the metric alone", counts: nil, want: stale},
		{name: "a pod without a count reports the local count", counts: map[string]int64{}, want: 3},
		{name: "a pod with a count reports it", counts: map[string]int64{podKey: 8}, want: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &Store{}
			if tt.counts != nil {
				store.runningRequestsGaugeCounts.Store(tt.counts)
			}
			pod := newGaugeTestPod(testPodName, 3)
			pod.Metrics.Store(metrics.RealtimeNumRequestsRunning, &metrics.SimpleMetricValue{Value: stale})

			store.syncRunningRequestsGlobally(pod)

			got, ok := pod.Metrics.Load(metrics.RealtimeNumRequestsRunning)
			require.True(t, ok)
			require.Equal(t, tt.want, got.GetSimpleValue())
		})
	}
}

// One refresh reads each ready pod once, plus the live gateways, whatever the number of
// gateways: the cost is O(N) per gateway, not O(G×N).
func TestRunningRequestsGauge_RefreshCostIndependentOfGateways(t *testing.T) {
	const pods, gateways = 50, 8
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := &Store{redisClient: client}
	for i := 0; i < pods; i++ {
		name := fmt.Sprintf("pod-%02d", i)
		store.metaPods.Store(testNamespace+"/"+name, newGaugeTestPod(name, 1))
		for g := 0; g < gateways; g++ {
			seedRemoteGateway(t, client, fmt.Sprintf("gw-%d", g), name, 1, true)
		}
	}
	// A pod that is not ready is not read.
	notReady := newGaugeTestPod("pod-not-ready", 1)
	notReady.Status.Conditions = nil
	store.metaPods.Store(testNamespace+"/pod-not-ready", notReady)

	before := mr.CommandCount()
	store.refreshRunningRequestsGaugeCounts()
	require.Equal(t, pods+1, mr.CommandCount()-before)

	pod, ok := store.metaPods.Load(testNamespace + "/pod-00")
	require.True(t, ok)
	store.syncRunningRequestsGlobally(pod)
	require.EqualValues(t, gateways+1, gaugeValue(t, store, "pod-00"))
}

// Without Redis there is nothing to aggregate: the gauge is left to the local tracking.
func TestRunningRequestsGauge_NoRedis(t *testing.T) {
	store := &Store{}
	pod := newGaugeTestPod(testPodName, 2)
	store.metaPods.Store(testNamespace+"/"+testPodName, pod)

	store.refreshRunningRequestsGaugeCounts()
	store.syncRunningRequestsGlobally(pod)
	_, err := store.GetMetricValueByPod(testPodName, testNamespace, metrics.RealtimeNumRequestsRunning)
	require.Error(t, err)
}

// capturedErrorLog records what a rateLimitedErrorLog would have written at Error level.
type capturedErrorLog struct {
	mu    sync.Mutex
	lines []capturedErrorLine
}

type capturedErrorLine struct {
	msg        string
	suppressed int
}

func (c *capturedErrorLog) emit(_ error, msg string, keysAndValues ...interface{}) {
	line := capturedErrorLine{msg: msg}
	for i := 0; i+1 < len(keysAndValues); i += 2 {
		if keysAndValues[i] == "suppressedSinceLastLog" {
			line.suppressed, _ = keysAndValues[i+1].(int)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, line)
}

func (c *capturedErrorLog) snapshot() []capturedErrorLine {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]capturedErrorLine(nil), c.lines...)
}

func newCapturingErrorLog(interval time.Duration) (*rateLimitedErrorLog, *capturedErrorLog) {
	captured := &capturedErrorLog{}
	l := newRateLimitedErrorLog(interval)
	l.emit = captured.emit
	return l, captured
}

// A failure that recurs every tick is logged once per interval, with a count of what it
// stands for; different kinds of failure are limited independently, so one cannot hide another.
func TestRateLimitedErrorLog(t *testing.T) {
	now := time.Unix(1000, 0)
	l, captured := newCapturingErrorLog(30 * time.Second)
	l.now = func() time.Time { return now }
	err := errors.New("boom")

	l.log("scan", err, "scan failed")
	l.log("scan", err, "scan failed")
	l.log("scan", err, "scan failed")
	l.log("read", err, "read failed")
	require.Equal(t, []capturedErrorLine{{"scan failed", 0}, {"read failed", 0}}, captured.snapshot(),
		"the first occurrence of each event is logged; repeats within the interval are not")

	now = now.Add(29 * time.Second)
	l.log("scan", err, "scan failed")
	require.Len(t, captured.snapshot(), 2, "still inside the interval")

	now = now.Add(time.Second)
	l.log("scan", err, "scan failed")
	lines := captured.snapshot()
	require.Len(t, lines, 3)
	require.Equal(t, capturedErrorLine{"scan failed", 3}, lines[2],
		"the next line says how many occurrences were swallowed since the last one")
}
