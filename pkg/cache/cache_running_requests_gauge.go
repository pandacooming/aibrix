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
	"errors"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	"github.com/vllm-project/aibrix/pkg/utils"
)

// runningRequestsGaugeErrorLogInterval is how often a failing gauge read is logged at Error
// level. The read runs every metrics refresh, so an outage would otherwise either flood the log
// or, at V(4), stay invisible while the gauge silently stops refreshing.
const runningRequestsGaugeErrorLogInterval = 30 * time.Second

var errRunningRequestsGaugeRead = errors.New("failed to read the running-request counters")

// refreshRunningRequestsGaugeCounts reads the cross-gateway running-request count of every
// ready pod from the running-request hashes, in one pipelined read, for
// syncRunningRequestsGlobally to report during this metrics refresh. These are the counts
// routing and the inflight cap read, so the gauge reports what routing sees, and the cost is
// one read of N pods per gateway per refresh, independent of the number of gateways.
//
// A failed read keeps the counts of the last successful one rather than replacing them with
// an empty map, which would drop every other gateway's requests from the gauge until the next
// success. Before the first successful read there is nothing to keep, so nothing is published
// and syncRunningRequestsGlobally leaves the metric alone.
func (c *Store) refreshRunningRequestsGaugeCounts() {
	if c.redisClient == nil {
		return
	}
	var pods []*v1.Pod
	c.metaPods.Range(func(_ string, metaPod *Pod) bool {
		if utils.FilterReadyPod(metaPod.Pod) {
			pods = append(pods, metaPod.Pod)
		}
		return true
	})
	if len(pods) == 0 {
		c.runningRequestsGaugeCounts.Store(map[string]int64{})
		return
	}
	// readPodsRunningRequests returns nil only when the read failed (it logs why at V(4)).
	counts := c.readPodsRunningRequests(pods)
	if counts == nil {
		if c.runningRequestsGaugeErrorLog == nil {
			c.runningRequestsGaugeErrorLog = newRateLimitedErrorLog(runningRequestsGaugeErrorLogInterval)
		}
		msg := "running-requests gauge cannot read Redis; keeping the last counts read"
		if c.runningRequestsGaugeCounts.Load() == nil {
			msg = "running-requests gauge cannot read Redis; not publishing the first counts yet"
		}
		c.runningRequestsGaugeErrorLog.log("read", errRunningRequestsGaugeRead, msg, "pods", len(pods))
		return
	}
	c.runningRequestsGaugeCounts.Store(counts)
}

// rateLimitedErrorLog emits an Error-level line for a recurring failure at most once per
// interval for each named event, and says how many occurrences the line stands for. It is for
// a loop that retries on every tick: logging each failure would flood the log, and logging none
// at Error level would leave a value that has silently stopped refreshing invisible.
// Not safe for concurrent use; the metrics refresh goroutine is its only caller.
type rateLimitedErrorLog struct {
	interval time.Duration
	now      func() time.Time
	emit     func(err error, msg string, keysAndValues ...interface{})
	events   map[string]*rateLimitedErrorEvent
}

type rateLimitedErrorEvent struct {
	last       time.Time
	suppressed int
}

func newRateLimitedErrorLog(interval time.Duration) *rateLimitedErrorLog {
	return &rateLimitedErrorLog{
		interval: interval,
		now:      time.Now,
		emit:     klog.ErrorS,
		events:   map[string]*rateLimitedErrorEvent{},
	}
}

// log emits msg for event unless one was emitted for it less than interval ago, in which case
// the occurrence is only counted toward the next line's "suppressed" value.
func (l *rateLimitedErrorLog) log(event string, err error, msg string, keysAndValues ...interface{}) {
	ev := l.events[event]
	if ev == nil {
		ev = &rateLimitedErrorEvent{}
		l.events[event] = ev
	}
	now := l.now()
	if !ev.last.IsZero() && now.Sub(ev.last) < l.interval {
		ev.suppressed++
		return
	}
	keysAndValues = append(keysAndValues, "suppressedSinceLastLog", ev.suppressed)
	ev.last, ev.suppressed = now, 0
	l.emit(err, msg, keysAndValues...)
}
