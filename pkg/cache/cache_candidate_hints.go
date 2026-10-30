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
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/vllm-project/aibrix/pkg/utils"
	v1 "k8s.io/api/core/v1"
)

// CachedRunningRequestHintProvider is optional. Implementations return only
// counts with a known scope and bounded observation age. Missing keys mean
// unknown, never zero. These approximate hints must not enter live scoring or
// admission snapshots and must never perform request-path Redis calls.
type CachedRunningRequestHintProvider interface {
	GetCachedRunningRequestHints(pods []*v1.Pod) map[string]int64
}

// Two background ticks is the current experimental recommendation window,
// tighter than Redis's retention TTL. It is not a user configuration option.
const candidateHintMaxObservationAge = 2 * gatewaySnapshotSyncInterval

type candidateObservation struct {
	progressAt           time.Time
	generation, identity string
	readAt, sampleAfter  time.Time
}

type candidateObservationTracker map[string]candidateObservation

// update bounds source age without comparing clocks on different gateways.
// A changed generation was written after the preceding successful read began.
// First sightings and unchanged, repeatedly-read rows do not establish a new
// sample. readStarts must precede the corresponding Redis reads.
func (t candidateObservationTracker) update(raw map[string]map[string]string, readStarts map[string]time.Time, selfGateway string) map[string]time.Time {
	observed := make(map[string]time.Time, len(raw))
	for key := range t {
		if _, exists := raw[key]; !exists {
			delete(t, key)
		}
	}
	for key, fields := range raw {
		old := t[key]
		if at, fresh := readStarts[key]; fresh {
			identity := fields["gateway_instance_id"] + "/" + fields["pod_uid"]
			generation := ""
			if stamp := fields["update_time"]; stamp != "" {
				generation = stamp + "/" + fields["requests_running"] + "/" + fields["seq"]
			}
			next := candidateObservation{generation: generation, identity: identity, readAt: at, progressAt: at}
			if fields["gateway_instance_id"] == selfGateway {
				// The self contribution is replaced by the live local atomic.
				next.sampleAfter = at
			} else if identity == old.identity && generation != "" && old.generation != "" {
				next.sampleAfter = old.sampleAfter
				next.progressAt = old.progressAt
				if generation != old.generation {
					next.progressAt = at
					next.sampleAfter = time.Time{}
					if !old.progressAt.IsZero() && at.Sub(old.progressAt) < candidateHintMaxObservationAge {
						next.sampleAfter = old.readAt
					}
				}
			}
			t[key] = next
			old = next
		}
		observed[key] = old.sampleAfter
	}
	return observed
}

type candidateRequestSnapshot struct {
	at   time.Time
	pods map[string]candidatePodRequests
}
type candidatePodRequests struct {
	uid      string
	remote   int64
	observed time.Time
	valid    bool
}

func buildCandidateRequestSnapshot(raw map[string]map[string]string, observed map[string]time.Time, now time.Time, selfGateway string) *candidateRequestSnapshot {
	out := &candidateRequestSnapshot{at: now, pods: make(map[string]candidatePodRequests, len(raw))}
	for key, fields := range raw {
		if fields["namespace"] == "" || fields["pod_name"] == "" {
			continue
		}
		podKey := utils.GeneratePodKey(fields["namespace"], fields["pod_name"])
		value, seen := out.pods[podKey]
		if !seen {
			value = candidatePodRequests{uid: fields["pod_uid"], valid: true}
		}
		if value.uid != fields["pod_uid"] || fields["gateway_instance_id"] == "" {
			value.valid = false
		}
		at := observed[key]
		if at.IsZero() || at.After(now) {
			value.valid = false
		}
		if value.observed.IsZero() || at.Before(value.observed) {
			value.observed = at
		}
		if fields["gateway_instance_id"] != selfGateway {
			n, err := strconv.ParseInt(fields["requests_running"], 10, 64)
			if err != nil || n < 0 || n > math.MaxInt64-value.remote {
				value.valid = false
			} else {
				value.remote += n
			}
		}
		out.pods[podKey] = value
	}
	return out
}

// GetCachedRunningRequestHints reads the current local counter plus remote
// contributions from the existing background gateway snapshot sync. Unlike
// RealtimeNumRequestsRunning, local request updates cannot overwrite the remote
// contribution. Reader-local ages avoid depending on gateway clock agreement.
//
// Absent, malformed, identity-mismatched or expired observations are omitted.
// Without Redis there is no evidence of other gateways' contributions, so this
// returns unknown rather than presenting local-only counts as global hints.
// Reader observation age is not source sample age or an admission guarantee.
func (c *Store) GetCachedRunningRequestHints(pods []*v1.Pod) map[string]int64 {
	return c.cachedRunningRequestHintsAt(pods, time.Now())
}
func (c *Store) cachedRunningRequestHintsAt(pods []*v1.Pod, now time.Time) map[string]int64 {
	out := make(map[string]int64, len(pods))
	if c.redisClient == nil {
		return out
	}
	// Keep the additional background aggregation dormant until a consumer
	// requests hints. The first caller may wait for the next existing tick.
	if !c.candidateHintsRequested.Load() {
		c.candidateHintsRequested.Store(true)
	}
	snapshot := c.candidateRequestSnapshot.Load()
	if snapshot == nil || now.Before(snapshot.at) || now.Sub(snapshot.at) >= candidateHintMaxObservationAge {
		return out
	}
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		key := utils.GeneratePodKey(pod.Namespace, pod.Name)
		meta, ok := c.metaPods.Load(key)
		if !ok || meta.UID != pod.UID {
			continue
		}
		local := int64(atomic.LoadInt32(&meta.runningRequests))
		if local < 0 {
			continue
		}
		remote, ok := snapshot.pods[key]
		if !ok || !remote.valid || remote.uid != string(pod.UID) || remote.observed.IsZero() || now.Before(remote.observed) || now.Sub(remote.observed) >= candidateHintMaxObservationAge || remote.remote > math.MaxInt64-local {
			continue
		}
		out[key] = local + remote.remote
	}
	return out
}
