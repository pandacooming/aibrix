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

// Package candidatesim compares candidate-selection policies for gateway routing (RFC #3188)
// against full scoring in a discrete-time simulation. It holds only tests: run it with
//
//	CANDIDATE_SIM=1 go test -run TestCandidateSelectionQuality -v ./pkg/plugins/gateway/algorithms/candidatesim/
//
// The model, deliberately simple:
//   - Pods have heterogeneous capacity (output tokens/s) and serve their running requests by
//     processor sharing.
//   - Several gateways route round-robin arrivals. Each sees its own picks live and the other
//     gateways' load only as of the last refresh, every SignalDelay (0 = always exact).
//   - The load score mirrors load-balance: running / capacity, lower is better.
//   - Before any scoring, the gateway's load-imbalance gate runs on the counts the gateway
//     observes: when max-min >= ImbalanceMinGap and max > ImbalanceFactor*(mean of the others+1),
//     only the pods at the minimum count stay eligible (ApplyLoadImbalanceGate). Every policy
//     picks its candidates from the pods the gate leaves, except with GateSample, which draws
//     the candidates first and gates only them, as the gateway's candidate stage does.
//   - With prefix caching on, each request belongs to a Zipf-distributed prefix group. A pod
//     holding the group's prefix serves the request with less work, and the blend adds an
//     equal-weight prefix term after min-max normalizing load over the scored pods, as
//     multiStrategyRouter does.
//
// For every request the simulation also computes what full scoring would have chosen on the
// same state, so "agreement" isolates the candidate stage's effect on each decision.
package candidatesim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type simConfig struct {
	Pods        int
	Gateways    int
	Utilization float64
	MeanWork    float64 // output tokens per request
	// HeavyTail draws 10% of requests with a mean of 5.5x MeanWork and the rest with 0.5x, keeping
	// the overall mean; otherwise request work is exponential.
	HeavyTail     bool
	PrefixOn      bool
	PrefixGroups  int
	PrefixPerPod  int     // prefix groups a pod keeps cached (LRU)
	PrefixSaving  float64 // fraction of work saved on a prefix hit
	SignalDelay   time.Duration
	Step          time.Duration
	Warmup        time.Duration
	Duration      time.Duration
	PrefixSeedMax int // M: prefix holders added to the candidate set
	// SeedRandom picks which prefix holders fill the M seed slots at random, as SeedCandidates
	// does; otherwise the least loaded holders, as the gateway observes them, come first.
	SeedRandom bool
	// Load-imbalance gate, as AIBRIX_LOAD_BALANCE_IMBALANCE_MIN_GAP and _FACTOR. MinGap 0 disables it.
	ImbalanceMinGap int
	ImbalanceFactor float64
	Seed            uint64
	// RefreshOnRead lets stage 2's exact read of the candidates also refresh the gateway's
	// snapshot of those pods (refTotal, refLocal and read time), as if the per-gateway fields
	// read from Redis were written back into the in-memory estimate.
	RefreshOnRead bool
	// MaxRemoteAge, when positive, drops a pod's remote share (refTotal-refLocal) from the
	// estimate once the gateway last read that pod longer ago than this: local count only.
	MaxRemoteAge time.Duration
}

type policyKind int

const (
	policyFull policyKind = iota
	policyRandom
	policyRandomSeeded
	policySnapshot
	policyCountIndex
	policyCountIndexMixed
	// policyBlendRanked is the K-only candidate stage: a scout of 4K random pods plus up to K
	// prefix holders is ranked on the blended score of the gateway's observed counts (the
	// in-memory estimate); the best K/2 are kept and the other K/2 are drawn at random.
	policyBlendRanked
	// policyScoutRecommend approximates #3353: scout 4K random pods, recommend the best
	// R = min(K/4, 4) of them on the observed blend, reserve max(min(K/2, R), K/2-4R) prefix
	// holders, and fill the rest at random.
	policyScoutRecommend
)

type policy struct {
	Kind            policyKind
	K               int
	SnapshotRefresh time.Duration
	// LiveScoring scores the candidates with exact running counts, as a gateway would after
	// reading only the candidates' counts from Redis. Candidate selection, and the imbalance
	// gate unless GateSample, still use the delayed counts.
	LiveScoring bool
	// GateSample runs the imbalance gate on the candidates instead of on the whole fleet, as the
	// gateway's candidate stage does: candidates first, then the gate and scoring on them only.
	// With LiveScoring the gate then sees the candidates' exact counts.
	GateSample bool
	// Ranked is how many of the K slots policyBlendRanked fills by rank; 0 means K/2.
	Ranked int
}

func (p policy) String() string {
	if p.GateSample {
		q := p
		q.GateSample = false
		return q.String() + ", gate on sample"
	}
	if p.LiveScoring {
		q := p
		q.LiveScoring = false
		return q.String() + ", live stage 2"
	}
	switch p.Kind {
	case policyFull:
		return "full"
	case policyRandom:
		return fmt.Sprintf("A random K=%d", p.K)
	case policyRandomSeeded:
		return fmt.Sprintf("A random+prefix K=%d", p.K)
	case policySnapshot:
		return fmt.Sprintf("D snapshot K=%d T=%v", p.K, p.SnapshotRefresh)
	case policyCountIndex:
		return fmt.Sprintf("C count index K=%d", p.K)
	case policyCountIndexMixed:
		return fmt.Sprintf("C+A index K/2 + random K/2, K=%d", p.K)
	case policyBlendRanked:
		if p.Ranked > 0 {
			return fmt.Sprintf("K-only: scout 4K+holders, rank %d + random, K=%d", p.Ranked, p.K)
		}
		return fmt.Sprintf("K-only: scout 4K+holders, rank K/2 + random K/2, K=%d", p.K)
	case policyScoutRecommend:
		return fmt.Sprintf("scout 4K, R=min(K/4,4) + prefix reserve + random, K=%d", p.K)
	}
	return "unknown"
}

type request struct {
	arrival   float64
	remaining float64
	gateway   int
	pod       int
}

type pod struct {
	capacity float64
	running  []*request
	groups   []int // cached prefix groups, most recent last
}

func (p *pod) holds(group int) bool {
	for _, g := range p.groups {
		if g == group {
			return true
		}
	}
	return false
}

func (p *pod) touch(group, limit int) {
	for i, g := range p.groups {
		if g == group {
			copy(p.groups[i:], p.groups[i+1:])
			p.groups[len(p.groups)-1] = group
			return
		}
	}
	if len(p.groups) == limit {
		copy(p.groups, p.groups[1:])
		p.groups = p.groups[:limit-1]
	}
	p.groups = append(p.groups, group)
}

type result struct {
	Policy        string
	Requests      int
	Agreement     float64 // share of decisions as good as full scoring's
	Gated         float64 // share of decisions the imbalance gate restricted
	MeanLatency   float64 // seconds
	P99Latency    float64 // seconds
	PrefixHit     float64
	ScoredPerReq  float64
	MaxToMeanLoad float64 // time-averaged max/mean running across pods
	// Ranked half (policyBlendRanked only): mean true running, and running/capacity, of the
	// ranked pods relative to the fleet mean at the same decisions; share of ranked slots that
	// the gateway's previous decision also ranked; and share that were ranked then and have
	// more running now than at that previous decision.
	RankedRunRatio, RankedLoadRatio float64
	Rerank, RerankRisen             float64
}

type sim struct {
	cfg    simConfig
	pol    policy
	rng    *rand.Rand
	pods   []*pod
	now    float64
	zipf   *rand.Zipf
	holder map[int][]int // prefix group -> pods holding it

	// Per gateway: running requests it routed per pod (live), and the total running per pod
	// and its own share as of the last refresh.
	local       [][]int
	refTotal    [][]int
	refLocal    [][]int
	lastRead    [][]float64 // per gateway, per pod: time of the last sweep or read-refresh
	nextRefresh float64

	// Per gateway ranking snapshot for policySnapshot.
	snapshot     [][]int
	nextSnapshot float64

	latencies      []float64
	agree, decided int
	gated          int
	exact          bool // observed returns exact counts (stage-2 live scoring)
	hits           int
	scored         int
	loadRatioSum   float64
	loadRatioN     int

	// Ranked-half quality of policyBlendRanked, after the warmup. ranked holds the pods the
	// current decision filled by rank; prevRanked, per gateway, the pods ranked in its previous
	// decision with their true running count at that decision.
	ranked                      []int
	prevRanked                  []map[int]int
	rankedRunSum, fleetRunSum   float64
	rankedLoadSum, fleetLoadSum float64
	rankedSlots, rerank, risen  int
}

func newSim(cfg simConfig, pol policy) *sim {
	s := &sim{cfg: cfg, pol: pol, rng: rand.New(rand.NewPCG(cfg.Seed, 7))}
	caps := []float64{3000, 4000, 6000}
	for i := 0; i < cfg.Pods; i++ {
		s.pods = append(s.pods, &pod{capacity: caps[i%len(caps)]})
	}
	s.zipf = rand.NewZipf(s.rng, 1.1, 1, uint64(cfg.PrefixGroups-1))
	s.holder = map[int][]int{}
	for g := 0; g < cfg.Gateways; g++ {
		s.local = append(s.local, make([]int, cfg.Pods))
		s.refTotal = append(s.refTotal, make([]int, cfg.Pods))
		s.refLocal = append(s.refLocal, make([]int, cfg.Pods))
		never := make([]float64, cfg.Pods)
		for i := range never {
			never[i] = math.Inf(-1)
		}
		s.lastRead = append(s.lastRead, never)
		s.prevRanked = append(s.prevRanked, nil)
		s.snapshot = append(s.snapshot, nil)
	}
	return s
}

// observed is the running count gateway g believes pod i has.
func (s *sim) observed(g, i int) int {
	if s.cfg.SignalDelay == 0 || s.exact {
		return len(s.pods[i].running)
	}
	if s.cfg.MaxRemoteAge > 0 && s.now-s.lastRead[g][i] > s.cfg.MaxRemoteAge.Seconds() {
		return s.local[g][i]
	}
	return s.refTotal[g][i] - s.refLocal[g][i] + s.local[g][i]
}

func (s *sim) loadScore(g, i int) float64 {
	return float64(s.observed(g, i)+1) / s.pods[i].capacity
}

func (s *sim) refreshSignals() {
	for g := range s.local {
		for i, p := range s.pods {
			s.refTotal[g][i] = len(p.running)
			s.refLocal[g][i] = s.local[g][i]
			s.lastRead[g][i] = s.now
		}
	}
}

func (s *sim) refreshSnapshots() {
	for g := range s.snapshot {
		order := make([]int, len(s.pods))
		for i := range order {
			order[i] = i
		}
		s.rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
		sort.SliceStable(order, func(a, b int) bool { return s.loadScore(g, order[a]) < s.loadScore(g, order[b]) })
		s.snapshot[g] = order
	}
}

// finalScores returns the blended score (higher is better) of each pod in set, normalizing load
// over the set itself as the router does.
func (s *sim) finalScores(g, group int, set []int) []float64 {
	scores := make([]float64, len(set))
	lo, hi := math.Inf(1), math.Inf(-1)
	for j, i := range set {
		l := s.loadScore(g, i)
		scores[j] = l
		lo, hi = math.Min(lo, l), math.Max(hi, l)
	}
	for j, i := range set {
		norm := 1.0
		if hi > lo {
			norm = 1 - (scores[j]-lo)/(hi-lo)
		}
		if s.cfg.PrefixOn {
			prefix := 0.0
			if s.pods[i].holds(group) {
				prefix = 1
			}
			scores[j] = 0.5*norm + 0.5*prefix
		} else {
			scores[j] = norm
		}
	}
	return scores
}

func (s *sim) argmax(set []int, scores []float64) int {
	best, bestScore, ties := -1, math.Inf(-1), 0
	for j, i := range set {
		switch {
		case scores[j] > bestScore+1e-12:
			best, bestScore, ties = i, scores[j], 1
		case math.Abs(scores[j]-bestScore) <= 1e-12:
			ties++
			if s.rng.IntN(ties) == 0 {
				best = i
			}
		}
	}
	return best
}

// eligible applies the load-imbalance gate to the counts gateway g observes and returns the
// pods left, or nil when the gate does not fire (every pod stays eligible).
func (s *sim) eligible(g int) []int {
	if s.cfg.ImbalanceMinGap == 0 {
		return nil
	}
	lo, hi, sum := math.MaxInt, 0, 0
	for i := range s.pods {
		v := s.observed(g, i)
		lo, hi, sum = min(lo, v), max(hi, v), sum+v
	}
	if hi-lo < s.cfg.ImbalanceMinGap {
		return nil
	}
	if n := len(s.pods); n > 2 && float64(hi) <= s.cfg.ImbalanceFactor*(float64(sum-hi)/float64(n-1)+1) {
		return nil
	}
	var out []int
	for i := range s.pods {
		if s.observed(g, i) == lo {
			out = append(out, i)
		}
	}
	return out
}

// eligibleAmong is the imbalance gate over pods only: nil when it does not fire, otherwise the
// pods at the minimum count.
func (s *sim) eligibleAmong(g int, pods []int) []int {
	if s.cfg.ImbalanceMinGap == 0 || len(pods) == 0 {
		return nil
	}
	lo, hi, sum := math.MaxInt, 0, 0
	for _, i := range pods {
		v := s.observed(g, i)
		lo, hi, sum = min(lo, v), max(hi, v), sum+v
	}
	if hi-lo < s.cfg.ImbalanceMinGap {
		return nil
	}
	if n := len(pods); n > 2 && float64(hi) <= s.cfg.ImbalanceFactor*(float64(sum-hi)/float64(n-1)+1) {
		return nil
	}
	var out []int
	for _, i := range pods {
		if s.observed(g, i) == lo {
			out = append(out, i)
		}
	}
	return out
}

func (s *sim) prefixSeeds(g, group int, allowed map[int]bool) []int {
	if !s.cfg.PrefixOn || s.cfg.PrefixSeedMax == 0 {
		return nil
	}
	var holders []int
	for _, h := range s.holder[group] {
		if allowed == nil || allowed[h] {
			holders = append(holders, h)
		}
	}
	if s.cfg.SeedRandom {
		// SeedCandidates ranks holders by match length only and breaks ties at random.
		s.rng.Shuffle(len(holders), func(a, b int) { holders[a], holders[b] = holders[b], holders[a] })
	} else {
		sort.Slice(holders, func(a, b int) bool { return s.loadScore(g, holders[a]) < s.loadScore(g, holders[b]) })
	}
	if len(holders) > s.cfg.PrefixSeedMax {
		holders = holders[:s.cfg.PrefixSeedMax]
	}
	return holders
}

func union(a, b []int) []int {
	seen := make(map[int]struct{}, len(a)+len(b))
	out := make([]int, 0, len(a)+len(b))
	for _, list := range [][]int{a, b} {
		for _, i := range list {
			if _, ok := seen[i]; !ok {
				seen[i] = struct{}{}
				out = append(out, i)
			}
		}
	}
	return out
}

// candidates returns the pods the policy scores, drawn from universe (the pods the gate left).
func (s *sim) candidates(g, group int, universe []int, allowed map[int]bool) []int {
	n := len(universe)
	k := min(s.pol.K, n)
	switch s.pol.Kind {
	case policyRandom, policyRandomSeeded:
		perm := s.rng.Perm(n)[:k]
		for j, idx := range perm {
			perm[j] = universe[idx]
		}
		if s.pol.Kind == policyRandomSeeded {
			return union(s.prefixSeeds(g, group, allowed), perm)
		}
		return perm
	case policySnapshot:
		picked := make([]int, 0, k)
		for _, i := range s.snapshot[g] {
			if len(picked) == k {
				break
			}
			if allowed == nil || allowed[i] {
				picked = append(picked, i)
			}
		}
		return union(s.prefixSeeds(g, group, allowed), picked)
	case policyCountIndex, policyCountIndexMixed:
		order := append([]int(nil), universe...)
		s.rng.Shuffle(n, func(a, b int) { order[a], order[b] = order[b], order[a] })
		sort.SliceStable(order, func(a, b int) bool { return s.observed(g, order[a]) < s.observed(g, order[b]) })
		if s.pol.Kind == policyCountIndexMixed {
			// Half the slots from the index, half drawn uniformly from the rest, so stale counts
			// cannot steer every candidate to the same few pods.
			rest := order[min(s.pol.K/2, n):]
			s.rng.Shuffle(len(rest), func(a, b int) { rest[a], rest[b] = rest[b], rest[a] })
		}
		return union(s.prefixSeeds(g, group, allowed), order[:k])
	case policyBlendRanked:
		if n <= k {
			return universe
		}
		scout := universe
		if 8*k < n {
			var holders []int
			if s.cfg.PrefixOn {
				for _, h := range s.holder[group] {
					if allowed == nil || allowed[h] {
						holders = append(holders, h)
					}
				}
				s.rng.Shuffle(len(holders), func(a, b int) { holders[a], holders[b] = holders[b], holders[a] })
				holders = holders[:min(k, len(holders))]
			}
			scout = s.fillRandom(universe, holders, len(holders)+4*k)
		}
		r := k / 2
		if s.pol.Ranked > 0 {
			r = min(s.pol.Ranked, k)
		}
		ranked := s.bestByEstimate(g, group, scout, r)
		s.ranked = append(s.ranked[:0], ranked...)
		return s.fillRandom(universe, ranked, k)
	case policyScoutRecommend:
		if n <= k {
			return universe
		}
		scout := make([]int, 0, 4*k)
		for _, idx := range s.rng.Perm(n)[:min(4*k, n)] {
			scout = append(scout, universe[idx])
		}
		r := min(k/4, 4)
		picked := s.bestByEstimate(g, group, scout, r)
		reserve := max(min(k/2, r), k/2-4*r)
		if s.cfg.PrefixOn {
			inUniverse := allowed
			holders := make([]int, 0, len(s.holder[group]))
			for _, h := range s.holder[group] {
				if inUniverse == nil || inUniverse[h] {
					holders = append(holders, h)
				}
			}
			s.rng.Shuffle(len(holders), func(a, b int) { holders[a], holders[b] = holders[b], holders[a] })
			before := len(picked)
			for _, h := range union(picked, holders)[before:] {
				if len(picked)-before >= reserve {
					break
				}
				picked = append(picked, h)
			}
		}
		return s.fillRandom(universe, picked, k)
	}
	return universe
}

// bestByEstimate returns the m pods of set with the best blended score on the counts gateway g
// observes, ties broken at random.
func (s *sim) bestByEstimate(g, group int, set []int, m int) []int {
	order := append([]int(nil), set...)
	s.rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
	scores := s.finalScores(g, group, order)
	idx := make([]int, len(order))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return scores[idx[a]] > scores[idx[b]] })
	out := make([]int, 0, m)
	for _, i := range idx[:min(m, len(idx))] {
		out = append(out, order[i])
	}
	return out
}

// fillRandom adds pods of universe not yet in picked, uniformly at random, until it holds k.
func (s *sim) fillRandom(universe, picked []int, k int) []int {
	seen := make(map[int]struct{}, k)
	for _, i := range picked {
		seen[i] = struct{}{}
	}
	for _, idx := range s.rng.Perm(len(universe)) {
		if len(picked) >= k {
			break
		}
		if _, dup := seen[universe[idx]]; !dup {
			seen[universe[idx]] = struct{}{}
			picked = append(picked, universe[idx])
		}
	}
	return picked
}

func (s *sim) route(r *request, group int) {
	if s.pol.GateSample {
		s.routeGateSample(r, group)
		return
	}
	g := r.gateway
	universe := s.eligible(g)
	var allowed map[int]bool
	if universe != nil {
		s.gated++
		allowed = make(map[int]bool, len(universe))
		for _, i := range universe {
			allowed[i] = true
		}
	} else {
		universe = make([]int, len(s.pods))
		for i := range universe {
			universe[i] = i
		}
	}
	cand := s.candidates(g, group, universe, allowed)
	s.exact = s.pol.LiveScoring
	defer func() { s.exact = false }()
	target := s.argmax(cand, s.finalScores(g, group, cand))
	if s.pol.Kind == policyFull {
		s.agree++
	} else {
		// Full scoring normalizes over every eligible pod, so compare on that scale.
		fullScores := s.finalScores(g, group, universe)
		pos := make(map[int]int, len(universe))
		for j, i := range universe {
			pos[i] = j
		}
		best := s.argmax(universe, fullScores)
		if fullScores[pos[target]] >= fullScores[pos[best]]-1e-9 {
			s.agree++
		}
	}
	s.decided++
	s.scored += len(cand)
	s.place(r, target, group)
}

// routeGateSample routes as the gateway's candidate stage does: draw the candidates from the
// whole fleet, then run the gate and scoring on them only.
func (s *sim) routeGateSample(r *request, group int) {
	g := r.gateway
	fleet := make([]int, len(s.pods))
	for i := range fleet {
		fleet[i] = i
	}
	s.ranked = s.ranked[:0]
	cand := s.candidates(g, group, fleet, nil)
	s.trackRanked(g)
	if s.cfg.RefreshOnRead {
		// Stage 2 reads the candidates' exact counts; keep them as this gateway's snapshot.
		for _, i := range cand {
			s.refTotal[g][i] = len(s.pods[i].running)
			s.refLocal[g][i] = s.local[g][i]
			s.lastRead[g][i] = s.now
		}
	}
	s.exact = s.pol.LiveScoring
	defer func() { s.exact = false }()
	if gated := s.eligibleAmong(g, cand); gated != nil {
		s.gated++
		cand = gated
	}
	target := s.argmax(cand, s.finalScores(g, group, cand))
	fullScores := s.finalScores(g, group, fleet)
	if fullScores[target] >= fullScores[s.argmax(fleet, fullScores)]-1e-9 {
		s.agree++
	}
	s.decided++
	s.scored += len(cand)
	s.place(r, target, group)
}

// trackRanked records, for the pods the current decision ranked, their true load against the
// fleet's and whether gateway g also ranked them in its previous decision while their true
// running count has since risen. It does not draw from the random source.
func (s *sim) trackRanked(g int) {
	if len(s.ranked) == 0 {
		return
	}
	cur := make(map[int]int, len(s.ranked))
	for _, i := range s.ranked {
		cur[i] = len(s.pods[i].running)
	}
	if s.now >= s.cfg.Warmup.Seconds() {
		fleetRun, fleetLoad := 0, 0.0
		for _, p := range s.pods {
			fleetRun += len(p.running)
			fleetLoad += float64(len(p.running)) / p.capacity
		}
		run, load := 0.0, 0.0
		for _, i := range s.ranked {
			run += float64(cur[i])
			load += float64(cur[i]) / s.pods[i].capacity
			if before, ok := s.prevRanked[g][i]; ok {
				s.rerank++
				if cur[i] > before {
					s.risen++
				}
			}
		}
		n, m := float64(len(s.ranked)), float64(len(s.pods))
		s.rankedRunSum += run / n
		s.fleetRunSum += float64(fleetRun) / m
		s.rankedLoadSum += load / n
		s.fleetLoadSum += fleetLoad / m
		s.rankedSlots += len(s.ranked)
	}
	s.prevRanked[g] = cur
}

func (s *sim) place(r *request, target, group int) {
	p := s.pods[target]
	r.pod = target
	if s.cfg.PrefixOn {
		if p.holds(group) {
			r.remaining *= 1 - s.cfg.PrefixSaving
			if s.now >= s.cfg.Warmup.Seconds() {
				s.hits++
			}
		} else {
			if len(p.groups) == s.cfg.PrefixPerPod {
				evicted := p.groups[0]
				hs := s.holder[evicted]
				for k, h := range hs {
					if h == target {
						s.holder[evicted] = append(hs[:k], hs[k+1:]...)
						break
					}
				}
			}
			s.holder[group] = append(s.holder[group], target)
		}
		p.touch(group, s.cfg.PrefixPerPod)
	}
	p.running = append(p.running, r)
	s.local[r.gateway][target]++
}

func (s *sim) work() float64 {
	if s.cfg.HeavyTail && s.rng.Float64() < 0.1 {
		return s.rng.ExpFloat64() * 5.5 * s.cfg.MeanWork
	}
	if s.cfg.HeavyTail {
		return s.rng.ExpFloat64() * 0.5 * s.cfg.MeanWork
	}
	return s.rng.ExpFloat64() * s.cfg.MeanWork
}

func (s *sim) run() result {
	cfg := s.cfg
	totalCap := 0.0
	for _, p := range s.pods {
		totalCap += p.capacity
	}
	work := cfg.MeanWork
	if cfg.PrefixOn {
		work *= 1 - cfg.PrefixSaving*0.5 // roughly half the requests hit
	}
	rate := cfg.Utilization * totalCap / work
	dt := cfg.Step.Seconds()
	end := cfg.Duration.Seconds()
	nextArrival := s.rng.ExpFloat64() / rate
	gw := 0
	if s.pol.Kind == policySnapshot {
		s.refreshSnapshots()
		s.nextSnapshot = s.pol.SnapshotRefresh.Seconds()
	}
	for s.now = 0; s.now < end; s.now += dt {
		if cfg.SignalDelay > 0 && s.now >= s.nextRefresh {
			s.refreshSignals()
			s.nextRefresh += cfg.SignalDelay.Seconds()
		}
		if s.pol.Kind == policySnapshot && s.now >= s.nextSnapshot {
			s.refreshSnapshots()
			s.nextSnapshot += s.pol.SnapshotRefresh.Seconds()
		}
		for nextArrival < s.now+dt {
			r := &request{arrival: nextArrival, remaining: s.work(), gateway: gw}
			gw = (gw + 1) % cfg.Gateways
			group := int(s.zipf.Uint64())
			if s.now < cfg.Warmup.Seconds() {
				// Warm-up decisions shape the state but are not counted.
				agree, decided, scored, gated := s.agree, s.decided, s.scored, s.gated
				s.route(r, group)
				s.agree, s.decided, s.scored, s.gated = agree, decided, scored, gated
			} else {
				s.route(r, group)
			}
			nextArrival += s.rng.ExpFloat64() / rate
		}
		maxRun, sumRun := 0, 0
		for i, p := range s.pods {
			if len(p.running) == 0 {
				continue
			}
			share := p.capacity * dt / float64(len(p.running))
			kept := p.running[:0]
			for _, r := range p.running {
				r.remaining -= share
				if r.remaining > 0 {
					kept = append(kept, r)
					continue
				}
				s.local[r.gateway][i]--
				if r.arrival >= cfg.Warmup.Seconds() {
					s.latencies = append(s.latencies, s.now+dt-r.arrival)
				}
			}
			p.running = kept
			maxRun = max(maxRun, len(kept))
			sumRun += len(kept)
		}
		if s.now >= cfg.Warmup.Seconds() && sumRun > 0 {
			s.loadRatioSum += float64(maxRun) / (float64(sumRun) / float64(len(s.pods)))
			s.loadRatioN++
		}
	}
	// Every ratio below needs completed requests after the warmup. A configuration without any
	// would print zeros or NaN into a results table, so fail loudly instead.
	if len(s.latencies) == 0 || s.decided == 0 || s.loadRatioN == 0 {
		panic(fmt.Sprintf("candidatesim: no requests completed between warmup %v and duration %v", cfg.Warmup, cfg.Duration))
	}
	sort.Float64s(s.latencies)
	mean := 0.0
	for _, l := range s.latencies {
		mean += l
	}
	res := result{
		Policy:        s.pol.String(),
		Requests:      s.decided,
		Agreement:     float64(s.agree) / float64(s.decided),
		Gated:         float64(s.gated) / float64(s.decided),
		MeanLatency:   mean / float64(len(s.latencies)),
		P99Latency:    s.latencies[len(s.latencies)*99/100],
		ScoredPerReq:  float64(s.scored) / float64(s.decided),
		MaxToMeanLoad: s.loadRatioSum / float64(s.loadRatioN),
	}
	if cfg.PrefixOn {
		res.PrefixHit = float64(s.hits) / float64(s.decided)
	}
	if s.rankedSlots > 0 {
		res.RankedRunRatio = s.rankedRunSum / s.fleetRunSum
		res.RankedLoadRatio = s.rankedLoadSum / s.fleetLoadSum
		res.Rerank = float64(s.rerank) / float64(s.rankedSlots)
		res.RerankRisen = float64(s.risen) / float64(s.rankedSlots)
	}
	return res
}

func TestCandidateSelectionQuality(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") == "" {
		t.Skip("set CANDIDATE_SIM=1 to run the candidate-selection simulation")
	}
	policies := []policy{
		{Kind: policyFull},
		{Kind: policyRandom, K: 8},
		{Kind: policyRandom, K: 16},
		{Kind: policyRandom, K: 32},
		{Kind: policyRandomSeeded, K: 16},
		{Kind: policyCountIndex, K: 16},
		{Kind: policyCountIndexMixed, K: 16},
		{Kind: policySnapshot, K: 16, SnapshotRefresh: 5 * time.Millisecond},
		{Kind: policySnapshot, K: 8, SnapshotRefresh: 50 * time.Millisecond},
		{Kind: policySnapshot, K: 16, SnapshotRefresh: 50 * time.Millisecond},
		{Kind: policySnapshot, K: 32, SnapshotRefresh: 50 * time.Millisecond},
	}
	var out strings.Builder
	for _, prefixOn := range []bool{false, true} {
		for _, delay := range []time.Duration{0, 100 * time.Millisecond, time.Second} {
			cfg := simConfig{
				Pods: 500, Gateways: 4, Utilization: 0.85, MeanWork: 200,
				PrefixOn: prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
				SignalDelay: delay, Step: 5 * time.Millisecond,
				Warmup: 5 * time.Second, Duration: 25 * time.Second,
				PrefixSeedMax: 4, Seed: 42,
				ImbalanceMinGap: 8, ImbalanceFactor: 2,
			}
			fmt.Fprintf(&out, "\n### prefix=%v signal delay=%v (N=%d, %d gateways, utilization %.2f)\n\n",
				prefixOn, delay, cfg.Pods, cfg.Gateways, cfg.Utilization)
			out.WriteString("| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |\n")
			out.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
			// Each policy's simulation has its own state and random source, so they run in parallel.
			results := make([]result, len(policies))
			var wg sync.WaitGroup
			for j, pol := range policies {
				wg.Add(1)
				go func() {
					defer wg.Done()
					results[j] = newSim(cfg, pol).run()
				}()
			}
			wg.Wait()
			for _, res := range results {
				hit := "-"
				if prefixOn {
					hit = fmt.Sprintf("%.1f%%", 100*res.PrefixHit)
				}
				fmt.Fprintf(&out, "| %s | %.1f%% | %.3f | %.3f | %.2f | %s | %.1f%% | %.0f |\n",
					res.Policy, 100*res.Agreement, res.MeanLatency, res.P99Latency, res.MaxToMeanLoad, hit, 100*res.Gated, res.ScoredPerReq)
			}
		}
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCandidateSelectionRobustness reruns the main policies across seeds, a larger fleet and
// heavy-tailed request sizes, and reports each policy's mean latency relative to full scoring on
// the same seed. Run it with CANDIDATE_SIM=robust; it takes tens of minutes.
func TestCandidateSelectionRobustness(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") != "robust" {
		t.Skip("set CANDIDATE_SIM=robust to run the robustness sweep")
	}
	policies := []policy{
		{Kind: policyFull},
		{Kind: policyRandomSeeded, K: 16},
		{Kind: policyCountIndex, K: 16},
		{Kind: policyCountIndexMixed, K: 16},
		{Kind: policySnapshot, K: 16, SnapshotRefresh: 50 * time.Millisecond},
	}
	type variant struct {
		name      string
		pods      int
		heavyTail bool
		seeds     []uint64
		duration  time.Duration
	}
	variants := []variant{
		{"N=500, exponential sizes, 5 seeds", 500, false, []uint64{1, 2, 3, 4, 5}, 25 * time.Second},
		{"N=500, heavy-tailed sizes, 3 seeds", 500, true, []uint64{1, 2, 3}, 25 * time.Second},
		{"N=2000, exponential sizes, 1 seed", 2000, false, []uint64{1}, 13 * time.Second},
	}
	type scenario struct {
		prefixOn bool
		delay    time.Duration
	}
	var scenarios []scenario
	for _, prefixOn := range []bool{false, true} {
		for _, delay := range []time.Duration{0, 100 * time.Millisecond, time.Second} {
			scenarios = append(scenarios, scenario{prefixOn, delay})
		}
	}

	var out strings.Builder
	for _, v := range variants {
		// results[scenario][seed][policy]
		results := make([][][]result, len(scenarios))
		var wg sync.WaitGroup
		sem := make(chan struct{}, 4)
		for si, sc := range scenarios {
			results[si] = make([][]result, len(v.seeds))
			for ki, seed := range v.seeds {
				results[si][ki] = make([]result, len(policies))
				for pi, pol := range policies {
					cfg := simConfig{
						Pods: v.pods, Gateways: 4, Utilization: 0.85, MeanWork: 200, HeavyTail: v.heavyTail,
						PrefixOn: sc.prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
						SignalDelay: sc.delay, Step: 5 * time.Millisecond,
						Warmup: 3 * time.Second, Duration: v.duration,
						PrefixSeedMax: 4, Seed: seed,
						ImbalanceMinGap: 8, ImbalanceFactor: 2,
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						sem <- struct{}{}
						defer func() { <-sem }()
						results[si][ki][pi] = newSim(cfg, pol).run()
					}()
				}
			}
		}
		wg.Wait()

		fmt.Fprintf(&out, "\n### %s\n\n", v.name)
		out.WriteString("Mean latency relative to full scoring on the same seed: mean (min to max). Full scoring's own mean latency in seconds in the first row; prefix hit ratio after the slash when prefix caching is on.\n\n")
		out.WriteString("| Policy |")
		for _, sc := range scenarios {
			fmt.Fprintf(&out, " prefix=%v delay=%v |", sc.prefixOn, sc.delay)
		}
		out.WriteString("\n| --- |" + strings.Repeat(" --- |", len(scenarios)) + "\n")
		for pi, pol := range policies {
			fmt.Fprintf(&out, "| %s |", pol)
			for si, sc := range scenarios {
				var sum, lo, hi, hit float64
				lo = math.Inf(1)
				for ki := range v.seeds {
					r := results[si][ki][pi]
					x := r.MeanLatency
					if pi > 0 {
						x /= results[si][ki][0].MeanLatency
					}
					sum += x
					lo, hi = math.Min(lo, x), math.Max(hi, x)
					hit += r.PrefixHit
				}
				n := float64(len(v.seeds))
				cell := fmt.Sprintf("%.3f s", sum/n)
				if pi > 0 {
					cell = fmt.Sprintf("%.2f", sum/n)
					if len(v.seeds) > 1 {
						cell += fmt.Sprintf(" (%.2f to %.2f)", lo, hi)
					}
				}
				if sc.prefixOn {
					cell += fmt.Sprintf(" / %.1f%%", 100*hit/n)
				}
				fmt.Fprintf(&out, " %s |", cell)
			}
			out.WriteString("\n")
		}
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCandidateSelectionKOnly compares the K-only candidate designs for Varun's second TODO on
// #3292 with the stage on main (K random plus M prefix seeds) and full scoring, all with live
// stage 2 and the gate on the candidates, as the gateway runs them. Run it with
// CANDIDATE_SIM=konly; CANDIDATE_SIM_PODS=2000 runs the larger fleet.
func TestCandidateSelectionKOnly(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") != "konly" {
		t.Skip("set CANDIDATE_SIM=konly to run the K-only comparison")
	}
	pods := 500
	if v, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_PODS")); err == nil && v > 0 {
		pods = v
	}
	live := func(p policy) policy { p.LiveScoring, p.GateSample = true, true; return p }
	policies := []policy{
		{Kind: policyFull},
		live(policy{Kind: policyRandomSeeded, K: 16}),
		live(policy{Kind: policyBlendRanked, K: 16}),
		live(policy{Kind: policyScoutRecommend, K: 16}),
	}
	seeds := []uint64{1, 2, 3}
	type scenario struct {
		prefixOn bool
		delay    time.Duration
	}
	var scenarios []scenario
	for _, prefixOn := range []bool{false, true} {
		for _, delay := range []time.Duration{100 * time.Millisecond, time.Second, 1000000 * time.Second} {
			scenarios = append(scenarios, scenario{prefixOn, delay})
		}
	}
	results := make([][][]result, len(scenarios))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for si, sc := range scenarios {
		results[si] = make([][]result, len(seeds))
		for ki, seed := range seeds {
			results[si][ki] = make([]result, len(policies))
			for pi, pol := range policies {
				cfg := simConfig{
					Pods: pods, Gateways: simGateways(), Utilization: 0.85, MeanWork: 200,
					PrefixOn: sc.prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
					SignalDelay: sc.delay, Step: 5 * time.Millisecond,
					Warmup: 3 * time.Second, Duration: 15 * time.Second,
					PrefixSeedMax: 16, SeedRandom: true, Seed: seed,
					ImbalanceMinGap: 8, ImbalanceFactor: 2,
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					results[si][ki][pi] = newSim(cfg, pol).run()
				}()
			}
		}
	}
	wg.Wait()

	var out strings.Builder
	fmt.Fprintf(&out, "\n### N=%d, %d gateways, utilization 0.85, %d seeds\n\n", pods, simGateways(), len(seeds))
	out.WriteString("Mean latency relative to full scoring on the same seed: mean (min to max); P99 ratio after the semicolon; prefix hit after the slash. Full scoring's mean latency in seconds in the first row.\n\n")
	out.WriteString("| Policy |")
	for _, sc := range scenarios {
		fmt.Fprintf(&out, " prefix=%v delay=%v |", sc.prefixOn, sc.delay)
	}
	out.WriteString("\n| --- |" + strings.Repeat(" --- |", len(scenarios)) + "\n")
	for pi, pol := range policies {
		fmt.Fprintf(&out, "| %s |", pol)
		for si, sc := range scenarios {
			var sum, p99, hit float64
			lo, hi := math.Inf(1), math.Inf(-1)
			for ki := range seeds {
				r, full := results[si][ki][pi], results[si][ki][0]
				x := r.MeanLatency
				if pi > 0 {
					x /= full.MeanLatency
					p99 += r.P99Latency / full.P99Latency
				}
				sum += x
				lo, hi = math.Min(lo, x), math.Max(hi, x)
				hit += r.PrefixHit
			}
			n := float64(len(seeds))
			cell := fmt.Sprintf("%.3f s", sum/n)
			if pi > 0 {
				cell = fmt.Sprintf("%.2f (%.2f to %.2f); %.2f", sum/n, lo, hi, p99/n)
			}
			if sc.prefixOn {
				cell += fmt.Sprintf(" / %.1f%%", 100*hit/n)
			}
			fmt.Fprintf(&out, " %s |", cell)
		}
		out.WriteString("\n")
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCandidateSelectionLiveStage2 compares today's design, full scoring on exact counts read
// from Redis for every pod on every request, with a candidate stage whose index is refreshed
// from Redis only every SignalDelay and which then reads exact counts for its candidates. Run it
// with CANDIDATE_SIM=live.
func TestCandidateSelectionLiveStage2(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") != "live" {
		t.Skip("set CANDIDATE_SIM=live to run the live stage-2 comparison")
	}
	policies := []policy{
		{Kind: policyRandomSeeded, K: 16, LiveScoring: true, GateSample: true},
		{Kind: policyCountIndex, K: 16, LiveScoring: true},
		{Kind: policyCountIndexMixed, K: 16, LiveScoring: true},
		{Kind: policyCountIndex, K: 16},
	}
	switch os.Getenv("CANDIDATE_SIM_GATE") {
	case "fleet":
		policies[0].GateSample = false
	case "sample":
		for i := range policies[:3] {
			policies[i].GateSample = true
		}
	}
	seeds := []uint64{1, 2, 3}
	delays := []time.Duration{100 * time.Millisecond, time.Second}
	if os.Getenv("CANDIDATE_SIM_QUICK") != "" {
		seeds, delays = seeds[:1], delays[:1]
		policies = []policy{policies[0], policies[2]}
	}
	var out strings.Builder
	out.WriteString("Mean latency relative to full scoring on exact counts (today with Redis), same seed: mean (min to max). Prefix hit ratio after the slash.\n\n")
	out.WriteString("| Policy | Index refresh | Prefix off | Prefix on |\n| --- | --- | --- | --- |\n")
	base := func(prefixOn bool, delay time.Duration, seed uint64) simConfig {
		return simConfig{
			Pods: livePods(), Gateways: 4, Utilization: 0.85, MeanWork: 200,
			PrefixOn: prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
			SignalDelay: delay, Step: 5 * time.Millisecond,
			Warmup: 3 * time.Second, Duration: 25 * time.Second,
			PrefixSeedMax: liveM(), SeedRandom: os.Getenv("CANDIDATE_SIM_SEED") != "load", Seed: seed,
			ImbalanceMinGap: 8, ImbalanceFactor: 2,
		}
	}
	full := map[[2]int]result{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	run := func(cfg simConfig, pol policy, store func(result)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			r := newSim(cfg, pol).run()
			mu.Lock()
			store(r)
			mu.Unlock()
		}()
	}
	for pi, prefixOn := range []bool{false, true} {
		for ki, seed := range seeds {
			run(base(prefixOn, 0, seed), policy{Kind: policyFull}, func(r result) { full[[2]int{pi, ki}] = r })
		}
	}
	got := map[[4]int]result{}
	for di, delay := range delays {
		for poi, pol := range policies {
			for pi, prefixOn := range []bool{false, true} {
				for ki, seed := range seeds {
					run(base(prefixOn, delay, seed), pol, func(r result) { got[[4]int{di, poi, pi, ki}] = r })
				}
			}
		}
	}
	wg.Wait()
	fmt.Fprintf(&out, "| full, exact counts | every request | %.3f s | %.3f s / %.1f%% |\n",
		full[[2]int{0, 0}].MeanLatency, full[[2]int{1, 0}].MeanLatency, 100*full[[2]int{1, 0}].PrefixHit)
	for di, delay := range delays {
		for poi, pol := range policies {
			fmt.Fprintf(&out, "| %s | %v |", pol, delay)
			for pi := range []bool{false, true} {
				var sum, hit float64
				lo, hi := math.Inf(1), math.Inf(-1)
				for ki := range seeds {
					r := got[[4]int{di, poi, pi, ki}]
					x := r.MeanLatency / full[[2]int{pi, ki}].MeanLatency
					sum += x
					lo, hi = math.Min(lo, x), math.Max(hi, x)
					hit += r.PrefixHit
				}
				n := float64(len(seeds))
				cell := fmt.Sprintf("%.2f (%.2f to %.2f)", sum/n, lo, hi)
				if pi == 1 {
					cell += fmt.Sprintf(" / %.1f%%", 100*hit/n)
				}
				fmt.Fprintf(&out, " %s |", cell)
			}
			out.WriteString("\n")
		}
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// The live stage-2 comparison runs the shipped defaults (500 pods, M=16, holders seeded at
// random, gate on the sample) unless CANDIDATE_SIM_PODS, CANDIDATE_SIM_M or CANDIDATE_SIM_SEED=load
// say otherwise; CANDIDATE_SIM_QUICK=1 runs one seed at the 100 ms refresh only. The index-backed
// policies (C, C+A) gate the whole fleet on the index; CANDIDATE_SIM_GATE=sample gates their
// sample instead, and CANDIDATE_SIM_GATE=fleet gates the whole fleet for policy A too.
func livePods() int {
	if n, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_PODS")); err == nil && n > 0 {
		return n
	}
	return 500
}

func liveM() int {
	if n, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_M")); err == nil && n >= 0 {
		return n
	}
	return 16
}

// simGateways is CANDIDATE_SIM_GATEWAYS, 4 by default (local experiment only).
func simGateways() int {
	if v, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_GATEWAYS")); err == nil && v > 0 {
		return v
	}
	return 4
}

// TestCandidateRefreshInterval measures how fresh the other gateways' running counts must be
// for the K-only candidate stage (#3442): it runs that policy with the remote counts refreshed
// every 100 ms, 500 ms, 1 s and 2 s, and never (local counts only), and reports absolute mean
// latency, P99 latency and prefix hit ratio, mean over seeds with the min-max range. Run it
// with CANDIDATE_SIM=refresh; CANDIDATE_SIM_PODS and CANDIDATE_SIM_GATEWAYS set the fleet.
func TestCandidateRefreshInterval(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") != "refresh" {
		t.Skip("set CANDIDATE_SIM=refresh to run the refresh-interval comparison")
	}
	pods := 500
	if v, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_PODS")); err == nil && v > 0 {
		pods = v
	}
	pol := policy{Kind: policyBlendRanked, K: 16, LiveScoring: true, GateSample: true}
	never := 1000000 * time.Second
	delays := []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, never}
	if v := os.Getenv("CANDIDATE_SIM_DELAYS"); v != "" {
		// A comma-separated list of refresh intervals, e.g. "3s,4s"; "never" means local counts only.
		delays = nil
		for _, f := range strings.Split(v, ",") {
			if f == "never" {
				delays = append(delays, never)
				continue
			}
			d, err := time.ParseDuration(f)
			if err != nil {
				t.Fatalf("CANDIDATE_SIM_DELAYS: %v", err)
			}
			delays = append(delays, d)
		}
	}
	seeds := []uint64{1, 2, 3}
	prefixes := []bool{false, true}

	results := make([][][]result, len(prefixes))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for pi, prefixOn := range prefixes {
		results[pi] = make([][]result, len(delays))
		for di, delay := range delays {
			results[pi][di] = make([]result, len(seeds))
			for si, seed := range seeds {
				cfg := simConfig{
					Pods: pods, Gateways: simGateways(), Utilization: 0.85, MeanWork: 200,
					PrefixOn: prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
					SignalDelay: delay, Step: 5 * time.Millisecond,
					Warmup: 3 * time.Second, Duration: 15 * time.Second,
					PrefixSeedMax: 16, SeedRandom: true, Seed: seed,
					ImbalanceMinGap: 8, ImbalanceFactor: 2,
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					results[pi][di][si] = newSim(cfg, pol).run()
				}()
			}
		}
	}
	wg.Wait()

	stat := func(rs []result, f func(result) float64) (mean, lo, hi float64) {
		lo, hi = math.Inf(1), math.Inf(-1)
		for _, r := range rs {
			x := f(r)
			mean += x
			lo, hi = math.Min(lo, x), math.Max(hi, x)
		}
		return mean / float64(len(rs)), lo, hi
	}
	var out strings.Builder
	fmt.Fprintf(&out, "\n### %s; N=%d, %d gateways, utilization 0.85, %d seeds\n\n", pol, pods, simGateways(), len(seeds))
	for pi, prefixOn := range prefixes {
		fmt.Fprintf(&out, "prefix=%v\n\n| Remote refresh | Mean latency (ms) | P99 latency (ms) | Prefix hit |\n| --- | --- | --- | --- |\n", prefixOn)
		for di, delay := range delays {
			name := delay.String()
			if delay == never {
				name = "never"
			}
			m, mlo, mhi := stat(results[pi][di], func(r result) float64 { return 1000 * r.MeanLatency })
			p, plo, phi := stat(results[pi][di], func(r result) float64 { return 1000 * r.P99Latency })
			h, hlo, hhi := stat(results[pi][di], func(r result) float64 { return 100 * r.PrefixHit })
			fmt.Fprintf(&out, "| %s | %.1f (%.1f to %.1f) | %.1f (%.1f to %.1f) | %.1f%% (%.1f to %.1f) |\n",
				name, m, mlo, mhi, p, plo, phi, h, hlo, hhi)
		}
		out.WriteString("\n")
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestCandidateRefreshPolicies compares three ways of keeping the other gateways' running
// counts fresh for the K-only candidate stage (#3442): a background sweep every second
// (sweep-only), stage 2's exact read of the K candidates written back into the estimate
// (refresh-on-read, no sweep), and both (hybrid). All three drop a pod's remote share once the
// gateway last read it more than 3 s ago. The per-gateway request rate is set by MeanWork at
// utilization 0.85, and the simulated time scales with the service time. Run it with
// CANDIDATE_SIM=policies; CANDIDATE_SIM_KS, CANDIDATE_SIM_RATES (req/s per gateway),
// CANDIDATE_SIM_PREFIX (off, on or both), CANDIDATE_SIM_PODS and CANDIDATE_SIM_GATEWAYS
// narrow the grid, and CANDIDATE_SIM_VALIDATE=1 instead reruns the 1 s row of
// TestCandidateRefreshInterval through the sweep-only path.
func TestCandidateRefreshPolicies(t *testing.T) {
	if os.Getenv("CANDIDATE_SIM") != "policies" {
		t.Skip("set CANDIDATE_SIM=policies to run the refresh-policy comparison")
	}
	ints := func(env string, def []int) []int {
		v := os.Getenv(env)
		if v == "" {
			return def
		}
		var out []int
		for _, f := range strings.Split(v, ",") {
			n, err := strconv.Atoi(f)
			if err != nil {
				t.Fatalf("%s: %v", env, err)
			}
			out = append(out, n)
		}
		return out
	}
	never := 1000000 * time.Second
	type refresh struct {
		name          string
		sweep         time.Duration
		refreshOnRead bool
	}
	refreshes := []refresh{
		{"sweep-only", time.Second, false},
		{"refresh-on-read", never, true},
		{"hybrid", time.Second, true},
	}
	pods, gateways := 2000, 8
	if v, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_PODS")); err == nil && v > 0 {
		pods = v
	}
	if v, err := strconv.Atoi(os.Getenv("CANDIDATE_SIM_GATEWAYS")); err == nil && v > 0 {
		gateways = v
	}
	ks := ints("CANDIDATE_SIM_KS", []int{16, 32, 100})
	rates := ints("CANDIDATE_SIM_RATES", []int{10, 100, 1000})
	prefixes := []bool{false}
	switch os.Getenv("CANDIDATE_SIM_PREFIX") {
	case "on":
		prefixes = []bool{true}
	case "both":
		prefixes = []bool{false, true}
	}
	seeds := []uint64{1, 2, 3}
	validate := os.Getenv("CANDIDATE_SIM_VALIDATE") != ""
	if validate {
		pods, gateways, ks, rates, prefixes = 500, 4, []int{16}, []int{0}, []bool{false, true}
		refreshes = []refresh{{"sweep-only", time.Second, false}}
	}
	fleetCap := 0.0
	caps := []float64{3000, 4000, 6000} // as newSim assigns them
	for i := 0; i < pods; i++ {
		fleetCap += caps[i%len(caps)]
	}
	const utilization = 0.85
	// meanWork returns the MeanWork that makes run() arrive at rate req/s per gateway.
	meanWork := func(rate int, prefixOn bool) float64 {
		w := utilization * fleetCap / float64(rate*gateways)
		if prefixOn {
			w /= 1 - 0.3*0.5 // run() assumes half the requests save 30%
		}
		return w
	}
	config := func(rate int, prefixOn bool, rf refresh, seed uint64) simConfig {
		cfg := simConfig{
			Pods: pods, Gateways: gateways, Utilization: utilization,
			PrefixOn: prefixOn, PrefixGroups: 2000, PrefixPerPod: 32, PrefixSaving: 0.3,
			SignalDelay: rf.sweep, Step: 5 * time.Millisecond,
			PrefixSeedMax: 16, SeedRandom: true, Seed: seed,
			ImbalanceMinGap: 8, ImbalanceFactor: 2,
			RefreshOnRead: rf.refreshOnRead, MaxRemoteAge: 3 * time.Second,
		}
		if validate {
			cfg.MeanWork, cfg.Warmup, cfg.Duration = 200, 3*time.Second, 15*time.Second
			return cfg
		}
		// About 10 service times of warmup and 40 measured, 64,000 measured requests at G=8.
		cfg.MeanWork = meanWork(rate, prefixOn)
		cfg.Warmup = time.Duration(2000/float64(rate)*1e9) * time.Nanosecond
		cfg.Duration = time.Duration(10000/float64(rate)*1e9) * time.Nanosecond
		return cfg
	}

	type key struct{ pi, ki, ri, fi, si int }
	results := map[key]result{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, runtime.NumCPU())
	start := time.Now()
	for pi, prefixOn := range prefixes {
		for ki, k := range ks {
			for ri, rate := range rates {
				for fi, rf := range refreshes {
					for si, seed := range seeds {
						cfg := config(rate, prefixOn, rf, seed)
						pol := policy{Kind: policyBlendRanked, K: k, LiveScoring: true, GateSample: true}
						wg.Add(1)
						go func() {
							defer wg.Done()
							sem <- struct{}{}
							defer func() { <-sem }()
							t0 := time.Now()
							r := newSim(cfg, pol).run()
							mu.Lock()
							results[key{pi, ki, ri, fi, si}] = r
							mu.Unlock()
							t.Logf("done prefix=%v K=%d rate=%d %s seed=%d in %v", prefixOn, k, rate, refreshes[fi].name, seed, time.Since(t0).Round(time.Second))
						}()
					}
				}
			}
		}
	}
	wg.Wait()

	var out strings.Builder
	fmt.Fprintf(&out, "\nN=%d, %d gateways, utilization %.2f, %d seeds, remote age limit 3s, wall %v, %d CPUs\n",
		pods, gateways, utilization, len(seeds), time.Since(start).Round(time.Second), runtime.NumCPU())
	if !validate {
		for _, rate := range rates {
			w := meanWork(rate, false)
			fmt.Fprintf(&out, "rate %d/gateway: MeanWork %.0f (prefix on %.0f), mean work / mean pod capacity %.3f s\n",
				rate, w, meanWork(rate, true), w/(fleetCap/float64(pods)))
		}
	}
	cell := func(rs []result, f func(result) float64, format string) string {
		mean, lo, hi := 0.0, math.Inf(1), math.Inf(-1)
		for _, r := range rs {
			x := f(r)
			mean += x
			lo, hi = math.Min(lo, x), math.Max(hi, x)
		}
		return fmt.Sprintf(format+" ("+format+"–"+format+")", mean/float64(len(rs)), lo, hi)
	}
	for pi, prefixOn := range prefixes {
		for ki, k := range ks {
			fmt.Fprintf(&out, "\n### K=%d, prefix=%v\n\n", k, prefixOn)
			out.WriteString("| Rate/gw | Policy | Mean (ms) | P99 (ms) | Gate fired | Ranked run / fleet | Ranked load / fleet | Re-ranked | Re-ranked, risen |")
			if prefixOn {
				out.WriteString(" Prefix hit |")
			}
			out.WriteString("\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
			if prefixOn {
				out.WriteString(" --- |")
			}
			out.WriteString("\n")
			for ri, rate := range rates {
				for fi, rf := range refreshes {
					rs := make([]result, len(seeds))
					for si := range seeds {
						rs[si] = results[key{pi, ki, ri, fi, si}]
					}
					fmt.Fprintf(&out, "| %d | %s | %s | %s | %s | %s | %s | %s | %s |", rate, rf.name,
						cell(rs, func(r result) float64 { return 1000 * r.MeanLatency }, "%.1f"),
						cell(rs, func(r result) float64 { return 1000 * r.P99Latency }, "%.0f"),
						cell(rs, func(r result) float64 { return 100 * r.Gated }, "%.1f%%"),
						cell(rs, func(r result) float64 { return r.RankedRunRatio }, "%.3f"),
						cell(rs, func(r result) float64 { return r.RankedLoadRatio }, "%.3f"),
						cell(rs, func(r result) float64 { return 100 * r.Rerank }, "%.2f%%"),
						cell(rs, func(r result) float64 { return 100 * r.RerankRisen }, "%.2f%%"))
					if prefixOn {
						fmt.Fprintf(&out, " %s |", cell(rs, func(r result) float64 { return 100 * r.PrefixHit }, "%.1f%%"))
					}
					out.WriteString("\n")
				}
			}
		}
	}
	t.Log(out.String())
	if path := os.Getenv("CANDIDATE_SIM_OUT"); path != "" {
		if err := os.WriteFile(path, []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}
