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

package routingalgorithms

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/vllm-project/aibrix/pkg/types"
)

func unifiedContext(t testing.TB, strategy string, k int) *types.RoutingContext {
	t.Helper()
	ctx := types.NewRoutingContext(context.Background(), types.RoutingAlgorithm(strategy), "m1", "hello", "unified", "")
	t.Cleanup(ctx.Delete)
	ov := *types.DefaultRoutingOverrides()
	ov.Candidate = types.CandidateOverrides{K: k}
	ctx.SetRoutingOverrides(&ov)
	ctx.ResolvedStrategy = strategy
	return ctx
}

func TestUnifiedCandidateBudget(t *testing.T) {
	for _, n := range []int{10, 50, 200} {
		pods := candidatePods(n)
		for _, k := range []int{0, 1, 2, 8, n, n + 1} {
			t.Run(fmt.Sprintf("N=%d/K=%d", n, k), func(t *testing.T) {
				router := &seedingRouter{seeds: append([]*v1.Pod{nil, pods[0], pods[0]}, pods[1:]...), ignoreMax: true}
				ctx := unifiedContext(t, "prefix-cache:1,load-balance:1", k)
				got := SelectCandidates(ctx, router, pods)
				requireDistinctSubset(t, got, pods)
				if k == 0 || k >= n {
					require.Equal(t, pods, got)
					return
				}
				require.Len(t, got, k)
				if k >= 2 {
					require.Equal(t, pods[0], got[0])
				}
			})
		}
	}
	pods := candidatePods(40)
	for _, strategy := range []string{"vtc-basic", "least-request:1,vtc-basic:1", "session-affinity", "queue", "pd", "slo"} {
		ctx := unifiedContext(t, strategy, 8)
		assert.Equal(t, pods, SelectCandidates(ctx, &seedingRouter{seeds: pods[:4]}, pods))
	}
	// A non-seeding strategy and unavailable hints return every slot to random sampling.
	for _, router := range []types.Router{nil, &seedingRouter{}} {
		ctx := unifiedContext(t, "load-balance", 8)
		require.Len(t, SelectCandidates(ctx, router, pods), 8)
	}
}

func TestUnifiedCandidatesRetainRandomExploration(t *testing.T) {
	pods := candidatePods(20)
	ctx := unifiedContext(t, "prefix-cache", 8)
	router := &seedingRouter{seeds: pods[:4]}
	counts := map[*v1.Pod]int{}
	for i := 0; i < 4000; i++ {
		got := SelectCandidates(ctx, router, pods)
		require.Len(t, got, 8)
		require.Equal(t, pods[:4], got[:4])
		for _, p := range got[4:] {
			counts[p]++
		}
	}
	for _, p := range pods[4:] {
		assert.InDelta(t, 1000, counts[p], 170, p.Name)
	}
}

func TestUnifiedCandidateProfilesAndLegacyInheritance(t *testing.T) {
	withDefaultOverrides(t, probeRoutingDefaults())
	cases := []struct {
		raw    string
		legacy bool
		count  int
	}{
		{`{"candidate":{"k":8}}`, false, 8},
		{`{"candidate":{"k":8,"prefixM":4}}`, true, 12},
		{`{"candidate":{"k":8,"prefixM":0}}`, true, 8},
		{`{"candidate":{"k":8,"prefixM":-1}}`, false, 8},
	}
	pods := candidatePods(40)
	for _, tc := range cases {
		ctx := unifiedContext(t, "prefix-cache", 8)
		raw := json.RawMessage(tc.raw)
		ctx.ConfigProfile = &types.ResolvedConfigProfile{Routing: parseProbeRoutingConfig(t, raw)}
		ResolveRoutingOverrides(ctx)
		require.Equal(t, tc.legacy, ctx.RoutingOverrides().Candidate.LegacyPrefixM)
		got := SelectCandidates(ctx, &seedingRouter{seeds: pods[:4]}, pods)
		require.Len(t, got, tc.count)
	}
	// Explicit process M is inherited until removed; no per-request state leaks.
	defaults := probeRoutingDefaults()
	defaults.Candidate.LegacyPrefixM = true
	types.SetDefaultRoutingOverrides(defaults)
	var cfg types.RoutingConfig
	require.NoError(t, json.Unmarshal([]byte(`{"candidate":{"k":8}}`), &cfg))
	require.True(t, resolveRoutingOverrides(&cfg).Candidate.LegacyPrefixM)
	require.True(t, types.DefaultRoutingOverrides().Candidate.LegacyPrefixM)
}

func parseProbeRoutingConfig(t *testing.T, raw json.RawMessage) *types.RoutingConfig {
	t.Helper()
	var cfg types.RoutingConfig
	require.NoError(t, json.Unmarshal(raw, &cfg))
	return &cfg
}

func TestLegacyPrefixMEnvironmentDetection(t *testing.T) {
	for _, value := range []string{"", "invalid", "0", "16", "-1"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("AIBRIX_ROUTING_CANDIDATE_PREFIX_M", value)
			assert.Equal(t, value != "" && value != "invalid", legacyPrefixMConfigured())
		})
	}
}

func BenchmarkUnifiedCandidateSelection(b *testing.B) {
	silenceKlog(b)
	for _, n := range []int{50, 500, 2000} {
		store, pods := newFleetBenchCache(n)
		b.Cleanup(store.Close)
		rm := NewRouterManagerWithCache(store)
		rm.Init()
		for _, legacy := range []bool{true, false} {
			mode := "unified"
			if legacy {
				mode = "legacy-random"
			}
			b.Run(fmt.Sprintf("N=%d/%s", n, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					ctx := types.NewRoutingContext(context.Background(), RouterLeastRequest, "m1", "hello", "bench", "")
					ov := *types.DefaultRoutingOverrides()
					ov.Candidate = types.CandidateOverrides{K: 16, LegacyPrefixM: legacy}
					ctx.SetRoutingOverrides(&ov)
					r, err := rm.Select(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if got := SelectCandidates(ctx, r, pods); len(got) != 16 {
						b.Fatalf("got %d candidates", len(got))
					}
					ctx.Delete()
				}
			})
		}
	}
}
