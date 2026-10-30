# Native single-K results

This implementation uses a single total candidate budget K. It builds on #3343 (7686f43c), preserving raw-prefix reuse. Exploratory simulation code is kept separately from the routing implementation.

## Policy

The user supplies K alone. Scout up to 4K pods and add up to K sampled prefix holders. Select at most R=min(floor(K/4),4) projected-cost recommendations, where R is the actual number returned. Keep up to max(0,floor(K/2)-4R) additional prefix holders, then fill randomly to K. Unknown counts, unreliable source cadence and K<4 directly use reference selection; its realized prefix pool may be smaller than K. Explicit deprecated M retains compatibility.

Source progress must occur less than 200 ms apart, with a conservative source-age bound below 200 ms. Unchanged readable rows and isolated slow updates do not renew validity. Final live counts and candidate-pool normalization are unchanged. CandidateScorer retains #3343 raw observations for final subset scoring.

## Final evidence

774 policy runs cover 258 paired-input cohorts, with input-hash checks and complete measured-request draining. The matrix contains 20 workloads × 3 source modes × 3 traffic seeds × 3 policies (reference, A/A reference, native). Extra checks use 36 fixed K=64 traffic flows, 12 each for marginal load/KV cases, and three each at 500/2,000 pods.

Working screening bands are +5% mean and +10% p99 per workload. All 19 healthy-source groups using recommendations pass. K=2 and all slow/missing-source groups directly use reference selection; independent random trajectories still show A/A variation, retained in raw data.

| Workload | Mean change | p99 change |
| --- | ---: | ---: |
| Default prefix blend, N=200/K=16 | -23.6% | -37.3% |
| K=64, 36 fixed flows | -2.6% | -2.9% |
| N=500/K=32 | -8.1% | -27.5% |
| N=2,000/K=32 | -5.3% | -25.2% |

K=64 descriptive bootstrap 95% ratio intervals are [0.957, 0.989] for mean and [0.945, 0.998] for p99. They resample traffic-seed runs and are exploratory statistics, not a universal non-inferiority certificate. Previous failed alternatives and all A/A results remain archived.

Engine service remains a processor-sharing model and KV is a workload proxy. These are not GPU measurements. The replay executes the actual selector/final routers/index; real background source synchronization is separately tested against Redis.

## Candidate-selection CPU

Linux/amd64, AMD EPYC 7763, Go 1.22.6. Five serial 250 ms samples per cell; medians. Plain load-balance, K=16, reused context, real metric cache plus an observed-count fixture. Gate, final scoring, Redis, inference and prefix lookup are excluded.

| Fleet | Reference random | Initial ranked draft | Native |
| --- | ---: | ---: | ---: |
| 50 | 1.25 us | 34.77 us | 44.31 us |
| 500 | 1.13 us | 397.70 us | 66.25 us |
| 2,000 | 1.15 us | 1878.29 us | 83.13 us |

At 2,000 pods native uses 23,322 B / 352 allocations versus 326,817 B / 10,039 allocations for the ranked draft. It remains more expensive than random sampling and can be more expensive than the draft at small N. Background aggregation has a separate O(N × gateways) local cost, enabled lazily; there is no new timer or Redis command. Prefix lookup and existing fleet filters can still scale with N.

## Checks and reproduction

Passed: cache/gateway/algorithm/types package tests, broad and final focused race checks, repository lint (0 issues), license headers, whitespace checks and English/Chinese HTML builds. The latter retain two existing warnings.

Regressions cover budgets, ready membership, bounded reads, invalid/missing capacity, untouched live snapshots, explicit-M compatibility, actual source warmup/expiry/isolated updates, gate/final-score sharing, and local/KV-sync raw-prefix reuse. A selector-to-final-subset test checks #3343 composition.

Use Go 1.22.6 and AIBRIX_PREFIX_CACHE_BLOCK_NUMBER=1024. Run CPU checks serially:

```sh
go test ./pkg/plugins/gateway/algorithms -run '^$' -bench '^BenchmarkNativeCandidateBudget$' -benchmem -benchtime=250ms -count=5
```

Research replay modes in the archived experimental worktree are guarded-native-full, guarded-native-confirm, guarded-native-other and guarded-native-scale. Source manifests, all rows, statistics, logs and replay scripts are in the local candidate-native-result artifact directory. The production result excludes that exploratory harness.
