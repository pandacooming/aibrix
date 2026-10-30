# Single candidate budget

Part of #3188 and the [second TODO in Varun's review of #3292](https://github.com/vllm-project/aibrix/pull/3292#discussion_r4403536333). Candidate routing is disabled unless K is enabled. This implementation builds on #3343 for decision-scoped raw prefix reuse.

## Selection

The complete resolved strategy, including auto-blending, must be candidate-safe. Global-view/exclusive strategies retain the full ready set. The central selector combines locally cached observations; there are no separate load/request/KV seeders.

1. Uniformly scout up to 4K ready pods. If prefix-cache is enabled, read prefix matches once and add up to K sampled prefix holders. Running-count hint reads cover at most 5K pods, issue no Redis commands and never populate the final live-count snapshot.
2. Recommend at most `R = min(floor(K/4), 4)` pods. Pure least-request ranks observed running counts. Pure least-kv-cache (and its least-gpu-cache alias) ranks the existing GPU + optional CPU KV usage, independently of request count and token rate; it does not require count hints.
3. In a KV blend, reserve `ceil(R * kvWeight / (kvWeight + loadWeight + requestWeight))` recommendation slots for the KV objective. Load/request recommendations fill the remaining slots, deduplicating overlaps; missing KV observations can release slots to the available count objective. Prefix affinity discounts KV scores when present.
4. Load-balance and prefix/load blends use the projected-cost heuristic:

   ```text
   (cachedRunning + 1) / capacity * (1 + kvAlpha * kvUsed²)
     / (1 + 0.5 * prefixWeight / otherWeight * matchFraction)
   ```

   Finite positive cached output rates estimate capacity; missing estimates use their mean. With no other strategy, prefix weight is its own denominator. This preselection heuristic is not equivalent to final normalized strategy scores. Pure least-request uses count alone.
5. Preserve up to `max(min(floor(K/2), R), floor(K/2) - 4R)` additional longest-prefix holders, where R is the actual recommendation count. This reserves prefix coverage even at K=16 while retaining at least half of K for random exploration. Exclude recommendations from this reserve and fill remaining slots uniformly without replacement. This path returns K distinct ready pods when N>K.
6. The existing gate, live final strategy scoring, normalization, port selection and admission operate on the selected pool.

If the requested count objective has no usable observations, use random K without prefix, or up to `floor(K/2)` prefix holders plus random pods for the remaining slots. Fewer prefix holders leave more room for random sampling, so the fallback also returns K distinct ready pods when N>K. K<4 uses the same fallback. Unknown counts are never assumed idle.

The user configures only K: no new allocation, scout-size or freshness setting. Prefix lookup and the existing full-fleet inflight prefilter can still scale with N.

## Observed count source

`CachedRunningRequestHintProvider` separates remote contributions from the current local atomic count. It reuses the existing background synchronization, without another timer or Redis command. Extra tracking/aggregation starts lazily and adds O(N × gateways) local work and snapshot storage.

Source progress is established without comparing gateway clocks. First sightings are unknown. A changed generation proves that a row was written after the preceding successful read began; that reader-local time bounds source age. Each HGETALL batch records its actual read start, excluding earlier write/SCAN phases and preceding batches. Successful unchanged reads and failed reads do not renew validity.

Source observations expire at 200 ms, and consecutive progress must be less than 200 ms apart. Isolated updates after a slow gap cannot temporarily re-enable recommendations. Missing Redis, invalid values or mismatched gateway/Pod identities return unknown. Per-pod observation ages also bound snapshot age.

This is an approximate source, not admission state. It cannot establish completeness when an entire gateway contribution disappears. Slow synchronization can reduce availability; a partial valid set can still supply recommendations. Final live APIs remain authoritative, and cached capacity/KV metrics keep their existing semantics.

## Observability

`aibrix_routing_candidate_selections_total{path}` reports `recommended`, `no_hints`, `small_k`, `no_valid_recommendation`, `unsupported_strategy`, `bypass` or `legacy`. Labels are fixed path names, not arbitrary strategy strings. K=0 leaves this selector inactive.

A one-time INFO log announces K-only mode and the change from the previous implicit M=16. Operators can compare recommended and fallback rates; source tests also measure full-fleet coverage, mean usable-pod coverage and usable observations in a 64-pod scout. These measure different aspects of availability.

## Migration / release note

Existing K-only configurations previously added implicit M=16. K now means the total budget: old K=16 plus implicit M=16 corresponds to new K=32 in maximum size, not identical composition. Explicit M in a profile/environment retains deprecated K-random-plus-M-prefix behavior, including M=0, and emits a deprecation warning. K=0 remains disabled.

## Verification and reproduction

Deterministic tests cover each cost term, strategy objectives, KV blends, recommendation caps, exact K=8/16/32/64 composition, insufficient prefix holders, outside-scout prefix holders, fallback paths and metric reasons. A real local prefix index regression checks that a lone holder participates in blended scoring without bypassing the overload gate. Cache tests cover freshness, unchanged/failing reads, mixed remote Pod identities, missing gateway identities and stale local Pod identity. A separate-process integration fixture runs the actual source, selector, gate and final scoring; expiry assertions start only after the full observation window following producer shutdown.

Static objective-quality probe (fixed fleet seeds, stochastic production sampling; this is not latency):

```sh
AIBRIX_PREFIX_CACHE_BLOCK_NUMBER=1024 AIBRIX_CANDIDATE_QUALITY_PROBE=1 go test ./pkg/plugins/gateway/algorithms -run '^TestCandidateSelectionQualityProbe$' -count=1 -v
```

It compares random-K and actual selection over 600 trials per cell, with least-request, load-balance, correlated KV and independently varying KV, K=16/32 and three fleet seeds. `gap_captured` is the fraction of the random-to-global-best objective gap closed, not a request-latency percentage.

Real-Redis source timing probe: use a fresh disposable Redis for each combination. For example:

```sh
docker run --rm -d --name aibrix-candidate-test -p 127.0.0.1:16379:6379 redis:7.4-alpine redis-server --save '' --appendonly no
AIBRIX_TEST_REDIS_ADDR=127.0.0.1:16379 AIBRIX_TEST_CANDIDATE_PODS=2000 AIBRIX_TEST_CANDIDATE_GATEWAYS=4 go test ./pkg/cache -run '^TestCandidateHintsRedisAvailability$' -count=1 -v
docker rm -f aibrix-candidate-test
```

The probe refuses a nonempty database, uses the real background source and co-located 100 ms remote writers, warms for two seconds and measures for three seconds. Test 64/500/2,000 pods and 1/4/8 gateways. It samples the full fleet every 20 ms; measurement and writers share the host CPU, so results characterize that setup rather than production capacity. No GPU or model is required.

Selection-only CPU benchmark, run serially:

```sh
go test ./pkg/plugins/gateway/algorithms -run '^$' -bench '^BenchmarkNativeCandidateBudget$' -benchmem -benchtime=250ms -count=5
```

For the prefix reserve, `BenchmarkBudgetPrefixSelection` measures the selector
with precomputed prefix scores and count hints over 500 and 2,000 pods with 1,
16 or 256 holders. It excludes prefix-index lookup, the gate and final scoring:

```sh
go test ./pkg/plugins/gateway/algorithms -run '^$' -bench '^BenchmarkBudgetPrefixSelection$' -benchmem -benchtime=250ms -count=5
```

Results belong in the PR description. This document makes no inference/GPU latency or production-wide non-regression claim.
