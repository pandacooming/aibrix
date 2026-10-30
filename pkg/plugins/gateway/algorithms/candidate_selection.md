# Single candidate budget

Follow-up to [Varun's review of #3292](https://github.com/vllm-project/aibrix/pull/3292#discussion_r4403536333), part of #3188. Candidate routing remains off unless K is enabled. This implementation replaces the initial half-RRF draft after comparative testing.

## Selection

Resolve the complete strategy, including automatic blending, and retain the existing candidate-safe allowlist. For load-balance, least-request, least-kv-cache and prefix-cache, including supported aliases/blends:

1. Uniformly scout up to 4K ready pods and read explicitly observed running-count hints. Selection does not issue Redis commands or populate the final live-count snapshot.
2. Without usable hints, call the existing reference selector directly: random K without prefix; with prefix, ceil(K/2) random plus up to floor(K/2) prefix seeds. K is an upper bound in this fallback. K<4 also uses the reference.
3. With usable counts, read prefix matches once and add at most K uniformly sampled holders to the scouts. Additional count reads cover only the added holders, so total hint reads cover at most 5K pods.
4. Recommend up to R=min(floor(K/4),4) low-cost pods, using the following admission-cost heuristic:

   ```text
   (cachedRunning + 1) / capacity * (1 + kvAlpha * kvUsed²)
     / (1 + 0.5 * prefixWeight / otherWeight * matchFraction)
   ```

   Finite positive cached output rates estimate capacity; missing estimates use their mean. With no other strategy, prefix weight is its own denominator. Missing counts are unknown, not zero. This heuristic is not equivalent to final normalized strategy scores.
5. Preserve up to max(0,floor(K/2)-4R) additional longest-prefix holders, excluding recommendations. Once R saturates, this retains affinity as K grows instead of giving every added slot to random sampling.
6. Uniformly fill remaining slots outside the seeds. This path returns K distinct ready pods when N>K. Existing gate, final strategy scoring, port selection and admission then run as before.

The user configures only candidate.k: no new ratio, scout-size, time-window or affinity knob is introduced. Other candidate-safe strategies retain reference sampling; global-view/exclusive strategies bypass sampling. Prefix-index lookup and the optional full-fleet inflight prefilter can still scale with N.

## Observed count source

The optional CachedRunningRequestHintProvider keeps remote contributions separate from the current local atomic count. Local updates cannot overwrite remote contributions. Getter calls issue no Redis commands and never populate RoutingContext.RunningRequests.

The existing background synchronization supplies hints, without another timer or Redis command. Extra tracking/aggregation starts lazily with the first consumer, and costs O(N × gateways) local work and snapshot storage in that loop.

Source progress is established without comparing clocks on different gateways. First sightings are unknown. A changed generation proves that a row was written after the preceding successful read began; that earlier reader-local time conservatively bounds age. Unchanged successful reads and failed reads do not renew it. Source observations expire at 200 ms; consecutive source progress must also be less than 200 ms apart. Isolated samples after a slow gap cannot temporarily re-enable recommendations. Identity changes reset warmup. No Redis, bad values, mismatched identity or stale observations return unknown.

The source is approximate, not admission state. It does not establish completeness when an entire gateway contribution disappears. Final live APIs remain authoritative; cached capacity and KV metrics retain their existing semantics.

## Compatibility

- New configuration uses K alone. Explicit M in a profile/environment retains deprecated K-random-plus-M-prefix behavior, including M=0, with the existing warning.
- Existing K-only configuration previously added implicit M=16. Old K=16 plus implicit M=16 corresponds to new K=32 in maximum size, not identical composition.
- K=0 stays disabled. Small K and missing observations use the reference. No additional service or API configuration is required.

## Verification

See [implementation results](candidatesim/NATIVE_RESULTS.md). Tests cover budgets, ready-set membership, bounded hint reads, prefix retention, untouched live snapshots and explicit-M compatibility. A Redis-backed integration test runs the actual background source, selector, gate and final router; freezing its remote writer while keeping rows readable verifies expiry.

Paired processor-sharing experiments use the real selector, final routers and prefix index, drain measured requests and include A/A controls. They establish results over the stated matrix, not a universal proof for arbitrary engines or traffic. The initial RRF implementation and failed alternatives remain research controls.
