# Candidate-selection simulation results

Output of `CANDIDATE_SIM=1 go test -run TestCandidateSelectionQuality ./pkg/plugins/gateway/algorithms/candidatesim/` (seed 42). See the package comment in sim_test.go for the model.

### prefix=false signal delay=0s (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.051 | 0.246 | 2.22 | - | 0.0% | 500 |
| A random K=8 | 18.7% | 0.059 | 0.281 | 2.91 | - | 0.0% | 8 |
| A random K=16 | 25.4% | 0.053 | 0.252 | 2.35 | - | 0.0% | 16 |
| A random K=32 | 31.1% | 0.051 | 0.245 | 2.22 | - | 0.0% | 32 |
| A random+prefix K=16 | 25.4% | 0.053 | 0.252 | 2.35 | - | 0.0% | 16 |
| C count index K=16 | 70.3% | 0.044 | 0.221 | 1.34 | - | 0.0% | 16 |
| C+A index K/2 + random K/2, K=16 | 53.7% | 0.046 | 0.227 | 2.52 | - | 0.0% | 16 |
| D snapshot K=16 T=5ms | 62.2% | 0.074 | 0.301 | 3.17 | - | 0.0% | 16 |
| D snapshot K=8 T=50ms | 52.7% | 0.066 | 0.335 | 6.63 | - | 80.9% | 8 |
| D snapshot K=16 T=50ms | 53.6% | 0.071 | 0.352 | 6.16 | - | 76.3% | 16 |
| D snapshot K=32 T=50ms | 39.2% | 0.099 | 0.420 | 4.20 | - | 47.5% | 30 |

### prefix=false signal delay=100ms (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.100 | 0.425 | 3.99 | - | 43.8% | 301 |
| A random K=8 | 48.3% | 0.109 | 0.461 | 3.82 | - | 54.1% | 8 |
| A random K=16 | 61.8% | 0.108 | 0.449 | 3.88 | - | 57.4% | 15 |
| A random K=32 | 67.7% | 0.105 | 0.438 | 3.88 | - | 52.8% | 28 |
| A random+prefix K=16 | 61.8% | 0.108 | 0.449 | 3.88 | - | 57.4% | 15 |
| C count index K=16 | 86.3% | 0.092 | 0.415 | 3.40 | - | 4.4% | 16 |
| C+A index K/2 + random K/2, K=16 | 72.1% | 0.093 | 0.413 | 3.59 | - | 7.3% | 16 |
| D snapshot K=16 T=5ms | 100.0% | 0.101 | 0.432 | 3.97 | - | 45.1% | 15 |
| D snapshot K=8 T=50ms | 72.1% | 0.104 | 0.469 | 6.35 | - | 94.4% | 7 |
| D snapshot K=16 T=50ms | 73.1% | 0.113 | 0.523 | 5.67 | - | 91.6% | 14 |
| D snapshot K=32 T=50ms | 78.9% | 0.113 | 0.492 | 4.71 | - | 84.8% | 25 |

### prefix=false signal delay=1s (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.540 | 2.367 | 3.29 | - | 87.7% | 132 |
| A random K=8 | 60.2% | 0.555 | 2.534 | 3.20 | - | 87.9% | 7 |
| A random K=16 | 48.8% | 0.501 | 2.034 | 3.33 | - | 60.7% | 14 |
| A random K=32 | 76.0% | 0.542 | 2.345 | 3.34 | - | 82.3% | 25 |
| A random+prefix K=16 | 48.8% | 0.501 | 2.034 | 3.33 | - | 60.7% | 14 |
| C count index K=16 | 78.4% | 0.631 | 2.966 | 2.87 | - | 91.6% | 13 |
| C+A index K/2 + random K/2, K=16 | 72.7% | 0.600 | 2.790 | 3.13 | - | 87.0% | 13 |
| D snapshot K=16 T=5ms | 100.0% | 0.544 | 2.453 | 3.19 | - | 82.5% | 13 |
| D snapshot K=8 T=50ms | 90.8% | 0.607 | 2.827 | 3.17 | - | 97.9% | 7 |
| D snapshot K=16 T=50ms | 81.4% | 0.551 | 2.463 | 3.37 | - | 85.7% | 13 |
| D snapshot K=32 T=50ms | 82.3% | 0.498 | 2.196 | 3.08 | - | 84.5% | 25 |

### prefix=true signal delay=0s (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.040 | 0.208 | 5.65 | 99.8% | 0.0% | 500 |
| A random K=8 | 10.3% | 0.054 | 0.273 | 3.57 | 66.0% | 0.0% | 8 |
| A random K=16 | 13.5% | 0.049 | 0.244 | 3.80 | 74.9% | 0.0% | 16 |
| A random K=32 | 15.8% | 0.049 | 0.240 | 4.02 | 81.6% | 0.0% | 32 |
| A random+prefix K=16 | 99.4% | 0.039 | 0.196 | 4.94 | 99.3% | 0.4% | 19 |
| C count index K=16 | 98.3% | 0.034 | 0.167 | 4.31 | 98.3% | 0.0% | 20 |
| C+A index K/2 + random K/2, K=16 | 99.1% | 0.039 | 0.193 | 4.62 | 99.1% | 0.0% | 19 |
| D snapshot K=16 T=5ms | 95.9% | 0.038 | 0.182 | 3.84 | 95.8% | 0.0% | 19 |
| D snapshot K=8 T=50ms | 98.0% | 0.040 | 0.192 | 3.94 | 97.9% | 0.0% | 12 |
| D snapshot K=16 T=50ms | 97.9% | 0.040 | 0.193 | 3.99 | 97.9% | 0.0% | 20 |
| D snapshot K=32 T=50ms | 98.4% | 0.039 | 0.188 | 4.10 | 98.3% | 0.0% | 35 |

### prefix=true signal delay=100ms (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.072 | 0.298 | 4.47 | 95.0% | 24.6% | 406 |
| A random K=8 | 29.2% | 0.087 | 0.395 | 3.90 | 64.7% | 40.4% | 8 |
| A random K=16 | 33.9% | 0.082 | 0.364 | 4.03 | 72.7% | 35.0% | 16 |
| A random K=32 | 40.4% | 0.079 | 0.345 | 4.18 | 79.6% | 31.9% | 32 |
| A random+prefix K=16 | 98.0% | 0.072 | 0.299 | 4.49 | 95.4% | 28.2% | 19 |
| C count index K=16 | 96.7% | 0.071 | 0.294 | 4.49 | 94.9% | 20.8% | 19 |
| C+A index K/2 + random K/2, K=16 | 98.0% | 0.072 | 0.297 | 4.52 | 95.5% | 27.2% | 19 |
| D snapshot K=16 T=5ms | 97.9% | 0.072 | 0.302 | 4.46 | 92.5% | 28.0% | 18 |
| D snapshot K=8 T=50ms | 97.7% | 0.072 | 0.298 | 4.52 | 95.1% | 25.9% | 11 |
| D snapshot K=16 T=50ms | 97.7% | 0.072 | 0.298 | 4.46 | 94.8% | 26.5% | 19 |
| D snapshot K=32 T=50ms | 98.0% | 0.072 | 0.298 | 4.49 | 94.9% | 25.9% | 35 |

### prefix=true signal delay=1s (N=500, 4 gateways, utilization 0.85)

| Policy | Agreement | Mean latency (s) | P99 latency (s) | Max/mean running | Prefix hit | Gate fired | Pods scored/req |
| --- | --- | --- | --- | --- | --- | --- | --- |
| full | 100.0% | 0.313 | 1.337 | 2.95 | 84.8% | 66.1% | 227 |
| A random K=8 | 46.6% | 0.361 | 1.443 | 2.59 | 61.9% | 80.3% | 7 |
| A random K=16 | 56.0% | 0.337 | 1.357 | 2.66 | 68.0% | 78.7% | 14 |
| A random K=32 | 60.4% | 0.321 | 1.377 | 2.95 | 74.7% | 67.0% | 27 |
| A random+prefix K=16 | 98.6% | 0.318 | 1.366 | 2.86 | 83.5% | 72.9% | 16 |
| C count index K=16 | 97.9% | 0.312 | 1.318 | 2.86 | 84.1% | 71.6% | 16 |
| C+A index K/2 + random K/2, K=16 | 98.2% | 0.308 | 1.268 | 2.73 | 82.8% | 79.9% | 16 |
| D snapshot K=16 T=5ms | 97.4% | 0.318 | 1.319 | 2.85 | 82.1% | 67.6% | 16 |
| D snapshot K=8 T=50ms | 97.4% | 0.315 | 1.312 | 2.79 | 81.7% | 78.1% | 9 |
| D snapshot K=16 T=50ms | 97.6% | 0.323 | 1.328 | 2.77 | 81.7% | 76.5% | 16 |
| D snapshot K=32 T=50ms | 98.1% | 0.323 | 1.381 | 2.99 | 84.0% | 64.8% | 28 |
