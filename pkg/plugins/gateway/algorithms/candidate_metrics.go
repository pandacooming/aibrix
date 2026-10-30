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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

var (
	candidateSelectionMetricsOnce sync.Once
	candidateSelectionPaths       *prometheus.CounterVec
)

func recordCandidateSelection(path string) {
	candidateSelectionMetricsOnce.Do(func() {
		candidateSelectionPaths = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "aibrix_routing_candidate_selections_total",
			Help: "Candidate selections by recommendation or fallback path",
		}, []string{"path"})
		if err := prometheus.Register(candidateSelectionPaths); err != nil {
			if registered, ok := err.(prometheus.AlreadyRegisteredError); ok {
				candidateSelectionPaths = registered.ExistingCollector.(*prometheus.CounterVec)
			} else {
				klog.ErrorS(err, "cannot register candidate selection metric")
			}
		}
	})
	candidateSelectionPaths.WithLabelValues(path).Inc()
}
