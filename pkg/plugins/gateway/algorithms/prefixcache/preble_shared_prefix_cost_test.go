package prefixcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/vllm-project/aibrix/pkg/utils"
)

// The prefill cost of a prefix node is work done once; when k pods of the model
// hold the node, the total charged across them should stay the same, not grow k-fold.
func TestSharedPrefixPrefillCostIsSplit(t *testing.T) {
	charge := func(holders ...string) map[string]float64 {
		router := newTestRouter(4, nil)
		tokens, _ := utils.TokenizeInputText("A shared system prompt that many requests reuse")
		node, _, _ := router.cache.AddPrefix(tokens, "m", "")
		for _, p := range holders {
			node.AddOrUpdatePodForModel("m", prebleTestPodKey(p), time.Now())
		}
		router.histogram.histogram[node] = 1
		router.histogram.nodeToCount[node] = 4
		router.histogram.decodingSize[node] = 0 // isolate the prefill term
		return router.histogram.getCurrentAllocationCostPerPod("V100")
	}

	one := charge("pod-a")
	two := charge("pod-a", "pod-b")
	total := func(m map[string]float64) (s float64) {
		for _, v := range m {
			s += v
		}
		return
	}
	t.Logf("one holder: %v; two holders: %v", one, two)
	require.InDelta(t, total(one), total(two), 1e-9, "sharing a prefix should not multiply its prefill cost")
}
