package utils

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Requires a local master/replica/sentinel (REVIEW_SENTINEL=127.0.0.1:26379).
func TestDerivedClientAfterFailover(t *testing.T) {
	addr := os.Getenv("REVIEW_SENTINEL")
	if addr == "" {
		t.Skip("REVIEW_SENTINEL not set")
	}
	ctx := context.Background()
	fo := &redis.FailoverOptions{MasterName: "mymaster", SentinelAddrs: []string{addr}}
	shared := redis.NewFailoverClient(fo)

	// What admitRunningRequestsClient / asyncJobStoreClient do today.
	copied := *shared.Options()
	copied.MaxRetries = -1
	derivedCopy := redis.NewClient(&copied)

	// The alternative: build the no-retry client through the failover constructor.
	fo2 := *fo
	fo2.MaxRetries = -1
	derivedFailover := redis.NewFailoverClient(&fo2)

	clients := map[string]*redis.Client{"shared": shared, "derived (Options copy)": derivedCopy, "derived (NewFailoverClient)": derivedFailover}
	for name, c := range clients {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _ = c.Incr(ctx, "k").Err() }()
		}
		wg.Wait()
		t.Logf("%s: idle conns before failover = %d", name, c.PoolStats().IdleConns)
	}

	s := redis.NewSentinelClient(&redis.Options{Addr: addr})
	before, _ := s.GetMasterAddrByName(ctx, "mymaster").Result()
	if os.Getenv("REVIEW_KILL") == "" {
		if err := s.Failover(ctx, "mymaster").Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Logf("killing master %v", before)
		_ = exec.Command("redis-cli", "-p", before[1], "debug", "segfault").Run()
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		now, _ := s.GetMasterAddrByName(ctx, "mymaster").Result()
		if len(now) == 2 && now[1] != before[1] {
			t.Logf("master moved %v -> %v", before, now)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failover did not happen")
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)

	for _, name := range []string{"shared", "derived (Options copy)", "derived (NewFailoverClient)"} {
		c := clients[name]
		failures, readonly := 0, 0
		for i := 0; i < 40; i++ {
			if err := c.Incr(ctx, "k").Err(); err != nil {
				failures++
				if strings.HasPrefix(err.Error(), "READONLY") {
					readonly++
				}
			}
		}
		t.Logf("%s: %d/40 writes failed after failover (%d READONLY)", name, failures, readonly)
	}
}
