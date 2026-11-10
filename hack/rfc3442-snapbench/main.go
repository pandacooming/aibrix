// Command rfc3442-snapbench measures the cost of one gateway's 100 ms background tick for the
// current gateway snapshot sync versus the redesigns discussed in #3442, against a real Redis.
// It is a measurement aid for that RFC, not part of AIBrix. Usage: start a throwaway Redis
// (redis-server --port 6390 --save "" --appendonly no), then go run ./hack/rfc3442-snapbench.
// It runs FLUSHALL on that Redis.
package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

type cost struct {
	wall   time.Duration
	rounds int     // sequential client round trips
	cmds   float64 // Redis commands processed
	cpuUs  float64 // Redis server CPU (user+sys), microseconds
	bytes  int     // payload bytes returned to the client (field + value lengths)
}

func info(c *redis.Client) (cmds float64, cpu float64) {
	s, _ := c.Info(ctx, "stats", "cpu").Result()
	for _, l := range strings.Split(s, "\r\n") {
		kv := strings.SplitN(l, ":", 2)
		if len(kv) != 2 {
			continue
		}
		v, _ := strconv.ParseFloat(kv[1], 64)
		switch kv[0] {
		case "total_commands_processed":
			cmds = v
		case "used_cpu_user", "used_cpu_sys":
			cpu += v
		}
	}
	return
}

func measure(c *redis.Client, ticks int, f func() (rounds, bytes int)) cost {
	f() // warm
	c0, p0 := info(c)
	start := time.Now()
	var r, b int
	for i := 0; i < ticks; i++ {
		rr, bb := f()
		r, b = r+rr, b+bb
	}
	wall := time.Since(start)
	c1, p1 := info(c)
	t := float64(ticks)
	// each INFO call is itself one command
	return cost{wall / time.Duration(ticks), r / ticks, (c1 - c0 - 1) / t, (p1 - p0) * 1e6 / t, b / ticks}
}

func snapKey(g, p int) string { return fmt.Sprintf("aibrix:pod:gw-%d:default:pod-%d", g, p) }
func rrqKey(p int) string     { return fmt.Sprintf("aibrix:rrq:default/pod-%d", p) }
func aggKey(g int) string     { return fmt.Sprintf("aibrix:gwrun:gw-%d", g) }

func snapFields(g, p int) map[string]any {
	return map[string]any{
		"gateway_instance_id": fmt.Sprintf("gw-%d", g), "pod_uid": "3f2a9c1e-7b4d-4e8a-9c2f-1a2b3c4d5e6f",
		"pod_name": fmt.Sprintf("pod-%d", p), "namespace": "default", "node_name": "node-a-12345",
		"requests_running": strconv.Itoa(p % 17), "seq": strconv.Itoa(100000 + p), "update_time": "12:34:56.789",
	}
}

func setup(c *redis.Client, G, N int) {
	c.FlushAll(ctx)
	for g := 0; g < G; g++ {
		pipe := c.Pipeline()
		agg := map[string]any{}
		for p := 0; p < N; p++ {
			pipe.HSet(ctx, snapKey(g, p), snapFields(g, p))
			agg[fmt.Sprintf("default/pod-%d", p)] = strconv.Itoa(p % 17)
		}
		pipe.HSet(ctx, aggKey(g), agg)
		pipe.Exec(ctx)
	}
	pipe := c.Pipeline()
	for p := 0; p < N; p++ {
		f := map[string]any{}
		for g := 0; g < G; g++ {
			f[fmt.Sprintf("gw-%d:1730000000000000000", g)] = strconv.Itoa(p % 5)
		}
		pipe.HSet(ctx, rrqKey(p), f)
	}
	z := []redis.Z{}
	for g := 0; g < G; g++ {
		z = append(z, redis.Z{Score: float64(time.Now().UnixMilli()), Member: fmt.Sprintf("gw-%d:1730000000000000000", g)})
	}
	pipe.ZAdd(ctx, "aibrix:rrq:gateways", z...)
	pipe.Exec(ctx)
}

func mapBytes(m map[string]string) (n int) {
	for k, v := range m {
		n += len(k) + len(v)
	}
	return
}

// current: write N snapshot hashes (HSET 8 fields + PEXPIRE), SCAN aibrix:pod:*, HGETALL every
// key in batches of 100. The real code uses a 500 ms TTL; here PEXPIRE uses a long one so keys
// survive the run.
func current(c *redis.Client, N int) (int, int) {
	rounds, bytes := 0, 0
	w := c.Pipeline()
	for p := 0; p < N; p++ {
		w.HSet(ctx, snapKey(0, p), snapFields(0, p))
		w.PExpire(ctx, snapKey(0, p), time.Hour)
	}
	w.Exec(ctx)
	rounds++
	var keys []string
	var cur uint64
	for {
		ks, next, _ := c.Scan(ctx, cur, "aibrix:pod:*", 500).Result()
		rounds++
		keys = append(keys, ks...)
		if cur = next; cur == 0 {
			break
		}
	}
	for i := 0; i < len(keys); i += 100 {
		j := i + 100
		if j > len(keys) {
			j = len(keys)
		}
		r := c.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, 0, j-i)
		for _, k := range keys[i:j] {
			cmds = append(cmds, r.HGetAll(ctx, k))
		}
		r.Exec(ctx)
		rounds++
		for _, cm := range cmds {
			bytes += mapBytes(cm.Val())
		}
	}
	return rounds, bytes
}

// optRRQ: no writes (the request path already keeps aibrix:rrq:* live), no SCAN (the pod list
// is local). One ZRANGEBYSCORE for live gateways, then HGETALL of the N rrq hashes in batches
// of 100.
func optRRQ(c *redis.Client, N int) (int, int) {
	rounds, bytes := 0, 0
	c.ZRangeByScore(ctx, "aibrix:rrq:gateways", &redis.ZRangeBy{Min: "0", Max: "+inf"})
	rounds++
	for i := 0; i < N; i += 100 {
		j := i + 100
		if j > N {
			j = N
		}
		r := c.Pipeline()
		cmds := make([]*redis.MapStringStringCmd, 0, j-i)
		for p := i; p < j; p++ {
			cmds = append(cmds, r.HGetAll(ctx, rrqKey(p)))
		}
		r.Exec(ctx)
		rounds++
		for _, cm := range cmds {
			bytes += mapBytes(cm.Val())
		}
	}
	return rounds, bytes
}

// optAgg: each gateway publishes one hash {pod -> its running count} and reads the G hashes.
// write is the full hash (worst case; a delta write sends only pods whose count changed).
func optAgg(c *redis.Client, G, N int) (int, int) {
	rounds, bytes := 0, 0
	agg := make(map[string]any, N)
	for p := 0; p < N; p++ {
		agg[fmt.Sprintf("default/pod-%d", p)] = strconv.Itoa(p % 17)
	}
	pipe := c.Pipeline()
	pipe.HSet(ctx, aggKey(0), agg)
	pipe.PExpire(ctx, aggKey(0), time.Hour)
	cmds := make([]*redis.MapStringStringCmd, 0, G)
	for g := 1; g < G; g++ {
		cmds = append(cmds, pipe.HGetAll(ctx, aggKey(g)))
	}
	pipe.Exec(ctx)
	rounds++
	for _, cm := range cmds {
		bytes += mapBytes(cm.Val())
	}
	return rounds, bytes
}

func main() {
	addr := flag.String("addr", "127.0.0.1:6390", "")
	ticks := flag.Int("ticks", 20, "")
	flag.Parse()
	c := redis.NewClient(&redis.Options{Addr: *addr, PoolSize: 4})
	fmt.Println("G\tN\tmethod\twall_ms\trounds\tcmds\tredis_cpu_ms\tKB")
	for _, gn := range [][2]int{{4, 500}, {4, 2000}, {8, 2000}, {8, 5000}, {16, 5000}} {
		G, N := gn[0], gn[1]
		setup(c, G, N)
		for _, m := range []struct {
			name string
			f    func() (int, int)
		}{
			{"current", func() (int, int) { return current(c, N) }},
			{"rrq-read", func() (int, int) { return optRRQ(c, N) }},
			{"per-gw-agg", func() (int, int) { return optAgg(c, G, N) }},
		} {
			r := measure(c, *ticks, m.f)
			fmt.Printf("%d\t%d\t%s\t%.2f\t%d\t%.0f\t%.2f\t%.0f\n", G, N, m.name,
				float64(r.wall.Microseconds())/1000, r.rounds, r.cmds, r.cpuUs/1000, float64(r.bytes)/1024)
		}
	}
}
