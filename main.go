package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"

	"google.golang.org/grpc"
)

var listen = flag.String("l", "", "")
var connections = flag.Int("n", 1, "")
var concurrency = flag.Int("p", 1, "")
var clientPayload = flag.Int("c", 10, "")
var serverPayload = flag.Int("s", 10, "")
var typ = flag.String("t", "grpc", "")
var duration = flag.Duration("d", 0, "")
var cpuprof = flag.String("cpuprof", "", "")

func doServer(port string) {
	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			log.Fatalf("error creating go cpu file %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("unable to start cpu profile: %v", err)
		}
		go func() {
			done := make(chan os.Signal, 3)
			signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
			<-done
			pprof.StopCPUProfile()
			fmt.Printf("profile stopped\n")
			f.Close()
			os.Exit(1)
		}()
	}

	switch *typ {
	case "grpc":
		doGrpcServer(port)

	case "x":
		doXServer(port)

	case "y":
		doYServer(port)

	default:
		log.Fatalf("unknown type: %s", *typ)
	}
}

const (
	minLatency = 10 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

var stats struct {
	sync.Mutex
	latency *hdrhistogram.WindowedHistogram
	ops     uint64
	bytes   uint64
}

func doClient() {
	addr := "localhost:50051"
	if args := flag.Args(); len(args) > 0 {
		addr = flag.Arg(0)
	}

	stats.latency = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	switch *typ {
	case "grpc":
		doGrpcClient(addr)

	case "x":
		doXClient(addr)

	case "y":
		doYClient(addr)

	default:
		log.Fatalf("unknown type: %s", *typ)
	}

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	start := time.Now()
	lastNow := start
	var lastOps uint64
	var lastBytes uint64

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			done <- syscall.Signal(0)
		}()
	}

	if *cpuprof != "" {
		f, err := os.Create(*cpuprof)
		if err != nil {
			log.Fatalf("error creating go cpu file %s", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatalf("unable to start cpu profile: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	for i := 0; ; {
		select {
		case <-tick:
			stats.Lock()
			ops := stats.ops
			bytes := stats.bytes
			h := stats.latency.Merge()
			stats.latency.Rotate()
			stats.Unlock()

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)
			now := time.Now()
			elapsed := now.Sub(lastNow).Seconds()

			if i%20 == 0 {
				fmt.Println("_elapsed____ops/s_____MB/s__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8.1f %8.1f %8.2f %8.2f %8.2f %8.2f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				float64(ops-lastOps)/elapsed,
				float64(bytes-lastBytes)/(1024*1024*elapsed),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)

			lastNow = now
			lastOps = ops
			lastBytes = bytes

		case <-done:
			stats.Lock()
			ops := stats.ops
			bytes := stats.bytes
			h := stats.latency.Merge()
			stats.Unlock()

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := cumLatency.ValueAtQuantile(95)
			p99 := cumLatency.ValueAtQuantile(99)
			pMax := cumLatency.ValueAtQuantile(100)
			elapsed := time.Since(start).Seconds()

			fmt.Println("\n_elapsed____ops/s_____MB/s__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			fmt.Printf("%8s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(elapsed+0.5)*time.Second,
				float64(ops)/elapsed,
				float64(bytes)/(1024*1024*elapsed),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			return
		}
	}
}

func main() {
	grpc.EnableTracing = false
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()

	if *listen != "" {
		doServer(*listen)
		return
	}
	doClient()
}
