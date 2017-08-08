package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/codahale/hdrhistogram"

	"golang.org/x/net/context"
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

type pinger struct {
	payload []byte
}

func newPinger() *pinger {
	payload := make([]byte, *serverPayload)
	_, _ = rand.Read(payload)
	return &pinger{payload: payload}
}

func (p *pinger) Ping(_ context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{Payload: p.payload}, nil
}

func doServerConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("%s", err)
			return
		}
		n, err = conn.Write(buf[:n])
		if err != nil {
			log.Printf("%s", err)
			return
		}
	}
}

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
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer(
			grpc.MaxMsgSize(math.MaxInt32),
			grpc.MaxConcurrentStreams(math.MaxInt32),
			grpc.InitialWindowSize(65535),
			grpc.InitialConnWindowSize(65535),
		)
		RegisterPingerServer(s, newPinger())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

	case "tcp":
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		for {
			conn, err := lis.Accept()
			if err != nil {
				log.Fatal(err)
			}
			go doServerConn(conn)
		}

	case "udp":
		addr, err := net.ResolveUDPAddr("udp", port)
		if err != nil {
			log.Fatal(err)
		}
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			log.Fatal(err)
		}
		buf := make([]byte, 1024)
		for {
			n, addr, err := conn.ReadFrom(buf)
			if err != nil {
				log.Fatal(err)
			}
			n, err = conn.WriteTo(buf[:n], addr)
			if err != nil {
				log.Fatal(err)
			}
		}

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

func grpcWorker(c PingerClient) {
	payload := make([]byte, *clientPayload)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		resp, err := c.Ping(context.TODO(), &PingRequest{Payload: payload})
		if err != nil {
			log.Fatal(err)
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		stats.Lock()
		if err := stats.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		stats.ops++
		stats.bytes += uint64(len(payload) + len(resp.Payload))
		stats.Unlock()
	}
}

func tcpWorker(conn net.Conn) {
	payload := make([]byte, *clientPayload)
	buf := make([]byte, 1024)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		n, err := conn.Write(payload)
		if err != nil {
			log.Fatal(err)
		}
		if n != len(payload) {
			log.Fatalf("truncated write: %d != %d", n, len(payload))
		}
		n, err = conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		stats.Lock()
		if err := stats.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		stats.ops++
		stats.bytes += uint64(len(payload) + n)
		stats.Unlock()
	}
}

func udpWorker(conn *net.UDPConn, addr *net.UDPAddr) {
	payload := make([]byte, *clientPayload)
	buf := make([]byte, 1024)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		n, err := conn.WriteTo(payload, addr)
		if err != nil {
			log.Fatal(err)
		}
		if n != len(payload) {
			log.Fatalf("truncated write: %d != %d", n, len(payload))
		}
		n, _, err = conn.ReadFrom(buf)
		if err != nil {
			log.Fatal(err)
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		stats.Lock()
		if err := stats.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		stats.ops++
		stats.bytes += uint64(len(payload) + n)
		stats.Unlock()
	}
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
		clients := make([]PingerClient, *connections)
		for i := 0; i < len(clients); i++ {
			conn, err := grpc.Dial(addr,
				grpc.WithInsecure(),
				grpc.WithBlock(),
				grpc.WithInitialWindowSize(65535),
				grpc.WithInitialConnWindowSize(65535),
			)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			clients[i] = NewPingerClient(conn)
		}

		for i := 0; i < *concurrency; i++ {
			go grpcWorker(clients[i%len(clients)])
		}

	case "tcp":
		for i := 0; i < *concurrency; i++ {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			go tcpWorker(conn)
		}

	case "udp":
		serverAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < *concurrency; i++ {
			localAddr, err := net.ResolveUDPAddr("udp", ":0")
			if err != nil {
				log.Fatal(err)
			}
			conn, err := net.ListenUDP("udp", localAddr)
			if err != nil {
				log.Fatal(err)
			}
			go udpWorker(conn, serverAddr)
		}

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
	flag.Parse()

	if *listen != "" {
		doServer(*listen)
		return
	}
	doClient()
}
