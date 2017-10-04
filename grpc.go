package main

import (
	"crypto/rand"
	"log"
	"math"
	"net"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

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

func doGrpcServer(port string) {
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

func doGrpcClient(addr string) {
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
}
