package main

import (
	"crypto/rand"
	"log"
	"time"

	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	context "golang.org/x/net/context"
)

const (
	servicePath   = "Pinger"
	serviceMethod = "Ping"
)

type rpcxPinger struct {
	payload []byte
}

func newRPCXPinger() *rpcxPinger {
	payload := make([]byte, *serverPayload)
	_, _ = rand.Read(payload)
	return &rpcxPinger{payload: payload}
}

func (p *rpcxPinger) Ping(_ context.Context, req *PingRequest, resp *PingResponse) error {
	*resp = PingResponse{Payload: p.payload}
	return nil
}

func doRPCXServer(addr string) {
	server := server.NewServer()
	if err := server.RegisterName(servicePath, newRPCXPinger(), ""); err != nil {
		log.Fatalf("failed to register server: %v", err)
	}
	if err := server.Serve("tcp", addr); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func rpcxWorker(discovery client.ServiceDiscovery) {
	option := client.DefaultOption
	option.SerializeType = protocol.ProtoBuffer
	c := client.NewXClient(servicePath, client.Failtry, client.RoundRobin, discovery, option)
	defer c.Close()

	payload := make([]byte, *clientPayload)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		var resp PingResponse
		if err := c.Call(context.Background(), serviceMethod, &PingRequest{Payload: payload}, &resp); err != nil {
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

func doRPCXClient(addr string) {
	discoveries := make([]client.ServiceDiscovery, *connections)
	for i := 0; i < len(discoveries); i++ {
		discoveries[i] = client.NewPeer2PeerDiscovery("tcp@"+addr, "")
	}

	for i := 0; i < *concurrency; i++ {
		go rpcxWorker(discoveries[i%len(discoveries)])
	}
}
