package main

import (
	"crypto/rand"
	"log"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/valyala/gorpc"
)

func doGorpcServer(port string) {
	payload := make([]byte, *serverPayload)
	_, _ = rand.Read(payload)
	s := gorpc.Server{
		Addr: port,
		Handler: func(clientAddr string, request interface{}) interface{} {
			var pingReq PingRequest
			if err := proto.Unmarshal(request.([]byte), &pingReq); err != nil {
				log.Fatal(err)
			}
			d, err := proto.Marshal(&PingResponse{Payload: payload})
			if err != nil {
				log.Fatal(err)
			}
			return d
		},
	}
	if err := s.Serve(); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func gorpcWorker(c *gorpc.Client) {
	payload := make([]byte, *clientPayload)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		d, err := proto.Marshal(&PingRequest{Payload: payload})
		if err != nil {
			log.Fatal(err)
		}
		resp, err := c.Call(d)
		if err != nil {
			log.Fatal(err)
		}
		var pingResp PingResponse
		if err := proto.Unmarshal(resp.([]byte), &pingResp); err != nil {
			log.Fatal(err)
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		stats.Lock()
		if err := stats.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		stats.ops++
		stats.bytes += uint64(len(payload) + len(pingResp.Payload))
		stats.Unlock()
	}
}

func doGorpcClient(addr string) {
	clients := make([]*gorpc.Client, *connections)
	for i := 0; i < len(clients); i++ {
		clients[i] = &gorpc.Client{Addr: addr}
		clients[i].Start()
	}

	for i := 0; i < *concurrency; i++ {
		go gorpcWorker(clients[i%len(clients)])
	}
}
