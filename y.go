package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"flag"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var sendHeaders = flag.Bool("h", true, "send http2 headers frame")

type yServerConn struct {
	conn net.Conn
	rd   *bufio.Reader
	wr   *bufio.Writer
	fr   *http2.Framer

	sender struct {
		sync.Mutex
		cond    sync.Cond
		pending []*yPending
	}
}

func newYServer(conn net.Conn) *yServerConn {
	y := &yServerConn{conn: conn}
	y.rd = bufio.NewReader(y.conn)
	y.wr = bufio.NewWriter(conn)
	y.fr = http2.NewFramer(y.wr, y.rd)
	y.fr.SetReuseFrames()
	y.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	y.sender.cond.L = &y.sender.Mutex
	return y
}

func (y *yServerConn) readLoop() {
	payload := make([]byte, *serverPayload)
	_, _ = rand.Read(payload)

	for {
		frame, err := y.fr.ReadFrame()
		if err != nil {
			log.Fatal(err)
		}

		switch frame := frame.(type) {
		case *http2.HeadersFrame:
		case *http2.MetaHeadersFrame:
		case *http2.DataFrame:
			var req PingRequest
			err := proto.Unmarshal(frame.Data(), &req)
			if err != nil {
				log.Fatal(err)
			}
			go func(streamID uint32, req PingRequest) {
				y.send(streamID, &PingResponse{Payload: payload})
			}(frame.StreamID, req)
		case *http2.RSTStreamFrame:
		case *http2.SettingsFrame:
		case *http2.PingFrame:
		case *http2.GoAwayFrame:
		case *http2.WindowUpdateFrame:
		default:
			log.Fatalf("transport: http2Client.reader got unhandled frame type %T: %v", frame, frame)
		}
	}
}

func (y *yServerConn) writeLoop() {
	s := &y.sender
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)

	headers := []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/main.Pinger/Ping"},
		{Name: ":authority", Value: "localhost:50051"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "user-agent", Value: "grpc-go/1.7.0-dev"},
		{Name: "te", Value: "trailers"},
	}

	for {
		s.Lock()
		for len(s.pending) == 0 {
			s.cond.Wait()
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			if *sendHeaders {
				buf.Reset()
				for _, f := range headers {
					if err := enc.WriteField(f); err != nil {
						log.Fatal(err)
					}
				}
				if err := y.fr.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      p.streamID,
					BlockFragment: buf.Bytes(),
					EndStream:     false,
					EndHeaders:    true,
				}); err != nil {
					log.Fatal(err)
				}
			}

			if err := y.fr.WriteData(p.streamID, true, p.data); err != nil {
				log.Fatal(err)
			}
		}

		if err := y.wr.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}

func (y *yServerConn) send(streamID uint32, m proto.Message) {
	d, err := proto.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	p := &yPending{streamID: streamID, data: d}

	s := &y.sender
	s.Lock()
	s.pending = append(s.pending, p)
	s.cond.Signal()
	s.Unlock()
}

func doYServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatal(err)
		}
		y := newYServer(conn)
		go y.readLoop()
		go y.writeLoop()
	}
}

type yPending struct {
	streamID uint32
	data     []byte
	resp     PingResponse
	wg       sync.WaitGroup
}

type yClientConn struct {
	conn net.Conn
	rd   *bufio.Reader
	wr   *bufio.Writer
	fr   *http2.Framer

	sender struct {
		sync.Mutex
		cond    sync.Cond
		pending []*yPending
	}
	receiver struct {
		sync.Mutex
		streamID uint32
		pending  map[uint32]*yPending
	}
}

func newYClient(conn net.Conn) *yClientConn {
	y := &yClientConn{
		conn: conn,
	}
	y.rd = bufio.NewReader(conn)
	y.wr = bufio.NewWriter(conn)
	y.fr = http2.NewFramer(y.wr, y.rd)
	y.fr.SetReuseFrames()
	y.fr.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
	y.sender.cond.L = &y.sender.Mutex
	y.receiver.streamID = 1
	y.receiver.pending = make(map[uint32]*yPending)
	return y
}

func (y *yClientConn) readLoop() {
	r := &y.receiver

	for {
		frame, err := y.fr.ReadFrame()
		if err != nil {
			log.Fatal(err)
		}

		switch frame := frame.(type) {
		case *http2.HeadersFrame:
		case *http2.MetaHeadersFrame:
		case *http2.DataFrame:
			r.Lock()
			p := r.pending[frame.StreamID]
			delete(r.pending, frame.StreamID)
			r.Unlock()
			err := proto.Unmarshal(frame.Data(), &p.resp)
			if err != nil {
				log.Fatal(err)
			}
			p.wg.Done()
		case *http2.RSTStreamFrame:
		case *http2.SettingsFrame:
		case *http2.PingFrame:
		case *http2.GoAwayFrame:
		case *http2.WindowUpdateFrame:
		default:
			log.Fatalf("transport: http2Client.reader got unhandled frame type %v.", frame)
		}
	}
}

func (y *yClientConn) writeLoop() {
	s := &y.sender
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)

	headers := []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":path", Value: "/main.Pinger/Ping"},
		{Name: ":authority", Value: "localhost:50051"},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "user-agent", Value: "grpc-go/1.7.0-dev"},
		{Name: "te", Value: "trailers"},
	}

	for {
		s.Lock()
		for len(s.pending) == 0 {
			s.cond.Wait()
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			if *sendHeaders {
				buf.Reset()
				for _, f := range headers {
					if err := enc.WriteField(f); err != nil {
						log.Fatal(err)
					}
				}
				if err := y.fr.WriteHeaders(http2.HeadersFrameParam{
					StreamID:      p.streamID,
					BlockFragment: buf.Bytes(),
					EndStream:     false,
					EndHeaders:    true,
				}); err != nil {
					log.Fatal(err)
				}
			}

			if err := y.fr.WriteData(p.streamID, true, p.data); err != nil {
				log.Fatal(err)
			}
		}
		if err := y.wr.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}

func (y *yClientConn) send(m proto.Message) PingResponse {
	d, err := proto.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	p := &yPending{data: d}
	p.wg.Add(1)

	r := &y.receiver
	r.Lock()
	p.streamID = r.streamID
	r.streamID++
	r.pending[p.streamID] = p
	r.Unlock()

	s := &y.sender
	s.Lock()
	s.pending = append(s.pending, p)
	s.cond.Signal()
	s.Unlock()

	p.wg.Wait()
	return p.resp
}

func yWorker(y *yClientConn) {
	payload := make([]byte, *clientPayload)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		resp := y.send(&PingRequest{Payload: payload})
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		stats.Lock()
		if err := stats.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		stats.ops++
		stats.bytes += uint64(len(payload) + resp.Size())
		stats.Unlock()
	}
}

func doYClient(addr string) {
	clients := make([]*yClientConn, *connections)
	for i := 0; i < *connections; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		clients[i] = newYClient(conn)
		go clients[i].readLoop()
		go clients[i].writeLoop()
	}
	for i := 0; i < *concurrency; i++ {
		go yWorker(clients[i%len(clients)])
	}
}
