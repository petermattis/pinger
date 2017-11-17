package main

import (
	"bufio"
	"crypto/rand"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
)

type xServerConn struct {
	conn net.Conn

	sender struct {
		sync.Mutex
		cond    sync.Cond
		wr      *bufio.Writer
		pending []*xPending
	}
}

func (x *xServerConn) readLoop() {
	payload := make([]byte, *serverPayload)
	_, _ = rand.Read(payload)

	rd := bufio.NewReader(x.conn)
	header := make([]byte, 12)

	for {
		if _, err := io.ReadFull(rd, header); err != nil {
			log.Fatal(err)
		}
		size := binary.LittleEndian.Uint32(header[:4])
		seq := binary.LittleEndian.Uint64(header[4:12])
		data := make([]byte, size)
		if _, err := io.ReadFull(rd, data); err != nil {
			log.Fatal(err)
		}
		go func() {
			// It's safe to access data asynchronously because we're allocating a new
			// buffer every time.
			var req PingRequest
			if err := proto.Unmarshal(data, &req); err != nil {
				log.Fatal(err)
			}
			x.send(seq, &PingResponse{Payload: payload})
		}()
	}
}

func (x *xServerConn) writeLoop() {
	s := &x.sender
	header := make([]byte, 12)

	for {
		s.Lock()
		for len(s.pending) == 0 {
			s.cond.Wait()
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			binary.LittleEndian.PutUint32(header[:4], uint32(len(p.data)))
			binary.LittleEndian.PutUint64(header[4:12], p.seq)
			if _, err := s.wr.Write(header); err != nil {
				log.Fatal(err)
			}
			if _, err := s.wr.Write(p.data); err != nil {
				log.Fatal(err)
			}
		}

		if err := s.wr.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}

func (x *xServerConn) send(seq uint64, m proto.Message) {
	d, err := proto.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	p := &xPending{data: d}
	p.seq = seq

	s := &x.sender
	s.Lock()
	s.pending = append(s.pending, p)
	s.cond.Signal()
	s.Unlock()
}

func doXServer(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Fatal(err)
		}
		x := &xServerConn{conn: conn}
		x.sender.cond.L = &x.sender.Mutex
		x.sender.wr = bufio.NewWriter(conn)
		go x.readLoop()
		go x.writeLoop()
	}
}

type xPending struct {
	seq  uint64
	data []byte
	resp []byte
	wg   sync.WaitGroup
}

type xClientConn struct {
	conn   net.Conn
	sender struct {
		sync.Mutex
		cond    sync.Cond
		wr      *bufio.Writer
		pending []*xPending
	}
	receiver struct {
		sync.Mutex
		seq     uint64
		pending map[uint64]*xPending
	}
}

func newXClient(conn net.Conn) *xClientConn {
	x := &xClientConn{
		conn: conn,
	}
	x.sender.wr = bufio.NewWriter(conn)
	x.sender.cond.L = &x.sender.Mutex
	x.receiver.seq = 1
	x.receiver.pending = make(map[uint64]*xPending)
	return x
}

func (x *xClientConn) readLoop() {
	r := &x.receiver
	rd := bufio.NewReader(x.conn)
	header := make([]byte, 12)

	for {
		if _, err := io.ReadFull(rd, header); err != nil {
			log.Fatal(err)
		}
		size := binary.LittleEndian.Uint32(header[:4])
		seq := binary.LittleEndian.Uint64(header[4:12])
		resp := make([]byte, size)
		if _, err := io.ReadFull(rd, resp); err != nil {
			log.Fatal(err)
		}

		r.Lock()
		p := r.pending[seq]
		delete(r.pending, seq)
		r.Unlock()
		p.resp = resp
		p.wg.Done()
	}
}

func (x *xClientConn) writeLoop() {
	s := &x.sender
	header := make([]byte, 12)

	for {
		s.Lock()
		for len(s.pending) == 0 {
			s.cond.Wait()
		}
		pending := s.pending
		s.pending = nil
		s.Unlock()

		for _, p := range pending {
			binary.LittleEndian.PutUint32(header[:4], uint32(len(p.data)))
			binary.LittleEndian.PutUint64(header[4:12], p.seq)
			if _, err := s.wr.Write(header); err != nil {
				log.Fatal(err)
			}
			if _, err := s.wr.Write(p.data); err != nil {
				log.Fatal(err)
			}
		}

		if err := s.wr.Flush(); err != nil {
			log.Fatal(err)
		}
	}
}

func (x *xClientConn) send(m proto.Message) PingResponse {
	d, err := proto.Marshal(m)
	if err != nil {
		log.Fatal(err)
	}
	p := &xPending{data: d}
	p.wg.Add(1)

	r := &x.receiver
	r.Lock()
	p.seq = r.seq
	r.seq++
	r.pending[p.seq] = p
	r.Unlock()

	s := &x.sender
	s.Lock()
	s.pending = append(s.pending, p)
	s.cond.Signal()
	s.Unlock()

	p.wg.Wait()
	// It's safe to access p.resp asynchronously because each response is put
	// into its own data buffer.
	var resp PingResponse
	if err := proto.Unmarshal(p.resp, &resp); err != nil {
		log.Fatal(err)
	}
	return resp
}

func xWorker(x *xClientConn) {
	payload := make([]byte, *clientPayload)
	_, _ = rand.Read(payload)

	for {
		start := time.Now()
		resp := x.send(&PingRequest{Payload: payload})
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

func doXClient(addr string) {
	clients := make([]*xClientConn, *connections)
	for i := 0; i < *connections; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		clients[i] = newXClient(conn)
		go clients[i].readLoop()
		go clients[i].writeLoop()
	}
	for i := 0; i < *concurrency; i++ {
		go xWorker(clients[i%len(clients)])
	}
}
