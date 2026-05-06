package master

import (
	"testing"
	"time"

	"goftpd/internal/protocol"
)

func TestFetchResponseReturnsBufferedEarlyResponse(t *testing.T) {
	rs := &RemoteSlave{
		indexPool:        make(chan string, 1),
		commandNotify:    make(chan struct{}, 1),
		remergeQueue:     make(chan *protocol.AsyncResponseRemerge, 1),
		remergeDrained:   make(chan struct{}, 1),
		heartbeatTimeout: time.Second,
	}

	rs.routeResponse("05", &protocol.AsyncResponse{Index: "05"})

	resp, err := rs.FetchResponse("05", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("FetchResponse returned error: %v", err)
	}

	got, ok := resp.(*protocol.AsyncResponse)
	if !ok {
		t.Fatalf("expected *protocol.AsyncResponse, got %T", resp)
	}
	if got.Index != "05" {
		t.Fatalf("expected response index 05, got %q", got.Index)
	}
}

func TestWaitForRemergeDrainReturnsAfterQueueClears(t *testing.T) {
	rs := &RemoteSlave{
		remergeDrained: make(chan struct{}, 1),
	}
	rs.online.Store(true)
	rs.remergeQueueDepth.Store(1)

	go func() {
		time.Sleep(20 * time.Millisecond)
		rs.remergeQueueDepth.Store(0)
		rs.remergeDrained <- struct{}{}
	}()

	if err := rs.WaitForRemergeDrain(200 * time.Millisecond); err != nil {
		t.Fatalf("WaitForRemergeDrain returned error: %v", err)
	}
}
