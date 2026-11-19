package pipeline

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
)

func newTestBatcher(s *fakeSender, maxCalls int) *Batcher {
	return NewBatcher(s, BatchConfig{MaxCalls: maxCalls, FlushInterval: time.Hour, SendTimeout: time.Second}, logr.Discard())
}

var errRejected = &rudderstack.Error{StatusCode: http.StatusBadRequest, Body: "bad"}

func TestBatcherAcksAfterSend(t *testing.T) {
	s := &fakeSender{}
	b := newTestBatcher(s, 10)
	m1, m2 := &fakeMsg{}, &fakeMsg{}
	b.Submit(m1, []rudderstack.Message{call("1a"), call("1b")})
	b.Submit(m2, []rudderstack.Message{call("2")})

	if acked, _ := m1.state(); acked {
		t.Fatal("message acked before its calls were sent")
	}
	b.flush(context.Background(), false)

	if got := s.sent(); !slices.Equal(got, []string{"1a", "1b", "2"}) {
		t.Fatalf("sent %v", got)
	}
	for _, m := range []*fakeMsg{m1, m2} {
		if acked, nacked := m.state(); !acked || nacked {
			t.Fatalf("acked = %v, nacked = %v, want acked", acked, nacked)
		}
	}
}

func TestBatcherSplitsByMaxCalls(t *testing.T) {
	s := &fakeSender{}
	b := newTestBatcher(s, 2)
	for _, id := range []string{"1", "2", "3"} {
		b.Submit(&fakeMsg{}, []rudderstack.Message{call(id)})
	}

	// A full-batch flush sends only complete batches.
	b.flush(context.Background(), true)
	if s.requests() != 1 || len(b.pending) != 1 {
		t.Fatalf("requests = %d, pending = %d, want 1 and 1", s.requests(), len(b.pending))
	}
	b.flush(context.Background(), false)
	if s.requests() != 2 || len(b.pending) != 0 {
		t.Fatalf("requests = %d, pending = %d, want 2 and 0", s.requests(), len(b.pending))
	}
}

func TestBatcherSplitsByMaxBytes(t *testing.T) {
	s := &fakeSender{}
	b := NewBatcher(s, BatchConfig{MaxCalls: 100, MaxBytes: 500, FlushInterval: time.Hour, SendTimeout: time.Second}, logr.Discard())
	big := strings.Repeat("x", 200)
	for _, id := range []string{"1", "2", "3"} {
		c := call(id)
		c.Properties = map[string]any{"p": big}
		b.Submit(&fakeMsg{}, []rudderstack.Message{c})
	}
	b.flush(context.Background(), false)
	if s.requests() != 3 {
		t.Fatalf("requests = %d, want one call per request under a 500 byte limit", s.requests())
	}
}

func TestBatcherNaksOnTemporaryFailure(t *testing.T) {
	s := &fakeSender{fail: func([]rudderstack.Message) error { return errors.New("connection refused") }}
	b := newTestBatcher(s, 10)
	m := &fakeMsg{}
	b.Submit(m, []rudderstack.Message{call("1")})
	b.flush(context.Background(), false)

	if acked, nacked := m.state(); acked || !nacked {
		t.Fatalf("acked = %v, nacked = %v, want nacked", acked, nacked)
	}
}

func TestBatcherIsolatesRejectedCall(t *testing.T) {
	s := &fakeSender{fail: func(batch []rudderstack.Message) error {
		for _, m := range batch {
			if m.MessageID == "bad" {
				return errRejected
			}
		}
		return nil
	}}
	b := newTestBatcher(s, 10)
	msgs := []*fakeMsg{{}, {}, {}, {}}
	for i, id := range []string{"1", "bad", "3", "4"} {
		b.Submit(msgs[i], []rudderstack.Message{call(id)})
	}
	b.flush(context.Background(), false)

	if got := s.sent(); !slices.Equal(got, []string{"1", "3", "4"}) {
		t.Fatalf("sent %v, want every call but the rejected one", got)
	}
	for i, m := range msgs {
		if acked, nacked := m.state(); !acked || nacked {
			t.Fatalf("message %d: acked = %v, nacked = %v, want acked", i, acked, nacked)
		}
	}
	if msgs[0].inProgress == 0 {
		t.Fatal("messages were not kept in progress while the batch was split")
	}
}

func TestBatcherNaksMessageWhenAnyCallFails(t *testing.T) {
	// The message's two calls land in different batches; the second fails.
	var n int
	s := &fakeSender{fail: func([]rudderstack.Message) error {
		n++
		if n == 2 {
			return errors.New("timeout")
		}
		return nil
	}}
	b := newTestBatcher(s, 1)
	m := &fakeMsg{}
	b.Submit(m, []rudderstack.Message{call("a"), call("b")})
	b.flush(context.Background(), false)

	if acked, nacked := m.state(); acked || !nacked {
		t.Fatalf("acked = %v, nacked = %v, want nacked", acked, nacked)
	}
}

func TestBatcherDropsOversizedCall(t *testing.T) {
	s := &fakeSender{}
	b := newTestBatcher(s, 10)
	c := call("big")
	c.Properties = map[string]any{"p": strings.Repeat("x", rudderstack.MaxMessageBytes)}
	m := &fakeMsg{}
	b.Submit(m, []rudderstack.Message{c})

	if acked, _ := m.state(); !acked {
		t.Fatal("message with only an oversized call was not acked")
	}
	if len(b.pending) != 0 {
		t.Fatalf("pending = %d, want 0", len(b.pending))
	}
}

func TestBatcherStartFlushesAndDrains(t *testing.T) {
	s := &fakeSender{}
	b := NewBatcher(s, BatchConfig{MaxCalls: 10, FlushInterval: 10 * time.Millisecond, SendTimeout: time.Second}, logr.Discard())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = b.Start(ctx)
		close(done)
	}()

	// Sent by the interval.
	m1 := &fakeMsg{}
	b.Submit(m1, []rudderstack.Message{call("1")})
	deadline := time.Now().Add(2 * time.Second)
	for acked, _ := m1.state(); !acked; acked, _ = m1.state() {
		if time.Now().After(deadline) {
			t.Fatal("call was not sent on the flush interval")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Sent on shutdown, whether or not another tick came first.
	m2 := &fakeMsg{}
	b.Submit(m2, []rudderstack.Message{call("2")})
	cancel()
	<-done
	if acked, _ := m2.state(); !acked {
		t.Fatal("pending call was not sent by shutdown")
	}
}

func TestBatcherDrainsOnShutdown(t *testing.T) {
	s := &fakeSender{}
	b := newTestBatcher(s, 10)
	m := &fakeMsg{}
	b.Submit(m, []rudderstack.Message{call("1")})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = b.Start(ctx)

	if acked, _ := m.state(); !acked {
		t.Fatal("pending call was not sent on shutdown")
	}
}
