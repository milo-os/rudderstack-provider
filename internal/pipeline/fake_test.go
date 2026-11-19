package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
)

// fakeMsg records how a message was settled. Methods the pipeline does not
// call panic through the nil embedded interface.
type fakeMsg struct {
	jetstream.Msg
	data []byte

	mu         sync.Mutex
	acked      bool
	nacked     bool
	inProgress int
}

func (m *fakeMsg) Data() []byte { return m.data }

func (m *fakeMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
	return nil
}

func (m *fakeMsg) NakWithDelay(time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = true
	return nil
}

func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgress++
	return nil
}

func (m *fakeMsg) state() (acked, nacked bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acked, m.nacked
}

// fakeSender records batches and fails them as told.
type fakeSender struct {
	mu      sync.Mutex
	batches [][]rudderstack.Message
	// fail returns the error for a batch, or nil to accept it.
	fail func([]rudderstack.Message) error
}

func (s *fakeSender) Send(_ context.Context, batch []rudderstack.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		if err := s.fail(batch); err != nil {
			s.batches = append(s.batches, nil)
			return err
		}
	}
	s.batches = append(s.batches, append([]rudderstack.Message(nil), batch...))
	return nil
}

// sent returns the IDs of accepted calls, in order.
func (s *fakeSender) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, b := range s.batches {
		for _, m := range b {
			ids = append(ids, m.MessageID)
		}
	}
	return ids
}

func (s *fakeSender) requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.batches)
}

func call(id string) rudderstack.Message {
	return rudderstack.Message{Type: rudderstack.TypeTrack, Event: "E", UserID: "u", MessageID: id}
}
