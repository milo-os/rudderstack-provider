package pipeline

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"
	toolscache "k8s.io/client-go/tools/cache"
)

// Consumer feeds messages from a JetStream consumer to a Processor. It waits
// for the rule store to sync first, so events are never evaluated against a
// partial set of rules.
type Consumer struct {
	JetStream jetstream.Consumer
	Processor *Processor
	Synced    toolscache.InformerSynced
	Log       logr.Logger

	attached atomic.Bool
}

// Start consumes until ctx ends. It implements manager.Runnable.
func (c *Consumer) Start(ctx context.Context) error {
	if !toolscache.WaitForCacheSync(ctx.Done(), c.Synced) {
		return nil
	}

	cc, err := c.JetStream.Consume(c.Processor.Handle)
	if err != nil {
		return err
	}
	defer cc.Stop()
	c.attached.Store(true)
	defer c.attached.Store(false)

	c.Log.Info("Consuming audit events")
	<-ctx.Done()
	return nil
}

// NeedLeaderElection reports that every replica consumes; JetStream spreads
// messages across them.
func (c *Consumer) NeedLeaderElection() bool { return false }

// ReadyCheck is a healthz.Checker that passes once the consumer is attached.
func (c *Consumer) ReadyCheck(_ *http.Request) error {
	if !c.attached.Load() {
		return errors.New("not consuming audit events yet")
	}
	return nil
}
