package limit

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDailyKey(t *testing.T) {
	day := time.Date(2026, 11, 19, 23, 30, 0, 0, time.FixedZone("PST", -8*3600))
	k := DailyKey("datumctl-used", "system:serviceaccount:x@y", day)

	// 23:30 PST is the next UTC day.
	if !regexp.MustCompile(`^datumctl-used\.20261120\.[0-9a-f]{24}$`).MatchString(k) {
		t.Fatalf("DailyKey() = %q", k)
	}
	if k == DailyKey("datumctl-used", "other", day) {
		t.Fatal("different subjects produced the same key")
	}
	if k == DailyKey("datumctl-used", "system:serviceaccount:x@y", day.Add(24*time.Hour)) {
		t.Fatal("different days produced the same key")
	}
}

func TestKVAllow(t *testing.T) {
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not start")
	}
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	bucket, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "limits", TTL: 25 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	l := &KV{Bucket: bucket}
	day := time.Now()

	for i, want := range []bool{true, false, false} {
		got, err := l.Allow(ctx, DailyKey("r", "u1", day))
		if err != nil || got != want {
			t.Fatalf("call %d: Allow() = %v, %v, want %v", i, got, err, want)
		}
	}
	if got, err := l.Allow(ctx, DailyKey("r", "u2", day)); err != nil || !got {
		t.Fatalf("other subject: Allow() = %v, %v, want true", got, err)
	}

	// A closed connection is an error, not a refusal.
	nc.Close()
	if _, err := l.Allow(ctx, DailyKey("r", "u3", day)); err == nil {
		t.Fatal("expected an error from a closed connection")
	}
}

type countingLimiter struct {
	calls int
	err   error
	seen  map[string]bool
}

func (l *countingLimiter) Allow(_ context.Context, key string) (bool, error) {
	l.calls++
	if l.err != nil {
		return false, l.err
	}
	if l.seen == nil {
		l.seen = map[string]bool{}
	}
	first := !l.seen[key]
	l.seen[key] = true
	return first, nil
}

func TestCached(t *testing.T) {
	ctx := context.Background()
	next := &countingLimiter{}
	c := NewCached(next)

	for i, want := range []bool{true, false, false} {
		got, err := c.Allow(ctx, "k1")
		if err != nil || got != want {
			t.Fatalf("call %d: Allow() = %v, %v, want %v", i, got, err, want)
		}
	}
	if next.calls != 1 {
		t.Fatalf("bucket calls = %d, want 1: repeats must come from the cache", next.calls)
	}

	// A key another replica took is cached once seen.
	next.seen["k2"] = true
	for range 2 {
		if got, _ := c.Allow(ctx, "k2"); got {
			t.Fatal("Allow(k2) = true for a key already taken")
		}
	}
	if next.calls != 2 {
		t.Fatalf("bucket calls = %d, want 2", next.calls)
	}
}

func TestCachedDoesNotCacheErrors(t *testing.T) {
	ctx := context.Background()
	next := &countingLimiter{err: errors.New("timeout")}
	c := NewCached(next)

	for range 2 {
		if _, err := c.Allow(ctx, "k"); err == nil {
			t.Fatal("expected the bucket error")
		}
	}
	if next.calls != 2 {
		t.Fatalf("bucket calls = %d, want 2: errors must not be cached", next.calls)
	}

	next.err = nil
	if got, err := c.Allow(ctx, "k"); err != nil || !got {
		t.Fatalf("after recovery Allow() = %v, %v, want true", got, err)
	}
}
