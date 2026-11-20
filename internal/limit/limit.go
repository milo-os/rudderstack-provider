// Package limit caps how often a rule sends for one subject.
package limit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// MinTTL is the shortest bucket TTL that still covers a whole UTC day.
const MinTTL = 24 * time.Hour

// Limiter decides whether a key may send.
type Limiter interface {
	// Allow reports whether key has not been seen before, and records it.
	Allow(ctx context.Context, key string) (bool, error)
}

// DailyKey is the limiter key for a rule, subject and UTC day. The subject is
// hashed because user IDs can hold characters NATS keys do not allow, such as
// "@" and ":".
func DailyKey(rule, subject string, day time.Time) string {
	sum := sha256.Sum256([]byte(subject))
	return rule + "." + day.UTC().Format("20060102") + "." + hex.EncodeToString(sum[:12])
}

// maxCachedKeys bounds Cached's memory. Keys hold the day they cover, so
// clearing the cache only costs a round trip to the bucket per key.
const maxCachedKeys = 100_000

// Cached remembers keys already taken, so most repeat checks of the day skip
// the bucket. Without it every matching request waits on a round trip, and a
// slow bucket stalls the consumer.
type Cached struct {
	next Limiter

	mu    sync.Mutex
	taken map[string]struct{}
}

// NewCached wraps next with an in-memory cache of taken keys.
func NewCached(next Limiter) *Cached {
	return &Cached{next: next, taken: map[string]struct{}{}}
}

// Allow refuses keys this process has already seen taken, and asks next for
// the rest. Errors are not cached, so a later call retries the bucket.
func (c *Cached) Allow(ctx context.Context, key string) (bool, error) {
	c.mu.Lock()
	_, seen := c.taken[key]
	c.mu.Unlock()
	if seen {
		return false, nil
	}

	allowed, err := c.next.Allow(ctx, key)
	if err != nil {
		return allowed, err
	}
	c.mu.Lock()
	if len(c.taken) >= maxCachedKeys {
		clear(c.taken)
	}
	c.taken[key] = struct{}{}
	c.mu.Unlock()
	return allowed, nil
}

// KV records keys in a NATS KV bucket. The bucket's TTL expires old keys;
// it must be at least MinTTL.
type KV struct {
	Bucket jetstream.KeyValue
}

// Allow creates key, which succeeds only for the first caller.
func (l *KV) Allow(ctx context.Context, key string) (bool, error) {
	_, err := l.Bucket.Create(ctx, key, nil)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, jetstream.ErrKeyExists):
		return false, nil
	default:
		return false, err
	}
}
