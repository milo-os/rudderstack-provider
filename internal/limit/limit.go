// Package limit caps how often a rule sends for one subject.
package limit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
