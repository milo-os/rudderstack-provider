// Package rudderstack sends calls to the RudderStack HTTP API.
package rudderstack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
)

const (
	// MaxBatchBytes is RudderStack's limit for one /v1/batch request body.
	MaxBatchBytes = 4000 * 1000
	// MaxMessageBytes is RudderStack's limit for one call in a batch.
	MaxMessageBytes = 32 * 1000
)

// Call types RudderStack accepts.
const (
	TypeTrack    = "track"
	TypeIdentify = "identify"
	TypeGroup    = "group"
)

// Message is one RudderStack call.
type Message struct {
	Type  string `json:"type"`
	Event string `json:"event,omitempty"`
	// UserID or AnonymousID identifies who the call is about; RudderStack
	// needs one of them.
	UserID            string    `json:"userId,omitempty"`
	AnonymousID       string    `json:"anonymousId,omitempty"`
	GroupID           string    `json:"groupId,omitempty"`
	MessageID         string    `json:"messageId"`
	OriginalTimestamp time.Time `json:"originalTimestamp"`
	// SentAt is set by Client.Send. RudderStack derives an event's timestamp
	// from originalTimestamp corrected by the gap between sentAt and when it
	// received the call, so late deliveries keep their original time.
	SentAt     *time.Time     `json:"sentAt,omitempty"`
	Properties map[string]any `json:"properties,omitempty"`
	Traits     map[string]any `json:"traits,omitempty"`
	Context    map[string]any `json:"context,omitempty"`
}

// Sender delivers a batch of calls. A nil error means RudderStack accepted
// every call in the batch.
type Sender interface {
	Send(ctx context.Context, batch []Message) error
}

// Error is a request RudderStack answered with a non-2xx status.
type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("rudderstack returned %d: %s", e.StatusCode, e.Body)
}

// IsPermanent reports whether err rejects the payload itself, so retrying the
// same calls cannot succeed: 400 Bad Request or 413 Payload Too Large. Every
// other failure is temporary, including 401, 403 and 404: they mean a wrong
// write key or data plane URL, which is a configuration fault that must show
// up as a growing backlog, not as calls dropped one by one.
func IsPermanent(err error) bool {
	var re *Error
	if !errors.As(err, &re) {
		return false
	}
	return re.StatusCode == http.StatusBadRequest || re.StatusCode == http.StatusRequestEntityTooLarge
}

// Client sends batches to a RudderStack data plane.
type Client struct {
	endpoint string
	writeKey string
	http     *http.Client
}

// NewClient returns a client for the data plane at dataPlaneURL.
func NewClient(dataPlaneURL, writeKey string, timeout time.Duration) *Client {
	return &Client{
		endpoint: strings.TrimSuffix(dataPlaneURL, "/") + "/v1/batch",
		writeKey: writeKey,
		http:     &http.Client{Timeout: timeout},
	}
}

// Send posts batch to /v1/batch.
func (c *Client) Send(ctx context.Context, batch []Message) error {
	sentAt := time.Now().UTC()
	stamped := make([]Message, len(batch))
	for i, m := range batch {
		m.SentAt = &sentAt
		stamped[i] = m
	}
	body, err := json.Marshal(map[string]any{"batch": stamped})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// RudderStack authenticates with the write key as the basic auth user and
	// an empty password.
	req.SetBasicAuth(c.writeKey, "")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return &Error{StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
}

// DryRunSender logs each call instead of sending it. It logs the call's shape
// and IDs but never property or trait values, which can hold personal data
// such as email addresses that must not end up in cluster logs. The userId is
// logged as a hash because it falls back to the username, which can be an
// email address.
type DryRunSender struct {
	Log logr.Logger
}

// Send logs batch and reports success.
func (d DryRunSender) Send(_ context.Context, batch []Message) error {
	for _, m := range batch {
		size := -1
		if b, err := json.Marshal(m); err == nil {
			size = len(b)
		}
		d.Log.Info("Dry run: would send RudderStack call",
			"type", m.Type,
			"event", m.Event,
			"messageId", m.MessageID,
			"userIdHash", hashID(m.UserID),
			"anonymousIdHash", hashID(m.AnonymousID),
			"groupId", groupID(m),
			"originalTimestamp", m.OriginalTimestamp,
			"properties", slices.Sorted(maps.Keys(m.Properties)),
			"traits", slices.Sorted(maps.Keys(m.Traits)),
			"bytes", size,
		)
	}
	return nil
}

// hashID returns a short, stable hash of id so log lines can be correlated
// without revealing it.
func hashID(id string) string {
	if id == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:6])
}

func groupID(m Message) string {
	if m.GroupID != "" {
		return m.GroupID
	}
	g, _ := m.Context["groupId"].(string)
	return g
}
