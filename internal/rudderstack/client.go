// Package rudderstack sends calls to the RudderStack HTTP API.
package rudderstack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	Type              string    `json:"type"`
	Event             string    `json:"event,omitempty"`
	UserID            string    `json:"userId"`
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

// DryRunSender logs each call instead of sending it.
type DryRunSender struct {
	Log logr.Logger
}

// Send logs batch and reports success.
func (d DryRunSender) Send(_ context.Context, batch []Message) error {
	for _, m := range batch {
		payload, err := json.Marshal(m)
		if err != nil {
			return err
		}
		d.Log.Info("Dry run: would send RudderStack call", "type", m.Type, "event", m.Event, "payload", string(payload))
	}
	return nil
}
