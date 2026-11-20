package rudderstack

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
)

func TestClientSend(t *testing.T) {
	var gotUser, gotPass, gotPath, gotType string
	var gotBody struct {
		Batch []map[string]any `json:"batch"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		gotPath = r.URL.Path
		gotType = r.Header.Get("Content-Type")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Error(err)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", "wk", time.Second)
	ts := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
	err := c.Send(context.Background(), []Message{{
		Type:              TypeTrack,
		Event:             "Domain Added",
		UserID:            "u1",
		MessageID:         "a1/rule",
		OriginalTimestamp: ts,
		Properties:        map[string]any{"domain": "example.com"},
	}})
	if err != nil {
		t.Fatal(err)
	}

	if gotUser != "wk" || gotPass != "" {
		t.Fatalf("basic auth = %q:%q, want the write key and an empty password", gotUser, gotPass)
	}
	if gotPath != "/v1/batch" || gotType != "application/json" {
		t.Fatalf("path = %q, content type = %q", gotPath, gotType)
	}
	if len(gotBody.Batch) != 1 {
		t.Fatalf("batch = %v", gotBody.Batch)
	}
	m := gotBody.Batch[0]
	if m["type"] != "track" || m["event"] != "Domain Added" || m["userId"] != "u1" ||
		m["messageId"] != "a1/rule" || m["originalTimestamp"] != "2026-11-19T10:00:00Z" {
		t.Fatalf("message = %v", m)
	}
	if _, ok := m["traits"]; ok {
		t.Fatalf("message has empty traits: %v", m)
	}
	sentAt, err := time.Parse(time.RFC3339Nano, m["sentAt"].(string))
	if err != nil {
		t.Fatalf("sentAt = %v: %v", m["sentAt"], err)
	}
	if time.Since(sentAt) > time.Minute {
		t.Fatalf("sentAt = %v, want the time of sending", sentAt)
	}
}

func TestClientSendErrors(t *testing.T) {
	tests := []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusRequestEntityTooLarge, true},
		{http.StatusUnauthorized, false},
		{http.StatusForbidden, false},
		{http.StatusNotFound, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusServiceUnavailable, false},
	}
	for _, tt := range tests {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "nope", tt.status)
			}))
			defer srv.Close()

			err := NewClient(srv.URL, "wk", time.Second).Send(context.Background(), []Message{{Type: TypeTrack}})
			var re *Error
			if !errors.As(err, &re) || re.StatusCode != tt.status || re.Body != "nope" {
				t.Fatalf("err = %v, want a %d Error", err, tt.status)
			}
			if IsPermanent(err) != tt.permanent {
				t.Fatalf("IsPermanent() = %v, want %v", IsPermanent(err), tt.permanent)
			}
		})
	}
}

func TestNetworkErrorIsNotPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Close()
	err := NewClient(srv.URL, "wk", time.Second).Send(context.Background(), []Message{{Type: TypeTrack}})
	if err == nil || IsPermanent(err) {
		t.Fatalf("err = %v, want a temporary error", err)
	}
}

func TestDryRunSenderLogsNoValues(t *testing.T) {
	var lines []string
	log := funcr.New(func(prefix, args string) { lines = append(lines, args) }, funcr.Options{})

	err := DryRunSender{Log: log}.Send(context.Background(), []Message{
		{
			Type:       TypeIdentify,
			UserID:     "alice@example.com",
			MessageID:  "a1/user-identify",
			Traits:     map[string]any{"email": "alice@example.com", "name": "Alice Example"},
			Properties: map[string]any{"project": "secret-project"},
			Context:    map[string]any{"groupId": "acme"},
		},
		{
			Type:      TypeGroup,
			UserID:    "u1",
			GroupID:   "acme",
			MessageID: "a2/organization-group",
			Traits:    map[string]any{"name": "acme", "displayName": "Acme Widgets Ltd"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 {
		t.Fatalf("logged %d lines, want 2", len(lines))
	}
	checks := []struct {
		line          string
		want, notWant []string
	}{
		{
			line:    lines[0],
			want:    []string{`"email"`, `"name"`, `"project"`, `"acme"`, `"a1/user-identify"`, hashID("alice@example.com"), `"bytes"`},
			notWant: []string{"alice@example.com", "Alice Example", "secret-project"},
		},
		{
			line:    lines[1],
			want:    []string{`"displayName"`, `"name"`, `"acme"`, `"a2/organization-group"`, hashID("u1")},
			notWant: []string{"Acme Widgets Ltd", `"u1"`},
		},
	}
	for _, c := range checks {
		for _, v := range c.notWant {
			if strings.Contains(c.line, v) {
				t.Errorf("dry run logged the value %q: %s", v, c.line)
			}
		}
		for _, v := range c.want {
			if !strings.Contains(c.line, v) {
				t.Errorf("dry run log %s does not mention %s", c.line, v)
			}
		}
	}
}

func TestClientSendDoesNotModifyBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	batch := []Message{{Type: TypeTrack, MessageID: "a1/r"}}
	if err := NewClient(srv.URL, "wk", time.Second).Send(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if batch[0].SentAt != nil {
		t.Fatal("Send set sentAt on the caller's batch; a retry must get a fresh one")
	}
}
