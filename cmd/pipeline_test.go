package main

import (
	"strings"
	"testing"
	"time"

	"go.miloapis.net/rudderstack-provider/internal/pipeline"
)

func TestPipelineOptionsValidate(t *testing.T) {
	valid := func() pipelineOptions {
		return pipelineOptions{
			natsURL:      "nats://localhost:4222",
			dataPlaneURL: "https://dataplane.example.com",
			batch:        pipeline.BatchConfig{MaxCalls: 100, FlushInterval: time.Second, SendTimeout: time.Second},
		}
	}

	tests := []struct {
		name     string
		mutate   func(*pipelineOptions)
		writeKey string
		wantErr  string
	}{
		{name: "valid", writeKey: "wk"},
		{name: "disabled", mutate: func(o *pipelineOptions) { *o = pipelineOptions{} }},
		{name: "dry run needs no RudderStack", writeKey: "",
			mutate: func(o *pipelineOptions) { o.dryRun = true; o.dataPlaneURL = "" }},
		{name: "no data plane", writeKey: "wk",
			mutate:  func(o *pipelineOptions) { o.dataPlaneURL = "" },
			wantErr: "--rudderstack-data-plane-url"},
		{name: "no write key", wantErr: writeKeyEnv},
		{name: "cert without key", writeKey: "wk",
			mutate:  func(o *pipelineOptions) { o.natsTLSCert = "tls.crt" },
			wantErr: "--nats-tls-key"},
		{name: "no calls per batch", writeKey: "wk",
			mutate:  func(o *pipelineOptions) { o.batch.MaxCalls = 0 },
			wantErr: "--batch-max-calls"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(writeKeyEnv, tt.writeKey)
			o := valid()
			if tt.mutate != nil {
				tt.mutate(&o)
			}
			err := o.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestCheckAckWait(t *testing.T) {
	cfg := pipeline.BatchConfig{FlushInterval: 5 * time.Second, SendTimeout: 30 * time.Second}
	if err := checkAckWait(65*time.Second, cfg); err != nil {
		t.Fatalf("65s ackWait: %v", err)
	}
	if err := checkAckWait(60*time.Second, cfg); err == nil {
		t.Fatal("60s ackWait: expected an error")
	}
}

func TestCheckBucketTTL(t *testing.T) {
	for ttl, ok := range map[time.Duration]bool{0: false, time.Hour: false, 24 * time.Hour: true, 25 * time.Hour: true} {
		if err := checkBucketTTL(ttl); (err == nil) != ok {
			t.Errorf("checkBucketTTL(%s) = %v, want ok=%v", ttl, err, ok)
		}
	}
}
