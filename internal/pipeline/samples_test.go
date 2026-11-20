package pipeline

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

// sampleProcessor loads the rules in config/samples.
func sampleProcessor(t *testing.T) (*Processor, *recordingSubmitter) {
	t.Helper()
	env, err := rules.NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	store := rules.NewStore(env)

	files, err := filepath.Glob(filepath.Join("..", "..", "config", "samples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if filepath.Base(file) == "kustomization.yaml" {
			continue
		}
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
		for {
			rule := &analyticsv1alpha1.TrackingRule{}
			if err := dec.Decode(rule); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			if err := store.Upsert(rule); err != nil {
				t.Fatalf("%s: %s: %v", file, rule.Name, err)
			}
		}
		_ = f.Close()
	}
	sub := &recordingSubmitter{}
	return &Processor{Rules: store, Batcher: sub, Log: logr.Discard()}, sub
}

func projectScope(name string) map[string]any {
	return map[string]any{
		"platform.miloapis.com/scope.type": "project",
		"platform.miloapis.com/scope.name": name,
	}
}

func onlyCall(t *testing.T, sub *recordingSubmitter) rudderstack.Message {
	t.Helper()
	if len(sub.calls) != 1 {
		t.Fatalf("calls = %+v, want exactly one", sub.calls)
	}
	return sub.calls[0]
}

func TestSampleDomainAddedByHuman(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{"annotations": projectScope("p1")}))

	c := onlyCall(t, sub)
	if c.Event != "Domain Added" || c.UserID != "u1" {
		t.Fatalf("call = %+v", c)
	}
	want := map[string]any{"name": "example-com", "namespace": "", "project": "p1", "changeSource": "human"}
	for k, v := range want {
		if c.Properties[k] != v {
			t.Fatalf("properties[%q] = %v, want %v (all: %v)", k, c.Properties[k], v, c.Properties)
		}
	}
}

func TestSampleDomainRemovedByGarbageCollector(t *testing.T) {
	// Deleting a project deletes its domains as the garbage collector. The
	// removal must still be sent so downstream counts go down.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"verb":           "delete",
		"user":           map[string]any{"username": "system:serviceaccount:kube-system:generic-garbage-collector", "uid": "gc"},
		"responseStatus": map[string]any{"code": 200},
		"responseObject": map[string]any{"kind": "Status", "status": "Success"},
		"annotations":    projectScope("p1"),
	}))

	c := onlyCall(t, sub)
	if c.Event != "Domain Removed" || c.Properties["changeSource"] != "system" ||
		c.Properties["name"] != "example-com" || c.Properties["project"] != "p1" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleProjectOutsideProjectScope(t *testing.T) {
	// Projects are created in an organization scope, so project is empty.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"objectRef": map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "projects", "name": "p1"},
	}))

	c := onlyCall(t, sub)
	if c.Event != "Project Created" || c.Properties["project"] != "" || c.Context["groupId"] != "acme" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleActivitySignalsAreLimited(t *testing.T) {
	// datumctl Used and Active carry limit: OncePerDay, which is skipped
	// until the limit is implemented, so a plain read sends nothing.
	p, sub := sampleProcessor(t)
	msg := auditMsg(t, map[string]any{"verb": "list", "objectRef": nil})
	p.Handle(msg)

	if len(sub.calls) != 0 {
		t.Fatalf("calls = %+v, want none until the once-per-day limit exists", sub.calls)
	}
	if acked, _ := msg.state(); !acked {
		t.Fatal("message was not acked")
	}
}
