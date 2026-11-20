package orgs

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	toolscache "k8s.io/client-go/tools/cache"

	"go.miloapis.net/rudderstack-provider/internal/audit"
)

func project(name, ownerKind, owner string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"ownerRef": map[string]any{"kind": ownerKind, "name": owner}},
	}}
	u.SetGroupVersionKind(ProjectGVK)
	u.SetName(name)
	return u
}

func TestResolver(t *testing.T) {
	r := NewResolver()
	r.upsert(project("p1", "Organization", "acme"))
	r.upsert(project("p2", "User", "alice"))

	tests := []struct {
		tenant audit.Tenant
		want   string
	}{
		{audit.Tenant{Type: "Organization", Name: "acme"}, "acme"},
		{audit.Tenant{Type: "organization", Name: "acme"}, "acme"},
		{audit.Tenant{Type: "project", Name: "p1"}, "acme"},
		{audit.Tenant{Type: "Project", Name: "p1"}, "acme"},
		{audit.Tenant{Type: "project", Name: "p2"}, ""},
		{audit.Tenant{Type: "project", Name: "unknown"}, ""},
		{audit.Tenant{Type: "user", Name: "alice"}, ""},
		{audit.Tenant{Type: "platform"}, ""},
	}
	for _, tt := range tests {
		if got := r.Organization(tt.tenant); got != tt.want {
			t.Errorf("Organization(%+v) = %q, want %q", tt.tenant, got, tt.want)
		}
	}

	// A project moved to another owner, then deleted.
	r.upsert(project("p1", "Organization", "globex"))
	if got := r.Organization(audit.Tenant{Type: "project", Name: "p1"}); got != "globex" {
		t.Fatalf("after update = %q, want globex", got)
	}
	r.Delete("p1")
	if got := r.Organization(audit.Tenant{Type: "project", Name: "p1"}); got != "" {
		t.Fatalf("after delete = %q, want empty", got)
	}
	if r.HasSynced() {
		t.Fatal("HasSynced() = true before registering with an informer")
	}
}

func TestTrimProject(t *testing.T) {
	full := project("p1", "Organization", "acme")
	full.SetUID("uid-1")
	full.SetResourceVersion("42")
	full.SetLabels(map[string]string{"a": "b"})
	full.SetAnnotations(map[string]string{"big": "value"})
	full.Object["status"] = map[string]any{"conditions": []any{map[string]any{"type": "Ready"}}}
	if err := unstructured.SetNestedField(full.Object, "Acme Widgets", "spec", "displayName"); err != nil {
		t.Fatal(err)
	}

	got, err := TrimProject(full)
	if err != nil {
		t.Fatal(err)
	}
	trimmed := got.(*unstructured.Unstructured)
	if trimmed.GetName() != "p1" || trimmed.GetUID() != "uid-1" || trimmed.GetResourceVersion() != "42" || trimmed.GroupVersionKind() != ProjectGVK {
		t.Fatalf("lost identity: %+v", trimmed.Object)
	}
	if trimmed.GetLabels() != nil || trimmed.GetAnnotations() != nil || trimmed.Object["status"] != nil {
		t.Fatalf("kept fields the resolver does not read: %+v", trimmed.Object)
	}
	if _, found, _ := unstructured.NestedString(trimmed.Object, "spec", "displayName"); found {
		t.Fatalf("kept spec.displayName: %+v", trimmed.Object)
	}

	// The resolver reads the trimmed object as it would the full one.
	r := NewResolver()
	r.upsert(trimmed)
	if got := r.Organization(audit.Tenant{Type: "project", Name: "p1"}); got != "acme" {
		t.Fatalf("Organization() = %q, want acme", got)
	}

	tomb := toolscache.DeletedFinalStateUnknown{Key: "p1", Obj: full}
	if got, err := TrimProject(tomb); err != nil || got != any(tomb) {
		t.Fatalf("TrimProject(tombstone) = %v, %v; want it unchanged", got, err)
	}
}
