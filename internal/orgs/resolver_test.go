package orgs

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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
