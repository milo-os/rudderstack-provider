// Package orgs resolves the organization a request was made in.
package orgs

import (
	"context"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"go.miloapis.net/rudderstack-provider/internal/audit"
)

// ProjectGVK is Milo's Project. It is read as unstructured so the provider
// does not depend on Milo's Go module.
var ProjectGVK = schema.GroupVersionKind{Group: "resourcemanager.miloapis.com", Version: "v1alpha1", Kind: "Project"}

// CacheByObject configures the manager's cache to keep only what the resolver
// reads from each project. There is one project per customer project, so
// caching whole objects would grow memory with the platform.
func CacheByObject() map[client.Object]cache.ByObject {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ProjectGVK)
	return map[client.Object]cache.ByObject{obj: {Transform: TrimProject}}
}

// TrimProject strips a project down to its name, identity and owner.
func TrimProject(obj any) (any, error) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return obj, nil
	}
	t := &unstructured.Unstructured{}
	t.SetGroupVersionKind(u.GroupVersionKind())
	t.SetName(u.GetName())
	t.SetUID(u.GetUID())
	t.SetResourceVersion(u.GetResourceVersion())
	for _, field := range []string{"kind", "name"} {
		if v, found, _ := unstructured.NestedString(u.Object, "spec", "ownerRef", field); found {
			if err := unstructured.SetNestedField(t.Object, v, "spec", "ownerRef", field); err != nil {
				return nil, err
			}
		}
	}
	return t, nil
}

// +kubebuilder:rbac:groups=resourcemanager.miloapis.com,resources=projects,verbs=get;list;watch

// Resolver maps projects to the organization that owns them, kept current by
// an informer on Milo's projects.
type Resolver struct {
	mu       sync.RWMutex
	projects map[string]string

	synced toolscache.InformerSynced
}

// NewResolver returns an empty resolver.
func NewResolver() *Resolver {
	return &Resolver{projects: map[string]string{}}
}

// SetupWithManager registers the resolver with an informer on projects.
func (r *Resolver) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ProjectGVK)
	inf, err := mgr.GetCache().GetInformer(ctx, obj)
	if err != nil {
		return err
	}
	reg, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { r.upsert(obj) },
		UpdateFunc: func(_, obj any) { r.upsert(obj) },
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if u, ok := obj.(*unstructured.Unstructured); ok {
				r.Delete(u.GetName())
			}
		},
	})
	if err != nil {
		return err
	}
	r.synced = reg.HasSynced
	return nil
}

// HasSynced reports whether the resolver has seen every existing project.
func (r *Resolver) HasSynced() bool {
	return r.synced != nil && r.synced()
}

func (r *Resolver) upsert(obj any) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	kind, _, _ := unstructured.NestedString(u.Object, "spec", "ownerRef", "kind")
	org, _, _ := unstructured.NestedString(u.Object, "spec", "ownerRef", "name")
	if kind != "Organization" || org == "" {
		r.Delete(u.GetName())
		return
	}
	r.Set(u.GetName(), org)
}

// Set records that project belongs to org.
func (r *Resolver) Set(project, org string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.projects[project] = org
}

// Delete forgets project.
func (r *Resolver) Delete(project string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.projects, project)
}

// Organization returns the organization a request with tenant t was made in,
// or "" when it was not made in one or its project is not known yet.
func (r *Resolver) Organization(t audit.Tenant) string {
	switch strings.ToLower(t.Type) {
	case "organization":
		return t.Name
	case "project":
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.projects[t.Name]
	default:
		return ""
	}
}
