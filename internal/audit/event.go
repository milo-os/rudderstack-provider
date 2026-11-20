// Package audit decodes the Kubernetes audit events the provider consumes.
package audit

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	// StageResponseComplete is the only stage whose outcome is known.
	StageResponseComplete = "ResponseComplete"

	// Scope annotations set by Milo on audit events, the same ones search
	// reads to find a request's tenant.
	ScopeTypeAnnotation = "platform.miloapis.com/scope.type"
	ScopeNameAnnotation = "platform.miloapis.com/scope.name"

	// User extra keys that carry the parent resource of a scoped request,
	// used when the scope annotations are absent.
	ParentTypeExtra = "iam.miloapis.com/parent-type"
	ParentNameExtra = "iam.miloapis.com/parent-name"
)

// Event is the subset of an audit.k8s.io/v1 Event the provider reads.
type Event struct {
	AuditID                  string            `json:"auditID"`
	Stage                    string            `json:"stage"`
	RequestURI               string            `json:"requestURI"`
	Verb                     string            `json:"verb"`
	User                     User              `json:"user"`
	UserAgent                string            `json:"userAgent"`
	ObjectRef                *ObjectRef        `json:"objectRef,omitempty"`
	ResponseStatus           *ResponseStatus   `json:"responseStatus,omitempty"`
	RequestObject            map[string]any    `json:"requestObject,omitempty"`
	ResponseObject           map[string]any    `json:"responseObject,omitempty"`
	RequestReceivedTimestamp time.Time         `json:"requestReceivedTimestamp"`
	StageTimestamp           time.Time         `json:"stageTimestamp"`
	Annotations              map[string]string `json:"annotations,omitempty"`
}

// User is the authenticated user that made the request.
type User struct {
	Username string              `json:"username"`
	UID      string              `json:"uid"`
	Groups   []string            `json:"groups,omitempty"`
	Extra    map[string][]string `json:"extra,omitempty"`
}

// ObjectRef is the target of the request.
type ObjectRef struct {
	APIGroup    string `json:"apiGroup"`
	APIVersion  string `json:"apiVersion"`
	Resource    string `json:"resource"`
	Subresource string `json:"subresource"`
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	UID         string `json:"uid"`
}

// ResponseStatus is the status the API server returned.
type ResponseStatus struct {
	Code int `json:"code"`
}

// Kinds of actor that make requests.
const (
	// ActorUser is a person.
	ActorUser = "user"
	// ActorServiceAccount is a customer's machine account, such as a CI
	// pipeline.
	ActorServiceAccount = "serviceaccount"
	// ActorSystem is the platform itself: a controller or Kubernetes
	// service account.
	ActorSystem = "system"
)

// ActorKind classifies who made the request. Platform identities have a
// "system:" username. Customer machine accounts authenticate as an email of
// the form {name}@{project}.{suffix}, such as
// ci-a1b2c3@my-project.iam.datumapis.com, so serviceAccountSuffix
// identifies them; when it is empty they count as users.
func (e *Event) ActorKind(serviceAccountSuffix string) string {
	name := e.User.Username
	switch {
	case strings.HasPrefix(name, "system:"):
		return ActorSystem
	case serviceAccountSuffix != "" && strings.HasSuffix(name, "."+strings.TrimPrefix(serviceAccountSuffix, ".")):
		return ActorServiceAccount
	default:
		return ActorUser
	}
}

// Tenant is the scope a request was made in.
type Tenant struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// Parse decodes an audit event.
func Parse(data []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// Trackable reports whether the event records a completed request that
// changed or read something for real. Failed requests, dry runs and
// in-progress stages are not tracked.
func (e *Event) Trackable() bool {
	if e.Stage != "" && e.Stage != StageResponseComplete {
		return false
	}
	if e.ResponseStatus != nil && e.ResponseStatus.Code >= 300 {
		return false
	}
	return !strings.Contains(e.RequestURI, "dryRun=")
}

// Tenant returns the scope of the request, or a platform tenant when the
// request was not scoped.
func (e *Event) Tenant() Tenant {
	t := Tenant{Type: e.Annotations[ScopeTypeAnnotation], Name: e.Annotations[ScopeNameAnnotation]}
	if t.Type == "" {
		t.Type = firstExtra(e.User.Extra, ParentTypeExtra)
		t.Name = firstExtra(e.User.Extra, ParentNameExtra)
	}
	if t.Type == "" {
		return Tenant{Type: "platform"}
	}
	return t
}

// Object is the resource the request acted on: the response object, or the
// request object when the response carries none.
func (e *Event) Object() map[string]any {
	if e.ResponseObject != nil {
		return e.ResponseObject
	}
	if e.RequestObject != nil {
		return e.RequestObject
	}
	return map[string]any{}
}

// Timestamp is when the request was received, falling back to the stage
// timestamp.
func (e *Event) Timestamp() time.Time {
	if !e.RequestReceivedTimestamp.IsZero() {
		return e.RequestReceivedTimestamp
	}
	return e.StageTimestamp
}

func firstExtra(extra map[string][]string, key string) string {
	if v := extra[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}
