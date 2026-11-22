/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// CallType is the RudderStack API call a rule emits.
// +kubebuilder:validation:Enum=Track;Identify;Group
type CallType string

const (
	// CallTrack records that the subject did something.
	CallTrack CallType = "Track"
	// CallIdentify records traits about a user.
	CallIdentify CallType = "Identify"
	// CallGroup records traits about an organization.
	CallGroup CallType = "Group"
)

// SubjectType is what a rule counts and fires for.
// +kubebuilder:validation:Enum=User;Organization
type SubjectType string

const (
	// SubjectUser keys the rule on the acting user.
	SubjectUser SubjectType = "User"
	// SubjectOrganization keys the rule on the organization that owns the
	// resource.
	SubjectOrganization SubjectType = "Organization"
)

// LimitType caps how often a rule sends.
// +kubebuilder:validation:Enum=OncePerDay
type LimitType string

const (
	// LimitOncePerDay sends at most one call per rule and subject per UTC
	// day. Use it for activity signals, such as "datumctl Used", whose
	// matching requests would otherwise flood RudderStack.
	LimitOncePerDay LimitType = "OncePerDay"
)

// TrackingRuleSpec defines which audit events become RudderStack calls.
type TrackingRuleSpec struct {
	// Event is the RudderStack event name, for example "Domain Added". Use
	// Object + Action naming. Ignored for Identify and Group calls.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +required
	Event string `json:"event"`

	// Call is the RudderStack call to make.
	// +kubebuilder:default=Track
	// +optional
	Call CallType `json:"call,omitempty"`

	// Match selects the audit events this rule applies to.
	// +required
	Match Match `json:"match"`

	// Subject is who the event is about: the acting user, or the organization
	// that owns the resource. Once-per-day limits are counted per subject.
	// +kubebuilder:default=User
	// +optional
	Subject SubjectType `json:"subject,omitempty"`

	// UserID is a CEL expression for the RudderStack userId, over the same
	// variables as conditions. Defaults to the acting user's uid. Set it when
	// the event is about a user other than the one making the request, such as
	// a User object created by the sign-up flow.
	// +optional
	UserID string `json:"userId,omitempty"`

	// GroupID is a CEL expression for the RudderStack groupId, over the same
	// variables as conditions. Defaults to the organization the request was
	// made in, resolved from its project when the request is project-scoped.
	// Set it when the event is about an organization other than the request's
	// scope, such as an Organization being created. An empty result falls back
	// to the request's organization.
	// +optional
	GroupID string `json:"groupId,omitempty"`

	// Limit caps how often the rule sends. When unset, every matching event
	// is sent.
	// +optional
	Limit LimitType `json:"limit,omitempty"`

	// Properties are sent with the call (as traits for Identify and Group).
	// Only fields listed here leave the platform.
	// +kubebuilder:validation:MaxItems=20
	// +listType=map
	// +listMapKey=name
	// +optional
	Properties []Property `json:"properties,omitempty"`
}

// Match selects audit events. All of resource, verbs and userAgent must match,
// and then at least one condition when any are set.
type Match struct {
	// Resource limits the rule to one resource type. When unset, every
	// resource matches, which suits user-level rules such as datumctl access.
	// +optional
	Resource *ResourceSelector `json:"resource,omitempty"`

	// Verbs are the audit verbs to match, for example create or delete.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:items:Enum=create;update;patch;delete;get;list;watch
	// +listType=set
	// +required
	Verbs []string `json:"verbs"`

	// Conditions filter matching events using CEL expressions, evaluated with
	// OR semantics. Each must evaluate to a boolean. The audit event is
	// available as:
	//   - verb: the audit verb
	//   - user: the authenticated user (username, uid, groups, extra)
	//   - userAgent: the client user agent, e.g. "datumctl/v0.12.0"
	//   - objectRef: the target (apiGroup, resource, namespace, name, uid)
	//   - object: the response object, or the request object when the
	//     response does not carry it
	//   - tenant: the scope of the request (type, name)
	// +kubebuilder:validation:MaxItems=10
	// +listType=map
	// +listMapKey=name
	// +optional
	Conditions []Condition `json:"conditions,omitempty"`
}

// ResourceSelector identifies a resource type the way audit events do.
type ResourceSelector struct {
	// Group is the API group. Empty for the core group.
	// +optional
	Group string `json:"group,omitempty"`

	// Resource is the plural resource name, for example "domains". Audit
	// events name the resource this way for every verb, including deletes
	// whose response carries no kind.
	// +kubebuilder:validation:MinLength=1
	// +required
	Resource string `json:"resource"`
}

// Condition is a named CEL condition.
type Condition struct {
	// Name identifies the condition in status and logs.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=256
	// +required
	Name string `json:"name"`

	// Expression is a CEL expression that must evaluate to a boolean.
	// +kubebuilder:validation:MinLength=1
	// +required
	Expression string `json:"expression"`
}

// Property is a named value sent with the call.
type Property struct {
	// Name is the property name in RudderStack.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=128
	// +required
	Name string `json:"name"`

	// Expression is a CEL expression over the same variables as conditions.
	// It must evaluate to a string, number or boolean.
	// +kubebuilder:validation:MinLength=1
	// +required
	Expression string `json:"expression"`
}

// TrackingRuleStatus defines the observed state of TrackingRule.
type TrackingRuleStatus struct {
	// Conditions report whether the rule's CEL compiled and the rule is
	// active. The Ready condition is False with reason InvalidExpression when
	// any expression fails to compile.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation the conditions were computed for.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:printcolumn:name="Event",type=string,JSONPath=`.spec.event`
// +kubebuilder:printcolumn:name="Limit",type=string,JSONPath=`.spec.limit`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TrackingRule maps platform audit events to RudderStack analytics calls.
type TrackingRule struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TrackingRule
	// +required
	Spec TrackingRuleSpec `json:"spec"`

	// status defines the observed state of TrackingRule
	// +optional
	Status TrackingRuleStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TrackingRuleList contains a list of TrackingRule
type TrackingRuleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TrackingRule `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TrackingRule{}, &TrackingRuleList{})
		return nil
	})
}
