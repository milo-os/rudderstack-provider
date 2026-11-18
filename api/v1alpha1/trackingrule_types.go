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

// TriggerType decides when a matching audit event produces a call.
// +kubebuilder:validation:Enum=Every;Once;Threshold;Inactivity;Frequency
type TriggerType string

const (
	// TriggerEvery fires once for every matching audit event.
	TriggerEvery TriggerType = "Every"
	// TriggerOnce fires the first time a subject matches, and never again.
	// Use it for "first X" events.
	TriggerOnce TriggerType = "Once"
	// TriggerThreshold fires once per subject when the number of distinct
	// resources it holds first reaches the threshold.
	TriggerThreshold TriggerType = "Threshold"
	// TriggerInactivity fires when a subject has not been seen for a period.
	TriggerInactivity TriggerType = "Inactivity"
	// TriggerFrequency fires when a subject has been seen on enough distinct
	// days within a window.
	TriggerFrequency TriggerType = "Frequency"
)

// TrackingRuleSpec defines which audit events become RudderStack calls.
// +kubebuilder:validation:XValidation:rule="!has(self.call) || self.call == 'Track' || self.trigger.type == 'Every'",message="identify and group calls require the Every trigger"
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
	// that owns the resource.
	// +kubebuilder:default=User
	// +optional
	Subject SubjectType `json:"subject,omitempty"`

	// UserID is a CEL expression for the RudderStack userId, over the same
	// variables as conditions. Defaults to the acting user's uid. Set it when
	// the event is about a user other than the one making the request, such as
	// a User object created by the sign-up flow.
	// +optional
	UserID string `json:"userId,omitempty"`

	// Trigger decides when matching events produce a call.
	// +required
	Trigger Trigger `json:"trigger"`

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

// Trigger decides when matching events produce a call.
// +kubebuilder:validation:XValidation:rule="(self.type == 'Threshold') == has(self.threshold)",message="threshold must be set exactly when type is Threshold"
// +kubebuilder:validation:XValidation:rule="(self.type == 'Inactivity') == has(self.inactivity)",message="inactivity must be set exactly when type is Inactivity"
// +kubebuilder:validation:XValidation:rule="(self.type == 'Frequency') == has(self.frequency)",message="frequency must be set exactly when type is Frequency"
type Trigger struct {
	// Type is the trigger kind.
	// +required
	Type TriggerType `json:"type"`

	// +optional
	Threshold *ThresholdTrigger `json:"threshold,omitempty"`

	// +optional
	Inactivity *InactivityTrigger `json:"inactivity,omitempty"`

	// +optional
	Frequency *FrequencyTrigger `json:"frequency,omitempty"`
}

// ThresholdTrigger fires once per subject when the number of distinct
// resources it holds first reaches AtLeast. Creates add a resource and deletes
// remove it, so the count reflects resources that currently exist.
type ThresholdTrigger struct {
	// AtLeast is the count that fires the rule. Use 1 for "first X".
	// +kubebuilder:validation:Minimum=1
	// +required
	AtLeast int32 `json:"atLeast"`
}

// InactivityTrigger fires when a subject has not been seen for After. It fires
// again only after the subject becomes active and then inactive once more.
type InactivityTrigger struct {
	// After is how long a subject must be inactive, for example "336h".
	// +required
	After metav1.Duration `json:"after"`
}

// FrequencyTrigger fires when a subject has been seen on at least Days
// distinct days within Window.
type FrequencyTrigger struct {
	// Days is the number of distinct active days required.
	// +kubebuilder:validation:Minimum=1
	// +required
	Days int32 `json:"days"`

	// Window is the period the days are counted over, for example "336h".
	// +required
	Window metav1.Duration `json:"window"`
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
// +kubebuilder:printcolumn:name="Trigger",type=string,JSONPath=`.spec.trigger.type`
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
