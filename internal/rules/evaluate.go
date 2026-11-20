package rules

import (
	"errors"
	"fmt"
	"slices"

	"github.com/google/cel-go/cel"

	"go.miloapis.net/rudderstack-provider/internal/audit"
)

// Result is a rule's outcome for an event that matched it.
type Result struct {
	// UserID is the RudderStack userId for the call.
	UserID string
	// Properties are the rule's evaluated properties.
	Properties map[string]any
}

// Activation builds the CEL variables for an event. It is built once per
// event and shared by every rule.
func Activation(e *audit.Event) map[string]any {
	user := map[string]any{
		"username": e.User.Username,
		"uid":      e.User.UID,
		"groups":   stringsToAny(e.User.Groups),
		"extra":    extraToAny(e.User.Extra),
	}
	objectRef := map[string]any{}
	if ref := e.ObjectRef; ref != nil {
		objectRef = map[string]any{
			"apiGroup":    ref.APIGroup,
			"apiVersion":  ref.APIVersion,
			"resource":    ref.Resource,
			"subresource": ref.Subresource,
			"namespace":   ref.Namespace,
			"name":        ref.Name,
			"uid":         ref.UID,
		}
	}
	tenant := e.Tenant()
	return map[string]any{
		VarVerb:      e.Verb,
		VarUser:      user,
		VarUserAgent: e.UserAgent,
		VarObjectRef: objectRef,
		VarObject:    e.Object(),
		VarTenant:    map[string]any{"type": tenant.Type, "name": tenant.Name},
	}
}

// Evaluate returns the rule's result for the event, or nil when the event
// does not match. vars must come from Activation for the same event.
func (cr *CompiledRule) Evaluate(e *audit.Event, vars map[string]any) (*Result, error) {
	if !cr.matchesRequest(e) {
		return nil, nil
	}

	matched, err := cr.matchesConditions(vars)
	if err != nil || !matched {
		return nil, err
	}

	res := &Result{Properties: make(map[string]any, len(cr.Properties))}
	for name, prg := range cr.Properties {
		v, err := evalScalar(prg, vars)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		res.Properties[name] = v
	}

	res.UserID, err = cr.userID(e, vars)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// matchesRequest checks the rule's resource and verbs. Subresource requests,
// such as status updates, never match a resource rule.
func (cr *CompiledRule) matchesRequest(e *audit.Event) bool {
	if !slices.Contains(cr.Rule.Spec.Match.Verbs, e.Verb) {
		return false
	}
	sel := cr.Rule.Spec.Match.Resource
	if sel == nil {
		return true
	}
	ref := e.ObjectRef
	return ref != nil && ref.Subresource == "" && ref.APIGroup == sel.Group && ref.Resource == sel.Resource
}

// matchesConditions applies OR semantics. A condition that fails to evaluate
// does not match; the error is returned only when no condition matched.
func (cr *CompiledRule) matchesConditions(vars map[string]any) (bool, error) {
	if len(cr.Conditions) == 0 {
		return true, nil
	}
	var errs []error
	for name, prg := range cr.Conditions {
		out, _, err := prg.Eval(vars)
		if err != nil {
			errs = append(errs, fmt.Errorf("condition %q: %w", name, err))
			continue
		}
		if b, ok := out.Value().(bool); ok && b {
			return true, nil
		}
	}
	return false, errors.Join(errs...)
}

func (cr *CompiledRule) userID(e *audit.Event, vars map[string]any) (string, error) {
	if cr.UserID != nil {
		v, err := evalScalar(cr.UserID, vars)
		if err != nil {
			return "", fmt.Errorf("userId: %w", err)
		}
		s, ok := v.(string)
		if !ok || s == "" {
			return "", errors.New("userId: did not evaluate to a non-empty string")
		}
		return s, nil
	}
	if e.User.UID != "" {
		return e.User.UID, nil
	}
	if e.User.Username != "" {
		return e.User.Username, nil
	}
	return "", errors.New("event has no user uid or username")
}

// evalScalar evaluates prg and requires a string, number or boolean, the
// value types RudderStack properties take.
func evalScalar(prg cel.Program, vars map[string]any) (any, error) {
	out, _, err := prg.Eval(vars)
	if err != nil {
		return nil, err
	}
	switch v := out.Value().(type) {
	case string, bool, int64, uint64, float64:
		return v, nil
	default:
		return nil, fmt.Errorf("evaluated to %s, want a string, number or boolean", out.Type().TypeName())
	}
}

func stringsToAny(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

func extraToAny(in map[string][]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = stringsToAny(v)
	}
	return out
}
