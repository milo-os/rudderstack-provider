// Package rules compiles TrackingRule CEL expressions and evaluates them
// against audit events.
package rules

import (
	"errors"
	"fmt"

	"github.com/google/cel-go/cel"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
)

// The variables an expression can read. Each holds the matching part of the
// audit event, decoded as JSON.
const (
	VarVerb      = "verb"
	VarUser      = "user"
	VarUserAgent = "userAgent"
	VarObjectRef = "objectRef"
	VarObject    = "object"
	VarTenant    = "tenant"
	VarActor     = "actor"
)

// CompiledRule is a TrackingRule with its CEL programs ready to evaluate.
type CompiledRule struct {
	Rule       *analyticsv1alpha1.TrackingRule
	Conditions map[string]cel.Program
	Properties map[string]cel.Program
	// UserID is nil when the rule uses the acting user's uid.
	UserID cel.Program
	// GroupID is nil when the rule uses the request's organization.
	GroupID cel.Program
}

// NewEnv returns the CEL environment rule expressions are compiled in.
func NewEnv() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable(VarVerb, cel.StringType),
		cel.Variable(VarUser, cel.DynType),
		cel.Variable(VarUserAgent, cel.StringType),
		cel.Variable(VarObjectRef, cel.DynType),
		cel.Variable(VarObject, cel.DynType),
		cel.Variable(VarTenant, cel.DynType),
		cel.Variable(VarActor, cel.DynType),
	)
}

// Compile compiles every expression in rule. It reports all failures at once
// so a rule's status names each broken expression.
func Compile(env *cel.Env, rule *analyticsv1alpha1.TrackingRule) (*CompiledRule, error) {
	cr := &CompiledRule{
		Rule:       rule,
		Conditions: make(map[string]cel.Program, len(rule.Spec.Match.Conditions)),
		Properties: make(map[string]cel.Program, len(rule.Spec.Properties)),
	}

	var errs []error
	for _, c := range rule.Spec.Match.Conditions {
		prg, err := compile(env, c.Expression, cel.BoolType)
		if err != nil {
			errs = append(errs, fmt.Errorf("condition %q: %w", c.Name, err))
			continue
		}
		cr.Conditions[c.Name] = prg
	}
	for _, p := range rule.Spec.Properties {
		prg, err := compile(env, p.Expression, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("property %q: %w", p.Name, err))
			continue
		}
		cr.Properties[p.Name] = prg
	}
	if rule.Spec.UserID != "" {
		prg, err := compile(env, rule.Spec.UserID, cel.StringType)
		if err != nil {
			errs = append(errs, fmt.Errorf("userId: %w", err))
		} else {
			cr.UserID = prg
		}
	}
	if rule.Spec.GroupID != "" {
		prg, err := compile(env, rule.Spec.GroupID, cel.StringType)
		if err != nil {
			errs = append(errs, fmt.Errorf("groupId: %w", err))
		} else {
			cr.GroupID = prg
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return cr, nil
}

// compile checks and plans one expression. When want is set, the expression
// must produce that type or dyn, since fields read from the event are dyn.
func compile(env *cel.Env, expr string, want *cel.Type) (cel.Program, error) {
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		return nil, iss.Err()
	}
	if want != nil && !ast.OutputType().IsExactType(want) && !ast.OutputType().IsExactType(cel.DynType) {
		return nil, fmt.Errorf("must evaluate to %s, got %s", want, ast.OutputType())
	}
	return env.Program(ast)
}
