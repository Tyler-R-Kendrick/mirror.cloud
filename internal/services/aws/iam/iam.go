// Package iam is IAM's policy evaluation: the authorizer the runtime checks
// every request with, and the operations behavior/aws/iam lists as native --
// SimulatePrincipalPolicy, SimulateCustomPolicy and GetAccountSummary. Both
// read the users, roles, groups and policies the bundle stores.
package iam

import (
	"context"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"GetAccountSummary", "SimulatePrincipalPolicy", "SimulateCustomPolicy"} {
		bundled.RegisterNative("aws.iam", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return (&native{deps}).invoke(ctx, req)
		})
	}
}

type native struct{ deps spi.Deps }

func (p *native) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	scope := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region)
	switch req.Operation {
	case "SimulatePrincipalPolicy":
		source := first(req.Input, "PolicySourceArn", "RoleName")
		kind, name := "role", roleNameFrom(source)
		if strings.Contains(source, ":user/") {
			kind, name = "user", userFromARN(source)
		}
		return p.simulate(ctx, loadPrincipalDocs(ctx, scope, kind, name), req, source)
	case "SimulateCustomPolicy":
		var docs []map[string]any
		for _, raw := range collectMembers(req.Input, "PolicyInputList") {
			docs = append(docs, parseDoc(raw))
		}
		return p.simulate(ctx, docs, req, "")
	default: // GetAccountSummary
		summary := map[string]any{}
		for member, collection := range map[string]string{"Users": "iamuser", "Roles": "iamrole", "Groups": "iamgroup", "Policies": "iampolicy", "InstanceProfiles": "iamip"} {
			kvs, _, _ := scope.Collection(collection).List(ctx, "", "", 0)
			summary[member] = len(kvs)
		}
		return &spi.Response{Output: map[string]any{"SummaryMap": summary}}, nil
	}
}

func (p *native) simulate(ctx context.Context, docs []map[string]any, req *spi.Request, source string) (*spi.Response, error) {
	actions := collectMembers(req.Input, "ActionNames")
	resources := collectMembers(req.Input, "ResourceArns")
	if len(resources) == 0 {
		resources = []string{"*"}
	}
	values := simulationContextValues(req)
	simulated := req.Identity
	if source != "" {
		simulated.ARN = source
		parts := strings.Split(source, ":")
		if len(parts) > 4 && len(parts[4]) == 12 {
			simulated.Account = parts[4]
		}
	}
	scps := loadSCPDocs(ctx, p.deps.Store, simulated)
	var results []any
	for _, act := range actions {
		for _, res := range resources {
			decision := decideWithContext(docs, act, res, values)
			orgDecision := "allowed"
			if len(scps) > 0 {
				orgDecision = decideWithContext(scps, act, res, values)
			}
			if orgDecision == "explicitDeny" || decision == "explicitDeny" {
				decision = "explicitDeny"
			} else if orgDecision != "allowed" {
				decision = "implicitDeny"
			}
			results = append(results, map[string]any{
				"EvalActionName": act, "EvalResourceName": res, "EvalDecision": decision,
				"OrganizationsDecisionDetail": map[string]any{"AllowedByOrganizations": orgDecision == "allowed"},
			})
		}
	}
	return &spi.Response{Output: map[string]any{"EvaluationResults": results}}, nil
}

func simulationContextValues(req *spi.Request) map[string]string {
	values := requestConditionValues(req)
	if entries, ok := req.Input["ContextEntries"].([]any); ok {
		for _, raw := range entries {
			entry, _ := raw.(map[string]any)
			if vals := asStrings(entry["ContextKeyValues"]); len(vals) > 0 {
				values[first(entry, "ContextKeyName")] = vals[0]
			}
		}
	}
	for key, raw := range req.Input {
		if !strings.HasSuffix(key, ".ContextKeyName") {
			continue
		}
		prefix := strings.TrimSuffix(key, ".ContextKeyName")
		name, _ := raw.(string)
		for candidate, value := range req.Input {
			if strings.HasPrefix(candidate, prefix+".ContextKeyValues") {
				if vals := asStrings(value); len(vals) > 0 {
					values[name] = vals[0]
				} else if text, ok := value.(string); ok {
					values[name] = text
				}
				break
			}
		}
	}
	return values
}

func collectMembers(in map[string]any, name string) []string {
	if v, ok := in[name]; ok {
		return asStrings(v)
	}
	var keys []string
	pfx := name + ".member."
	for k := range in {
		if strings.HasPrefix(k, pfx) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out []string
	for _, k := range keys {
		if s, ok := in[k].(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func roleNameFrom(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
