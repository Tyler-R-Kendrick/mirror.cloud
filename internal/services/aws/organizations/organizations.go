// Package organizations stores organization, account, OU, and SCP records.
package organizations

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.organizations", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Organizations-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.organizations" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateOrganization", "DescribeOrganization", "DeleteOrganization",
		"CreateAccount", "DescribeAccount", "ListAccounts", "MoveAccount", "ListParents", "ListChildren",
		"CreateOrganizationalUnit", "DescribeOrganizationalUnit", "ListOrganizationalUnitsForParent", "DeleteOrganizationalUnit",
		"ListRoots", "EnablePolicyType", "DisablePolicyType",
		"CreatePolicy", "DescribePolicy", "ListPolicies", "DeletePolicy",
		"AttachPolicy", "DetachPolicy", "ListPoliciesForTarget", "ListTargetsForPolicy",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateOrganization":
		id := "o-" + p.deps.Rand.Hex(8)
		root := "r-" + p.deps.Rand.Hex(4)
		rec := map[string]any{
			"Id": id, "Arn": "arn:aws:organizations::" + req.Identity.Account + ":organization/" + id,
			"MasterAccountId": req.Identity.Account, "FeatureSet": first(req.Input, "FeatureSet"), "RootId": root,
		}
		if rec["FeatureSet"] == "" {
			rec["FeatureSet"] = "ALL"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "org").Put(ctx, "current", b)
		rootRec := map[string]any{"Id": root, "Name": "Root", "Arn": "arn:aws:organizations::" + req.Identity.Account + ":root/" + id + "/" + root}
		if rec["FeatureSet"] == "ALL" {
			rootRec["PolicyTypes"] = []any{map[string]any{"Type": "SERVICE_CONTROL_POLICY", "Status": "ENABLED"}}
		}
		rb, _ := json.Marshal(rootRec)
		_ = p.col(req, "oroot").Put(ctx, root, rb)
		full := map[string]any{
			"PolicySummary": map[string]any{"Id": "p-FullAWSAccess", "Arn": "arn:aws:organizations::aws:policy/service_control_policy/p-FullAWSAccess", "Name": "FullAWSAccess", "Type": "SERVICE_CONTROL_POLICY", "AwsManaged": true},
			"Content":       `{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"*","Resource":"*"}}`,
		}
		p.put(ctx, req, "opolicy", "p-FullAWSAccess", full)
		p.put(ctx, req, "oattach", root+"/p-FullAWSAccess", map[string]any{"TargetId": root, "PolicyId": "p-FullAWSAccess"})
		return &spi.Response{Output: map[string]any{"Organization": rec}}, nil
	case "DescribeOrganization":
		b, ok, _ := p.col(req, "org").Get(ctx, "current")
		if !ok {
			return nil, &spi.Fault{Code: "AWSOrganizationsNotInUseException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Organization": rec}}, nil
	case "DeleteOrganization":
		accounts, _, _ := p.col(req, "oacct").List(ctx, "", "", 0)
		for _, account := range accounts {
			_ = p.deps.Store.Scope("_mirror", "global").Collection("orgmembers").Delete(ctx, account.Key)
		}
		_ = p.col(req, "org").Delete(ctx, "current")
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAccount":
		id := p.deps.Rand.Hex(12)
		email := first(req.Input, "Email")
		name := first(req.Input, "AccountName")
		org, _, _ := p.get(ctx, req, "org", "current")
		root := first(org, "RootId")
		rec := map[string]any{"Id": id, "Name": name, "Email": email, "Status": "ACTIVE", "ParentId": root}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "oacct").Put(ctx, id, b)
		member, _ := json.Marshal(map[string]any{"ManagementAccount": req.Identity.Account, "Region": req.Identity.Region, "RootId": root, "ParentId": root})
		_ = p.deps.Store.Scope("_mirror", "global").Collection("orgmembers").Put(ctx, id, member)
		return &spi.Response{Output: map[string]any{"CreateAccountStatus": map[string]any{"Id": "car-" + id, "AccountId": id, "State": "SUCCEEDED", "AccountName": name}}}, nil
	case "DescribeAccount":
		id := first(req.Input, "AccountId")
		b, ok, _ := p.col(req, "oacct").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "AccountNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Account": rec}}, nil
	case "ListAccounts":
		kvs, _, _ := p.col(req, "oacct").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Accounts": items}}, nil
	case "MoveAccount":
		id := first(req.Input, "AccountId")
		account, ok, _ := p.get(ctx, req, "oacct", id)
		if !ok {
			return nil, &spi.Fault{Code: "AccountNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		parent := first(req.Input, "DestinationParentId")
		account["ParentId"] = parent
		p.put(ctx, req, "oacct", id, account)
		members := p.deps.Store.Scope("_mirror", "global").Collection("orgmembers")
		if b, found, _ := members.Get(ctx, id); found {
			var member map[string]any
			_ = json.Unmarshal(b, &member)
			member["ParentId"] = parent
			updated, _ := json.Marshal(member)
			_ = members.Put(ctx, id, updated)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListParents":
		id := first(req.Input, "ChildId")
		rec, ok, _ := p.get(ctx, req, "oacct", id)
		if !ok {
			rec, ok, _ = p.get(ctx, req, "oou", id)
		}
		if !ok {
			return &spi.Response{Output: map[string]any{"Parents": []any{}}}, nil
		}
		parent := first(rec, "ParentId")
		kind := "ORGANIZATIONAL_UNIT"
		if len(parent) > 1 && parent[:2] == "r-" {
			kind = "ROOT"
		}
		return &spi.Response{Output: map[string]any{"Parents": []any{map[string]any{"Id": parent, "Type": kind}}}}, nil
	case "ListChildren":
		return p.listChildren(ctx, req)
	case "CreateOrganizationalUnit":
		id := "ou-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "ParentId": first(req.Input, "ParentId"), "Tags": req.Input["Tags"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "oou").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"OrganizationalUnit": rec}}, nil
	case "DescribeOrganizationalUnit":
		id := first(req.Input, "OrganizationalUnitId")
		b, ok, _ := p.col(req, "oou").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "OrganizationalUnitNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"OrganizationalUnit": rec}}, nil
	case "ListOrganizationalUnitsForParent":
		parent := first(req.Input, "ParentId")
		kvs, _, _ := p.col(req, "oou").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if parent != "" && rec["ParentId"] != parent {
				continue
			}
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"OrganizationalUnits": items}}, nil
	case "DeleteOrganizationalUnit":
		_ = p.col(req, "oou").Delete(ctx, first(req.Input, "OrganizationalUnitId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRoots":
		kvs, _, _ := p.col(req, "oroot").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Roots": items}}, nil
	case "EnablePolicyType", "DisablePolicyType":
		rootID := first(req.Input, "RootId")
		root, ok, _ := p.get(ctx, req, "oroot", rootID)
		if !ok {
			return nil, &spi.Fault{Code: "RootNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		status := "ENABLED"
		if req.Operation == "DisablePolicyType" {
			status = "PENDING_DISABLE"
		}
		root["PolicyTypes"] = []any{map[string]any{"Type": first(req.Input, "PolicyType"), "Status": status}}
		p.put(ctx, req, "oroot", rootID, root)
		return &spi.Response{Output: map[string]any{"Root": root}}, nil
	case "CreatePolicy":
		id := "p-" + p.deps.Rand.Hex(8)
		typ := first(req.Input, "Type")
		if typ == "" {
			typ = "SERVICE_CONTROL_POLICY"
		}
		summary := map[string]any{
			"Id": id, "Arn": "arn:aws:organizations::" + req.Identity.Account + ":policy/" + typ + "/" + id,
			"Name": first(req.Input, "Name"), "Description": first(req.Input, "Description"), "Type": typ, "AwsManaged": false,
		}
		policy := map[string]any{"PolicySummary": summary, "Content": req.Input["Content"]}
		p.put(ctx, req, "opolicy", id, policy)
		return &spi.Response{Output: map[string]any{"Policy": policy}}, nil
	case "DescribePolicy":
		policy, ok, _ := p.get(ctx, req, "opolicy", first(req.Input, "PolicyId"))
		if !ok {
			return nil, policyMissing()
		}
		return &spi.Response{Output: map[string]any{"Policy": policy}}, nil
	case "ListPolicies":
		return p.listPolicies(ctx, req)
	case "DeletePolicy":
		id := first(req.Input, "PolicyId")
		if id == "p-FullAWSAccess" {
			return nil, &spi.Fault{Code: "PolicyNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "opolicy").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{}}, nil
	case "AttachPolicy":
		target, policy := first(req.Input, "TargetId"), first(req.Input, "PolicyId")
		if _, ok, _ := p.get(ctx, req, "opolicy", policy); !ok {
			return nil, policyMissing()
		}
		p.put(ctx, req, "oattach", target+"/"+policy, map[string]any{"TargetId": target, "PolicyId": policy})
		return &spi.Response{Output: map[string]any{}}, nil
	case "DetachPolicy":
		_ = p.col(req, "oattach").Delete(ctx, first(req.Input, "TargetId")+"/"+first(req.Input, "PolicyId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPoliciesForTarget":
		return p.listPoliciesForTarget(ctx, req)
	case "ListTargetsForPolicy":
		return p.listTargetsForPolicy(ctx, req)
	default:
		return nil, spi.NotImplemented("aws.organizations", req.Operation, "emulate")
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func (p *Pack) put(ctx context.Context, req *spi.Request, collection, key string, value map[string]any) {
	b, _ := json.Marshal(value)
	_ = p.col(req, collection).Put(ctx, key, b)
}

func (p *Pack) get(ctx context.Context, req *spi.Request, collection, key string) (map[string]any, bool, error) {
	b, ok, err := p.col(req, collection).Get(ctx, key)
	if !ok || err != nil {
		return nil, ok, err
	}
	var value map[string]any
	_ = json.Unmarshal(b, &value)
	return value, true, nil
}

func (p *Pack) listPolicies(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	want := first(req.Input, "Filter")
	kvs, _, _ := p.col(req, "opolicy").List(ctx, "", "", 0)
	policies := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var policy map[string]any
		_ = json.Unmarshal(kv.Value, &policy)
		summary, _ := policy["PolicySummary"].(map[string]any)
		if want == "" || first(summary, "Type") == want {
			policies = append(policies, summary)
		}
	}
	return &spi.Response{Output: map[string]any{"Policies": policies}}, nil
}

func (p *Pack) listPoliciesForTarget(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	target := first(req.Input, "TargetId")
	kvs, _, _ := p.col(req, "oattach").List(ctx, target+"/", "", 0)
	policies := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var attachment map[string]any
		_ = json.Unmarshal(kv.Value, &attachment)
		policy, ok, _ := p.get(ctx, req, "opolicy", first(attachment, "PolicyId"))
		if !ok {
			continue
		}
		summary, _ := policy["PolicySummary"].(map[string]any)
		if want := first(req.Input, "Filter"); want == "" || first(summary, "Type") == want {
			policies = append(policies, summary)
		}
	}
	return &spi.Response{Output: map[string]any{"Policies": policies}}, nil
}

func (p *Pack) listTargetsForPolicy(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	policyID := first(req.Input, "PolicyId")
	kvs, _, _ := p.col(req, "oattach").List(ctx, "", "", 0)
	targets := []any{}
	for _, kv := range kvs {
		var attachment map[string]any
		_ = json.Unmarshal(kv.Value, &attachment)
		if first(attachment, "PolicyId") == policyID {
			targets = append(targets, map[string]any{"TargetId": first(attachment, "TargetId")})
		}
	}
	return &spi.Response{Output: map[string]any{"Targets": targets}}, nil
}

func (p *Pack) listChildren(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	parent, typ := first(req.Input, "ParentId"), first(req.Input, "ChildType")
	collection, idType := "oacct", "ACCOUNT"
	if typ == "ORGANIZATIONAL_UNIT" {
		collection, idType = "oou", "ORGANIZATIONAL_UNIT"
	}
	kvs, _, _ := p.col(req, collection).List(ctx, "", "", 0)
	children := []any{}
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if first(rec, "ParentId") == parent {
			children = append(children, map[string]any{"Id": first(rec, "Id"), "Type": idType})
		}
	}
	return &spi.Response{Output: map[string]any{"Children": children}}, nil
}

func policyMissing() error {
	return &spi.Fault{Code: "PolicyNotFoundException", HTTPStatus: 400, Fault: "client"}
}
