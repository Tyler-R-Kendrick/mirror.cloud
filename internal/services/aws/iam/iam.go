// Package iam stores roles and policies and evaluates them via Authorizer.
package iam

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.iam", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements IAM-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.iam" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateRole", "GetRole", "UpdateRole", "DeleteRole", "ListRoles", "UpdateAssumeRolePolicy",
		"PutRolePolicy", "GetRolePolicy", "DeleteRolePolicy", "ListRolePolicies",
		"AttachRolePolicy", "DetachRolePolicy", "ListAttachedRolePolicies",
		"CreatePolicy", "GetPolicy", "DeletePolicy", "ListPolicies",
		"CreatePolicyVersion", "GetPolicyVersion", "DeletePolicyVersion", "ListPolicyVersions", "SetDefaultPolicyVersion",
		"CreateUser", "GetUser", "UpdateUser", "DeleteUser", "ListUsers",
		"PutUserPolicy", "GetUserPolicy", "DeleteUserPolicy", "ListUserPolicies",
		"AttachUserPolicy", "DetachUserPolicy", "ListAttachedUserPolicies",
		"CreateAccessKey", "ListAccessKeys", "UpdateAccessKey", "DeleteAccessKey",
		"CreateLoginProfile", "GetLoginProfile", "UpdateLoginProfile", "DeleteLoginProfile",
		"CreateGroup", "GetGroup", "UpdateGroup", "DeleteGroup", "ListGroups",
		"AddUserToGroup", "RemoveUserFromGroup", "ListGroupsForUser",
		"PutGroupPolicy", "GetGroupPolicy", "DeleteGroupPolicy", "ListGroupPolicies",
		"AttachGroupPolicy", "DetachGroupPolicy", "ListAttachedGroupPolicies",
		"CreateInstanceProfile", "GetInstanceProfile", "DeleteInstanceProfile", "ListInstanceProfiles",
		"AddRoleToInstanceProfile", "RemoveRoleFromInstanceProfile", "ListInstanceProfilesForRole",
		"TagRole", "UntagRole", "ListRoleTags", "TagUser", "UntagUser", "ListUserTags",
		"CreateAccountAlias", "ListAccountAliases", "DeleteAccountAlias",
		"GetAccountSummary", "GetAccountPasswordPolicy", "UpdateAccountPasswordPolicy", "DeleteAccountPasswordPolicy",
		"CreateOpenIDConnectProvider", "GetOpenIDConnectProvider", "DeleteOpenIDConnectProvider", "ListOpenIDConnectProviders", "UpdateOpenIDConnectProviderThumbprint",
		"CreateSAMLProvider", "GetSAMLProvider", "DeleteSAMLProvider", "ListSAMLProviders", "UpdateSAMLProvider",
		"SimulatePrincipalPolicy", "SimulateCustomPolicy",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("iam")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	acct := req.Identity.Account
	switch req.Operation {
	case "CreateRole", "UpdateRole":
		name := first(req.Input, "RoleName")
		rec := map[string]any{"RoleName": name, "Arn": "arn:aws:iam::" + acct + ":role/" + name, "AssumeRolePolicyDocument": req.Input["AssumeRolePolicyDocument"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "role:"+name, b)
		return &spi.Response{Output: map[string]any{"Role": rec}}, nil
	case "GetRole":
		name := first(req.Input, "RoleName")
		b, ok, _ := p.col(req).Get(ctx, "role:"+name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Role": rec}}, nil
	case "DeleteRole":
		_ = p.col(req).Delete(ctx, "role:"+first(req.Input, "RoleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRoles":
		return p.listKind(ctx, req, "role:", "Roles")
	case "CreateUser":
		name := first(req.Input, "UserName")
		rec := map[string]any{"UserName": name, "Arn": "arn:aws:iam::" + acct + ":user/" + name}
		p.put(ctx, req, "user:"+name, rec)
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "GetUser":
		rec, ok := p.get(ctx, req, "user:"+first(req.Input, "UserName"))
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "UpdateUser":
		old := first(req.Input, "UserName")
		rec, ok := p.get(ctx, req, "user:"+old)
		if !ok {
			return nil, missing()
		}
		if neu := first(req.Input, "NewUserName"); neu != "" {
			_ = p.col(req).Delete(ctx, "user:"+old)
			rec["UserName"] = neu
			rec["Arn"] = "arn:aws:iam::" + acct + ":user/" + neu
			p.put(ctx, req, "user:"+neu, rec)
		} else {
			p.put(ctx, req, "user:"+old, rec)
		}
		return &spi.Response{Output: map[string]any{"User": rec}}, nil
	case "DeleteUser":
		_ = p.col(req).Delete(ctx, "user:"+first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListUsers":
		return p.listKind(ctx, req, "user:", "Users")
	case "CreatePolicy":
		name := first(req.Input, "PolicyName")
		doc := req.Input["PolicyDocument"]
		rec := map[string]any{"PolicyName": name, "Arn": "arn:aws:iam::" + acct + ":policy/" + name, "PolicyDocument": doc, "DefaultVersionId": "v1"}
		p.put(ctx, req, "policy:"+name, rec)
		p.put(ctx, req, "polver:"+name+":v1", map[string]any{"VersionId": "v1", "Document": doc, "IsDefaultVersion": true})
		p.put(ctx, req, "polverseq:"+name, map[string]any{"n": 1})
		return &spi.Response{Output: map[string]any{"Policy": rec}}, nil
	case "GetPolicy":
		name := policyName(req.Input)
		b, ok, _ := p.col(req).Get(ctx, "policy:"+name)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Policy": rec}}, nil
	case "DeletePolicy":
		_ = p.col(req).Delete(ctx, "policy:"+policyName(req.Input))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPolicies":
		return p.listKind(ctx, req, "policy:", "Policies")
	case "PutRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req).Put(ctx, "rolepolicy:"+role+":"+pol, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		b, ok, _ := p.col(req).Get(ctx, "rolepolicy:"+role+":"+pol)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteRolePolicy":
		role, pol := first(req.Input, "RoleName"), first(req.Input, "PolicyName")
		_ = p.col(req).Delete(ctx, "rolepolicy:"+role+":"+pol)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRolePolicies":
		role := first(req.Input, "RoleName")
		kvs, _, _ := p.col(req).List(ctx, "rolepolicy:"+role+":", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key[len("rolepolicy:"+role+":"):])
		}
		return &spi.Response{Output: map[string]any{"PolicyNames": names}}, nil
	case "AttachRolePolicy":
		role, arn := first(req.Input, "RoleName"), first(req.Input, "PolicyArn")
		b, _ := json.Marshal(map[string]any{"PolicyArn": arn, "RoleName": role})
		_ = p.col(req).Put(ctx, "attached:"+role+":"+arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DetachRolePolicy":
		role, arn := first(req.Input, "RoleName"), first(req.Input, "PolicyArn")
		_ = p.col(req).Delete(ctx, "attached:"+role+":"+arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAttachedRolePolicies":
		role := first(req.Input, "RoleName")
		kvs, _, _ := p.col(req).List(ctx, "attached:"+role+":", "", 0)
		var ps []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			ps = append(ps, rec)
		}
		return &spi.Response{Output: map[string]any{"AttachedPolicies": ps}}, nil
	case "TagRole":
		p.put(ctx, req, "roletags:"+first(req.Input, "RoleName"), collectTags(req.Input))
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagRole":
		_ = p.col(req).Delete(ctx, "roletags:"+first(req.Input, "RoleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListRoleTags":
		tags, _ := p.getList(ctx, req, "roletags:"+first(req.Input, "RoleName"))
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "TagUser":
		p.put(ctx, req, "usertags:"+first(req.Input, "UserName"), collectTags(req.Input))
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagUser":
		_ = p.col(req).Delete(ctx, "usertags:"+first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListUserTags":
		tags, _ := p.getList(ctx, req, "usertags:"+first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "UpdateAssumeRolePolicy":
		name := first(req.Input, "RoleName")
		rec, ok := p.get(ctx, req, "role:"+name)
		if !ok {
			return nil, missing()
		}
		rec["AssumeRolePolicyDocument"] = first(req.Input, "PolicyDocument", "AssumeRolePolicyDocument")
		p.put(ctx, req, "role:"+name, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePolicyVersion":
		return p.createPolicyVersion(ctx, req)
	case "GetPolicyVersion":
		name, vid := policyName(req.Input), first(req.Input, "VersionId")
		rec, ok := p.get(ctx, req, "polver:"+name+":"+vid)
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: map[string]any{"PolicyVersion": rec}}, nil
	case "DeletePolicyVersion":
		name, vid := policyName(req.Input), first(req.Input, "VersionId")
		pol, _ := p.get(ctx, req, "policy:"+name)
		if str(pol["DefaultVersionId"]) == vid {
			return nil, &spi.Fault{Code: "DeleteConflict", Message: "cannot delete default version", HTTPStatus: 409, Fault: "client"}
		}
		_ = p.col(req).Delete(ctx, "polver:"+name+":"+vid)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPolicyVersions":
		name := policyName(req.Input)
		kvs, _, _ := p.col(req).List(ctx, "polver:"+name+":", "", 0)
		var vers []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			vers = append(vers, rec)
		}
		return &spi.Response{Output: map[string]any{"Versions": vers}}, nil
	case "SetDefaultPolicyVersion":
		name, vid := policyName(req.Input), first(req.Input, "VersionId")
		pol, ok := p.get(ctx, req, "policy:"+name)
		if !ok {
			return nil, missing()
		}
		pol["DefaultVersionId"] = vid
		p.put(ctx, req, "policy:"+name, pol)
		kvs, _, _ := p.col(req).List(ctx, "polver:"+name+":", "", 0)
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			rec["IsDefaultVersion"] = str(rec["VersionId"]) == vid
			p.put(ctx, req, kv.Key, rec)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutUserPolicy", "GetUserPolicy", "DeleteUserPolicy", "ListUserPolicies":
		return p.inlinePolicy(ctx, req, "userpolicy:", first(req.Input, "UserName"), "UserName")
	case "AttachUserPolicy":
		user, arn := first(req.Input, "UserName"), first(req.Input, "PolicyArn")
		p.put(ctx, req, "uattached:"+user+":"+arn, map[string]any{"PolicyArn": arn, "UserName": user})
		return &spi.Response{Output: map[string]any{}}, nil
	case "DetachUserPolicy":
		_ = p.col(req).Delete(ctx, "uattached:"+first(req.Input, "UserName")+":"+first(req.Input, "PolicyArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAttachedUserPolicies":
		return p.listKind(ctx, req, "uattached:"+first(req.Input, "UserName")+":", "AttachedPolicies")
	case "CreateLoginProfile":
		user := first(req.Input, "UserName")
		rec := map[string]any{"UserName": user, "PasswordResetRequired": req.Input["PasswordResetRequired"]}
		p.put(ctx, req, "login:"+user, rec)
		return &spi.Response{Output: map[string]any{"LoginProfile": rec}}, nil
	case "GetLoginProfile":
		rec, ok := p.get(ctx, req, "login:"+first(req.Input, "UserName"))
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: map[string]any{"LoginProfile": rec}}, nil
	case "UpdateLoginProfile":
		user := first(req.Input, "UserName")
		rec, ok := p.get(ctx, req, "login:"+user)
		if !ok {
			rec = map[string]any{"UserName": user}
		}
		if v, ok := req.Input["PasswordResetRequired"]; ok {
			rec["PasswordResetRequired"] = v
		}
		p.put(ctx, req, "login:"+user, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteLoginProfile":
		_ = p.col(req).Delete(ctx, "login:"+first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateGroup":
		name := first(req.Input, "GroupName")
		rec := map[string]any{"GroupName": name, "Arn": "arn:aws:iam::" + acct + ":group/" + name, "Path": first(req.Input, "Path")}
		if rec["Path"] == "" {
			rec["Path"] = "/"
		}
		p.put(ctx, req, "group:"+name, rec)
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "GetGroup":
		name := first(req.Input, "GroupName")
		rec, ok := p.get(ctx, req, "group:"+name)
		if !ok {
			return nil, missing()
		}
		kvs, _, _ := p.col(req).List(ctx, "gm:"+name+":", "", 0)
		var users []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if u, ok := p.get(ctx, req, "user:"+str(m["UserName"])); ok {
				users = append(users, u)
			} else {
				users = append(users, map[string]any{"UserName": str(m["UserName"])})
			}
		}
		return &spi.Response{Output: map[string]any{"Group": rec, "Users": users}}, nil
	case "UpdateGroup":
		old := first(req.Input, "GroupName")
		rec, ok := p.get(ctx, req, "group:"+old)
		if !ok {
			return nil, missing()
		}
		if neu := first(req.Input, "NewGroupName"); neu != "" {
			_ = p.col(req).Delete(ctx, "group:"+old)
			rec["GroupName"] = neu
			rec["Arn"] = "arn:aws:iam::" + acct + ":group/" + neu
			p.put(ctx, req, "group:"+neu, rec)
		} else {
			p.put(ctx, req, "group:"+old, rec)
		}
		return &spi.Response{Output: map[string]any{"Group": rec}}, nil
	case "DeleteGroup":
		_ = p.col(req).Delete(ctx, "group:"+first(req.Input, "GroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListGroups":
		return p.listKind(ctx, req, "group:", "Groups")
	case "AddUserToGroup":
		g, u := first(req.Input, "GroupName"), first(req.Input, "UserName")
		p.put(ctx, req, "gm:"+g+":"+u, map[string]any{"GroupName": g, "UserName": u})
		p.put(ctx, req, "ug:"+u+":"+g, map[string]any{"GroupName": g, "UserName": u})
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemoveUserFromGroup":
		g, u := first(req.Input, "GroupName"), first(req.Input, "UserName")
		_ = p.col(req).Delete(ctx, "gm:"+g+":"+u)
		_ = p.col(req).Delete(ctx, "ug:"+u+":"+g)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListGroupsForUser":
		return p.listKind(ctx, req, "ug:"+first(req.Input, "UserName")+":", "Groups")
	case "PutGroupPolicy", "GetGroupPolicy", "DeleteGroupPolicy", "ListGroupPolicies":
		return p.inlinePolicy(ctx, req, "grouppolicy:", first(req.Input, "GroupName"), "GroupName")
	case "AttachGroupPolicy":
		g, arn := first(req.Input, "GroupName"), first(req.Input, "PolicyArn")
		p.put(ctx, req, "gattached:"+g+":"+arn, map[string]any{"PolicyArn": arn, "GroupName": g})
		return &spi.Response{Output: map[string]any{}}, nil
	case "DetachGroupPolicy":
		_ = p.col(req).Delete(ctx, "gattached:"+first(req.Input, "GroupName")+":"+first(req.Input, "PolicyArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAttachedGroupPolicies":
		return p.listKind(ctx, req, "gattached:"+first(req.Input, "GroupName")+":", "AttachedPolicies")
	case "CreateInstanceProfile":
		name := first(req.Input, "InstanceProfileName")
		rec := map[string]any{"InstanceProfileName": name, "Arn": "arn:aws:iam::" + acct + ":instance-profile/" + name, "Roles": []any{}}
		p.put(ctx, req, "ip:"+name, rec)
		return &spi.Response{Output: map[string]any{"InstanceProfile": rec}}, nil
	case "GetInstanceProfile":
		rec, ok := p.get(ctx, req, "ip:"+first(req.Input, "InstanceProfileName"))
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: map[string]any{"InstanceProfile": rec}}, nil
	case "DeleteInstanceProfile":
		_ = p.col(req).Delete(ctx, "ip:"+first(req.Input, "InstanceProfileName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListInstanceProfiles":
		return p.listKind(ctx, req, "ip:", "InstanceProfiles")
	case "AddRoleToInstanceProfile":
		name, role := first(req.Input, "InstanceProfileName"), first(req.Input, "RoleName")
		rec, ok := p.get(ctx, req, "ip:"+name)
		if !ok {
			return nil, missing()
		}
		rr, _ := p.get(ctx, req, "role:"+role)
		if rr == nil {
			rr = map[string]any{"RoleName": role, "Arn": "arn:aws:iam::" + acct + ":role/" + role}
		}
		roles, _ := rec["Roles"].([]any)
		roles = append(roles, rr)
		rec["Roles"] = roles
		p.put(ctx, req, "ip:"+name, rec)
		p.put(ctx, req, "iprole:"+role+":"+name, map[string]any{"InstanceProfileName": name})
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemoveRoleFromInstanceProfile":
		name, role := first(req.Input, "InstanceProfileName"), first(req.Input, "RoleName")
		rec, ok := p.get(ctx, req, "ip:"+name)
		if ok {
			var keep []any
			for _, r := range asSlice(rec["Roles"]) {
				m, _ := r.(map[string]any)
				if str(m["RoleName"]) != role {
					keep = append(keep, r)
				}
			}
			rec["Roles"] = keep
			p.put(ctx, req, "ip:"+name, rec)
		}
		_ = p.col(req).Delete(ctx, "iprole:"+role+":"+name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListInstanceProfilesForRole":
		return p.listKind(ctx, req, "iprole:"+first(req.Input, "RoleName")+":", "InstanceProfiles")
	case "CreateAccountAlias":
		alias := first(req.Input, "AccountAlias")
		p.put(ctx, req, "alias:"+alias, map[string]any{"AccountAlias": alias})
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAccountAliases":
		return p.listKind(ctx, req, "alias:", "AccountAliases")
	case "DeleteAccountAlias":
		_ = p.col(req).Delete(ctx, "alias:"+first(req.Input, "AccountAlias"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetAccountSummary":
		return p.accountSummary(ctx, req)
	case "UpdateAccountPasswordPolicy":
		p.put(ctx, req, "pwpolicy", req.Input)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetAccountPasswordPolicy":
		rec, ok := p.get(ctx, req, "pwpolicy")
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: map[string]any{"PasswordPolicy": rec}}, nil
	case "DeleteAccountPasswordPolicy":
		_ = p.col(req).Delete(ctx, "pwpolicy")
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateOpenIDConnectProvider":
		url := first(req.Input, "Url", "URL")
		arn := "arn:aws:iam::" + acct + ":oidc-provider/" + strings.TrimPrefix(strings.TrimPrefix(url, "https://"), "http://")
		rec := map[string]any{"Url": url, "Arn": arn, "ClientIDList": req.Input["ClientIDList"], "ThumbprintList": req.Input["ThumbprintList"]}
		p.put(ctx, req, "oidc:"+arn, rec)
		return &spi.Response{Output: map[string]any{"OpenIDConnectProviderArn": arn}}, nil
	case "GetOpenIDConnectProvider":
		arn := first(req.Input, "OpenIDConnectProviderArn")
		rec, ok := p.get(ctx, req, "oidc:"+arn)
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: rec}, nil
	case "DeleteOpenIDConnectProvider":
		_ = p.col(req).Delete(ctx, "oidc:"+first(req.Input, "OpenIDConnectProviderArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListOpenIDConnectProviders":
		kvs, _, _ := p.col(req).List(ctx, "oidc:", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, map[string]any{"Arn": rec["Arn"]})
		}
		return &spi.Response{Output: map[string]any{"OpenIDConnectProviderList": items}}, nil
	case "UpdateOpenIDConnectProviderThumbprint":
		arn := first(req.Input, "OpenIDConnectProviderArn")
		rec, ok := p.get(ctx, req, "oidc:"+arn)
		if !ok {
			return nil, missing()
		}
		rec["ThumbprintList"] = req.Input["ThumbprintList"]
		p.put(ctx, req, "oidc:"+arn, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateSAMLProvider":
		name := first(req.Input, "Name")
		arn := "arn:aws:iam::" + acct + ":saml-provider/" + name
		rec := map[string]any{"Name": name, "Arn": arn, "SAMLMetadataDocument": first(req.Input, "SAMLMetadataDocument")}
		p.put(ctx, req, "saml:"+name, rec)
		return &spi.Response{Output: map[string]any{"SAMLProviderArn": arn}}, nil
	case "GetSAMLProvider":
		name := samlName(first(req.Input, "SAMLProviderArn", "Name"))
		rec, ok := p.get(ctx, req, "saml:"+name)
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: rec}, nil
	case "DeleteSAMLProvider":
		_ = p.col(req).Delete(ctx, "saml:"+samlName(first(req.Input, "SAMLProviderArn", "Name")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListSAMLProviders":
		return p.listKind(ctx, req, "saml:", "SAMLProviderList")
	case "UpdateSAMLProvider":
		name := samlName(first(req.Input, "SAMLProviderArn", "Name"))
		rec, ok := p.get(ctx, req, "saml:"+name)
		if !ok {
			return nil, missing()
		}
		if d := first(req.Input, "SAMLMetadataDocument"); d != "" {
			rec["SAMLMetadataDocument"] = d
		}
		p.put(ctx, req, "saml:"+name, rec)
		return &spi.Response{Output: map[string]any{"SAMLProviderArn": rec["Arn"]}}, nil
	case "SimulatePrincipalPolicy":
		source := first(req.Input, "PolicySourceArn", "RoleName")
		kind, name := "role", roleNameFrom(source)
		if strings.Contains(source, ":user/") {
			kind, name = "user", userFromARN(source)
		}
		docs := loadPrincipalDocs(ctx, p.col(req), kind, name)
		return p.simulate(ctx, docs, req, source)
	case "SimulateCustomPolicy":
		var docs []map[string]any
		for _, raw := range collectMembers(req.Input, "PolicyInputList") {
			docs = append(docs, parseDoc(raw))
		}
		return p.simulate(ctx, docs, req, "")
	case "CreateAccessKey":
		user := first(req.Input, "UserName")
		ak := p.deps.Rand.Derive("ak:" + user + ":" + strconv.Itoa(p.countPrefix(ctx, req, "ak:"+user+":"))).Hex(20)
		rec := map[string]any{
			"AccessKeyId": ak, "SecretAccessKey": p.deps.Rand.Derive(ak).Hex(40),
			"UserName": user, "Status": "Active",
		}
		p.put(ctx, req, "ak:"+user+":"+ak, rec)
		return &spi.Response{Output: map[string]any{"AccessKey": rec}}, nil
	case "ListAccessKeys":
		user := first(req.Input, "UserName")
		kvs, _, _ := p.col(req).List(ctx, "ak:"+user+":", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			delete(rec, "SecretAccessKey")
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"AccessKeyMetadata": items}}, nil
	case "UpdateAccessKey":
		user, id := first(req.Input, "UserName"), first(req.Input, "AccessKeyId")
		rec, ok := p.get(ctx, req, "ak:"+user+":"+id)
		if !ok {
			return nil, missing()
		}
		if st := first(req.Input, "Status"); st != "" {
			rec["Status"] = st
		}
		p.put(ctx, req, "ak:"+user+":"+id, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteAccessKey":
		_ = p.col(req).Delete(ctx, "ak:"+first(req.Input, "UserName")+":"+first(req.Input, "AccessKeyId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) listKind(ctx context.Context, req *spi.Request, prefix, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req).List(ctx, prefix, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		if skipIAMKey(prefix, kv.Key) {
			continue
		}
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func (p *Pack) put(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req).Put(ctx, key, b)
}

func (p *Pack) get(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req).Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) getList(ctx context.Context, req *spi.Request, key string) ([]any, bool) {
	b, ok, _ := p.col(req).Get(ctx, key)
	if !ok {
		return []any{}, false
	}
	var rec []any
	if err := json.Unmarshal(b, &rec); err != nil {
		return []any{}, false
	}
	return rec, true
}

func missing() error {
	return &spi.Fault{Code: "NoSuchEntity", HTTPStatus: 404, Fault: "client"}
}

func (p *Pack) inlinePolicy(ctx context.Context, req *spi.Request, pfx, owner, _ string) (*spi.Response, error) {
	pol := first(req.Input, "PolicyName")
	key := pfx + owner + ":" + pol
	switch req.Operation {
	case "PutUserPolicy", "PutGroupPolicy":
		p.put(ctx, req, key, req.Input)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetUserPolicy", "GetGroupPolicy":
		rec, ok := p.get(ctx, req, key)
		if !ok {
			return nil, missing()
		}
		return &spi.Response{Output: rec}, nil
	case "DeleteUserPolicy", "DeleteGroupPolicy":
		_ = p.col(req).Delete(ctx, key)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListUserPolicies", "ListGroupPolicies":
		kvs, _, _ := p.col(req).List(ctx, pfx+owner+":", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key[len(pfx+owner+":"):])
		}
		return &spi.Response{Output: map[string]any{"PolicyNames": names}}, nil
	}
	return nil, spi.NotImplemented("aws.iam", req.Operation, "emulate")
}

func (p *Pack) createPolicyVersion(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := policyName(req.Input)
	pol, ok := p.get(ctx, req, "policy:"+name)
	if !ok {
		return nil, missing()
	}
	seq, _ := p.get(ctx, req, "polverseq:"+name)
	n := 1
	switch t := seq["n"].(type) {
	case float64:
		n = int(t)
	case int:
		n = t
	}
	n++
	vid := "v" + strconv.Itoa(n)
	p.put(ctx, req, "polverseq:"+name, map[string]any{"n": n})
	rec := map[string]any{"VersionId": vid, "Document": req.Input["PolicyDocument"], "IsDefaultVersion": false}
	if truthy(req.Input["SetAsDefault"]) {
		rec["IsDefaultVersion"] = true
		pol["DefaultVersionId"] = vid
		p.put(ctx, req, "policy:"+name, pol)
	}
	p.put(ctx, req, "polver:"+name+":"+vid, rec)
	return &spi.Response{Output: map[string]any{"PolicyVersion": rec}}, nil
}

func (p *Pack) accountSummary(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{"SummaryMap": map[string]any{
		"Users":            p.countPrefix(ctx, req, "user:"),
		"Roles":            p.countPrefix(ctx, req, "role:"),
		"Groups":           p.countPrefix(ctx, req, "group:"),
		"Policies":         p.countPrefix(ctx, req, "policy:"),
		"InstanceProfiles": p.countPrefix(ctx, req, "ip:"),
	}}}, nil
}

func (p *Pack) countPrefix(ctx context.Context, req *spi.Request, pfx string) int {
	kvs, _, _ := p.col(req).List(ctx, pfx, "", 0)
	n := 0
	for _, kv := range kvs {
		if skipIAMKey(pfx, kv.Key) {
			continue
		}
		n++
	}
	return n
}

func skipIAMKey(prefix, key string) bool {
	switch prefix {
	case "role:":
		return strings.HasPrefix(key, "rolepolicy:") || strings.HasPrefix(key, "roletags:")
	case "user:":
		return strings.HasPrefix(key, "userpolicy:") || strings.HasPrefix(key, "usertags:")
	case "group:":
		return strings.HasPrefix(key, "grouppolicy:")
	}
	return false
}

func collectTags(in map[string]any) []any {
	if v, ok := in["Tags"]; ok {
		if s, ok := v.([]any); ok {
			return s
		}
	}
	keys, vals := map[string]string{}, map[string]string{}
	for k, v := range in {
		if !strings.HasPrefix(k, "Tags.member.") {
			continue
		}
		s, _ := v.(string)
		if strings.HasSuffix(k, ".Key") {
			keys[strings.TrimSuffix(k, ".Key")] = s
		}
		if strings.HasSuffix(k, ".Value") {
			vals[strings.TrimSuffix(k, ".Value")] = s
		}
	}
	var pfxs []string
	for p := range keys {
		pfxs = append(pfxs, p)
	}
	sort.Strings(pfxs)
	out := make([]any, 0, len(pfxs))
	for _, p := range pfxs {
		out = append(out, map[string]any{"Key": keys[p], "Value": vals[p]})
	}
	return out
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "True"
	}
	return false
}

func samlName(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func (p *Pack) simulate(ctx context.Context, docs []map[string]any, req *spi.Request, source string) (*spi.Response, error) {
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

func policyName(in map[string]any) string {
	if n := first(in, "PolicyName"); n != "" {
		return n
	}
	arn := first(in, "PolicyArn")
	if i := lastSlash(arn); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
