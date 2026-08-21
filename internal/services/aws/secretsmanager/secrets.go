// Package secretsmanager implements Secrets Manager with AWSCURRENT/AWSPREVIOUS staging.
package secretsmanager

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.secretsmanager", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements Secrets Manager.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.secretsmanager" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateSecret", "GetSecretValue", "PutSecretValue", "UpdateSecret", "DeleteSecret",
		"RestoreSecret", "ListSecrets", "DescribeSecret", "ListSecretVersionIds",
		"GetRandomPassword", "TagResource", "UntagResource",
		"BatchGetSecretValue", "PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"ValidateResourcePolicy", "ReplicateSecretToRegions", "RemoveRegionsFromReplication",
		"StopReplicationToReplica", "RotateSecret", "CancelRotateSecret", "UpdateSecretVersionStage"}
	return core
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("secrets")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "SecretId")
	arn := "arn:aws:secretsmanager:" + req.Identity.Region + ":" + req.Identity.Account + ":secret:" + name
	switch req.Operation {
	case "CreateSecret", "PutSecretValue", "UpdateSecret":
		val := first(req.Input, "SecretString", "SecretBinary")
		ver := p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "ARN": arn}
		if old, ok, _ := p.col(req).Get(ctx, name); ok {
			_ = json.Unmarshal(old, &rec)
		}
		vers := asSlice(rec["Versions"])
		for _, v := range vers {
			m := asMap(v)
			var stages []any
			hadCurrent := false
			for _, s := range asSlice(m["VersionStages"]) {
				if str(s) == "AWSCURRENT" {
					hadCurrent = true
					continue
				}
				if str(s) != "AWSPREVIOUS" {
					stages = append(stages, s)
				}
			}
			if hadCurrent {
				stages = append(stages, "AWSPREVIOUS")
			}
			m["VersionStages"] = stages
		}
		vers = append(vers, map[string]any{"VersionId": ver, "SecretString": val, "VersionStages": []any{"AWSCURRENT"}})
		rec["Versions"] = vers
		rec["VersionId"] = ver
		rec["SecretString"] = val
		rec["VersionStages"] = []any{"AWSCURRENT"}
		rec["Deleted"] = false
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ARN": arn, "Name": name, "VersionId": ver}}, nil
	case "GetSecretValue", "DescribeSecret":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if deleted, _ := m["Deleted"].(bool); deleted {
			return nil, &spi.Fault{Code: "InvalidRequestException", Message: "secret scheduled for deletion", HTTPStatus: 400, Fault: "client"}
		}
		picked := pickVer(m, first(req.Input, "VersionId"), first(req.Input, "VersionStage"))
		out := map[string]any{
			"ARN": m["ARN"], "Name": m["Name"],
			"SecretString": picked["SecretString"], "VersionId": picked["VersionId"],
			"VersionStages": picked["VersionStages"],
		}
		if req.Operation == "DescribeSecret" {
			out["VersionIdsToStages"] = stagesByID(m)
		}
		return &spi.Response{Output: out}, nil
	case "ListSecrets":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var ss []any
		for _, kv := range kvs {
			if len(kv.Key) > 5 && kv.Key[len(kv.Key)-5:] == ":tags" {
				continue
			}
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			ss = append(ss, map[string]any{"ARN": m["ARN"], "Name": m["Name"]})
		}
		return &spi.Response{Output: map[string]any{"SecretList": ss}}, nil
	case "DeleteSecret":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if truthy(req.Input["ForceDeleteWithoutRecovery"]) {
			_ = p.col(req).Delete(ctx, name)
			return &spi.Response{Output: map[string]any{"Name": name}}, nil
		}
		m["Deleted"] = true
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"Name": name, "DeletionDate": p.deps.Clock.Now().UTC().Format("2006-01-02T15:04:05Z")}}, nil
	case "RestoreSecret":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		delete(m, "Deleted")
		m["Deleted"] = false
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"Name": name}}, nil
	case "ListSecretVersionIds":
		b, ok, _ := p.col(req).Get(ctx, name)
		var vers []any
		if ok {
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			for _, v := range asSlice(m["Versions"]) {
				vm := asMap(v)
				vers = append(vers, map[string]any{"VersionId": vm["VersionId"], "VersionStages": vm["VersionStages"]})
			}
		}
		return &spi.Response{Output: map[string]any{"Versions": vers, "Name": name}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req).Put(ctx, name+":tags", b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req).Delete(ctx, name+":tags")
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetRandomPassword":
		return &spi.Response{Output: map[string]any{"RandomPassword": p.deps.Rand.Hex(32)}}, nil
	case "BatchGetSecretValue":
		ids := asSlice(req.Input["SecretIdList"])
		if len(ids) == 0 {
			if s := first(req.Input, "SecretId"); s != "" {
				ids = []any{s}
			}
		}
		var vals, errs []any
		for _, id := range ids {
			n := str(id)
			b, ok, _ := p.col(req).Get(ctx, n)
			if !ok {
				errs = append(errs, map[string]any{"SecretId": n, "ErrorCode": "ResourceNotFoundException"})
				continue
			}
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			picked := pickVer(m, "", "AWSCURRENT")
			vals = append(vals, map[string]any{"ARN": m["ARN"], "Name": m["Name"], "SecretString": picked["SecretString"], "VersionId": picked["VersionId"]})
		}
		return &spi.Response{Output: map[string]any{"SecretValues": vals, "Errors": errs}}, nil
	case "PutResourcePolicy":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		m["ResourcePolicy"] = first(req.Input, "ResourcePolicy")
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "Name": name}}, nil
	case "GetResourcePolicy":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "Name": name, "ResourcePolicy": m["ResourcePolicy"]}}, nil
	case "DeleteResourcePolicy":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		delete(m, "ResourcePolicy")
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "Name": name}}, nil
	case "ValidateResourcePolicy":
		raw := first(req.Input, "ResourcePolicy")
		var js any
		if raw == "" || json.Unmarshal([]byte(raw), &js) != nil {
			return nil, &spi.Fault{Code: "MalformedPolicyDocumentException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"ValidationErrors": []any{}}}, nil
	case "ReplicateSecretToRegions":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		reps := asSlice(m["ReplicaRegions"])
		for _, r := range asSlice(req.Input["AddReplicaRegions"]) {
			rm := asMap(r)
			if rg := first(rm, "Region"); rg != "" {
				reps = append(reps, map[string]any{"Region": rg, "Status": "InSync"})
			}
		}
		if rg := first(req.Input, "Region"); rg != "" {
			reps = append(reps, map[string]any{"Region": rg, "Status": "InSync"})
		}
		m["ReplicaRegions"] = reps
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "ReplicationStatus": reps}}, nil
	case "RemoveRegionsFromReplication":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		drop := map[string]bool{}
		for _, r := range asSlice(req.Input["RemoveReplicaRegions"]) {
			drop[str(r)] = true
		}
		var kept []any
		for _, r := range asSlice(m["ReplicaRegions"]) {
			if drop[str(asMap(r)["Region"])] {
				continue
			}
			kept = append(kept, r)
		}
		m["ReplicaRegions"] = kept
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "ReplicationStatus": kept}}, nil
	case "StopReplicationToReplica":
		return &spi.Response{Output: map[string]any{"ARN": arn}}, nil
	case "RotateSecret":
		req.Input["SecretString"] = first(req.Input, "SecretString")
		if req.Input["SecretString"] == "" {
			if old, ok, _ := p.col(req).Get(ctx, name); ok {
				var m map[string]any
				_ = json.Unmarshal(old, &m)
				req.Input["SecretString"] = pickVer(m, "", "AWSCURRENT")["SecretString"]
			}
		}
		req.Operation = "PutSecretValue"
		resp, err := p.Invoke(ctx, req)
		if err != nil {
			return nil, err
		}
		resp.Output["VersionId"] = resp.Output["VersionId"]
		return resp, nil
	case "CancelRotateSecret":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		m["RotationInProgress"] = false
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "Name": name, "VersionId": m["VersionId"]}}, nil
	case "UpdateSecretVersionStage":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		stage := first(req.Input, "VersionStage")
		moveTo := first(req.Input, "MoveToVersionId")
		removeFrom := first(req.Input, "RemoveFromVersionId")
		if stage == "" {
			stage = "AWSCURRENT"
		}
		for _, v := range asSlice(m["Versions"]) {
			vm := asMap(v)
			id := str(vm["VersionId"])
			var stages []any
			for _, s := range asSlice(vm["VersionStages"]) {
				if str(s) == stage && (id == removeFrom || (removeFrom == "" && moveTo != "" && id != moveTo)) {
					continue
				}
				stages = append(stages, s)
			}
			if id == moveTo {
				stages = append(stages, stage)
			}
			vm["VersionStages"] = stages
		}
		nb, _ := json.Marshal(m)
		_ = p.col(req).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ARN": m["ARN"], "Name": name}}, nil
	default:
		return nil, spi.NotImplemented("aws.secretsmanager", req.Operation, "emulate")
	}
}

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "True" || t == "1"
	}
	return false
}

func pickVer(rec map[string]any, versionID, stage string) map[string]any {
	if stage == "" && versionID == "" {
		stage = "AWSCURRENT"
	}
	for _, v := range asSlice(rec["Versions"]) {
		m := asMap(v)
		if versionID != "" && str(m["VersionId"]) == versionID {
			return m
		}
		if versionID == "" {
			for _, s := range asSlice(m["VersionStages"]) {
				if str(s) == stage {
					return m
				}
			}
		}
	}
	return rec
}

func stagesByID(rec map[string]any) map[string]any {
	out := map[string]any{}
	for _, v := range asSlice(rec["Versions"]) {
		m := asMap(v)
		out[str(m["VersionId"])] = m["VersionStages"]
	}
	return out
}
