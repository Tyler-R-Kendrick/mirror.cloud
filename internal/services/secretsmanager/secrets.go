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

func (p *Pack) ServiceID() string { return "aws.secretsmanager" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateSecret", "GetSecretValue", "PutSecretValue", "UpdateSecret", "DeleteSecret",
		"RestoreSecret", "ListSecrets", "DescribeSecret", "ListSecretVersionIds",
		"GetRandomPassword", "TagResource", "UntagResource"}
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("secrets")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "SecretId")
	switch req.Operation {
	case "CreateSecret", "PutSecretValue", "UpdateSecret":
		val := first(req.Input, "SecretString", "SecretBinary")
		ver := p.deps.Rand.Hex(8)
		b, _ := json.Marshal(map[string]any{"Name": name, "SecretString": val, "VersionId": ver, "VersionStages": []any{"AWSCURRENT"}})
		_ = p.col(req).Put(ctx, name, b)
		arn := "arn:aws:secretsmanager:" + req.Identity.Region + ":" + req.Identity.Account + ":secret:" + name
		return &spi.Response{Output: map[string]any{"ARN": arn, "Name": name, "VersionId": ver}}, nil
	case "GetSecretValue", "DescribeSecret":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: m}, nil
	case "ListSecrets":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var ss []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			ss = append(ss, m)
		}
		return &spi.Response{Output: map[string]any{"SecretList": ss}}, nil
	case "GetRandomPassword":
		return &spi.Response{Output: map[string]any{"RandomPassword": p.deps.Rand.Hex(32)}}, nil
	default:
		return &spi.Response{Output: map[string]any{}}, nil
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
