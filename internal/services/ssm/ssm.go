// Package ssm is Parameter Store. SecureString is reversible local encoding, not encryption.
package ssm

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ssm", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements SSM Parameter Store.
type Pack struct{ deps spi.Deps }

func (p *Pack) ServiceID() string { return "aws.ssm" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"PutParameter", "GetParameter", "GetParameters", "GetParametersByPath",
		"DeleteParameter", "DeleteParameters", "DescribeParameters", "LabelParameterVersion",
		"GetParameterHistory", "AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource"}
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("ssm")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["Name"])
	switch req.Operation {
	case "PutParameter":
		val := str(req.Input["Value"])
		typ := str(req.Input["Type"])
		if typ == "SecureString" {
			val = base64.StdEncoding.EncodeToString([]byte(val))
		}
		b, _ := json.Marshal(map[string]any{"Name": name, "Value": val, "Type": typ, "Version": 1})
		_ = p.col(req).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Version": 1}}, nil
	case "GetParameter":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ParameterNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if str(m["Type"]) == "SecureString" {
			raw, _ := base64.StdEncoding.DecodeString(str(m["Value"]))
			m["Value"] = string(raw)
		}
		return &spi.Response{Output: map[string]any{"Parameter": m}}, nil
	case "GetParameters", "GetParametersByPath", "DescribeParameters":
		kvs, _, _ := p.col(req).List(ctx, str(req.Input["Path"]), "", 0)
		var ps []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			ps = append(ps, m)
		}
		return &spi.Response{Output: map[string]any{"Parameters": ps}}, nil
	case "DeleteParameter", "DeleteParameters":
		_ = p.col(req).Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return &spi.Response{Output: map[string]any{}}, nil
	}
}

func str(v any) string { s, _ := v.(string); return s }
