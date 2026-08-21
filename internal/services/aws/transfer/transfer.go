// Package transfer stores Transfer Family servers and users (no real FTP/SFTP daemon).
package transfer

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.transfer", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Transfer-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.transfer" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateServer", "DescribeServer", "ListServers", "DeleteServer", "StartServer", "StopServer",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"ImportSshPublicKey", "DeleteSshPublicKey",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	sid := first(req.Input, "ServerId")
	switch req.Operation {
	case "CreateServer":
		id := "s-" + p.deps.Rand.Hex(8)
		rec := map[string]any{
			"ServerId": id, "State": "ONLINE", "Domain": first(req.Input, "Domain"),
			"EndpointType": first(req.Input, "EndpointType"), "Protocols": req.Input["Protocols"],
			"Arn": "arn:aws:transfer:" + req.Identity.Region + ":" + req.Identity.Account + ":server/" + id,
		}
		if rec["Domain"] == "" {
			rec["Domain"] = "S3"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "xfer").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ServerId": id}}, nil
	case "DescribeServer":
		b, ok, _ := p.col(req, "xfer").Get(ctx, sid)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Server": rec}}, nil
	case "ListServers":
		return listCol(ctx, p.col(req, "xfer"), "Servers")
	case "DeleteServer":
		_ = p.col(req, "xfer").Delete(ctx, sid)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartServer", "StopServer":
		b, ok, _ := p.col(req, "xfer").Get(ctx, sid)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if req.Operation == "StartServer" {
			rec["State"] = "ONLINE"
		} else {
			rec["State"] = "OFFLINE"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "xfer").Put(ctx, sid, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUser":
		user := first(req.Input, "UserName")
		rec := map[string]any{"UserName": user, "ServerId": sid, "Role": first(req.Input, "Role"), "HomeDirectory": first(req.Input, "HomeDirectory")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "xferu:"+sid).Put(ctx, user, b)
		return &spi.Response{Output: map[string]any{"ServerId": sid, "UserName": user}}, nil
	case "DescribeUser":
		user := first(req.Input, "UserName")
		b, ok, _ := p.col(req, "xferu:"+sid).Get(ctx, user)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ServerId": sid, "User": rec}}, nil
	case "ListUsers":
		return listCol(ctx, p.col(req, "xferu:"+sid), "Users")
	case "DeleteUser":
		_ = p.col(req, "xferu:"+sid).Delete(ctx, first(req.Input, "UserName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ImportSshPublicKey":
		user := first(req.Input, "UserName")
		kid := "key-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"SshPublicKeyId": kid, "SshPublicKeyBody": first(req.Input, "SshPublicKeyBody")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "xferk:"+sid+":"+user).Put(ctx, kid, b)
		return &spi.Response{Output: map[string]any{"ServerId": sid, "UserName": user, "SshPublicKeyId": kid}}, nil
	case "DeleteSshPublicKey":
		_ = p.col(req, "xferk:"+sid+":"+first(req.Input, "UserName")).Delete(ctx, first(req.Input, "SshPublicKeyId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.transfer", req.Operation, "emulate")
	}
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
