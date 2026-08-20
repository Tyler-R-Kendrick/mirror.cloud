// Package sts is the emulate-tier STS pack.
package sts

import (
	"context"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sts", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements STS.
type Pack struct{ deps spi.Deps }

func (p *Pack) ServiceID() string { return "aws.sts" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"GetCallerIdentity", "AssumeRole", "GetSessionToken", "GetFederationToken"}
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "GetCallerIdentity":
		return &spi.Response{Output: map[string]any{
			"Account": req.Identity.Account,
			"Arn":     req.Identity.ARN,
			"UserId":  req.Identity.AccessKeyID,
		}}, nil
	case "AssumeRole":
		role := str(req.Input["RoleArn"])
		sess := str(req.Input["RoleSessionName"])
		if sess == "" {
			sess = "session"
		}
		ak := p.deps.Rand.Derive(role + "/" + sess).Hex(20)
		arn := "arn:aws:sts::" + req.Identity.Account + ":assumed-role/" + roleName(role) + "/" + sess
		return &spi.Response{Output: map[string]any{
			"AssumedRoleUser": map[string]any{"AssumedRoleId": ak, "Arn": arn},
			"Credentials": map[string]any{
				"AccessKeyId": ak, "SecretAccessKey": p.deps.Rand.Derive(ak).Hex(40),
				"SessionToken": p.deps.Rand.Derive(ak + "tok").Hex(32),
				"Expiration":   p.deps.Clock.Now().Add(3600 * 1e9).UTC().Format("2006-01-02T15:04:05Z"),
			},
		}}, nil
	case "GetSessionToken", "GetFederationToken":
		ak := p.deps.Rand.Hex(20)
		return &spi.Response{Output: map[string]any{"Credentials": map[string]any{
			"AccessKeyId": ak, "SecretAccessKey": p.deps.Rand.Hex(40),
			"SessionToken": p.deps.Rand.Hex(32),
		}}}, nil
	default:
		return nil, spi.NotImplemented("aws.sts", req.Operation, "emulate")
	}
}

func str(v any) string { s, _ := v.(string); return s }

func roleName(arn string) string {
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
