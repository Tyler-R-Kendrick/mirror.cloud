// Package sts is the emulate-tier STS pack.
package sts

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"time"

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

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sts" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"GetCallerIdentity", "AssumeRole", "GetSessionToken", "GetFederationToken",
		"AssumeRoleWithSAML", "AssumeRoleWithWebIdentity", "AssumeRoot",
		"DecodeAuthorizationMessage", "GetAccessKeyInfo", "GetDelegatedAccessToken", "GetWebIdentityToken",
	}
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
		sess := str(req.Input["RoleSessionName"])
		if sess == "" {
			sess = "session"
		}
		return p.assume(ctx, req, str(req.Input["RoleArn"]), sess)
	case "GetSessionToken":
		return p.creds(req, "session/"+req.Identity.Account, "", ""), nil
	case "GetFederationToken":
		name := str(req.Input["Name"])
		if name == "" {
			name = "federated"
		}
		arn := "arn:aws:sts::" + req.Identity.Account + ":federated-user/" + name
		return p.creds(req, "fed/"+name, "FederatedUser", arn), nil
	case "AssumeRoleWithSAML":
		if str(req.Input["SAMLAssertion"]) == "" || str(req.Input["RoleArn"]) == "" {
			return nil, &spi.Fault{Code: "MissingParameter", Message: "RoleArn and SAMLAssertion", HTTPStatus: 400, Fault: "client"}
		}
		return p.assume(ctx, req, str(req.Input["RoleArn"]), "saml")
	case "AssumeRoleWithWebIdentity":
		if str(req.Input["WebIdentityToken"]) == "" || str(req.Input["RoleArn"]) == "" {
			return nil, &spi.Fault{Code: "MissingParameter", Message: "RoleArn and WebIdentityToken", HTTPStatus: 400, Fault: "client"}
		}
		return p.assume(ctx, req, str(req.Input["RoleArn"]), "web")
	case "AssumeRoot":
		return p.creds(req, "root/"+req.Identity.Account, "", ""), nil
	case "DecodeAuthorizationMessage":
		raw := str(req.Input["EncodedMessage"])
		if raw == "" {
			return nil, &spi.Fault{Code: "MissingParameter", Message: "EncodedMessage", HTTPStatus: 400, Fault: "client"}
		}
		msg := decodeAuthz(raw)
		return &spi.Response{Output: map[string]any{"DecodedMessage": msg}}, nil
	case "GetAccessKeyInfo":
		ak := str(req.Input["AccessKeyId"])
		if ak == "" {
			return nil, &spi.Fault{Code: "MissingParameter", Message: "AccessKeyId", HTTPStatus: 400, Fault: "client"}
		}
		acct := req.Identity.Account
		if b, ok, _ := p.col(req, "stsk").Get(ctx, ak); ok {
			acct = string(b)
		}
		return &spi.Response{Output: map[string]any{"Account": acct}}, nil
	case "GetDelegatedAccessToken":
		tok := p.deps.Rand.Derive("delegated/" + req.Identity.Account).Hex(32)
		return &spi.Response{Output: map[string]any{"Credentials": map[string]any{
			"AccessKeyId": tok[:20], "SecretAccessKey": p.deps.Rand.Derive(tok).Hex(40),
			"SessionToken": tok, "Expiration": p.deps.Clock.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}}}, nil
	case "GetWebIdentityToken":
		tok := p.deps.Rand.Derive("wit/" + req.Identity.Account).Hex(48)
		return &spi.Response{Output: map[string]any{
			"WebIdentityToken": tok,
			"Expiration":       p.deps.Clock.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		}}, nil
	default:
		return nil, spi.NotImplemented("aws.sts", req.Operation, "emulate")
	}
}

func (p *Pack) col(_ *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope("_mirror", "global").Collection(n)
}

func (p *Pack) assume(ctx context.Context, req *spi.Request, role, sess string) (*spi.Response, error) {
	ak := p.deps.Rand.Derive(role + "/" + sess).Hex(20)
	_ = p.col(req, "stsk").Put(ctx, ak, []byte(req.Identity.Account))
	arn := "arn:aws:sts::" + req.Identity.Account + ":assumed-role/" + roleName(role) + "/" + sess
	return &spi.Response{Output: map[string]any{
		"AssumedRoleUser": map[string]any{"AssumedRoleId": ak, "Arn": arn},
		"Credentials": map[string]any{
			"AccessKeyId": ak, "SecretAccessKey": p.deps.Rand.Derive(ak).Hex(40),
			"SessionToken": p.deps.Rand.Derive(ak + "tok").Hex(32),
			"Expiration":   p.deps.Clock.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		},
	}}, nil
}

func decodeAuthz(raw string) string {
	if b, err := hex.DecodeString(raw); err == nil {
		return string(b)
	}
	if b, err := base64.StdEncoding.DecodeString(raw); err == nil {
		var js any
		if json.Unmarshal(b, &js) == nil || len(b) > 0 {
			return string(b)
		}
	}
	return raw
}

func (p *Pack) creds(req *spi.Request, seed, userKey, arn string) *spi.Response {
	ak := p.deps.Rand.Derive(seed).Hex(20)
	_ = p.col(req, "stsk").Put(context.Background(), ak, []byte(req.Identity.Account))
	out := map[string]any{"Credentials": map[string]any{
		"AccessKeyId":     ak,
		"SecretAccessKey": p.deps.Rand.Derive(ak).Hex(40),
		"SessionToken":    p.deps.Rand.Derive(ak + "tok").Hex(32),
		"Expiration":      p.deps.Clock.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
	}}
	if userKey != "" {
		out[userKey] = map[string]any{"Arn": arn, "FederatedUserId": ak}
	}
	return &spi.Response{Output: out}
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
