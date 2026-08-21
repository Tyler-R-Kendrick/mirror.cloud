// Package ses stores identities and sent messages (no SMTP).
package ses

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ses", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements SES-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ses" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"VerifyEmailIdentity", "DeleteIdentity", "ListIdentities", "GetIdentityVerificationAttributes",
		"SendEmail", "SendRawEmail", "GetSendQuota", "GetSendStatistics",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "VerifyEmailIdentity":
		id := first(req.Input, "EmailAddress")
		rec := map[string]any{"EmailAddress": id, "VerificationStatus": "Success"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sesid").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteIdentity":
		_ = p.col(req, "sesid").Delete(ctx, first(req.Input, "Identity"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListIdentities":
		kvs, _, _ := p.col(req, "sesid").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"Identities": ids}}, nil
	case "GetIdentityVerificationAttributes":
		id := first(req.Input, "Identities.member.1", "Identity")
		b, ok, _ := p.col(req, "sesid").Get(ctx, id)
		attrs := map[string]any{}
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			attrs[id] = rec
		}
		return &spi.Response{Output: map[string]any{"VerificationAttributes": attrs}}, nil
	case "SendEmail", "SendRawEmail":
		mid := "0000-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"MessageId": mid, "Source": first(req.Input, "Source"), "Destination": req.Input["Destination"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sesmsg").Put(ctx, mid, b)
		return &spi.Response{Output: map[string]any{"MessageId": mid}}, nil
	case "GetSendQuota":
		return &spi.Response{Output: map[string]any{"Max24HourSend": 200.0, "MaxSendRate": 1.0, "SentLast24Hours": 0.0}}, nil
	case "GetSendStatistics":
		return &spi.Response{Output: map[string]any{"SendDataPoints": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.ses", req.Operation, "emulate")
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
