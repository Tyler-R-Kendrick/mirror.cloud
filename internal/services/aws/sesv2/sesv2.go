// Package sesv2 stores SESv2 identities and sent messages (no SMTP).
package sesv2

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sesv2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements SESv2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sesv2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateEmailIdentity", "GetEmailIdentity", "ListEmailIdentities", "DeleteEmailIdentity",
		"SendEmail", "SendBulkEmail", "GetAccount",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateEmailIdentity":
		id := first(req.Input, "EmailIdentity")
		rec := map[string]any{"EmailIdentity": id, "IdentityType": "EMAIL_ADDRESS", "VerifiedForSendingStatus": true}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sesv2id").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetEmailIdentity":
		id := first(req.Input, "EmailIdentity")
		b, ok, _ := p.col(req, "sesv2id").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListEmailIdentities":
		kvs, _, _ := p.col(req, "sesv2id").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"EmailIdentities": items}}, nil
	case "DeleteEmailIdentity":
		_ = p.col(req, "sesv2id").Delete(ctx, first(req.Input, "EmailIdentity"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SendEmail", "SendBulkEmail":
		mid := p.deps.Rand.Hex(16)
		rec := map[string]any{"MessageId": mid, "FromEmailAddress": first(req.Input, "FromEmailAddress")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sesv2msg").Put(ctx, mid, b)
		return &spi.Response{Output: map[string]any{"MessageId": mid}}, nil
	case "GetAccount":
		return &spi.Response{Output: map[string]any{"SendingEnabled": true, "ProductionAccessEnabled": true}}, nil
	default:
		return nil, spi.NotImplemented("aws.sesv2", req.Operation, "emulate")
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
