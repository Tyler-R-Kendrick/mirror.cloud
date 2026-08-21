// Package pinpoint stores apps and campaigns (no message delivery).
package pinpoint

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.pinpoint", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Pinpoint-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.pinpoint" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateApp", "GetApp", "GetApps", "DeleteApp",
		"CreateCampaign", "GetCampaign", "DeleteCampaign",
		"SendMessages",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateApp":
		id := p.deps.Rand.Hex(8)
		name := first(req.Input, "Name")
		if name == "" {
			if n, ok := req.Input["CreateApplicationRequest"].(map[string]any); ok {
				name = first(n, "Name")
			}
		}
		rec := map[string]any{"Id": id, "Name": name, "Arn": "arn:aws:mobiletargeting:" + req.Identity.Region + ":" + req.Identity.Account + ":apps/" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ppapp").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ApplicationResponse": rec}}, nil
	case "GetApp":
		id := first(req.Input, "ApplicationId", "Id")
		b, ok, _ := p.col(req, "ppapp").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ApplicationResponse": rec}}, nil
	case "GetApps":
		return listWrap(ctx, p.col(req, "ppapp"), "ApplicationsResponse", "Item")
	case "DeleteApp":
		_ = p.col(req, "ppapp").Delete(ctx, first(req.Input, "ApplicationId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCampaign":
		id := p.deps.Rand.Hex(8)
		app := first(req.Input, "ApplicationId")
		rec := map[string]any{"Id": id, "ApplicationId": app, "Name": first(req.Input, "Name"), "State": "SCHEDULED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ppcamp").Put(ctx, app+"/"+id, b)
		return &spi.Response{Output: map[string]any{"CampaignResponse": rec}}, nil
	case "GetCampaign":
		key := first(req.Input, "ApplicationId") + "/" + first(req.Input, "CampaignId", "Id")
		b, ok, _ := p.col(req, "ppcamp").Get(ctx, key)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"CampaignResponse": rec}}, nil
	case "DeleteCampaign":
		_ = p.col(req, "ppcamp").Delete(ctx, first(req.Input, "ApplicationId")+"/"+first(req.Input, "CampaignId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SendMessages":
		return &spi.Response{Output: map[string]any{"MessageResponse": map[string]any{"ApplicationId": first(req.Input, "ApplicationId"), "Result": map[string]any{}}}}, nil
	default:
		return nil, spi.NotImplemented("aws.pinpoint", req.Operation, "emulate")
	}
}

func listWrap(ctx context.Context, c spi.Collection, outer, inner string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outer: map[string]any{inner: items}}}, nil
}

func first(in map[string]any, keys ...string) string {
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
