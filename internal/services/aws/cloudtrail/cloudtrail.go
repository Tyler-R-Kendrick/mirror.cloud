// Package cloudtrail stores trails and lookup events (no real account activity feed).
package cloudtrail

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cloudtrail", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudTrail-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cloudtrail" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateTrail", "DescribeTrails", "GetTrail", "DeleteTrail", "UpdateTrail",
		"StartLogging", "StopLogging", "GetTrailStatus",
		"PutEventSelectors", "GetEventSelectors", "LookupEvents",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "TrailName")
	switch req.Operation {
	case "CreateTrail":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidTrailNameException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:cloudtrail:" + req.Identity.Region + ":" + req.Identity.Account + ":trail/" + name
		rec := map[string]any{
			"Name": name, "TrailARN": arn, "S3BucketName": first(req.Input, "S3BucketName"),
			"IsMultiRegionTrail": req.Input["IsMultiRegionTrail"], "IsLogging": true,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ct").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetTrail":
		b, ok, _ := p.col(req, "ct").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "TrailNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Trail": rec}}, nil
	case "DescribeTrails":
		kvs, _, _ := p.col(req, "ct").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"trailList": items}}, nil
	case "UpdateTrail":
		b, ok, _ := p.col(req, "ct").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "TrailNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if s := first(req.Input, "S3BucketName"); s != "" {
			rec["S3BucketName"] = s
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ct").Put(ctx, name, nb)
		return &spi.Response{Output: rec}, nil
	case "DeleteTrail":
		_ = p.col(req, "ct").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartLogging", "StopLogging":
		b, ok, _ := p.col(req, "ct").Get(ctx, name)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["IsLogging"] = req.Operation == "StartLogging"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ct").Put(ctx, name, nb)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTrailStatus":
		b, ok, _ := p.col(req, "ct").Get(ctx, name)
		logging := false
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			logging, _ = rec["IsLogging"].(bool)
		}
		return &spi.Response{Output: map[string]any{"IsLogging": logging}}, nil
	case "PutEventSelectors":
		b, _ := json.Marshal(req.Input["EventSelectors"])
		_ = p.col(req, "ctsel").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TrailARN": first(req.Input, "TrailARN"), "EventSelectors": req.Input["EventSelectors"]}}, nil
	case "GetEventSelectors":
		b, ok, _ := p.col(req, "ctsel").Get(ctx, name)
		var sel any = []any{}
		if ok {
			_ = json.Unmarshal(b, &sel)
		}
		return &spi.Response{Output: map[string]any{"EventSelectors": sel}}, nil
	case "LookupEvents":
		kvs, _, _ := p.col(req, "ctev").List(ctx, "", "", 0)
		var ev []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			ev = append(ev, rec)
		}
		return &spi.Response{Output: map[string]any{"Events": ev}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudtrail", req.Operation, "emulate")
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
