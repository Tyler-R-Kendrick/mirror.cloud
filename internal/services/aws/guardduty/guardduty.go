// Package guardduty stores detectors, IP sets, filters, and findings (no threat intel feed).
package guardduty

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.guardduty", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements GuardDuty-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.guardduty" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDetector", "GetDetector", "ListDetectors", "UpdateDetector", "DeleteDetector",
		"CreateIPSet", "GetIPSet", "ListIPSets", "DeleteIPSet",
		"CreateFilter", "GetFilter", "ListFilters", "DeleteFilter",
		"CreateSampleFindings", "GetFindings", "ListFindings",
		"CreateMembers", "GetMembers", "ListMembers", "DeleteMembers",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	did := first(req.Input, "DetectorId")
	switch req.Operation {
	case "CreateDetector":
		id := p.deps.Rand.Hex(16)
		rec := map[string]any{"DetectorId": id, "Status": first(req.Input, "Enable"), "FindingPublishingFrequency": first(req.Input, "FindingPublishingFrequency")}
		if rec["Status"] == "" || rec["Status"] == "true" {
			rec["Status"] = "ENABLED"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gd").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DetectorId": id}}, nil
	case "GetDetector":
		b, ok, _ := p.col(req, "gd").Get(ctx, did)
		if !ok {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListDetectors":
		kvs, _, _ := p.col(req, "gd").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"DetectorIds": ids}}, nil
	case "UpdateDetector":
		b, ok, _ := p.col(req, "gd").Get(ctx, did)
		if !ok {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if f := first(req.Input, "FindingPublishingFrequency"); f != "" {
			rec["FindingPublishingFrequency"] = f
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "gd").Put(ctx, did, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteDetector":
		_ = p.col(req, "gd").Delete(ctx, did)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateIPSet":
		id := "ipset-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": first(req.Input, "Name"), "IpSetId": id, "Location": first(req.Input, "Location"), "Format": first(req.Input, "Format")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gdip:"+did).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"IpSetId": id}}, nil
	case "GetIPSet":
		return getNamed(ctx, p.col(req, "gdip:"+did), first(req.Input, "IpSetId"))
	case "ListIPSets":
		return listIDs(ctx, p.col(req, "gdip:"+did), "IpSetIds")
	case "DeleteIPSet":
		_ = p.col(req, "gdip:"+did).Delete(ctx, first(req.Input, "IpSetId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateFilter":
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "Action": first(req.Input, "Action"), "FindingCriteria": req.Input["FindingCriteria"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gdflt:"+did).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Name": name}}, nil
	case "GetFilter":
		return getNamed(ctx, p.col(req, "gdflt:"+did), first(req.Input, "FilterName", "Name"))
	case "ListFilters":
		return listIDs(ctx, p.col(req, "gdflt:"+did), "FilterNames")
	case "DeleteFilter":
		_ = p.col(req, "gdflt:"+did).Delete(ctx, first(req.Input, "FilterName", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateSampleFindings":
		id := "finding-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Type": "Recon:EC2/PortProbeUnprotectedPort", "Severity": 5, "DetectorId": did, "Title": "sample"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gdfind:"+did).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetFindings":
		ids := stringList(req.Input["FindingIds"])
		var items []any
		for _, id := range ids {
			b, ok, _ := p.col(req, "gdfind:"+did).Get(ctx, id)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
		if len(ids) == 0 {
			return listCol(ctx, p.col(req, "gdfind:"+did), "Findings")
		}
		return &spi.Response{Output: map[string]any{"Findings": items}}, nil
	case "ListFindings":
		return listIDs(ctx, p.col(req, "gdfind:"+did), "FindingIds")
	case "CreateMembers":
		accts, _ := req.Input["AccountDetails"].([]any)
		var unprocessed []any
		for _, a := range accts {
			am, _ := a.(map[string]any)
			id := first(am, "AccountId")
			rec := map[string]any{"AccountId": id, "Email": first(am, "Email"), "RelationshipStatus": "Enabled"}
			b, _ := json.Marshal(rec)
			_ = p.col(req, "gdmem:"+did).Put(ctx, id, b)
			_ = unprocessed
		}
		return &spi.Response{Output: map[string]any{"UnprocessedAccounts": []any{}}}, nil
	case "GetMembers":
		return listCol(ctx, p.col(req, "gdmem:"+did), "Members")
	case "ListMembers":
		return listCol(ctx, p.col(req, "gdmem:"+did), "Members")
	case "DeleteMembers":
		ids := stringList(req.Input["AccountIds"])
		for _, id := range ids {
			_ = p.col(req, "gdmem:"+did).Delete(ctx, id)
		}
		return &spi.Response{Output: map[string]any{"UnprocessedAccounts": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.guardduty", req.Operation, "emulate")
	}
}

func getNamed(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listIDs(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var ids []any
	for _, kv := range kvs {
		ids = append(ids, kv.Key)
	}
	return &spi.Response{Output: map[string]any{key: ids}}, nil
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

func stringList(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case []string:
		return t
	}
	return nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		switch v := in[k].(type) {
		case string:
			if v != "" {
				return v
			}
		case bool:
			if v {
				return "true"
			}
			return "false"
		}
	}
	return ""
}
