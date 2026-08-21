// Package route53 emulates hosted zones and resource record sets.
package route53

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.route53", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Route 53-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.route53" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateHostedZone", "GetHostedZone", "ListHostedZones", "DeleteHostedZone",
		"ChangeResourceRecordSets", "ListResourceRecordSets"}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil {
		req.Operation = route(req)
		p.fill(req)
	}
	switch req.Operation {
	case "CreateHostedZone":
		name := str(req.Input["Name"])
		id := "Z" + strings.ToUpper(p.deps.Rand.Hex(8))
		rec := map[string]any{"Id": "/hostedzone/" + id, "Name": name, "CallerReference": str(req.Input["CallerReference"])}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "hz").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"HostedZone": rec, "ChangeInfo": map[string]any{"Id": "/change/" + id, "Status": "INSYNC"}}}, nil
	case "GetHostedZone":
		id := zoneID(str(req.Input["Id"]))
		b, ok, _ := p.col(req, "hz").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "NoSuchHostedZone", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"HostedZone": rec}}, nil
	case "ListHostedZones":
		kvs, _, _ := p.col(req, "hz").List(ctx, "", "", 0)
		var zs []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			zs = append(zs, rec)
		}
		return &spi.Response{Output: map[string]any{"HostedZones": zs}}, nil
	case "DeleteHostedZone":
		id := zoneID(str(req.Input["Id"]))
		_ = p.col(req, "hz").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Id": "/change/" + id, "Status": "INSYNC"}}}, nil
	case "ChangeResourceRecordSets":
		id := zoneID(str(req.Input["Id"]))
		p.applyChanges(ctx, req, id)
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Id": "/change/" + p.deps.Rand.Hex(8), "Status": "INSYNC"}}}, nil
	case "ListResourceRecordSets":
		id := zoneID(str(req.Input["Id"]))
		kvs, _, _ := p.col(req, "rr:"+id).List(ctx, "", "", 0)
		var recs []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			recs = append(recs, rec)
		}
		return &spi.Response{Output: map[string]any{"ResourceRecordSets": recs}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) applyChanges(ctx context.Context, req *spi.Request, zone string) {
	raw := str(req.Input["_body"])
	var batch struct {
		ChangeBatch struct {
			Changes struct {
				Change []struct {
					Action string `xml:"Action"`
					Set    struct {
						Name   string `xml:"Name"`
						Type   string `xml:"Type"`
						TTL    string `xml:"TTL"`
						Values struct {
							RR []struct {
								Value string `xml:"Value"`
							} `xml:"ResourceRecord"`
						} `xml:"ResourceRecords"`
					} `xml:"ResourceRecordSet"`
				} `xml:"Change"`
			} `xml:"Changes"`
		} `xml:"ChangeBatch"`
	}
	_ = xml.Unmarshal([]byte(raw), &batch)
	for _, ch := range batch.ChangeBatch.Changes.Change {
		key := ch.Set.Name + "|" + ch.Set.Type
		if strings.EqualFold(ch.Action, "DELETE") {
			_ = p.col(req, "rr:"+zone).Delete(ctx, key)
			continue
		}
		var vals []any
		for _, rr := range ch.Set.Values.RR {
			vals = append(vals, map[string]any{"Value": rr.Value})
		}
		rec := map[string]any{"Name": ch.Set.Name, "Type": ch.Set.Type, "TTL": ch.Set.TTL, "ResourceRecords": vals}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rr:"+zone).Put(ctx, key, b)
	}
}

func (p *Pack) fill(req *spi.Request) {
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	// 2013-04-01 / hostedzone / {id} / rrset
	if len(parts) >= 3 && parts[1] == "hostedzone" {
		req.Input["Id"] = parts[2]
	}
	if str(req.Input["_body"]) == "" && req.HTTP.Body != nil {
		b, _ := io.ReadAll(req.HTTP.Body)
		req.Input["_body"] = string(b)
	}
	if body := str(req.Input["_body"]); body != "" {
		var hz struct {
			Name            string `xml:"Name"`
			CallerReference string `xml:"CallerReference"`
		}
		if xml.Unmarshal([]byte(body), &hz) == nil {
			if hz.Name != "" {
				req.Input["Name"] = hz.Name
			}
			if hz.CallerReference != "" {
				req.Input["CallerReference"] = hz.CallerReference
			}
		}
	}
}

func route(req *spi.Request) string {
	if a := req.HTTP.URL.Query().Get("Action"); a != "" {
		return a
	}
	path := req.HTTP.URL.Path
	m := req.HTTP.Method
	switch {
	case strings.Contains(path, "/rrset") && m == http.MethodPost:
		return "ChangeResourceRecordSets"
	case strings.Contains(path, "/rrset") && m == http.MethodGet:
		return "ListResourceRecordSets"
	case strings.HasSuffix(path, "/hostedzone") && m == http.MethodPost:
		return "CreateHostedZone"
	case strings.HasSuffix(path, "/hostedzone") && m == http.MethodGet:
		return "ListHostedZones"
	case m == http.MethodDelete:
		return "DeleteHostedZone"
	case m == http.MethodGet:
		return "GetHostedZone"
	default:
		return req.Operation
	}
}

func zoneID(id string) string {
	id = strings.TrimPrefix(id, "/hostedzone/")
	if i := strings.LastIndex(id, "/"); i >= 0 {
		id = id[i+1:]
	}
	return id
}

func str(v any) string { s, _ := v.(string); return s }
