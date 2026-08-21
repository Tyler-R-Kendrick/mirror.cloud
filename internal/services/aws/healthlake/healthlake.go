// Package healthlake stores FHIR datastore records (no FHIR server).
package healthlake

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.healthlake", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements HealthLake-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.healthlake" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateFHIRDatastore", "DescribeFHIRDatastore", "ListFHIRDatastores", "DeleteFHIRDatastore",
		"StartFHIRImportJob", "DescribeFHIRImportJob",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateFHIRDatastore":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"DatastoreId": id, "DatastoreName": first(req.Input, "DatastoreName"),
			"DatastoreStatus": "ACTIVE", "DatastoreTypeVersion": first(req.Input, "DatastoreTypeVersion"),
			"DatastoreArn": "arn:aws:healthlake:" + req.Identity.Region + ":" + req.Identity.Account + ":datastore/fhir/" + id,
		}
		if rec["DatastoreTypeVersion"] == "" {
			rec["DatastoreTypeVersion"] = "R4"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "hlds").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"DatastoreId": id, "DatastoreArn": rec["DatastoreArn"], "DatastoreStatus": "ACTIVE"}}, nil
	case "DescribeFHIRDatastore":
		id := first(req.Input, "DatastoreId")
		b, ok, _ := p.col(req, "hlds").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DatastoreProperties": rec}}, nil
	case "ListFHIRDatastores":
		return listWrap(ctx, p.col(req, "hlds"), "DatastorePropertiesList")
	case "DeleteFHIRDatastore":
		_ = p.col(req, "hlds").Delete(ctx, first(req.Input, "DatastoreId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartFHIRImportJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "JobName": first(req.Input, "JobName"), "JobStatus": "COMPLETED", "DatastoreId": first(req.Input, "DatastoreId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "hljob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobId": id, "JobStatus": "COMPLETED"}}, nil
	case "DescribeFHIRImportJob":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "hljob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ImportJobProperties": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.healthlake", req.Operation, "emulate")
	}
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
