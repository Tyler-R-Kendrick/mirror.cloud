// Package comprehendmedical returns canned medical NLP (no clinical model).
package comprehendmedical

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.comprehendmedical", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Comprehend Medical-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.comprehendmedical" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"DetectEntitiesV2", "DetectPHI", "InferICD10CM",
		"StartEntitiesDetectionV2Job", "DescribeEntitiesDetectionV2Job", "ListEntitiesDetectionV2Jobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func canned() []any {
	return []any{map[string]any{"Category": "MEDICAL_CONDITION", "Text": "cough", "Score": 0.99, "Type": "DX_NAME"}}
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "DetectEntitiesV2":
		return &spi.Response{Output: map[string]any{"Entities": canned()}}, nil
	case "DetectPHI":
		return &spi.Response{Output: map[string]any{"Entities": []any{}}}, nil
	case "InferICD10CM":
		return &spi.Response{Output: map[string]any{"Entities": canned()}}, nil
	case "StartEntitiesDetectionV2Job":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "JobName": first(req.Input, "JobName"), "JobStatus": "COMPLETED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cmjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobId": id}}, nil
	case "DescribeEntitiesDetectionV2Job":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "cmjob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ComprehendMedicalAsyncJobProperties": rec}}, nil
	case "ListEntitiesDetectionV2Jobs":
		return listWrap(ctx, p.col(req, "cmjob"), "ComprehendMedicalAsyncJobPropertiesList")
	default:
		return nil, spi.NotImplemented("aws.comprehendmedical", req.Operation, "emulate")
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
