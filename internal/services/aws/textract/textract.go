// Package textract stores jobs and returns canned blocks (no OCR).
package textract

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.textract", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Textract-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.textract" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"DetectDocumentText", "AnalyzeDocument",
		"StartDocumentTextDetection", "GetDocumentTextDetection",
		"StartDocumentAnalysis", "GetDocumentAnalysis",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func canned() []any {
	return []any{map[string]any{"BlockType": "LINE", "Text": "HELLO", "Confidence": 99.0}}
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "DetectDocumentText":
		return &spi.Response{Output: map[string]any{"Blocks": canned(), "DocumentMetadata": map[string]any{"Pages": 1}}}, nil
	case "AnalyzeDocument":
		return &spi.Response{Output: map[string]any{"Blocks": canned(), "DocumentMetadata": map[string]any{"Pages": 1}}}, nil
	case "StartDocumentTextDetection", "StartDocumentAnalysis":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "JobStatus": "SUCCEEDED", "Blocks": canned()}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "txjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobId": id}}, nil
	case "GetDocumentTextDetection", "GetDocumentAnalysis":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "txjob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidJobIdException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.textract", req.Operation, "emulate")
	}
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
