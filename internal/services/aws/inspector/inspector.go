// Package inspector stores assessment targets/templates/runs (no agent scan).
package inspector

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.inspector", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Inspector-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.inspector" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateAssessmentTarget", "DescribeAssessmentTargets", "DeleteAssessmentTarget",
		"CreateAssessmentTemplate", "DescribeAssessmentTemplates", "DeleteAssessmentTemplate",
		"StartAssessmentRun", "DescribeAssessmentRuns", "ListAssessmentRuns", "StopAssessmentRun",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateAssessmentTarget":
		arn := "arn:aws:inspector:" + req.Identity.Region + ":" + req.Identity.Account + ":target/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"arn": arn, "name": first(req.Input, "assessmentTargetName", "name")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "insp-t").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"assessmentTargetArn": arn}}, nil
	case "DescribeAssessmentTargets":
		return p.describe(ctx, req, "insp-t", "assessmentTargetArns", "assessmentTargets")
	case "DeleteAssessmentTarget":
		_ = p.col(req, "insp-t").Delete(ctx, first(req.Input, "assessmentTargetArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateAssessmentTemplate":
		arn := "arn:aws:inspector:" + req.Identity.Region + ":" + req.Identity.Account + ":template/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"arn": arn, "name": first(req.Input, "assessmentTemplateName", "name"), "assessmentTargetArn": first(req.Input, "assessmentTargetArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "insp-p").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"assessmentTemplateArn": arn}}, nil
	case "DescribeAssessmentTemplates":
		return p.describe(ctx, req, "insp-p", "assessmentTemplateArns", "assessmentTemplates")
	case "DeleteAssessmentTemplate":
		_ = p.col(req, "insp-p").Delete(ctx, first(req.Input, "assessmentTemplateArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartAssessmentRun":
		arn := "arn:aws:inspector:" + req.Identity.Region + ":" + req.Identity.Account + ":run/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"arn": arn, "state": "COMPLETED", "assessmentTemplateArn": first(req.Input, "assessmentTemplateArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "insp-r").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"assessmentRunArn": arn}}, nil
	case "DescribeAssessmentRuns":
		return p.describe(ctx, req, "insp-r", "assessmentRunArns", "assessmentRuns")
	case "ListAssessmentRuns":
		kvs, _, _ := p.col(req, "insp-r").List(ctx, "", "", 0)
		var arns []any
		for _, kv := range kvs {
			arns = append(arns, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"assessmentRunArns": arns}}, nil
	case "StopAssessmentRun":
		arn := first(req.Input, "assessmentRunArn")
		b, ok, _ := p.col(req, "insp-r").Get(ctx, arn)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["state"] = "COMPLETED"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "insp-r").Put(ctx, arn, nb)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.inspector", req.Operation, "emulate")
	}
}

func (p *Pack) describe(ctx context.Context, req *spi.Request, col, inKey, outKey string) (*spi.Response, error) {
	arns := stringList(req.Input[inKey])
	var items []any
	if len(arns) == 0 {
		kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
	} else {
		for _, arn := range arns {
			b, ok, _ := p.col(req, col).Get(ctx, arn)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			items = append(items, rec)
		}
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
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
