// Package translate stores terminologies and echoes text (no MT).
package translate

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.translate", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Translate-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.translate" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"TranslateText",
		"CreateTerminology", "GetTerminology", "ListTerminologies", "DeleteTerminology",
		"StartTextTranslationJob", "DescribeTextTranslationJob", "ListTextTranslationJobs", "StopTextTranslationJob",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "TranslateText":
		text := first(req.Input, "Text")
		if text == "" {
			return nil, &spi.Fault{Code: "InvalidRequestException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{
			"TranslatedText": text, "SourceLanguageCode": first(req.Input, "SourceLanguageCode"), "TargetLanguageCode": first(req.Input, "TargetLanguageCode"),
		}}, nil
	case "CreateTerminology":
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "SourceLanguageCode": first(req.Input, "SourceLanguageCode"), "TargetLanguageCodes": req.Input["TargetLanguageCodes"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "trterm").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TerminologyProperties": rec}}, nil
	case "GetTerminology":
		name := first(req.Input, "Name")
		b, ok, _ := p.col(req, "trterm").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"TerminologyProperties": rec}}, nil
	case "ListTerminologies":
		return listWrap(ctx, p.col(req, "trterm"), "TerminologyPropertiesList")
	case "DeleteTerminology":
		_ = p.col(req, "trterm").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartTextTranslationJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "JobName": first(req.Input, "JobName"), "JobStatus": "COMPLETED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "trjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobId": id, "JobStatus": "COMPLETED"}}, nil
	case "DescribeTextTranslationJob":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "trjob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"TextTranslationJobProperties": rec}}, nil
	case "ListTextTranslationJobs":
		return listWrap(ctx, p.col(req, "trjob"), "TextTranslationJobPropertiesList")
	case "StopTextTranslationJob":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "trjob").Get(ctx, id)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			rec["JobStatus"] = "STOPPED"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "trjob").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{"JobId": id, "JobStatus": "STOPPED"}}, nil
	default:
		return nil, spi.NotImplemented("aws.translate", req.Operation, "emulate")
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
