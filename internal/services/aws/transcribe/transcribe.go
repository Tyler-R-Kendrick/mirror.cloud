// Package transcribe stores transcription jobs (no ASR).
package transcribe

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.transcribe", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Transcribe-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.transcribe" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"StartTranscriptionJob", "GetTranscriptionJob", "ListTranscriptionJobs", "DeleteTranscriptionJob",
		"CreateVocabulary", "GetVocabulary", "ListVocabularies", "DeleteVocabulary",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "StartTranscriptionJob":
		name := first(req.Input, "TranscriptionJobName")
		if name == "" {
			return nil, &spi.Fault{Code: "BadRequestException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"TranscriptionJobName": name, "TranscriptionJobStatus": "COMPLETED",
			"LanguageCode": first(req.Input, "LanguageCode"), "Media": req.Input["Media"],
			"Transcript": map[string]any{"TranscriptFileUri": "s3://mirror-transcribe/" + name + ".json"},
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "trjob").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TranscriptionJob": rec}}, nil
	case "GetTranscriptionJob":
		return getWrap(ctx, p.col(req, "trjob"), first(req.Input, "TranscriptionJobName"), "TranscriptionJob", "NotFoundException")
	case "ListTranscriptionJobs":
		return listWrap(ctx, p.col(req, "trjob"), "TranscriptionJobSummaries")
	case "DeleteTranscriptionJob":
		_ = p.col(req, "trjob").Delete(ctx, first(req.Input, "TranscriptionJobName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateVocabulary":
		name := first(req.Input, "VocabularyName")
		rec := map[string]any{"VocabularyName": name, "LanguageCode": first(req.Input, "LanguageCode"), "VocabularyState": "READY"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "trvocab").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetVocabulary":
		return getBare(ctx, p.col(req, "trvocab"), first(req.Input, "VocabularyName"), "NotFoundException")
	case "ListVocabularies":
		return listWrap(ctx, p.col(req, "trvocab"), "Vocabularies")
	case "DeleteVocabulary":
		_ = p.col(req, "trvocab").Delete(ctx, first(req.Input, "VocabularyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.transcribe", req.Operation, "emulate")
	}
}

func getWrap(ctx context.Context, c spi.Collection, id, key, code string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: code, HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{key: rec}}, nil
}

func getBare(ctx context.Context, c spi.Collection, id, code string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: code, HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
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
