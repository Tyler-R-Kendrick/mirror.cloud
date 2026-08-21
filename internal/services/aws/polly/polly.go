// Package polly stores synthesis tasks and returns empty audio (no TTS).
package polly

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.polly", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Polly-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.polly" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"SynthesizeSpeech", "DescribeVoices",
		"StartSpeechSynthesisTask", "GetSpeechSynthesisTask", "ListSpeechSynthesisTasks",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "SynthesizeSpeech":
		text := first(req.Input, "Text")
		if text == "" {
			return nil, &spi.Fault{Code: "InvalidSsmlException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{
			"ContentType": "audio/mpeg", "AudioStream": "", "RequestCharacters": len(text),
		}}, nil
	case "DescribeVoices":
		return &spi.Response{Output: map[string]any{"Voices": []any{
			map[string]any{"Id": "Joanna", "LanguageCode": "en-US", "Gender": "Female", "Name": "Joanna"},
		}}}, nil
	case "StartSpeechSynthesisTask":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"TaskId": id, "TaskStatus": "completed", "OutputUri": "s3://mirror-polly/" + id + ".mp3",
			"Text": first(req.Input, "Text"), "VoiceId": first(req.Input, "VoiceId"),
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "potask").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"SynthesisTask": rec}}, nil
	case "GetSpeechSynthesisTask":
		id := first(req.Input, "TaskId")
		b, ok, _ := p.col(req, "potask").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "SynthesisTaskNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"SynthesisTask": rec}}, nil
	case "ListSpeechSynthesisTasks":
		kvs, _, _ := p.col(req, "potask").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"SynthesisTasks": items}}, nil
	default:
		return nil, spi.NotImplemented("aws.polly", req.Operation, "emulate")
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
