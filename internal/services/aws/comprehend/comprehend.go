// Package comprehend stores endpoints and returns canned NLP (no model).
package comprehend

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.comprehend", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Comprehend-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.comprehend" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"DetectSentiment", "DetectEntities", "DetectKeyPhrases", "DetectDominantLanguage", "BatchDetectSentiment",
		"CreateEndpoint", "DescribeEndpoint", "ListEndpoints", "DeleteEndpoint",
		"StartDocumentClassificationJob", "DescribeDocumentClassificationJob", "ListDocumentClassificationJobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "DetectSentiment":
		return &spi.Response{Output: map[string]any{
			"Sentiment":      "POSITIVE",
			"SentimentScore": map[string]any{"Positive": 0.9, "Negative": 0.05, "Neutral": 0.04, "Mixed": 0.01},
		}}, nil
	case "DetectEntities":
		return &spi.Response{Output: map[string]any{"Entities": []any{}}}, nil
	case "DetectKeyPhrases":
		return &spi.Response{Output: map[string]any{"KeyPhrases": []any{}}}, nil
	case "DetectDominantLanguage":
		return &spi.Response{Output: map[string]any{"Languages": []any{map[string]any{"LanguageCode": "en", "Score": 0.99}}}}, nil
	case "BatchDetectSentiment":
		texts, _ := req.Input["TextList"].([]any)
		var results []any
		for i := range texts {
			results = append(results, map[string]any{"Index": i, "Sentiment": "POSITIVE"})
		}
		return &spi.Response{Output: map[string]any{"ResultList": results, "ErrorList": []any{}}}, nil
	case "CreateEndpoint":
		name := first(req.Input, "EndpointName")
		arn := "arn:aws:comprehend:" + req.Identity.Region + ":" + req.Identity.Account + ":endpoint/" + name
		rec := map[string]any{"EndpointName": name, "EndpointArn": arn, "Status": "IN_SERVICE", "ModelArn": first(req.Input, "ModelArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cpep").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"EndpointArn": arn}}, nil
	case "DescribeEndpoint":
		id := first(req.Input, "EndpointArn", "EndpointName")
		name := lastSlash(id)
		b, ok, _ := p.col(req, "cpep").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"EndpointProperties": rec}}, nil
	case "ListEndpoints":
		return listWrap(ctx, p.col(req, "cpep"), "EndpointPropertiesList")
	case "DeleteEndpoint":
		id := first(req.Input, "EndpointArn", "EndpointName")
		_ = p.col(req, "cpep").Delete(ctx, lastSlash(id))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartDocumentClassificationJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "JobName": first(req.Input, "JobName"), "JobStatus": "COMPLETED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cpjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobId": id, "JobStatus": "COMPLETED"}}, nil
	case "DescribeDocumentClassificationJob":
		id := first(req.Input, "JobId")
		b, ok, _ := p.col(req, "cpjob").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DocumentClassificationJobProperties": rec}}, nil
	case "ListDocumentClassificationJobs":
		return listWrap(ctx, p.col(req, "cpjob"), "DocumentClassificationJobPropertiesList")
	default:
		return nil, spi.NotImplemented("aws.comprehend", req.Operation, "emulate")
	}
}

func lastSlash(s string) string {
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		return s[i+1:]
	}
	return s
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
