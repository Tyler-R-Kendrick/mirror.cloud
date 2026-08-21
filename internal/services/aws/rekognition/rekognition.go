// Package rekognition stores collections and returns canned detections (no CV).
package rekognition

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.rekognition", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Rekognition-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.rekognition" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCollection", "DescribeCollection", "ListCollections", "DeleteCollection",
		"IndexFaces", "SearchFacesByImage",
		"DetectLabels", "DetectFaces", "DetectText", "DetectModerationLabels",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCollection":
		id := first(req.Input, "CollectionId")
		if id == "" {
			return nil, &spi.Fault{Code: "InvalidParameterException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"CollectionId": id, "FaceCount": 0, "CollectionARN": "arn:aws:rekognition:" + req.Identity.Region + ":" + req.Identity.Account + ":collection/" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "rkcol").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"StatusCode": 200, "CollectionArn": rec["CollectionARN"]}}, nil
	case "DescribeCollection":
		id := first(req.Input, "CollectionId")
		b, ok, _ := p.col(req, "rkcol").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListCollections":
		kvs, _, _ := p.col(req, "rkcol").List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			ids = append(ids, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"CollectionIds": ids}}, nil
	case "DeleteCollection":
		_ = p.col(req, "rkcol").Delete(ctx, first(req.Input, "CollectionId"))
		return &spi.Response{Output: map[string]any{"StatusCode": 200}}, nil
	case "IndexFaces":
		col := first(req.Input, "CollectionId")
		fid := p.deps.Rand.Hex(8)
		face := map[string]any{"FaceId": fid, "CollectionId": col}
		b, _ := json.Marshal(face)
		_ = p.col(req, "rkface:"+col).Put(ctx, fid, b)
		return &spi.Response{Output: map[string]any{"FaceRecords": []any{map[string]any{"Face": face}}}}, nil
	case "SearchFacesByImage":
		col := first(req.Input, "CollectionId")
		kvs, _, _ := p.col(req, "rkface:"+col).List(ctx, "", "", 0)
		var matches []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			matches = append(matches, map[string]any{"Face": rec, "Similarity": 99.0})
		}
		return &spi.Response{Output: map[string]any{"FaceMatches": matches}}, nil
	case "DetectLabels":
		return &spi.Response{Output: map[string]any{"Labels": []any{map[string]any{"Name": "Person", "Confidence": 99.0}}}}, nil
	case "DetectFaces":
		return &spi.Response{Output: map[string]any{"FaceDetails": []any{map[string]any{"Confidence": 99.0}}}}, nil
	case "DetectText":
		return &spi.Response{Output: map[string]any{"TextDetections": []any{map[string]any{"DetectedText": "HELLO", "Confidence": 99.0, "Type": "LINE"}}}}, nil
	case "DetectModerationLabels":
		return &spi.Response{Output: map[string]any{"ModerationLabels": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.rekognition", req.Operation, "emulate")
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
