// Package cloudcontrol stores resource records (no CloudFormation handler).
package cloudcontrol

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cloudcontrol", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Cloud Control-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cloudcontrol" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateResource", "GetResource", "ListResources", "DeleteResource", "UpdateResource", "GetResourceRequestStatus"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateResource":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Identifier": id, "TypeName": first(req.Input, "TypeName"), "Properties": first(req.Input, "DesiredState")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ccres").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ProgressEvent": map[string]any{"Identifier": id, "OperationStatus": "SUCCESS", "TypeName": rec["TypeName"]}}}, nil
	case "GetResource":
		id := first(req.Input, "Identifier")
		b, ok, _ := p.col(req, "ccres").Get(ctx, id)
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			return &spi.Response{Output: map[string]any{"ResourceDescription": rec}}, nil
		}
		if first(req.Input, "TypeName") == "AWS::S3::Bucket" {
			return p.s3Resource(ctx, req, id)
		}
		if collection := backingCollection(first(req.Input, "TypeName")); collection != "" {
			b, ok, _ = p.col(req, collection).Get(ctx, id)
		}
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return resourceDescription(id, first(req.Input, "TypeName"), b), nil
	case "ListResources":
		if records := listRecords(ctx, p.col(req, "ccres"), first(req.Input, "TypeName")); len(records) != 0 {
			return &spi.Response{Output: map[string]any{"ResourceDescriptions": records}}, nil
		}
		if first(req.Input, "TypeName") == "AWS::S3::Bucket" {
			return p.listS3Resources(ctx, req), nil
		}
		if collection := backingCollection(first(req.Input, "TypeName")); collection != "" {
			return listBacking(ctx, p.col(req, collection), first(req.Input, "TypeName")), nil
		}
		return listWrap(ctx, p.col(req, "ccres"), "ResourceDescriptions")
	case "DeleteResource":
		_ = p.col(req, "ccres").Delete(ctx, first(req.Input, "Identifier"))
		return &spi.Response{Output: map[string]any{"ProgressEvent": map[string]any{"OperationStatus": "SUCCESS"}}}, nil
	case "UpdateResource":
		id := first(req.Input, "Identifier")
		b, ok, _ := p.col(req, "ccres").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if p := first(req.Input, "PatchDocument", "DesiredState"); p != "" {
			rec["Properties"] = p
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ccres").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"ProgressEvent": map[string]any{"Identifier": id, "OperationStatus": "SUCCESS"}}}, nil
	case "GetResourceRequestStatus":
		return &spi.Response{Output: map[string]any{"ProgressEvent": map[string]any{"OperationStatus": "SUCCESS"}}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudcontrol", req.Operation, "emulate")
	}
}

// BackingCollection is exported because it is a cross-service contract rather
// than an implementation detail: CloudControl does not call the services it
// reports on, it reads their store collections directly, by name. Nothing else
// declares that dependency, so the check that keeps it honest lives outside
// this package and needs to see the map.
func BackingCollection(typeName string) string { return backingCollection(typeName) }

func backingCollection(typeName string) string {
	switch typeName {
	case "AWS::ApiGatewayV2::Api":
		return "ag2"
	case "AWS::RDS::DBInstance":
		return "dbinst"
	case "AWS::RDS::DBCluster":
		return "dbcluster"
	}
	return ""
}

func (p *Pack) s3Resource(ctx context.Context, req *spi.Request, bucket string) (*spi.Response, error) {
	if _, ok, _ := p.col(req, "buckets").Get(ctx, bucket); !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	props := map[string]any{"BucketName": bucket}
	if version, ok, _ := p.col(req, "versioning").Get(ctx, bucket); ok {
		props["VersioningConfiguration"] = map[string]any{"Status": string(version)}
	}
	if tags, ok, _ := p.col(req, "tags").Get(ctx, bucket); ok {
		var value any
		_ = json.Unmarshal(tags, &value)
		props["Tags"] = value
	}
	configs := map[string]string{
		"cors": "CorsConfiguration", "encryption": "BucketEncryption", "lifecycle": "LifecycleConfiguration",
		"replication": "ReplicationConfiguration", "acl": "AccessControl",
	}
	for kind, property := range configs {
		if raw, ok, _ := p.col(req, "bktcfg").Get(ctx, bucket+"/"+kind); ok {
			var doc map[string]any
			_ = json.Unmarshal(raw, &doc)
			if value, found := doc[property]; found {
				props[property] = value
			} else {
				props[property] = doc
			}
		}
	}
	raw, _ := json.Marshal(props)
	return resourceDescription(bucket, "AWS::S3::Bucket", raw), nil
}

func (p *Pack) listS3Resources(ctx context.Context, req *spi.Request) *spi.Response {
	kvs, _, _ := p.col(req, "buckets").List(ctx, "", "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		if resp, err := p.s3Resource(ctx, req, kv.Key); err == nil {
			items = append(items, resp.Output["ResourceDescription"])
		}
	}
	return &spi.Response{Output: map[string]any{"ResourceDescriptions": items}}
}

func resourceDescription(identifier, typeName string, properties []byte) *spi.Response {
	return &spi.Response{Output: map[string]any{"ResourceDescription": map[string]any{
		"Identifier": identifier, "TypeName": typeName, "Properties": string(properties),
	}}}
}

func listBacking(ctx context.Context, collection spi.Collection, typeName string) *spi.Response {
	kvs, _, _ := collection.List(ctx, "", "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		items = append(items, resourceDescription(kv.Key, typeName, kv.Value).Output["ResourceDescription"])
	}
	return &spi.Response{Output: map[string]any{"ResourceDescriptions": items}}
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	return &spi.Response{Output: map[string]any{key: listRecords(ctx, c, "")}}, nil
}

func listRecords(ctx context.Context, c spi.Collection, typeName string) []any {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if typeName != "" && rec["TypeName"] != typeName {
			continue
		}
		items = append(items, rec)
	}
	return items
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
