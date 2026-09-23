// Package cloudcontrol reads the resources Cloud Control answers for: its own
// records, served from its bundle, and the resources other services own.
package cloudcontrol

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"GetResource", "ListResources"} {
		bundled.RegisterNative("aws.cloudcontrol", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return reader{deps}.invoke(ctx, req)
		})
	}
}

// reader answers GetResource and ListResources: the bundle's own records, and
// the resources other services own.
//
// ponytail: those are read out of the owners' stores in their layouts -- S3's
// buckets and bucket configuration, API Gateway v2's ag2, RDS's dbinst and
// dbcluster. Calling each owner's Get/Describe operation, as CloudFormation
// now does to provision, is the upgrade.
type reader struct{ deps spi.Deps }

func (p reader) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func notFound() error {
	return &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
}

func (p reader) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
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
			return nil, notFound()
		}
		return resourceDescription(id, first(req.Input, "TypeName"), b), nil
	default: // ListResources
		if records := listRecords(ctx, p.col(req, "ccres"), first(req.Input, "TypeName")); len(records) != 0 {
			return &spi.Response{Output: map[string]any{"ResourceDescriptions": records}}, nil
		}
		if first(req.Input, "TypeName") == "AWS::S3::Bucket" {
			return p.listS3Resources(ctx, req), nil
		}
		if collection := backingCollection(first(req.Input, "TypeName")); collection != "" {
			return listBacking(ctx, p.col(req, collection), first(req.Input, "TypeName")), nil
		}
		return &spi.Response{Output: map[string]any{"ResourceDescriptions": listRecords(ctx, p.col(req, "ccres"), "")}}, nil
	}
}

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

func (p reader) s3Resource(ctx context.Context, req *spi.Request, bucket string) (*spi.Response, error) {
	if _, ok, _ := p.col(req, "buckets").Get(ctx, bucket); !ok {
		return nil, notFound()
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

func (p reader) listS3Resources(ctx context.Context, req *spi.Request) *spi.Response {
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
