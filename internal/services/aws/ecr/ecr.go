// Package ecr stores repository metadata and image manifests (not a real registry daemon).
package ecr

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.api.ecr", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ECR-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.api.ecr" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateRepository", "DescribeRepositories", "DeleteRepository",
		"PutImage", "BatchGetImage", "ListImages", "BatchDeleteImage",
		"GetAuthorizationToken", "SetRepositoryPolicy", "GetRepositoryPolicy", "DeleteRepositoryPolicy",
		"TagResource", "ListTagsForResource", "UntagResource",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	acct, region := req.Identity.Account, req.Identity.Region
	switch req.Operation {
	case "CreateRepository":
		name := first(req.Input, "repositoryName")
		arn := "arn:aws:ecr:" + region + ":" + acct + ":repository/" + name
		uri := acct + ".dkr.ecr." + region + ".amazonaws.com/" + name
		rec := map[string]any{"repositoryName": name, "repositoryArn": arn, "repositoryUri": uri, "registryId": acct}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecrrepo").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"repository": rec}}, nil
	case "DescribeRepositories":
		kvs, _, _ := p.col(req, "ecrrepo").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"repositories": items}}, nil
	case "DeleteRepository":
		_ = p.col(req, "ecrrepo").Delete(ctx, first(req.Input, "repositoryName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutImage":
		repo := first(req.Input, "repositoryName")
		tag := first(req.Input, "imageTag")
		if tag == "" {
			tag = "latest"
		}
		digest := "sha256:" + p.deps.Rand.Hex(16)
		rec := map[string]any{"imageTag": tag, "imageDigest": digest, "imageManifest": req.Input["imageManifest"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecrimg:"+repo).Put(ctx, tag, b)
		return &spi.Response{Output: map[string]any{"image": rec}}, nil
	case "BatchGetImage":
		repo := first(req.Input, "repositoryName")
		var imgs []any
		ids, _ := req.Input["imageIds"].([]any)
		for _, id := range ids {
			m, _ := id.(map[string]any)
			tag := first(m, "imageTag")
			b, ok, _ := p.col(req, "ecrimg:"+repo).Get(ctx, tag)
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			imgs = append(imgs, rec)
		}
		return &spi.Response{Output: map[string]any{"images": imgs, "failures": []any{}}}, nil
	case "ListImages":
		repo := first(req.Input, "repositoryName")
		kvs, _, _ := p.col(req, "ecrimg:"+repo).List(ctx, "", "", 0)
		var ids []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			ids = append(ids, map[string]any{"imageTag": rec["imageTag"], "imageDigest": rec["imageDigest"]})
		}
		return &spi.Response{Output: map[string]any{"imageIds": ids}}, nil
	case "BatchDeleteImage":
		repo := first(req.Input, "repositoryName")
		ids, _ := req.Input["imageIds"].([]any)
		for _, id := range ids {
			m, _ := id.(map[string]any)
			_ = p.col(req, "ecrimg:"+repo).Delete(ctx, first(m, "imageTag"))
		}
		return &spi.Response{Output: map[string]any{"imageIds": ids, "failures": []any{}}}, nil
	case "GetAuthorizationToken":
		tok := p.deps.Rand.Hex(16)
		return &spi.Response{Output: map[string]any{"authorizationData": []any{map[string]any{"authorizationToken": tok, "proxyEndpoint": "https://" + acct + ".dkr.ecr." + region + ".amazonaws.com"}}}}, nil
	case "SetRepositoryPolicy":
		_ = p.col(req, "ecrpol").Put(ctx, first(req.Input, "repositoryName"), []byte(first(req.Input, "policyText")))
		return &spi.Response{Output: map[string]any{"policyText": first(req.Input, "policyText")}}, nil
	case "GetRepositoryPolicy":
		b, ok, _ := p.col(req, "ecrpol").Get(ctx, first(req.Input, "repositoryName"))
		if !ok {
			return nil, &spi.Fault{Code: "RepositoryPolicyNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"policyText": string(b)}}, nil
	case "DeleteRepositoryPolicy":
		_ = p.col(req, "ecrpol").Delete(ctx, first(req.Input, "repositoryName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["tags"])
		_ = p.col(req, "ecrtags").Put(ctx, first(req.Input, "resourceArn"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "ecrtags").Get(ctx, first(req.Input, "resourceArn"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	case "UntagResource":
		_ = p.col(req, "ecrtags").Delete(ctx, first(req.Input, "resourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
