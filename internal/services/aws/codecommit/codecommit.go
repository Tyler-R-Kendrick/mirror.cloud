// Package codecommit stores repositories, branches, and files (no git daemon).
package codecommit

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.codecommit", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CodeCommit-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.codecommit" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateRepository", "GetRepository", "ListRepositories", "UpdateRepository", "DeleteRepository",
		"CreateBranch", "GetBranch", "ListBranches", "DeleteBranch",
		"PutFile", "GetFile", "DeleteFile",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "repositoryName", "RepositoryName")
	switch req.Operation {
	case "CreateRepository", "UpdateRepository":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidRepositoryNameException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"repositoryName": name,
			"repositoryId":   p.deps.Rand.Hex(8),
			"arn":            "arn:aws:codecommit:" + req.Identity.Region + ":" + req.Identity.Account + ":" + name,
			"cloneUrlHttp":   "http://127.0.0.1/codecommit/" + name,
		}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["repositoryName"] = name
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ccrepo").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"repositoryMetadata": rec}}, nil
	case "GetRepository":
		b, ok, _ := p.col(req, "ccrepo").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "RepositoryDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"repositoryMetadata": rec}}, nil
	case "ListRepositories":
		kvs, _, _ := p.col(req, "ccrepo").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, map[string]any{"repositoryName": rec["repositoryName"]})
		}
		return &spi.Response{Output: map[string]any{"repositories": items}}, nil
	case "DeleteRepository":
		_ = p.col(req, "ccrepo").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateBranch":
		br := first(req.Input, "branchName", "BranchName")
		rec := map[string]any{"branchName": br, "commitId": first(req.Input, "commitId", "CommitId")}
		if rec["commitId"] == "" {
			rec["commitId"] = p.deps.Rand.Hex(8)
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ccbr:"+name).Put(ctx, br, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetBranch":
		br := first(req.Input, "branchName", "BranchName")
		b, ok, _ := p.col(req, "ccbr:"+name).Get(ctx, br)
		if !ok {
			return nil, &spi.Fault{Code: "BranchDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"branch": rec}}, nil
	case "ListBranches":
		kvs, _, _ := p.col(req, "ccbr:"+name).List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"branches": names}}, nil
	case "DeleteBranch":
		_ = p.col(req, "ccbr:"+name).Delete(ctx, first(req.Input, "branchName", "BranchName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutFile":
		path := first(req.Input, "filePath", "FilePath")
		rec := map[string]any{"filePath": path, "commitId": p.deps.Rand.Hex(8), "blobId": p.deps.Rand.Hex(8)}
		if v := req.Input["fileContent"]; v != nil {
			rec["fileContent"] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ccfile:"+name).Put(ctx, path, b)
		return &spi.Response{Output: rec}, nil
	case "GetFile":
		path := first(req.Input, "filePath", "FilePath")
		b, ok, _ := p.col(req, "ccfile:"+name).Get(ctx, path)
		if !ok {
			return nil, &spi.Fault{Code: "FileDoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteFile":
		path := first(req.Input, "filePath", "FilePath")
		_ = p.col(req, "ccfile:"+name).Delete(ctx, path)
		return &spi.Response{Output: map[string]any{"commitId": p.deps.Rand.Hex(8)}}, nil
	default:
		return nil, spi.NotImplemented("aws.codecommit", req.Operation, "emulate")
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
