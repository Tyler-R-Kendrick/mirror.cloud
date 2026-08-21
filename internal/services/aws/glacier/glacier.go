// Package glacier stores vaults and archives (no retrieval delay or tape).
package glacier

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.glacier", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Glacier-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.glacier" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateVault", "DescribeVault", "ListVaults", "DeleteVault",
		"UploadArchive", "DeleteArchive",
		"InitiateJob", "DescribeJob", "ListJobs",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	vault := first(req.Input, "vaultName", "VaultName")
	switch req.Operation {
	case "CreateVault":
		if vault == "" {
			return nil, &spi.Fault{Code: "InvalidParameter", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"VaultName":        vault,
			"VaultARN":         "arn:aws:glacier:" + req.Identity.Region + ":" + req.Identity.Account + ":vaults/" + vault,
			"NumberOfArchives": 0, "SizeInBytes": 0,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glvault").Put(ctx, vault, b)
		return &spi.Response{Output: map[string]any{"location": "/vaults/" + vault}}, nil
	case "DescribeVault":
		b, ok, _ := p.col(req, "glvault").Get(ctx, vault)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListVaults":
		return listWrap(ctx, p.col(req, "glvault"), "VaultList")
	case "DeleteVault":
		_ = p.col(req, "glvault").Delete(ctx, vault)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UploadArchive":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"ArchiveId": id, "VaultName": vault, "Checksum": first(req.Input, "checksum", "Checksum")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glarch:"+vault).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"archiveId": id, "location": "/vaults/" + vault + "/archives/" + id}}, nil
	case "DeleteArchive":
		_ = p.col(req, "glarch:"+vault).Delete(ctx, first(req.Input, "archiveId", "ArchiveId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "InitiateJob":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"JobId": id, "VaultName": vault, "StatusCode": "Succeeded", "Action": first(req.Input, "Type", "Action")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gljob:"+vault).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"jobId": id, "location": "/vaults/" + vault + "/jobs/" + id}}, nil
	case "DescribeJob":
		id := first(req.Input, "jobId", "JobId")
		b, ok, _ := p.col(req, "gljob:"+vault).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListJobs":
		return listWrap(ctx, p.col(req, "gljob:"+vault), "JobList")
	default:
		return nil, spi.NotImplemented("aws.glacier", req.Operation, "emulate")
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
