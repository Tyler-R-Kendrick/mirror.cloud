// Package blobs is the emulate-tier Azure Blob Storage REST control plane.
package blobs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "azure.blobs", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements azure.blobs.
type Pack struct {
	deps spi.Deps
	// ponytail: process-wide lock; per-account locks if concurrent create throughput matters.
	mu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "azure.blobs" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateContainer", "GetContainer", "ListContainers", "DeleteContainer", "PutBlob", "GetBlob", "ListBlobs", "DeleteBlob"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	acct := req.Identity.Account
	if acct == "" {
		acct = "000000000000"
	}
	reg := req.Identity.Region
	if reg == "" {
		reg = "us-east-1"
	}
	return p.deps.Store.Scope(acct, reg).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	if req.HTTP != nil {
		hydrate(req)
	}
	switch req.Operation {
	case "CreateContainer":
		return p.createContainer(ctx, req)
	case "GetContainer":
		return p.getContainer(ctx, req)
	case "ListContainers":
		return p.listContainers(ctx, req)
	case "DeleteContainer":
		return p.deleteContainer(ctx, req)
	case "PutBlob":
		return p.putBlob(ctx, req)
	case "GetBlob":
		return p.getBlob(ctx, req)
	case "ListBlobs":
		return p.listBlobs(ctx, req)
	case "DeleteBlob":
		return p.deleteBlob(ctx, req)
	default:
		return nil, spi.NotImplemented("azure.blobs", req.Operation, "emulate")
	}
}

func (p *Pack) createContainer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := strings.ToLower(str(req.Input["container"]))
	if name == "" {
		return nil, azFault("InvalidResourceName", "The specified resource name contains invalid characters.", 400)
	}
	if _, exists, _ := p.col(req, "azctr").Get(ctx, name); exists {
		return nil, azFault("ContainerAlreadyExists", "The specified container already exists.", 409)
	}
	rec := map[string]any{"name": name}
	_ = p.col(req, "azctr").Put(ctx, name, mustJSON(rec))
	return &spi.Response{Status: 201, Output: rec}, nil
}

func (p *Pack) getContainer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := strings.ToLower(str(req.Input["container"]))
	b, ok, _ := p.col(req, "azctr").Get(ctx, name)
	if !ok {
		return nil, azFault("ContainerNotFound", "The specified container does not exist.", 404)
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) listContainers(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "azctr").List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, rec)
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_kind": "containers"}}, nil
}

func (p *Pack) deleteContainer(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getContainer(ctx, req); err != nil {
		return nil, err
	}
	name := strings.ToLower(str(req.Input["container"]))
	_ = p.col(req, "azctr").Delete(ctx, name)
	return &spi.Response{Status: 202}, nil
}

func (p *Pack) putBlob(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getContainer(ctx, req); err != nil {
		return nil, err
	}
	container := strings.ToLower(str(req.Input["container"]))
	name := str(req.Input["blob"])
	if name == "" {
		return nil, azFault("InvalidResourceName", "The specified blob name is invalid.", 400)
	}
	body, _ := io.ReadAll(readerOf(req))
	_, err := p.deps.Blobs.Put(ctx, blobKey(req, container, name), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	rec := map[string]any{"container": container, "name": name}
	_ = p.col(req, "azblob").Put(ctx, container+"/"+name, mustJSON(rec))
	return &spi.Response{Status: 201, Output: rec}, nil
}

func (p *Pack) getBlob(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getContainer(ctx, req); err != nil {
		return nil, err
	}
	container := strings.ToLower(str(req.Input["container"]))
	name := str(req.Input["blob"])
	_, ok, _ := p.col(req, "azblob").Get(ctx, container+"/"+name)
	if !ok {
		return nil, azFault("BlobNotFound", "The specified blob does not exist.", 404)
	}
	var data []byte
	if rc, _, err := p.deps.Blobs.Get(ctx, blobKey(req, container, name)); err == nil {
		data, _ = io.ReadAll(rc)
		_ = rc.Close()
	}
	return &spi.Response{Status: 200, Stream: io.NopCloser(bytes.NewReader(data)), Output: map[string]any{"_raw": string(data)}}, nil
}

func (p *Pack) listBlobs(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getContainer(ctx, req); err != nil {
		return nil, err
	}
	container := strings.ToLower(str(req.Input["container"]))
	kvs, _, _ := p.col(req, "azblob").List(ctx, container+"/", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) == nil {
			items = append(items, rec)
		}
	}
	if items == nil {
		items = []any{}
	}
	return &spi.Response{Output: map[string]any{"_list": items, "_kind": "blobs", "container": container}}, nil
}

func (p *Pack) deleteBlob(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if _, err := p.getContainer(ctx, req); err != nil {
		return nil, err
	}
	container := strings.ToLower(str(req.Input["container"]))
	name := str(req.Input["blob"])
	_, ok, _ := p.col(req, "azblob").Get(ctx, container+"/"+name)
	if !ok {
		return nil, azFault("BlobNotFound", "The specified blob does not exist.", 404)
	}
	_ = p.col(req, "azblob").Delete(ctx, container+"/"+name)
	_ = p.deps.Blobs.Delete(ctx, blobKey(req, container, name))
	return &spi.Response{Status: 202}, nil
}

func hydrate(req *spi.Request) {
	path := strings.Trim(req.HTTP.URL.Path, "/")
	container, blob, _ := strings.Cut(path, "/")
	if path == "" {
		container, blob = "", ""
	}
	if str(req.Input["container"]) == "" && container != "" {
		req.Input["container"] = container
	}
	if str(req.Input["blob"]) == "" && blob != "" {
		req.Input["blob"] = blob
	}
}

func azFault(code, msg string, status int) *spi.Fault {
	return &spi.Fault{Code: code, Message: msg, HTTPStatus: status, Fault: "client"}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func readerOf(req *spi.Request) io.Reader {
	if req.Body != nil {
		return req.Body
	}
	if req.HTTP != nil && req.HTTP.Body != nil {
		return req.HTTP.Body
	}
	return bytes.NewReader(nil)
}

func blobKey(req *spi.Request, c, n string) string {
	return req.Identity.Account + "/" + req.Identity.Region + "/azure/" + c + "/" + n
}
