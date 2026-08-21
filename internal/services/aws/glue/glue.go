// Package glue stores Glue catalogs, tables, jobs, and crawlers (no Spark).
package glue

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.glue", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Glue-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.glue" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDatabase", "GetDatabase", "GetDatabases", "UpdateDatabase", "DeleteDatabase",
		"CreateTable", "GetTable", "GetTables", "UpdateTable", "DeleteTable",
		"CreateJob", "GetJob", "GetJobs", "DeleteJob", "StartJobRun", "GetJobRun", "GetJobRuns",
		"CreateCrawler", "GetCrawler", "GetCrawlers", "DeleteCrawler",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateDatabase":
		in, _ := req.Input["DatabaseInput"].(map[string]any)
		name := first(in, "Name")
		if name == "" {
			name = first(req.Input, "Name")
		}
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidInputException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"Name": name, "Description": first(in, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gldb").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetDatabase":
		name := first(req.Input, "Name")
		b, ok, _ := p.col(req, "gldb").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Database": rec}}, nil
	case "GetDatabases":
		return listWrap(ctx, p.col(req, "gldb"), "DatabaseList")
	case "UpdateDatabase":
		name := first(req.Input, "Name")
		b, ok, _ := p.col(req, "gldb").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if in, ok := req.Input["DatabaseInput"].(map[string]any); ok {
			if d := first(in, "Description"); d != "" {
				rec["Description"] = d
			}
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "gldb").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteDatabase":
		_ = p.col(req, "gldb").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTable":
		db := first(req.Input, "DatabaseName")
		in, _ := req.Input["TableInput"].(map[string]any)
		name := first(in, "Name")
		rec := map[string]any{"Name": name, "DatabaseName": db, "StorageDescriptor": in["StorageDescriptor"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gltbl:"+db).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTable":
		db, name := first(req.Input, "DatabaseName"), first(req.Input, "Name")
		b, ok, _ := p.col(req, "gltbl:"+db).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Table": rec}}, nil
	case "GetTables":
		return listWrap(ctx, p.col(req, "gltbl:"+first(req.Input, "DatabaseName")), "TableList")
	case "UpdateTable":
		db, in := first(req.Input, "DatabaseName"), map[string]any{}
		if m, ok := req.Input["TableInput"].(map[string]any); ok {
			in = m
		}
		name := first(in, "Name")
		b, _ := json.Marshal(map[string]any{"Name": name, "DatabaseName": db, "StorageDescriptor": in["StorageDescriptor"]})
		_ = p.col(req, "gltbl:"+db).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteTable":
		_ = p.col(req, "gltbl:"+first(req.Input, "DatabaseName")).Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateJob":
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "Role": first(req.Input, "Role"), "Command": req.Input["Command"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "gljob").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"Name": name}}, nil
	case "GetJob":
		name := first(req.Input, "JobName", "Name")
		b, ok, _ := p.col(req, "gljob").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Job": rec}}, nil
	case "GetJobs":
		return listWrap(ctx, p.col(req, "gljob"), "Jobs")
	case "DeleteJob":
		_ = p.col(req, "gljob").Delete(ctx, first(req.Input, "JobName", "Name"))
		return &spi.Response{Output: map[string]any{"JobName": first(req.Input, "JobName")}}, nil
	case "StartJobRun":
		job := first(req.Input, "JobName")
		id := "jr-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "JobName": job, "JobRunState": "SUCCEEDED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "glrun:"+job).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"JobRunId": id}}, nil
	case "GetJobRun":
		job, id := first(req.Input, "JobName"), first(req.Input, "RunId", "JobRunId")
		b, ok, _ := p.col(req, "glrun:"+job).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"JobRun": rec}}, nil
	case "GetJobRuns":
		return listWrap(ctx, p.col(req, "glrun:"+first(req.Input, "JobName")), "JobRuns")
	case "CreateCrawler":
		name := first(req.Input, "Name")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "glcr").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetCrawler":
		name := first(req.Input, "Name")
		b, ok, _ := p.col(req, "glcr").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "EntityNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Crawler": rec}}, nil
	case "GetCrawlers":
		return listWrap(ctx, p.col(req, "glcr"), "Crawlers")
	case "DeleteCrawler":
		_ = p.col(req, "glcr").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.glue", req.Operation, "emulate")
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
