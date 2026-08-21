// Package qldb stores ledgers and a journal of statements (not PartiQL, no hash-chained ledger).
package qldb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.qldb", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements QLDB-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.qldb" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateLedger", "DescribeLedger", "ListLedgers", "UpdateLedger", "DeleteLedger",
		"GetDigest", "GetBlock", "SendCommand",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreateLedger", "UpdateLedger":
		if name == "" {
			return nil, &spi.Fault{Code: "InvalidParameterException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"Name": name, "State": "ACTIVE",
			"Arn": "arn:aws:qldb:" + req.Identity.Region + ":" + req.Identity.Account + ":ledger/" + name,
		}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["Name"] = name
		rec["State"] = "ACTIVE"
		b, _ := json.Marshal(rec)
		_ = p.col(req, "qldb").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeLedger":
		b, ok, _ := p.col(req, "qldb").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListLedgers":
		kvs, _, _ := p.col(req, "qldb").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Ledgers": items}}, nil
	case "DeleteLedger":
		_ = p.col(req, "qldb").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetDigest":
		kvs, _, _ := p.col(req, "qlj:"+name).List(ctx, "", "", 0)
		h := sha256.Sum256([]byte{byte(len(kvs))})
		return &spi.Response{Output: map[string]any{"Digest": hex.EncodeToString(h[:])}}, nil
	case "GetBlock":
		id := first(req.Input, "BlockAddress", "Id")
		b, ok, _ := p.col(req, "qlj:"+name).Get(ctx, id)
		if !ok {
			return &spi.Response{Output: map[string]any{"Block": map[string]any{}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Block": rec}}, nil
	case "SendCommand":
		// ponytail: session/txn ids only; ExecuteStatement appends the statement text, not PartiQL.
		sess := first(req.Input, "SessionToken")
		if sess == "" {
			sess = p.deps.Rand.Hex(8)
		}
		out := map[string]any{"SessionToken": sess}
		if st, ok := req.Input["StartTransaction"].(map[string]any); ok || req.Input["StartTransaction"] != nil {
			_ = st
			tid := p.deps.Rand.Hex(8)
			out["StartTransaction"] = map[string]any{"TransactionId": tid}
		}
		if ex, ok := req.Input["ExecuteStatement"].(map[string]any); ok {
			stmt := first(ex, "Statement")
			id := p.deps.Rand.Hex(8)
			rec := map[string]any{"Id": id, "Statement": stmt}
			b, _ := json.Marshal(rec)
			ledger := first(req.Input, "LedgerName", "Name")
			_ = p.col(req, "qlj:"+ledger).Put(ctx, id, b)
			out["ExecuteStatement"] = map[string]any{"FirstPage": map[string]any{"Values": []any{rec}}}
		}
		if req.Input["StartSession"] != nil {
			if m, ok := req.Input["StartSession"].(map[string]any); ok {
				if n := first(m, "LedgerName"); n != "" {
					out["LedgerName"] = n
				}
			}
			out["StartSession"] = map[string]any{"SessionToken": sess}
		}
		if req.Input["CommitTransaction"] != nil {
			out["CommitTransaction"] = map[string]any{"TransactionId": p.deps.Rand.Hex(8), "CommitDigest": p.deps.Rand.Hex(16)}
		}
		return &spi.Response{Output: out}, nil
	default:
		return nil, spi.NotImplemented("aws.qldb", req.Operation, "emulate")
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
