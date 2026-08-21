// Package iotwireless stores destination and device records (no LoRaWAN).
package iotwireless

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.iotwireless", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements IoT Wireless-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.iotwireless" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDestination", "GetDestination", "ListDestinations", "DeleteDestination",
		"CreateWirelessDevice", "GetWirelessDevice",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "DestinationName")
	switch req.Operation {
	case "CreateDestination":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:iotwireless:" + req.Identity.Region + ":" + req.Identity.Account + ":Destination/" + name
		rec := map[string]any{"Name": name, "Arn": arn, "Expression": first(req.Input, "Expression"), "RoleArn": first(req.Input, "RoleArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotwdst").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetDestination":
		b, ok, _ := p.col(req, "iotwdst").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListDestinations":
		return listWrap(ctx, p.col(req, "iotwdst"), "DestinationList")
	case "DeleteDestination":
		_ = p.col(req, "iotwdst").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateWirelessDevice":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Type": first(req.Input, "Type"), "DestinationName": first(req.Input, "DestinationName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotwdev").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"Id": id}}, nil
	case "GetWirelessDevice":
		id := first(req.Input, "Identifier", "Id")
		b, ok, _ := p.col(req, "iotwdev").Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.iotwireless", req.Operation, "emulate")
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
