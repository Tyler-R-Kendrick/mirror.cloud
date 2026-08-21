// Package location stores indexes and geofences (canned geocode, no map tiles).
package location

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.location", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Amazon Location-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.location" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePlaceIndex", "DescribePlaceIndex", "ListPlaceIndexes", "DeletePlaceIndex",
		"SearchPlaceIndexForText",
		"CreateGeofenceCollection", "DescribeGeofenceCollection", "DeleteGeofenceCollection",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreatePlaceIndex":
		name := first(req.Input, "IndexName")
		rec := map[string]any{"IndexName": name, "DataSource": first(req.Input, "DataSource"), "IndexArn": "arn:aws:geo:" + req.Identity.Region + ":" + req.Identity.Account + ":place-index/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "geoix").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"IndexName": name, "IndexArn": rec["IndexArn"]}}, nil
	case "DescribePlaceIndex":
		return getBare(ctx, p.col(req, "geoix"), first(req.Input, "IndexName"))
	case "ListPlaceIndexes":
		return listWrap(ctx, p.col(req, "geoix"), "Entries")
	case "DeletePlaceIndex":
		_ = p.col(req, "geoix").Delete(ctx, first(req.Input, "IndexName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SearchPlaceIndexForText":
		text := first(req.Input, "Text")
		return &spi.Response{Output: map[string]any{"Results": []any{map[string]any{"Place": map[string]any{"Label": text, "Geometry": map[string]any{"Point": []any{-122.3, 47.6}}}}}}}, nil
	case "CreateGeofenceCollection":
		name := first(req.Input, "CollectionName")
		rec := map[string]any{"CollectionName": name, "CollectionArn": "arn:aws:geo:" + req.Identity.Region + ":" + req.Identity.Account + ":geofence-collection/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "geofc").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeGeofenceCollection":
		return getBare(ctx, p.col(req, "geofc"), first(req.Input, "CollectionName"))
	case "DeleteGeofenceCollection":
		_ = p.col(req, "geofc").Delete(ctx, first(req.Input, "CollectionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.location", req.Operation, "emulate")
	}
}

func getBare(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
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
