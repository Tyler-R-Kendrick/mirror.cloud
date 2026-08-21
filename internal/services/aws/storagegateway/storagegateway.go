// Package storagegateway stores gateway and share records (no NFS/iSCSI).
package storagegateway

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.storagegateway", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Storage Gateway-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.storagegateway" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"ActivateGateway", "DescribeGatewayInformation", "ListGateways", "DeleteGateway",
		"CreateNFSFileShare", "DescribeNFSFileShares", "DeleteFileShare",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "ActivateGateway":
		arn := "arn:aws:storagegateway:" + req.Identity.Region + ":" + req.Identity.Account + ":gateway/sgw-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"GatewayARN": arn, "GatewayName": first(req.Input, "GatewayName"), "GatewayType": first(req.Input, "GatewayType"), "GatewayState": "RUNNING"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sgw").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"GatewayARN": arn}}, nil
	case "DescribeGatewayInformation":
		arn := first(req.Input, "GatewayARN")
		b, ok, _ := p.col(req, "sgw").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidGatewayRequestException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListGateways":
		return listWrap(ctx, p.col(req, "sgw"), "Gateways")
	case "DeleteGateway":
		_ = p.col(req, "sgw").Delete(ctx, first(req.Input, "GatewayARN"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateNFSFileShare":
		arn := "arn:aws:storagegateway:" + req.Identity.Region + ":" + req.Identity.Account + ":share/share-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"FileShareARN": arn, "GatewayARN": first(req.Input, "GatewayARN"), "LocationARN": first(req.Input, "LocationARN"), "FileShareStatus": "AVAILABLE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sgwshare").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"FileShareARN": arn}}, nil
	case "DescribeNFSFileShares":
		return listWrap(ctx, p.col(req, "sgwshare"), "NFSFileShareInfoList")
	case "DeleteFileShare":
		_ = p.col(req, "sgwshare").Delete(ctx, first(req.Input, "FileShareARN"))
		return &spi.Response{Output: map[string]any{"FileShareARN": first(req.Input, "FileShareARN")}}, nil
	default:
		return nil, spi.NotImplemented("aws.storagegateway", req.Operation, "emulate")
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
