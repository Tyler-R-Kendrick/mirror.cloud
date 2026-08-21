// Package kafka stores MSK cluster records (no Kafka broker).
package kafka

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.kafka", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MSK-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.kafka" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCluster", "DescribeCluster", "ListClusters", "DeleteCluster",
		"GetBootstrapBrokers", "ListNodes", "UpdateBrokerCount",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := first(req.Input, "ClusterArn")
	switch req.Operation {
	case "CreateCluster":
		name := first(req.Input, "ClusterName")
		id := p.deps.Rand.Hex(8)
		arn = "arn:aws:kafka:" + req.Identity.Region + ":" + req.Identity.Account + ":cluster/" + name + "/" + id
		rec := map[string]any{"ClusterName": name, "ClusterArn": arn, "State": "ACTIVE", "BrokerNodeGroupInfo": req.Input["BrokerNodeGroupInfo"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "msk").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"ClusterArn": arn, "ClusterName": name, "State": "ACTIVE"}}, nil
	case "DescribeCluster":
		b, ok, _ := p.col(req, "msk").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ClusterInfo": rec}}, nil
	case "ListClusters":
		kvs, _, _ := p.col(req, "msk").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"ClusterInfoList": items}}, nil
	case "DeleteCluster":
		_ = p.col(req, "msk").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{"ClusterArn": arn, "State": "DELETING"}}, nil
	case "GetBootstrapBrokers":
		return &spi.Response{Output: map[string]any{"BootstrapBrokerString": "localhost:9092", "BootstrapBrokerStringTls": "localhost:9094"}}, nil
	case "ListNodes":
		return &spi.Response{Output: map[string]any{"NodeInfoList": []any{map[string]any{"BrokerNodeInfo": map[string]any{"BrokerId": "1", "Endpoints": []any{"localhost:9092"}}}}}}, nil
	case "UpdateBrokerCount":
		return &spi.Response{Output: map[string]any{"ClusterArn": arn, "ClusterOperationArn": arn + "/op"}}, nil
	default:
		return nil, spi.NotImplemented("aws.kafka", req.Operation, "emulate")
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
