// Package mq stores Amazon MQ brokers, users, and configurations (no ActiveMQ/RabbitMQ process).
package mq

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.mq", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements MQ-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.mq" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateBroker", "DescribeBroker", "ListBrokers", "DeleteBroker", "RebootBroker",
		"CreateUser", "DescribeUser", "ListUsers", "DeleteUser",
		"CreateConfiguration", "DescribeConfiguration", "ListConfigurations", "DeleteConfiguration",
		"DescribeBrokerEngineTypes",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	bid := first(req.Input, "BrokerId")
	switch req.Operation {
	case "CreateBroker":
		id := "b-" + p.deps.Rand.Hex(8)
		name := first(req.Input, "BrokerName")
		arn := "arn:aws:mq:" + req.Identity.Region + ":" + req.Identity.Account + ":broker:" + id
		rec := map[string]any{
			"BrokerId": id, "BrokerName": name, "BrokerArn": arn, "BrokerState": "RUNNING",
			"EngineType": first(req.Input, "EngineType"), "EngineVersion": first(req.Input, "EngineVersion"),
			"HostInstanceType": first(req.Input, "HostInstanceType"),
		}
		if rec["EngineType"] == "" {
			rec["EngineType"] = "ACTIVEMQ"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mq").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"BrokerId": id, "BrokerArn": arn}}, nil
	case "DescribeBroker":
		b, ok, _ := p.col(req, "mq").Get(ctx, bid)
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListBrokers":
		kvs, _, _ := p.col(req, "mq").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"BrokerSummaries": items}}, nil
	case "DeleteBroker":
		_ = p.col(req, "mq").Delete(ctx, bid)
		return &spi.Response{Output: map[string]any{"BrokerId": bid}}, nil
	case "RebootBroker":
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateUser":
		user := first(req.Input, "Username")
		rec := map[string]any{"Username": user, "BrokerId": bid, "ConsoleAccess": req.Input["ConsoleAccess"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mqu:"+bid).Put(ctx, user, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeUser":
		b, ok, _ := p.col(req, "mqu:"+bid).Get(ctx, first(req.Input, "Username"))
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListUsers":
		kvs, _, _ := p.col(req, "mqu:"+bid).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Users": items}}, nil
	case "DeleteUser":
		_ = p.col(req, "mqu:"+bid).Delete(ctx, first(req.Input, "Username"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateConfiguration":
		id := "c-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "EngineType": first(req.Input, "EngineType"), "Arn": "arn:aws:mq:" + req.Identity.Region + ":" + req.Identity.Account + ":configuration:" + id}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "mqc").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeConfiguration":
		b, ok, _ := p.col(req, "mqc").Get(ctx, first(req.Input, "ConfigurationId", "Id"))
		if !ok {
			return nil, &spi.Fault{Code: "NotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListConfigurations":
		kvs, _, _ := p.col(req, "mqc").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Configurations": items}}, nil
	case "DeleteConfiguration":
		_ = p.col(req, "mqc").Delete(ctx, first(req.Input, "ConfigurationId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeBrokerEngineTypes":
		return &spi.Response{Output: map[string]any{"BrokerEngineTypes": []any{
			map[string]any{"EngineType": "ACTIVEMQ"},
			map[string]any{"EngineType": "RABBITMQ"},
		}}}, nil
	default:
		return nil, spi.NotImplemented("aws.mq", req.Operation, "emulate")
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
