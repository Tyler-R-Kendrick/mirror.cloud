// Package iot stores things, policies, rules, jobs, and certificates (no device MQTT broker).
package iot

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.iot", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements IoT Core-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.iot" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateThing", "DescribeThing", "ListThings", "UpdateThing", "DeleteThing",
		"CreateThingType", "DescribeThingType", "ListThingTypes", "DeleteThingType",
		"CreatePolicy", "GetPolicy", "ListPolicies", "DeletePolicy",
		"CreateTopicRule", "GetTopicRule", "ListTopicRules", "DeleteTopicRule",
		"CreateJob", "DescribeJob", "ListJobs", "DeleteJob",
		"CreateKeysAndCertificate", "DescribeCertificate", "ListCertificates", "DeleteCertificate",
		"AttachThingPrincipal", "ListThingPrincipals", "DetachThingPrincipal",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateThing":
		name := first(req.Input, "thingName")
		arn := "arn:aws:iot:" + req.Identity.Region + ":" + req.Identity.Account + ":thing/" + name
		rec := map[string]any{"thingName": name, "thingArn": arn, "thingTypeName": first(req.Input, "thingTypeName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotthing").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeThing":
		return get(ctx, p.col(req, "iotthing"), first(req.Input, "thingName"))
	case "ListThings":
		return listCol(ctx, p.col(req, "iotthing"), "things")
	case "UpdateThing":
		name := first(req.Input, "thingName")
		b, ok, _ := p.col(req, "iotthing").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if t := first(req.Input, "thingTypeName"); t != "" {
			rec["thingTypeName"] = t
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "iotthing").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteThing":
		_ = p.col(req, "iotthing").Delete(ctx, first(req.Input, "thingName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateThingType":
		name := first(req.Input, "thingTypeName")
		rec := map[string]any{"thingTypeName": name, "thingTypeArn": "arn:aws:iot:" + req.Identity.Region + ":" + req.Identity.Account + ":thingtype/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iottt").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeThingType":
		return get(ctx, p.col(req, "iottt"), first(req.Input, "thingTypeName"))
	case "ListThingTypes":
		return listCol(ctx, p.col(req, "iottt"), "thingTypes")
	case "DeleteThingType":
		_ = p.col(req, "iottt").Delete(ctx, first(req.Input, "thingTypeName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePolicy":
		name := first(req.Input, "policyName")
		rec := map[string]any{"policyName": name, "policyDocument": first(req.Input, "policyDocument"), "policyArn": "arn:aws:iot:" + req.Identity.Region + ":" + req.Identity.Account + ":policy/" + name}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotpol").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetPolicy":
		return get(ctx, p.col(req, "iotpol"), first(req.Input, "policyName"))
	case "ListPolicies":
		return listCol(ctx, p.col(req, "iotpol"), "policies")
	case "DeletePolicy":
		_ = p.col(req, "iotpol").Delete(ctx, first(req.Input, "policyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTopicRule":
		name := first(req.Input, "ruleName")
		rec := map[string]any{"ruleName": name, "topicRulePayload": req.Input["topicRulePayload"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotrule").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTopicRule":
		return get(ctx, p.col(req, "iotrule"), first(req.Input, "ruleName"))
	case "ListTopicRules":
		return listCol(ctx, p.col(req, "iotrule"), "rules")
	case "DeleteTopicRule":
		_ = p.col(req, "iotrule").Delete(ctx, first(req.Input, "ruleName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateJob":
		id := first(req.Input, "jobId")
		rec := map[string]any{"jobId": id, "status": "IN_PROGRESS", "targets": req.Input["targets"], "document": first(req.Input, "document")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iotjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"jobId": id, "jobArn": "arn:aws:iot:" + req.Identity.Region + ":" + req.Identity.Account + ":job/" + id}}, nil
	case "DescribeJob":
		return get(ctx, p.col(req, "iotjob"), first(req.Input, "jobId"))
	case "ListJobs":
		return listCol(ctx, p.col(req, "iotjob"), "jobs")
	case "DeleteJob":
		_ = p.col(req, "iotjob").Delete(ctx, first(req.Input, "jobId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateKeysAndCertificate":
		id := p.deps.Rand.Hex(16)
		arn := "arn:aws:iot:" + req.Identity.Region + ":" + req.Identity.Account + ":cert/" + id
		rec := map[string]any{
			"certificateId": id, "certificateArn": arn, "certificatePem": "-----BEGIN CERTIFICATE-----\nMIRROR\n-----END CERTIFICATE-----",
			"keyPair": map[string]any{"PublicKey": "-----BEGIN PUBLIC KEY-----\nMIRROR\n-----END PUBLIC KEY-----", "PrivateKey": "mirror-private-key-" + id},
		}
		b, _ := json.Marshal(map[string]any{"certificateId": id, "certificateArn": arn, "status": "ACTIVE"})
		_ = p.col(req, "iotcert").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeCertificate":
		return get(ctx, p.col(req, "iotcert"), first(req.Input, "certificateId"))
	case "ListCertificates":
		return listCol(ctx, p.col(req, "iotcert"), "certificates")
	case "DeleteCertificate":
		_ = p.col(req, "iotcert").Delete(ctx, first(req.Input, "certificateId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AttachThingPrincipal":
		thing := first(req.Input, "thingName")
		prin := first(req.Input, "principal")
		b, _ := json.Marshal(map[string]any{"principal": prin})
		_ = p.col(req, "iotprin:"+thing).Put(ctx, prin, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListThingPrincipals":
		kvs, _, _ := p.col(req, "iotprin:"+first(req.Input, "thingName")).List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			items = append(items, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"principals": items}}, nil
	case "DetachThingPrincipal":
		_ = p.col(req, "iotprin:"+first(req.Input, "thingName")).Delete(ctx, first(req.Input, "principal"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.iot", req.Operation, "emulate")
	}
}

func get(ctx context.Context, c spi.Collection, id string) (*spi.Response, error) {
	b, ok, _ := c.Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
