// Package logs is CloudWatch Logs emulate (in-memory groups/streams/events).
package logs

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.logs", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudWatch Logs.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.logs" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateLogGroup", "DeleteLogGroup", "DescribeLogGroups",
		"CreateLogStream", "DescribeLogStreams", "DeleteLogStream",
		"PutLogEvents", "GetLogEvents", "FilterLogEvents",
		"PutRetentionPolicy", "DeleteRetentionPolicy",
		"PutSubscriptionFilter", "DescribeSubscriptionFilters", "DeleteSubscriptionFilter",
		"PutMetricFilter", "DescribeMetricFilters", "DeleteMetricFilter",
		"PutResourcePolicy", "DescribeResourcePolicies", "DeleteResourcePolicy",
		"TagLogGroup", "UntagLogGroup", "ListTagsLogGroup",
		"PutDestination", "DescribeDestinations", "DeleteDestination",
		"PutQueryDefinition", "DescribeQueryDefinitions", "DeleteQueryDefinition",
		"StartQuery", "GetQueryResults", "StopQuery",
		"AssociateKmsKey", "DisassociateKmsKey"}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateLogGroup":
		name := first(req.Input, "logGroupName", "LogGroupName")
		_ = p.col(req, "groups").Put(ctx, name, []byte(name))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteLogGroup":
		_ = p.col(req, "groups").Delete(ctx, first(req.Input, "logGroupName", "LogGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeLogGroups":
		kvs, _, _ := p.col(req, "groups").List(ctx, "", "", 0)
		var gs []any
		for _, kv := range kvs {
			gs = append(gs, map[string]any{"logGroupName": kv.Key})
		}
		return &spi.Response{Output: map[string]any{"logGroups": gs}}, nil
	case "CreateLogStream":
		g, s := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "logStreamName", "LogStreamName")
		_ = p.col(req, "streams").Put(ctx, g+"/"+s, []byte(s))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeLogStreams":
		g := first(req.Input, "logGroupName", "LogGroupName")
		kvs, _, _ := p.col(req, "streams").List(ctx, g+"/", "", 0)
		var ss []any
		for _, kv := range kvs {
			ss = append(ss, map[string]any{"logStreamName": strAfter(kv.Key, "/")})
		}
		return &spi.Response{Output: map[string]any{"logStreams": ss}}, nil
	case "PutLogEvents":
		g, s := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "logStreamName", "LogStreamName")
		evs, _ := req.Input["logEvents"].([]any)
		if evs == nil {
			evs, _ = req.Input["LogEvents"].([]any)
		}
		key := g + "/" + s
		var all []any
		if b, ok, _ := p.col(req, "events").Get(ctx, key); ok {
			_ = json.Unmarshal(b, &all)
		}
		all = append(all, evs...)
		raw, _ := json.Marshal(all)
		_ = p.col(req, "events").Put(ctx, key, raw)
		return &spi.Response{Output: map[string]any{"nextSequenceToken": strconv.Itoa(len(all))}}, nil
	case "GetLogEvents":
		g, s := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "logStreamName", "LogStreamName")
		b, _, _ := p.col(req, "events").Get(ctx, g+"/"+s)
		var evs []any
		_ = json.Unmarshal(b, &evs)
		if evs == nil {
			evs = []any{}
		}
		return &spi.Response{Output: map[string]any{"events": evs}}, nil
	case "FilterLogEvents":
		g := first(req.Input, "logGroupName", "LogGroupName")
		kvs, _, _ := p.col(req, "events").List(ctx, g+"/", "", 0)
		var evs []any
		for _, kv := range kvs {
			var part []any
			_ = json.Unmarshal(kv.Value, &part)
			evs = append(evs, part...)
		}
		return &spi.Response{Output: map[string]any{"events": evs}}, nil
	case "DeleteLogStream":
		g, s := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "logStreamName", "LogStreamName")
		_ = p.col(req, "streams").Delete(ctx, g+"/"+s)
		_ = p.col(req, "events").Delete(ctx, g+"/"+s)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutRetentionPolicy":
		g := first(req.Input, "logGroupName", "LogGroupName")
		days := req.Input["retentionInDays"]
		if days == nil {
			days = req.Input["RetentionInDays"]
		}
		b, _ := json.Marshal(map[string]any{"retentionInDays": days})
		_ = p.col(req, "retention").Put(ctx, g, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteRetentionPolicy":
		_ = p.col(req, "retention").Delete(ctx, first(req.Input, "logGroupName", "LogGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutSubscriptionFilter":
		g, n := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "filterName", "FilterName")
		rec := map[string]any{"filterName": n, "filterPattern": first(req.Input, "filterPattern", "FilterPattern"), "destinationArn": first(req.Input, "destinationArn", "DestinationArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "subfilter").Put(ctx, g+"/"+n, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeSubscriptionFilters":
		g := first(req.Input, "logGroupName", "LogGroupName")
		return listCol(ctx, p.col(req, "subfilter"), g+"/", "subscriptionFilters")
	case "DeleteSubscriptionFilter":
		g, n := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "filterName", "FilterName")
		_ = p.col(req, "subfilter").Delete(ctx, g+"/"+n)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutMetricFilter":
		g, n := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "filterName", "FilterName")
		rec := map[string]any{"filterName": n, "filterPattern": first(req.Input, "filterPattern", "FilterPattern")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "metricfilter").Put(ctx, g+"/"+n, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeMetricFilters":
		g := first(req.Input, "logGroupName", "LogGroupName")
		return listCol(ctx, p.col(req, "metricfilter"), g+"/", "metricFilters")
	case "DeleteMetricFilter":
		g, n := first(req.Input, "logGroupName", "LogGroupName"), first(req.Input, "filterName", "FilterName")
		_ = p.col(req, "metricfilter").Delete(ctx, g+"/"+n)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutResourcePolicy":
		n := first(req.Input, "policyName", "PolicyName")
		rec := map[string]any{"policyName": n, "policyDocument": first(req.Input, "policyDocument", "PolicyDocument")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "logpol").Put(ctx, n, b)
		return &spi.Response{Output: rec}, nil
	case "DescribeResourcePolicies":
		return listCol(ctx, p.col(req, "logpol"), "", "resourcePolicies")
	case "DeleteResourcePolicy":
		_ = p.col(req, "logpol").Delete(ctx, first(req.Input, "policyName", "PolicyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagLogGroup":
		g := first(req.Input, "logGroupName", "LogGroupName")
		b, _ := json.Marshal(req.Input["tags"])
		if req.Input["tags"] == nil {
			b, _ = json.Marshal(req.Input["Tags"])
		}
		_ = p.col(req, "logtags").Put(ctx, g, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagLogGroup":
		_ = p.col(req, "logtags").Delete(ctx, first(req.Input, "logGroupName", "LogGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsLogGroup":
		g := first(req.Input, "logGroupName", "LogGroupName")
		b, ok, _ := p.col(req, "logtags").Get(ctx, g)
		tags := any(map[string]any{})
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	case "PutDestination":
		n := first(req.Input, "destinationName", "DestinationName")
		rec := map[string]any{"destinationName": n, "targetArn": first(req.Input, "targetArn", "TargetArn"), "roleArn": first(req.Input, "roleArn", "RoleArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "logdest").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"destination": rec}}, nil
	case "DescribeDestinations":
		return listCol(ctx, p.col(req, "logdest"), "", "destinations")
	case "DeleteDestination":
		_ = p.col(req, "logdest").Delete(ctx, first(req.Input, "destinationName", "DestinationName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutQueryDefinition":
		id := first(req.Input, "queryDefinitionId", "QueryDefinitionId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"queryDefinitionId": id, "name": first(req.Input, "name", "Name"), "queryString": first(req.Input, "queryString", "QueryString")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "logqd").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"queryDefinitionId": id}}, nil
	case "DescribeQueryDefinitions":
		return listCol(ctx, p.col(req, "logqd"), "", "queryDefinitions")
	case "DeleteQueryDefinition":
		_ = p.col(req, "logqd").Delete(ctx, first(req.Input, "queryDefinitionId", "QueryDefinitionId"))
		return &spi.Response{Output: map[string]any{"success": true}}, nil
	case "StartQuery":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"queryId": id, "status": "Complete", "queryString": first(req.Input, "queryString", "QueryString")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "logq").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"queryId": id}}, nil
	case "GetQueryResults":
		id := first(req.Input, "queryId", "QueryId")
		b, ok, _ := p.col(req, "logq").Get(ctx, id)
		rec := map[string]any{"status": "Complete", "results": []any{}}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["status"] = "Complete"
			if rec["results"] == nil {
				rec["results"] = []any{}
			}
		}
		return &spi.Response{Output: rec}, nil
	case "StopQuery":
		id := first(req.Input, "queryId", "QueryId")
		if rec, ok, _ := p.col(req, "logq").Get(ctx, id); ok {
			var m map[string]any
			_ = json.Unmarshal(rec, &m)
			m["status"] = "Cancelled"
			nb, _ := json.Marshal(m)
			_ = p.col(req, "logq").Put(ctx, id, nb)
		}
		return &spi.Response{Output: map[string]any{"success": true}}, nil
	case "AssociateKmsKey":
		g := first(req.Input, "logGroupName", "LogGroupName")
		_ = p.col(req, "logkms").Put(ctx, g, []byte(first(req.Input, "kmsKeyId", "KmsKeyId")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DisassociateKmsKey":
		_ = p.col(req, "logkms").Delete(ctx, first(req.Input, "logGroupName", "LogGroupName"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func listCol(ctx context.Context, c spi.Collection, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, pfx, "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func str(v any) string { s, _ := v.(string); return s }

func first(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func strAfter(s, sep string) string {
	if i := lastIndex(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	return s
}

func lastIndex(s, sep string) int {
	n := -1
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			n = i
		}
	}
	return n
}
