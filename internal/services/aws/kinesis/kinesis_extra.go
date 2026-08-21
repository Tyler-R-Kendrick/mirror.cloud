package kinesis

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) extra(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	if name == "" {
		name = first(req.Input, "StreamARN", "StreamArn", "ResourceARN", "ResourceArn")
		if i := strings.LastIndex(name, "/"); i >= 0 {
			name = name[i+1:]
		}
	}
	switch req.Operation {
	case "AddTagsToStream", "ListTagsForStream", "RemoveTagsFromStream",
		"TagResource", "UntagResource", "ListTagsForResource":
		return p.tags(ctx, req, name)
	case "IncreaseStreamRetentionPeriod", "DecreaseStreamRetentionPeriod":
		return p.retention(ctx, req, name)
	case "PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy":
		return p.policy(ctx, req, name)
	case "RegisterStreamConsumer", "DeregisterStreamConsumer", "DescribeStreamConsumer", "ListStreamConsumers":
		return p.consumers(ctx, req, name)
	case "SubscribeToShard":
		return &spi.Response{Output: map[string]any{"EventStream": []any{}}}, nil
	case "EnableEnhancedMonitoring", "DisableEnhancedMonitoring":
		return p.monitoring(ctx, req, name)
	case "SplitShard", "MergeShards", "UpdateShardCount":
		return p.shards(ctx, req, name)
	case "StartStreamEncryption", "StopStreamEncryption":
		return p.encryption(ctx, req, name)
	case "DescribeLimits":
		return &spi.Response{Output: map[string]any{"ShardLimit": 500, "OpenShardCount": 1, "OnDemandStreamCount": 0, "OnDemandStreamCountLimit": 50}}, nil
	case "DescribeAccountSettings", "UpdateAccountSettings":
		return p.account(ctx, req)
	case "UpdateMaxRecordSize", "UpdateStreamMode", "UpdateStreamWarmThroughput":
		return p.streamOpts(ctx, req, name)
	default:
		return nil, spi.NotImplemented("aws.kinesis", req.Operation, "emulate")
	}
}

func (p *Pack) loadStream(ctx context.Context, req *spi.Request, name string) map[string]any {
	b, ok, _ := p.col(req, "kinesis").Get(ctx, name)
	rec := map[string]any{"StreamName": name, "Status": "ACTIVE"}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	return rec
}

func (p *Pack) saveStream(ctx context.Context, req *spi.Request, name string, rec map[string]any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, "kinesis").Put(ctx, name, b)
}

func (p *Pack) tags(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	key := name
	if r := first(req.Input, "ResourceARN", "ResourceArn"); r != "" {
		key = r
	}
	col := p.col(req, "ktags")
	cur := map[string]any{}
	if b, ok, _ := col.Get(ctx, key); ok {
		_ = json.Unmarshal(b, &cur)
	}
	switch req.Operation {
	case "AddTagsToStream", "TagResource":
		tags := asMap(req.Input["Tags"])
		for k, v := range tags {
			cur[k] = v
		}
		b, _ := json.Marshal(cur)
		_ = col.Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemoveTagsFromStream", "UntagResource":
		for _, t := range asSlice(req.Input["TagKeys"]) {
			delete(cur, str(t))
		}
		b, _ := json.Marshal(cur)
		_ = col.Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return &spi.Response{Output: map[string]any{"Tags": cur}}, nil
	}
}

func (p *Pack) retention(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	rec := p.loadStream(ctx, req, name)
	h := asInt(req.Input["RetentionPeriodHours"])
	if h <= 0 {
		h = 24
	}
	rec["RetentionPeriodHours"] = h
	p.saveStream(ctx, req, name, rec)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) policy(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	arn := first(req.Input, "ResourceARN", "ResourceArn")
	if arn == "" {
		arn = name
	}
	col := p.col(req, "kpol")
	switch req.Operation {
	case "PutResourcePolicy":
		_ = col.Put(ctx, arn, []byte(str(req.Input["Policy"])))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteResourcePolicy":
		_ = col.Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return &spi.Response{Output: map[string]any{"Policy": ""}}, nil
		}
		return &spi.Response{Output: map[string]any{"Policy": string(b)}}, nil
	}
}

func (p *Pack) consumers(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	col := p.col(req, "kcons")
	switch req.Operation {
	case "RegisterStreamConsumer":
		cn := str(req.Input["ConsumerName"])
		arn := "arn:aws:kinesis:" + req.Identity.Region + ":" + req.Identity.Account + ":stream/" + name + "/consumer/" + cn
		rec := map[string]any{"ConsumerName": cn, "ConsumerARN": arn, "ConsumerStatus": "ACTIVE", "StreamARN": req.Input["StreamARN"]}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Consumer": rec}}, nil
	case "DeregisterStreamConsumer":
		_ = col.Delete(ctx, first(req.Input, "ConsumerARN", "ConsumerArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListStreamConsumers":
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Consumers": out}}, nil
	default:
		arn := first(req.Input, "ConsumerARN", "ConsumerArn")
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"ConsumerDescription": rec}}, nil
	}
}

func (p *Pack) monitoring(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	rec := p.loadStream(ctx, req, name)
	on := strings.HasPrefix(req.Operation, "Enable")
	rec["EnhancedMonitoring"] = on
	p.saveStream(ctx, req, name, rec)
	return &spi.Response{Output: map[string]any{"StreamName": name, "CurrentShardLevelMetrics": req.Input["ShardLevelMetrics"]}}, nil
}

func (p *Pack) shards(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	rec := p.loadStream(ctx, req, name)
	n := asInt(rec["OpenShardCount"])
	if n <= 0 {
		n = 1
	}
	switch req.Operation {
	case "SplitShard":
		n++
	case "MergeShards":
		if n > 1 {
			n--
		}
	case "UpdateShardCount":
		if t := asInt(req.Input["TargetShardCount"]); t > 0 {
			n = t
		}
	}
	rec["OpenShardCount"] = n
	p.saveStream(ctx, req, name, rec)
	return &spi.Response{Output: map[string]any{"StreamName": name, "CurrentShardCount": n, "TargetShardCount": n}}, nil
}

func (p *Pack) encryption(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	rec := p.loadStream(ctx, req, name)
	if req.Operation == "StartStreamEncryption" {
		rec["EncryptionType"] = "KMS"
		rec["KeyId"] = req.Input["KeyId"]
	} else {
		rec["EncryptionType"] = "NONE"
	}
	p.saveStream(ctx, req, name, rec)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) account(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "kacct")
	if req.Operation == "UpdateAccountSettings" {
		b, _ := json.Marshal(req.Input)
		_ = col.Put(ctx, "default", b)
		return &spi.Response{Output: req.Input}, nil
	}
	b, ok, _ := col.Get(ctx, "default")
	if !ok {
		return &spi.Response{Output: map[string]any{"MinimumThroughputBillingCommitment": map[string]any{"Status": "DISABLED"}}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) streamOpts(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	rec := p.loadStream(ctx, req, name)
	switch req.Operation {
	case "UpdateMaxRecordSize":
		rec["MaxRecordSizeInKiB"] = req.Input["MaxRecordSizeInKiB"]
	case "UpdateStreamMode":
		rec["StreamModeDetails"] = req.Input["StreamModeDetails"]
	case "UpdateStreamWarmThroughput":
		rec["WarmThroughput"] = req.Input["WarmThroughputMiBPerSecond"]
	}
	p.saveStream(ctx, req, name, rec)
	return &spi.Response{Output: rec}, nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}
