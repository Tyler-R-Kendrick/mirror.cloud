// Package kinesis is a single-shard in-memory emulate of Kinesis Data Streams.
package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.kinesis", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Kinesis-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.kinesis" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateStream", "DeleteStream", "ListStreams", "DescribeStream", "DescribeStreamSummary",
		"PutRecord", "PutRecords", "GetShardIterator", "GetRecords", "ListShards",
		"AddTagsToStream", "ListTagsForStream", "RemoveTagsFromStream",
		"TagResource", "UntagResource", "ListTagsForResource",
		"IncreaseStreamRetentionPeriod", "DecreaseStreamRetentionPeriod",
		"PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"RegisterStreamConsumer", "DeregisterStreamConsumer", "DescribeStreamConsumer", "ListStreamConsumers",
		"SubscribeToShard",
		"EnableEnhancedMonitoring", "DisableEnhancedMonitoring",
		"SplitShard", "MergeShards", "UpdateShardCount",
		"StartStreamEncryption", "StopStreamEncryption",
		"DescribeLimits", "DescribeAccountSettings", "UpdateAccountSettings",
		"UpdateMaxRecordSize", "UpdateStreamMode", "UpdateStreamWarmThroughput"}
	return core
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "StreamName")
	switch req.Operation {
	case "CreateStream":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"StreamName": name, "Status": "ACTIVE", "Seq": 0}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "kinesis").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteStream":
		_ = p.col(req, "kinesis").Delete(ctx, name)
		kvs, _, _ := p.col(req, "kinesis:"+name).List(ctx, "", "", 0)
		for _, kv := range kvs {
			_ = p.col(req, "kinesis:"+name).Delete(ctx, kv.Key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListStreams":
		kvs, _, _ := p.col(req, "kinesis").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"StreamNames": names, "HasMoreStreams": false}}, nil
	case "DescribeStream", "DescribeStreamSummary":
		b, ok, _ := p.col(req, "kinesis").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		arn := "arn:aws:kinesis:" + req.Identity.Region + ":" + req.Identity.Account + ":stream/" + name
		desc := map[string]any{
			"StreamName": name, "StreamARN": arn, "StreamStatus": "ACTIVE",
			"Shards": []any{map[string]any{
				"ShardId":             "shardId-000000000000",
				"HashKeyRange":        map[string]any{"StartingHashKey": "0", "EndingHashKey": "340282366920938463463374607431768211455"},
				"SequenceNumberRange": map[string]any{"StartingSequenceNumber": "0"},
			}},
		}
		if req.Operation == "DescribeStreamSummary" {
			return &spi.Response{Output: map[string]any{"StreamDescriptionSummary": map[string]any{"StreamName": name, "StreamARN": arn, "StreamStatus": "ACTIVE", "OpenShardCount": 1}}}, nil
		}
		_ = rec
		return &spi.Response{Output: map[string]any{"StreamDescription": desc}}, nil
	case "ListShards":
		return &spi.Response{Output: map[string]any{"Shards": []any{map[string]any{"ShardId": "shardId-000000000000"}}}}, nil
	case "PutRecord":
		return p.put(ctx, req, name, req.Input["Data"], str(req.Input["PartitionKey"]))
	case "PutRecords":
		recs, _ := req.Input["Records"].([]any)
		var out []any
		for _, r := range recs {
			m, _ := r.(map[string]any)
			resp, err := p.put(ctx, req, name, m["Data"], str(m["PartitionKey"]))
			if err != nil {
				out = append(out, map[string]any{"ErrorCode": "InternalFailure"})
				continue
			}
			out = append(out, resp.Output)
		}
		return &spi.Response{Output: map[string]any{"Records": out, "FailedRecordCount": 0}}, nil
	case "GetShardIterator":
		seq := 0
		switch str(req.Input["ShardIteratorType"]) {
		case "LATEST":
			seq = p.curSeq(ctx, req, name)
		case "AT_SEQUENCE_NUMBER":
			seq, _ = strconv.Atoi(str(req.Input["StartingSequenceNumber"]))
		case "AFTER_SEQUENCE_NUMBER":
			seq, _ = strconv.Atoi(str(req.Input["StartingSequenceNumber"]))
			seq++
		default: // TRIM_HORIZON
			seq = 0
		}
		it := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%d", name, seq)))
		return &spi.Response{Output: map[string]any{"ShardIterator": it}}, nil
	case "GetRecords":
		raw, err := base64.StdEncoding.DecodeString(str(req.Input["ShardIterator"]))
		if err != nil {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		parts := strings.SplitN(string(raw), "|", 2)
		if len(parts) != 2 {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		stream := parts[0]
		start, _ := strconv.Atoi(parts[1])
		limit := asInt(req.Input["Limit"])
		if limit <= 0 {
			limit = 1000
		}
		kvs, _, _ := p.col(req, "kinesis:"+stream).List(ctx, "", "", 0)
		type rec struct {
			n int
			m map[string]any
		}
		var all []rec
		for _, kv := range kvs {
			n, _ := strconv.Atoi(kv.Key)
			if n < start {
				continue
			}
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			all = append(all, rec{n, m})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].n < all[j].n })
		var recs []any
		maxSeq := start
		for _, r := range all {
			recs = append(recs, r.m)
			if r.n+1 > maxSeq {
				maxSeq = r.n + 1
			}
			if len(recs) >= limit {
				break
			}
		}
		next := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%d", stream, maxSeq)))
		return &spi.Response{Output: map[string]any{"Records": recs, "NextShardIterator": next, "MillisBehindLatest": 0}}, nil
	default:
		return p.extra(ctx, req, name)
	}
}

func (p *Pack) put(ctx context.Context, req *spi.Request, name string, data any, pk string) (*spi.Response, error) {
	if _, ok, _ := p.col(req, "kinesis").Get(ctx, name); !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	seq := p.nextSeq(ctx, req, name)
	seqStr := strconv.Itoa(seq)
	rec := map[string]any{"SequenceNumber": seqStr, "PartitionKey": pk, "Data": data, "ApproximateArrivalTimestamp": 0}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "kinesis:"+name).Put(ctx, seqStr, b)
	return &spi.Response{Output: map[string]any{"SequenceNumber": seqStr, "ShardId": "shardId-000000000000"}}, nil
}

func (p *Pack) curSeq(ctx context.Context, req *spi.Request, name string) int {
	b, ok, _ := p.col(req, "kinesis").Get(ctx, name)
	if !ok {
		return 0
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return asInt(rec["Seq"])
}

func (p *Pack) nextSeq(ctx context.Context, req *spi.Request, name string) int {
	b, ok, _ := p.col(req, "kinesis").Get(ctx, name)
	var rec map[string]any
	if ok {
		_ = json.Unmarshal(b, &rec)
	} else {
		rec = map[string]any{"StreamName": name, "Status": "ACTIVE"}
	}
	n := asInt(rec["Seq"])
	rec["Seq"] = n + 1
	raw, _ := json.Marshal(rec)
	_ = p.col(req, "kinesis").Put(ctx, name, raw)
	return n
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case float64:
		return int(t)
	case json.Number:
		n, _ := t.Int64()
		return int(n)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	}
	return 0
}
