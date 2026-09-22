// Package kinesis is the Kinesis record plane: the four operations
// behavior/aws/kinesis lists as native. PutRecord publishes each record to the
// deps.Bus "kinesis" topic that aws.pipes and aws.firehose read, and a shard
// iterator is opaque base64 state with a timestamp scan behind it; the effect
// vocabulary has neither. Streams themselves are the bundle's.
package kinesis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"PutRecord", "PutRecords", "GetShardIterator", "GetRecords"} {
		bundled.RegisterNative("aws.kinesis", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return records{deps}.invoke(ctx, req)
		})
	}
}

type records struct{ deps spi.Deps }

func (p records) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p records) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "StreamName")
	switch req.Operation {
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
		case "AT_TIMESTAMP":
			seq = p.atTimestampSeq(ctx, req, name, asFloat(req.Input["Timestamp"]))
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
	default: // GetRecords
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
	}
}

func (p records) put(ctx context.Context, req *spi.Request, name string, data any, pk string) (*spi.Response, error) {
	if _, ok, _ := p.col(req, "kinesis").Get(ctx, name); !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	seq := p.nextSeq(ctx, req, name)
	seqStr := strconv.Itoa(seq)
	rec := map[string]any{"SequenceNumber": seqStr, "PartitionKey": pk, "Data": data, "ApproximateArrivalTimestamp": float64(p.deps.Clock.Now().UnixMilli()) / 1000}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "kinesis:"+name).Put(ctx, seqStr, b)
	if p.deps.Bus != nil {
		event, _ := json.Marshal(map[string]any{"Account": req.Identity.Account, "Region": req.Identity.Region, "StreamName": name, "Record": rec})
		_ = p.deps.Bus.Publish(ctx, "kinesis", event)
	}
	return &spi.Response{Output: map[string]any{"SequenceNumber": seqStr, "ShardId": "shardId-000000000000"}}, nil
}

func (p records) atTimestampSeq(ctx context.Context, req *spi.Request, name string, timestamp float64) int {
	kvs, _, _ := p.col(req, "kinesis:"+name).List(ctx, "", "", 0)
	seq := p.curSeq(ctx, req, name)
	for _, kv := range kvs {
		var record map[string]any
		if json.Unmarshal(kv.Value, &record) == nil && asFloat(record["ApproximateArrivalTimestamp"]) >= timestamp {
			if n, err := strconv.Atoi(kv.Key); err == nil && n < seq {
				seq = n
			}
		}
	}
	return seq
}

func (p records) curSeq(ctx context.Context, req *spi.Request, name string) int {
	b, ok, _ := p.col(req, "kinesis").Get(ctx, name)
	if !ok {
		return 0
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return asInt(rec["Seq"])
}

func (p records) nextSeq(ctx context.Context, req *spi.Request, name string) int {
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

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case json.Number:
		n, _ := t.Float64()
		return n
	}
	return 0
}
