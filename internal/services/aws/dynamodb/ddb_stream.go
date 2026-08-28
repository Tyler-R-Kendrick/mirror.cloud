package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func streamARN(req *spi.Request, table, label string) string {
	return "arn:aws:dynamodb:" + req.Identity.Region + ":" + req.Identity.Account + ":table/" + table + "/stream/" + label
}

func tableFromStreamARN(arn string) string {
	i := strings.Index(arn, ":table/")
	if i < 0 {
		return arn
	}
	rest := arn[i+len(":table/"):]
	if j := strings.Index(rest, "/stream/"); j >= 0 {
		return rest[:j]
	}
	return rest
}

func (p *Pack) ensureStream(req *spi.Request, rec map[string]any, table string) {
	spec := asMap(rec["StreamSpecification"])
	if !truthy(spec["StreamEnabled"]) {
		return
	}
	if str(rec["LatestStreamArn"]) != "" {
		return
	}
	label := p.deps.Rand.Hex(8)
	rec["LatestStreamLabel"] = label
	rec["LatestStreamArn"] = streamARN(req, table, label)
}

func (p *Pack) streamEnabled(ctx context.Context, req *spi.Request, table string) (map[string]any, bool) {
	td := p.tableDef(ctx, req, table)
	spec := asMap(td["StreamSpecification"])
	return td, truthy(spec["StreamEnabled"])
}

func (p *Pack) emitStream(ctx context.Context, req *spi.Request, table, event string, item, old map[string]any) {
	td, ok := p.streamEnabled(ctx, req, table)
	if !ok {
		return
	}
	view := str(asMap(td["StreamSpecification"])["StreamViewType"])
	if view == "" {
		view = "NEW_AND_OLD_IMAGES"
	}
	seq := p.nextStreamSeq(ctx, req, table)
	keys := p.tableKey(td, item)
	if len(keys) == 0 {
		keys = p.tableKey(td, old)
	}
	ddb := map[string]any{
		"ApproximateCreationDateTime": float64(p.deps.Clock.Now().UnixMilli()) / 1000,
		"Keys":                        keys,
		"SequenceNumber":              fmt.Sprintf("%015d", seq),
		"SizeBytes":                   0,
		"StreamViewType":              view,
	}
	switch view {
	case "KEYS_ONLY":
	case "OLD_IMAGE":
		if old != nil {
			ddb["OldImage"] = old
		}
	case "NEW_IMAGE":
		if event != "REMOVE" && item != nil {
			ddb["NewImage"] = item
		}
	default:
		if event != "REMOVE" && item != nil {
			ddb["NewImage"] = item
		}
		if old != nil {
			ddb["OldImage"] = old
		}
	}
	rec := map[string]any{
		"eventID":      p.deps.Rand.Hex(16),
		"eventName":    event,
		"eventVersion": "1.1",
		"eventSource":  "aws:dynamodb",
		"awsRegion":    req.Identity.Region,
		"dynamodb":     ddb,
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "ddbstream:"+table).Put(ctx, fmt.Sprintf("%015d", seq), b)
	if p.deps.Bus != nil {
		_ = p.deps.Bus.Publish(ctx, "dynamodb-stream", b)
	}
}

func (p *Pack) nextStreamSeq(ctx context.Context, req *spi.Request, table string) int {
	b, ok, _ := p.col(req, "ddbseq").Get(ctx, table)
	n := 0
	if ok {
		n, _ = strconv.Atoi(string(b))
	}
	n++
	_ = p.col(req, "ddbseq").Put(ctx, table, []byte(strconv.Itoa(n)))
	return n
}

func (p *Pack) listStreams(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	want := first(req.Input, "TableName")
	kvs, _, _ := p.col(req, "tables").List(ctx, "", "", 0)
	streams := []any{}
	for _, kv := range kvs {
		if want != "" && kv.Key != want {
			continue
		}
		var td map[string]any
		_ = json.Unmarshal(kv.Value, &td)
		if !truthy(asMap(td["StreamSpecification"])["StreamEnabled"]) {
			continue
		}
		streams = append(streams, map[string]any{
			"StreamArn":   td["LatestStreamArn"],
			"TableName":   kv.Key,
			"StreamLabel": td["LatestStreamLabel"],
		})
	}
	return &spi.Response{Output: map[string]any{"Streams": streams}}, nil
}

func (p *Pack) describeStream(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := first(req.Input, "StreamArn")
	table := tableFromStreamARN(arn)
	td := p.tableDef(ctx, req, table)
	if str(td["TableName"]) == "" && table != "" {
		td["TableName"] = table
	}
	spec := asMap(td["StreamSpecification"])
	view := str(spec["StreamViewType"])
	if view == "" {
		view = "NEW_AND_OLD_IMAGES"
	}
	status := "DISABLED"
	if truthy(spec["StreamEnabled"]) {
		status = "ENABLED"
	}
	return &spi.Response{Output: map[string]any{"StreamDescription": map[string]any{
		"StreamArn":      arn,
		"StreamStatus":   status,
		"StreamViewType": view,
		"TableName":      table,
		"Shards": []any{map[string]any{
			"ShardId": "shardId-000000000000",
			"SequenceNumberRange": map[string]any{
				"StartingSequenceNumber": "000000000000001",
			},
		}},
	}}}, nil
}

func (p *Pack) getShardIterator(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := first(req.Input, "StreamArn")
	table := tableFromStreamARN(arn)
	seq := 1
	switch str(req.Input["ShardIteratorType"]) {
	case "LATEST":
		b, ok, _ := p.col(req, "ddbseq").Get(ctx, table)
		n := 0
		if ok {
			n, _ = strconv.Atoi(string(b))
		}
		seq = n + 1
	case "AT_SEQUENCE_NUMBER":
		seq, _ = strconv.Atoi(str(req.Input["SequenceNumber"]))
	case "AFTER_SEQUENCE_NUMBER":
		seq, _ = strconv.Atoi(str(req.Input["SequenceNumber"]))
		seq++
	default: // TRIM_HORIZON
		seq = 1
	}
	it := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%d", table, seq)))
	return &spi.Response{Output: map[string]any{"ShardIterator": it}}, nil
}

func (p *Pack) getStreamRecords(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	raw, err := base64.StdEncoding.DecodeString(str(req.Input["ShardIterator"]))
	if err != nil {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	table := parts[0]
	start, _ := strconv.Atoi(parts[1])
	limit := asInt(req.Input["Limit"])
	if limit <= 0 {
		limit = 1000
	}
	kvs, _, _ := p.col(req, "ddbstream:"+table).List(ctx, "", "", 0)
	recs := []any{}
	next := start
	for _, kv := range kvs {
		n, _ := strconv.Atoi(kv.Key)
		if n < start {
			continue
		}
		var m map[string]any
		_ = json.Unmarshal(kv.Value, &m)
		recs = append(recs, m)
		next = n + 1
		if len(recs) >= limit {
			break
		}
	}
	it := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s|%d", table, next)))
	return &spi.Response{Output: map[string]any{"Records": recs, "NextShardIterator": it}}, nil
}
