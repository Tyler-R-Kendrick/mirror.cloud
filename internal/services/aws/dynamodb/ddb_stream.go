package dynamodb

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
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
	if event == "REMOVE" && old == nil || event == "MODIFY" && reflect.DeepEqual(item, old) {
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
	size := streamItemSize(keys)
	if image := asMap(ddb["NewImage"]); image != nil {
		size += streamItemSize(image)
	}
	if image := asMap(ddb["OldImage"]); image != nil {
		size += streamItemSize(image)
	}
	ddb["SizeBytes"] = size
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
	const shardID = "shardId-000000000000"
	shards := []any{map[string]any{
		"ShardId": shardID,
		"SequenceNumberRange": map[string]any{
			"StartingSequenceNumber": "000000000000001",
		},
	}}
	if first(req.Input, "ExclusiveStartShardId") == shardID {
		shards = []any{}
	}
	return &spi.Response{Output: map[string]any{"StreamDescription": map[string]any{
		"StreamArn":      arn,
		"StreamLabel":    td["LatestStreamLabel"],
		"StreamStatus":   status,
		"StreamViewType": view,
		"TableName":      table,
		"KeySchema":      td["KeySchema"],
		"Shards":         shards,
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
	it := fmt.Sprintf("%s|%d|%s", arn, seq, p.deps.Rand.Hex(8))
	return &spi.Response{Output: map[string]any{"ShardIterator": it}}, nil
}

func (p *Pack) getStreamRecords(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	parts := strings.SplitN(str(req.Input["ShardIterator"]), "|", 3)
	if len(parts) != 3 {
		return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
	}
	arn := parts[0]
	table := tableFromStreamARN(arn)
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
	it := fmt.Sprintf("%s|%d|%s", arn, next, parts[2])
	return &spi.Response{Output: map[string]any{"Records": recs, "NextShardIterator": it}}, nil
}

func streamItemSize(item map[string]any) int {
	size := 0
	for name, raw := range item {
		size += len(name) + streamAttributeSize(asMap(raw))
	}
	return size
}

func streamAttributeSize(attribute map[string]any) int {
	for kind, raw := range attribute {
		switch kind {
		case "S", "N":
			return len(str(raw))
		case "B":
			decoded, _ := base64.StdEncoding.DecodeString(str(raw))
			return len(decoded)
		case "BOOL", "NULL":
			return 1
		case "SS", "NS":
			size := 0
			for _, value := range asSlice(raw) {
				size += len(str(value))
			}
			return size
		case "BS":
			size := 0
			for _, value := range asSlice(raw) {
				decoded, _ := base64.StdEncoding.DecodeString(str(value))
				size += len(decoded)
			}
			return size
		case "L":
			size := 0
			for _, value := range asSlice(raw) {
				size += streamAttributeSize(asMap(value))
			}
			return size
		case "M":
			return streamItemSize(asMap(raw))
		}
	}
	return 0
}
