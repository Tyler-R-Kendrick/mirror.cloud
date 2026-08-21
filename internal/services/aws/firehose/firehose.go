// Package firehose stores delivery streams and PutRecord writes to an S3 destination (no Redshift/OpenSearch/HTTP).
package firehose

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.firehose", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Firehose-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.firehose" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateDeliveryStream", "DeleteDeliveryStream", "DescribeDeliveryStream", "ListDeliveryStreams",
		"PutRecord", "PutRecordBatch", "UpdateDestination",
		"ListTagsForDeliveryStream", "TagDeliveryStream", "UntagDeliveryStream",
		"StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "DeliveryStreamName", "deliveryStreamName")
	switch req.Operation {
	case "CreateDeliveryStream":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:firehose:" + req.Identity.Region + ":" + req.Identity.Account + ":deliverystream/" + name
		rec := map[string]any{
			"DeliveryStreamName": name, "DeliveryStreamARN": arn, "DeliveryStreamStatus": "ACTIVE",
			"DeliveryStreamType": first(req.Input, "DeliveryStreamType"),
		}
		if rec["DeliveryStreamType"] == "" {
			rec["DeliveryStreamType"] = "DirectPut"
		}
		copyDest(rec, req.Input)
		if err := validateDestination(rec); err != nil {
			return nil, err
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "fh").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"DeliveryStreamARN": arn}}, nil
	case "DeleteDeliveryStream":
		_ = p.col(req, "fh").Delete(ctx, name)
		kvs, _, _ := p.col(req, "fhrec:"+name).List(ctx, "", "", 0)
		for _, kv := range kvs {
			_ = p.col(req, "fhrec:"+name).Delete(ctx, kv.Key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDeliveryStream":
		b, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		kvs, _, _ := p.col(req, "fhrec:"+name).List(ctx, "", "", 0)
		rec["HasMoreDestinations"] = false
		return &spi.Response{Output: map[string]any{"DeliveryStreamDescription": rec, "RecordCount": len(kvs)}}, nil
	case "ListDeliveryStreams":
		kvs, _, _ := p.col(req, "fh").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"DeliveryStreamNames": names, "HasMoreDeliveryStreams": false}}, nil
	case "PutRecord":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		id := p.putOne(ctx, req, name, req.Input["Record"])
		return &spi.Response{Output: map[string]any{"RecordId": id, "Encrypted": false}}, nil
	case "PutRecordBatch":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		recs, _ := req.Input["Records"].([]any)
		var resp []any
		for _, r := range recs {
			id := p.putOne(ctx, req, name, r)
			resp = append(resp, map[string]any{"RecordId": id, "ErrorCode": nil})
		}
		return &spi.Response{Output: map[string]any{"FailedPutCount": 0, "RequestResponses": resp}}, nil
	case "UpdateDestination":
		b, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["Destination"] = req.Input
		copyDest(rec, req.Input)
		if err := validateDestination(rec); err != nil {
			return nil, err
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "fh").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagDeliveryStream":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "fhtag").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagDeliveryStream":
		_ = p.col(req, "fhtag").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForDeliveryStream":
		b, ok, _ := p.col(req, "fhtag").Get(ctx, name)
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags, "HasMoreTags": false}}, nil
	case "StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.firehose", req.Operation, "emulate")
	}
}

func (p *Pack) putOne(ctx context.Context, req *spi.Request, name string, rec any) string {
	id := p.deps.Rand.Hex(16)
	payload := map[string]any{"Record": rec}
	var decoded []byte
	if m, ok := rec.(map[string]any); ok {
		if d, ok := m["Data"].(string); ok {
			if raw, err := base64.StdEncoding.DecodeString(d); err == nil {
				decoded = raw
				payload["Decoded"] = string(raw)
			} else {
				decoded = []byte(d)
			}
		}
	}
	b, _ := json.Marshal(payload)
	_ = p.col(req, "fhrec:"+name).Put(ctx, id, b)
	p.deliverS3(ctx, req, name, id, decoded)
	return id
}

func copyDest(rec, in map[string]any) {
	for _, k := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		if in[k] != nil {
			rec[k] = in[k]
		}
	}
}

func s3Dest(rec map[string]any) (bucket, prefix, timezone string) {
	for _, k := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		m, _ := rec[k].(map[string]any)
		if m == nil {
			continue
		}
		arn := first(m, "BucketARN")
		if arn == "" {
			continue
		}
		bucket = bucketFromARN(arn)
		prefix = first(m, "Prefix")
		timezone = first(m, "CustomTimeZone")
		return bucket, prefix, timezone
	}
	return "", "", ""
}

func validateDestination(rec map[string]any) error {
	_, _, timezone := s3Dest(rec)
	if timezone != "" {
		if _, err := time.LoadLocation(timezone); err != nil {
			return &spi.Fault{Code: "ValidationException", Message: "CustomTimeZone is invalid.", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func bucketFromARN(arn string) string {
	if i := strings.Index(arn, ":::"); i >= 0 {
		return arn[i+3:]
	}
	if i := strings.LastIndexByte(arn, ':'); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func (p *Pack) deliverS3(ctx context.Context, req *spi.Request, stream, recID string, data []byte) {
	raw, ok, _ := p.col(req, "fh").Get(ctx, stream)
	if !ok {
		return
	}
	var rec map[string]any
	_ = json.Unmarshal(raw, &rec)
	bucket, prefix, timezone := s3Dest(rec)
	if bucket == "" {
		return
	}
	now := p.deps.Clock.Now().UTC()
	if timezone != "" {
		location, _ := time.LoadLocation(timezone)
		now = now.In(location)
	}
	key := p.evaluatedS3Prefix(prefix, now) + stream + "-1-" + now.Format("2006-01-02-15-04-05-") + recID
	info, err := p.deps.Blobs.Put(ctx, req.Identity.Account+"/"+req.Identity.Region+"/"+bucket+"/"+key, bytes.NewReader(data))
	if err != nil {
		return
	}
	etag := `"` + info.MD5 + `"`
	mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	meta, _ := json.Marshal(map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "deleteMarker": false})
	_ = p.col(req, "objects").Put(ctx, bucket+"/"+key, meta)
}

var firehoseTimestampPrefix = regexp.MustCompile(`!\{timestamp:([^}]*)\}`)

func (p *Pack) evaluatedS3Prefix(prefix string, now time.Time) string {
	if !firehoseTimestampPrefix.MatchString(prefix) {
		prefix += now.Format("2006/01/02/15/")
	}
	prefix = firehoseTimestampPrefix.ReplaceAllStringFunc(prefix, func(expression string) string {
		pattern := expression[len("!{timestamp:") : len(expression)-1]
		layout := strings.NewReplacer("yyyy", "2006", "MM", "01", "dd", "02", "HH", "15", "mm", "04", "ss", "05").Replace(pattern)
		return now.Format(layout)
	})
	for strings.Contains(prefix, "!{firehose:random-string}") {
		prefix = strings.Replace(prefix, "!{firehose:random-string}", p.deps.Rand.Hex(11), 1)
	}
	return prefix
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
