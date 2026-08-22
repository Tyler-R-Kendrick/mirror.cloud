// Package firehose stores delivery streams and PutRecord writes to an S3 destination (no Redshift/OpenSearch/HTTP).
package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
	"math"
	"net/http"
	"regexp"
	"strconv"
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

const destinationID = "destinationId-000000000001"

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
		if len(name) > 64 || !firehoseStreamName.MatchString(name) {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:firehose:" + req.Identity.Region + ":" + req.Identity.Account + ":deliverystream/" + name
		rec := map[string]any{
			"DeliveryStreamName": name, "DeliveryStreamARN": arn, "DeliveryStreamStatus": "ACTIVE",
			"DeliveryStreamType": first(req.Input, "DeliveryStreamType"), "VersionId": "1",
		}
		if rec["DeliveryStreamType"] == "" {
			rec["DeliveryStreamType"] = "DirectPut"
		}
		copyDest(rec, req.Input, "Configuration")
		if err := validateDestination(rec); err != nil {
			return nil, err
		}
		b, _ := json.Marshal(rec)
		if err := p.col(req, "fh").Txn(ctx, func(tx spi.Tx) error {
			if _, ok, err := tx.Get(name); err != nil {
				return err
			} else if ok {
				return &spi.Fault{Code: "ResourceInUseException", HTTPStatus: 400, Fault: "client"}
			}
			return tx.Put(name, b)
		}); err != nil {
			return nil, err
		}
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
		return &spi.Response{Output: map[string]any{"DeliveryStreamDescription": describeRecord(rec)}}, nil
	case "ListDeliveryStreams":
		limit := 10
		if value, ok := req.Input["Limit"]; ok {
			switch value := value.(type) {
			case int:
				limit = value
			case float64:
				if value != math.Trunc(value) || value < 1 || value > 10000 {
					return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
				limit = int(value)
			default:
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
		if limit < 1 || limit > 10000 {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		after := first(req.Input, "ExclusiveStartDeliveryStreamName")
		if after != "" && (len(after) > 64 || !firehoseStreamName.MatchString(after)) {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		streamType := first(req.Input, "DeliveryStreamType")
		switch streamType {
		case "", "DirectPut", "KinesisStreamAsSource", "MSKAsSource", "DatabaseAsSource":
		default:
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		kvs, _, err := p.col(req, "fh").List(ctx, "", after, 0)
		if err != nil {
			return nil, err
		}
		names := make([]any, 0, min(limit, len(kvs)))
		more := false
		// ponytail: scan at most the regional 5,000-stream ceiling; add indexed type pages if that ceiling becomes hot.
		for _, kv := range kvs {
			if streamType != "" {
				var rec map[string]any
				if json.Unmarshal(kv.Value, &rec) != nil || first(rec, "DeliveryStreamType") != streamType {
					continue
				}
			}
			if len(names) == limit {
				more = true
				break
			}
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"DeliveryStreamNames": names, "HasMoreDeliveryStreams": more}}, nil
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
		err := p.col(req, "fh").Txn(ctx, func(tx spi.Tx) error {
			b, ok, err := tx.Get(name)
			if err != nil {
				return err
			}
			if !ok {
				return &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			current := first(req.Input, "CurrentDeliveryStreamVersionId")
			if current == "" || first(req.Input, "DestinationId") != destinationID {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if current != first(rec, "VersionId") {
				return &spi.Fault{Code: "ConcurrentModificationException", HTTPStatus: 400, Fault: "client"}
			}
			copyDest(rec, req.Input, "Update")
			if err := validateDestination(rec); err != nil {
				return err
			}
			version, _ := strconv.Atoi(current)
			rec["VersionId"] = strconv.Itoa(version + 1)
			nb, _ := json.Marshal(rec)
			return tx.Put(name, nb)
		})
		if err != nil {
			return nil, err
		}
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

func copyDest(rec, in map[string]any, suffix string) {
	for _, base := range []string{"S3Destination", "ExtendedS3Destination"} {
		patch, _ := in[base+suffix].(map[string]any)
		if patch == nil {
			continue
		}
		destination, _ := rec[base+"Configuration"].(map[string]any)
		if destination == nil {
			destination = map[string]any{}
		}
		maps.Copy(destination, patch)
		rec[base+"Configuration"] = destination
	}
}

func describeRecord(rec map[string]any) map[string]any {
	description := maps.Clone(rec)
	destination := map[string]any{"DestinationId": destinationID}
	for _, base := range []string{"S3Destination", "ExtendedS3Destination"} {
		if configuration, ok := description[base+"Configuration"]; ok {
			destination[base+"Description"] = configuration
			delete(description, base+"Configuration")
		}
	}
	description["Destinations"] = []any{destination}
	description["HasMoreDestinations"] = false
	return description
}

func s3Dest(rec map[string]any) (bucket, prefix, errorPrefix, timezone, extension, compression string) {
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
		errorPrefix = first(m, "ErrorOutputPrefix")
		timezone = first(m, "CustomTimeZone")
		extension = first(m, "FileExtension")
		compression = first(m, "CompressionFormat")
		return bucket, prefix, errorPrefix, timezone, extension, compression
	}
	return "", "", "", "", "", ""
}

func validateDestination(rec map[string]any) error {
	_, prefix, errorPrefix, timezone, extension, compression := s3Dest(rec)
	if err := validatePrefixes(prefix, errorPrefix); err != nil {
		return err
	}
	if timezone != "" {
		if _, err := time.LoadLocation(timezone); err != nil {
			return &spi.Fault{Code: "ValidationException", Message: "CustomTimeZone is invalid.", HTTPStatus: 400, Fault: "client"}
		}
	}
	if extension != "" && (len(extension) > 128 || !firehoseFileExtension.MatchString(extension)) {
		return &spi.Fault{Code: "ValidationException", Message: "FileExtension is invalid.", HTTPStatus: 400, Fault: "client"}
	}
	if compression != "" && compression != "UNCOMPRESSED" && compression != "GZIP" {
		return spi.NotImplemented("aws.firehose", "CompressionFormat="+compression, "emulate")
	}
	return nil
}

func validatePrefixes(prefix, errorPrefix string) error {
	if strings.Contains(prefix, "!{") && errorPrefix == "" {
		return &spi.Fault{Code: "ValidationException", Message: "ErrorOutputPrefix is required when Prefix contains expressions.", HTTPStatus: 400, Fault: "client"}
	}
	for _, candidate := range []struct {
		value       string
		errorPrefix bool
	}{{prefix, false}, {errorPrefix, true}} {
		matches := firehosePrefixExpression.FindAllStringSubmatch(candidate.value, -1)
		if strings.Contains(firehosePrefixExpression.ReplaceAllString(candidate.value, ""), "!{") {
			return &spi.Fault{Code: "ValidationException", Message: "Prefix expression is invalid.", HTTPStatus: 400, Fault: "client"}
		}
		hasErrorType := false
		for _, match := range matches {
			namespace, value := match[1], match[2]
			switch {
			case namespace == "timestamp" && value != "":
			case namespace == "firehose" && value == "random-string":
			case namespace == "firehose" && value == "error-output-type" && candidate.errorPrefix:
				hasErrorType = true
			case namespace == "partitionKeyFromQuery" || namespace == "partitionKeyFromLambda":
				if candidate.errorPrefix {
					return &spi.Fault{Code: "ValidationException", Message: "Dynamic partitioning is invalid in ErrorOutputPrefix.", HTTPStatus: 400, Fault: "client"}
				}
				return spi.NotImplemented("aws.firehose", "dynamic partitioning", "emulate")
			default:
				return &spi.Fault{Code: "ValidationException", Message: "Prefix expression is invalid.", HTTPStatus: 400, Fault: "client"}
			}
		}
		if candidate.errorPrefix && len(matches) > 0 && !hasErrorType {
			return &spi.Fault{Code: "ValidationException", Message: "ErrorOutputPrefix must contain !{firehose:error-output-type}.", HTTPStatus: 400, Fault: "client"}
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
	bucket, prefix, _, timezone, extension, compression := s3Dest(rec)
	if bucket == "" {
		return
	}
	version := first(rec, "VersionId")
	if version == "" {
		version = "1"
	}
	now := p.deps.Clock.Now().UTC()
	if timezone != "" {
		location, _ := time.LoadLocation(timezone)
		now = now.In(location)
	}
	if compression == "GZIP" {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write(data)
		_ = writer.Close()
		data = compressed.Bytes()
		if extension == "" {
			extension = ".gz"
		}
	}
	key := p.evaluatedS3Prefix(prefix, now) + stream + "-" + version + "-" + now.Format("2006-01-02-15-04-05-") + recID + extension
	info, err := p.deps.Blobs.Put(ctx, req.Identity.Account+"/"+req.Identity.Region+"/"+bucket+"/"+key, bytes.NewReader(data))
	if err != nil {
		return
	}
	etag := `"` + info.MD5 + `"`
	mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	meta, _ := json.Marshal(map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "deleteMarker": false})
	_ = p.col(req, "objects").Put(ctx, bucket+"/"+key, meta)
}

var (
	firehoseTimestampPrefix  = regexp.MustCompile(`!\{timestamp:([^}]*)\}`)
	firehosePrefixExpression = regexp.MustCompile(`!\{([^}:]+):([^}]*)\}`)
	firehoseFileExtension    = regexp.MustCompile(`^\.[0-9a-z!\-_.*'()]+$`)
	firehoseStreamName       = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
)

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
