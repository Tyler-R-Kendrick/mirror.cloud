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
	"slices"
	"sort"
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

const (
	maxRecordBytes = 1000 * 1024
	maxBatchBytes  = 4 * 1024 * 1024
)

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
	if req.Operation != "ListDeliveryStreams" && slices.Contains(p.Operations(), req.Operation) && (len(name) > 64 || !firehoseStreamName.MatchString(name)) {
		return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	switch req.Operation {
	case "CreateDeliveryStream":
		streamType := first(req.Input, "DeliveryStreamType")
		if streamType == "" {
			streamType = "DirectPut"
		}
		var source, directPutSource map[string]any
		switch streamType {
		case "DirectPut":
			if req.Input["KinesisStreamSourceConfiguration"] != nil {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if value := req.Input["DirectPutSourceConfiguration"]; value != nil {
				var ok bool
				directPutSource, ok = value.(map[string]any)
				if !ok || directPutSource["ThroughputHintInMBs"] == nil {
					return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
				if _, valid := inputLimit(directPutSource["ThroughputHintInMBs"], 0, 100); !valid {
					return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "KinesisStreamAsSource":
			if req.Input["DirectPutSourceConfiguration"] != nil {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			var ok bool
			source, ok = req.Input["KinesisStreamSourceConfiguration"].(map[string]any)
			if !ok || len(first(source, "KinesisStreamARN")) > 512 || !firehoseKinesisStreamARN.MatchString(first(source, "KinesisStreamARN")) || !validRoleARN(first(source, "RoleARN")) {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		case "MSKAsSource", "DatabaseAsSource":
			return nil, spi.NotImplemented("aws.firehose", streamType, "emulate")
		default:
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if err := validateCreateDestination(req.Input); err != nil {
			return nil, err
		}
		arn := "arn:aws:firehose:" + req.Identity.Region + ":" + req.Identity.Account + ":deliverystream/" + name
		timestamp := float64(p.deps.Clock.Now().UnixNano()) / float64(time.Second)
		rec := map[string]any{
			"DeliveryStreamName": name, "DeliveryStreamARN": arn, "DeliveryStreamStatus": "ACTIVE",
			"DeliveryStreamType": streamType, "VersionId": "1", "CreateTimestamp": timestamp, "LastUpdateTimestamp": timestamp,
		}
		if source != nil {
			rec["KinesisStreamSourceConfiguration"] = maps.Clone(source)
		}
		if directPutSource != nil {
			rec["DirectPutSourceConfiguration"] = maps.Clone(directPutSource)
		}
		copyDest(rec, req.Input, "Configuration")
		if err := validateDestination(rec); err != nil {
			return nil, err
		}
		encryption, valid := encryptionDescription(req.Input["DeliveryStreamEncryptionConfigurationInput"], false)
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if encryption["Status"] == "ENABLED" && streamType != "DirectPut" {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		rec["DeliveryStreamEncryptionConfiguration"] = encryption
		tags, valid := parseTags(req.Input["Tags"], false)
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
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
		if err := putTags(ctx, p.col(req, "fhtag"), name, tags); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"DeliveryStreamARN": arn}}, nil
	case "DeleteDeliveryStream":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "fh").Delete(ctx, name)
		_ = p.col(req, "fhtag").Delete(ctx, name)
		kvs, _, _ := p.col(req, "fhrec:"+name).List(ctx, "", "", 0)
		for _, kv := range kvs {
			_ = p.col(req, "fhrec:"+name).Delete(ctx, kv.Key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDeliveryStream":
		if _, valid := inputLimit(req.Input["Limit"], 10000, 10000); !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		after := ""
		if value, exists := req.Input["ExclusiveStartDestinationId"]; exists {
			var ok bool
			after, ok = value.(string)
			if !ok || len(after) < 1 || len(after) > 100 || !firehoseDestinationID.MatchString(after) {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
		b, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"DeliveryStreamDescription": describeRecord(rec, after)}}, nil
	case "ListDeliveryStreams":
		limit, valid := inputLimit(req.Input["Limit"], 10, 10000)
		if !valid {
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
		stream, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		decoded, valid := recordData(req.Input["Record"])
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		id := p.putOne(ctx, req, name, req.Input["Record"], decoded)
		return &spi.Response{Output: map[string]any{"RecordId": id, "Encrypted": streamEncrypted(stream)}}, nil
	case "PutRecordBatch":
		stream, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		recs, ok := req.Input["Records"].([]any)
		if !ok || len(recs) < 1 || len(recs) > 500 {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		decoded := make([][]byte, len(recs))
		total := 0
		for i, rec := range recs {
			var valid bool
			decoded[i], valid = recordData(rec)
			total += len(decoded[i])
			if !valid || total > maxBatchBytes {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
		resp := make([]any, 0, len(recs))
		for i, rec := range recs {
			id := p.putOne(ctx, req, name, rec, decoded[i])
			resp = append(resp, map[string]any{"RecordId": id})
		}
		return &spi.Response{Output: map[string]any{"Encrypted": streamEncrypted(stream), "FailedPutCount": 0, "RequestResponses": resp}}, nil
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
			rec["LastUpdateTimestamp"] = float64(p.deps.Clock.Now().UnixNano()) / float64(time.Second)
			nb, _ := json.Marshal(rec)
			return tx.Put(name, nb)
		})
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagDeliveryStream":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		updates, valid := parseTags(req.Input["Tags"], true)
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if err := p.col(req, "fhtag").Txn(ctx, func(tx spi.Tx) error {
			b, _, err := tx.Get(name)
			if err != nil {
				return err
			}
			tags := loadTags(b)
			maps.Copy(tags, updates)
			if len(tags) > 50 {
				return &spi.Fault{Code: "LimitExceededException", HTTPStatus: 400, Fault: "client"}
			}
			return putTagsTx(tx, name, tags)
		}); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagDeliveryStream":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		keys, valid := tagKeys(req.Input["TagKeys"])
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if err := p.col(req, "fhtag").Txn(ctx, func(tx spi.Tx) error {
			b, _, err := tx.Get(name)
			if err != nil {
				return err
			}
			tags := loadTags(b)
			for _, key := range keys {
				delete(tags, key)
			}
			return putTagsTx(tx, name, tags)
		}); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForDeliveryStream":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		limit, valid := inputLimit(req.Input["Limit"], 50, 50)
		after := first(req.Input, "ExclusiveStartTagKey")
		if !valid || (after != "" && !validTagKey(after)) {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := p.col(req, "fhtag").Get(ctx, name)
		tags := map[string]string{}
		if ok {
			tags = loadTags(b)
		}
		listed := make([]any, 0, min(limit, len(tags)))
		more := false
		for _, tag := range tagList(tags) {
			if tag["Key"].(string) <= after {
				continue
			}
			if len(listed) == limit {
				more = true
				break
			}
			listed = append(listed, tag)
		}
		return &spi.Response{Output: map[string]any{"Tags": listed, "HasMoreTags": more}}, nil
	case "StartDeliveryStreamEncryption", "StopDeliveryStreamEncryption":
		encryption := map[string]any{"Status": "DISABLED"}
		if req.Operation == "StartDeliveryStreamEncryption" {
			var valid bool
			encryption, valid = encryptionDescription(req.Input["DeliveryStreamEncryptionConfigurationInput"], true)
			if !valid {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
		if err := p.col(req, "fh").Txn(ctx, func(tx spi.Tx) error {
			b, ok, err := tx.Get(name)
			if err != nil {
				return err
			}
			if !ok {
				return &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			if first(rec, "DeliveryStreamType") != "DirectPut" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			rec["DeliveryStreamEncryptionConfiguration"] = encryption
			rec["LastUpdateTimestamp"] = float64(p.deps.Clock.Now().UnixNano()) / float64(time.Second)
			nb, _ := json.Marshal(rec)
			return tx.Put(name, nb)
		}); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.firehose", req.Operation, "emulate")
	}
}

func recordData(rec any) ([]byte, bool) {
	m, ok := rec.(map[string]any)
	if !ok {
		return nil, false
	}
	switch data := m["Data"].(type) {
	case string:
		decoded, err := base64.StdEncoding.DecodeString(data)
		return decoded, err == nil && len(decoded) <= maxRecordBytes
	case []byte:
		return data, len(data) <= maxRecordBytes
	default:
		return nil, false
	}
}

func encryptionDescription(value any, defaultAWS bool) (map[string]any, bool) {
	if value == nil {
		if defaultAWS {
			return map[string]any{"KeyType": "AWS_OWNED_CMK", "Status": "ENABLED"}, true
		}
		return map[string]any{"Status": "DISABLED"}, true
	}
	input, ok := value.(map[string]any)
	if !ok {
		return nil, false
	}
	keyType, keyARN := first(input, "KeyType"), first(input, "KeyARN")
	switch keyType {
	case "AWS_OWNED_CMK":
		if keyARN != "" {
			return nil, false
		}
		return map[string]any{"KeyType": keyType, "Status": "ENABLED"}, true
	case "CUSTOMER_MANAGED_CMK":
		if len(keyARN) > 512 || !firehoseKMSKeyARN.MatchString(keyARN) {
			return nil, false
		}
		return map[string]any{"KeyARN": keyARN, "KeyType": keyType, "Status": "ENABLED"}, true
	default:
		return nil, false
	}
}

func streamEncrypted(b []byte) bool {
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	encryption, _ := rec["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	return first(encryption, "Status") == "ENABLED"
}

func (p *Pack) putOne(ctx context.Context, req *spi.Request, name string, rec any, decoded []byte) string {
	id := p.deps.Rand.Hex(16)
	payload := map[string]any{"Record": rec, "Decoded": string(decoded)}
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

func describeRecord(rec map[string]any, after string) map[string]any {
	description := maps.Clone(rec)
	if configuration, ok := description["DirectPutSourceConfiguration"].(map[string]any); ok {
		description["Source"] = map[string]any{"DirectPutSourceDescription": maps.Clone(configuration)}
		delete(description, "DirectPutSourceConfiguration")
	}
	if configuration, ok := description["KinesisStreamSourceConfiguration"].(map[string]any); ok {
		source := maps.Clone(configuration)
		source["DeliveryStartTimestamp"] = description["CreateTimestamp"]
		description["Source"] = map[string]any{"KinesisStreamSourceDescription": source}
		delete(description, "KinesisStreamSourceConfiguration")
	}
	destination := map[string]any{"DestinationId": destinationID}
	for _, base := range []string{"S3Destination", "ExtendedS3Destination"} {
		if configuration, ok := description[base+"Configuration"]; ok {
			destination[base+"Description"] = configuration
			delete(description, base+"Configuration")
		}
	}
	destinations := []any{}
	if destinationID > after {
		destinations = append(destinations, destination)
	}
	description["Destinations"] = destinations
	description["HasMoreDestinations"] = false
	return description
}

func inputLimit(value any, fallback, maximum int) (int, bool) {
	if value == nil {
		return fallback, true
	}
	var limit int
	switch value := value.(type) {
	case int:
		limit = value
	case float64:
		if value != math.Trunc(value) || value < 1 || value > float64(maximum) {
			return 0, false
		}
		limit = int(value)
	default:
		return 0, false
	}
	return limit, limit >= 1 && limit <= maximum
}

func parseTags(value any, required bool) (map[string]string, bool) {
	if value == nil && !required {
		return map[string]string{}, true
	}
	items, ok := value.([]any)
	if !ok || len(items) < 1 || len(items) > 50 {
		return nil, false
	}
	tags := make(map[string]string, len(items))
	for _, item := range items {
		tag, ok := item.(map[string]any)
		key, keyOK := tag["Key"].(string)
		if !ok || !keyOK || !validTagKey(key) {
			return nil, false
		}
		value := ""
		if raw, exists := tag["Value"]; exists {
			value, ok = raw.(string)
			if !ok || len(value) > 256 || !firehoseTagValue.MatchString(value) {
				return nil, false
			}
		}
		if _, duplicate := tags[key]; duplicate {
			return nil, false
		}
		tags[key] = value
	}
	return tags, true
}

func tagKeys(value any) ([]string, bool) {
	items, ok := value.([]any)
	if !ok || len(items) < 1 || len(items) > 50 {
		return nil, false
	}
	keys := make([]string, len(items))
	for i, item := range items {
		keys[i], ok = item.(string)
		if !ok || !validTagKey(keys[i]) {
			return nil, false
		}
	}
	return keys, true
}

func validTagKey(key string) bool {
	return len(key) <= 128 && !strings.HasPrefix(key, "aws:") && firehoseTagKey.MatchString(key)
}

func loadTags(b []byte) map[string]string {
	var stored []map[string]any
	_ = json.Unmarshal(b, &stored)
	tags := make(map[string]string, len(stored))
	for _, tag := range stored {
		key, _ := tag["Key"].(string)
		value, _ := tag["Value"].(string)
		if key != "" {
			tags[key] = value
		}
	}
	return tags
}

func tagList(tags map[string]string) []map[string]any {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	listed := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		listed = append(listed, map[string]any{"Key": key, "Value": tags[key]})
	}
	return listed
}

func putTags(ctx context.Context, collection spi.Collection, key string, tags map[string]string) error {
	if len(tags) == 0 {
		return collection.Delete(ctx, key)
	}
	b, _ := json.Marshal(tagList(tags))
	return collection.Put(ctx, key, b)
}

func putTagsTx(tx spi.Tx, key string, tags map[string]string) error {
	if len(tags) == 0 {
		return tx.Delete(key)
	}
	b, _ := json.Marshal(tagList(tags))
	return tx.Put(key, b)
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
	count := 0
	var destination map[string]any
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		if value, exists := rec[key]; exists {
			var ok bool
			destination, ok = value.(map[string]any)
			if !ok {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			count++
		}
	}
	if count != 1 || len(first(destination, "BucketARN")) > 2048 || !firehoseBucketARN.MatchString(first(destination, "BucketARN")) || !validRoleARN(first(destination, "RoleARN")) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
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

func validateCreateDestination(input map[string]any) error {
	destination := ""
	for key := range input {
		if strings.HasSuffix(key, "DestinationConfiguration") {
			if destination != "" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			destination = key
		}
	}
	switch destination {
	case "S3DestinationConfiguration", "ExtendedS3DestinationConfiguration":
		return nil
	case "":
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	default:
		return spi.NotImplemented("aws.firehose", destination, "emulate")
	}
}

func validRoleARN(arn string) bool {
	return len(arn) <= 512 && firehoseRoleARN.MatchString(arn)
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
	firehoseTagKey           = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]+$`)
	firehoseTagValue         = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]*$`)
	firehoseKMSKeyARN        = regexp.MustCompile(`^arn:[^:]+:kms:[a-zA-Z0-9\-]+:\d{12}:key/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseBucketARN        = regexp.MustCompile(`^arn:.*:s3:::[\w.\-]{1,255}$`)
	firehoseRoleARN          = regexp.MustCompile(`^arn:.*:iam::\d{12}:role/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseKinesisStreamARN = regexp.MustCompile(`^arn:.*:kinesis:[a-zA-Z0-9\-]+:\d{12}:stream/[a-zA-Z0-9_.-]+$`)
	firehoseDestinationID    = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)
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
