// Package firehose stores delivery streams and PutRecord writes to an S3 destination (no Redshift/OpenSearch/HTTP).
package firehose

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/logs"
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
		if err := validateDestination(rec, req.Identity.Region); err != nil {
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
			extended, _ := rec["ExtendedS3DestinationConfiguration"].(map[string]any)
			backupEnabled := first(extended, "S3BackupMode") == "Enabled"
			dynamicEnabled := dynamicPartitioningEnabled(extended)
			copyDest(rec, req.Input, "Update")
			extended, _ = rec["ExtendedS3DestinationConfiguration"].(map[string]any)
			if backupEnabled && first(extended, "S3BackupMode") != "Enabled" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if dynamicEnabled != dynamicPartitioningEnabled(extended) {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if err := validateDestination(rec, req.Identity.Region); err != nil {
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
		dynamic, _ := destination["DynamicPartitioningConfiguration"].(map[string]any)
		maps.Copy(destination, patch)
		if dynamicPatch, ok := patch["DynamicPartitioningConfiguration"].(map[string]any); ok {
			if dynamic == nil {
				dynamic = map[string]any{}
			}
			maps.Copy(dynamic, dynamicPatch)
			destination["DynamicPartitioningConfiguration"] = dynamic
		}
		if backupPatch, ok := patch["S3BackupUpdate"].(map[string]any); ok {
			backup, _ := destination["S3BackupConfiguration"].(map[string]any)
			if backup == nil {
				backup = map[string]any{}
			}
			maps.Copy(backup, backupPatch)
			destination["S3BackupConfiguration"] = backup
			delete(destination, "S3BackupUpdate")
		}
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
		if configuration, ok := description[base+"Configuration"].(map[string]any); ok {
			describeS3Configuration(configuration)
			if base == "ExtendedS3Destination" {
				if first(configuration, "S3BackupMode") == "" {
					configuration["S3BackupMode"] = "Disabled"
				}
				if backup, ok := configuration["S3BackupConfiguration"].(map[string]any); ok {
					describeS3Configuration(backup)
					configuration["S3BackupDescription"] = backup
					delete(configuration, "S3BackupConfiguration")
				}
			}
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

func describeS3Configuration(configuration map[string]any) {
	if configuration["BufferingHints"] == nil {
		configuration["BufferingHints"] = map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}
	}
	if first(configuration, "CompressionFormat") == "" {
		configuration["CompressionFormat"] = "UNCOMPRESSED"
	}
	if configuration["EncryptionConfiguration"] == nil {
		configuration["EncryptionConfiguration"] = map[string]any{"NoEncryptionConfig": "NoEncryption"}
	}
	if dynamic, ok := configuration["DynamicPartitioningConfiguration"].(map[string]any); ok && dynamic["RetryOptions"] == nil {
		dynamic["RetryOptions"] = map[string]any{"DurationInSeconds": 300}
	}
}

func inputLimit(value any, fallback, maximum int) (int, bool) {
	if value == nil {
		return fallback, true
	}
	return inputInteger(value, 1, maximum)
}

func inputInteger(value any, minimum, maximum int) (int, bool) {
	var limit int
	switch value := value.(type) {
	case int:
		limit = value
	case float64:
		if value != math.Trunc(value) || value < float64(minimum) || value > float64(maximum) {
			return 0, false
		}
		limit = int(value)
	default:
		return 0, false
	}
	return limit, limit >= minimum && limit <= maximum
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

func s3Configuration(configuration map[string]any) (bucket, prefix, errorPrefix, timezone, extension, compression, kmsARN string) {
	bucket = bucketFromARN(first(configuration, "BucketARN"))
	prefix, errorPrefix = first(configuration, "Prefix"), first(configuration, "ErrorOutputPrefix")
	timezone, extension, compression = first(configuration, "CustomTimeZone"), first(configuration, "FileExtension"), first(configuration, "CompressionFormat")
	encryption, _ := configuration["EncryptionConfiguration"].(map[string]any)
	kms, _ := encryption["KMSEncryptionConfig"].(map[string]any)
	kmsARN = first(kms, "AWSKMSKeyARN")
	return
}

func validateDestination(rec map[string]any, region string) error {
	count := 0
	var destination map[string]any
	extended := false
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		if value, exists := rec[key]; exists {
			var ok bool
			destination, ok = value.(map[string]any)
			if !ok {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			count++
			extended = key == "ExtendedS3DestinationConfiguration"
		}
	}
	if count != 1 {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(destination, region, extended); err != nil {
		return err
	}
	mode := ""
	if raw, exists := destination["S3BackupMode"]; exists {
		var ok bool
		mode, ok = raw.(string)
		if !ok || (mode != "Disabled" && mode != "Enabled") {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	backupRaw, backupExists := destination["S3BackupConfiguration"]
	if mode == "Enabled" && !backupExists {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if backupExists {
		backup, ok := backupRaw.(map[string]any)
		if !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		return validateS3Configuration(backup, region, false)
	}
	return nil
}

func validateS3Configuration(destination map[string]any, region string, processingAllowed bool) error {
	if len(first(destination, "BucketARN")) > 2048 || !firehoseBucketARN.MatchString(first(destination, "BucketARN")) || !validRoleARN(first(destination, "RoleARN")) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	if raw, exists := destination["ProcessingConfiguration"]; exists {
		if !processingAllowed {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if err := validateProcessingConfiguration(raw); err != nil {
			return err
		}
	}
	if raw, exists := destination["BufferingHints"]; exists {
		hints, ok := raw.(map[string]any)
		_, hasInterval := hints["IntervalInSeconds"]
		_, hasSize := hints["SizeInMBs"]
		if !ok || hasInterval != hasSize {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if hasInterval {
			if _, valid := inputInteger(hints["IntervalInSeconds"], 0, 900); !valid {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if _, valid := inputInteger(hints["SizeInMBs"], 1, 128); !valid {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	if raw, exists := destination["EncryptionConfiguration"]; exists {
		encryption, ok := raw.(map[string]any)
		noEncryption, noEncryptionOK := encryption["NoEncryptionConfig"].(string)
		kmsRaw, kmsOK := encryption["KMSEncryptionConfig"]
		if !ok || noEncryptionOK == kmsOK || (noEncryptionOK && noEncryption != "NoEncryption") {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if kmsOK {
			kms, ok := kmsRaw.(map[string]any)
			arn := first(kms, "AWSKMSKeyARN")
			if !ok || len(arn) > 512 || !firehoseDestinationKMSARN.MatchString(arn) || !strings.Contains(arn, ":kms:"+region+":") {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	prefix, errorPrefix := first(destination, "Prefix"), first(destination, "ErrorOutputPrefix")
	timezone, extension, compression := first(destination, "CustomTimeZone"), first(destination, "FileExtension"), first(destination, "CompressionFormat")
	dynamic, err := validateDynamicPartitioning(destination, processingAllowed)
	if err != nil {
		return err
	}
	if err := validatePrefixes(prefix, errorPrefix, dynamic); err != nil {
		return err
	}
	if timezone != "" {
		if len(timezone) > 50 || !firehoseTimeZone.MatchString(timezone) {
			return &spi.Fault{Code: "ValidationException", Message: "CustomTimeZone is invalid.", HTTPStatus: 400, Fault: "client"}
		}
		if _, err := time.LoadLocation(timezone); err != nil {
			return &spi.Fault{Code: "ValidationException", Message: "CustomTimeZone is invalid.", HTTPStatus: 400, Fault: "client"}
		}
	}
	if extension != "" && (len(extension) > 128 || !firehoseFileExtension.MatchString(extension)) {
		return &spi.Fault{Code: "ValidationException", Message: "FileExtension is invalid.", HTTPStatus: 400, Fault: "client"}
	}
	if compression != "" && compression != "UNCOMPRESSED" && compression != "GZIP" && compression != "ZIP" {
		return spi.NotImplemented("aws.firehose", "CompressionFormat="+compression, "emulate")
	}
	return nil
}

func validateDynamicPartitioning(destination map[string]any, allowed bool) (bool, error) {
	raw, exists := destination["DynamicPartitioningConfiguration"]
	if !exists {
		return false, nil
	}
	configuration, ok := raw.(map[string]any)
	if !allowed || !ok {
		return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	enabled := false
	if value, exists := configuration["Enabled"]; exists {
		enabled, ok = value.(bool)
		if !ok {
			return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if rawRetry, exists := configuration["RetryOptions"]; exists {
		retry, ok := rawRetry.(map[string]any)
		if !ok {
			return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if value, exists := retry["DurationInSeconds"]; exists {
			if _, valid := inputInteger(value, 0, 7200); !valid {
				return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	prefix := first(destination, "Prefix")
	if enabled && (first(destination, "ErrorOutputPrefix") == "" || !strings.Contains(prefix, "!{partitionKeyFrom")) {
		return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if strings.Contains(prefix, "!{partitionKeyFromLambda:") && !hasProcessor(destination, "Lambda") {
		return false, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	return enabled, nil
}

func dynamicPartitioningEnabled(destination map[string]any) bool {
	configuration, _ := destination["DynamicPartitioningConfiguration"].(map[string]any)
	return configuration["Enabled"] == true
}

func hasProcessor(destination map[string]any, typeName string) bool {
	processing, _ := destination["ProcessingConfiguration"].(map[string]any)
	processors, _ := processing["Processors"].([]any)
	for _, raw := range processors {
		processor, _ := raw.(map[string]any)
		if first(processor, "Type") == typeName {
			return processing["Enabled"] == true
		}
	}
	return false
}

func validateCloudWatchLogging(raw any) error {
	if raw == nil {
		return nil
	}
	options, ok := raw.(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	enabled, _ := options["Enabled"].(bool)
	if value, exists := options["Enabled"]; exists {
		if _, ok := value.(bool); !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	group, stream := first(options, "LogGroupName"), first(options, "LogStreamName")
	if value, exists := options["LogGroupName"]; exists {
		if _, ok := value.(string); !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if value, exists := options["LogStreamName"]; exists {
		if _, ok := value.(string); !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if (enabled && (group == "" || stream == "")) || len(group) > 512 || len(stream) > 512 || !firehoseLogGroup.MatchString(group) || !firehoseLogStream.MatchString(stream) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func validateProcessingConfiguration(raw any) error {
	if raw == nil {
		return nil
	}
	configuration, ok := raw.(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	enabled, _ := configuration["Enabled"].(bool)
	if value, exists := configuration["Enabled"]; exists {
		if _, ok := value.(bool); !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	rawProcessors, exists := configuration["Processors"]
	if !exists {
		if enabled {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		return nil
	}
	processors, ok := rawProcessors.([]any)
	if !ok || (enabled && len(processors) == 0) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	decompressed := false
	for _, rawProcessor := range processors {
		processor, ok := rawProcessor.(map[string]any)
		typeName := first(processor, "Type")
		if !ok || typeName == "" {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if rawParameters, exists := processor["Parameters"]; exists {
			parameters, ok := rawParameters.([]any)
			if !ok {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			for _, rawParameter := range parameters {
				parameter, ok := rawParameter.(map[string]any)
				name, value := first(parameter, "ParameterName"), first(parameter, "ParameterValue")
				if !ok || !validProcessorParameter(name) || len(value) > 5120 || strings.TrimSpace(value) == "" {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		}
		switch typeName {
		case "AppendDelimiterToRecord":
		case "RecordDeAggregation":
			subRecordType, delimiter := processorParameter(processor, "SubRecordType"), processorParameter(processor, "Delimiter")
			if subRecordType != "JSON" && subRecordType != "DELIMITED" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if decoded, err := base64.StdEncoding.DecodeString(delimiter); subRecordType == "DELIMITED" && (err != nil || len(decoded) == 0) {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			} else if subRecordType == "JSON" && delimiter != "" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			parameters, _ := processor["Parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, _ := rawParameter.(map[string]any)
				name := first(parameter, "ParameterName")
				if name != "SubRecordType" && name != "Delimiter" {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "Decompression":
			format := processorParameter(processor, "CompressionFormat")
			if format != "" && format != "GZIP" {
				return spi.NotImplemented("aws.firehose", "Decompression="+format, "emulate")
			}
			parameters, _ := processor["Parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, _ := rawParameter.(map[string]any)
				if first(parameter, "ParameterName") != "CompressionFormat" {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
			decompressed = true
		case "CloudWatchLogProcessing":
			if !decompressed || processorParameter(processor, "DataMessageExtraction") != "true" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			parameters, _ := processor["Parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, _ := rawParameter.(map[string]any)
				if first(parameter, "ParameterName") != "DataMessageExtraction" {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "Lambda":
			arn := processorParameter(processor, "LambdaArn")
			if len(arn) > 512 || !firehoseLambdaARN.MatchString(arn) {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if retries := processorParameter(processor, "NumberOfRetries"); retries != "" {
				value, err := strconv.Atoi(retries)
				if err != nil || value < 0 || value > 300 {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "MetadataExtraction":
			if enabled {
				return spi.NotImplemented("aws.firehose", "Processor="+typeName, "emulate")
			}
		default:
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func processorParameter(processor map[string]any, name string) string {
	parameters, _ := processor["Parameters"].([]any)
	for _, raw := range parameters {
		parameter, _ := raw.(map[string]any)
		if first(parameter, "ParameterName") == name {
			return first(parameter, "ParameterValue")
		}
	}
	return ""
}

func validProcessorParameter(name string) bool {
	switch name {
	case "LambdaArn", "NumberOfRetries", "MetadataExtractionQuery", "JsonParsingEngine", "RoleArn", "BufferSizeInMBs", "BufferIntervalInSeconds", "SubRecordType", "Delimiter", "CompressionFormat", "DataMessageExtraction":
		return true
	default:
		return false
	}
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

func validatePrefixes(prefix, errorPrefix string, dynamicConfig ...bool) error {
	dynamic := len(dynamicConfig) == 1 && dynamicConfig[0]
	if len(prefix) > 1024 || len(errorPrefix) > 1024 {
		return &spi.Fault{Code: "ValidationException", Message: "Prefix is too long.", HTTPStatus: 400, Fault: "client"}
	}
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
			case namespace == "partitionKeyFromLambda" && dynamic && value != "" && !candidate.errorPrefix:
			case namespace == "partitionKeyFromQuery" && dynamic && value != "" && !candidate.errorPrefix:
				return spi.NotImplemented("aws.firehose", "MetadataExtraction", "emulate")
			case (namespace == "partitionKeyFromLambda" || namespace == "partitionKeyFromQuery") && candidate.errorPrefix:
				return &spi.Fault{Code: "ValidationException", Message: "Dynamic partitioning is invalid in ErrorOutputPrefix.", HTTPStatus: 400, Fault: "client"}
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
	var destination map[string]any
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration"} {
		if configuration, ok := rec[key].(map[string]any); ok {
			destination = configuration
			break
		}
	}
	version := first(rec, "VersionId")
	if version == "" {
		version = "1"
	}
	now := p.deps.Clock.Now().UTC()
	p.deliverS3Configuration(ctx, req, destination, stream, version, recID, data, now)
	extended, _ := rec["ExtendedS3DestinationConfiguration"].(map[string]any)
	if first(extended, "S3BackupMode") == "Enabled" {
		backup, _ := extended["S3BackupConfiguration"].(map[string]any)
		p.deliverS3Configuration(ctx, req, backup, stream, version, recID, data, now)
	}
}

func (p *Pack) deliverS3Configuration(ctx context.Context, req *spi.Request, configuration map[string]any, stream, version, recID string, data []byte, now time.Time) {
	bucket, prefix, errorPrefix, timezone, extension, compression, kmsARN := s3Configuration(configuration)
	if bucket == "" {
		return
	}
	if timezone != "" {
		location, _ := time.LoadLocation(timezone)
		now = now.In(location)
	}
	records, failures := p.processData(ctx, req, configuration, stream, recID, data, now)
	for _, failure := range failures {
		p.logDeliveryError(ctx, req, configuration, stream, failure.message, now)
		p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
	}
	if len(records) == 0 {
		return
	}
	if !dynamicPartitioningEnabled(configuration) {
		var combined bytes.Buffer
		for _, record := range records {
			combined.Write(record.data)
		}
		records = []processingRecord{{recID: recID, data: combined.Bytes(), raw: data}}
	}
	for _, record := range records {
		data, recordExtension := record.data, extension
		switch compression {
		case "GZIP":
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			_, _ = writer.Write(data)
			_ = writer.Close()
			data = compressed.Bytes()
			if recordExtension == "" {
				recordExtension = ".gz"
			}
		case "ZIP":
			var compressed bytes.Buffer
			writer := zip.NewWriter(&compressed)
			entry, _ := writer.Create(stream)
			_, _ = entry.Write(data)
			_ = writer.Close()
			data = compressed.Bytes()
			if recordExtension == "" {
				recordExtension = ".zip"
			}
		}
		evaluatedPrefix, err := p.evaluatedDynamicS3Prefix(prefix, now, record.partitionKeys)
		if err != nil {
			failure := &processingFailure{typeName: "processing-failed", code: "DynamicPartitioning.Failed", message: err.Error(), attempts: 1, recID: record.recID, data: record.raw}
			p.logDeliveryError(ctx, req, configuration, stream, failure.message, now)
			p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
			continue
		}
		key := evaluatedPrefix + stream + "-" + version + "-" + now.Format("2006-01-02-15-04-05-") + record.recID + recordExtension
		p.deliverS3Object(ctx, req, bucket, key, kmsARN, data)
	}
}

func (p *Pack) logDeliveryError(ctx context.Context, req *spi.Request, configuration map[string]any, stream, message string, now time.Time) {
	options, _ := configuration["CloudWatchLoggingOptions"].(map[string]any)
	if options["Enabled"] != true {
		return
	}
	payload, _ := json.Marshal(map[string]any{"deliveryStreamName": stream, "errorMessage": message})
	_, _ = logs.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "PutLogEvents", Input: map[string]any{
		"logGroupName": first(options, "LogGroupName"), "logStreamName": first(options, "LogStreamName"),
		"logEvents": []any{map[string]any{"message": string(payload), "timestamp": now.UnixMilli()}},
	}})
}

type processingFailure struct {
	typeName, code, message, lambdaARN string
	attempts                           int
	recID                              string
	data                               []byte
}

type processingRecord struct {
	recID         string
	data          []byte
	raw           []byte
	partitionKeys map[string]string
}

func (p *Pack) processData(ctx context.Context, req *spi.Request, configuration map[string]any, stream, recID string, data []byte, now time.Time) ([]processingRecord, []*processingFailure) {
	processing, _ := configuration["ProcessingConfiguration"].(map[string]any)
	if processing["Enabled"] != true {
		return []processingRecord{{recID: recID, data: data, raw: data}}, nil
	}
	records := []processingRecord{{recID: recID, data: data, raw: data}}
	var failures []*processingFailure
	for _, raw := range processing["Processors"].([]any) {
		processor, _ := raw.(map[string]any)
		switch first(processor, "Type") {
		case "RecordDeAggregation":
			var next []processingRecord
			for _, record := range records {
				parts, err := deaggregateData(processor, record.data)
				if err != nil {
					failures = append(failures, &processingFailure{typeName: "processing-failed", code: "RecordDeAggregation.Failed", message: err.Error(), attempts: 1, recID: record.recID, data: record.raw})
					continue
				}
				for index, part := range parts {
					id := record.recID
					if len(parts) > 1 {
						id += "." + strconv.Itoa(index)
					}
					next = append(next, processingRecord{recID: id, data: part, raw: part})
				}
			}
			records = next
		case "AppendDelimiterToRecord":
			for index := range records {
				records[index].data = append(bytes.Clone(records[index].data), '\n')
			}
		case "Decompression":
			var next []processingRecord
			for _, record := range records {
				reader, err := gzip.NewReader(bytes.NewReader(record.data))
				if err != nil {
					failures = append(failures, &processingFailure{typeName: "decompression-failed", code: "Decompression.Failed", message: err.Error(), attempts: 1, recID: record.recID, data: record.raw})
					continue
				}
				decompressed, err := io.ReadAll(reader)
				_ = reader.Close()
				if err != nil {
					failures = append(failures, &processingFailure{typeName: "decompression-failed", code: "Decompression.Failed", message: err.Error(), attempts: 1, recID: record.recID, data: record.raw})
					continue
				}
				record.data = decompressed
				next = append(next, record)
			}
			records = next
		case "CloudWatchLogProcessing":
			var next []processingRecord
			for _, record := range records {
				var envelope struct {
					MessageType string `json:"messageType"`
					LogEvents   []struct {
						Message string `json:"message"`
					} `json:"logEvents"`
				}
				if err := json.Unmarshal(record.data, &envelope); err != nil || (envelope.MessageType != "DATA_MESSAGE" && envelope.MessageType != "CONTROL_MESSAGE") || (envelope.MessageType == "DATA_MESSAGE" && len(envelope.LogEvents) == 0) {
					message := "CloudWatch Logs message extraction failed."
					if err != nil {
						message = err.Error()
					}
					failures = append(failures, &processingFailure{typeName: "processing-failed", code: "CloudWatchLogProcessing.Failed", message: message, attempts: 1, recID: record.recID, data: record.raw})
					continue
				}
				if envelope.MessageType == "CONTROL_MESSAGE" {
					continue
				}
				var extracted bytes.Buffer
				for _, event := range envelope.LogEvents {
					extracted.WriteString(event.Message)
					extracted.WriteByte('\n')
				}
				record.data = extracted.Bytes()
				next = append(next, record)
			}
			records = next
		case "Lambda":
			var next []processingRecord
			for _, record := range records {
				transformed, partitionKeys, deliver, message, attempts := p.invokeProcessingLambda(ctx, req, processor, stream, record.recID, record.data, now)
				if message != "" {
					failures = append(failures, &processingFailure{typeName: "processing-failed", code: "Lambda.ProcessingFailed", message: message, lambdaARN: processorParameter(processor, "LambdaArn"), attempts: attempts, recID: record.recID, data: record.raw})
					continue
				}
				if deliver {
					record.data = transformed
					record.partitionKeys = partitionKeys
					next = append(next, record)
				}
			}
			records = next
		}
	}
	return records, failures
}

func deaggregateData(processor map[string]any, data []byte) ([][]byte, error) {
	if processorParameter(processor, "SubRecordType") == "DELIMITED" {
		delimiter, _ := base64.StdEncoding.DecodeString(processorParameter(processor, "Delimiter"))
		records := bytes.Split(data, delimiter)
		if len(records) > 0 && len(records[len(records)-1]) == 0 {
			records = records[:len(records)-1]
		}
		if len(records) > 500 {
			return [][]byte{data}, nil
		}
		return records, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var records [][]byte
	for {
		var record json.RawMessage
		if err := decoder.Decode(&record); err != nil {
			if err == io.EOF && len(records) > 0 {
				return records, nil
			}
			return nil, err
		}
		record = bytes.TrimSpace(record)
		if len(record) == 0 || record[0] != '{' {
			return nil, errors.New("record is not a JSON object")
		}
		records = append(records, record)
		if len(records) > 500 {
			return [][]byte{data}, nil
		}
	}
}

func (p *Pack) invokeProcessingLambda(ctx context.Context, req *spi.Request, processor map[string]any, stream, recID string, data []byte, now time.Time) ([]byte, map[string]string, bool, string, int) {
	arn := processorParameter(processor, "LambdaArn")
	name := arn[strings.Index(arn, ":function:")+len(":function:"):]
	if index := strings.IndexByte(name, ':'); index >= 0 {
		name = name[:index]
	}
	event, _ := json.Marshal(map[string]any{
		"invocationId": recID, "deliveryStreamArn": "arn:aws:firehose:" + req.Identity.Region + ":" + req.Identity.Account + ":deliverystream/" + stream, "region": req.Identity.Region,
		"records": []any{map[string]any{"recordId": recID, "approximateArrivalTimestamp": float64(now.UnixNano()) / float64(time.Second), "data": base64.StdEncoding.EncodeToString(data)}},
	})
	retries := 3
	if value := processorParameter(processor, "NumberOfRetries"); value != "" {
		retries, _ = strconv.Atoi(value)
	}
	var response *spi.Response
	var err error
	attempts := 0
	for attempt := 0; attempt <= retries; attempt++ {
		attempts++
		response, err = lambda.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: map[string]any{"FunctionName": name}, Body: io.NopCloser(bytes.NewReader(event))})
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, nil, false, err.Error(), attempts
	}
	raw, _ := response.Output["Payload"].(json.RawMessage)
	var output map[string]any
	if json.Unmarshal(raw, &output) != nil {
		return nil, nil, false, "Lambda returned an invalid response.", attempts
	}
	records, ok := output["records"].([]any)
	if !ok || len(records) != 1 {
		return nil, nil, false, "Lambda returned an invalid record count.", attempts
	}
	record, ok := records[0].(map[string]any)
	decoded, decodeErr := base64.StdEncoding.DecodeString(first(record, "data"))
	if !ok || first(record, "recordId") != recID || decodeErr != nil {
		return nil, nil, false, "Lambda returned an invalid record.", attempts
	}
	partitionKeys := map[string]string{}
	if rawMetadata, exists := record["metadata"]; exists {
		metadata, ok := rawMetadata.(map[string]any)
		rawKeys, keysExist := metadata["partitionKeys"]
		keys, keysOK := rawKeys.(map[string]any)
		if !ok || !keysExist || !keysOK {
			return nil, nil, false, "Lambda returned invalid partition keys.", attempts
		}
		for key, rawValue := range keys {
			value, ok := rawValue.(string)
			if !ok || key == "" || value == "" {
				return nil, nil, false, "Lambda returned invalid partition keys.", attempts
			}
			partitionKeys[key] = value
		}
	}
	switch first(record, "result") {
	case "Ok":
		return decoded, partitionKeys, true, "", attempts
	case "Dropped":
		return nil, nil, false, "", attempts
	case "ProcessingFailed":
		return nil, nil, false, "Lambda processing failed.", attempts
	default:
		return nil, nil, false, "Lambda returned an invalid result.", attempts
	}
}

func (p *Pack) deliverProcessingFailure(ctx context.Context, req *spi.Request, bucket, prefix, kmsARN, stream, version string, now time.Time, failure *processingFailure) {
	if prefix == "" {
		prefix = failure.typeName + "/"
	}
	prefix = strings.ReplaceAll(prefix, "!{firehose:error-output-type}", failure.typeName)
	timestamp := now.UTC().Format(time.RFC3339Nano)
	payloadFields := map[string]any{
		"attemptsMade": strconv.Itoa(failure.attempts), "arrivalTimestamp": timestamp, "errorCode": failure.code, "errorMessage": failure.message,
		"attemptEndingTimestamp": timestamp, "rawData": base64.StdEncoding.EncodeToString(failure.data),
	}
	if failure.lambdaARN != "" {
		payloadFields["lambdaArn"] = failure.lambdaARN
	}
	payload, _ := json.Marshal(payloadFields)
	key := p.evaluatedS3Prefix(prefix, now) + stream + "-" + version + "-" + now.Format("2006-01-02-15-04-05-") + failure.recID
	p.deliverS3Object(ctx, req, bucket, key, kmsARN, payload)
}

func (p *Pack) deliverS3Object(ctx context.Context, req *spi.Request, bucket, key, kmsARN string, data []byte) {
	info, err := p.deps.Blobs.Put(ctx, req.Identity.Account+"/"+req.Identity.Region+"/"+bucket+"/"+key, bytes.NewReader(data))
	if err != nil {
		return
	}
	etag := `"` + info.MD5 + `"`
	mtime := p.deps.Clock.Now().UTC().Format(http.TimeFormat)
	metadata := map[string]any{"etag": etag, "size": info.Size, "md5": info.MD5, "mtime": mtime, "deleteMarker": false}
	if kmsARN != "" {
		metadata["serverSideEncryption"] = "aws:kms"
		metadata["ssekmsKeyId"] = kmsARN
	}
	meta, _ := json.Marshal(metadata)
	_ = p.col(req, "objects").Put(ctx, bucket+"/"+key, meta)
}

var (
	firehoseTimestampPrefix   = regexp.MustCompile(`!\{timestamp:([^}]*)\}`)
	firehosePrefixExpression  = regexp.MustCompile(`!\{([^}:]+):([^}]*)\}`)
	firehoseFileExtension     = regexp.MustCompile(`^\.[0-9a-z!\-_.*'()]+$`)
	firehoseStreamName        = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	firehoseTagKey            = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]+$`)
	firehoseTagValue          = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]*$`)
	firehoseKMSKeyARN         = regexp.MustCompile(`^arn:[^:]+:kms:[a-zA-Z0-9\-]+:\d{12}:key/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseDestinationKMSARN = regexp.MustCompile(`^arn:.*:kms:[a-zA-Z0-9\-]+:\d{12}:(key|alias)/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseTimeZone          = regexp.MustCompile(`^[a-zA-Z/_]+$`)
	firehoseBucketARN         = regexp.MustCompile(`^arn:.*:s3:::[\w.\-]{1,255}$`)
	firehoseRoleARN           = regexp.MustCompile(`^arn:.*:iam::\d{12}:role/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseKinesisStreamARN  = regexp.MustCompile(`^arn:.*:kinesis:[a-zA-Z0-9\-]+:\d{12}:stream/[a-zA-Z0-9_.-]+$`)
	firehoseLambdaARN         = regexp.MustCompile(`^arn:.*:lambda:[a-zA-Z0-9\-]+:\d{12}:function:[a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)?$`)
	firehoseLogGroup          = regexp.MustCompile(`^[.\-_/#A-Za-z0-9]*$`)
	firehoseLogStream         = regexp.MustCompile(`^[^:*]*$`)
	firehoseDestinationID     = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)
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

func (p *Pack) evaluatedDynamicS3Prefix(prefix string, now time.Time, partitionKeys map[string]string) (string, error) {
	missing := ""
	prefix = firehosePrefixExpression.ReplaceAllStringFunc(prefix, func(expression string) string {
		match := firehosePrefixExpression.FindStringSubmatch(expression)
		if match[1] != "partitionKeyFromLambda" {
			return expression
		}
		value, ok := partitionKeys[match[2]]
		if !ok {
			missing = match[2]
			return expression
		}
		return value
	})
	if missing != "" {
		return "", errors.New("missing Lambda partition key: " + missing)
	}
	return p.evaluatedS3Prefix(prefix, now), nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
