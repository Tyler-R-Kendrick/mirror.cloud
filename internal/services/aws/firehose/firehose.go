// Package firehose stores delivery streams and delivers direct puts or local Kinesis and MSK source records to S3, HTTP, OpenSearch, Splunk, or Redshift destinations.
package firehose

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/golang/snappy"
	"github.com/itchyny/gojq"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	kafkaservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kafka"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/logs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/opensearch"
	redshiftservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/redshift"
	s3tablesservice "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3tables"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/secretsmanager"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.firehose", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Firehose-lite.
type Pack struct {
	deps          spi.Deps
	httpClient    *http.Client
	wake          chan struct{}
	stop          chan struct{}
	done          chan struct{}
	retryOnce     sync.Once
	closeOnce     sync.Once
	cancelKinesis func()
	cancelMSK     func()
}

type httpRetry struct {
	Stream       string
	Destination  string
	RequestID    string
	DataKey      string
	Next         time.Time
	Expires      time.Time
	Retries      int
	ErrorCode    string
	ErrorMessage string
}

type httpRetryPayload struct {
	BackupRecordIDs []string
	BackupData      [][]byte
	ProcessedData   [][]byte
	Arrivals        []time.Time
}

type httpBuffer struct {
	Stream      string
	Destination string
	RequestID   string
	DataKey     string
	Size        int
	Order       int
	Next        time.Time
}

type httpBufferItem struct {
	Key    string
	Buffer httpBuffer
}

type searchWork struct {
	Stream       string
	DataKey      string
	State        string
	Size         int
	Order        int
	Next         time.Time
	Expires      time.Time
	Retries      int
	ErrorCode    string
	ErrorMessage string
}

type redshiftWork struct {
	Stream       string
	DataKey      string
	Next         time.Time
	Expires      time.Time
	Retries      int
	ErrorMessage string
}

type searchPayload struct {
	RecordIDs []string
	RawData   [][]byte
	Data      [][]byte
	Arrivals  []time.Time
}

type searchWorkItem struct {
	Key  string
	Work searchWork
}

const destinationID = "destinationId-000000000001"

var firehoseEncryptedPrefix = []byte("mirror-firehose-kms-v1:")

const (
	maxRecordBytes = 1000 * 1024
	maxBatchBytes  = 4 * 1024 * 1024
)

// New constructs the pack.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), httpClient: &http.Client{
		Timeout: 3 * time.Minute,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
	if d.Bus != nil {
		p.cancelKinesis = d.Bus.Subscribe("kinesis", p.consumeKinesis)
		p.cancelMSK = d.Bus.Subscribe("kafka", p.consumeMSK)
	}
	if p.hasHTTPWork(context.Background()) {
		p.startRetryLoop()
	}
	return p
}

// Close stops the HTTP retry worker.
func (p *Pack) Close() error {
	p.startRetryLoop()
	p.closeOnce.Do(func() {
		if p.cancelKinesis != nil {
			p.cancelKinesis()
		}
		if p.cancelMSK != nil {
			p.cancelMSK()
		}
		close(p.stop)
	})
	<-p.done
	return nil
}

func (p *Pack) consumeMSK(ctx context.Context, payload []byte) {
	var event struct {
		Account, Region, ClusterARN, Topic string
		Message                            kafkaservice.Message
	}
	if json.Unmarshal(payload, &event) != nil || event.Account == "" || event.Region == "" || event.ClusterARN == "" || event.Topic == "" || event.Message.Timestamp.IsZero() {
		return
	}
	req := &spi.Request{Identity: spi.Identity{Account: event.Account, Region: event.Region}}
	streams, _, err := p.col(req, "fh").List(ctx, "", "", 0)
	if err != nil {
		return
	}
	for _, stream := range streams {
		var configuration map[string]any
		if json.Unmarshal(stream.Value, &configuration) != nil || first(configuration, "DeliveryStreamType") != "MSKAsSource" {
			continue
		}
		source, _ := configuration["MSKSourceConfiguration"].(map[string]any)
		if first(source, "MSKClusterARN") != event.ClusterARN || first(source, "TopicName") != event.Topic || event.Message.Timestamp.Before(mskStart(source, configuration["CreateTimestamp"])) {
			continue
		}
		_, _ = p.putOne(ctx, req, stream.Key, map[string]any{"Data": base64.StdEncoding.EncodeToString(event.Message.Data)}, event.Message.Data, "")
	}
}

func (p *Pack) replayMSK(ctx context.Context, req *spi.Request, stream string, source map[string]any, created any) {
	messages, err := kafkaservice.New(p.deps).Messages(ctx, req.Identity, first(source, "MSKClusterARN"), first(source, "TopicName"), mskStart(source, created))
	if err != nil {
		return
	}
	for _, message := range messages {
		_, _ = p.putOne(ctx, req, stream, map[string]any{"Data": base64.StdEncoding.EncodeToString(message.Data)}, message.Data, "")
	}
}

func mskStart(source map[string]any, created any) time.Time {
	value := created
	if explicit, ok := source["ReadFromTimestamp"]; ok {
		value = explicit
	}
	seconds, _ := value.(float64)
	return time.Unix(0, int64(seconds*float64(time.Second))).UTC()
}

func (p *Pack) consumeKinesis(ctx context.Context, payload []byte) {
	var event struct {
		Account, Region, StreamName string
		Record                      map[string]any
	}
	if json.Unmarshal(payload, &event) != nil || event.Account == "" || event.Region == "" || event.StreamName == "" {
		return
	}
	req := &spi.Request{Identity: spi.Identity{Account: event.Account, Region: event.Region}}
	streams, _, err := p.col(req, "fh").List(ctx, "", "", 0)
	if err != nil {
		return
	}
	data, valid := recordData(event.Record)
	if !valid {
		return
	}
	records := deaggregateKPL(data)
	for _, stream := range streams {
		var configuration map[string]any
		if json.Unmarshal(stream.Value, &configuration) != nil || first(configuration, "DeliveryStreamType") != "KinesisStreamAsSource" {
			continue
		}
		source, _ := configuration["KinesisStreamSourceConfiguration"].(map[string]any)
		parts := strings.SplitN(first(source, "KinesisStreamARN"), ":", 6)
		if len(parts) != 6 || parts[3] != event.Region || parts[4] != event.Account || parts[5] != "stream/"+event.StreamName {
			continue
		}
		for _, data := range records {
			_, _ = p.putOne(ctx, req, stream.Key, map[string]any{"Data": base64.StdEncoding.EncodeToString(data)}, data, "")
		}
	}
}

var kplMagic = []byte{0xf3, 0x89, 0x9a, 0xc2}

type kplRecord struct {
	data                               []byte
	partition, explicit                uint64
	hasPartition, hasExplicit, hasData bool
}

func deaggregateKPL(data []byte) [][]byte {
	if len(data) < len(kplMagic)+md5.Size || !bytes.HasPrefix(data, kplMagic) {
		return [][]byte{data}
	}
	message := data[len(kplMagic) : len(data)-md5.Size]
	digest := md5.Sum(message)
	if !bytes.Equal(digest[:], data[len(data)-md5.Size:]) {
		return [][]byte{data}
	}
	partitionKeys, explicitKeys := 0, 0
	var records []kplRecord
	valid := eachProtoField(message, func(field, wire int, value []byte) bool {
		switch {
		case field == 1 && wire == 2:
			partitionKeys++
		case field == 2 && wire == 2:
			explicitKeys++
		case field == 3 && wire == 2:
			record, ok := parseKPLRecord(value)
			if !ok {
				return false
			}
			records = append(records, record)
		}
		return true
	})
	result := make([][]byte, 0, len(records))
	for _, record := range records {
		if !valid || !record.hasPartition || !record.hasData || record.partition >= uint64(partitionKeys) || (record.hasExplicit && record.explicit >= uint64(explicitKeys)) {
			return [][]byte{data}
		}
		result = append(result, record.data)
	}
	if len(result) == 0 {
		return [][]byte{data}
	}
	return result
}

func parseKPLRecord(message []byte) (record kplRecord, valid bool) {
	valid = eachProtoField(message, func(field, wire int, value []byte) bool {
		switch {
		case field == 1 && wire == 0:
			record.partition, _ = binary.Uvarint(value)
			record.hasPartition = true
		case field == 2 && wire == 0:
			record.explicit, _ = binary.Uvarint(value)
			record.hasExplicit = true
		case field == 3 && wire == 2:
			record.data, record.hasData = value, true
		}
		return true
	})
	return
}

func eachProtoField(message []byte, visit func(field, wire int, value []byte) bool) bool {
	for len(message) > 0 {
		key, n := binary.Uvarint(message)
		if n <= 0 || key>>3 == 0 {
			return false
		}
		message = message[n:]
		wire := int(key & 7)
		var value []byte
		switch wire {
		case 0:
			_, n = binary.Uvarint(message)
			if n <= 0 {
				return false
			}
			value, message = message[:n], message[n:]
		case 1:
			if len(message) < 8 {
				return false
			}
			value, message = message[:8], message[8:]
		case 2:
			size, sizeBytes := binary.Uvarint(message)
			if sizeBytes <= 0 || size > uint64(len(message)-sizeBytes) {
				return false
			}
			message = message[sizeBytes:]
			value, message = message[:int(size)], message[int(size):]
		case 5:
			if len(message) < 4 {
				return false
			}
			value, message = message[:4], message[4:]
		default:
			return false
		}
		if !visit(int(key>>3), wire, value) {
			return false
		}
	}
	return true
}

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
		var source, directPutSource, mskSource, databaseSource map[string]any
		switch streamType {
		case "DirectPut":
			if req.Input["KinesisStreamSourceConfiguration"] != nil || req.Input["MSKSourceConfiguration"] != nil || req.Input["DatabaseSourceConfiguration"] != nil {
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
			if req.Input["DirectPutSourceConfiguration"] != nil || req.Input["MSKSourceConfiguration"] != nil || req.Input["DatabaseSourceConfiguration"] != nil {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			var ok bool
			source, ok = req.Input["KinesisStreamSourceConfiguration"].(map[string]any)
			if !ok || len(first(source, "KinesisStreamARN")) > 512 || !firehoseKinesisStreamARN.MatchString(first(source, "KinesisStreamARN")) || !validRoleARN(first(source, "RoleARN")) {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		case "MSKAsSource":
			if req.Input["DirectPutSourceConfiguration"] != nil || req.Input["KinesisStreamSourceConfiguration"] != nil || req.Input["DatabaseSourceConfiguration"] != nil {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			var ok bool
			mskSource, ok = req.Input["MSKSourceConfiguration"].(map[string]any)
			authentication, authOK := mskSource["AuthenticationConfiguration"].(map[string]any)
			cluster, topic := first(mskSource, "MSKClusterARN"), first(mskSource, "TopicName")
			connectivity := first(authentication, "Connectivity")
			if !ok || !authOK || len(cluster) > 512 || !firehoseMSKClusterARN.MatchString(cluster) || len(topic) > 255 || !firehoseMSKTopic.MatchString(topic) || (connectivity != "PUBLIC" && connectivity != "PRIVATE") || !validRoleARN(first(authentication, "RoleARN")) {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if readFrom, exists := mskSource["ReadFromTimestamp"]; exists {
				value, valid := readFrom.(float64)
				if !valid || math.IsNaN(value) || math.IsInf(value, 0) {
					return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "DatabaseAsSource":
			if req.Input["DirectPutSourceConfiguration"] != nil || req.Input["KinesisStreamSourceConfiguration"] != nil || req.Input["MSKSourceConfiguration"] != nil {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			var valid bool
			databaseSource, valid = validDatabaseSource(req.Input["DatabaseSourceConfiguration"])
			if !valid {
				return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
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
		if mskSource != nil {
			rec["MSKSourceConfiguration"] = maps.Clone(mskSource)
		}
		if databaseSource != nil {
			rec["DatabaseSourceConfiguration"] = maps.Clone(databaseSource)
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
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); ok {
			return nil, &spi.Fault{Code: "ResourceInUseException", HTTPStatus: 400, Fault: "client"}
		}
		if _, err := p.ensureEncryptionKey(ctx, req, encryption); err != nil {
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
		if err := putTags(ctx, p.col(req, "fhtag"), name, tags); err != nil {
			return nil, err
		}
		if mskSource != nil {
			p.replayMSK(ctx, req, name, mskSource, timestamp)
		}
		return &spi.Response{Output: map[string]any{"DeliveryStreamARN": arn}}, nil
	case "DeleteDeliveryStream":
		if _, ok, _ := p.col(req, "fh").Get(ctx, name); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		p.deleteHTTPStreamWork(ctx, req, name)
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
		if !directPutStream(stream) {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if requiresCloudWatchLogsSource(stream) && req.SourceService != "aws.logs" {
			return nil, invalidSource(req, name)
		}
		decoded, valid := recordData(req.Input["Record"])
		if !valid {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		keyID, err := p.recordEncryptionKey(ctx, req, stream)
		if err != nil {
			return nil, err
		}
		id, err := p.putOne(ctx, req, name, req.Input["Record"], decoded, keyID)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"RecordId": id, "Encrypted": streamEncrypted(stream)}}, nil
	case "PutRecordBatch":
		stream, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		if !directPutStream(stream) {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if requiresCloudWatchLogsSource(stream) && req.SourceService != "aws.logs" {
			return nil, invalidSource(req, name)
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
		keyID, err := p.recordEncryptionKey(ctx, req, stream)
		if err != nil {
			return nil, err
		}
		resp := make([]any, 0, len(recs))
		ids := make([]string, len(recs))
		for i, rec := range recs {
			ids[i], err = p.storeOne(ctx, req, name, rec, decoded[i], keyID)
			if err != nil {
				return nil, err
			}
			resp = append(resp, map[string]any{"RecordId": ids[i]})
		}
		p.deliver(ctx, req, name, ids, decoded)
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
			redshift, _ := rec["RedshiftDestinationConfiguration"].(map[string]any)
			redshiftBackupEnabled := first(redshift, "S3BackupMode") == "Enabled"
			splunk, _ := rec["SplunkDestinationConfiguration"].(map[string]any)
			splunkAllEvents := first(splunk, "S3BackupMode") == "AllEvents"
			copyDest(rec, req.Input, "Update")
			extended, _ = rec["ExtendedS3DestinationConfiguration"].(map[string]any)
			if backupEnabled && first(extended, "S3BackupMode") != "Enabled" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if dynamicEnabled != dynamicPartitioningEnabled(extended) {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			redshift, _ = rec["RedshiftDestinationConfiguration"].(map[string]any)
			if redshiftBackupEnabled && first(redshift, "S3BackupMode") != "Enabled" {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			splunk, _ = rec["SplunkDestinationConfiguration"].(map[string]any)
			if splunkAllEvents && first(splunk, "S3BackupMode") != "AllEvents" {
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
		stored, ok, _ := p.col(req, "fh").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var current map[string]any
		_ = json.Unmarshal(stored, &current)
		if first(current, "DeliveryStreamType") != "DirectPut" {
			return nil, &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if req.Operation == "StartDeliveryStreamEncryption" {
			if _, err := p.ensureEncryptionKey(ctx, req, encryption); err != nil {
				return nil, err
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

func (p *Pack) ensureEncryptionKey(ctx context.Context, req *spi.Request, encryption map[string]any) (string, error) {
	if first(encryption, "Status") != "ENABLED" {
		return "", nil
	}
	keyID := first(encryption, "KeyARN")
	if first(encryption, "KeyType") == "AWS_OWNED_CMK" {
		if stored, ok, _ := p.col(req, "fhkms").Get(ctx, "aws-owned"); ok {
			keyID = string(stored)
		} else {
			created, err := kms.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "CreateKey", Input: map[string]any{}})
			if err != nil {
				return "", invalidKMSResource(err)
			}
			keyID = first(created.Output["KeyMetadata"].(map[string]any), "KeyId")
			if err := p.col(req, "fhkms").Put(ctx, "aws-owned", []byte(keyID)); err != nil {
				return "", err
			}
		}
	}
	described, err := kms.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "DescribeKey", Input: map[string]any{"KeyId": keyID}})
	if err != nil {
		return "", invalidKMSResource(err)
	}
	if first(described.Output["KeyMetadata"].(map[string]any), "KeyState") != "Enabled" {
		return "", invalidKMSResource(errors.New("KMS key is not enabled"))
	}
	return keyID, nil
}

func (p *Pack) recordEncryptionKey(ctx context.Context, req *spi.Request, stream []byte) (string, error) {
	var rec map[string]any
	_ = json.Unmarshal(stream, &rec)
	encryption, _ := rec["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	return p.ensureEncryptionKey(ctx, req, encryption)
}

func (p *Pack) encryptAtRest(ctx context.Context, req *spi.Request, keyID string, plaintext []byte) ([]byte, error) {
	if keyID == "" {
		return plaintext, nil
	}
	response, err := kms.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Encrypt", Input: map[string]any{"KeyId": keyID, "Plaintext": plaintext}})
	if err != nil {
		return nil, invalidKMSResource(err)
	}
	ciphertext, _ := response.Output["CiphertextBlob"].([]byte)
	return append(bytes.Clone(firehoseEncryptedPrefix), ciphertext...), nil
}

func (p *Pack) protectStreamData(ctx context.Context, req *spi.Request, stream string, plaintext []byte) ([]byte, error) {
	record, ok, err := p.col(req, "fh").Get(ctx, stream)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("delivery stream no longer exists")
	}
	keyID, err := p.recordEncryptionKey(ctx, req, record)
	if err != nil {
		return nil, err
	}
	return p.encryptAtRest(ctx, req, keyID, plaintext)
}

func (p *Pack) decryptAtRest(ctx context.Context, req *spi.Request, stored []byte) ([]byte, error) {
	if !bytes.HasPrefix(stored, firehoseEncryptedPrefix) {
		return stored, nil
	}
	response, err := kms.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Decrypt", Input: map[string]any{"CiphertextBlob": stored[len(firehoseEncryptedPrefix):]}})
	if err != nil {
		return nil, invalidKMSResource(err)
	}
	plaintext, _ := response.Output["Plaintext"].([]byte)
	return plaintext, nil
}

func invalidKMSResource(err error) *spi.Fault {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return &spi.Fault{Code: "InvalidKMSResourceException", Message: message, HTTPStatus: 400, Fault: "client"}
}

func streamEncrypted(b []byte) bool {
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	encryption, _ := rec["DeliveryStreamEncryptionConfiguration"].(map[string]any)
	return first(encryption, "Status") == "ENABLED"
}

func directPutStream(b []byte) bool {
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return first(rec, "DeliveryStreamType") == "DirectPut"
}

func requiresCloudWatchLogsSource(b []byte) bool {
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "HttpEndpointDestinationConfiguration"} {
		if destination, ok := rec[key].(map[string]any); ok && hasProcessor(destination, "Decompression") {
			return true
		}
	}
	return false
}

func invalidSource(req *spi.Request, stream string) *spi.Fault {
	return &spi.Fault{
		Code:       "InvalidSourceException",
		Message:    fmt.Sprintf("Put to Firehose failed for AccountId: %s, FirehoseName: %s because the request is not originating from allowed source types.", req.Identity.Account, stream),
		HTTPStatus: 400,
		Fault:      "client",
	}
}

func (p *Pack) putOne(ctx context.Context, req *spi.Request, name string, rec any, decoded []byte, keyID string) (string, error) {
	id, err := p.storeOne(ctx, req, name, rec, decoded, keyID)
	if err != nil {
		return "", err
	}
	p.deliver(ctx, req, name, []string{id}, [][]byte{decoded})
	return id, nil
}

func (p *Pack) storeOne(ctx context.Context, req *spi.Request, name string, rec any, decoded []byte, keyID string) (string, error) {
	id := p.deps.Rand.Hex(16)
	payload := map[string]any{"Record": rec, "Decoded": string(decoded)}
	b, _ := json.Marshal(payload)
	b, err := p.encryptAtRest(ctx, req, keyID, b)
	if err != nil {
		return "", err
	}
	return id, p.col(req, "fhrec:"+name).Put(ctx, id, b)
}

func copyDest(rec, in map[string]any, suffix string) {
	for _, base := range []string{"S3Destination", "ExtendedS3Destination", "HttpEndpointDestination", "ElasticsearchDestination", "RedshiftDestination", "SplunkDestination", "IcebergDestination"} {
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
		if s3Patch, ok := patch["S3Update"].(map[string]any); ok {
			s3, _ := destination["S3Configuration"].(map[string]any)
			if s3 == nil {
				s3 = map[string]any{}
			}
			maps.Copy(s3, s3Patch)
			destination["S3Configuration"] = s3
			delete(destination, "S3Update")
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
	if configuration, ok := description["MSKSourceConfiguration"].(map[string]any); ok {
		source := maps.Clone(configuration)
		created := description["CreateTimestamp"]
		source["DeliveryStartTimestamp"] = source["ReadFromTimestamp"]
		if source["DeliveryStartTimestamp"] == nil {
			source["DeliveryStartTimestamp"] = created
		}
		delete(source, "ReadFromTimestamp")
		description["Source"] = map[string]any{"MSKSourceDescription": source}
		delete(description, "MSKSourceConfiguration")
	}
	if configuration, ok := description["DatabaseSourceConfiguration"].(map[string]any); ok {
		description["Source"] = map[string]any{"DatabaseSourceDescription": maps.Clone(configuration)}
		delete(description, "DatabaseSourceConfiguration")
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
	if configuration, ok := description["HttpEndpointDestinationConfiguration"].(map[string]any); ok {
		if endpoint, ok := configuration["EndpointConfiguration"].(map[string]any); ok {
			endpoint = maps.Clone(endpoint)
			delete(endpoint, "AccessKey")
			configuration["EndpointConfiguration"] = endpoint
		}
		if s3, ok := configuration["S3Configuration"].(map[string]any); ok {
			describeS3Configuration(s3)
			configuration["S3DestinationDescription"] = s3
			delete(configuration, "S3Configuration")
		}
		if configuration["BufferingHints"] == nil {
			configuration["BufferingHints"] = map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}
		}
		if configuration["RetryOptions"] == nil {
			configuration["RetryOptions"] = map[string]any{"DurationInSeconds": 300}
		}
		if first(configuration, "S3BackupMode") == "" {
			configuration["S3BackupMode"] = "FailedDataOnly"
		}
		destination["HttpEndpointDestinationDescription"] = configuration
		delete(description, "HttpEndpointDestinationConfiguration")
	}
	if configuration, ok := description["ElasticsearchDestinationConfiguration"].(map[string]any); ok {
		if s3, ok := configuration["S3Configuration"].(map[string]any); ok {
			describeS3Configuration(s3)
			configuration["S3DestinationDescription"] = s3
			delete(configuration, "S3Configuration")
		}
		if configuration["BufferingHints"] == nil {
			configuration["BufferingHints"] = map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}
		}
		if configuration["RetryOptions"] == nil {
			configuration["RetryOptions"] = map[string]any{"DurationInSeconds": 300}
		}
		if first(configuration, "IndexRotationPeriod") == "" {
			configuration["IndexRotationPeriod"] = "OneDay"
		}
		if first(configuration, "S3BackupMode") == "" {
			configuration["S3BackupMode"] = "FailedDocumentsOnly"
		}
		if configuration["DocumentIdOptions"] == nil {
			configuration["DocumentIdOptions"] = map[string]any{"DefaultDocumentIdFormat": "FIREHOSE_DEFAULT"}
		}
		destination["ElasticsearchDestinationDescription"] = configuration
		delete(description, "ElasticsearchDestinationConfiguration")
	}
	if configuration, ok := description["RedshiftDestinationConfiguration"].(map[string]any); ok {
		configuration = maps.Clone(configuration)
		delete(configuration, "Password")
		if s3, ok := configuration["S3Configuration"].(map[string]any); ok {
			describeS3Configuration(s3)
			configuration["S3DestinationDescription"] = s3
			delete(configuration, "S3Configuration")
		}
		if backup, ok := configuration["S3BackupConfiguration"].(map[string]any); ok {
			describeS3Configuration(backup)
			configuration["S3BackupDescription"] = backup
			delete(configuration, "S3BackupConfiguration")
		}
		if configuration["RetryOptions"] == nil {
			configuration["RetryOptions"] = map[string]any{"DurationInSeconds": 3600}
		}
		if first(configuration, "S3BackupMode") == "" {
			configuration["S3BackupMode"] = "Disabled"
		}
		destination["RedshiftDestinationDescription"] = configuration
		delete(description, "RedshiftDestinationConfiguration")
	}
	if configuration, ok := description["SplunkDestinationConfiguration"].(map[string]any); ok {
		configuration = maps.Clone(configuration)
		delete(configuration, "HECToken")
		if s3, ok := configuration["S3Configuration"].(map[string]any); ok {
			describeS3Configuration(s3)
			configuration["S3DestinationDescription"] = s3
			delete(configuration, "S3Configuration")
		}
		if configuration["BufferingHints"] == nil {
			configuration["BufferingHints"] = map[string]any{"IntervalInSeconds": 60, "SizeInMBs": 5}
		}
		if configuration["RetryOptions"] == nil {
			configuration["RetryOptions"] = map[string]any{"DurationInSeconds": 300}
		}
		if configuration["HECAcknowledgmentTimeoutInSeconds"] == nil {
			configuration["HECAcknowledgmentTimeoutInSeconds"] = 300
		}
		if first(configuration, "S3BackupMode") == "" {
			configuration["S3BackupMode"] = "FailedEventsOnly"
		}
		destination["SplunkDestinationDescription"] = configuration
		delete(description, "SplunkDestinationConfiguration")
	}
	if configuration, ok := description["IcebergDestinationConfiguration"].(map[string]any); ok {
		configuration = maps.Clone(configuration)
		if s3, ok := configuration["S3Configuration"].(map[string]any); ok {
			describeS3Configuration(s3)
			configuration["S3DestinationDescription"] = s3
			delete(configuration, "S3Configuration")
		}
		if configuration["BufferingHints"] == nil {
			configuration["BufferingHints"] = map[string]any{"IntervalInSeconds": 300, "SizeInMBs": 5}
		}
		if configuration["RetryOptions"] == nil {
			configuration["RetryOptions"] = map[string]any{"DurationInSeconds": 300}
		}
		if first(configuration, "S3BackupMode") == "" {
			configuration["S3BackupMode"] = "FailedDataOnly"
		}
		destination["IcebergDestinationDescription"] = configuration
		delete(description, "IcebergDestinationConfiguration")
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

func validDatabaseSource(value any) (map[string]any, bool) {
	configuration, ok := value.(map[string]any)
	authentication, authOK := configuration["DatabaseSourceAuthenticationConfiguration"].(map[string]any)
	secrets, secretsOK := authentication["SecretsManagerConfiguration"].(map[string]any)
	vpc, vpcOK := configuration["DatabaseSourceVPCConfiguration"].(map[string]any)
	enabled, enabledOK := secrets["Enabled"].(bool)
	engine, endpoint := first(configuration, "Type"), first(configuration, "Endpoint")
	port, portOK := inputInteger(configuration["Port"], 0, 65535)
	watermark, sslMode := first(configuration, "SnapshotWatermarkTable"), first(configuration, "SSLMode")
	service, role, secret := first(vpc, "VpcEndpointServiceName"), first(secrets, "RoleARN"), first(secrets, "SecretARN")
	if !ok || !authOK || !secretsOK || !vpcOK || !enabledOK || strings.TrimSpace(endpoint) == "" || utf8.RuneCountInString(endpoint) > 255 || !validDatabaseString(watermark, 129, false) || !portOK || (engine == "MySQL" && port != 3306) || (engine == "PostgreSQL" && port != 5432) || (engine != "MySQL" && engine != "PostgreSQL") || (sslMode != "" && sslMode != "Disabled" && sslMode != "Enabled") {
		return nil, false
	}
	if len(service) < 47 || len(service) > 255 || !firehoseVPCEndpointService.MatchString(service) || !validDatabaseList(configuration["Databases"], 64, true) || !validDatabaseList(configuration["Tables"], 129, true) || !validDatabaseList(configuration["Columns"], 194, false) || !validDatabaseStrings(configuration["SurrogateKeys"], 1024) {
		return nil, false
	}
	if (role != "" && !validRoleARN(role)) || (secret != "" && (len(secret) > 2048 || !firehoseSecretARN.MatchString(secret))) || (enabled && secret == "") {
		return nil, false
	}
	return configuration, true
}

func validDatabaseList(value any, maximum int, required bool) bool {
	if value == nil {
		return !required
	}
	patterns, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for _, key := range []string{"Include", "Exclude"} {
		if raw := patterns[key]; raw != nil {
			items, ok := raw.([]any)
			if !ok {
				return false
			}
			for _, item := range items {
				pattern, ok := item.(string)
				if !ok || !validDatabaseString(pattern, maximum, false) {
					return false
				}
			}
		}
	}
	return true
}

func validDatabaseStrings(value any, maximum int) bool {
	if value == nil {
		return true
	}
	items, ok := value.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		text, ok := item.(string)
		if !ok || !validDatabaseString(text, maximum, true) {
			return false
		}
	}
	return true
}

func validDatabaseString(value string, maximum int, noWhitespace bool) bool {
	length := utf8.RuneCountInString(value)
	if length < 1 || length > maximum {
		return false
	}
	for _, char := range value {
		if char == 0 || char > '\uffff' || (noWhitespace && unicode.IsSpace(char)) {
			return false
		}
	}
	return true
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
	destinationType := ""
	for _, key := range []string{"S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "HttpEndpointDestinationConfiguration", "ElasticsearchDestinationConfiguration", "RedshiftDestinationConfiguration", "SplunkDestinationConfiguration", "IcebergDestinationConfiguration"} {
		if value, exists := rec[key]; exists {
			var ok bool
			destination, ok = value.(map[string]any)
			if !ok {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			count++
			destinationType = key
		}
	}
	if count != 1 {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if destinationType == "HttpEndpointDestinationConfiguration" {
		return validateHTTPEndpointDestination(destination, region)
	}
	if destinationType == "ElasticsearchDestinationConfiguration" {
		return validateElasticsearchDestination(destination, region)
	}
	if destinationType == "RedshiftDestinationConfiguration" {
		return validateRedshiftDestination(destination, region)
	}
	if destinationType == "SplunkDestinationConfiguration" {
		return validateSplunkDestination(destination, region)
	}
	if destinationType == "IcebergDestinationConfiguration" {
		return validateIcebergDestination(destination, region)
	}
	if err := validateS3Configuration(destination, region, destinationType == "ExtendedS3DestinationConfiguration"); err != nil {
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

func validateIcebergDestination(destination map[string]any, region string) error {
	catalog, catalogOK := destination["CatalogConfiguration"].(map[string]any)
	s3, s3OK := destination["S3Configuration"].(map[string]any)
	if !catalogOK || !s3OK || !validRoleARN(first(destination, "RoleARN")) || !firehoseGlueCatalogARN.MatchString(first(catalog, "CatalogARN")) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(s3, region, false); err != nil {
		return err
	}
	if hints, ok := destination["BufferingHints"].(map[string]any); destination["BufferingHints"] != nil && (!ok || !optionalInteger(hints, "IntervalInSeconds", 0, 900) || !optionalInteger(hints, "SizeInMBs", 1, 128)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if retry, ok := destination["RetryOptions"].(map[string]any); destination["RetryOptions"] != nil && (!ok || !optionalInteger(retry, "DurationInSeconds", 0, 7200)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if mode := first(destination, "S3BackupMode"); mode != "" && mode != "FailedDataOnly" && mode != "AllData" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if appendOnly, exists := destination["AppendOnly"]; exists {
		if _, ok := appendOnly.(bool); !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	tables, ok := destination["DestinationTableConfigurationList"].([]any)
	if destination["DestinationTableConfigurationList"] != nil && !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	seen := map[string]bool{}
	for _, raw := range tables {
		table, ok := raw.(map[string]any)
		database, name := first(table, "DestinationDatabaseName"), first(table, "DestinationTableName")
		key := database + "." + name
		if !ok || !firehoseIcebergName.MatchString(database) || !firehoseIcebergName.MatchString(name) || len(database) > 255 || len(name) > 255 || seen[key] || len(first(table, "S3ErrorOutputPrefix")) > 1024 {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		seen[key] = true
		if keys := table["UniqueKeys"]; keys != nil && !validIcebergKeys(keys) {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	return validateProcessingConfiguration(destination["ProcessingConfiguration"])
}

func validIcebergKeys(value any) bool {
	keys, ok := value.([]any)
	if !ok || len(keys) == 0 {
		return false
	}
	for _, value := range keys {
		key, ok := value.(string)
		if !ok || len(key) > 1024 || strings.TrimSpace(key) != key || key == "" {
			return false
		}
	}
	return true
}

func validateElasticsearchDestination(destination map[string]any, region string) error {
	index, domainARN, endpoint := first(destination, "IndexName"), first(destination, "DomainARN"), first(destination, "ClusterEndpoint")
	if len(index) < 1 || len(index) > 80 || !validRoleARN(first(destination, "RoleARN")) || (domainARN == "") == (endpoint == "") {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if domainARN != "" && (len(domainARN) > 512 || !firehoseDomainARN.MatchString(domainARN)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if len(endpoint) > 512 || err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if typeName := first(destination, "TypeName"); len(typeName) > 100 {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	switch first(destination, "IndexRotationPeriod") {
	case "", "NoRotation", "OneHour", "OneDay", "OneWeek", "OneMonth":
	default:
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if hints, ok := destination["BufferingHints"].(map[string]any); destination["BufferingHints"] != nil && (!ok || !optionalInteger(hints, "IntervalInSeconds", 0, 900) || !optionalInteger(hints, "SizeInMBs", 1, 100)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if retry, ok := destination["RetryOptions"].(map[string]any); destination["RetryOptions"] != nil && (!ok || !optionalInteger(retry, "DurationInSeconds", 0, 7200)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if mode := first(destination, "S3BackupMode"); mode != "" && mode != "FailedDocumentsOnly" && mode != "AllDocuments" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if options, ok := destination["DocumentIdOptions"].(map[string]any); destination["DocumentIdOptions"] != nil && (!ok || (first(options, "DefaultDocumentIdFormat") != "FIREHOSE_DEFAULT" && first(options, "DefaultDocumentIdFormat") != "NO_DOCUMENT_ID")) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	s3, ok := destination["S3Configuration"].(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(s3, region, false); err != nil {
		return err
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	if raw := destination["ProcessingConfiguration"]; raw != nil {
		return validateProcessingConfiguration(raw)
	}
	return nil
}

func validateRedshiftDestination(destination map[string]any, region string) error {
	jdbc := first(destination, "ClusterJDBCURL")
	if len(jdbc) < 1 || len(jdbc) > 512 || !firehoseRedshiftJDBC.MatchString(jdbc) || !validRoleARN(first(destination, "RoleARN")) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	copyCommand, ok := destination["CopyCommand"].(map[string]any)
	table := first(copyCommand, "DataTableName")
	if !ok || len(table) < 1 || len(table) > 512 {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	for _, key := range []string{"DataTableColumns", "CopyOptions"} {
		if raw := copyCommand[key]; raw != nil {
			value, ok := raw.(string)
			if !ok || len(value) > 10240 {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	username, usernameOK := destination["Username"].(string)
	password, passwordOK := destination["Password"].(string)
	if (destination["Username"] != nil && (!usernameOK || len(username) < 1 || len(username) > 512)) || (destination["Password"] != nil && (!passwordOK || len(password) < 6 || len(password) > 512)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	secrets, hasSecrets := destination["SecretsManagerConfiguration"].(map[string]any)
	enabled, enabledOK := secrets["Enabled"].(bool)
	if destination["SecretsManagerConfiguration"] != nil && (!hasSecrets || !enabledOK || (first(secrets, "RoleARN") != "" && !validRoleARN(first(secrets, "RoleARN"))) || (enabled && (!validRoleARN(first(secrets, "RoleARN")) || !firehoseSecretARN.MatchString(first(secrets, "SecretARN"))))) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if !enabled && (!usernameOK || !passwordOK) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if retry, ok := destination["RetryOptions"].(map[string]any); destination["RetryOptions"] != nil && (!ok || !optionalInteger(retry, "DurationInSeconds", 0, 7200)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	mode := first(destination, "S3BackupMode")
	if mode != "" && mode != "Disabled" && mode != "Enabled" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	s3, ok := destination["S3Configuration"].(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(s3, region, false); err != nil {
		return err
	}
	if compression := first(s3, "CompressionFormat"); compression != "" && compression != "UNCOMPRESSED" && compression != "GZIP" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
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
		if err := validateS3Configuration(backup, region, false); err != nil {
			return err
		}
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	if raw := destination["ProcessingConfiguration"]; raw != nil {
		return validateProcessingConfiguration(raw)
	}
	return nil
}

func validateSplunkDestination(destination map[string]any, region string) error {
	endpoint := first(destination, "HECEndpoint")
	parsed, err := url.Parse(endpoint)
	if len(endpoint) > 2048 || err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || (first(destination, "HECEndpointType") != "Raw" && first(destination, "HECEndpointType") != "Event") {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	token, hasToken := destination["HECToken"].(string)
	if (destination["HECToken"] != nil && !hasToken) || len(token) > 2048 || strings.ContainsAny(token, "\r\n") {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if hints, ok := destination["BufferingHints"].(map[string]any); destination["BufferingHints"] != nil && (!ok || !optionalInteger(hints, "IntervalInSeconds", 0, 60) || !optionalInteger(hints, "SizeInMBs", 1, 5)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if retry, ok := destination["RetryOptions"].(map[string]any); destination["RetryOptions"] != nil && (!ok || !optionalInteger(retry, "DurationInSeconds", 0, 7200)) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if timeout, exists := destination["HECAcknowledgmentTimeoutInSeconds"]; exists {
		if _, valid := inputInteger(timeout, 180, 600); !valid {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if mode := first(destination, "S3BackupMode"); mode != "" && mode != "FailedEventsOnly" && mode != "AllEvents" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	secrets, hasSecrets := destination["SecretsManagerConfiguration"].(map[string]any)
	enabled, enabledOK := secrets["Enabled"].(bool)
	if destination["SecretsManagerConfiguration"] != nil && (!hasSecrets || !enabledOK || (first(secrets, "RoleARN") != "" && !validRoleARN(first(secrets, "RoleARN"))) || (enabled && (!validRoleARN(first(secrets, "RoleARN")) || !firehoseSecretARN.MatchString(first(secrets, "SecretARN"))))) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if !enabled && token == "" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	s3, ok := destination["S3Configuration"].(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(s3, region, false); err != nil {
		return err
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	if raw := destination["ProcessingConfiguration"]; raw != nil {
		return validateProcessingConfiguration(raw)
	}
	return nil
}

func optionalInteger(values map[string]any, key string, minimum, maximum int) bool {
	if values[key] == nil {
		return true
	}
	_, valid := inputInteger(values[key], minimum, maximum)
	return valid
}

func validateHTTPEndpointDestination(destination map[string]any, region string) error {
	endpoint, endpointOK := destination["EndpointConfiguration"].(map[string]any)
	rawURL, urlOK := endpoint["Url"].(string)
	parsed, err := url.Parse(rawURL)
	if !endpointOK || !urlOK || len(rawURL) < 1 || len(rawURL) > 1000 || err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || (parsed.Port() != "" && parsed.Port() != "443") {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if raw, exists := endpoint["Name"]; exists {
		name, ok := raw.(string)
		if !ok || len(name) > 256 || strings.TrimSpace(name) == "" {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if raw, exists := endpoint["AccessKey"]; exists {
		accessKey, ok := raw.(string)
		if !ok || !validHTTPAccessKey(accessKey) {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if role := first(destination, "RoleARN"); role != "" && !validRoleARN(role) {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	s3, ok := destination["S3Configuration"].(map[string]any)
	if !ok {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if err := validateS3Configuration(s3, region, false); err != nil {
		return err
	}
	if err := validateCloudWatchLogging(destination["CloudWatchLoggingOptions"]); err != nil {
		return err
	}
	if raw, exists := destination["ProcessingConfiguration"]; exists {
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
			if _, valid := inputInteger(hints["SizeInMBs"], 1, 64); !valid {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	if raw, exists := destination["RetryOptions"]; exists {
		retry, ok := raw.(map[string]any)
		if !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if duration, exists := retry["DurationInSeconds"]; exists {
			if _, valid := inputInteger(duration, 0, 7200); !valid {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	if mode := first(destination, "S3BackupMode"); mode != "" && mode != "FailedDataOnly" && mode != "AllData" {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
	}
	if raw, exists := destination["RequestConfiguration"]; exists {
		request, ok := raw.(map[string]any)
		encoding := first(request, "ContentEncoding")
		if !ok || (encoding != "" && encoding != "NONE" && encoding != "GZIP") || !validHTTPCommonAttributes(request["CommonAttributes"]) {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	if raw, exists := destination["SecretsManagerConfiguration"]; exists {
		secrets, ok := raw.(map[string]any)
		enabled, enabledOK := secrets["Enabled"].(bool)
		role, secret := first(secrets, "RoleARN"), first(secrets, "SecretARN")
		if !ok || !enabledOK || (role != "" && !validRoleARN(role)) || (secret != "" && (len(secret) > 2048 || !firehoseSecretARN.MatchString(secret))) || (enabled && secret == "") {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func validHTTPAccessKey(accessKey string) bool {
	return len(accessKey) <= 4096 && !strings.ContainsAny(accessKey, "\r\n")
}

func validHTTPCommonAttributes(value any) bool {
	if value == nil {
		return true
	}
	attributes, ok := value.([]any)
	if !ok || len(attributes) > 50 {
		return false
	}
	for _, raw := range attributes {
		attribute, ok := raw.(map[string]any)
		name, nameOK := attribute["AttributeName"].(string)
		value, valueOK := attribute["AttributeValue"].(string)
		if !ok || !nameOK || !valueOK || len(name) > 256 || strings.TrimSpace(name) == "" || len(value) > 1024 || strings.ContainsAny(name+value, "\r\n") {
			return false
		}
	}
	return true
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
	if raw, exists := destination["DataFormatConversionConfiguration"]; exists {
		configuration, ok := raw.(map[string]any)
		enabled := true
		if value, configured := configuration["Enabled"]; configured {
			enabled, ok = value.(bool)
		}
		if !processingAllowed || !ok {
			return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
		}
		if enabled {
			return spi.NotImplemented("aws.firehose", "DataFormatConversionConfiguration", "emulate")
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
	if hasProcessor(destination, "MetadataExtraction") && !dynamic {
		return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
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
	if compression != "" && compression != "UNCOMPRESSED" && compression != "GZIP" && compression != "ZIP" && compression != "Snappy" && compression != "HADOOP_SNAPPY" {
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
	if strings.Contains(prefix, "!{partitionKeyFromQuery:") && !hasProcessor(destination, "MetadataExtraction") {
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
			if role := processorParameter(processor, "RoleArn"); role != "" && !validRoleARN(role) {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if retries := processorParameter(processor, "NumberOfRetries"); retries != "" {
				value, err := strconv.Atoi(retries)
				if err != nil || value < 0 || value > 300 {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
			if size := processorParameter(processor, "BufferSizeInMBs"); size != "" {
				value, err := strconv.ParseFloat(size, 64)
				if err != nil || math.IsNaN(value) || value < 0.2 || value > 3 {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
			if interval := processorParameter(processor, "BufferIntervalInSeconds"); interval != "" {
				value, err := strconv.Atoi(interval)
				if err != nil || value < 0 || value > 900 {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
			parameters, _ := processor["Parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, _ := rawParameter.(map[string]any)
				switch first(parameter, "ParameterName") {
				case "LambdaArn", "NumberOfRetries", "RoleArn", "BufferSizeInMBs", "BufferIntervalInSeconds":
				default:
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
			}
		case "MetadataExtraction":
			query, engine := processorParameter(processor, "MetadataExtractionQuery"), processorParameter(processor, "JsonParsingEngine")
			parsed, err := gojq.Parse(query)
			if query == "" || engine != "JQ-1.6" || err != nil {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			if _, err := gojq.Compile(parsed); err != nil {
				return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
			}
			parameters, _ := processor["Parameters"].([]any)
			for _, rawParameter := range parameters {
				parameter, _ := rawParameter.(map[string]any)
				name := first(parameter, "ParameterName")
				if name != "MetadataExtractionQuery" && name != "JsonParsingEngine" {
					return &spi.Fault{Code: "InvalidArgumentException", HTTPStatus: 400, Fault: "client"}
				}
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
	case "S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "HttpEndpointDestinationConfiguration", "ElasticsearchDestinationConfiguration", "RedshiftDestinationConfiguration", "SplunkDestinationConfiguration", "IcebergDestinationConfiguration":
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

func (p *Pack) deliver(ctx context.Context, req *spi.Request, stream string, recIDs []string, data [][]byte) {
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
	for _, endpoint := range []struct{ key, all string }{{"HttpEndpointDestinationConfiguration", "AllData"}, {"SplunkDestinationConfiguration", "AllEvents"}} {
		if destination, ok := rec[endpoint.key].(map[string]any); ok {
			p.deliverEndpointRecords(ctx, req, rec, destination, endpoint.key, endpoint.all, stream, version, recIDs, data, now)
			return
		}
	}
	if destination, ok := rec["RedshiftDestinationConfiguration"].(map[string]any); ok {
		p.deliverRedshiftRecords(ctx, req, destination, stream, version, recIDs, data, now)
		return
	}
	if destination, ok := rec["IcebergDestinationConfiguration"].(map[string]any); ok {
		p.deliverIcebergRecords(ctx, req, destination, stream, version, recIDs, data, now)
		return
	}
	if destination, ok := rec["ElasticsearchDestinationConfiguration"].(map[string]any); ok {
		backup, _ := destination["S3Configuration"].(map[string]any)
		bucket, _, errorPrefix, _, _, _, kmsARN := s3Configuration(backup)
		payload := searchPayload{}
		for i := range data {
			if first(destination, "S3BackupMode") == "AllDocuments" {
				p.deliverS3Configuration(ctx, req, backup, stream, version, recIDs[i], data[i], now)
			}
			records, failures := p.processData(ctx, req, destination, stream, recIDs[i], data[i], now)
			for _, failure := range failures {
				p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
			}
			for _, record := range records {
				payload.RecordIDs = append(payload.RecordIDs, record.recID)
				payload.RawData = append(payload.RawData, record.raw)
				payload.Data = append(payload.Data, record.data)
				payload.Arrivals = append(payload.Arrivals, now)
			}
		}
		if len(payload.Data) == 0 {
			return
		}
		if p.bufferSearch(ctx, req, stream, payload, destination, now) {
			return
		}
		retryable, permanent, code, message := p.deliverSearch(ctx, req, destination, payload)
		p.backupSearchFailures(ctx, req, rec, destination, stream, permanent, 1, "400", "record is not a JSON object", now)
		p.retryOrBackupSearch(ctx, req, rec, destination, stream, retryable, code, message, now)
		return
	}
	for i := range data {
		p.deliverS3Configuration(ctx, req, destination, stream, version, recIDs[i], data[i], now)
		extended, _ := rec["ExtendedS3DestinationConfiguration"].(map[string]any)
		if first(extended, "S3BackupMode") == "Enabled" {
			backup, _ := extended["S3BackupConfiguration"].(map[string]any)
			p.deliverS3Configuration(ctx, req, backup, stream, version, recIDs[i], data[i], now)
		}
	}
}

func (p *Pack) deliverIcebergRecords(ctx context.Context, req *spi.Request, destination map[string]any, stream, version string, recIDs []string, data [][]byte, now time.Time) {
	s3, _ := destination["S3Configuration"].(map[string]any)
	bucket, _, errorPrefix, _, _, _, kmsARN := s3Configuration(s3)
	tables, _ := destination["DestinationTableConfigurationList"].([]any)
	catalog, _ := destination["CatalogConfiguration"].(map[string]any)
	tableBucket := icebergTableBucket(first(catalog, "CatalogARN"))
	for index := range data {
		if first(destination, "S3BackupMode") == "AllData" {
			p.deliverS3Configuration(ctx, req, s3, stream, version, recIDs[index], data[index], now)
		}
		records, failures := p.processData(ctx, req, destination, stream, recIDs[index], data[index], now)
		for _, failure := range failures {
			p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
		}
		for _, record := range records {
			values := map[string]any{}
			if json.Unmarshal(record.data, &values) != nil || values == nil {
				p.deliverIcebergFailure(ctx, req, s3, stream, version, now, record, "Iceberg record is not a JSON object")
				continue
			}
			metadata := maps.Clone(record.queryPartitionKeys)
			maps.Copy(metadata, record.partitionKeys)
			table := icebergTable(tables, metadata)
			if table == nil || tableBucket == "" {
				p.deliverIcebergFailure(ctx, req, s3, stream, version, now, record, "Iceberg destination table was not found")
				continue
			}
			operation := metadata["operation"]
			if operation == "" {
				operation = "insert"
			}
			if destination["AppendOnly"] == true && operation != "insert" {
				p.deliverIcebergFailure(ctx, req, icebergFailureS3(s3, table), stream, version, now, record, "Iceberg append-only stream rejected a mutation")
				continue
			}
			err := s3tablesservice.New(p.deps).ApplyRows(ctx, req.Identity, tableBucket, first(table, "DestinationDatabaseName"), first(table, "DestinationTableName"), []s3tablesservice.RowMutation{{
				Operation: operation, Values: values, UniqueKeys: icebergUniqueKeys(table["UniqueKeys"]),
			}})
			if err != nil {
				p.deliverIcebergFailure(ctx, req, icebergFailureS3(s3, table), stream, version, now, record, err.Error())
			}
		}
	}
}

func (p *Pack) deliverIcebergFailure(ctx context.Context, req *spi.Request, s3 map[string]any, stream, version string, now time.Time, record processingRecord, message string) {
	bucket, _, errorPrefix, _, _, _, kmsARN := s3Configuration(s3)
	failure := &processingFailure{typeName: "iceberg-failed", code: "Iceberg.DeliveryFailed", message: message, attempts: 1, recID: record.recID, data: record.raw}
	p.logDeliveryError(ctx, req, s3, stream, message, now)
	p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
}

func icebergFailureS3(s3, table map[string]any) map[string]any {
	if prefix := first(table, "S3ErrorOutputPrefix"); prefix != "" {
		s3 = maps.Clone(s3)
		s3["ErrorOutputPrefix"] = prefix
	}
	return s3
}

func icebergTableBucket(catalogARN string) string {
	const marker = "/s3tablescatalog/"
	if index := strings.Index(catalogARN, marker); index >= 0 {
		return catalogARN[index+len(marker):]
	}
	return ""
}

func icebergTable(tables []any, metadata map[string]string) map[string]any {
	database, tableName := metadata["icebergDestinationDatabaseName"], metadata["icebergDestinationTableName"]
	if database == "" && tableName == "" && len(tables) == 1 {
		table, _ := tables[0].(map[string]any)
		return table
	}
	for _, raw := range tables {
		table, _ := raw.(map[string]any)
		if first(table, "DestinationDatabaseName") == database && first(table, "DestinationTableName") == tableName {
			return table
		}
	}
	return nil
}

func icebergUniqueKeys(raw any) []string {
	values, _ := raw.([]any)
	keys := make([]string, 0, len(values))
	for _, value := range values {
		if key, ok := value.(string); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

func (p *Pack) deliverRedshiftRecords(ctx context.Context, req *spi.Request, destination map[string]any, stream, version string, recIDs []string, data [][]byte, now time.Time) {
	staging, _ := destination["S3Configuration"].(map[string]any)
	failureConfiguration := staging
	backup, _ := destination["S3BackupConfiguration"].(map[string]any)
	backupEnabled := first(destination, "S3BackupMode") == "Enabled"
	if backupEnabled {
		failureConfiguration = backup
	}
	bucket, _, errorPrefix, _, _, _, kmsARN := s3Configuration(failureConfiguration)
	var copied [][]byte
	for index := range data {
		if backupEnabled {
			p.deliverS3Configuration(ctx, req, backup, stream, version, recIDs[index], data[index], now)
		}
		records, failures := p.processData(ctx, req, destination, stream, recIDs[index], data[index], now)
		for _, failure := range failures {
			p.logDeliveryError(ctx, req, destination, stream, failure.message, now)
			p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
		}
		for _, record := range records {
			p.deliverS3Configuration(ctx, req, staging, stream, version, record.recID, record.data, now)
			copied = append(copied, record.data)
		}
	}
	if len(copied) == 0 {
		return
	}
	username, password, err := p.redshiftCredentials(ctx, req, destination)
	if err != nil {
		p.logDeliveryError(ctx, req, destination, stream, err.Error(), now)
		return
	}
	cluster, database := redshiftTarget(first(destination, "ClusterJDBCURL"))
	command, _ := destination["CopyCommand"].(map[string]any)
	err = redshiftservice.New(p.deps).Copy(ctx, req.Identity, redshiftservice.CopyInput{
		Cluster: cluster, Database: database, Table: first(command, "DataTableName"), Username: username, Password: password,
		Columns: first(command, "DataTableColumns"), Options: first(command, "CopyOptions"), Data: copied,
	})
	if err != nil {
		p.logDeliveryError(ctx, req, destination, stream, err.Error(), now)
		p.scheduleRedshiftRetry(ctx, req, stream, copied, err.Error(), now, redshiftRetryDuration(destination))
	}
}

func (p *Pack) redshiftCredentials(ctx context.Context, req *spi.Request, destination map[string]any) (string, string, error) {
	username, password := first(destination, "Username"), first(destination, "Password")
	if secrets, _ := destination["SecretsManagerConfiguration"].(map[string]any); secrets["Enabled"] == true {
		response, err := secretsmanager.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "GetSecretValue", Input: map[string]any{"SecretId": first(secrets, "SecretARN")}})
		var secret string
		var ok bool
		if response != nil {
			secret, ok = response.Output["SecretString"].(string)
		}
		value := map[string]any{}
		if err != nil || !ok || json.Unmarshal([]byte(secret), &value) != nil {
			return "", "", errors.New("unable to retrieve Redshift credentials")
		}
		username, ok = value["username"].(string)
		if !ok {
			return "", "", errors.New("Redshift secret must contain username and password")
		}
		password, ok = value["password"].(string)
		if !ok || username == "" || len(password) < 6 {
			return "", "", errors.New("Redshift secret must contain username and password")
		}
	}
	return username, password, nil
}

func redshiftTarget(jdbc string) (string, string) {
	parsed, _ := url.Parse(strings.TrimPrefix(jdbc, "jdbc:"))
	return strings.SplitN(parsed.Hostname(), ".", 2)[0], strings.TrimPrefix(parsed.Path, "/")
}

func (p *Pack) scheduleRedshiftRetry(ctx context.Context, req *spi.Request, stream string, data [][]byte, message string, now time.Time, duration time.Duration) bool {
	if duration <= 0 || p.deps.Blobs == nil {
		return false
	}
	key := p.deps.Rand.UUID()
	dataKey := req.Identity.Account + "/" + req.Identity.Region + "/_mirror/firehose-redshift-work/" + key
	body, err := json.Marshal(data)
	if err == nil {
		body, err = p.protectStreamData(ctx, req, stream, body)
	}
	if err != nil {
		return false
	}
	if _, err := p.deps.Blobs.Put(ctx, dataKey, bytes.NewReader(body)); err != nil {
		return false
	}
	expires := now.Add(duration)
	next := minTime(now.Add(5*time.Minute), expires)
	work := redshiftWork{Stream: stream, DataKey: dataKey, Next: next, Expires: expires, ErrorMessage: message}
	stored, err := json.Marshal(work)
	if err != nil || p.col(req, "fh-redshift-work").Put(ctx, stream+"/"+key, stored) != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
		return false
	}
	p.startRetryLoop()
	p.notifyRetryLoop()
	return true
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

func (p *Pack) runRedshiftRetries(ctx context.Context) time.Time {
	if p.deps.Store == nil || p.deps.Clock == nil {
		return time.Time{}
	}
	now := p.deps.Clock.Now().UTC()
	var earliest time.Time
	scopes, _ := p.deps.Store.Scopes(ctx)
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("fh-redshift-work")
		items, _, _ := collection.List(ctx, "", "", 0)
		for _, item := range items {
			var work redshiftWork
			if json.Unmarshal(item.Value, &work) != nil {
				continue
			}
			next := work.Next
			if !next.After(now) {
				next = p.runRedshiftRetry(ctx, identity, collection, item.Key, &work, now)
			}
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return earliest
}

func (p *Pack) deleteRedshiftWork(ctx context.Context, collection spi.Collection, key, dataKey string) {
	_ = collection.Delete(ctx, key)
	if p.deps.Blobs != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
	}
}

func (p *Pack) runRedshiftRetry(ctx context.Context, identity spi.Identity, collection spi.Collection, key string, work *redshiftWork, now time.Time) time.Time {
	req := &spi.Request{Identity: identity}
	raw, ok, _ := p.col(req, "fh").Get(ctx, work.Stream)
	if !ok {
		p.deleteRedshiftWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	var stream map[string]any
	_ = json.Unmarshal(raw, &stream)
	destination, ok := stream["RedshiftDestinationConfiguration"].(map[string]any)
	if !ok {
		p.deleteRedshiftWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	var data [][]byte
	payloadOK := false
	if p.deps.Blobs != nil {
		if reader, _, err := p.deps.Blobs.Get(ctx, work.DataKey); err == nil {
			body, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr == nil {
				body, readErr = p.decryptAtRest(ctx, req, body)
			}
			payloadOK = readErr == nil && json.Unmarshal(body, &data) == nil && len(data) != 0
		}
	}
	if !payloadOK {
		work.Next = now.Add(time.Second)
		stored, _ := json.Marshal(work)
		_ = collection.Put(ctx, key, stored)
		return work.Next
	}
	if !now.Before(work.Expires) {
		p.deleteRedshiftWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	username, password, err := p.redshiftCredentials(ctx, req, destination)
	cluster, database := redshiftTarget(first(destination, "ClusterJDBCURL"))
	command, _ := destination["CopyCommand"].(map[string]any)
	if err == nil {
		err = redshiftservice.New(p.deps).Copy(ctx, identity, redshiftservice.CopyInput{
			Cluster: cluster, Database: database, Table: first(command, "DataTableName"), Username: username, Password: password,
			Columns: first(command, "DataTableColumns"), Options: first(command, "CopyOptions"), Data: data,
		})
	}
	if err == nil {
		p.deleteRedshiftWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	p.logDeliveryError(ctx, req, destination, work.Stream, err.Error(), now)
	work.Retries++
	work.ErrorMessage = err.Error()
	work.Next = minTime(now.Add(5*time.Minute), work.Expires)
	stored, _ := json.Marshal(work)
	_ = collection.Put(ctx, key, stored)
	return work.Next
}

func (p *Pack) deliverEndpointRecords(ctx context.Context, req *spi.Request, streamRecord, destination map[string]any, destinationKey, allMode, stream, version string, recIDs []string, data [][]byte, now time.Time) {
	processedIDs := make([]string, 0, len(recIDs))
	processedData := make([][]byte, 0, len(data))
	backup, _ := destination["S3Configuration"].(map[string]any)
	bucket, _, errorPrefix, _, _, _, kmsARN := s3Configuration(backup)
	for index := range data {
		records, failures := p.processData(ctx, req, destination, stream, recIDs[index], data[index], now)
		for _, failure := range failures {
			p.logDeliveryError(ctx, req, destination, stream, failure.message, now)
			p.deliverProcessingFailure(ctx, req, bucket, errorPrefix, kmsARN, stream, version, now, failure)
		}
		for _, record := range records {
			processedIDs = append(processedIDs, record.recID)
			processedData = append(processedData, record.data)
		}
	}
	backupMode := first(destination, "S3BackupMode")
	if backupMode == allMode {
		for index := range data {
			p.deliverS3Configuration(ctx, req, backup, stream, version, recIDs[index], data[index], now)
		}
	}
	if len(processedData) == 0 {
		return
	}
	backupFailures := destinationKey == "SplunkDestinationConfiguration" || backupMode != allMode
	if p.bufferEndpoint(ctx, req, stream, destinationKey, processedIDs[0], recIDs, data, processedData, backupFailures, destination, now) {
		return
	}
	delivered, permanent, code, message := p.deliverEndpoint(ctx, req, streamRecord, destination, destinationKey, processedIDs[0], processedData, now)
	if destinationKey == "SplunkDestinationConfiguration" {
		now = p.deps.Clock.Now().UTC()
	}
	if !delivered {
		p.logDeliveryError(ctx, req, destination, stream, message, now)
	}
	retryDuration := httpRetryDuration(destination)
	retrying := !delivered && !permanent && retryDuration > 0 && p.scheduleEndpointRetry(ctx, req, stream, destinationKey, processedIDs[0], recIDs, data, processedData, backupFailures, code, message, now, retryDuration)
	if backupFailures && !delivered && !permanent && !retrying {
		payload := httpRetryPayload{BackupRecordIDs: recIDs, BackupData: data, Arrivals: repeatTime(now, len(data))}
		p.backupEndpointPayload(ctx, req, streamRecord, destination, destinationKey, stream, payload, 0, code, message, now)
	}
}

func repeatTime(value time.Time, count int) []time.Time {
	values := make([]time.Time, count)
	for index := range values {
		values[index] = value
	}
	return values
}

func elasticsearchDomain(destination map[string]any) string {
	if arn := first(destination, "DomainARN"); arn != "" {
		return arn[strings.LastIndex(arn, "/")+1:]
	}
	parsed, _ := url.Parse(first(destination, "ClusterEndpoint"))
	return strings.SplitN(parsed.Hostname(), ".", 2)[0]
}

func elasticsearchIndex(destination map[string]any, now time.Time) string {
	index := first(destination, "IndexName")
	switch first(destination, "IndexRotationPeriod") {
	case "", "OneDay":
		return index + "-" + now.Format("2006-01-02")
	case "OneHour":
		return index + "-" + now.Format("2006-01-02-15")
	case "OneWeek":
		year, week := now.ISOWeek()
		return fmt.Sprintf("%s-%04d-w%02d", index, year, week)
	case "OneMonth":
		return index + "-" + now.Format("2006-01")
	default:
		return index
	}
}

func httpRetryDuration(destination map[string]any) time.Duration {
	retry, _ := destination["RetryOptions"].(map[string]any)
	seconds, ok := inputInteger(retry["DurationInSeconds"], 0, 7200)
	if !ok {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}

func redshiftRetryDuration(destination map[string]any) time.Duration {
	retry, _ := destination["RetryOptions"].(map[string]any)
	seconds, ok := inputInteger(retry["DurationInSeconds"], 0, 7200)
	if !ok {
		seconds = 3600
	}
	return time.Duration(seconds) * time.Second
}

func httpBufferingHints(destination map[string]any) (time.Duration, int) {
	hints, _ := destination["BufferingHints"].(map[string]any)
	interval, intervalOK := inputInteger(hints["IntervalInSeconds"], 0, 900)
	size, sizeOK := inputInteger(hints["SizeInMBs"], 1, 64)
	if !intervalOK || !sizeOK {
		return 300 * time.Second, 5 * 1024 * 1024
	}
	return time.Duration(interval) * time.Second, size * 1024 * 1024
}

func endpointBufferingHints(destinationKey string, destination map[string]any) (time.Duration, int) {
	if destinationKey != "SplunkDestinationConfiguration" {
		return httpBufferingHints(destination)
	}
	hints, _ := destination["BufferingHints"].(map[string]any)
	interval, intervalOK := inputInteger(hints["IntervalInSeconds"], 0, 60)
	size, sizeOK := inputInteger(hints["SizeInMBs"], 1, 5)
	if !intervalOK || !sizeOK {
		return 60 * time.Second, 5 * 1024 * 1024
	}
	return time.Duration(interval) * time.Second, size * 1024 * 1024
}

func searchBufferingHints(destination map[string]any) (time.Duration, int) {
	hints, _ := destination["BufferingHints"].(map[string]any)
	interval, intervalOK := inputInteger(hints["IntervalInSeconds"], 0, 900)
	size, sizeOK := inputInteger(hints["SizeInMBs"], 1, 100)
	if !intervalOK || !sizeOK {
		return 300 * time.Second, 5 * 1024 * 1024
	}
	return time.Duration(interval) * time.Second, size * 1024 * 1024
}

func validSearchPayload(payload searchPayload) bool {
	return len(payload.Data) != 0 && len(payload.RecordIDs) == len(payload.Data) && len(payload.RawData) == len(payload.Data) && len(payload.Arrivals) == len(payload.Data)
}

func appendSearchPayload(destination *searchPayload, source searchPayload, index int) {
	destination.RecordIDs = append(destination.RecordIDs, source.RecordIDs[index])
	destination.RawData = append(destination.RawData, source.RawData[index])
	destination.Data = append(destination.Data, source.Data[index])
	destination.Arrivals = append(destination.Arrivals, source.Arrivals[index])
}

func (p *Pack) putSearchBlob(ctx context.Context, req *spi.Request, stream string, payload searchPayload) (string, string, bool) {
	if p.deps.Blobs == nil || !validSearchPayload(payload) {
		return "", "", false
	}
	key := p.deps.Rand.UUID()
	dataKey := req.Identity.Account + "/" + req.Identity.Region + "/_mirror/firehose-search-work/" + key
	body, err := json.Marshal(payload)
	if err == nil {
		body, err = p.protectStreamData(ctx, req, stream, body)
	}
	if err != nil {
		return "", "", false
	}
	if _, err = p.deps.Blobs.Put(ctx, dataKey, bytes.NewReader(body)); err != nil {
		return "", "", false
	}
	return key, dataKey, true
}

func (p *Pack) bufferSearch(ctx context.Context, req *spi.Request, stream string, payload searchPayload, destination map[string]any, now time.Time) bool {
	interval, sizeLimit := searchBufferingHints(destination)
	size := 0
	for _, data := range payload.Data {
		size += len(data)
	}
	key, dataKey, ok := p.putSearchBlob(ctx, req, stream, payload)
	if !ok {
		return false
	}
	key = stream + "/buffer/" + key
	collection := p.col(req, "fh-search-work")
	err := collection.Txn(ctx, func(tx spi.Tx) error {
		items, _, err := tx.List(stream+"/buffer/", "", 0)
		if err != nil {
			return err
		}
		total, order, next := 0, 0, now.Add(interval)
		for _, item := range items {
			var work searchWork
			if json.Unmarshal(item.Value, &work) != nil {
				return errors.New("invalid persisted OpenSearch buffer")
			}
			total += work.Size
			order = max(order, work.Order)
			if work.Next.Before(next) {
				next = work.Next
			}
		}
		total += size
		if total >= sizeLimit {
			next = now
		}
		for _, item := range items {
			var work searchWork
			_ = json.Unmarshal(item.Value, &work)
			work.Next = next
			stored, _ := json.Marshal(work)
			if err := tx.Put(item.Key, stored); err != nil {
				return err
			}
		}
		stored, _ := json.Marshal(searchWork{Stream: stream, DataKey: dataKey, State: "buffer", Size: size, Order: order + 1, Next: next})
		return tx.Put(key, stored)
	})
	if err != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
		return false
	}
	if interval == 0 {
		p.runSearchWork(ctx)
		return true
	}
	p.startRetryLoop()
	p.notifyRetryLoop()
	return true
}

func (p *Pack) loadSearchPayload(ctx context.Context, req *spi.Request, dataKey string) (searchPayload, bool) {
	if p.deps.Blobs == nil {
		return searchPayload{}, false
	}
	reader, _, err := p.deps.Blobs.Get(ctx, dataKey)
	if err != nil {
		return searchPayload{}, false
	}
	body, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr == nil {
		body, readErr = p.decryptAtRest(ctx, req, body)
	}
	var payload searchPayload
	return payload, readErr == nil && json.Unmarshal(body, &payload) == nil && validSearchPayload(payload)
}

func (p *Pack) writeSearchPayload(ctx context.Context, req *spi.Request, stream, dataKey string, payload searchPayload) bool {
	body, err := json.Marshal(payload)
	if err == nil {
		body, err = p.protectStreamData(ctx, req, stream, body)
	}
	if err != nil || p.deps.Blobs == nil {
		return false
	}
	_, err = p.deps.Blobs.Put(ctx, dataKey, bytes.NewReader(body))
	return err == nil
}

func (p *Pack) runSearchWork(ctx context.Context) time.Time {
	if p.deps.Store == nil || p.deps.Clock == nil {
		return time.Time{}
	}
	now := p.deps.Clock.Now().UTC()
	var earliest time.Time
	scopes, _ := p.deps.Store.Scopes(ctx)
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("fh-search-work")
		stored, _, _ := collection.List(ctx, "", "", 0)
		buffers := map[string][]searchWorkItem{}
		var retries []searchWorkItem
		for _, item := range stored {
			var work searchWork
			if json.Unmarshal(item.Value, &work) != nil {
				continue
			}
			switch work.State {
			case "buffer":
				buffers[work.Stream] = append(buffers[work.Stream], searchWorkItem{Key: item.Key, Work: work})
			case "retry":
				retries = append(retries, searchWorkItem{Key: item.Key, Work: work})
			}
		}
		for stream, items := range buffers {
			sort.Slice(items, func(i, j int) bool { return items[i].Work.Order < items[j].Work.Order })
			next := items[0].Work.Next
			for _, item := range items[1:] {
				if item.Work.Next.Before(next) {
					next = item.Work.Next
				}
			}
			if !next.After(now) {
				next = p.flushSearchBuffer(ctx, identity, collection, stream, items, now)
			}
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
		for _, item := range retries {
			next := item.Work.Next
			if !next.After(now) {
				next = p.runSearchRetry(ctx, identity, collection, item.Key, &item.Work, now)
			}
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return earliest
}

func (p *Pack) deleteSearchWork(ctx context.Context, collection spi.Collection, key, dataKey string) {
	_ = collection.Delete(ctx, key)
	if p.deps.Blobs != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
	}
}

func (p *Pack) deleteSearchItems(ctx context.Context, collection spi.Collection, items []searchWorkItem) {
	for _, item := range items {
		p.deleteSearchWork(ctx, collection, item.Key, item.Work.DataKey)
	}
}

func (p *Pack) flushSearchBuffer(ctx context.Context, identity spi.Identity, collection spi.Collection, name string, items []searchWorkItem, now time.Time) time.Time {
	req := &spi.Request{Identity: identity}
	raw, ok, _ := p.col(req, "fh").Get(ctx, name)
	if !ok {
		p.deleteSearchItems(ctx, collection, items)
		return time.Time{}
	}
	var stream map[string]any
	_ = json.Unmarshal(raw, &stream)
	destination, ok := stream["ElasticsearchDestinationConfiguration"].(map[string]any)
	if !ok {
		p.deleteSearchItems(ctx, collection, items)
		return time.Time{}
	}
	var payload searchPayload
	for _, item := range items {
		entry, ok := p.loadSearchPayload(ctx, req, item.Work.DataKey)
		if !ok {
			next := now.Add(time.Second)
			for _, item := range items {
				item.Work.Next = next
				stored, _ := json.Marshal(item.Work)
				_ = collection.Put(ctx, item.Key, stored)
			}
			return next
		}
		for index := range entry.Data {
			appendSearchPayload(&payload, entry, index)
		}
	}
	retryable, permanent, code, message := p.deliverSearch(ctx, req, destination, payload)
	p.backupSearchFailures(ctx, req, stream, destination, name, permanent, 1, "400", "record is not a JSON object", now)
	p.retryOrBackupSearch(ctx, req, stream, destination, name, retryable, code, message, now)
	p.deleteSearchItems(ctx, collection, items)
	return time.Time{}
}

func (p *Pack) deliverSearch(ctx context.Context, req *spi.Request, destination map[string]any, payload searchPayload) (searchPayload, searchPayload, string, string) {
	var retryable, permanent searchPayload
	code, message := "", ""
	options, _ := destination["DocumentIdOptions"].(map[string]any)
	useID := first(options, "DefaultDocumentIdFormat") != "NO_DOCUMENT_ID"
	for index, data := range payload.Data {
		var document map[string]any
		if json.Unmarshal(data, &document) != nil || document == nil {
			appendSearchPayload(&permanent, payload, index)
			continue
		}
		input := map[string]any{
			"DomainName": elasticsearchDomain(destination), "Index": elasticsearchIndex(destination, payload.Arrivals[index]), "Document": document,
		}
		if useID {
			input["Id"] = payload.RecordIDs[index]
		}
		if _, err := opensearch.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "IndexDocument", Input: input}); err != nil {
			appendSearchPayload(&retryable, payload, index)
			code, message = "500", err.Error()
			var fault *spi.Fault
			if errors.As(err, &fault) {
				code = strconv.Itoa(fault.HTTPStatus)
			}
		}
	}
	return retryable, permanent, code, message
}

func (p *Pack) retryOrBackupSearch(ctx context.Context, req *spi.Request, stream, destination map[string]any, name string, payload searchPayload, code, message string, now time.Time) {
	if !validSearchPayload(payload) {
		return
	}
	duration := httpRetryDuration(destination)
	if duration > 0 {
		key, dataKey, ok := p.putSearchBlob(ctx, req, name, payload)
		if !ok {
			p.backupSearchFailures(ctx, req, stream, destination, name, payload, 1, code, message, now)
			return
		}
		next := now.Add(p.httpRetryDelay(key, 0))
		expires := now.Add(duration)
		if next.After(expires) {
			next = expires
		}
		work := searchWork{Stream: name, DataKey: dataKey, State: "retry", Next: next, Expires: expires, ErrorCode: code, ErrorMessage: message}
		stored, err := json.Marshal(work)
		if err == nil && p.col(req, "fh-search-work").Put(ctx, name+"/retry/"+key, stored) == nil {
			p.startRetryLoop()
			p.notifyRetryLoop()
			return
		}
		_ = p.deps.Blobs.Delete(ctx, dataKey)
	}
	p.backupSearchFailures(ctx, req, stream, destination, name, payload, 1, code, message, now)
}

func (p *Pack) runSearchRetry(ctx context.Context, identity spi.Identity, collection spi.Collection, key string, work *searchWork, now time.Time) time.Time {
	req := &spi.Request{Identity: identity}
	raw, ok, _ := p.col(req, "fh").Get(ctx, work.Stream)
	if !ok {
		p.deleteSearchWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	var stream map[string]any
	_ = json.Unmarshal(raw, &stream)
	destination, ok := stream["ElasticsearchDestinationConfiguration"].(map[string]any)
	if !ok {
		p.deleteSearchWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	payload, ok := p.loadSearchPayload(ctx, req, work.DataKey)
	if !ok {
		work.Next = now.Add(time.Second)
		stored, _ := json.Marshal(work)
		_ = collection.Put(ctx, key, stored)
		return work.Next
	}
	if !now.Before(work.Expires) {
		p.backupSearchFailures(ctx, req, stream, destination, work.Stream, payload, work.Retries+1, work.ErrorCode, work.ErrorMessage, now)
		p.deleteSearchWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	retryable, permanent, code, message := p.deliverSearch(ctx, req, destination, payload)
	p.backupSearchFailures(ctx, req, stream, destination, work.Stream, permanent, work.Retries+2, "400", "record is not a JSON object", now)
	if !validSearchPayload(retryable) {
		p.deleteSearchWork(ctx, collection, key, work.DataKey)
		return time.Time{}
	}
	work.Retries++
	work.ErrorCode, work.ErrorMessage = code, message
	work.Next = now.Add(p.httpRetryDelay(key, work.Retries))
	if work.Next.After(work.Expires) {
		work.Next = work.Expires
	}
	if !p.writeSearchPayload(ctx, req, work.Stream, work.DataKey, retryable) {
		work.Next = now.Add(time.Second)
	}
	stored, _ := json.Marshal(work)
	_ = collection.Put(ctx, key, stored)
	return work.Next
}

func (p *Pack) backupSearchFailures(ctx context.Context, req *spi.Request, stream, destination map[string]any, name string, payload searchPayload, attempts int, code, message string, now time.Time) {
	if !validSearchPayload(payload) {
		return
	}
	backup, _ := destination["S3Configuration"].(map[string]any)
	bucket, prefix, _, _, _, _, kmsARN := s3Configuration(backup)
	prefix += "AmazonOpenSearchService-failed/"
	version := first(stream, "VersionId")
	if version == "" {
		version = "1"
	}
	options, _ := destination["DocumentIdOptions"].(map[string]any)
	useID := first(options, "DefaultDocumentIdFormat") != "NO_DOCUMENT_ID"
	for index := range payload.Data {
		failure := &processingFailure{
			typeName: "AmazonOpenSearchService-failed", code: code, message: message, attempts: attempts, recID: payload.RecordIDs[index], data: payload.RawData[index],
			arrival: payload.Arrivals[index], searchIndex: elasticsearchIndex(destination, payload.Arrivals[index]), searchType: first(destination, "TypeName"),
		}
		if useID {
			failure.documentID = payload.RecordIDs[index]
		}
		p.deliverProcessingFailure(ctx, req, bucket, prefix, kmsARN, name, version, now, failure)
	}
}

func (p *Pack) bufferEndpoint(ctx context.Context, req *spi.Request, stream, destinationKey, requestID string, recordIDs []string, rawData, processedData [][]byte, backup bool, destination map[string]any, now time.Time) bool {
	if p.deps.Blobs == nil {
		return false
	}
	key := p.deps.Rand.UUID()
	dataKey := req.Identity.Account + "/" + req.Identity.Region + "/_mirror/firehose-http-buffers/" + key
	payload := httpRetryPayload{ProcessedData: processedData}
	if backup {
		payload.BackupRecordIDs, payload.BackupData = recordIDs, rawData
		payload.Arrivals = repeatTime(now, len(rawData))
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return false
	}
	body, err = p.protectStreamData(ctx, req, stream, body)
	if err != nil {
		return false
	}
	if _, err := p.deps.Blobs.Put(ctx, dataKey, bytes.NewReader(body)); err != nil {
		return false
	}
	interval, sizeLimit := endpointBufferingHints(destinationKey, destination)
	size := 0
	for _, data := range rawData {
		size += len(data)
	}
	collection := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("fh-http-buffers")
	err = collection.Txn(ctx, func(tx spi.Tx) error {
		items, _, err := tx.List(stream+"/", "", 0)
		if err != nil {
			return err
		}
		next, total, order := now.Add(interval), size, 0
		for _, item := range items {
			var buffer httpBuffer
			if json.Unmarshal(item.Value, &buffer) != nil {
				return errors.New("invalid persisted HTTP buffer")
			}
			total += buffer.Size
			order = max(order, buffer.Order)
			if buffer.Next.Before(next) {
				next = buffer.Next
			}
		}
		if total >= sizeLimit {
			next = now
			for _, item := range items {
				var buffer httpBuffer
				_ = json.Unmarshal(item.Value, &buffer)
				buffer.Next = now
				stored, _ := json.Marshal(buffer)
				if err := tx.Put(item.Key, stored); err != nil {
					return err
				}
			}
		}
		stored, _ := json.Marshal(httpBuffer{Stream: stream, Destination: destinationKey, RequestID: requestID, DataKey: dataKey, Size: size, Order: order + 1, Next: next})
		return tx.Put(stream+"/"+key, stored)
	})
	if err != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
		return false
	}
	p.startRetryLoop()
	p.notifyRetryLoop()
	return true
}

func (p *Pack) hasHTTPWork(ctx context.Context) bool {
	if p.deps.Store == nil || p.deps.Clock == nil {
		return false
	}
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return false
	}
	for _, identity := range scopes {
		for _, name := range []string{"fh-http-buffers", "fh-http-retries", "fh-search-work", "fh-redshift-work"} {
			if work, _, _ := p.deps.Store.Scope(identity.Account, identity.Region).Collection(name).List(ctx, "", "", 1); len(work) != 0 {
				return true
			}
		}
	}
	return false
}

func (p *Pack) startRetryLoop() {
	p.retryOnce.Do(func() { go p.httpRetryLoop() })
}

func (p *Pack) notifyRetryLoop() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Pack) scheduleEndpointRetry(ctx context.Context, req *spi.Request, stream, destinationKey, requestID string, recordIDs []string, rawData, processedData [][]byte, backup bool, code, message string, now time.Time, duration time.Duration) bool {
	retryPayload := httpRetryPayload{ProcessedData: processedData}
	if backup {
		retryPayload.BackupRecordIDs, retryPayload.BackupData = recordIDs, rawData
		retryPayload.Arrivals = repeatTime(now, len(rawData))
	}
	return p.scheduleEndpointRetryPayload(ctx, req, stream, destinationKey, requestID, retryPayload, code, message, now, duration)
}

func (p *Pack) scheduleEndpointRetryPayload(ctx context.Context, req *spi.Request, stream, destinationKey, requestID string, retryPayload httpRetryPayload, code, message string, now time.Time, duration time.Duration) bool {
	key := p.deps.Rand.UUID()
	dataKey := req.Identity.Account + "/" + req.Identity.Region + "/_mirror/firehose-http-retries/" + key
	payload, err := json.Marshal(retryPayload)
	if err != nil || p.deps.Blobs == nil {
		return false
	}
	payload, err = p.protectStreamData(ctx, req, stream, payload)
	if err != nil {
		return false
	}
	if _, err := p.deps.Blobs.Put(ctx, dataKey, bytes.NewReader(payload)); err != nil {
		return false
	}
	retry := httpRetry{
		Stream: stream, Destination: destinationKey, RequestID: requestID, DataKey: dataKey,
		Next: now.Add(p.httpRetryDelay(key, 0)), ErrorCode: code, ErrorMessage: message,
	}
	retry.Expires = now.Add(duration)
	if retry.Next.After(retry.Expires) {
		retry.Next = retry.Expires
	}
	body, err := json.Marshal(retry)
	if err != nil || p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("fh-http-retries").Put(ctx, key, body) != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
		return false
	}
	p.startRetryLoop()
	p.notifyRetryLoop()
	return true
}

func (p *Pack) httpRetryDelay(key string, retry int) time.Duration {
	delay := time.Second << min(retry, 7)
	if delay > 2*time.Minute {
		delay = 2 * time.Minute
	}
	jitter := 100
	if p.deps.Rand != nil {
		jitter = 85 + p.deps.Rand.Derive(key+"|"+strconv.Itoa(retry)).Intn(31)
	}
	delay = delay * time.Duration(jitter) / 100
	return min(delay, 2*time.Minute)
}

func (p *Pack) httpRetryLoop() {
	defer close(p.done)
	for {
		next := p.runHTTPBuffers(context.Background())
		if retryNext := p.runHTTPRetries(context.Background()); !retryNext.IsZero() && (next.IsZero() || retryNext.Before(next)) {
			next = retryNext
		}
		if searchNext := p.runSearchWork(context.Background()); !searchNext.IsZero() && (next.IsZero() || searchNext.Before(next)) {
			next = searchNext
		}
		if redshiftNext := p.runRedshiftRetries(context.Background()); !redshiftNext.IsZero() && (next.IsZero() || redshiftNext.Before(next)) {
			next = redshiftNext
		}
		if next.IsZero() {
			select {
			case <-p.wake:
			case <-p.stop:
				return
			}
			continue
		}
		delay := next.Sub(p.deps.Clock.Now())
		if delay < 0 {
			delay = 0
		}
		select {
		case <-p.deps.Clock.After(delay):
		case <-p.wake:
		case <-p.stop:
			return
		}
	}
}

func (p *Pack) runHTTPBuffers(ctx context.Context) time.Time {
	if p.deps.Store == nil || p.deps.Clock == nil {
		return time.Time{}
	}
	now := p.deps.Clock.Now().UTC()
	var earliest time.Time
	scopes, _ := p.deps.Store.Scopes(ctx)
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("fh-http-buffers")
		buffers, _, _ := collection.List(ctx, "", "", 0)
		streams := map[string][]httpBufferItem{}
		for _, item := range buffers {
			var buffer httpBuffer
			if json.Unmarshal(item.Value, &buffer) != nil {
				continue
			}
			streams[buffer.Stream] = append(streams[buffer.Stream], httpBufferItem{Key: item.Key, Buffer: buffer})
		}
		for stream, items := range streams {
			sort.Slice(items, func(i, j int) bool { return items[i].Buffer.Order < items[j].Buffer.Order })
			next := items[0].Buffer.Next
			for _, item := range items[1:] {
				if item.Buffer.Next.Before(next) {
					next = item.Buffer.Next
				}
			}
			if !next.After(now) {
				next = p.flushHTTPBuffer(ctx, identity, collection, stream, items, now)
			}
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return earliest
}

func (p *Pack) deleteHTTPBuffer(ctx context.Context, collection spi.Collection, items []httpBufferItem) {
	for _, item := range items {
		_ = collection.Delete(ctx, item.Key)
	}
	if p.deps.Blobs != nil {
		for _, item := range items {
			_ = p.deps.Blobs.Delete(ctx, item.Buffer.DataKey)
		}
	}
}

func (p *Pack) deleteHTTPStreamWork(ctx context.Context, req *spi.Request, stream string) {
	buffers := p.col(req, "fh-http-buffers")
	items, _, _ := buffers.List(ctx, stream+"/", "", 0)
	for _, item := range items {
		var buffer httpBuffer
		_ = json.Unmarshal(item.Value, &buffer)
		p.deleteHTTPBuffer(ctx, buffers, []httpBufferItem{{Key: item.Key, Buffer: buffer}})
	}
	retries := p.col(req, "fh-http-retries")
	items, _, _ = retries.List(ctx, "", "", 0)
	for _, item := range items {
		var retry httpRetry
		if json.Unmarshal(item.Value, &retry) == nil && retry.Stream == stream {
			p.deleteHTTPRetry(ctx, retries, item.Key, retry.DataKey)
		}
	}
	search := p.col(req, "fh-search-work")
	items, _, _ = search.List(ctx, stream+"/", "", 0)
	for _, item := range items {
		var work searchWork
		_ = json.Unmarshal(item.Value, &work)
		p.deleteSearchWork(ctx, search, item.Key, work.DataKey)
	}
	redshift := p.col(req, "fh-redshift-work")
	items, _, _ = redshift.List(ctx, stream+"/", "", 0)
	for _, item := range items {
		var work redshiftWork
		_ = json.Unmarshal(item.Value, &work)
		p.deleteRedshiftWork(ctx, redshift, item.Key, work.DataKey)
	}
}

func (p *Pack) backupHTTPPayload(ctx context.Context, req *spi.Request, stream map[string]any, destination map[string]any, name string, payload httpRetryPayload, now time.Time) {
	backup, _ := destination["S3Configuration"].(map[string]any)
	version := first(stream, "VersionId")
	if version == "" {
		version = "1"
	}
	for i := range payload.BackupData {
		p.deliverS3Configuration(ctx, req, backup, name, version, payload.BackupRecordIDs[i], payload.BackupData[i], now)
	}
}

func (p *Pack) backupEndpointPayload(ctx context.Context, req *spi.Request, stream, destination map[string]any, destinationKey, name string, payload httpRetryPayload, attempts int, code, message string, now time.Time) {
	if destinationKey != "SplunkDestinationConfiguration" {
		p.backupHTTPPayload(ctx, req, stream, destination, name, payload, now)
		return
	}
	backup, _ := destination["S3Configuration"].(map[string]any)
	bucket, prefix, errorPrefix, _, _, _, kmsARN := s3Configuration(backup)
	if errorPrefix == "" {
		errorPrefix = prefix + "splunk-failed/"
	}
	version := first(stream, "VersionId")
	if version == "" {
		version = "1"
	}
	for index := range payload.BackupData {
		arrival := now
		if len(payload.Arrivals) == len(payload.BackupData) {
			arrival = payload.Arrivals[index]
		}
		fields := map[string]any{
			"attemptsMade": attempts, "arrivalTimestamp": arrival.UnixMilli(), "errorCode": code, "errorMessage": message,
			"attemptEndingTimestamp": now.UnixMilli(), "rawData": base64.StdEncoding.EncodeToString(payload.BackupData[index]), "EventId": payload.BackupRecordIDs[index],
		}
		body, _ := json.Marshal(fields)
		failedPrefix := strings.ReplaceAll(errorPrefix, "!{firehose:error-output-type}", "splunk-failed")
		key := p.evaluatedS3Prefix(failedPrefix, now) + name + "-" + version + "-" + now.Format("2006-01-02-15-04-05-") + payload.BackupRecordIDs[index]
		p.deliverS3Object(ctx, req, bucket, key, kmsARN, body)
	}
}

func (p *Pack) flushHTTPBuffer(ctx context.Context, identity spi.Identity, collection spi.Collection, name string, items []httpBufferItem, now time.Time) time.Time {
	req := &spi.Request{Identity: identity}
	raw, ok, _ := p.col(req, "fh").Get(ctx, name)
	if !ok {
		p.deleteHTTPBuffer(ctx, collection, items)
		return time.Time{}
	}
	var stream map[string]any
	_ = json.Unmarshal(raw, &stream)
	destinationKey := endpointDestinationKey(items[0].Buffer.Destination)
	destination, ok := stream[destinationKey].(map[string]any)
	if !ok {
		p.deleteHTTPBuffer(ctx, collection, items)
		return time.Time{}
	}
	var payload httpRetryPayload
	payloadOK := p.deps.Blobs != nil && len(items) != 0
	for _, item := range items {
		if !payloadOK {
			break
		}
		reader, _, err := p.deps.Blobs.Get(ctx, item.Buffer.DataKey)
		if err != nil {
			payloadOK = false
			break
		}
		body, readErr := io.ReadAll(reader)
		_ = reader.Close()
		if readErr == nil {
			body, readErr = p.decryptAtRest(ctx, req, body)
		}
		var entry httpRetryPayload
		if readErr != nil || json.Unmarshal(body, &entry) != nil || len(entry.BackupRecordIDs) != len(entry.BackupData) || (len(entry.Arrivals) != 0 && len(entry.Arrivals) != len(entry.BackupData)) || len(entry.ProcessedData) == 0 {
			payloadOK = false
			break
		}
		payload.BackupRecordIDs = append(payload.BackupRecordIDs, entry.BackupRecordIDs...)
		payload.BackupData = append(payload.BackupData, entry.BackupData...)
		payload.ProcessedData = append(payload.ProcessedData, entry.ProcessedData...)
		payload.Arrivals = append(payload.Arrivals, entry.Arrivals...)
	}
	if !payloadOK {
		next := now.Add(time.Second)
		for _, item := range items {
			item.Buffer.Next = next
			body, _ := json.Marshal(item.Buffer)
			_ = collection.Put(ctx, item.Key, body)
		}
		return next
	}
	requestID := items[0].Buffer.RequestID
	delivered, permanent, code, message := p.deliverEndpoint(ctx, req, stream, destination, destinationKey, requestID, payload.ProcessedData, now)
	if destinationKey == "SplunkDestinationConfiguration" {
		now = p.deps.Clock.Now().UTC()
	}
	if !delivered {
		p.logDeliveryError(ctx, req, destination, name, message, now)
	}
	retryDuration := httpRetryDuration(destination)
	retrying := !delivered && !permanent && retryDuration > 0 && p.scheduleEndpointRetryPayload(ctx, req, name, destinationKey, requestID, payload, code, message, now, retryDuration)
	if !delivered && !permanent && !retrying {
		p.backupEndpointPayload(ctx, req, stream, destination, destinationKey, name, payload, 0, code, message, now)
	}
	p.deleteHTTPBuffer(ctx, collection, items)
	return time.Time{}
}

func (p *Pack) runHTTPRetries(ctx context.Context) time.Time {
	if p.deps.Store == nil || p.deps.Clock == nil {
		return time.Time{}
	}
	now := p.deps.Clock.Now().UTC()
	var earliest time.Time
	scopes, _ := p.deps.Store.Scopes(ctx)
	for _, identity := range scopes {
		collection := p.deps.Store.Scope(identity.Account, identity.Region).Collection("fh-http-retries")
		retries, _, _ := collection.List(ctx, "", "", 0)
		for _, item := range retries {
			var retry httpRetry
			if json.Unmarshal(item.Value, &retry) != nil {
				continue
			}
			next := retry.Next
			if !next.After(now) {
				next = p.runHTTPRetry(ctx, identity, collection, item.Key, &retry, now)
			}
			if !next.IsZero() && (earliest.IsZero() || next.Before(earliest)) {
				earliest = next
			}
		}
	}
	return earliest
}

func (p *Pack) deleteHTTPRetry(ctx context.Context, collection spi.Collection, key, dataKey string) {
	_ = collection.Delete(ctx, key)
	if p.deps.Blobs != nil {
		_ = p.deps.Blobs.Delete(ctx, dataKey)
	}
}

func (p *Pack) runHTTPRetry(ctx context.Context, identity spi.Identity, collection spi.Collection, key string, retry *httpRetry, now time.Time) time.Time {
	req := &spi.Request{Identity: identity}
	raw, ok, _ := p.col(req, "fh").Get(ctx, retry.Stream)
	if !ok {
		p.deleteHTTPRetry(ctx, collection, key, retry.DataKey)
		return time.Time{}
	}
	var stream map[string]any
	_ = json.Unmarshal(raw, &stream)
	destinationKey := endpointDestinationKey(retry.Destination)
	destination, ok := stream[destinationKey].(map[string]any)
	if !ok {
		p.deleteHTTPRetry(ctx, collection, key, retry.DataKey)
		return time.Time{}
	}
	var payload httpRetryPayload
	payloadOK := false
	if p.deps.Blobs != nil {
		if reader, _, err := p.deps.Blobs.Get(ctx, retry.DataKey); err == nil {
			payloadBody, readErr := io.ReadAll(reader)
			_ = reader.Close()
			if readErr == nil {
				payloadBody, readErr = p.decryptAtRest(ctx, req, payloadBody)
			}
			payloadOK = readErr == nil && json.Unmarshal(payloadBody, &payload) == nil && len(payload.BackupRecordIDs) == len(payload.BackupData) && (len(payload.Arrivals) == 0 || len(payload.Arrivals) == len(payload.BackupData)) && len(payload.ProcessedData) != 0
		}
	}
	if !payloadOK {
		retry.Next = now.Add(time.Second)
		body, _ := json.Marshal(retry)
		_ = collection.Put(ctx, key, body)
		return retry.Next
	}
	if !now.Before(retry.Expires) {
		p.backupEndpointPayload(ctx, req, stream, destination, destinationKey, retry.Stream, payload, retry.Retries, retry.ErrorCode, retry.ErrorMessage, now)
		p.deleteHTTPRetry(ctx, collection, key, retry.DataKey)
		return time.Time{}
	}
	delivered, permanent, code, message := p.deliverEndpoint(ctx, req, stream, destination, destinationKey, retry.RequestID, payload.ProcessedData, now)
	attemptEnded := now
	if destinationKey == "SplunkDestinationConfiguration" {
		attemptEnded = p.deps.Clock.Now().UTC()
		retry.Expires = retry.Expires.Add(attemptEnded.Sub(now))
	}
	if delivered || permanent {
		p.deleteHTTPRetry(ctx, collection, key, retry.DataKey)
		return time.Time{}
	}
	p.logDeliveryError(ctx, req, destination, retry.Stream, message, attemptEnded)
	retry.Retries++
	retry.ErrorCode, retry.ErrorMessage = code, message
	retry.Next = attemptEnded.Add(p.httpRetryDelay(key, retry.Retries))
	if retry.Next.After(retry.Expires) {
		retry.Next = retry.Expires
	}
	body, _ := json.Marshal(retry)
	_ = collection.Put(ctx, key, body)
	return retry.Next
}

func endpointDestinationKey(key string) string {
	if key == "" {
		return "HttpEndpointDestinationConfiguration"
	}
	return key
}

func (p *Pack) deliverEndpoint(ctx context.Context, req *spi.Request, stream, destination map[string]any, destinationKey, requestID string, data [][]byte, now time.Time) (bool, bool, string, string) {
	if destinationKey == "SplunkDestinationConfiguration" {
		return p.deliverSplunk(ctx, req, destination, requestID, data)
	}
	delivered, permanent, message := p.deliverHTTP(ctx, req, stream, destination, requestID, data, now)
	return delivered, permanent, "", message
}

func (p *Pack) deliverSplunk(ctx context.Context, req *spi.Request, destination map[string]any, requestID string, data [][]byte) (bool, bool, string, string) {
	token := first(destination, "HECToken")
	if secrets, _ := destination["SecretsManagerConfiguration"].(map[string]any); secrets["Enabled"] == true {
		response, err := secretsmanager.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "GetSecretValue", Input: map[string]any{"SecretId": first(secrets, "SecretARN")}})
		var secret string
		var ok bool
		if response != nil {
			secret, ok = response.Output["SecretString"].(string)
		}
		value := map[string]any{}
		if err != nil || !ok || json.Unmarshal([]byte(secret), &value) != nil {
			return false, false, "Splunk.InvalidToken", "unable to retrieve Splunk HEC token"
		}
		token, ok = value["hec_token"].(string)
		if !ok || token == "" || len(token) > 2048 || strings.ContainsAny(token, "\r\n") {
			return false, false, "Splunk.InvalidToken", "Splunk secret must contain a valid hec_token"
		}
	}

	path := "/services/collector/event"
	if first(destination, "HECEndpointType") == "Raw" {
		path = "/services/collector/raw"
	}
	endpoint := strings.TrimRight(first(destination, "HECEndpoint"), "/")
	body := bytes.Join(data, nil)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, bytes.NewReader(body))
	if err != nil {
		return false, false, "Splunk.InvalidEndpoint", err.Error()
	}
	request.Header.Set("Authorization", "Splunk "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Splunk-Request-Channel", requestID)
	response, err := p.httpClient.Do(request)
	if err != nil {
		return false, false, "Splunk.InvalidEndpoint", err.Error()
	}
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, false, splunkStatusError(response.StatusCode), response.Status
	}
	if readErr != nil || len(responseBody) > 1<<20 {
		return false, false, "Splunk.InvalidHecResponseCharacter", "invalid Splunk HEC acknowledgment body"
	}
	ackID, ackKey, ok := splunkAckID(responseBody)
	if !ok {
		return false, false, "Splunk.AcknowledgementsDisabled", "invalid Splunk HEC acknowledgment ID"
	}

	timeout, ok := inputInteger(destination["HECAcknowledgmentTimeoutInSeconds"], 180, 600)
	if !ok {
		timeout = 300
	}
	deadline := p.deps.Clock.Now().Add(time.Duration(timeout) * time.Second)
	ackBody, _ := json.Marshal(map[string]any{"acks": []any{ackID}})
	for {
		ackRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/services/collector/ack", bytes.NewReader(ackBody))
		if err != nil {
			return false, false, "Splunk.InvalidEndpoint", err.Error()
		}
		ackRequest.Header.Set("Authorization", "Splunk "+token)
		ackRequest.Header.Set("Content-Type", "application/json")
		ackRequest.Header.Set("X-Splunk-Request-Channel", requestID)
		ackResponse, err := p.httpClient.Do(ackRequest)
		if err != nil {
			return false, false, "Splunk.InvalidEndpoint", err.Error()
		}
		ackResponseBody, readErr := io.ReadAll(io.LimitReader(ackResponse.Body, (1<<20)+1))
		_ = ackResponse.Body.Close()
		if ackResponse.StatusCode != http.StatusOK {
			return false, false, splunkStatusError(ackResponse.StatusCode), ackResponse.Status
		}
		var acknowledgment struct {
			Acks map[string]bool `json:"acks"`
		}
		if readErr != nil || len(ackResponseBody) > 1<<20 || json.Unmarshal(ackResponseBody, &acknowledgment) != nil {
			return false, false, "Splunk.InvalidHecResponseCharacter", "invalid Splunk HEC acknowledgment status"
		}
		if acknowledgment.Acks[ackKey] {
			return true, false, "", ""
		}
		if !p.deps.Clock.Now().Before(deadline) {
			return false, false, "Splunk.AckTimeout", "Did not receive an acknowledgement from HEC before the HEC acknowledgement timeout expired."
		}
		select {
		case <-ctx.Done():
			return false, false, "Splunk.ConnectionClosed", ctx.Err().Error()
		case <-p.deps.Clock.After(time.Second):
		}
	}
}

func splunkStatusError(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return "Splunk.InvalidToken"
	case http.StatusNotFound:
		return "Splunk.URLNotFound"
	case http.StatusRequestEntityTooLarge:
		return "Splunk.ServerError.ContentTooLarge"
	default:
		return "Splunk.ServerError"
	}
}

func splunkAckID(body []byte) (any, string, bool) {
	var acknowledgment map[string]any
	if json.Unmarshal(body, &acknowledgment) != nil {
		return nil, "", false
	}
	value := acknowledgment["ackID"]
	if value == nil {
		value = acknowledgment["ackId"]
	}
	switch value := value.(type) {
	case float64:
		if value < 0 || value != math.Trunc(value) {
			return nil, "", false
		}
		return int64(value), strconv.FormatInt(int64(value), 10), true
	case string:
		if _, err := strconv.ParseUint(value, 10, 64); err != nil {
			return nil, "", false
		}
		return value, value, true
	default:
		return nil, "", false
	}
}

func (p *Pack) deliverHTTP(ctx context.Context, req *spi.Request, stream, destination map[string]any, requestID string, data [][]byte, now time.Time) (bool, bool, string) {
	records := make([]any, len(data))
	for i := range data {
		records[i] = map[string]any{"data": base64.StdEncoding.EncodeToString(data[i])}
	}
	payload, _ := json.Marshal(map[string]any{
		"requestId": requestID,
		"timestamp": now.UnixMilli(),
		"records":   records,
	})
	requestConfiguration, _ := destination["RequestConfiguration"].(map[string]any)
	encoding := first(requestConfiguration, "ContentEncoding")
	if encoding == "GZIP" {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		_, _ = writer.Write(payload)
		_ = writer.Close()
		payload = compressed.Bytes()
	}
	endpoint, _ := destination["EndpointConfiguration"].(map[string]any)
	accessKey, hasAccessKey := endpoint["AccessKey"].(string)
	if secrets, _ := destination["SecretsManagerConfiguration"].(map[string]any); secrets["Enabled"] == true {
		response, err := secretsmanager.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "GetSecretValue", Input: map[string]any{"SecretId": first(secrets, "SecretARN")}})
		var secret string
		var ok bool
		if response != nil {
			secret, ok = response.Output["SecretString"].(string)
		}
		value := map[string]any{}
		if err != nil || !ok || json.Unmarshal([]byte(secret), &value) != nil {
			return false, false, "unable to retrieve HTTP endpoint access key"
		}
		accessKey, hasAccessKey = value["api_key"].(string)
		if !hasAccessKey || !validHTTPAccessKey(accessKey) {
			return false, false, "HTTP endpoint secret must contain a valid api_key"
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, first(endpoint, "Url"), bytes.NewReader(payload))
	if err != nil {
		return false, false, err.Error()
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Firehose-Protocol-Version", "1.0")
	request.Header.Set("X-Amz-Firehose-Request-Id", requestID)
	request.Header.Set("X-Amz-Firehose-Source-Arn", first(stream, "DeliveryStreamARN"))
	if encoding == "GZIP" {
		request.Header.Set("Content-Encoding", "gzip")
	}
	if hasAccessKey {
		request.Header.Set("X-Amz-Firehose-Access-Key", accessKey)
	}
	if raw, exists := requestConfiguration["CommonAttributes"]; exists {
		common := map[string]string{}
		for _, rawAttribute := range raw.([]any) {
			attribute := rawAttribute.(map[string]any)
			common[first(attribute, "AttributeName")] = first(attribute, "AttributeValue")
		}
		encoded, _ := json.Marshal(map[string]any{"commonAttributes": common})
		request.Header.Set("X-Amz-Firehose-Common-Attributes", string(encoded))
	}
	response, err := p.httpClient.Do(request)
	if err != nil {
		return false, false, err.Error()
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestEntityTooLarge {
		return false, true, response.Status
	}
	if response.StatusCode != http.StatusOK {
		return false, false, response.Status
	}
	if !strings.EqualFold(strings.TrimSpace(response.Header.Get("Content-Type")), "application/json") || response.Header.Get("Content-Encoding") != "" {
		return false, false, "invalid HTTP endpoint acknowledgment headers"
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		return false, false, "invalid HTTP endpoint acknowledgment body"
	}
	var acknowledgment map[string]any
	if json.Unmarshal(body, &acknowledgment) != nil || first(acknowledgment, "requestId") != requestID {
		return false, false, "invalid HTTP endpoint acknowledgment request ID"
	}
	timestamp, ok := acknowledgment["timestamp"].(float64)
	if !ok || timestamp != math.Trunc(timestamp) {
		return false, false, "invalid HTTP endpoint acknowledgment timestamp"
	}
	return true, false, ""
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
		case "Snappy":
			data = snappy.Encode(nil, data)
			if recordExtension == "" {
				recordExtension = ".snappy"
			}
		case "HADOOP_SNAPPY":
			data = hadoopSnappy(data)
			if recordExtension == "" {
				recordExtension = ".hsnappy"
			}
		}
		evaluatedPrefix, err := p.evaluatedDynamicS3Prefix(prefix, now, record.partitionKeys, record.queryPartitionKeys)
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

func hadoopSnappy(data []byte) []byte {
	var result bytes.Buffer
	_ = binary.Write(&result, binary.BigEndian, uint32(len(data)))
	const blockSize = 262144 - 262144/6 - 32
	for len(data) > 0 {
		block := data[:min(len(data), blockSize)]
		compressed := snappy.Encode(nil, block)
		_, prefix := binary.Uvarint(compressed)
		_ = binary.Write(&result, binary.BigEndian, uint32(len(compressed)-prefix))
		result.Write(compressed[prefix:])
		data = data[len(block):]
	}
	return result.Bytes()
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
	recID, documentID                  string
	data                               []byte
	arrival                            time.Time
	searchIndex, searchType            string
}

type processingRecord struct {
	recID              string
	data               []byte
	raw                []byte
	partitionKeys      map[string]string
	queryPartitionKeys map[string]string
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
		case "MetadataExtraction":
			var next []processingRecord
			for _, record := range records {
				partitionKeys, err := extractMetadata(processor, record.data)
				if err != nil {
					failures = append(failures, &processingFailure{typeName: "processing-failed", code: "MetadataExtraction.Failed", message: err.Error(), attempts: 1, recID: record.recID, data: record.raw})
					continue
				}
				record.queryPartitionKeys = partitionKeys
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

func extractMetadata(processor map[string]any, data []byte) (map[string]string, error) {
	var input any
	if err := json.Unmarshal(data, &input); err != nil {
		return nil, err
	}
	// ponytail: compile per record; cache by query if inline-partition throughput becomes hot.
	query, _ := gojq.Parse(processorParameter(processor, "MetadataExtractionQuery"))
	code, _ := gojq.Compile(query)
	iter := code.Run(input)
	result, ok := iter.Next()
	if !ok {
		return nil, errors.New("metadata extraction returned no result")
	}
	if err, ok := result.(error); ok {
		return nil, err
	}
	if _, more := iter.Next(); more {
		return nil, errors.New("metadata extraction returned multiple results")
	}
	values, ok := result.(map[string]any)
	if !ok || len(values) == 0 {
		return nil, errors.New("metadata extraction must return an object")
	}
	partitionKeys := make(map[string]string, len(values))
	for key, rawValue := range values {
		switch value := rawValue.(type) {
		case string:
			partitionKeys[key] = value
		case bool, int, float64:
			partitionKeys[key] = fmt.Sprint(value)
		default:
			return nil, errors.New("metadata extraction values must be scalar")
		}
		if key == "" || partitionKeys[key] == "" {
			return nil, errors.New("metadata extraction keys and values must be non-empty")
		}
	}
	return partitionKeys, nil
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
	arrivalTimestamp := timestamp
	if !failure.arrival.IsZero() {
		arrivalTimestamp = failure.arrival.UTC().Format(time.RFC3339Nano)
	}
	payloadFields := map[string]any{
		"attemptsMade": strconv.Itoa(failure.attempts), "arrivalTimestamp": arrivalTimestamp, "errorCode": failure.code, "errorMessage": failure.message,
		"attemptEndingTimestamp": timestamp, "rawData": base64.StdEncoding.EncodeToString(failure.data),
	}
	if failure.lambdaARN != "" {
		payloadFields["lambdaArn"] = failure.lambdaARN
	}
	if failure.documentID != "" {
		payloadFields["esDocumentId"] = failure.documentID
	}
	if failure.searchIndex != "" {
		payloadFields["esIndexName"] = failure.searchIndex
		payloadFields["esTypeName"] = failure.searchType
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
	firehoseTimestampPrefix    = regexp.MustCompile(`!\{timestamp:([^}]*)\}`)
	firehosePrefixExpression   = regexp.MustCompile(`!\{([^}:]+):([^}]*)\}`)
	firehoseFileExtension      = regexp.MustCompile(`^\.[0-9a-z!\-_.*'()]+$`)
	firehoseStreamName         = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	firehoseTagKey             = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]+$`)
	firehoseTagValue           = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@%]*$`)
	firehoseKMSKeyARN          = regexp.MustCompile(`^arn:[^:]+:kms:[a-zA-Z0-9\-]+:\d{12}:key/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseDestinationKMSARN  = regexp.MustCompile(`^arn:.*:kms:[a-zA-Z0-9\-]+:\d{12}:(key|alias)/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseTimeZone           = regexp.MustCompile(`^[a-zA-Z/_]+$`)
	firehoseBucketARN          = regexp.MustCompile(`^arn:.*:s3:::[\w.\-]{1,255}$`)
	firehoseRoleARN            = regexp.MustCompile(`^arn:.*:iam::\d{12}:role/[a-zA-Z_0-9+=,.@\-_/]+$`)
	firehoseRedshiftJDBC       = regexp.MustCompile(`^jdbc:(redshift|postgresql)://([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+redshift(-serverless)?\.[A-Za-z0-9.-]+:[0-9]{1,5}/[A-Za-z0-9_$-]+$`)
	firehoseKinesisStreamARN   = regexp.MustCompile(`^arn:.*:kinesis:[a-zA-Z0-9\-]+:\d{12}:stream/[a-zA-Z0-9_.-]+$`)
	firehoseDomainARN          = regexp.MustCompile(`^arn:.*:es:[a-zA-Z0-9\-]+:\d{12}:domain/[a-z][-0-9a-z]{2,27}$`)
	firehoseMSKClusterARN      = regexp.MustCompile(`^arn:.*:kafka:[a-zA-Z0-9\-]+:\d{12}:cluster/[^/]+/.+$`)
	firehoseMSKTopic           = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	firehoseVPCEndpointService = regexp.MustCompile(`^([a-zA-Z0-9\-_]+\.){2,3}vpce\.[a-zA-Z0-9\-]*\.vpce-svc-[a-zA-Z0-9\-]{17}$`)
	firehoseSecretARN          = regexp.MustCompile(`^arn:.*:secretsmanager:[a-zA-Z0-9\-]+:\d{12}:secret:[a-zA-Z0-9\-/_+=.@!]+$`)
	firehoseLambdaARN          = regexp.MustCompile(`^arn:.*:lambda:[a-zA-Z0-9\-]+:\d{12}:function:[a-zA-Z0-9_-]+(?::[a-zA-Z0-9_-]+)?$`)
	firehoseLogGroup           = regexp.MustCompile(`^[.\-_/#A-Za-z0-9]*$`)
	firehoseLogStream          = regexp.MustCompile(`^[^:*]*$`)
	firehoseDestinationID      = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)
	firehoseGlueCatalogARN     = regexp.MustCompile(`^arn:.*:glue:[a-zA-Z0-9-]+:\d{12}:catalog(?:/[a-z0-9_-]+(?:/[a-zA-Z0-9_.-]+)?)?$`)
	firehoseIcebergName        = regexp.MustCompile(`^[a-zA-Z0-9._]+$`)
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

func (p *Pack) evaluatedDynamicS3Prefix(prefix string, now time.Time, partitionKeys, queryPartitionKeys map[string]string) (string, error) {
	missing := ""
	prefix = firehosePrefixExpression.ReplaceAllStringFunc(prefix, func(expression string) string {
		match := firehosePrefixExpression.FindStringSubmatch(expression)
		keys := partitionKeys
		if match[1] == "partitionKeyFromQuery" {
			keys = queryPartitionKeys
		} else if match[1] != "partitionKeyFromLambda" {
			return expression
		}
		value, ok := keys[match[2]]
		if !ok {
			missing = match[2]
			return expression
		}
		return value
	})
	if missing != "" {
		return "", errors.New("missing partition key: " + missing)
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
