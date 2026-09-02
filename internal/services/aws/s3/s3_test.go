package s3_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bus"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/logging"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kms"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func ident() spi.Identity {
	return spi.Identity{Account: "123456789012", Region: "us-east-1"}
}

func invoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) (*spi.Response, error) {
	return invokeAs(t, p, ident(), op, in, body)
}

func invokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) (*spi.Response, error) {
	t.Helper()
	var rc io.ReadCloser
	if body != nil {
		rc = io.NopCloser(bytes.NewReader(body))
	}
	if in == nil {
		in = map[string]any{}
	}
	return p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.s3",
		Operation: op,
		Input:     in,
		Identity:  id,
		Body:      rc,
	})
}

func mustInvokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invokeAs(t, p, id, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func mustInvoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invoke(t, p, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

var errInjectedRead = errors.New("injected read failure")

type failingReadCloser struct {
	sent bool
}

func (r *failingReadCloser) Read(p []byte) (int, error) {
	if r.sent {
		return 0, errInjectedRead
	}
	r.sent = true
	return copy(p, "partial"), nil
}

func (*failingReadCloser) Close() error { return nil }

type failingReadSeekCloser struct {
	io.ReadSeekCloser
	sent bool
}

func (r *failingReadSeekCloser) Read(p []byte) (int, error) {
	if r.sent {
		return 0, errInjectedRead
	}
	r.sent = true
	return r.ReadSeekCloser.Read(p[:min(len(p), 3)])
}

type failingReadBlobs struct {
	spi.BlobStore
	fail bool
}

func (b *failingReadBlobs) Get(ctx context.Context, key string) (io.ReadSeekCloser, spi.BlobInfo, error) {
	r, info, err := b.BlobStore.Get(ctx, key)
	if err != nil || !b.fail {
		return r, info, err
	}
	return &failingReadSeekCloser{ReadSeekCloser: r}, info, nil
}

func TestBodyReadErrorsDoNotCommitPartialWrites(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "reads"}, nil)
	request := func(operation string, input map[string]any) error {
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: operation, Input: input, Identity: ident(), Body: &failingReadCloser{}})
		return err
	}
	if err := request("PutObject", map[string]any{"Bucket": "reads", "Key": "object"}); !errors.Is(err, errInjectedRead) {
		t.Fatalf("put error = %v", err)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "reads", "Key": "object"}, nil); err == nil {
		t.Fatal("failed PutObject committed partial data")
	}
	upload := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "reads", "Key": "multipart"}, nil).Output["UploadId"]
	partInput := map[string]any{"Bucket": "reads", "Key": "multipart", "UploadId": upload, "PartNumber": 1}
	if err := request("UploadPart", partInput); !errors.Is(err, errInjectedRead) {
		t.Fatalf("part error = %v", err)
	}
	parts := mustInvoke(t, p, "ListParts", partInput, nil).Output["Parts"]
	if listed, _ := parts.([]any); len(listed) != 0 {
		t.Fatalf("failed UploadPart committed partial data: %#v", listed)
	}
}

func TestBlobReadErrorsDoNotReturnOrCopyPartialData(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failingReadBlobs{BlobStore: deps.Blobs}
	deps.Blobs = blobs
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "reads"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "reads", "Key": "source"}, []byte("complete body"))
	blobs.fail = true
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "reads", "Key": "source"}, nil); !errors.Is(err, errInjectedRead) {
		t.Fatalf("get error = %v", err)
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "reads", "Key": "copy", "CopySource": "reads/source"}, nil); !errors.Is(err, errInjectedRead) {
		t.Fatalf("copy error = %v", err)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "reads", "Key": "copy"}, nil); err == nil {
		t.Fatal("failed CopyObject committed partial data")
	}
	upload := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "reads", "Key": "multipart-copy"}, nil).Output["UploadId"]
	partInput := map[string]any{"Bucket": "reads", "Key": "multipart-copy", "UploadId": upload, "PartNumber": 1, "CopySource": "reads/source", "CopySourceRange": "bytes=0-2"}
	if _, err := invoke(t, p, "UploadPartCopy", partInput, nil); !errors.Is(err, errInjectedRead) {
		t.Fatalf("part copy error = %v", err)
	}
	if listed, _ := mustInvoke(t, p, "ListParts", partInput, nil).Output["Parts"].([]any); len(listed) != 0 {
		t.Fatalf("failed UploadPartCopy committed partial data: %#v", listed)
	}
}

func TestExtraBodyReadErrorsDoNotCommitPartialData(t *testing.T) {
	deps := spitest.Deps(t)
	blobs := &failingReadBlobs{BlobStore: deps.Blobs}
	deps.Blobs = blobs
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "reads"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "reads", "Key": "source"}, []byte("id,name\n1,Ada\n"))
	blobs.fail = true
	if _, err := invoke(t, p, "SelectObjectContent", map[string]any{"Bucket": "reads", "Key": "source", "Expression": "SELECT * FROM S3Object"}, nil); !errors.Is(err, errInjectedRead) {
		t.Fatalf("select error = %v", err)
	}

	failedWrite := func(operation string, input map[string]any) error {
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: operation, Input: input, Identity: ident(), Body: &failingReadCloser{}})
		return err
	}
	if err := failedWrite("WriteGetObjectResponse", map[string]any{"RequestRoute": "route", "RequestToken": "token"}); !errors.Is(err, errInjectedRead) {
		t.Fatalf("write response error = %v", err)
	}
	ctx := context.Background()
	scope := deps.Store.Scope(ident().Account, ident().Region)
	if _, ok, err := scope.Collection("wgor").Get(ctx, "route/token"); err != nil || ok {
		t.Fatalf("failed response write persisted: ok=%v err=%v", ok, err)
	}
	if err := failedWrite("CreateBucketMetadataConfiguration", map[string]any{"Bucket": "reads"}); !errors.Is(err, errInjectedRead) {
		t.Fatalf("metadata error = %v", err)
	}
	if _, ok, err := scope.Collection("bktcfg").Get(ctx, "reads/metadata"); err != nil || ok {
		t.Fatalf("failed metadata write persisted: ok=%v err=%v", ok, err)
	}
}

func TestCreateSessionRegistersTemporaryCredential(t *testing.T) {
	deps := spitest.Deps(t)
	response, err := invoke(t, s3.New(deps), "CreateSession", map[string]any{"Bucket": "directory-bucket"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	credentials, _ := response.Output["Credentials"].(map[string]any)
	ak, _ := credentials["AccessKeyId"].(string)
	account, ok, err := deps.Store.Scope("_mirror", "global").Collection("stsk").Get(context.Background(), ak)
	if err != nil || !ok || string(account) != ident().Account {
		t.Fatalf("global session credential marker: account=%q ok=%v err=%v", account, ok, err)
	}
}

func asFault(t *testing.T, err error) *spi.Fault {
	t.Helper()
	f, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("got %T %v, want *spi.Fault", err, err)
	}
	return f
}

func readStream(t *testing.T, resp *spi.Response) []byte {
	t.Helper()
	if resp.Stream == nil {
		t.Fatal("nil stream")
	}
	defer resp.Stream.Close()
	b, err := io.ReadAll(resp.Stream)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func completedPart(number int, response *spi.Response) any {
	return map[string]any{"PartNumber": number, "ETag": response.Headers.Get("ETag")}
}

func completedPartWithChecksum(number int, response *spi.Response, input, header string) any {
	part := completedPart(number, response).(map[string]any)
	part[input] = response.Headers.Get(header)
	return part
}

func completeInput(uploadID string, parts ...any) map[string]any {
	return map[string]any{"UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": parts}}
}

func TestCreatePutGetBytesMatch(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("payload-bytes")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, body)
	resp := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	if got := readStream(t, resp); !bytes.Equal(got, body) {
		t.Fatalf("get bytes %q want %q", got, body)
	}
}

func TestCreateBucketGlobalCollisions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	owner := ident()
	other := spi.Identity{Account: "999999999999", Region: owner.Region}
	west := spi.Identity{Account: owner.Account, Region: "us-west-2"}
	input := map[string]any{"Bucket": "shared-bucket"}
	if response, err := invokeAs(t, p, owner, "CreateBucket", input, nil); err != nil || response.Status != http.StatusOK || response.Headers.Get("Location") != "/shared-bucket" {
		t.Fatalf("initial create = %#v %v", response, err)
	}
	mustInvokeAs(t, p, owner, "PutObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, []byte("preserved"))
	if response, err := invokeAs(t, p, owner, "CreateBucket", input, nil); err != nil || response.Status != http.StatusOK {
		t.Fatalf("us-east-1 recreate = %#v %v", response, err)
	}
	preserved := ""
	if got := mustInvokeAs(t, p, owner, "GetObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, nil); string(readStream(t, got)) != "preserved" {
		t.Fatal("us-east-1 recreation replaced bucket contents")
	} else {
		preserved = "preserved"
	}
	collisions := map[string]any{}
	for name, identity := range map[string]spi.Identity{"owner-other-region": west, "other-account": other} {
		collisionInput := input
		if identity.Region != "us-east-1" {
			collisionInput = map[string]any{"Bucket": "shared-bucket", "LocationConstraint": identity.Region}
		}
		_, err := invokeAs(t, p, identity, "CreateBucket", collisionInput, nil)
		fault := asFault(t, err)
		want := "BucketAlreadyExists"
		if identity.Account == owner.Account {
			want = "BucketAlreadyOwnedByYou"
		}
		if fault.Code != want || fault.HTTPStatus != http.StatusConflict || fault.Fields["BucketName"] != "shared-bucket" {
			t.Fatalf("%s collision = %#v", name, fault)
		}
		head, headErr := invokeAs(t, p, identity, "HeadBucket", input, nil)
		if identity.Account == owner.Account {
			if headErr != nil || head.Headers.Get("x-amz-bucket-region") != owner.Region {
				t.Fatalf("%s cross-region head = %#v %v", name, head, headErr)
			}
		} else if asFault(t, headErr).Code != "NoSuchBucket" {
			t.Fatalf("%s collision exposed foreign bucket: %v", name, headErr)
		}
		collisions[name] = fault.Code
	}
	western := map[string]any{"Bucket": "western-bucket", "LocationConstraint": "us-west-2"}
	mustInvokeAs(t, p, west, "CreateBucket", western, nil)
	_, err := invokeAs(t, p, west, "CreateBucket", western, nil)
	if fault := asFault(t, err); fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict {
		t.Fatalf("non-us-east-1 recreate = %#v", fault)
	} else {
		collisions["owner-non-us-east-recreate"] = fault.Code
	}
	_, err = invokeAs(t, p, owner, "CreateBucket", map[string]any{"Bucket": "western-bucket"}, nil)
	if fault := asFault(t, err); fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict {
		t.Fatalf("stored region collision = %#v", fault)
	} else {
		collisions["owner-stored-other-region"] = fault.Code
	}
	mustInvokeAs(t, p, owner, "DeleteObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, nil)
	mustInvokeAs(t, p, owner, "DeleteBucket", input, nil)
	if _, err := invokeAs(t, p, other, "CreateBucket", input, nil); err != nil {
		t.Fatalf("create after delete: %v", err)
	}
	golden.AssertJSON(t, map[string]any{"collisions": collisions, "recreate": map[string]any{"status": http.StatusOK, "object": preserved}, "reuse_after_delete": "created"})
}

func TestCreateBucketTags(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	tags := []any{map[string]any{"Key": "team", "Value": "storage"}, map[string]any{"Key": "env", "Value": "test"}}
	input := map[string]any{"Bucket": "tagged-bucket", "CreateBucketConfiguration": map[string]any{"Tags": tags}}
	mustInvoke(t, p, "CreateBucket", input, nil)
	response := mustInvoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "tagged-bucket"}, nil)
	if !reflect.DeepEqual(response.Output["TagSet"], tags) {
		t.Fatalf("created bucket tags = %#v", response.Output["TagSet"])
	}
	_, err := invoke(t, p, "CreateBucket", input, nil)
	recreate := asFault(t, err)
	if recreate.Code != "BucketAlreadyOwnedByYou" {
		t.Fatalf("tagged recreation = %v", err)
	}
	invalid := map[string]any{"Bucket": "invalid-tagged-bucket", "CreateBucketConfiguration": map[string]any{"Tags": []any{
		map[string]any{"Key": "duplicate", "Value": "one"}, map[string]any{"Key": "duplicate", "Value": "two"},
	}}}
	_, err = invoke(t, p, "CreateBucket", invalid, nil)
	invalidTags := asFault(t, err)
	if invalidTags.Code != "InvalidTag" {
		t.Fatalf("duplicate create tags = %v", err)
	}
	_, err = invoke(t, p, "HeadBucket", map[string]any{"Bucket": "invalid-tagged-bucket"}, nil)
	invalidBucket := asFault(t, err)
	if invalidBucket.Code != "NoSuchBucket" {
		t.Fatalf("invalid tags reserved bucket = %v", err)
	}
	identity := ident()
	accountRegional := "tagged-" + identity.Account + "-" + identity.Region + "-an"
	mustInvokeAs(t, p, identity, "CreateBucket", map[string]any{
		"Bucket": accountRegional, "BucketNamespace": "account-regional", "CreateBucketConfiguration": map[string]any{"Tags": tags},
	}, nil)
	if response := mustInvokeAs(t, p, identity, "GetBucketTagging", map[string]any{"Bucket": accountRegional}, nil); !reflect.DeepEqual(response.Output["TagSet"], tags) {
		t.Fatalf("account-regional create tags = %#v", response.Output["TagSet"])
	}
	golden.AssertJSON(t, map[string]any{
		"tags": response.Output["TagSet"], "tagged recreation": recreate.Code,
		"invalid tags": invalidTags.Code, "invalid bucket": invalidBucket.Code,
	})
}

func TestBucketNotificationConfiguration(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	input := map[string]any{"Bucket": "notifications"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	for collection, name := range map[string]string{"queues": "queue", "topics": "topic", "lambda": "handler"} {
		if err := deps.Store.Scope("111111111111", "us-east-1").Collection(collection).Put(context.Background(), name, []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	if got := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil).Output; len(got) != 0 {
		t.Fatalf("default notifications = %#v", got)
	}
	configuration := map[string]any{
		"QueueConfigurations":          []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:111111111111:queue", "Events": []any{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{map[string]any{"Name": "prefix", "Value": "images/"}}}}}},
		"TopicConfigurations":          []any{map[string]any{"Id": "topic", "TopicArn": "arn:aws:sns:us-east-1:111111111111:topic", "Events": []any{"s3:ObjectRemoved:*"}}},
		"LambdaFunctionConfigurations": []any{map[string]any{"LambdaFunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:handler", "Events": []any{"s3:ObjectCreated:Put"}}},
		"EventBridgeConfiguration":     map[string]any{},
	}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": configuration}, nil)
	got := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil).Output
	queue := asMapForTest(asSliceForTest(got["QueueConfigurations"])[0])
	id, _ := queue["Id"].(string)
	if len(id) != 8 || asMapForTest(asSliceForTest(asMapForTest(asMapForTest(queue["Filter"])["Key"])["FilterRules"])[0])["Name"] != "Prefix" {
		t.Fatalf("normalized queue = %#v", queue)
	}
	if !reflect.DeepEqual(got["EventBridgeConfiguration"], map[string]any{}) || asMapForTest(asSliceForTest(got["TopicConfigurations"])[0])["Id"] != "topic" {
		t.Fatalf("notifications = %#v", got)
	}
	notificationWithFilter := func(rule map[string]any) map[string]any {
		return map[string]any{
			"QueueConfigurations": []any{
				map[string]any{
					"QueueArn": "arn:aws:sqs:us-east-1:111111111111:queue", "Events": []any{"s3:ObjectCreated:*"},
					"Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{rule}}},
				},
			},
		}
	}
	tests := []struct {
		name, code string
		config     any
	}{
		{"malformed document", "MalformedXML", nil},
		{"missing events", "MalformedXML", map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:111111111111:queue"}}}},
		{"wrong arn service", "InvalidArgument", map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sns:us-east-1:111111111111:queue", "Events": []any{"s3:ObjectCreated:*"}}}}},
		{"missing destination", "InvalidArgument", map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:111111111111:missing", "Events": []any{"s3:ObjectCreated:*"}}}}},
		{"missing filter value", "MalformedXML", notificationWithFilter(map[string]any{"Name": "prefix"})},
		{"invalid filter name", "InvalidArgument", notificationWithFilter(map[string]any{"Name": "contains", "Value": "x"})},
		{"unknown field", "MalformedXML", map[string]any{"UnknownConfigurations": []any{}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": tc.config}
			if tc.name == "malformed document" {
				delete(request, "NotificationConfiguration")
				request["_body"] = "<broken"
			}
			_, err := invoke(t, p, "PutBucketNotificationConfiguration", request, nil)
			if fault := asFault(t, err); fault.Code != tc.code {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}
	if preserved := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil).Output; !reflect.DeepEqual(preserved, got) {
		t.Fatalf("invalid replacement = %#v", preserved)
	}
	skipped := map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:111111111111:missing", "Events": []any{"s3:ObjectCreated:*"}}}}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": skipped, "SkipDestinationValidation": true}, nil)
	if stored := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil).Output; len(asSliceForTest(stored["QueueConfigurations"])) != 1 {
		t.Fatalf("skipped destination validation = %#v", stored)
	}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": map[string]any{}}, nil)
	if cleared := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil).Output; len(cleared) != 0 {
		t.Fatalf("cleared notifications = %#v", cleared)
	}
}

func TestBucketNotificationDeliveryFilters(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	input := map[string]any{"Bucket": "notification-delivery"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("queues").Put(context.Background(), "queue", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	configuration := map[string]any{"QueueConfigurations": []any{
		map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{map[string]any{"Name": "prefix", "Value": "images/"}, map[string]any{"Name": "suffix", "Value": ".jpg"}}}}},
		map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectRemoved:*"}},
	}}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": configuration}, nil)
	for _, key := range []string{"images/photo.jpg", "images/photo.png", "docs/photo.jpg"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": input["Bucket"], "Key": key}, []byte(key))
	}
	messages, _, err := deps.Store.Scope(ident().Account, ident().Region).Collection("msgs:queue").List(context.Background(), "", "", 0)
	if err != nil || len(messages) != 3 {
		t.Fatalf("filtered notifications = %#v, err=%v", messages, err)
	}
	found := false
	for _, stored := range messages {
		var message map[string]any
		if err := json.Unmarshal(stored.Value, &message); err != nil {
			t.Fatal(err)
		}
		body, _ := message["body"].(string)
		found = found || strings.Contains(body, `"key":"images/photo.jpg"`)
	}
	if !found {
		t.Fatalf("notification messages = %#v", messages)
	}
}

func TestBucketNotificationRemovalAndTaggingEvents(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	bucket := "notification-events"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("queues").Put(context.Background(), "queue", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{
		"Bucket": bucket,
		"NotificationConfiguration": map[string]any{"QueueConfigurations": []any{map[string]any{
			"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue",
			"Events":   []any{"s3:ObjectRemoved:*", "s3:ObjectTagging:*"},
		}}},
	}, nil)

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "plain"}, []byte("plain"))
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": "plain"}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": "missing"}, nil)

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}, nil)
	version := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "versioned"}, []byte("versioned")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": "versioned"}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": "versioned", "VersionId": version}, nil)

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "tagged"}, []byte("tagged"))
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": bucket, "Key": "tagged", "TagSet": []any{map[string]any{"Key": "kind", "Value": "test"}}}, nil)
	mustInvoke(t, p, "DeleteObjectTagging", map[string]any{"Bucket": bucket, "Key": "tagged"}, nil)

	messages, _, err := deps.Store.Scope(ident().Account, ident().Region).Collection("msgs:queue").List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	events := map[string]int{}
	for _, stored := range messages {
		var message map[string]any
		if err := json.Unmarshal(stored.Value, &message); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["Records"] == nil {
			continue
		}
		record := asMapForTest(asSliceForTest(payload["Records"])[0])
		events[record["eventName"].(string)]++
	}
	want := map[string]int{"ObjectRemoved:Delete": 2, "ObjectRemoved:DeleteMarkerCreated": 1, "ObjectTagging:Put": 1, "ObjectTagging:Delete": 1}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("notification events = %#v, want %#v", events, want)
	}
}

func TestBucketNotificationLambdaDelivery(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := ident()
	path := t.TempDir() + "/event.json"
	source := "import json\n\ndef lambda_handler(event, context):\n    open(" + strconv.Quote(path) + ", 'w').write(json.dumps(event))\n"
	if _, err := lambda.New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"FunctionName": "handler", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code": map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(source))},
	}}); err != nil {
		t.Fatal(err)
	}
	p := s3.New(deps)
	bucket := "notification-lambda"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	arn := "arn:aws:lambda:us-east-1:123456789012:function:handler:live"
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{
		"Bucket": bucket,
		"NotificationConfiguration": map[string]any{"LambdaFunctionConfigurations": []any{map[string]any{
			"LambdaFunctionArn": arn, "Events": []any{"s3:ObjectCreated:Put"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "created"}, []byte("created"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	record := asMapForTest(asSliceForTest(payload["Records"])[0])
	if record["eventName"] != "ObjectCreated:Put" || asMapForTest(asMapForTest(record["s3"])["object"])["key"] != "created" {
		t.Fatalf("lambda notification = %#v", payload)
	}
}

func TestBucketNotificationTopicDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	topicPack, queuePack := sns.New(deps), sqs.New(deps)
	ctx, id := context.Background(), ident()
	invokePack := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	invokePack(queuePack, "CreateQueue", map[string]any{"QueueName": "subscriber"})
	topicARN := invokePack(topicPack, "CreateTopic", map[string]any{"Name": "object-events"}).Output["TopicArn"].(string)
	invokePack(topicPack, "Subscribe", map[string]any{
		"TopicArn": topicARN, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:123456789012:subscriber", "RawMessageDelivery": "true",
	})

	p := s3.New(deps)
	bucket := "notification-topic"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{
		"Bucket": bucket,
		"NotificationConfiguration": map[string]any{"TopicConfigurations": []any{map[string]any{
			"TopicArn": topicARN, "Events": []any{"s3:ObjectCreated:Put"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "created"}, []byte("created"))
	messages := invokePack(queuePack, "ReceiveMessage", map[string]any{"QueueName": "subscriber", "MaxNumberOfMessages": 10}).Output["Messages"].([]any)
	if len(messages) != 2 {
		t.Fatalf("topic notification = %#v", messages)
	}
	found := false
	for _, message := range messages {
		found = found || strings.Contains(asMapForTest(message)["Body"].(string), `"eventName":"ObjectCreated:Put"`)
	}
	if !found {
		t.Fatalf("topic notification = %#v", messages)
	}
}

func TestBucketNotificationEventBridgeDelivery(t *testing.T) {
	deps := spitest.Deps(t)
	eventPack, queuePack := events.New(deps), sqs.New(deps)
	defer eventPack.Close()
	ctx, id := context.Background(), ident()
	invokePack := func(pack spi.BehaviorPack, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := pack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	invokePack(queuePack, "CreateQueue", map[string]any{"QueueName": "events"})
	invokePack(eventPack, "PutRule", map[string]any{"Name": "s3", "EventPattern": `{"source":["aws.s3"],"detail-type":["Object Created"]}`})
	invokePack(eventPack, "PutTargets", map[string]any{"Rule": "s3", "Targets": []any{map[string]any{
		"Id": "queue", "Arn": "arn:aws:sqs:us-east-1:123456789012:events",
	}}})

	p := s3.New(deps)
	bucket := "notification-eventbridge"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{
		"Bucket": bucket, "NotificationConfiguration": map[string]any{"EventBridgeConfiguration": map[string]any{}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "created"}, []byte("created"))

	messages := invokePack(queuePack, "ReceiveMessage", map[string]any{"QueueName": "events", "MaxNumberOfMessages": 10}).Output["Messages"].([]any)
	if len(messages) != 1 {
		t.Fatalf("eventbridge messages = %#v", messages)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(asMapForTest(messages[0])["Body"].(string)), &event); err != nil {
		t.Fatal(err)
	}
	detail := asMapForTest(event["detail"])
	if event["source"] != "aws.s3" || event["detail-type"] != "Object Created" || asMapForTest(detail["bucket"])["name"] != bucket || asMapForTest(detail["object"])["key"] != "created" {
		t.Fatalf("eventbridge notification = %#v", event)
	}
}

func TestBucketNotificationRestoreAndACLEvents(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	bucket := "notification-restore-acl"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("queues").Put(context.Background(), "queue", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{
		"Bucket": bucket,
		"NotificationConfiguration": map[string]any{"QueueConfigurations": []any{map[string]any{
			"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectRestore:*", "s3:ObjectAcl:*"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "archived", "StorageClass": "GLACIER"}, []byte("archive"))
	mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": bucket, "Key": "archived", "Days": 1}, nil)
	if _, err := invoke(t, p, "PutObjectAcl", map[string]any{"Bucket": bucket, "Key": "missing", "ACL": "private"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("missing object ACL = %v", err)
	}
	mustInvoke(t, p, "PutObjectAcl", map[string]any{"Bucket": bucket, "Key": "archived", "ACL": "private"}, nil)

	messages, _, err := deps.Store.Scope(ident().Account, ident().Region).Collection("msgs:queue").List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	events := map[string]map[string]any{}
	for _, stored := range messages {
		var message map[string]any
		if err := json.Unmarshal(stored.Value, &message); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["Records"] == nil {
			continue
		}
		record := asMapForTest(asSliceForTest(payload["Records"])[0])
		events[record["eventName"].(string)] = record
	}
	if len(events) != 3 || events["ObjectRestore:Post"] == nil || events["ObjectRestore:Completed"] == nil || events["ObjectAcl:Put"] == nil {
		t.Fatalf("restore/ACL events = %#v", events)
	}
	restoreData := asMapForTest(asMapForTest(events["ObjectRestore:Completed"]["glacierEventData"])["restoreEventData"])
	if restoreData["lifecycleRestoreStorageClass"] != "GLACIER" || restoreData["lifecycleRestorationExpiryTime"] == "" {
		t.Fatalf("completed restore event = %#v", events["ObjectRestore:Completed"])
	}
	if events["ObjectAcl:Put"]["eventVersion"] != "2.3" {
		t.Fatalf("ACL event = %#v", events["ObjectAcl:Put"])
	}
	if events["ObjectRestore:Post"]["eventTime"].(string) >= events["ObjectRestore:Completed"]["eventTime"].(string) {
		t.Fatalf("restore event ordering = %#v", events)
	}
}

func TestCreateBucketObjectOwnership(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{}
	assertOwnership := func(bucket, want string) {
		t.Helper()
		response := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": bucket}, nil)
		rules := asSliceForTest(asMapForTest(response.Output["OwnershipControls"])["Rules"])
		if len(rules) != 1 || asMapForTest(rules[0])["ObjectOwnership"] != want {
			t.Fatalf("%s ownership = %#v", bucket, response.Output)
		}
		characterization[bucket] = want
	}

	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "default-ownership"}, nil)
	assertOwnership("default-ownership", "BucketOwnerEnforced")
	for _, ownership := range []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"} {
		bucket := strings.ToLower(ownership)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket, "ObjectOwnership": ownership}, nil)
		assertOwnership(bucket, ownership)
	}
	if _, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucketownerpreferred", "ObjectOwnership": ""}, nil); err != nil {
		t.Fatalf("us-east-1 ownership recreation: %v", err)
	}
	assertOwnership("bucketownerpreferred", "BucketOwnerPreferred")

	_, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": "invalid-ownership", "ObjectOwnership": ""}, nil)
	fault := asFault(t, err)
	if fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "x-amz-object-ownership" {
		t.Fatalf("invalid ownership = %#v", fault)
	}
	characterization["invalid"] = fault.Code
	if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "invalid-ownership"}, nil); asFault(t, err).Code != "NoSuchBucket" {
		t.Fatalf("invalid ownership reserved bucket: %v", err)
	}

	id := ident()
	regional := "owned-" + id.Account + "-" + id.Region + "-an"
	mustInvokeAs(t, p, id, "CreateBucket", map[string]any{"Bucket": regional, "BucketNamespace": "account-regional", "ObjectOwnership": "ObjectWriter"}, nil)
	response := mustInvokeAs(t, p, id, "GetBucketOwnershipControls", map[string]any{"Bucket": regional}, nil)
	rules := asSliceForTest(asMapForTest(response.Output["OwnershipControls"])["Rules"])
	if len(rules) != 1 || asMapForTest(rules[0])["ObjectOwnership"] != "ObjectWriter" {
		t.Fatalf("account-regional ownership = %#v", response.Output)
	}
	characterization["account-regional"] = "ObjectWriter"
	golden.AssertJSON(t, characterization)
}

func TestBucketOwnershipControls(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "ownership-controls"}, nil)
	for _, ownership := range []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"} {
		controls := map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": ownership}}}
		if response := mustInvoke(t, p, "PutBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls", "OwnershipControls": controls}, nil); len(response.Output) != 0 {
			t.Fatalf("%s put output = %#v", ownership, response.Output)
		}
		response := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls"}, nil)
		if !reflect.DeepEqual(response.Output["OwnershipControls"], controls) {
			t.Fatalf("%s controls = %#v", ownership, response.Output)
		}
	}

	invalid := []any{
		nil,
		map[string]any{},
		map[string]any{"Rules": []any{}},
		map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "ObjectWriter"}, map[string]any{"ObjectOwnership": "BucketOwnerPreferred"}}},
		map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": ""}}},
		map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "invalid"}}},
	}
	for _, controls := range invalid {
		_, err := invoke(t, p, "PutBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls", "OwnershipControls": controls}, nil)
		if fault := asFault(t, err); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("controls %#v = %#v", controls, fault)
		}
	}

	response := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls"}, nil)
	if got := asMapForTest(asSliceForTest(asMapForTest(response.Output["OwnershipControls"])["Rules"])[0])["ObjectOwnership"]; got != "BucketOwnerEnforced" {
		t.Fatalf("invalid put replaced controls = %v", got)
	}
	mustInvoke(t, p, "DeleteBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls"}, nil)
	mustInvoke(t, p, "DeleteBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls"}, nil)
	if _, err := invoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls"}, nil); asFault(t, err).Code != "OwnershipControlsNotFoundError" {
		t.Fatalf("get deleted controls: %v", err)
	}
}

func TestPublicAccessBlock(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "public-access-block"}, nil)
	put := func(configuration any) error {
		_, err := invoke(t, p, "PutPublicAccessBlock", map[string]any{"Bucket": "public-access-block", "PublicAccessBlockConfiguration": configuration}, nil)
		return err
	}
	if err := put(map[string]any{"BlockPublicAcls": true}); err != nil {
		t.Fatal(err)
	}
	response := mustInvoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block"}, nil)
	want := map[string]any{"BlockPublicAcls": true, "BlockPublicPolicy": false, "IgnorePublicAcls": false, "RestrictPublicBuckets": false}
	if got := response.Output["PublicAccessBlockConfiguration"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("configuration = %#v", got)
	}
	for _, invalid := range []any{nil, map[string]any{"Unknown": true}, map[string]any{"BlockPublicAcls": "true"}} {
		if fault := asFault(t, put(invalid)); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("configuration %#v fault = %#v", invalid, fault)
		}
	}
	response = mustInvoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block"}, nil)
	if got := response.Output["PublicAccessBlockConfiguration"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid put replaced configuration = %#v", got)
	}
	mustInvoke(t, p, "DeletePublicAccessBlock", map[string]any{"Bucket": "public-access-block"}, nil)
	mustInvoke(t, p, "DeletePublicAccessBlock", map[string]any{"Bucket": "public-access-block"}, nil)
	if _, err := invoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block"}, nil); asFault(t, err).Code != "NoSuchPublicAccessBlockConfiguration" {
		t.Fatalf("get deleted configuration: %v", err)
	}
}

func TestBucketRequestPayment(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "request-payment"}, nil)
	get := func() string {
		payer, _ := mustInvoke(t, p, "GetBucketRequestPayment", map[string]any{"Bucket": "request-payment"}, nil).Output["Payer"].(string)
		return payer
	}
	if got := get(); got != "BucketOwner" {
		t.Fatalf("default payer = %q", got)
	}
	for _, payer := range []string{"Requester", "BucketOwner"} {
		response := mustInvoke(t, p, "PutBucketRequestPayment", map[string]any{"Bucket": "request-payment", "RequestPaymentConfiguration": map[string]any{"Payer": payer}}, nil)
		if len(response.Output) != 0 || get() != payer {
			t.Fatalf("payer %q response=%#v got=%q", payer, response, get())
		}
	}
	for _, payer := range []string{"", "Invalid"} {
		_, err := invoke(t, p, "PutBucketRequestPayment", map[string]any{"Bucket": "request-payment", "RequestPaymentConfiguration": map[string]any{"Payer": payer}}, nil)
		if fault := asFault(t, err); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("payer %q fault=%#v", payer, fault)
		}
	}
	if got := get(); got != "BucketOwner" {
		t.Fatalf("invalid put replaced payer = %q", got)
	}
}

func TestBucketAccelerateConfiguration(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "accelerate"}, nil)
	if output := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate"}, nil).Output; len(output) != 0 {
		t.Fatalf("default configuration = %#v", output)
	}
	get := func() string {
		status, _ := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate"}, nil).Output["Status"].(string)
		return status
	}
	for _, status := range []string{"Enabled", "Suspended"} {
		response := mustInvoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate", "AccelerateConfiguration": map[string]any{"Status": status}}, nil)
		if len(response.Output) != 0 || get() != status {
			t.Fatalf("status %q response=%#v got=%q", status, response, get())
		}
	}
	for _, status := range []string{"", "Invalid"} {
		_, err := invoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate", "AccelerateConfiguration": map[string]any{"Status": status}}, nil)
		if fault := asFault(t, err); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("status %q fault=%#v", status, fault)
		}
	}
	if got := get(); got != "Suspended" {
		t.Fatalf("invalid put replaced status = %q", got)
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "accelerate.with.period"}, nil)
	_, err := invoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate.with.period", "AccelerateConfiguration": map[string]any{"Status": "Enabled"}}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("period bucket fault=%#v", fault)
	}
}

func TestBucketLogging(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	for _, bucket := range []string{"logging-source", "logging-target"} {
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	}
	input := map[string]any{"Bucket": "logging-source"}
	if output := mustInvoke(t, p, "GetBucketLogging", input, nil).Output; len(output) != 0 {
		t.Fatalf("default logging = %#v", output)
	}
	configuration := map[string]any{"TargetBucket": "logging-target", "TargetGrants": []any{map[string]any{"Permission": "READ"}}}
	mustInvoke(t, p, "PutBucketLogging", map[string]any{"Bucket": "logging-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": configuration}}, nil)
	want := map[string]any{"TargetBucket": "logging-target", "TargetPrefix": "", "TargetGrants": []any{map[string]any{"Permission": "READ"}}}
	if got := mustInvoke(t, p, "GetBucketLogging", input, nil).Output["LoggingEnabled"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("logging = %#v", got)
	}
	for _, tc := range []struct {
		name   string
		config map[string]any
		code   string
	}{
		{"missing target name", map[string]any{"TargetPrefix": "logs/"}, "MalformedXML"},
		{"missing target bucket", map[string]any{"TargetBucket": "missing"}, "InvalidTargetBucketForLogging"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutBucketLogging", map[string]any{"Bucket": "logging-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": tc.config}}, nil)
			if fault := asFault(t, err); fault.Code != tc.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}
	if got := mustInvoke(t, p, "GetBucketLogging", input, nil).Output["LoggingEnabled"]; !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid put replaced logging = %#v", got)
	}
	west := ident()
	west.Region = "us-west-2"
	for _, bucket := range []string{"logging-west-source", "logging-west-target"} {
		mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": bucket, "CreateBucketConfiguration": map[string]any{"LocationConstraint": "us-west-2"}}, nil)
	}
	_, err := invoke(t, p, "PutBucketLogging", map[string]any{"Bucket": "logging-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "logging-west-target"}}}, nil)
	if fault := asFault(t, err); fault.Code != "CrossLocationLoggingProhibitted" || fault.Fields["TargetBucketLocation"] != "us-west-2" || fault.Fields["SourceBucketLocation"] != nil {
		t.Fatalf("east cross-location fault = %#v", fault)
	}
	_, err = invokeAs(t, p, west, "PutBucketLogging", map[string]any{"Bucket": "logging-west-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "logging-target"}}}, nil)
	if fault := asFault(t, err); fault.Code != "CrossLocationLoggingProhibitted" || fault.Fields["SourceBucketLocation"] != "us-west-2" || fault.Fields["TargetBucketLocation"] != "us-east-1" {
		t.Fatalf("west cross-location fault = %#v", fault)
	}
	mustInvoke(t, p, "PutBucketLogging", map[string]any{"Bucket": "logging-source", "BucketLoggingStatus": map[string]any{}}, nil)
	if output := mustInvoke(t, p, "GetBucketLogging", input, nil).Output; len(output) != 0 {
		t.Fatalf("disabled logging = %#v", output)
	}
}

func TestBucketCors(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "cors"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, err := invoke(t, p, "GetBucketCors", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchCORSConfiguration" || fault.HTTPStatus != http.StatusNotFound || fault.Fields["BucketName"] != "cors" {
		t.Fatalf("default CORS fault = %#v", fault)
	}
	rules := []any{
		map[string]any{"AllowedMethods": []any{"GET", "HEAD"}, "AllowedOrigins": []any{"https://example.test"}, "AllowedHeaders": []any{"*"}, "ExposeHeaders": []any{"ETag"}, "MaxAgeSeconds": float64(300), "ID": "read"},
		map[string]any{"AllowedMethods": []any{"PUT", "POST", "DELETE"}, "AllowedOrigins": []any{"*"}},
	}
	mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": "cors", "CORSConfiguration": map[string]any{"CORSRules": rules}}, nil)
	if got := mustInvoke(t, p, "GetBucketCors", input, nil).Output["CORSRules"]; !reflect.DeepEqual(got, rules) {
		t.Fatalf("CORS rules = %#v", got)
	}
	tooMany := make([]any, 101)
	for i := range tooMany {
		tooMany[i] = rules[0]
	}
	for _, tc := range []struct {
		name  string
		input map[string]any
		code  string
	}{
		{"malformed body", map[string]any{"_body": "<broken"}, "MalformedXML"},
		{"no rules", map[string]any{"CORSConfiguration": map[string]any{}}, "MalformedXML"},
		{"too many rules", map[string]any{"CORSConfiguration": map[string]any{"CORSRules": tooMany}}, "MalformedXML"},
		{"missing methods", map[string]any{"CORSRules": []any{map[string]any{"AllowedOrigins": []any{"*"}}}}, "MalformedXML"},
		{"missing origins", map[string]any{"CORSRules": []any{map[string]any{"AllowedMethods": []any{"GET"}}}}, "MalformedXML"},
		{"unknown field", map[string]any{"CORSRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}, "Unknown": true}}}, "MalformedXML"},
		{"unsupported method", map[string]any{"CORSRules": []any{map[string]any{"AllowedMethods": []any{"OPTIONS"}, "AllowedOrigins": []any{"*"}}}}, "InvalidRequest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.input["Bucket"] = "cors"
			_, err := invoke(t, p, "PutBucketCors", tc.input, nil)
			if fault := asFault(t, err); fault.Code != tc.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}
	if got := mustInvoke(t, p, "GetBucketCors", input, nil).Output["CORSRules"]; !reflect.DeepEqual(got, rules) {
		t.Fatalf("invalid put replaced CORS = %#v", got)
	}
	for range 2 {
		mustInvoke(t, p, "DeleteBucketCors", input, nil)
	}
	_, err = invoke(t, p, "GetBucketCors", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchCORSConfiguration" {
		t.Fatalf("deleted CORS fault = %#v", fault)
	}
}

func TestBucketCorsHTTP(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "cors-http"}, nil)
	rules := []any{map[string]any{
		"AllowedMethods": []any{"GET", "PUT"}, "AllowedOrigins": []any{"https://*.example.test"},
		"AllowedHeaders": []any{"x-amz-*"}, "ExposeHeaders": []any{"ETag"}, "MaxAgeSeconds": float64(300),
	}}
	mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": "cors-http", "CORSConfiguration": map[string]any{"CORSRules": rules}}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "cors-http", "Key": "key"}, []byte("body"))

	request := httptest.NewRequest(http.MethodOptions, "https://cors-http.s3.us-east-1.amazonaws.com/key", nil)
	request.Header.Set("Origin", "https://app.example.test")
	request.Header.Set("Access-Control-Request-Method", "GET")
	request.Header.Set("Access-Control-Request-Headers", "x-amz-request-payer,x-AMZ-meta-team")
	response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
	if err != nil || response.Status != http.StatusOK || response.Headers.Get("Access-Control-Allow-Origin") != "https://app.example.test" || response.Headers.Get("Access-Control-Allow-Credentials") != "true" || response.Headers.Get("Access-Control-Allow-Headers") != "x-amz-request-payer, x-amz-meta-team" || response.Headers.Get("Access-Control-Expose-Headers") != "ETag" || response.Headers.Get("Access-Control-Max-Age") != "300" || response.Headers.Get("Vary") == "" {
		t.Fatalf("matching preflight = %#v, %v", response, err)
	}
	request.Header.Set("Origin", "https://.example.test")
	request.Header.Set("Access-Control-Request-Headers", " ")
	response, err = p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
	if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != "https://.example.test" || response.Headers.Get("Access-Control-Allow-Headers") != "" {
		t.Fatalf("empty requested header = %#v, %v", response, err)
	}

	for _, rejected := range []struct{ name, origin, method, headers string }{
		{"method", "https://app.example.test", "DELETE", ""},
		{"origin", "https://wrong.test", "GET", ""},
		{"partial origin", "https://app.example.test/", "GET", ""},
		{"header", "https://app.example.test", "GET", "content-type"},
	} {
		t.Run(rejected.name, func(t *testing.T) {
			request.Header.Set("Origin", rejected.origin)
			request.Header.Set("Access-Control-Request-Method", rejected.method)
			request.Header.Set("Access-Control-Request-Headers", rejected.headers)
			_, err = p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
			if fault := asFault(t, err); fault.Code != "AccessForbidden" || fault.HTTPStatus != http.StatusForbidden || fault.Fields["Method"] != rejected.method || fault.Fields["ResourceType"] != "OBJECT" {
				t.Fatalf("rejected preflight = %#v", fault)
			}
		})
	}

	noOrigin := httptest.NewRequest(http.MethodOptions, "https://cors-http.s3.us-east-1.amazonaws.com/key", nil)
	_, err = p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: noOrigin})
	if fault := asFault(t, err); fault.Code != "BadRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing origin = %#v", fault)
	}

	unconfigured := s3.New(spitest.Deps(t))
	mustInvoke(t, unconfigured, "CreateBucket", map[string]any{"Bucket": "cors-none"}, nil)
	noConfig := httptest.NewRequest(http.MethodOptions, "https://cors-none.s3.us-east-1.amazonaws.com/key", nil)
	noConfig.Header.Set("Origin", "https://app.example.test")
	_, err = unconfigured.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: noConfig})
	if fault := asFault(t, err); fault.Code != "AccessForbidden" || fault.Message != "CORSResponse: CORS is not enabled for this bucket." || fault.Fields["Method"] != http.MethodOptions {
		t.Fatalf("unconfigured preflight = %#v", fault)
	}
	noConfig.Header.Set("Origin", "https://app.localstack.cloud")
	noConfig.Header.Set("Access-Control-Request-Private-Network", "true")
	response, err = unconfigured.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: noConfig})
	if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != "https://app.localstack.cloud" || response.Headers.Get("Access-Control-Allow-Methods") != "HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH" || response.Headers.Get("Access-Control-Allow-Private-Network") != "true" || response.Headers.Get("Vary") != "Origin" {
		t.Fatalf("LocalStack default preflight = %#v, %v", response, err)
	}
	for _, origin := range []string{"http://app.localstack.cloud", "https://localhost", "https://localhost.localstack.cloud", "file://", "http://localhost:4566", "https://localhost.localstack.cloud:4566", "http://bucket.s3-website.localhost.localstack.cloud:4566", "http://distribution.cloudfront.localhost:4566"} {
		request := httptest.NewRequest(http.MethodOptions, "https://cors-none.s3.us-east-1.amazonaws.com:4566/key", nil)
		request.Header.Set("Origin", origin)
		response, err := unconfigured.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
		if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != origin {
			t.Errorf("LocalStack default origin %q = %#v, %v", origin, response, err)
		}
	}
	for _, origin := range []string{"http://localhost:9999", "http://bucket.s3-website.evil.test:4566", "http://distribution.cloudfront.evil.test:4566"} {
		forbidden := httptest.NewRequest(http.MethodOptions, "https://cors-none.s3.us-east-1.amazonaws.com:4566/key", nil)
		forbidden.Header.Set("Origin", origin)
		_, err = unconfigured.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: forbidden})
		if fault := asFault(t, err); fault.Code != "AccessForbidden" {
			t.Errorf("forbidden default origin %q = %#v", origin, fault)
		}
	}

	get := httptest.NewRequest(http.MethodGet, "https://cors-http.s3.us-east-1.amazonaws.com/key", nil)
	get.Header.Set("Origin", "https://app.example.test")
	response, err = p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: get})
	if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != "https://app.example.test" || response.Headers.Get("Access-Control-Allow-Methods") != "GET, PUT" {
		t.Fatalf("matching actual request = %#v, %v", response, err)
	}
}

func TestBucketWebsite(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "website"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, err := invoke(t, p, "GetBucketWebsite", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchWebsiteConfiguration" || fault.HTTPStatus != http.StatusNotFound || fault.Fields["BucketName"] != "website" {
		t.Fatalf("default website fault = %#v", fault)
	}
	redirect := map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test", "Protocol": "https"}}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website", "WebsiteConfiguration": redirect}, nil)
	if got := mustInvoke(t, p, "GetBucketWebsite", input, nil).Output; !reflect.DeepEqual(got, redirect) {
		t.Fatalf("redirect website = %#v", got)
	}
	website := map[string]any{
		"IndexDocument": map[string]any{"Suffix": "index.html"},
		"ErrorDocument": map[string]any{"Key": "error.html"},
		"RoutingRules":  []any{map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "docs/"}, "Redirect": map[string]any{"Protocol": "https", "ReplaceKeyPrefixWith": "manual/"}}},
	}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website", "WebsiteConfiguration": website}, nil)
	if got := mustInvoke(t, p, "GetBucketWebsite", input, nil).Output; !reflect.DeepEqual(got, website) {
		t.Fatalf("website = %#v", got)
	}
	tooMany := make([]any, 51)
	for i := range tooMany {
		tooMany[i] = map[string]any{"Redirect": map[string]any{}}
	}
	for _, tc := range []struct {
		name   string
		config any
		code   string
		status int
	}{
		{"malformed body", nil, "MalformedXML", http.StatusBadRequest},
		{"redirect with index", map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test"}, "IndexDocument": map[string]any{"Suffix": "index.html"}}, "InvalidArgument", http.StatusBadRequest},
		{"redirect without host", map[string]any{"RedirectAllRequestsTo": map[string]any{"Protocol": "https"}}, "MalformedXML", http.StatusBadRequest},
		{"redirect protocol", map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test", "Protocol": "ftp"}}, "InvalidRequest", http.StatusBadRequest},
		{"missing index", map[string]any{}, "InvalidArgument", http.StatusBadRequest},
		{"empty suffix", map[string]any{"IndexDocument": map[string]any{"Suffix": ""}}, "InvalidArgument", http.StatusBadRequest},
		{"slash suffix", map[string]any{"IndexDocument": map[string]any{"Suffix": "dir/index.html"}}, "InvalidArgument", http.StatusBadRequest},
		{"empty error key", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "ErrorDocument": map[string]any{}}, "MalformedXML", http.StatusBadRequest},
		{"empty routing rules", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{}}, "MalformedXML", http.StatusBadRequest},
		{"too many routing rules", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": tooMany}, "InternalError", http.StatusInternalServerError},
		{"two replacement forms", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Redirect": map[string]any{"ReplaceKeyPrefixWith": "a", "ReplaceKeyWith": "b"}}}}, "InvalidRequest", http.StatusBadRequest},
		{"empty condition", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Condition": map[string]any{}, "Redirect": map[string]any{}}}}, "InvalidRequest", http.StatusBadRequest},
		{"routing protocol", map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Redirect": map[string]any{"Protocol": "ftp"}}}}, "InvalidRequest", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := map[string]any{"Bucket": "website", "WebsiteConfiguration": tc.config}
			if tc.name == "malformed body" {
				request["_body"] = "<broken"
			}
			_, err := invoke(t, p, "PutBucketWebsite", request, nil)
			fault := asFault(t, err)
			if fault.Code != tc.code || fault.HTTPStatus != tc.status {
				t.Fatalf("fault = %#v", fault)
			}
			if tc.name == "missing index" && fault.Message != "A value for IndexDocument Suffix must be provided if RedirectAllRequestsTo is empty" {
				t.Fatalf("missing index fault = %#v", fault)
			}
		})
	}
	if got := mustInvoke(t, p, "GetBucketWebsite", input, nil).Output; !reflect.DeepEqual(got, website) {
		t.Fatalf("invalid put replaced website = %#v", got)
	}
	for range 2 {
		mustInvoke(t, p, "DeleteBucketWebsite", input, nil)
	}
	_, err = invoke(t, p, "GetBucketWebsite", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchWebsiteConfiguration" {
		t.Fatalf("deleted website fault = %#v", fault)
	}
}

func TestStaticWebsiteHostingCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	ctx := logging.WithRequestID(context.Background(), "request-id")
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "website-hosting"}, nil)
	put := func(key, body, redirect string) {
		t.Helper()
		input := map[string]any{"Bucket": "website-hosting", "Key": key, "ContentType": "text/html"}
		if redirect != "" {
			input["WebsiteRedirectLocation"] = redirect
		}
		mustInvoke(t, p, "PutObject", input, []byte(body))
	}
	put("index.html", "index", "")
	put("docs/index.html", "docs", "")
	put("error.html", "error", "")
	put("redirected.html", "redirected", "")
	put("object-redirect", "", "/redirected.html")
	put("error-redirect", "", "/redirected.html")
	put("prefixed-object", "prefixed", "/object-target.html")
	put("both/existing", "existing", "")
	configuration := map[string]any{
		"IndexDocument": map[string]any{"Suffix": "index.html"},
		"ErrorDocument": map[string]any{"Key": "error.html"},
	}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website-hosting", "WebsiteConfiguration": configuration}, nil)

	request := func(method, path string, headers ...http.Header) map[string]any {
		t.Helper()
		httpRequest := httptest.NewRequest(method, "http://website-hosting.s3-website.localhost.localstack.cloud"+path, nil)
		if len(headers) != 0 {
			httpRequest.Header = headers[0]
		}
		response, err := p.Invoke(ctx, &spi.Request{Identity: ident(), Operation: "GetObject", Input: map[string]any{}, HTTP: httpRequest})
		if err != nil {
			t.Fatal(err)
		}
		body := ""
		if response.Stream != nil {
			body = string(readStream(t, response))
		}
		return map[string]any{
			"status": response.Status,
			"body":   body,
			"type":   response.Headers.Get("Content-Type"),
			"etag":   response.Headers.Get("ETag"),
			"where":  response.Headers.Get("Location"),
		}
	}

	characterization := map[string]any{
		"root":          request(http.MethodGet, "/"),
		"directory":     request(http.MethodGet, "/docs/"),
		"directory-hop": request(http.MethodGet, "/docs"),
		"missing":       request(http.MethodGet, "/missing"),
		"object-hop":    request(http.MethodGet, "/object-redirect"),
		"method":        request(http.MethodPost, "/"),
	}
	etag, _ := characterization["root"].(map[string]any)["etag"].(string)
	characterization["not-modified"] = request(http.MethodGet, "/", http.Header{"If-None-Match": []string{etag}})
	configuration["ErrorDocument"] = map[string]any{"Key": "missing-error.html"}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website-hosting", "WebsiteConfiguration": configuration}, nil)
	characterization["missing-error-document"] = request(http.MethodGet, "/missing")
	configuration["ErrorDocument"] = map[string]any{"Key": "error-redirect"}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website-hosting", "WebsiteConfiguration": configuration}, nil)
	characterization["error-document-hop"] = request(http.MethodGet, "/missing")
	configuration["ErrorDocument"] = map[string]any{"Key": "error.html"}

	configuration["RoutingRules"] = []any{
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "both/", "HttpErrorCodeReturnedEquals": "404"}, "Redirect": map[string]any{"ReplaceKeyWith": "redirected.html"}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "host/"}, "Redirect": map[string]any{"HostName": "example.test"}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "protocol/"}, "Redirect": map[string]any{"Protocol": "https"}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "code/"}, "Redirect": map[string]any{"HttpRedirectCode": "307"}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "prefixed"}, "Redirect": map[string]any{"ReplaceKeyWith": "redirected.html"}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "old/"}, "Redirect": map[string]any{"ReplaceKeyPrefixWith": ""}},
		map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "index"}, "Redirect": map[string]any{"ReplaceKeyWith": "redirected.html"}},
		map[string]any{"Condition": map[string]any{"HttpErrorCodeReturnedEquals": "404"}, "Redirect": map[string]any{"ReplaceKeyWith": "redirected.html"}},
	}
	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website-hosting", "WebsiteConfiguration": configuration}, nil)
	characterization["combined-rule"] = request(http.MethodGet, "/both/missing")
	characterization["combined-existing"] = request(http.MethodGet, "/both/existing")
	characterization["host-rule"] = request(http.MethodGet, "/host/key")
	characterization["protocol-rule"] = request(http.MethodGet, "/protocol/key")
	characterization["status-rule"] = request(http.MethodGet, "/code/key")
	characterization["rule-before-object"] = request(http.MethodGet, "/prefixed-object")
	characterization["prefix-rule"] = request(http.MethodGet, "/old/index.html")
	characterization["error-rule"] = request(http.MethodGet, "/still-missing")
	characterization["root-with-rules"] = request(http.MethodGet, "/")

	mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": "website-hosting", "WebsiteConfiguration": map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test", "Protocol": "https"}}}, nil)
	characterization["redirect-all"] = request(http.MethodGet, "/path?q=1")
	golden.AssertJSON(t, characterization)
}

func TestBucketLifecycleConfiguration(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "lifecycle"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, err := invoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchLifecycleConfiguration" || fault.HTTPStatus != http.StatusNotFound || fault.Fields["BucketName"] != "lifecycle" {
		t.Fatalf("default lifecycle fault = %#v", fault)
	}
	rules := []any{
		map[string]any{
			"ID": "expire-images", "Status": "Enabled",
			"Filter":                         map[string]any{"And": map[string]any{"Prefix": "images/", "Tags": []any{map[string]any{"Key": "class", "Value": "temporary"}}}},
			"Expiration":                     map[string]any{"Days": float64(7)},
			"Transitions":                    []any{map[string]any{"Days": float64(1), "StorageClass": "GLACIER"}},
			"NoncurrentVersionExpiration":    map[string]any{"NoncurrentDays": float64(30)},
			"AbortIncompleteMultipartUpload": map[string]any{"DaysAfterInitiation": float64(2)},
		},
	}
	put := mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": "lifecycle", "LifecycleConfiguration": map[string]any{"Rules": rules}}, nil)
	if put.Status != http.StatusOK || put.Headers.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" || len(put.Output) != 0 {
		t.Fatalf("put lifecycle = %#v", put)
	}
	get := mustInvoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	if get.Headers.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" || !reflect.DeepEqual(get.Output["Rules"], rules) {
		t.Fatalf("get lifecycle = %#v", get)
	}
	mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{
		"Bucket": "lifecycle", "TransitionDefaultMinimumObjectSize": "varies_by_storage_class",
		"LifecycleConfiguration": map[string]any{"Rules": rules},
	}, nil)
	if got := mustInvoke(t, p, "GetBucketLifecycleConfiguration", input, nil).Headers.Get("x-amz-transition-default-minimum-object-size"); got != "varies_by_storage_class" {
		t.Fatalf("transition minimum = %q", got)
	}
	invalid := []struct {
		name          string
		configuration any
		minimum       string
		code          string
	}{
		{"missing rules", map[string]any{}, "", "MalformedXML"},
		{"missing id", map[string]any{"Rules": []any{map[string]any{"Filter": map[string]any{}, "Status": "Enabled"}}}, "", "MalformedXML"},
		{"missing filter", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled"}}}, "", "MalformedXML"},
		{"missing status", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Filter": map[string]any{}}}}, "", "MalformedXML"},
		{"multiple filters", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled", "Filter": map[string]any{"Prefix": "a", "Tag": map[string]any{"Key": "k", "Value": "v"}}}}}, "", "MalformedXML"},
		{"empty noncurrent expiration", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled", "Filter": map[string]any{}, "NoncurrentVersionExpiration": map[string]any{}}}}, "", "MalformedXML"},
		{"delete marker with days", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled", "Filter": map[string]any{}, "Expiration": map[string]any{"Days": 1, "ExpiredObjectDeleteMarker": true}}}}, "", "MalformedXML"},
		{"non-midnight date", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled", "Filter": map[string]any{}, "Expiration": map[string]any{"Date": "2030-01-01T01:00:00Z"}}}}, "", "InvalidArgument"},
		{"duplicate tags", map[string]any{"Rules": []any{map[string]any{"ID": "id", "Status": "Enabled", "Filter": map[string]any{"And": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "a"}, map[string]any{"Key": "k", "Value": "b"}}}}}}}, "", "InvalidRequest"},
		{"invalid transition minimum", map[string]any{"Rules": rules}, "invalid", "InvalidRequest"},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			request := map[string]any{"Bucket": "lifecycle", "LifecycleConfiguration": test.configuration}
			if test.minimum != "" {
				request["TransitionDefaultMinimumObjectSize"] = test.minimum
			}
			_, err := invoke(t, p, "PutBucketLifecycleConfiguration", request, nil)
			if fault := asFault(t, err); fault.Code != test.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}
	_, err = invoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{
		"Bucket": "lifecycle", "TransitionDefaultMinimumObjectSize": "",
		"LifecycleConfiguration": map[string]any{"Rules": rules},
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("empty transition minimum fault = %#v", fault)
	}
	if got := mustInvoke(t, p, "GetBucketLifecycleConfiguration", input, nil); got.Headers.Get("x-amz-transition-default-minimum-object-size") != "varies_by_storage_class" || !reflect.DeepEqual(got.Output["Rules"], rules) {
		t.Fatalf("invalid put replaced lifecycle = %#v", got)
	}
	for range 2 {
		mustInvoke(t, p, "DeleteBucketLifecycle", input, nil)
	}
	_, err = invoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchLifecycleConfiguration" {
		t.Fatalf("deleted lifecycle fault = %#v", fault)
	}
}

func TestBucketPolicyConfiguration(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "policy"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, err := invoke(t, p, "GetBucketPolicy", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucketPolicy" || fault.HTTPStatus != http.StatusNotFound || fault.Fields["BucketName"] != "policy" {
		t.Fatalf("default policy fault = %#v", fault)
	}

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy/*"}]}`
	put := mustInvoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": "policy", "Policy": policy}, nil)
	if put.Status != http.StatusOK || len(put.Output) != 0 {
		t.Fatalf("put policy = %#v", put)
	}
	if got := mustInvoke(t, p, "GetBucketPolicy", input, nil).Output["Policy"]; got != policy {
		t.Fatalf("policy = %q", got)
	}

	for _, test := range []struct {
		name, policy, message string
	}{
		{"empty", "", "Policies must be valid JSON and the first byte must be '{'"},
		{"leading whitespace", " " + policy, "Policies must be valid JSON and the first byte must be '{'"},
		{"array", `[]`, "Policies must be valid JSON and the first byte must be '{'"},
		{"invalid json", `{`, "Policies must be valid JSON and the first byte must be '{'"},
		{"empty object", `{}`, "Missing required field Statement"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": "policy", "Policy": test.policy}, nil)
			fault := asFault(t, err)
			if fault.Code != "MalformedPolicy" || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
			if got := mustInvoke(t, p, "GetBucketPolicy", input, nil).Output["Policy"]; got != policy {
				t.Fatalf("invalid put replaced policy = %q", got)
			}
		})
	}
	for range 2 {
		mustInvoke(t, p, "DeleteBucketPolicy", input, nil)
	}
	_, err = invoke(t, p, "GetBucketPolicy", input, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucketPolicy" {
		t.Fatalf("deleted policy fault = %#v", fault)
	}
}

func TestBucketLifecycleExpirationHeaders(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "lifecycle-expiration"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	rules := []any{
		map[string]any{"ID": "marker", "Filter": map[string]any{}, "Status": "Enabled", "Expiration": map[string]any{"ExpiredObjectDeleteMarker": true}},
		map[string]any{
			"ID": "expire-images", "Status": "Enabled", "Expiration": map[string]any{"Days": 7},
			"Filter": map[string]any{"And": map[string]any{
				"Prefix": "images/", "ObjectSizeGreaterThan": 4, "ObjectSizeLessThan": 10,
				"Tags": []any{map[string]any{"Key": "class", "Value": "temporary"}},
			}},
		},
	}
	mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": bucket["Bucket"], "LifecycleConfiguration": map[string]any{"Rules": rules}}, nil)
	expected := `expiry-date="Fri, 09 Jan 1970 00:00:00 GMT", rule-id="expire-images"`
	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/a.jpg", "Tagging": "class=temporary"}, []byte("photo"))
	if got := put.Headers.Get("x-amz-expiration"); got != expected {
		t.Fatalf("put expiration = %q", got)
	}
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/multipart.jpg", "Tagging": "class=temporary"}, nil).Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/multipart.jpg", "UploadId": uploadID, "PartNumber": 1}, []byte("photo"))
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := completed.Headers.Get("x-amz-expiration"); got != expected {
		t.Fatalf("complete expiration = %q", got)
	}
	copy := mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/copied.jpg", "CopySource": "lifecycle-expiration/images/a.jpg"}, nil)
	if got := copy.Headers.Get("x-amz-expiration"); got != "" {
		t.Fatalf("LocalStack copy expiration = %q", got)
	}
	for _, operation := range []string{"GetObject", "HeadObject"} {
		response := mustInvoke(t, p, operation, map[string]any{"Bucket": bucket["Bucket"], "Key": "images/a.jpg"}, nil)
		if got := response.Headers.Get("x-amz-expiration"); got != expected {
			t.Fatalf("%s expiration = %q", operation, got)
		}
		if response.Stream != nil {
			_ = response.Stream.Close()
		}
	}
	nonmatch := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/b.jpg", "Tagging": "class=permanent"}, []byte("photo"))
	if got := nonmatch.Headers.Get("x-amz-expiration"); got != "" {
		t.Fatalf("nonmatching expiration = %q", got)
	}
	dateRule := []any{map[string]any{"ID": "dated", "Filter": map[string]any{"Tag": map[string]any{"Key": "class", "Value": "temporary"}}, "Status": "Enabled", "Expiration": map[string]any{"Date": "2030-01-01T00:00:00Z"}}}
	mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": bucket["Bucket"], "LifecycleConfiguration": map[string]any{"Rules": dateRule}}, nil)
	if got := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "images/a.jpg"}, nil).Headers.Get("x-amz-expiration"); got != `expiry-date="Tue, 01 Jan 2030 00:00:00 GMT", rule-id="dated"` {
		t.Fatalf("dated expiration = %q", got)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket["Bucket"], "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned", "Tagging": "class=temporary"}, []byte("first")).Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned", "Tagging": "class=temporary"}, []byte("second")).Headers.Get("x-amz-version-id")
	for _, test := range []struct {
		operation, version string
		want               bool
	}{
		{"GetObject", first, false}, {"GetObject", second, true}, {"HeadObject", second, false},
	} {
		response := mustInvoke(t, p, test.operation, map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned", "VersionId": test.version}, nil)
		if got := response.Headers.Get("x-amz-expiration") != ""; got != test.want {
			t.Fatalf("%s version %s expiration present=%v want=%v", test.operation, test.version, got, test.want)
		}
		if response.Stream != nil {
			_ = response.Stream.Close()
		}
	}
}

func TestNamedBucketConfigurations(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "named-configurations"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	put := func(operation, field, id string, configuration map[string]any) (*spi.Response, error) {
		return invoke(t, p, operation, map[string]any{"Bucket": bucket["Bucket"], "Id": id, field: configuration}, nil)
	}
	mustPut := func(operation, field, id string, configuration map[string]any) {
		if _, err := put(operation, field, id, configuration); err != nil {
			t.Fatal(err)
		}
	}
	stringValue := func(value any) string { result, _ := value.(string); return result }

	if _, err := put("PutBucketAnalyticsConfiguration", "AnalyticsConfiguration", "request-id", map[string]any{"Id": "body-id"}); asFault(t, err).Code != "MalformedXML" {
		t.Fatalf("analytics id mismatch = %v", err)
	}
	for _, id := range []string{"z-analysis", "a-analysis"} {
		mustPut("PutBucketAnalyticsConfiguration", "AnalyticsConfiguration", id, map[string]any{"Id": id, "Filter": map[string]any{"Prefix": id}})
	}
	analytics := mustInvoke(t, p, "ListBucketAnalyticsConfigurations", bucket, nil)
	analyticsList := asSliceForTest(analytics.Output["AnalyticsConfigurationList"])
	if analytics.Output["IsTruncated"] != false || len(analyticsList) != 2 || asMapForTest(analyticsList[0])["Id"] != "a-analysis" || asMapForTest(analyticsList[1])["Id"] != "z-analysis" {
		t.Fatalf("analytics list = %#v", analytics.Output)
	}
	gotAnalytics := asMapForTest(mustInvoke(t, p, "GetBucketAnalyticsConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "a-analysis"}, nil).Output["AnalyticsConfiguration"])
	if asMapForTest(gotAnalytics["Filter"])["Prefix"] != "a-analysis" {
		t.Fatalf("analytics get = %#v", gotAnalytics)
	}
	if _, err := invoke(t, p, "DeleteBucketAnalyticsConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "missing"}, nil); asFault(t, err).Code != "NoSuchConfiguration" {
		t.Fatalf("missing analytics delete = %v", err)
	}

	if _, err := put("PutBucketIntelligentTieringConfiguration", "IntelligentTieringConfiguration", "request-id", map[string]any{"Id": "body-id"}); asFault(t, err).Code != "MalformedXML" {
		t.Fatalf("intelligent tiering id mismatch = %v", err)
	}
	mustPut("PutBucketIntelligentTieringConfiguration", "IntelligentTieringConfiguration", "tiering", map[string]any{"Id": "tiering", "Status": "Enabled"})
	if got := asSliceForTest(mustInvoke(t, p, "ListBucketIntelligentTieringConfigurations", bucket, nil).Output["IntelligentTieringConfigurationList"]); len(got) != 1 || asMapForTest(got[0])["Id"] != "tiering" {
		t.Fatalf("intelligent tiering list = %#v", got)
	}
	if _, err := invoke(t, p, "GetBucketIntelligentTieringConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "tiering", "ExpectedBucketOwner": "999999999999"}, nil); err != nil {
		t.Fatalf("intelligent tiering expected owner = %v", err)
	}

	inventory := func() map[string]any {
		return map[string]any{
			"Id": "inventory", "IsEnabled": true, "IncludedObjectVersions": "All",
			"Destination":    map[string]any{"S3BucketDestination": map[string]any{"Bucket": "arn:aws:s3:::destination", "Format": "CSV"}},
			"Schedule":       map[string]any{"Frequency": "Daily"},
			"OptionalFields": []any{"Size", "ETag"},
		}
	}
	mustPut("PutBucketInventoryConfiguration", "InventoryConfiguration", "inventory", inventory())
	invalidInventory := []struct {
		name string
		code string
		edit func(map[string]any)
	}{
		{"unknown root field", "MalformedXML", func(v map[string]any) { v["Unknown"] = true }},
		{"id mismatch", "IdMismatch", func(v map[string]any) { v["Id"] = "other" }},
		{"invalid destination", "InvalidS3DestinationBucket", func(v map[string]any) {
			asMapForTest(asMapForTest(v["Destination"])["S3BucketDestination"])["Bucket"] = "destination"
		}},
		{"invalid format", "MalformedXML", func(v map[string]any) {
			asMapForTest(asMapForTest(v["Destination"])["S3BucketDestination"])["Format"] = "JSON"
		}},
		{"invalid frequency", "MalformedXML", func(v map[string]any) { asMapForTest(v["Schedule"])["Frequency"] = "Hourly" }},
		{"invalid versions", "MalformedXML", func(v map[string]any) { v["IncludedObjectVersions"] = "Previous" }},
		{"invalid optional field", "MalformedXML", func(v map[string]any) { v["OptionalFields"] = []any{"Unknown"} }},
	}
	invalidInventoryCodes := map[string]any{}
	for _, test := range invalidInventory {
		t.Run(test.name, func(t *testing.T) {
			configuration := inventory()
			test.edit(configuration)
			_, err := put("PutBucketInventoryConfiguration", "InventoryConfiguration", "inventory", configuration)
			if fault := asFault(t, err); fault.Code != test.code {
				t.Fatalf("fault = %#v", fault)
			} else {
				invalidInventoryCodes[test.name] = fault.Code
			}
		})
	}
	storedInventory := asMapForTest(mustInvoke(t, p, "GetBucketInventoryConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "inventory"}, nil).Output["InventoryConfiguration"])
	if stringValue(asMapForTest(asMapForTest(storedInventory["Destination"])["S3BucketDestination"])["Format"]) != "CSV" {
		t.Fatalf("invalid inventory replaced baseline = %#v", storedInventory)
	}

	for i := range 1000 {
		id := fmt.Sprintf("%04d", i)
		mustPut("PutBucketMetricsConfiguration", "MetricsConfiguration", id, map[string]any{"Id": id})
	}
	mustPut("PutBucketMetricsConfiguration", "MetricsConfiguration", "0000", map[string]any{"Id": "0000", "Filter": map[string]any{"Prefix": "updated"}})
	if _, err := put("PutBucketMetricsConfiguration", "MetricsConfiguration", "1000", map[string]any{"Id": "1000"}); asFault(t, err).Code != "TooManyConfigurations" {
		t.Fatalf("metrics limit = %v", err)
	}
	first := mustInvoke(t, p, "ListBucketMetricsConfigurations", bucket, nil)
	firstPage := asSliceForTest(first.Output["MetricsConfigurationList"])
	next := stringValue(first.Output["NextContinuationToken"])
	if first.Output["IsTruncated"] != true || len(firstPage) != 100 || stringValue(asMapForTest(firstPage[0])["Id"]) != "0000" || next == "" {
		t.Fatalf("metrics first page = %#v", first.Output)
	}
	second := mustInvoke(t, p, "ListBucketMetricsConfigurations", map[string]any{"Bucket": bucket["Bucket"], "ContinuationToken": next}, nil)
	secondPage := asSliceForTest(second.Output["MetricsConfigurationList"])
	if second.Output["ContinuationToken"] != next || len(secondPage) != 100 || stringValue(asMapForTest(secondPage[0])["Id"]) != "0100" {
		t.Fatalf("metrics second page = %#v", second.Output)
	}
	if _, err := invoke(t, p, "ListBucketMetricsConfigurations", map[string]any{"Bucket": bucket["Bucket"], "ContinuationToken": "invalid"}, nil); asFault(t, err).Code != "InvalidToken" {
		t.Fatalf("invalid metrics token = %v", err)
	}
	golden.AssertJSON(t, map[string]any{
		"analytics":          analytics.Output,
		"inventory":          map[string]any{"configuration": storedInventory, "invalid": invalidInventoryCodes},
		"intelligentTiering": mustInvoke(t, p, "ListBucketIntelligentTieringConfigurations", bucket, nil).Output,
		"metrics":            map[string]any{"firstCount": len(firstPage), "firstId": asMapForTest(firstPage[0])["Id"], "secondCount": len(secondPage), "secondId": asMapForTest(secondPage[0])["Id"], "truncated": first.Output["IsTruncated"]},
	})
}

func TestBucketAndObjectACLConfigurations(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "acl-configurations"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	defaultACL := mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output
	if len(asSliceForTest(defaultACL["Grants"])) != 1 || asMapForTest(defaultACL["Owner"])["ID"] != ident().Account {
		t.Fatalf("default bucket ACL = %#v", defaultACL)
	}
	mustInvoke(t, p, "PutBucketAcl", map[string]any{"Bucket": bucket["Bucket"], "ACL": "public-read"}, nil)
	if grants := asSliceForTest(mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output["Grants"]); len(grants) != 2 || asMapForTest(asMapForTest(grants[1])["Grantee"])["URI"] != "http://acs.amazonaws.com/groups/global/AllUsers" {
		t.Fatalf("public bucket ACL = %#v", grants)
	}
	mustInvoke(t, p, "PutBucketAcl", map[string]any{"Bucket": bucket["Bucket"], "GrantRead": `uri="http://acs.amazonaws.com/groups/s3/LogDelivery"`}, nil)
	if grants := asSliceForTest(mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output["Grants"]); len(grants) != 1 || asMapForTest(grants[0])["Permission"] != "READ" {
		t.Fatalf("grant-header bucket ACL = %#v", grants)
	}
	validPolicy := map[string]any{
		"Owner":  map[string]any{"ID": ident().Account},
		"Grants": []any{map[string]any{"Grantee": map[string]any{"Type": "CanonicalUser", "ID": ident().Account}, "Permission": "FULL_CONTROL"}},
	}
	mustInvoke(t, p, "PutBucketAcl", map[string]any{"Bucket": bucket["Bucket"], "AccessControlPolicy": validPolicy}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "object-acl"}, []byte("body"))
	for _, put := range []map[string]any{
		{"ACL": "public-read"},
		{"GrantRead": `uri="http://acs.amazonaws.com/groups/s3/LogDelivery"`},
		{"AccessControlPolicy": validPolicy},
		{"GrantRead": `id="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"`},
	} {
		put["Bucket"], put["Key"] = bucket["Bucket"], "object-acl"
		mustInvoke(t, p, "PutObjectAcl", put, nil)
	}
	if grants := asSliceForTest(mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "object-acl"}, nil).Output["Grants"]); len(grants) != 1 || asMapForTest(asMapForTest(grants[0])["Grantee"])["ID"] != "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" {
		t.Fatalf("object ACL replacement = %#v", grants)
	}
	invalid := []struct {
		name, code string
		input      map[string]any
	}{
		{"invalid canned ACL", "InvalidArgument", map[string]any{"ACL": "fake-acl"}},
		{"invalid grant key", "InvalidArgument", map[string]any{"GrantWrite": `fakekey="1234"`}},
		{"invalid grant URI", "InvalidArgument", map[string]any{"GrantWrite": `uri="http://acs.amazonaws.com/groups/s3/FakeGroup"`}},
		{"invalid grant ID", "InvalidArgument", map[string]any{"GrantWrite": `id="wrong-id"`}},
		{"empty policy", "MalformedACLError", map[string]any{"AccessControlPolicy": map[string]any{}}},
		{"missing policy owner", "MalformedACLError", map[string]any{"AccessControlPolicy": map[string]any{"Grants": validPolicy["Grants"]}}},
		{"invalid policy permission", "MalformedACLError", map[string]any{"AccessControlPolicy": map[string]any{"Owner": validPolicy["Owner"], "Grants": []any{map[string]any{"Grantee": map[string]any{"Type": "CanonicalUser", "ID": ident().Account}, "Permission": "INVALID"}}}}},
		{"invalid policy owner", "InvalidArgument", map[string]any{"AccessControlPolicy": map[string]any{"Owner": map[string]any{"ID": "wrong-id"}, "Grants": validPolicy["Grants"]}}},
		{"invalid policy group", "InvalidArgument", map[string]any{"AccessControlPolicy": map[string]any{"Owner": validPolicy["Owner"], "Grants": []any{map[string]any{"Grantee": map[string]any{"Type": "Group", "URI": "http://acs.amazonaws.com/groups/s3/FakeGroup"}, "Permission": "READ"}}}}},
		{"invalid policy grantee type", "MalformedACLError", map[string]any{"AccessControlPolicy": map[string]any{"Owner": validPolicy["Owner"], "Grants": []any{map[string]any{"Grantee": map[string]any{"Type": "BadType"}, "Permission": "READ"}}}}},
		{"missing ACL", "MissingSecurityHeader", map[string]any{}},
		{"canned and grant", "InvalidRequest", map[string]any{"ACL": "private", "GrantRead": `uri="http://acs.amazonaws.com/groups/s3/LogDelivery"`}},
		{"canned and policy", "UnexpectedContent", map[string]any{"ACL": "private", "AccessControlPolicy": validPolicy}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			test.input["Bucket"] = bucket["Bucket"]
			_, err := invoke(t, p, "PutBucketAcl", test.input, nil)
			if fault := asFault(t, err); fault.Code != test.code {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}

	if _, err := invoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "rejected", "ACL": "fake-acl"}, []byte("body")); asFault(t, err).Code != "InvalidArgument" {
		t.Fatalf("invalid object canned ACL = %v", err)
	}
	if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "rejected"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("invalid ACL created object = %v", err)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket["Bucket"], "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned", "ACL": "public-read"}, []byte("one"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned"}, []byte("two"))
	current := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned"}, nil).Output
	old := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "versioned", "VersionId": firstVersion}, nil).Output
	if len(asSliceForTest(current["Grants"])) != 1 || len(asSliceForTest(old["Grants"])) != 2 {
		t.Fatalf("versioned ACLs current=%#v old=%#v", current, old)
	}
	for _, test := range []struct {
		key, acl string
		grants   int
	}{{"multipart-private", "", 1}, {"multipart-public", "public-read-write", 3}} {
		input := map[string]any{"Bucket": bucket["Bucket"], "Key": test.key}
		if test.acl != "" {
			input["ACL"] = test.acl
		}
		created := mustInvoke(t, p, "CreateMultipartUpload", input, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": bucket["Bucket"], "Key": test.key, "UploadId": uploadID, "PartNumber": 1}, []byte("part"))
		complete := completeInput(uploadID, map[string]any{"PartNumber": 1, "ETag": part.Headers.Get("ETag")})
		complete["Bucket"], complete["Key"] = bucket["Bucket"], test.key
		mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
		acl := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": test.key}, nil).Output
		if grants := len(asSliceForTest(acl["Grants"])); grants != test.grants {
			t.Fatalf("%s ACL grants = %d, want %d: %#v", test.key, grants, test.grants, acl)
		}
	}
}

func TestOwnerDisplayNamesMatchCurrentLocalStack(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "owner-display-name"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)

	listedOwner := asMapForTest(mustInvoke(t, p, "ListBuckets", nil, nil).Output["Owner"])
	defaultACL := mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output
	defaultOwner := asMapForTest(defaultACL["Owner"])
	defaultGrantee := asMapForTest(asMapForTest(asSliceForTest(defaultACL["Grants"])[0])["Grantee"])
	if listedOwner["DisplayName"] != nil || defaultOwner["DisplayName"] != nil || defaultGrantee["DisplayName"] != nil {
		t.Fatalf("deprecated owner display names: list=%#v owner=%#v grantee=%#v", listedOwner, defaultOwner, defaultGrantee)
	}

	id := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	mustInvoke(t, p, "PutBucketAcl", map[string]any{"Bucket": bucket["Bucket"], "GrantRead": `id="` + id + `"`}, nil)
	grant := asMapForTest(asMapForTest(asSliceForTest(mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output["Grants"])[0])["Grantee"])
	if grant["ID"] != id || grant["DisplayName"] != nil {
		t.Fatalf("canonical grant = %#v", grant)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "private", "ACL": "private"}, []byte("body"))
	privateACL := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "private"}, nil).Output
	privateOwner := asMapForTest(privateACL["Owner"])
	privateGrantee := asMapForTest(asMapForTest(asSliceForTest(privateACL["Grants"])[0])["Grantee"])
	if privateOwner["DisplayName"] != nil || privateGrantee["DisplayName"] != nil {
		t.Fatalf("private object display names: owner=%#v grantee=%#v", privateOwner, privateGrantee)
	}

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": bucket["Bucket"], "Key": "multipart"}, nil)
	parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": bucket["Bucket"], "Key": "multipart", "UploadId": created.Output["UploadId"]}, nil).Output
	if asMapForTest(parts["Initiator"])["DisplayName"] != "webfile" || asMapForTest(parts["Owner"])["DisplayName"] != nil {
		t.Fatalf("multipart identities = %#v", parts)
	}
}

func TestACLCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "acl-characterization"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	before := mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output
	mustInvoke(t, p, "PutBucketAcl", map[string]any{"Bucket": bucket["Bucket"], "ACL": "public-read"}, nil)
	after := mustInvoke(t, p, "GetBucketAcl", bucket, nil).Output
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "object", "ACL": "authenticated-read"}, []byte("body"))
	object := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "object"}, nil).Output
	_, invalidErr := invoke(t, p, "PutObjectAcl", map[string]any{"Bucket": bucket["Bucket"], "Key": "object", "GrantRead": `uri="invalid"`}, nil)
	golden.AssertJSON(t, map[string]any{"bucketDefault": before, "bucketPublic": after, "invalid": asFault(t, invalidErr).Code, "object": object})
}

func TestObjectACLDeleteMarkerCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	base := map[string]any{"Bucket": "acl-delete-marker", "Key": "versioned"}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": base["Bucket"]}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": base["Bucket"], "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutObject", base, []byte("body"))
	marker := mustInvoke(t, p, "DeleteObject", base, nil).Headers.Get("x-amz-version-id")
	characterization := map[string]any{}
	for _, test := range []struct {
		name, operation, version, code string
		status                         int
		method                         string
	}{
		{"put current", "PutObjectAcl", "", "MethodNotAllowed", http.StatusMethodNotAllowed, "PUT"},
		{"get current", "GetObjectAcl", "", "NoSuchKey", http.StatusNotFound, ""},
		{"put explicit", "PutObjectAcl", marker, "MethodNotAllowed", http.StatusMethodNotAllowed, "PUT"},
		{"get explicit", "GetObjectAcl", marker, "MethodNotAllowed", http.StatusMethodNotAllowed, "GET"},
	} {
		input := maps.Clone(base)
		if test.version != "" {
			input["VersionId"] = test.version
		}
		if test.operation == "PutObjectAcl" {
			input["ACL"] = "public-read"
		}
		_, err := invoke(t, p, test.operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != test.code || fault.HTTPStatus != test.status || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != marker {
			t.Fatalf("%s fault = %#v", test.name, fault)
		}
		if test.method == "" {
			if fault.Fields["Key"] != base["Key"] {
				t.Fatalf("%s fields = %#v", test.name, fault.Fields)
			}
		} else if fault.Message != "The specified method is not allowed against this resource." || fault.Fields["Method"] != test.method || fault.Fields["ResourceType"] != "DeleteMarker" {
			t.Fatalf("%s fields = %#v message=%q", test.name, fault.Fields, fault.Message)
		}
		characterization[test.name] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus, "message": fault.Message, "fields": fault.Fields, "headers": fault.Headers}
	}
	golden.AssertJSON(t, characterization)
}

func TestBucketPolicyCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "policy-characterization"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, missingErr := invoke(t, p, "GetBucketPolicy", input, nil)
	policy := `{"Version":"2012-10-17", "Statement":[{"Effect":"Allow","Principal":"*"}]}`
	put := mustInvoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": input["Bucket"], "Policy": policy}, nil)
	configured := mustInvoke(t, p, "GetBucketPolicy", input, nil)
	_, invalidErr := invoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": input["Bucket"], "Policy": `{}`}, nil)
	preserved := mustInvoke(t, p, "GetBucketPolicy", input, nil)
	deleted := mustInvoke(t, p, "DeleteBucketPolicy", input, nil)
	_, finalErr := invoke(t, p, "GetBucketPolicy", input, nil)
	missing, invalid, final := asFault(t, missingErr), asFault(t, invalidErr), asFault(t, finalErr)
	golden.AssertJSON(t, map[string]any{
		"configured": configured.Output,
		"deleted":    deleted.Status,
		"final":      map[string]any{"code": final.Code, "bucket": final.Fields["BucketName"]},
		"invalid":    map[string]any{"code": invalid.Code, "message": invalid.Message},
		"missing":    map[string]any{"code": missing.Code, "bucket": missing.Fields["BucketName"]},
		"preserved":  preserved.Output,
		"put":        put.Status,
	})
}

func TestBucketEncryptionCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "encryption-characterization"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	before := mustInvoke(t, p, "GetBucketEncryption", bucket, nil)
	keyID := "arn:aws:kms:us-east-1:000000000000:key/characterization"
	rules := []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": keyID}, "BucketKeyEnabled": true}}
	put := mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": map[string]any{"Rules": rules}}, nil)
	configured := mustInvoke(t, p, "GetBucketEncryption", bucket, nil)
	_, invalidErr := invoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256", "KMSMasterKeyID": keyID}}}}}, nil)
	preserved := mustInvoke(t, p, "GetBucketEncryption", bucket, nil)
	object := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "object"}, []byte("body"))
	deleted := mustInvoke(t, p, "DeleteBucketEncryption", bucket, nil)
	after := mustInvoke(t, p, "GetBucketEncryption", bucket, nil)
	invalid := asFault(t, invalidErr)
	golden.AssertJSON(t, map[string]any{
		"afterDelete": after.Output,
		"before":      before.Output,
		"configured":  configured.Output,
		"deleted":     deleted.Status,
		"inherited":   map[string]any{"algorithm": object.Headers.Get("x-amz-server-side-encryption"), "bucketKey": object.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled"), "key": object.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id")},
		"invalid":     map[string]any{"argument": invalid.Fields["ArgumentName"], "code": invalid.Code, "message": invalid.Message},
		"preserved":   preserved.Output,
		"put":         put.Status,
	})
}

func TestBucketLifecycleCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "lifecycle-characterization"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, beforeErr := invoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	before := asFault(t, beforeErr)
	rules := []any{map[string]any{"ID": "expire", "Filter": map[string]any{"Prefix": "logs/"}, "Status": "Enabled", "Expiration": map[string]any{"Days": 2}}}
	put := mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": input["Bucket"], "TransitionDefaultMinimumObjectSize": "varies_by_storage_class", "LifecycleConfiguration": map[string]any{"Rules": rules}}, nil)
	configured := mustInvoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	object := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": input["Bucket"], "Key": "logs/app.log"}, []byte("entry"))
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": input["Bucket"], "Key": "logs/app.log"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": input["Bucket"], "Key": "logs/multipart.log"}, nil).Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": input["Bucket"], "Key": "logs/multipart.log", "UploadId": uploadID, "PartNumber": 1}, []byte("entry"))
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	_, invalidErr := invoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": input["Bucket"], "LifecycleConfiguration": map[string]any{"Rules": []any{map[string]any{"ID": "invalid", "Filter": map[string]any{"Prefix": "a", "Tag": map[string]any{"Key": "k", "Value": "v"}}, "Status": "Enabled"}}}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	deleted := mustInvoke(t, p, "DeleteBucketLifecycle", input, nil)
	_, finalErr := invoke(t, p, "GetBucketLifecycleConfiguration", input, nil)
	final := asFault(t, finalErr)
	golden.AssertJSON(t, map[string]any{
		"default": map[string]any{"code": before.Code, "message": before.Message, "status": before.HTTPStatus, "bucket": before.Fields["BucketName"]},
		"put":     map[string]any{"status": put.Status, "transitionMinimum": put.Headers.Get("x-amz-transition-default-minimum-object-size")},
		"get":     map[string]any{"output": configured.Output, "transitionMinimum": configured.Headers.Get("x-amz-transition-default-minimum-object-size")},
		"object":  map[string]any{"putExpiration": object.Headers.Get("x-amz-expiration"), "headExpiration": head.Headers.Get("x-amz-expiration"), "completeExpiration": completed.Headers.Get("x-amz-expiration")},
		"invalid": map[string]any{"code": invalid.Code, "status": invalid.HTTPStatus}, "preserved": preserved.Output,
		"delete":  map[string]any{"status": deleted.Status},
		"deleted": map[string]any{"code": final.Code, "message": final.Message, "status": final.HTTPStatus, "bucket": final.Fields["BucketName"]},
	})
}

func TestBucketCorsCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "cors-characterization"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, beforeErr := invoke(t, p, "GetBucketCors", input, nil)
	before := asFault(t, beforeErr)
	rules := []any{map[string]any{"AllowedMethods": []any{"GET", "PUT"}, "AllowedOrigins": []any{"https://*.example.test"}, "AllowedHeaders": []any{"x-amz-*"}, "ExposeHeaders": []any{"ETag"}, "MaxAgeSeconds": float64(300), "ID": "read"}}
	put := mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": input["Bucket"], "CORSConfiguration": map[string]any{"CORSRules": rules}}, nil)
	after := mustInvoke(t, p, "GetBucketCors", input, nil)
	preflightRequest := httptest.NewRequest(http.MethodOptions, "https://cors-characterization.s3.us-east-1.amazonaws.com/key", nil)
	preflightRequest.Header.Set("Origin", "https://app.example.test")
	preflightRequest.Header.Set("Access-Control-Request-Method", "GET")
	preflightRequest.Header.Set("Access-Control-Request-Headers", "x-amz-request-payer,x-amz-meta-team")
	preflight, preflightErr := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: preflightRequest})
	if preflightErr != nil {
		t.Fatal(preflightErr)
	}
	preflightRequest.Header.Set("Origin", "https://wrong.test")
	_, rejectedErr := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: preflightRequest})
	rejected := asFault(t, rejectedErr)
	_, invalidErr := invoke(t, p, "PutBucketCors", map[string]any{"Bucket": input["Bucket"], "CORSRules": []any{map[string]any{"AllowedMethods": []any{"OPTIONS"}, "AllowedOrigins": []any{"*"}}}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketCors", input, nil)
	deleted := mustInvoke(t, p, "DeleteBucketCors", input, nil)
	_, finalErr := invoke(t, p, "GetBucketCors", input, nil)
	final := asFault(t, finalErr)
	defaultRequest := httptest.NewRequest(http.MethodOptions, "https://cors-characterization.s3.us-east-1.amazonaws.com/key", nil)
	defaultRequest.Header.Set("Origin", "https://app.localstack.cloud")
	defaultRequest.Header.Set("Access-Control-Request-Method", "GET")
	localstackDefault, defaultErr := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: defaultRequest})
	if defaultErr != nil {
		t.Fatal(defaultErr)
	}
	golden.AssertJSON(t, map[string]any{
		"default": map[string]any{"code": before.Code, "status": before.HTTPStatus, "bucket": before.Fields["BucketName"]},
		"put":     put.Output, "get": after.Output,
		"preflight":         map[string]any{"status": preflight.Status, "headers": preflight.Headers},
		"rejected":          map[string]any{"code": rejected.Code, "message": rejected.Message, "method": rejected.Fields["Method"], "resourceType": rejected.Fields["ResourceType"], "status": rejected.HTTPStatus},
		"localstackDefault": map[string]any{"status": localstackDefault.Status, "headers": localstackDefault.Headers},
		"invalid":           map[string]any{"code": invalid.Code, "message": invalid.Message, "status": invalid.HTTPStatus},
		"preserved":         preserved.Output, "delete": deleted.Output,
		"deleted": map[string]any{"code": final.Code, "status": final.HTTPStatus, "bucket": final.Fields["BucketName"]},
	})
}

func TestBucketWebsiteCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "website-characterization"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	_, beforeErr := invoke(t, p, "GetBucketWebsite", input, nil)
	before := asFault(t, beforeErr)
	website := map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "ErrorDocument": map[string]any{"Key": "error.html"}}
	put := mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": input["Bucket"], "WebsiteConfiguration": website}, nil)
	after := mustInvoke(t, p, "GetBucketWebsite", input, nil)
	_, invalidErr := invoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": input["Bucket"], "WebsiteConfiguration": map[string]any{"IndexDocument": map[string]any{"Suffix": "dir/index.html"}}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketWebsite", input, nil)
	deleted := mustInvoke(t, p, "DeleteBucketWebsite", input, nil)
	_, finalErr := invoke(t, p, "GetBucketWebsite", input, nil)
	final := asFault(t, finalErr)
	golden.AssertJSON(t, map[string]any{
		"default": map[string]any{"code": before.Code, "message": before.Message, "status": before.HTTPStatus, "bucket": before.Fields["BucketName"]},
		"put":     put.Output, "get": after.Output,
		"invalid":   map[string]any{"code": invalid.Code, "message": invalid.Message, "status": invalid.HTTPStatus, "argument": invalid.Fields["ArgumentName"], "value": invalid.Fields["ArgumentValue"]},
		"preserved": preserved.Output, "delete": deleted.Output,
		"deleted": map[string]any{"code": final.Code, "message": final.Message, "status": final.HTTPStatus, "bucket": final.Fields["BucketName"]},
	})
}

func TestBucketNotificationConfigurationCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	input := map[string]any{"Bucket": "notification-characterization"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("queues").Put(context.Background(), "queue", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	before := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil)
	configuration := map[string]any{"QueueConfigurations": []any{map[string]any{"Id": "images", "QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{map[string]any{"Name": "prefix", "Value": "images/"}}}}}}}
	put := mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": configuration}, nil)
	after := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": input["Bucket"], "Key": "images/a+b c.jpg"}, []byte("photo"))
	messages, _, err := deps.Store.Scope(ident().Account, ident().Region).Collection("msgs:queue").List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	delivery := map[string]any{}
	for _, stored := range messages {
		var message map[string]any
		if err := json.Unmarshal(stored.Value, &message); err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &payload); err != nil {
			t.Fatal(err)
		}
		if payload["Event"] == "s3:TestEvent" {
			delivery["test"] = payload
		} else {
			delivery["object"] = payload
		}
	}
	_, invalidErr := invoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:missing", "Events": []any{"s3:ObjectCreated:*"}}}}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil)
	cleared := mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": map[string]any{}}, nil)
	final := mustInvoke(t, p, "GetBucketNotificationConfiguration", input, nil)
	golden.AssertJSON(t, map[string]any{
		"default": before.Output, "put": put.Output, "get": after.Output, "delivery": delivery,
		"invalid":   map[string]any{"code": invalid.Code, "message": invalid.Message, "status": invalid.HTTPStatus, "argument": invalid.Fields["ArgumentName"], "value": invalid.Fields["ArgumentValue"]},
		"preserved": preserved.Output, "clear": cleared.Output, "cleared": final.Output,
	})
}

func TestBucketLoggingCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	for _, bucket := range []string{"logging-characterization-source", "logging-characterization-target"} {
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	}
	input := map[string]any{"Bucket": "logging-characterization-source"}
	before := mustInvoke(t, p, "GetBucketLogging", input, nil)
	put := mustInvoke(t, p, "PutBucketLogging", map[string]any{"Bucket": input["Bucket"], "BucketLoggingStatus": map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "logging-characterization-target", "TargetPrefix": "logs/"}}}, nil)
	after := mustInvoke(t, p, "GetBucketLogging", input, nil)
	_, invalidErr := invoke(t, p, "PutBucketLogging", map[string]any{"Bucket": input["Bucket"], "BucketLoggingStatus": map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "missing"}}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketLogging", input, nil)
	disabled := mustInvoke(t, p, "PutBucketLogging", map[string]any{"Bucket": input["Bucket"], "BucketLoggingStatus": map[string]any{}}, nil)
	final := mustInvoke(t, p, "GetBucketLogging", input, nil)
	golden.AssertJSON(t, map[string]any{
		"default": before.Output, "put": put.Output, "get": after.Output,
		"invalid":   map[string]any{"code": invalid.Code, "message": invalid.Message, "status": invalid.HTTPStatus, "target": invalid.Fields["TargetBucket"]},
		"preserved": preserved.Output, "disable": disabled.Output, "disabled": final.Output,
	})
}

func TestBucketAccelerateConfigurationCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "accelerate-characterization"}, nil)
	before := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate-characterization"}, nil)
	put := mustInvoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate-characterization", "AccelerateConfiguration": map[string]any{"Status": "Enabled"}}, nil)
	after := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate-characterization"}, nil)
	_, invalidErr := invoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate-characterization", "AccelerateConfiguration": map[string]any{"Status": "Invalid"}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": "accelerate-characterization"}, nil)
	golden.AssertJSON(t, map[string]any{
		"default": before.Output, "put": put.Output, "get": after.Output,
		"invalid": map[string]any{"code": invalid.Code, "status": invalid.HTTPStatus}, "preserved": preserved.Output,
	})
}

func TestBucketRequestPaymentCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "request-payment-characterization"}, nil)
	before := mustInvoke(t, p, "GetBucketRequestPayment", map[string]any{"Bucket": "request-payment-characterization"}, nil)
	put := mustInvoke(t, p, "PutBucketRequestPayment", map[string]any{"Bucket": "request-payment-characterization", "RequestPaymentConfiguration": map[string]any{"Payer": "Requester"}}, nil)
	after := mustInvoke(t, p, "GetBucketRequestPayment", map[string]any{"Bucket": "request-payment-characterization"}, nil)
	_, invalidErr := invoke(t, p, "PutBucketRequestPayment", map[string]any{"Bucket": "request-payment-characterization", "RequestPaymentConfiguration": map[string]any{"Payer": "Invalid"}}, nil)
	invalid := asFault(t, invalidErr)
	preserved := mustInvoke(t, p, "GetBucketRequestPayment", map[string]any{"Bucket": "request-payment-characterization"}, nil)
	golden.AssertJSON(t, map[string]any{
		"default": before.Output, "put": put.Output, "get": after.Output,
		"invalid": map[string]any{"code": invalid.Code, "status": invalid.HTTPStatus}, "preserved": preserved.Output,
	})
}

func TestPublicAccessBlockCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "public-access-block-characterization"}, nil)
	put := mustInvoke(t, p, "PutPublicAccessBlock", map[string]any{"Bucket": "public-access-block-characterization", "PublicAccessBlockConfiguration": map[string]any{"IgnorePublicAcls": true}}, nil)
	get := mustInvoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block-characterization"}, nil)
	_, invalidErr := invoke(t, p, "PutPublicAccessBlock", map[string]any{"Bucket": "public-access-block-characterization", "PublicAccessBlockConfiguration": map[string]any{"Unknown": true}}, nil)
	invalid := asFault(t, invalidErr)
	deleted := mustInvoke(t, p, "DeletePublicAccessBlock", map[string]any{"Bucket": "public-access-block-characterization"}, nil)
	_, missingErr := invoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block-characterization"}, nil)
	missing := asFault(t, missingErr)
	golden.AssertJSON(t, map[string]any{
		"put": put.Output, "get": get.Output,
		"invalid": map[string]any{"code": invalid.Code, "status": invalid.HTTPStatus},
		"delete":  deleted.Status,
		"missing": map[string]any{"code": missing.Code, "status": missing.HTTPStatus},
	})
}

func TestBucketOwnershipControlsCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "ownership-characterization"}, nil)
	controls := map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "ObjectWriter"}}}
	put := mustInvoke(t, p, "PutBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization", "OwnershipControls": controls}, nil)
	get := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization"}, nil)
	_, invalidErr := invoke(t, p, "PutBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization", "OwnershipControls": map[string]any{"Rules": []any{}}}, nil)
	invalid := asFault(t, invalidErr)
	firstDelete := mustInvoke(t, p, "DeleteBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization"}, nil)
	secondDelete := mustInvoke(t, p, "DeleteBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization"}, nil)
	_, missingErr := invoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-characterization"}, nil)
	missing := asFault(t, missingErr)
	golden.AssertJSON(t, map[string]any{
		"put":     map[string]any{"status": put.Status, "output": put.Output},
		"get":     get.Output,
		"invalid": map[string]any{"code": invalid.Code, "status": invalid.HTTPStatus},
		"delete":  []any{firstDelete.Status, secondDelete.Status},
		"missing": map[string]any{"code": missing.Code, "message": missing.Message, "status": missing.HTTPStatus},
	})
}

func TestCreateBucketAccountRegionalNamespace(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	east := ident()
	characterization := map[string]any{}
	name := "team-" + east.Account + "-" + east.Region + "-an"
	input := map[string]any{"Bucket": name, "BucketNamespace": "account-regional"}
	if got, err := invokeAs(t, p, east, "CreateBucket", input, nil); err != nil || got.Headers.Get("Location") != "/"+name || got.Output["BucketArn"] != "arn:aws:s3:::"+name {
		t.Fatalf("create = %#v %v", got, err)
	} else {
		characterization["east-location"] = got.Headers.Get("Location")
	}
	_, err := invokeAs(t, p, east, "CreateBucket", input, nil)
	fault := asFault(t, err)
	if fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict || fault.Fields["BucketName"] != name {
		t.Fatalf("recreate = %#v", fault)
	}
	characterization["recreate"] = fault.Code
	mustInvokeAs(t, p, east, "PutObject", map[string]any{"Bucket": name, "Key": "key"}, []byte("value"))

	other := spi.Identity{Account: "999999999999", Region: east.Region}
	_, err = invokeAs(t, p, other, "CreateBucket", input, nil)
	fault = asFault(t, err)
	if fault.Code != "InvalidBucketName" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["BucketName"] != name {
		t.Fatalf("foreign suffix = %#v", fault)
	}
	characterization["foreign-suffix"] = fault.Code
	if _, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": name, "BucketNamespace": "global"}, nil); asFault(t, err).Code != "InvalidBucketName" {
		t.Fatalf("global -an name = %v", err)
	} else {
		characterization["global-an"] = asFault(t, err).Code
	}
	_, err = invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "bucket", "BucketNamespace": "regional"}, nil)
	fault = asFault(t, err)
	if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "x-amz-bucket-namespace" || fault.Fields["ArgumentValue"] != "regional" {
		t.Fatalf("unknown namespace = %#v", fault)
	}
	characterization["unknown"] = fault.Code
	if _, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "explicit-global", "BucketNamespace": "global"}, nil); err != nil {
		t.Fatalf("explicit global namespace: %v", err)
	}
	for _, region := range []string{"me-central-1", "me-south-1"} {
		id := spi.Identity{Account: east.Account, Region: region}
		bucket := "team-" + id.Account + "-" + region + "-an"
		_, err := invokeAs(t, p, id, "CreateBucket", map[string]any{"Bucket": bucket, "BucketNamespace": "account-regional", "LocationConstraint": region}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("unsupported region %s = %#v", region, fault)
		}
	}

	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	westName := "team-" + west.Account + "-" + west.Region + "-an"
	westInput := map[string]any{"Bucket": westName, "BucketNamespace": "account-regional", "LocationConstraint": west.Region}
	if got, err := invokeAs(t, p, west, "CreateBucket", westInput, nil); err != nil || got.Headers.Get("Location") != "/"+westName || got.Output["BucketArn"] != "arn:aws:s3:::"+westName {
		t.Fatalf("west create = %#v %v", got, err)
	} else {
		characterization["west-location"] = got.Headers.Get("Location")
	}
	if got := mustInvokeAs(t, p, west, "GetBucketLocation", map[string]any{"Bucket": westName}, nil); got.Output["LocationConstraint"] != west.Region {
		t.Fatalf("west location = %#v", got.Output)
	} else {
		characterization["west-constraint"] = got.Output["LocationConstraint"]
	}
	golden.AssertJSON(t, characterization)
}

func TestListBucketsPaginationAndFilters(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	east := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	other := spi.Identity{Account: "999999999999", Region: east.Region}
	create := func(id spi.Identity, name string) {
		t.Helper()
		input := map[string]any{"Bucket": name}
		if id.Region != "us-east-1" {
			input["LocationConstraint"] = id.Region
		}
		created := mustInvokeAs(t, p, id, "CreateBucket", input, nil)
		if created.Output["BucketArn"] != "arn:aws:s3:::"+name {
			t.Fatalf("create bucket ARN = %#v", created.Output)
		}
		if err := deps.Clock.Advance(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	create(east, "alpha-bucket")
	create(east, "team-alpha")
	create(west, "team-beta")
	create(west, "team-charlie")
	create(other, "team-private")
	stringValue := func(value any) string { text, _ := value.(string); return text }
	names := func(response *spi.Response) []string {
		t.Helper()
		var got []string
		for _, item := range response.Output["Buckets"].([]any) {
			got = append(got, stringValue(asMapForTest(item)["Name"]))
		}
		return got
	}

	all := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{}, nil)
	if got := strings.Join(names(all), ","); got != "alpha-bucket,team-alpha,team-beta,team-charlie" {
		t.Fatalf("all buckets = %s", got)
	}
	firstCreated := stringValue(asMapForTest(all.Output["Buckets"].([]any)[0])["CreationDate"])
	if firstCreated == "" || asMapForTest(all.Output["Buckets"].([]any)[0])["BucketRegion"] != nil || asMapForTest(all.Output["Buckets"].([]any)[0])["BucketArn"] != "arn:aws:s3:::alpha-bucket" {
		t.Fatalf("unpaginated bucket = %#v", all.Output["Buckets"].([]any)[0])
	}
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	listedAgain := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{}, nil)
	if created := stringValue(asMapForTest(listedAgain.Output["Buckets"].([]any)[0])["CreationDate"]); created != firstCreated {
		t.Fatalf("creation date changed from %q to %q", firstCreated, created)
	}

	page := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{"MaxBuckets": 2, "Prefix": "team-"}, nil)
	if got := strings.Join(names(page), ","); got != "team-alpha,team-beta" || page.Output["Prefix"] != "team-" {
		t.Fatalf("first page = %#v", page.Output)
	}
	for _, item := range page.Output["Buckets"].([]any) {
		if asMapForTest(item)["BucketRegion"] == "" {
			t.Fatalf("paginated bucket = %#v", item)
		}
	}
	token := stringValue(page.Output["ContinuationToken"])
	if token == "" || token == "team-beta" {
		t.Fatalf("continuation token = %q", token)
	}
	last := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{"MaxBuckets": 2, "Prefix": "team-", "ContinuationToken": token}, nil)
	if got := strings.Join(names(last), ","); got != "team-charlie" || last.Output["ContinuationToken"] != nil {
		t.Fatalf("last page = %#v", last.Output)
	}
	regional := mustInvokeAs(t, p, west, "ListBuckets", map[string]any{"BucketRegion": west.Region, "Prefix": "team-"}, nil)
	if got := strings.Join(names(regional), ","); got != "team-beta,team-charlie" {
		t.Fatalf("regional buckets = %#v", regional.Output)
	}

	for _, input := range []map[string]any{{"MaxBuckets": 0}, {"MaxBuckets": 10001}, {"MaxBuckets": "invalid"}, {"ContinuationToken": "!"}, {"ContinuationToken": strings.Repeat("a", 1025)}} {
		_, err := invokeAs(t, p, east, "ListBuckets", input, nil)
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid input %#v = %#v", input, fault)
		}
	}
	golden.AssertJSON(t, map[string]any{"all": all.Output, "page": page.Output, "last": last.Output, "regional": regional.Output})
}

func TestCreateBucketLocationConstraints(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	east := ident()
	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	characterization := map[string]any{}

	if got := mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "east-location"}, nil).Headers.Get("Location"); got != "/east-location" {
		t.Fatalf("default create location = %q", got)
	}
	if got := mustInvokeAs(t, p, east, "GetBucketLocation", map[string]any{"Bucket": "east-location"}, nil); got.Output["LocationConstraint"] != "" {
		t.Fatalf("default location = %#v", got.Output)
	} else {
		characterization["default"] = got.Output["LocationConstraint"]
	}

	for name, input := range map[string]map[string]any{
		"missing":  {"Bucket": "west-missing"},
		"mismatch": {"Bucket": "west-mismatch", "LocationConstraint": "eu-west-1"},
	} {
		_, err := invokeAs(t, p, west, "CreateBucket", input, nil)
		fault := asFault(t, err)
		if fault.Code != "IllegalLocationConstraintException" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s constraint = %#v", name, fault)
		}
		if _, err := invokeAs(t, p, west, "HeadBucket", map[string]any{"Bucket": input["Bucket"]}, nil); asFault(t, err).Code != "NoSuchBucket" {
			t.Fatalf("%s created bucket: %v", name, err)
		}
		characterization[name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	}

	if got := mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": "west-match", "CreateBucketConfiguration": map[string]any{"LocationConstraint": "us-west-2"}}, nil).Headers.Get("Location"); got != "http://west-match.s3.amazonaws.com/" {
		t.Fatalf("regional create location = %q", got)
	} else {
		characterization["matching-header"] = got
	}
	if got := mustInvokeAs(t, p, west, "GetBucketLocation", map[string]any{"Bucket": "west-match"}, nil); got.Output["LocationConstraint"] != "us-west-2" {
		t.Fatalf("west location = %#v", got.Output)
	} else {
		characterization["matching"] = got.Output["LocationConstraint"]
	}
	secure, secureErr := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CreateBucket", Identity: west, Input: map[string]any{"Bucket": "secure-location", "LocationConstraint": "us-west-2"}, HTTP: &http.Request{Method: http.MethodPut, URL: &url.URL{Path: "/secure-location"}, Host: "s3.test", TLS: &tls.ConnectionState{}}})
	if secureErr != nil || secure.Headers.Get("Location") != "https://s3.test/secure-location/" {
		t.Fatalf("secure create location = %#v %v", secure, secureErr)
	}
	characterization["secure-header"] = secure.Headers.Get("Location")

	_, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "invalid-location", "LocationConstraint": "moon-west-1"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidLocationConstraint" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["LocationConstraint"] != "moon-west-1" {
		t.Fatalf("invalid constraint = %#v", fault)
	} else {
		characterization["invalid"] = map[string]any{"code": fault.Code, "field": fault.Fields["LocationConstraint"], "message": fault.Message, "status": fault.HTTPStatus}
	}

	if got := mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "eu-alias", "LocationConstraint": "EU"}, nil).Headers.Get("Location"); got != "http://eu-alias.s3.amazonaws.com/" {
		t.Fatalf("EU create location = %q", got)
	} else {
		characterization["EU-header"] = got
	}
	if got := mustInvokeAs(t, p, east, "HeadBucket", map[string]any{"Bucket": "eu-alias"}, nil).Headers.Get("x-amz-bucket-region"); got != "eu-west-1" {
		t.Fatalf("EU bucket region = %q", got)
	} else {
		characterization["EU-region"] = got
	}
	europe := spi.Identity{Account: east.Account, Region: "eu-west-1"}
	if got := mustInvokeAs(t, p, europe, "GetBucketLocation", map[string]any{"Bucket": "eu-alias"}, nil); got.Output["LocationConstraint"] != "EU" {
		t.Fatalf("EU alias = %#v", got.Output)
	} else {
		characterization["EU"] = map[string]any{"reported": got.Output["LocationConstraint"], "stored_region": europe.Region}
	}
	golden.AssertJSON(t, characterization)
}

func TestCrossRegionBucketResolutionAndHeadMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	east := ident()
	mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "cross-region", "LocationConstraint": "us-west-2"}, nil)

	head := mustInvokeAs(t, p, east, "HeadBucket", map[string]any{"Bucket": "cross-region"}, nil)
	if head.Headers.Get("Content-Type") != "application/xml" || head.Headers.Get("x-amz-access-point-alias") != "false" || head.Headers.Get("x-amz-bucket-region") != "us-west-2" || head.Headers.Get("x-amz-bucket-arn") != "arn:aws:s3:::cross-region" {
		t.Fatalf("head headers = %#v", head.Headers)
	}
	mustInvokeAs(t, p, east, "PutObject", map[string]any{"Bucket": "cross-region", "Key": "key"}, []byte("body"))
	listed := mustInvokeAs(t, p, east, "ListObjectsV2", map[string]any{"Bucket": "cross-region"}, nil)
	if listed.Headers.Get("x-amz-bucket-region") != "us-west-2" || len(asSliceForTest(listed.Output["Contents"])) != 1 {
		t.Fatalf("list = %#v headers=%#v", listed.Output, listed.Headers)
	}

	global := east
	global.Region = "aws-global"
	_, err := invokeAs(t, p, global, "HeadBucket", map[string]any{"Bucket": "cross-region"}, nil)
	fault := asFault(t, err)
	if fault.Code != "AuthorizationHeaderMalformed" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["Region"] != "us-east-1" {
		t.Fatalf("global head = %#v", fault)
	}
	foreign := east
	foreign.Account = "999999999999"
	if _, err := invokeAs(t, p, foreign, "HeadBucket", map[string]any{"Bucket": "cross-region"}, nil); asFault(t, err).Code != "NoSuchBucket" {
		t.Fatalf("foreign head = %v", err)
	}
	golden.AssertJSON(t, map[string]any{"head": head.Headers, "list": listed.Headers, "global": map[string]any{"code": fault.Code, "region": fault.Fields["Region"], "status": fault.HTTPStatus}})
}

func TestCreateBucketValidatesGlobalNames(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{"invalid": map[string]any{}, "valid": map[string]any{}}
	invalid := []string{"", "ab", strings.Repeat("a", 64), "Uppercase", "under_score", "-starts", "ends-", "adjacent..dots", "192.168.5.4", "999.999.999.999", "192.168.005.4", "xn--reserved", "sthree-reserved", "amzn-s3-demo-reserved", "reserved-s3alias", "reserved--ol-s3", "reserved.mrap", "reserved--x-s3", "reserved--table-s3", "reserved-an"}
	for _, name := range invalid {
		_, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketName" || fault.Message != "The specified bucket is not valid." || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["BucketName"] != name {
			t.Fatalf("name %q = %#v", name, fault)
		}
		characterization["invalid"].(map[string]any)[name] = fault.Code
	}
	for _, name := range []string{"123", "abc", "bucket-name", "example.com", "abc.def.ghi.jkl", strings.Repeat("a", 63)} {
		response, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name}, nil)
		if err != nil {
			t.Fatalf("valid name %q: %v", name, err)
		}
		characterization["valid"].(map[string]any)[name] = response.Headers.Get("Location")
	}
	golden.AssertJSON(t, characterization)
}

func TestDeleteBucketRequiresEmptyBucket(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	input := map[string]any{"Bucket": "non-empty-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "object"}, []byte("body"))
	characterization := map[string]any{}

	_, err := invoke(t, p, "DeleteBucket", input, nil)
	fault := asFault(t, err)
	if fault.Code != "BucketNotEmpty" || fault.HTTPStatus != http.StatusConflict || fault.Message != "The bucket you tried to delete is not empty" || fault.Fields["BucketName"] != "non-empty-bucket" {
		t.Fatalf("unversioned delete = %#v", fault)
	}
	if got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "object"}, nil); string(readStream(t, got)) != "body" {
		t.Fatal("failed bucket deletion changed object")
	}
	characterization["unversioned"] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "preserved": true}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "non-empty-bucket", "Status": "Enabled"}, nil)
	_, err = invoke(t, p, "DeleteBucket", input, nil)
	fault = asFault(t, err)
	if fault.Message != "The bucket you tried to delete is not empty. You must delete all versions in the bucket." {
		t.Fatalf("versioned delete = %#v", fault)
	}
	characterization["versioned"] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "historical"}, []byte("version"))
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("objects").Delete(context.Background(), "non-empty-bucket/historical"); err != nil {
		t.Fatal(err)
	}
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("objects").Delete(context.Background(), "non-empty-bucket/object"); err != nil {
		t.Fatal(err)
	}
	_, err = invoke(t, p, "DeleteBucket", input, nil)
	if fault := asFault(t, err); fault.Code != "BucketNotEmpty" {
		t.Fatalf("historical-only version delete = %#v", fault)
	}
	characterization["historical-only-version"] = "BucketNotEmpty"
	golden.AssertJSON(t, characterization)
}

func TestDeleteBucketClearsBucketState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "recreated-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "recreated-bucket", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "recreated-bucket", "TagSet": []any{map[string]any{"Key": "old", "Value": "state"}}}, nil)
	mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": "recreated-bucket", "CORSRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}}, nil)
	mustInvoke(t, p, "PutBucketAnalyticsConfiguration", map[string]any{"Bucket": "recreated-bucket", "Id": "old", "AnalyticsConfiguration": map[string]any{"Id": "old"}}, nil)
	mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "recreated-bucket", "Key": "unfinished"}, nil)
	mustInvoke(t, p, "DeleteBucket", input, nil)
	if _, err := invoke(t, p, "HeadBucket", input, nil); asFault(t, err).Code != "NoSuchBucket" {
		t.Fatalf("deleted bucket remained registered: %v", err)
	}
	mustInvoke(t, p, "CreateBucket", input, nil)

	if got := mustInvoke(t, p, "GetBucketVersioning", input, nil); got.Output["Status"] == "Enabled" {
		t.Fatalf("recreated bucket inherited versioning: %#v", got.Output)
	}
	if _, err := invoke(t, p, "GetBucketTagging", input, nil); asFault(t, err).Code != "NoSuchTagSet" {
		t.Fatalf("recreated bucket inherited tags: %v", err)
	}
	if _, err := invoke(t, p, "GetBucketCors", input, nil); asFault(t, err).Code != "NoSuchCORSConfiguration" {
		t.Fatalf("recreated bucket inherited CORS: %v", err)
	}
	if got := mustInvoke(t, p, "ListBucketAnalyticsConfigurations", input, nil); len(asSliceForTest(got.Output["AnalyticsConfigurationList"])) != 0 {
		t.Fatalf("recreated bucket inherited named configuration: %#v", got.Output)
	}
	if got := mustInvoke(t, p, "ListMultipartUploads", input, nil); len(got.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("recreated bucket inherited multipart uploads: %#v", got.Output)
	}
}

func TestBucketVersioningState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "versioning-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	characterization := map[string]any{}
	if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output; len(got) != 0 {
		t.Fatalf("unset versioning = %#v", got)
	} else {
		characterization["unset"] = got
	}

	for _, test := range []struct {
		status  string
		code    string
		message string
	}{
		{"", "IllegalVersioningConfigurationException", "The Versioning element must be specified"},
		{"Invalid", "MalformedXML", ""},
	} {
		_, err := invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "versioning-bucket", "Status": test.status}, nil)
		fault := asFault(t, err)
		if fault.Code != test.code || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("status %q = %#v", test.status, fault)
		}
		characterization["rejected-"+test.code] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
		if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output; len(got) != 0 {
			t.Fatalf("status %q persisted: %#v", test.status, got)
		}
	}

	for _, status := range []string{"Enabled", "Suspended"} {
		if got := mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "versioning-bucket", "Status": status}, nil).Output; len(got) != 0 {
			t.Fatalf("put %s output = %#v", status, got)
		}
		if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output["Status"]; got != status {
			t.Fatalf("get %s = %v", status, got)
		} else {
			characterization[status] = got
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestObjectMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{
		"Bucket": "bucket", "Key": "source", "CacheControl": "max-age=60", "ContentDisposition": `attachment; filename="one.txt"`,
		"ContentEncoding": "gzip", "ContentLanguage": "en-US", "ContentType": "text/plain", "Expires": "Wed, 21 Oct 2026 07:28:00 GMT",
		"Metadata": map[string]any{"Owner": "mirror", "Empty": ""}, "WebsiteRedirectLocation": "/old",
	}, []byte("first"))
	assert := func(name string, response *spi.Response, contentType, owner string) {
		t.Helper()
		if response.Headers.Get("Content-Type") != contentType || response.Headers.Get("x-amz-meta-owner") != owner {
			t.Fatalf("%s metadata = %v", name, response.Headers)
		}
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("get", get, "text/plain", "mirror")
	if get.Headers.Get("Cache-Control") != "max-age=60" || get.Headers.Get("Content-Disposition") != `attachment; filename="one.txt"` || get.Headers.Get("Content-Encoding") != "gzip" || get.Headers.Get("Content-Language") != "en-US" || get.Headers.Get("Expires") != "Wed, 21 Oct 2026 07:28:00 GMT" || get.Headers.Get("x-amz-website-redirect-location") != "/old" {
		t.Fatalf("get system metadata = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("head", head, "text/plain", "mirror")

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source"}, nil)
	copied := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	assert("copied", copied, "text/plain", "mirror")
	if copied.Headers.Get("x-amz-website-redirect-location") != "" {
		t.Fatalf("copy inherited website redirect = %v", copied.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "redirected", "CopySource": "bucket/source", "WebsiteRedirectLocation": "/new"}, nil)
	if redirected := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "redirected"}, nil); redirected.Headers.Get("x-amz-website-redirect-location") != "/new" {
		t.Fatalf("explicit copy redirect = %v", redirected.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "replaced", "CopySource": "bucket/source", "MetadataDirective": "REPLACE",
		"ContentType": "application/json", "Metadata": map[string]any{"Owner": "new"},
	}, nil)
	replaced := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "replaced"}, nil)
	assert("replaced", replaced, "application/json", "new")
	if replaced.Headers.Get("Cache-Control") != "" || replaced.Headers.Get("Content-Encoding") != "" {
		t.Fatalf("replace inherited system metadata = %v", replaced.Headers)
	}

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "default"}, []byte("body"))
	defaultHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "default"}, nil)
	assert("default", defaultHead, "binary/octet-stream", "")
	golden.AssertJSON(t, map[string]any{
		"get":      map[string]any{"contentType": get.Headers.Get("Content-Type"), "cacheControl": get.Headers.Get("Cache-Control"), "owner": get.Headers.Get("x-amz-meta-owner"), "redirect": get.Headers.Get("x-amz-website-redirect-location")},
		"head":     map[string]any{"contentType": head.Headers.Get("Content-Type"), "owner": head.Headers.Get("x-amz-meta-owner")},
		"replaced": map[string]any{"contentType": replaced.Headers.Get("Content-Type"), "cacheControl": replaced.Headers.Get("Cache-Control"), "owner": replaced.Headers.Get("x-amz-meta-owner")},
		"default":  map[string]any{"contentType": defaultHead.Headers.Get("Content-Type")},
	})
}

func TestUserMetadataRFC2047Characterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "metadata-bucket"}, nil)
	safe := "! \"#$%&'()*+,-./0123456789:;<>'?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~\t"
	mustInvoke(t, p, "PutObject", map[string]any{
		"Bucket": "metadata-bucket", "Key": "source",
		"Metadata": map[string]any{
			"Non-ASCII":    "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=",
			"Binary":       "=?UTF-8?B?AAECAw==?=",
			"Fake-Encoded": "=?UTF-8?Q?actually-ascii?=",
			"ASCII-B64":    "=?UTF-8?B?YWJj?=",
			"Bad-B64":      "=?UTF-8?B?=GGG?=",
			"Bad-Q":        "=?UTF-8?Q?bad=4A=ZZ_value?=",
			"Raw-Unicode":  "ÄMÄZÕÑ S3",
			"Safe":         safe,
		},
	}, []byte("body"))

	read := func(operation, key string) map[string]any {
		t.Helper()
		response := mustInvoke(t, p, operation, map[string]any{"Bucket": "metadata-bucket", "Key": key}, nil)
		return map[string]any{
			"nonASCII":    response.Headers.Get("x-amz-meta-non-ascii"),
			"binary":      response.Headers.Get("x-amz-meta-binary"),
			"fakeEncoded": response.Headers.Get("x-amz-meta-fake-encoded"),
			"asciiB64":    response.Headers.Get("x-amz-meta-ascii-b64"),
			"badB64":      response.Headers.Get("x-amz-meta-bad-b64"),
			"badQ":        response.Headers.Get("x-amz-meta-bad-q"),
			"rawUnicode":  response.Headers.Get("x-amz-meta-raw-unicode"),
			"safe":        response.Headers.Get("x-amz-meta-safe"),
		}
	}
	get := read("GetObject", "source")
	head := read("HeadObject", "source")
	for name, got := range map[string]map[string]any{"get": get, "head": head} {
		if got["fakeEncoded"] != "actually-ascii" || got["asciiB64"] != "abc" || got["badB64"] != "=?UTF-8?B?77+977+977+9?=" || got["badQ"] != "badJ=ZZ value" || got["safe"] != safe {
			t.Fatalf("%s decoded metadata = %#v", name, got)
		}
		if got["nonASCII"] != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || got["rawUnicode"] != got["nonASCII"] || got["binary"] != "=?UTF-8?B?AAECAw==?=" {
			t.Fatalf("%s encoded metadata = %#v", name, got)
		}
	}

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "metadata-bucket", "Key": "copy", "CopySource": "metadata-bucket/source"}, nil)
	copyMetadata := read("HeadObject", "copy")
	if !reflect.DeepEqual(copyMetadata, head) {
		t.Fatalf("copied metadata = %#v, want %#v", copyMetadata, head)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "metadata-bucket", "Key": "replace", "CopySource": "metadata-bucket/source", "MetadataDirective": "REPLACE",
		"Metadata": map[string]any{"Fake-Encoded": "=?UTF-8?Q?replacement?="},
	}, nil)
	replaced := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "metadata-bucket", "Key": "replace"}, nil)
	if replaced.Headers.Get("x-amz-meta-fake-encoded") != "replacement" {
		t.Fatalf("replacement metadata = %v", replaced.Headers)
	}
	golden.AssertJSON(t, map[string]any{"get": get, "head": head, "copy": copyMetadata, "replace": replaced.Headers.Get("x-amz-meta-fake-encoded")})
}

func TestGetObjectResponseHeaderOverrides(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "response-overrides"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{
		"Bucket": "response-overrides", "Key": "object", "CacheControl": "stored", "ContentDisposition": "inline",
		"ContentEncoding": "gzip", "ContentLanguage": "en-US", "ContentType": "application/json", "Expires": "Thu, 22 Oct 2026 07:28:00 GMT",
	}, []byte("body"))
	overrides := map[string]any{
		"Bucket": "response-overrides", "Key": "object", "ResponseCacheControl": "max-age=74",
		"ResponseContentDisposition": `attachment; filename="foo.jpg"`, "ResponseContentEncoding": "identity",
		"ResponseContentLanguage": "de-DE", "ResponseContentType": "image/jpeg", "ResponseExpires": "Wed, 21 Oct 2015 07:28:00 GMT",
	}
	response := mustInvoke(t, p, "GetObject", overrides, nil)
	if string(readStream(t, response)) != "body" {
		t.Fatal("response override changed body")
	}
	got := map[string]any{
		"cacheControl": response.Headers.Get("Cache-Control"), "contentDisposition": response.Headers.Get("Content-Disposition"),
		"contentEncoding": response.Headers.Get("Content-Encoding"), "contentLanguage": response.Headers.Get("Content-Language"),
		"contentType": response.Headers.Get("Content-Type"), "expires": response.Headers.Get("Expires"),
	}
	ranged := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "response-overrides", "Key": "object", "Range": "bytes=0-1", "response-content-type": "text/csv"}, nil)
	if ranged.Status != http.StatusPartialContent || ranged.Headers.Get("Content-Type") != "text/csv" || string(readStream(t, ranged)) != "bo" {
		t.Fatalf("ranged override = %d %v", ranged.Status, ranged.Headers)
	}
	stored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "response-overrides", "Key": "object"}, nil)
	readStream(t, stored)
	if stored.Headers.Get("Cache-Control") != "stored" || stored.Headers.Get("Content-Type") != "application/json" || stored.Headers.Get("Expires") != "Thu, 22 Oct 2026 07:28:00 GMT" {
		t.Fatalf("overrides changed stored metadata = %v", stored.Headers)
	}
	golden.AssertJSON(t, map[string]any{"overrides": got, "range": map[string]any{"body": "bo", "contentType": ranged.Headers.Get("Content-Type"), "status": ranged.Status}, "stored": map[string]any{"cacheControl": stored.Headers.Get("Cache-Control"), "contentType": stored.Headers.Get("Content-Type"), "expires": stored.Headers.Get("Expires")}})
}

func TestObjectServerSideEncryption(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "encrypted"}, nil)
	defaultPut := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "default"}, []byte("body"))
	if defaultPut.Headers.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatalf("default encryption = %v", defaultPut.Headers)
	}
	defaultHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "encrypted", "Key": "default"}, nil)
	if defaultHead.Headers.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatalf("stored default encryption = %v", defaultHead.Headers)
	}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "encrypted", "Status": "Enabled"}, nil)
	keyID := "arn:aws:kms:us-east-1:123456789012:key/test"
	spitest.SeedKMSKey(t, deps, ident(), keyID, "Enabled")
	kms := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "kms", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID, "BucketKeyEnabled": true}, []byte("body"))
	if kms.Headers.Get("x-amz-server-side-encryption") != "aws:kms" || kms.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || kms.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" || kms.Headers.Get("x-amz-version-id") == "" {
		t.Fatalf("kms response = %v", kms.Headers)
	}
	kmsHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "encrypted", "Key": "kms", "VersionId": kms.Headers.Get("x-amz-version-id")}, nil)
	if kmsHead.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || kmsHead.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
		t.Fatalf("stored kms encryption = %v", kmsHead.Headers)
	}

	configuration := map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": keyID}, "BucketKeyEnabled": true}}}
	mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": "encrypted", "ServerSideEncryptionConfiguration": configuration}, nil)
	inherited := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "inherited"}, []byte("body"))
	if inherited.Headers.Get("x-amz-server-side-encryption") != "aws:kms" || inherited.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || inherited.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
		t.Fatalf("inherited encryption = %v", inherited.Headers)
	}
	overridden := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "overridden", "ServerSideEncryption": "AES256"}, []byte("body"))
	if overridden.Headers.Get("x-amz-server-side-encryption") != "AES256" || overridden.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != "" || overridden.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "" {
		t.Fatalf("overridden encryption = %v", overridden.Headers)
	}

	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "invalid", "ServerSideEncryption": "invalid"}, []byte("body"))
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("invalid encryption fault = %+v", fault)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "encrypted", "Key": "invalid"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("invalid encryption stored object")
	}
	mustInvoke(t, p, "DeleteBucketEncryption", map[string]any{"Bucket": "encrypted"}, nil)
	if restored := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "restored-default"}, []byte("body")); restored.Headers.Get("x-amz-server-side-encryption") != "AES256" {
		t.Fatalf("restored default = %v", restored.Headers)
	}
	golden.AssertJSON(t, map[string]any{
		"default":   map[string]any{"put": defaultPut.Headers.Get("x-amz-server-side-encryption"), "head": defaultHead.Headers.Get("x-amz-server-side-encryption")},
		"explicit":  map[string]any{"algorithm": kms.Headers.Get("x-amz-server-side-encryption"), "key": kmsHead.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id"), "bucketKey": kmsHead.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled"), "versioned": kms.Headers.Get("x-amz-version-id") != ""},
		"inherited": map[string]any{"algorithm": inherited.Headers.Get("x-amz-server-side-encryption"), "key": inherited.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id"), "bucketKey": inherited.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled")},
		"override":  map[string]any{"algorithm": overridden.Headers.Get("x-amz-server-side-encryption"), "key": overridden.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id"), "bucketKey": overridden.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled")},
	})
}

func TestExplicitKMSKeyValidation(t *testing.T) {
	deps := spitest.Deps(t)
	s3Pack, kmsPack := s3.New(deps), kms.New(deps)
	ctx, owner := context.Background(), ident()
	kmsCall := func(t *testing.T, id spi.Identity, operation string, input map[string]any) *spi.Response {
		t.Helper()
		response, err := kmsPack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input})
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		return response
	}
	createKey := func(t *testing.T, id spi.Identity) (string, string) {
		t.Helper()
		metadata := kmsCall(t, id, "CreateKey", nil).Output["KeyMetadata"].(map[string]any)
		return metadata["KeyId"].(string), metadata["Arn"].(string)
	}
	wantFault := func(t *testing.T, operation, keyID, code string) *spi.Fault {
		t.Helper()
		input := map[string]any{"Bucket": "kms-validation", "Key": operation, "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID}
		if operation == "CopyObject" {
			input["CopySource"] = "kms-validation/source"
		}
		_, err := invoke(t, s3Pack, operation, input, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != code || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s fault = %#v", operation, fault)
		}
		return fault
	}

	mustInvoke(t, s3Pack, "CreateBucket", map[string]any{"Bucket": "kms-validation"}, nil)
	mustInvoke(t, s3Pack, "PutObject", map[string]any{"Bucket": "kms-validation", "Key": "source"}, []byte("source"))
	keyID, keyARN := createKey(t, owner)
	mustInvoke(t, s3Pack, "PutObject", map[string]any{"Bucket": "kms-validation", "Key": "enabled", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID}, []byte("body"))
	kmsCall(t, owner, "CreateAlias", map[string]any{"AliasName": "alias/s3", "TargetKeyId": keyID})
	mustInvoke(t, s3Pack, "PutObject", map[string]any{"Bucket": "kms-validation", "Key": "alias", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": "arn:aws:kms:us-east-1:123456789012:alias/s3"}, []byte("body"))
	mustInvoke(t, s3Pack, "PutObject", map[string]any{"Bucket": "kms-validation", "Key": "managed", "ServerSideEncryption": "aws:kms"}, []byte("body"))
	mustInvoke(t, s3Pack, "GetObject", map[string]any{"Bucket": "kms-validation", "Key": "managed"}, nil)

	faults := map[string]any{}
	faults["putMissing"] = wantFault(t, "PutObject", "arn:aws:kms:us-east-1:123456789012:key/missing", "KMS.NotFoundException").Code
	faults["multipartMissing"] = wantFault(t, "CreateMultipartUpload", "arn:aws:kms:us-east-1:123456789012:key/missing", "KMS.NotFoundException").Code
	faults["copyMissing"] = wantFault(t, "CopyObject", "arn:aws:kms:us-east-1:123456789012:key/missing", "KMS.NotFoundException").Code

	_, westARN := createKey(t, spi.Identity{Account: owner.Account, Region: "us-west-2"})
	crossRegion := wantFault(t, "PutObject", westARN, "KMS.NotFoundException")
	if crossRegion.Message != "Invalid arn us-west-2" {
		t.Fatalf("cross-region message = %q", crossRegion.Message)
	}
	faults["crossRegion"] = map[string]any{"code": crossRegion.Code, "message": crossRegion.Message}
	kmsCall(t, owner, "DisableKey", map[string]any{"KeyId": keyID})
	faults["disabledWrite"] = wantFault(t, "PutObject", keyARN, "KMS.DisabledException").Code
	if _, err := invoke(t, s3Pack, "GetObject", map[string]any{"Bucket": "kms-validation", "Key": "enabled"}, nil); asFault(t, err).Code != "KMS.DisabledException" {
		t.Fatal("disabled key allowed encrypted read")
	} else {
		faults["disabledRead"] = asFault(t, err).Code
	}

	pendingID, pendingARN := createKey(t, owner)
	kmsCall(t, owner, "ScheduleKeyDeletion", map[string]any{"KeyId": pendingID})
	faults["pendingDeletion"] = wantFault(t, "PutObject", pendingARN, "KMS.KMSInvalidStateException").Code
	pendingImportARN := "arn:aws:kms:us-east-1:123456789012:key/pending-import"
	spitest.SeedKMSKey(t, deps, owner, pendingImportARN, "PendingImport")
	faults["pendingImport"] = wantFault(t, "PutObject", pendingImportARN, "KMS.DisabledException").Code

	other := spi.Identity{Account: "999999999999", Region: owner.Region}
	_, otherARN := createKey(t, other)
	mustInvoke(t, s3Pack, "PutObject", map[string]any{"Bucket": "kms-validation", "Key": "cross-account", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": otherARN}, []byte("body"))
	golden.AssertJSON(t, map[string]any{"accepted": []any{"alias-arn", "bare-key-id", "cross-account-arn", "managed-key"}, "faults": faults})
}

func TestBucketEncryptionConfiguration(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	bucket := map[string]any{"Bucket": "bucket-encryption"}
	mustInvoke(t, p, "CreateBucket", bucket, nil)
	if got := mustInvoke(t, p, "GetBucketEncryption", bucket, nil).Output; len(got) != 0 {
		t.Fatalf("default encryption = %#v", got)
	}
	rule := func(algorithm string, keyID any, bucketKey bool) map[string]any {
		defaults := map[string]any{"SSEAlgorithm": algorithm}
		if keyID != nil {
			defaults["KMSMasterKeyID"] = keyID
		}
		return map[string]any{"ApplyServerSideEncryptionByDefault": defaults, "BucketKeyEnabled": bucketKey}
	}
	put := func(value map[string]any) (*spi.Response, error) {
		return invoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": value}, nil)
	}
	for _, algorithm := range []string{"AES256", "aws:fsx", "aws:backup", "aws:kms", "aws:kms:dsse"} {
		configuration := map[string]any{"Rules": []any{rule(algorithm, nil, false)}}
		if _, err := put(configuration); err != nil {
			t.Fatalf("put %s: %v", algorithm, err)
		}
		if got := mustInvoke(t, p, "GetBucketEncryption", bucket, nil).Output["Rules"]; !reflect.DeepEqual(got, configuration["Rules"]) {
			t.Fatalf("get %s = %#v", algorithm, got)
		}
		object := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "object-" + strings.ReplaceAll(algorithm, ":", "-")}, []byte("body"))
		if got := object.Headers.Get("x-amz-server-side-encryption"); got != algorithm {
			t.Fatalf("%s object encryption = %q", algorithm, got)
		}
	}
	keyID := "arn:aws:kms:us-east-1:000000000000:key/bucket-default"
	baseline := map[string]any{"Rules": []any{rule("aws:kms", keyID, true)}}
	if _, err := put(baseline); err != nil {
		t.Fatal(err)
	}
	inherited := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": "kms-default"}, []byte("body"))
	if inherited.Headers.Get("x-amz-server-side-encryption") != "aws:kms" || inherited.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || inherited.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
		t.Fatalf("inherited bucket encryption = %v", inherited.Headers)
	}

	invalid := []struct {
		name, code string
		value      any
	}{
		{"missing configuration", "MalformedXML", nil},
		{"missing rules", "MalformedXML", map[string]any{}},
		{"empty rules", "MalformedXML", map[string]any{"Rules": []any{}}},
		{"multiple rules", "MalformedXML", map[string]any{"Rules": []any{rule("AES256", nil, false), rule("AES256", nil, false)}}},
		{"invalid rule", "MalformedXML", map[string]any{"Rules": []any{"rule"}}},
		{"missing defaults", "MalformedXML", map[string]any{"Rules": []any{map[string]any{}}}},
		{"missing algorithm", "MalformedXML", map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{}}}}},
		{"invalid algorithm", "MalformedXML", map[string]any{"Rules": []any{rule("invalid", nil, false)}}},
		{"AES key id", "InvalidArgument", map[string]any{"Rules": []any{rule("AES256", keyID, false)}}},
		{"DSSE key id", "InvalidArgument", map[string]any{"Rules": []any{rule("aws:kms:dsse", keyID, false)}}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": test.value}, nil)
			fault := asFault(t, err)
			if fault.Code != test.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
			if test.code == "InvalidArgument" && (fault.Message != "a KMSMasterKeyID is not applicable if the default sse algorithm is not aws:kms or aws:kms:dsse" || fault.Fields["ArgumentName"] != "ApplyServerSideEncryptionByDefault") {
				t.Fatalf("invalid key fault = %#v", fault)
			}
			if got := mustInvoke(t, p, "GetBucketEncryption", bucket, nil).Output["Rules"]; !reflect.DeepEqual(got, baseline["Rules"]) {
				t.Fatalf("invalid put replaced baseline = %#v", got)
			}
		})
	}
	for range 2 {
		mustInvoke(t, p, "DeleteBucketEncryption", bucket, nil)
	}
	if got := mustInvoke(t, p, "GetBucketEncryption", bucket, nil).Output; len(got) != 0 {
		t.Fatalf("deleted encryption = %#v", got)
	}
}

func TestObjectSSECustomerKey(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "sse-c"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "sse-c", "Status": "Enabled"}, nil)
	rawKey := []byte("0123456789abcdef0123456789abcdef")
	key := base64.StdEncoding.EncodeToString(rawKey)
	digest := md5.Sum(rawKey)
	keyMD5 := base64.StdEncoding.EncodeToString(digest[:])
	input := map[string]any{"Bucket": "sse-c", "Key": "object", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": key, "SSECustomerKeyMD5": keyMD5}
	put := mustInvoke(t, p, "PutObject", input, []byte("secret"))
	if put.Headers.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" || put.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 || put.Headers.Get("x-amz-server-side-encryption") != "" {
		t.Fatalf("put SSE-C headers = %v", put.Headers)
	}
	if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "sse-c", "Key": "object"}, nil); asFault(t, err).Code != "InvalidRequest" {
		t.Fatal("SSE-C object read without key")
	}
	readInput := map[string]any{"Bucket": "sse-c", "Key": "object", "VersionId": put.Headers.Get("x-amz-version-id"), "SSECustomerAlgorithm": "AES256", "SSECustomerKey": key, "SSECustomerKeyMD5": keyMD5}
	head := mustInvoke(t, p, "HeadObject", readInput, nil)
	if head.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
		t.Fatalf("head SSE-C headers = %v", head.Headers)
	}
	body := string(readStream(t, mustInvoke(t, p, "GetObject", readInput, nil)))
	if body != "secret" {
		t.Fatalf("SSE-C body = %q", body)
	}
	md5Only := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "sse-c", "Key": "object", "VersionId": put.Headers.Get("x-amz-version-id"), "SSECustomerKeyMD5": keyMD5}, nil)
	shortDigest := md5.Sum([]byte("short"))
	for name, test := range map[string]struct {
		changes map[string]any
		code    string
	}{
		"algorithm":         {map[string]any{"SSECustomerAlgorithm": "AES128"}, "InvalidEncryptionAlgorithmError"},
		"short key":         {map[string]any{"SSECustomerKey": base64.StdEncoding.EncodeToString([]byte("short")), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(shortDigest[:])}, "InvalidArgument"},
		"key encoding":      {map[string]any{"SSECustomerKey": "*"}, "InvalidArgument"},
		"key digest":        {map[string]any{"SSECustomerKeyMD5": "AAAAAAAAAAAAAAAAAAAAAA=="}, "InvalidArgument"},
		"mixed SSE":         {map[string]any{"ServerSideEncryption": "AES256"}, "InvalidArgument"},
		"missing algorithm": {map[string]any{"SSECustomerAlgorithm": ""}, "InvalidArgument"},
		"missing key":       {map[string]any{"SSECustomerKey": ""}, "InvalidArgument"},
	} {
		invalid := maps.Clone(input)
		for key, value := range test.changes {
			invalid[key] = value
		}
		if _, err := invoke(t, p, "PutObject", invalid, []byte("bad")); asFault(t, err).Code != test.code {
			t.Fatalf("%s SSE-C fault = %v", name, err)
		}
	}
	golden.AssertJSON(t, map[string]any{
		"put":     map[string]any{"algorithm": put.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": put.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5},
		"head":    map[string]any{"algorithm": head.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": head.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5},
		"md5Only": md5Only.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5,
		"body":    body,
		"version": put.Headers.Get("x-amz-version-id") != "",
	})
}

func TestMultipartServerSideEncryption(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-encryption"}, nil)
	keyID := "arn:aws:kms:us-east-1:123456789012:key/multipart"
	spitest.SeedKMSKey(t, deps, ident(), keyID, "Enabled")
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-encryption", "Key": "object", "ChecksumAlgorithm": "CRC64NVME", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID, "BucketKeyEnabled": true}, nil)
	assertEncryption := func(name string, response *spi.Response) {
		t.Helper()
		if response.Headers.Get("x-amz-server-side-encryption") != "aws:kms" || response.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || response.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
			t.Fatalf("%s encryption = %v", name, response.Headers)
		}
	}
	assertEncryption("create", created)
	uploadID := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte("body"))
	assertEncryption("part", part)
	aes := map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256"}}}}
	mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": "multipart-encryption", "ServerSideEncryptionConfiguration": aes}, nil)
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	assertEncryption("complete", completed)
	if completed.Headers.Get("x-amz-checksum-crc64nvme") != "" || completed.Headers.Get("x-amz-checksum-type") != "" || completed.Output["ChecksumCRC64NVME"] != nil || completed.Output["ChecksumType"] != nil {
		t.Fatalf("KMS completion exposed checksum = headers %v output %#v", completed.Headers, completed.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "multipart-encryption", "Key": "object", "ChecksumMode": "ENABLED"}, nil)
	assertEncryption("head", head)
	if head.Headers.Get("x-amz-checksum-crc64nvme") == "" || head.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("KMS object did not persist checksum = %v", head.Headers)
	}

	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-encryption", "Key": "invalid", "ServerSideEncryption": "invalid"}, nil)
	fault := asFault(t, err)
	if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("invalid encryption fault = %+v", fault)
	}
	snapshot := func(response *spi.Response) map[string]any {
		return map[string]any{"algorithm": response.Headers.Get("x-amz-server-side-encryption"), "key": response.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id"), "bucketKey": response.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled"), "checksum": response.Headers.Get("x-amz-checksum-crc64nvme"), "checksumType": response.Headers.Get("x-amz-checksum-type")}
	}
	golden.AssertJSON(t, map[string]any{"create": snapshot(created), "part": snapshot(part), "complete": snapshot(completed), "head": snapshot(head), "invalid": map[string]any{"code": fault.Code, "status": fault.HTTPStatus}})
}

func TestMultipartSSECustomerKey(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-sse-c"}, nil)
	rawKey := []byte("0123456789abcdef0123456789abcdef")
	digest := md5.Sum(rawKey)
	key, keyMD5 := base64.StdEncoding.EncodeToString(rawKey), base64.StdEncoding.EncodeToString(digest[:])
	encryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": key, "SSECustomerKeyMD5": keyMD5}
	createInput := maps.Clone(encryption)
	createInput["Bucket"], createInput["Key"] = "multipart-sse-c", "object"
	invalidCreate := maps.Clone(createInput)
	invalidCreate["SSECustomerAlgorithm"] = "AES128"
	if _, err := invoke(t, p, "CreateMultipartUpload", invalidCreate, nil); asFault(t, err).Code != "InvalidEncryptionAlgorithmError" {
		t.Fatalf("invalid create SSE-C = %v", err)
	}
	created := mustInvoke(t, p, "CreateMultipartUpload", createInput, nil)
	if created.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 || created.Headers.Get("x-amz-server-side-encryption") != "" {
		t.Fatalf("create SSE-C headers = %v", created.Headers)
	}
	uploadID := created.Output["UploadId"].(string)
	partInput := map[string]any{"UploadId": uploadID, "PartNumber": 1}
	if _, err := invoke(t, p, "UploadPart", partInput, []byte("body")); asFault(t, err).Code != "InvalidRequest" {
		t.Fatalf("part without SSE-C = %v", err)
	}
	for key, value := range encryption {
		partInput[key] = value
	}
	part := mustInvoke(t, p, "UploadPart", partInput, []byte("body"))
	if part.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
		t.Fatalf("part SSE-C headers = %v", part.Headers)
	}
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
		t.Fatalf("complete SSE-C headers = %v", completed.Headers)
	}
	readInput := maps.Clone(encryption)
	readInput["Bucket"], readInput["Key"] = "multipart-sse-c", "object"
	get := mustInvoke(t, p, "GetObject", readInput, nil)
	body := string(readStream(t, get))
	if body != "body" || get.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
		t.Fatalf("stored multipart SSE-C headers = %v", get.Headers)
	}
	golden.AssertJSON(t, map[string]any{
		"create":   map[string]any{"algorithm": created.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": created.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5},
		"part":     map[string]any{"algorithm": part.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": part.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5},
		"complete": map[string]any{"algorithm": completed.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5},
		"get":      map[string]any{"algorithm": get.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": get.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == keyMD5, "body": body},
	})
}

func TestUploadPartSSECustomerKeyFaults(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-sse-c-faults"}, nil)
	key := bytes.Repeat([]byte{'a'}, 32)
	digest := md5.Sum(key)
	encryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(key), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(digest[:])}
	create := maps.Clone(encryption)
	create["Bucket"], create["Key"] = "upload-part-sse-c-faults", "encrypted"
	encryptedID := mustInvoke(t, p, "CreateMultipartUpload", create, nil).Output["UploadId"].(string)
	plainID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-sse-c-faults", "Key": "plain"}, nil).Output["UploadId"].(string)

	otherKey := bytes.Repeat([]byte{'b'}, 32)
	otherDigest := md5.Sum(otherKey)
	wrongEncryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(otherKey), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(otherDigest[:])}
	tests := []struct {
		name, key, uploadID string
		encryption          map[string]any
		message             string
	}{
		{"missing", "encrypted", encryptedID, nil, "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters."},
		{"unexpected", "plain", plainID, encryption, "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters."},
		{"mismatch", "encrypted", encryptedID, wrongEncryption, "The provided encryption parameters did not match the ones used originally."},
	}
	characterization := map[string]any{}
	for index, test := range tests {
		input := maps.Clone(test.encryption)
		if input == nil {
			input = map[string]any{}
		}
		input["Bucket"], input["Key"], input["UploadId"], input["PartNumber"] = "upload-part-sse-c-faults", test.key, test.uploadID, index+1
		_, err := invoke(t, p, "UploadPart", input, []byte("part"))
		if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("case %d fault = %#v", index, fault)
		} else {
			characterization[test.name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
		}
	}
	for _, upload := range []struct{ key, id string }{{"encrypted", encryptedID}, {"plain", plainID}} {
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "upload-part-sse-c-faults", "Key": upload.key, "UploadId": upload.id}, nil)
		if len(listed.Output["Parts"].([]any)) != 0 {
			t.Fatalf("rejected SSE-C request stored parts = %#v", listed.Output)
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestCopyObjectSSECustomerKeys(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "copy-sse-c"}, nil)
	sourceRaw, destinationRaw := bytes.Repeat([]byte{1}, 32), bytes.Repeat([]byte{2}, 32)
	sourceDigest, destinationDigest := md5.Sum(sourceRaw), md5.Sum(destinationRaw)
	sourceKey, sourceMD5 := base64.StdEncoding.EncodeToString(sourceRaw), base64.StdEncoding.EncodeToString(sourceDigest[:])
	destinationKey, destinationMD5 := base64.StdEncoding.EncodeToString(destinationRaw), base64.StdEncoding.EncodeToString(destinationDigest[:])
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "copy-sse-c", "Key": "source", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": sourceKey, "SSECustomerKeyMD5": sourceMD5}, []byte("secret"))
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "copy-sse-c", "Key": "missing-source-key", "CopySource": "copy-sse-c/source"}, nil); asFault(t, err).Code != "InvalidRequest" {
		t.Fatalf("copy without source SSE-C = %v", err)
	}
	source := map[string]any{"CopySourceSSECustomerAlgorithm": "AES256", "CopySourceSSECustomerKey": sourceKey, "CopySourceSSECustomerKeyMD5": sourceMD5}
	plainInput := maps.Clone(source)
	plainInput["Bucket"], plainInput["Key"], plainInput["CopySource"] = "copy-sse-c", "plain", "copy-sse-c/source"
	plain := mustInvoke(t, p, "CopyObject", plainInput, nil)
	if plain.Headers.Get("x-amz-server-side-encryption") != "AES256" || plain.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != "" {
		t.Fatalf("plain copy encryption = %v", plain.Headers)
	}
	customerInput := maps.Clone(plainInput)
	customerInput["Key"] = "customer"
	customerInput["SSECustomerAlgorithm"], customerInput["SSECustomerKey"], customerInput["SSECustomerKeyMD5"] = "AES256", destinationKey, destinationMD5
	customer := mustInvoke(t, p, "CopyObject", customerInput, nil)
	if customer.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != destinationMD5 {
		t.Fatalf("customer copy encryption = %v", customer.Headers)
	}
	if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "copy-sse-c", "Key": "customer"}, nil); asFault(t, err).Code != "InvalidRequest" {
		t.Fatal("customer copy readable without destination key")
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "copy-sse-c", "Key": "customer", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": destinationKey, "SSECustomerKeyMD5": destinationMD5}, nil)
	body := string(readStream(t, get))
	if body != "secret" {
		t.Fatal("customer copy body mismatch")
	}
	invalidSource := maps.Clone(customerInput)
	invalidSource["Key"], invalidSource["CopySourceSSECustomerKeyMD5"] = "invalid-source-key", "AAAAAAAAAAAAAAAAAAAAAA=="
	if _, err := invoke(t, p, "CopyObject", invalidSource, nil); asFault(t, err).Code != "InvalidArgument" {
		t.Fatalf("invalid copy source SSE-C = %v", err)
	}
	golden.AssertJSON(t, map[string]any{
		"plain":    map[string]any{"algorithm": plain.Headers.Get("x-amz-server-side-encryption"), "customerKey": plain.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != ""},
		"customer": map[string]any{"algorithm": customer.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": customer.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == destinationMD5},
		"get":      map[string]any{"body": body, "keyMD5Matches": get.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == destinationMD5},
	})
}

func TestUploadPartCopySSECustomerKeys(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "part-copy-sse-c"}, nil)
	sourceRaw, destinationRaw := bytes.Repeat([]byte{3}, 32), bytes.Repeat([]byte{4}, 32)
	sourceDigest, destinationDigest := md5.Sum(sourceRaw), md5.Sum(destinationRaw)
	sourceKey, sourceMD5 := base64.StdEncoding.EncodeToString(sourceRaw), base64.StdEncoding.EncodeToString(sourceDigest[:])
	destinationKey, destinationMD5 := base64.StdEncoding.EncodeToString(destinationRaw), base64.StdEncoding.EncodeToString(destinationDigest[:])
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "part-copy-sse-c", "Key": "source", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": sourceKey, "SSECustomerKeyMD5": sourceMD5}, []byte("copied part"))
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "part-copy-sse-c", "Key": "destination", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": destinationKey, "SSECustomerKeyMD5": destinationMD5}, nil)
	base := map[string]any{"Bucket": "part-copy-sse-c", "Key": "destination", "UploadId": created.Output["UploadId"], "PartNumber": 1, "CopySource": "part-copy-sse-c/source"}
	destination := maps.Clone(base)
	destination["SSECustomerAlgorithm"], destination["SSECustomerKey"], destination["SSECustomerKeyMD5"] = "AES256", destinationKey, destinationMD5
	if _, err := invoke(t, p, "UploadPartCopy", destination, nil); asFault(t, err).Code != "InvalidRequest" {
		t.Fatalf("part copy without source SSE-C = %v", err)
	}
	source := maps.Clone(base)
	source["CopySourceSSECustomerAlgorithm"], source["CopySourceSSECustomerKey"], source["CopySourceSSECustomerKeyMD5"] = "AES256", sourceKey, sourceMD5
	if _, err := invoke(t, p, "UploadPartCopy", source, nil); asFault(t, err).Code != "InvalidRequest" {
		t.Fatalf("part copy without destination SSE-C = %v", err)
	}
	for key, value := range destination {
		source[key] = value
	}
	part := mustInvoke(t, p, "UploadPartCopy", source, nil)
	if part.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != destinationMD5 {
		t.Fatalf("part copy encryption = %v", part.Headers)
	}
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(created.Output["UploadId"].(string), completedPart(1, part)), nil)
	if completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != destinationMD5 {
		t.Fatalf("part copy completion encryption = %v", completed.Headers)
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "part-copy-sse-c", "Key": "destination", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": destinationKey, "SSECustomerKeyMD5": destinationMD5}, nil)
	body := string(readStream(t, get))
	if body != "copied part" {
		t.Fatal("part copy body mismatch")
	}
	golden.AssertJSON(t, map[string]any{
		"part":     map[string]any{"algorithm": part.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": part.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == destinationMD5},
		"complete": map[string]any{"algorithm": completed.Headers.Get("x-amz-server-side-encryption-customer-algorithm"), "keyMD5Matches": completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == destinationMD5},
		"get":      map[string]any{"body": body, "keyMD5Matches": get.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") == destinationMD5},
	})
}

func TestCopyObjectTaggingDirective(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source", "Tagging": "team=data"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source"}, nil)
	copied := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	if tags := copied.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "team" {
		t.Fatalf("copied tags = %#v", tags)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "replaced", "CopySource": "bucket/source", "TaggingDirective": "REPLACE", "Tagging": "owner=mirror"}, nil)
	replaced := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "replaced"}, nil)
	if tags := replaced.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "owner" {
		t.Fatalf("replaced tags = %#v", tags)
	}
}

func TestCopyObjectChecksums(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("copy me")
	sha256Sum := sha256.Sum256(body)
	sha256Value := base64.StdEncoding.EncodeToString(sha256Sum[:])
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source", "ChecksumSHA256": sha256Value}, body)

	inherited := mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "inherited", "CopySource": "bucket/source"}, nil)
	if inherited.Output["ChecksumSHA256"] != sha256Value || inherited.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("inherited copy checksum = %#v", inherited.Output)
	}
	inheritedHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "inherited", "ChecksumMode": "ENABLED"}, nil)
	if inheritedHead.Headers.Get("x-amz-checksum-sha256") != sha256Value || inheritedHead.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("inherited stored checksum = %v", inheritedHead.Headers)
	}

	crc32Sum := make([]byte, 4)
	binary.BigEndian.PutUint32(crc32Sum, crc32.ChecksumIEEE(body))
	crc32Value := base64.StdEncoding.EncodeToString(crc32Sum)
	overridden := mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "overridden", "CopySource": "bucket/source", "ChecksumAlgorithm": "CRC32"}, nil)
	if overridden.Output["ChecksumCRC32"] != crc32Value || overridden.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("overridden copy checksum = %#v", overridden.Output)
	}
	overriddenHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "overridden", "ChecksumMode": "ENABLED"}, nil)
	if overriddenHead.Headers.Get("x-amz-checksum-crc32") != crc32Value || overriddenHead.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("overridden stored checksum = %v", overriddenHead.Headers)
	}
	golden.AssertJSON(t, map[string]any{
		"inherited":  map[string]any{"checksum": inherited.Output["ChecksumSHA256"], "type": inherited.Output["ChecksumType"], "stored": inheritedHead.Headers.Get("x-amz-checksum-sha256")},
		"overridden": map[string]any{"checksum": overridden.Output["ChecksumCRC32"], "type": overridden.Output["ChecksumType"], "stored": overriddenHead.Headers.Get("x-amz-checksum-crc32")},
	})
}

func TestCopyObjectLastModifiedCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("body"))
	if err := deps.Clock.Advance(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	copied := mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/source"}, nil)
	modifiedValue, ok := copied.Output["LastModified"].(string)
	modified, err := time.Parse(time.RFC3339, modifiedValue)
	if !ok || err != nil {
		t.Fatalf("copy LastModified = %#v: %v", copied.Output["LastModified"], err)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "copy"}, nil)
	stored, err := http.ParseTime(head.Headers.Get("Last-Modified"))
	if err != nil || !modified.Equal(stored) {
		t.Fatalf("copy LastModified %s, stored %q: %v", modified, head.Headers.Get("Last-Modified"), err)
	}
	golden.AssertJSON(t, map[string]any{"response": copied.Output["LastModified"], "stored": head.Headers.Get("Last-Modified")})
}

func TestCopyObjectDirectiveValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("body"))
	errors := map[string]any{}
	for _, test := range []struct{ input, value string }{
		{"MetadataDirective", "INVALID"},
		{"MetadataDirective", "copy"},
		{"TaggingDirective", "INVALID"},
		{"TaggingDirective", "replace"},
	} {
		key := test.input + "-" + test.value
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": key, "CopySource": "bucket/source", test.input: test.value}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s=%s fault = %#v", test.input, test.value, fault)
		}
		errors[key] = fault.Code
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("invalid directive created %s: %v", key, err)
		}
	}
	golden.AssertJSON(t, errors)
}

func TestCopyObjectRejectsUnchangedSelfCopy(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k", "Metadata": map[string]any{"owner": "old"}}, []byte("body"))
	selfCopy := func(input map[string]any) (*spi.Response, error) {
		t.Helper()
		input["Bucket"], input["Key"], input["CopySource"] = "bucket", "k", "bucket/k"
		return invoke(t, p, "CopyObject", input, nil)
	}
	characterization := map[string]any{}
	for _, test := range []struct {
		name  string
		input map[string]any
	}{
		{"unchanged", map[string]any{}},
		{"copyMetadata", map[string]any{"MetadataDirective": "COPY"}},
		{"replaceTags", map[string]any{"TaggingDirective": "REPLACE", "Tagging": "stage=new"}},
	} {
		_, err := selfCopy(test.input)
		fault := asFault(t, err)
		if fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s self-copy = %#v", test.name, fault)
		}
		characterization[test.name] = fault.Code
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil))); body != "body" {
		t.Fatalf("rejected self-copy changed body: %q", body)
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "other", "CopySource": "bucket/k"}, nil); err != nil {
		t.Fatalf("same-bucket copy to a different key: %v", err)
	}
	characterization["differentKey"] = "allowed"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "k", "CopySource": "bucket/k"}, nil); err != nil {
		t.Fatalf("cross-bucket copy to the same key: %v", err)
	}
	characterization["differentBucket"] = "allowed"
	for _, test := range []struct {
		name  string
		input map[string]any
	}{
		{"replaceMetadata", map[string]any{"MetadataDirective": "REPLACE", "Metadata": map[string]any{"owner": "new"}}},
		{"storageClass", map[string]any{"StorageClass": "STANDARD_IA"}},
		{"websiteRedirect", map[string]any{"WebsiteRedirectLocation": "/new"}},
		{"serverEncryption", map[string]any{"ServerSideEncryption": "AES256"}},
		{"customerEncryption", map[string]any{"SSECustomerKeyMD5": "digest"}},
	} {
		if _, err := selfCopy(test.input); err != nil {
			t.Fatalf("%s self-copy: %v", test.name, err)
		}
		characterization[test.name] = "allowed"
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "encrypted"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "k"}, []byte("body"))
	mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": "encrypted", "ServerSideEncryptionConfiguration": map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256"}}}}}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "encrypted", "Key": "k", "CopySource": "encrypted/k"}, nil); err != nil {
		t.Fatalf("default-encrypted bucket self-copy: %v", err)
	}
	characterization["bucketEncryption"] = "allowed"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "restored"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "restored", "Key": "k", "StorageClass": "GLACIER"}, []byte("body"))
	mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "restored", "Key": "k", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "restored", "Key": "k", "CopySource": "restored/k"}, nil); err != nil {
		t.Fatalf("restored source self-copy: %v", err)
	}
	characterization["restoredSource"] = "allowed"
	golden.AssertJSON(t, characterization)
}

func TestArchiveRestoreCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "archive"}, nil)
	for key, storageClass := range map[string]string{"glacier": "GLACIER", "deep": "DEEP_ARCHIVE", "instant": "GLACIER_IR", "standard": "STANDARD"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": key, "StorageClass": storageClass}, []byte(key))
	}

	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if head.Headers.Get("x-amz-storage-class") != "GLACIER" || head.Headers.Get("x-amz-restore") != "" {
		t.Fatalf("archived head before restore = %v", head.Headers)
	}
	before := map[string]any{"head": "allowed"}
	for _, key := range []string{"glacier", "deep"} {
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": key}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidObjectState" || fault.HTTPStatus != http.StatusForbidden || fault.Fields["StorageClass"] != map[string]string{"glacier": "GLACIER", "deep": "DEEP_ARCHIVE"}[key] {
			t.Fatalf("%s before restore = %#v", key, fault)
		}
		before[key] = fault.Code
	}
	instant := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "instant"}, nil)
	_ = instant.Stream.Close()
	before["instant"] = "allowed"

	_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "archive", "Key": "rejected-copy", "CopySource": "archive/glacier"}, nil)
	copyFault := asFault(t, err)
	if copyFault.Code != "InvalidObjectState" {
		t.Fatalf("unrestored copy = %#v", copyFault)
	}
	mpu := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "archive", "Key": "multipart-copy"}, nil)
	uploadID := mpu.Output["UploadId"].(string)
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID, "PartNumber": 1, "CopySource": "archive/glacier"}, nil)
	partFault := asFault(t, err)
	parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID}, nil)
	if partFault.Code != "InvalidObjectState" || len(parts.Output["Parts"].([]any)) != 0 {
		t.Fatalf("unrestored part copy = %#v parts=%#v", partFault, parts.Output)
	}

	_, err = invoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "missing", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	missing := asFault(t, err)
	_, err = invoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "standard", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	standard := asFault(t, err)
	if missing.Code != "NoSuchKey" || standard.Code != "InvalidObjectState" || standard.Fields["StorageClass"] != "STANDARD" {
		t.Fatalf("restore boundaries missing=%#v standard=%#v", missing, standard)
	}
	withoutDays := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if withoutDays.Status != http.StatusOK {
		t.Fatalf("restore without days = %d", withoutDays.Status)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil); asFault(t, err).Code != "InvalidObjectState" {
		t.Fatalf("restore without days unlocked object: %v", err)
	}
	first := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier", "RestoreRequest": map[string]any{"Days": 2}}, nil)
	second := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier", "Days": 2}, nil)
	if first.Status != http.StatusAccepted || second.Status != http.StatusOK {
		t.Fatalf("restore statuses = %d, %d", first.Status, second.Status)
	}
	restoredHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	restoreHeader := restoredHead.Headers.Get("x-amz-restore")
	if restoreHeader != `ongoing-request="false", expiry-date="Sun, 04 Jan 1970 00:00:00 GMT"` {
		t.Fatalf("restore header = %q", restoreHeader)
	}
	restoredGet := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if body := string(readStream(t, restoredGet)); body != "glacier" || restoredGet.Headers.Get("x-amz-restore") != restoreHeader {
		t.Fatalf("restored get body=%q headers=%v", body, restoredGet.Headers)
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "archive", "Key": "copied", "CopySource": "archive/glacier"}, nil); err != nil {
		t.Fatalf("restored copy: %v", err)
	}
	if _, err := invoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID, "PartNumber": 1, "CopySource": "archive/glacier"}, nil); err != nil {
		t.Fatalf("restored part copy: %v", err)
	}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "archive", "Status": "Enabled"}, nil)
	old := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": "versioned", "StorageClass": "GLACIER"}, []byte("old"))
	oldVersion := old.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "versioned", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": "versioned", "StorageClass": "GLACIER"}, []byte("new"))
	_, err = invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "versioned"}, nil)
	currentFault := asFault(t, err)
	oldGet := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "versioned", "VersionId": oldVersion}, nil)
	if body := string(readStream(t, oldGet)); body != "old" || oldGet.Headers.Get("x-amz-restore") == "" || currentFault.Code != "InvalidObjectState" {
		t.Fatalf("version restore body=%q header=%q current=%#v", body, oldGet.Headers.Get("x-amz-restore"), currentFault)
	}

	golden.AssertJSON(t, map[string]any{
		"before": before,
		"copy":   map[string]any{"object": copyFault.Code, "part": partFault.Code, "partsWritten": 0},
		"restore": map[string]any{
			"missing": missing.Code, "standard": standard.Code, "withoutDays": withoutDays.Status,
			"first": first.Status, "second": second.Status, "header": restoreHeader,
		},
		"version": map[string]any{"current": currentFault.Code, "old": "allowed"},
	})
}

func TestStorageClassValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "classes"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": "source"}, []byte("source"))
	allowed := map[string]any{}
	for _, storageClass := range []string{"STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "SNOW", "EXPRESS_ONEZONE"} {
		key := "allowed-" + storageClass
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": key, "StorageClass": storageClass}, []byte(storageClass))
		head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": key}, nil)
		header := head.Headers.Get("x-amz-storage-class")
		if storageClass == "STANDARD" {
			if header != "" {
				t.Fatalf("STANDARD header = %q", header)
			}
		} else if header != storageClass {
			t.Fatalf("%s header = %q", storageClass, header)
		}
		allowed[storageClass] = header
	}
	defaulted := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": "defaulted"}, []byte("default"))
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": "defaulted"}, nil); defaulted.Headers.Get("x-amz-storage-class") != "" || head.Headers.Get("x-amz-storage-class") != "" {
		t.Fatalf("default storage class headers put=%v head=%v", defaulted.Headers, head.Headers)
	}

	invalid := map[string]any{}
	for _, storageClass := range []string{"INVALID", "standard", "OUTPOSTS", " STANDARD"} {
		key := "invalid-" + storageClass
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": key, "StorageClass": storageClass}, []byte("rejected"))
		fault := asFault(t, err)
		if fault.Code != "InvalidStorageClass" || fault.Message != "The storage class you specified is not valid" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["StorageClassRequested"] != storageClass {
			t.Fatalf("put %q = %#v", storageClass, fault)
		}
		if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("invalid put %q created object: %v", storageClass, err)
		}
		invalid[storageClass] = fault.Code
	}
	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "classes", "Key": "multipart", "StorageClass": "OUTPOSTS"}, nil)
	mpuFault := asFault(t, err)
	uploads := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "classes"}, nil)
	if mpuFault.Code != "InvalidStorageClass" || len(uploads.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("invalid multipart = %#v uploads=%#v", mpuFault, uploads.Output)
	}
	_, err = invoke(t, p, "CopyObject", map[string]any{"Bucket": "classes", "Key": "copy", "CopySource": "classes/source", "StorageClass": "invalid"}, nil)
	copyFault := asFault(t, err)
	if copyFault.Code != "InvalidStorageClass" {
		t.Fatalf("invalid copy = %#v", copyFault)
	}
	if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": "copy"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("invalid copy created object: %v", err)
	}

	golden.AssertJSON(t, map[string]any{
		"allowed":    allowed,
		"default":    "STANDARD",
		"invalid":    invalid,
		"operations": map[string]any{"copy": copyFault.Code, "multipart": mpuFault.Code},
	})
}

func TestObjectKeyLengthValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "keys"}, nil)
	valid := map[string]any{}
	for name, key := range map[string]string{"ascii": strings.Repeat("a", 1024), "utf8": strings.Repeat("é", 512)} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "keys", "Key": key}, []byte(name))
		if got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "keys", "Key": key}, nil); string(readStream(t, got)) != name {
			t.Fatalf("%s boundary key was not stored", name)
		}
		valid[name] = len(key)
	}
	invalid := map[string]any{}
	for name, key := range map[string]string{"ascii": strings.Repeat("a", 1025), "utf8": strings.Repeat("é", 513)} {
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "keys", "Key": key}, []byte("rejected"))
		fault := asFault(t, err)
		if fault.Code != "KeyTooLongError" || fault.Message != "Your key is too long" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["MaxSizeAllowed"] != "1024" || fault.Fields["Size"] != strconv.Itoa(len(key)) {
			t.Fatalf("%s oversized key = %#v", name, fault)
		}
		if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "keys", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("%s oversized key created object: %v", name, err)
		}
		invalid[name] = map[string]any{"code": fault.Code, "max": fault.Fields["MaxSizeAllowed"], "size": fault.Fields["Size"]}
	}
	longKey := strings.Repeat("x", 1025)
	operations := map[string]any{}
	for operation, input := range map[string]map[string]any{
		"CopyObject":            {"Bucket": "keys", "Key": longKey, "CopySource": "missing/source"},
		"CreateMultipartUpload": {"Bucket": "keys", "Key": longKey},
	} {
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "KeyTooLongError" {
			t.Fatalf("%s oversized key = %#v", operation, fault)
		}
		operations[operation] = fault.Code
	}
	if uploads := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "keys"}, nil); len(uploads.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("oversized key created multipart upload: %#v", uploads.Output)
	}
	golden.AssertJSON(t, map[string]any{"valid_bytes": valid, "invalid": invalid, "operations": operations})
}

func TestExpectedBucketOwnerAndDeleteBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("body"))
	if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "bucket", "ExpectedBucketOwner": ident().Account}, nil); err != nil {
		t.Fatalf("matching owner: %v", err)
	}
	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "bucket", "ExpectedBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"HeadBucket", map[string]any{"Bucket": "bucket"}},
		{"GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
		{"HeadObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
		{"PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "k", "TagSet": []any{}}},
		{"DeleteObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
	} {
		test.input["ExpectedBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, operation := range []string{"DeleteObject", "DeleteObjects", "GetObjectTagging"} {
		input := map[string]any{"Bucket": "missing", "Key": "k", "Objects": []any{map[string]any{"Key": "k"}}}
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		errors[operation+"Missing"] = fault.Code
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedSourceBucketOwnerAndCopyBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source"}, nil)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "key"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "copy", "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)

	upload := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "destination", "Key": "multipart"}, nil)
	uploadID := upload.Output["UploadId"].(string)
	mustInvoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1, "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)
	httpReq := httptest.NewRequest(http.MethodPut, "/destination/header-denied", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", "999999999999")
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-denied", "CopySource": "source/key"}}); asFault(t, err).Code != "AccessDenied" {
		t.Fatalf("mismatched source owner header: %v", err)
	}
	httpReq = httptest.NewRequest(http.MethodPut, "/destination/header-copy", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", ident().Account)
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-copy", "CopySource": "source/key"}}); err != nil {
		t.Fatalf("matching source owner header: %v", err)
	}

	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "invalid-" + expected, "CopySource": "source/key", "ExpectedSourceBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected source owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "denied", "CopySource": "source/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "source/key"}},
	} {
		test.input["ExpectedSourceBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "missing", "CopySource": "missing/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "missing/key"}},
	} {
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing source = %#v", test.operation, fault)
		}
		errors[test.operation+"Missing"] = fault.Code
	}
	for _, key := range []string{"denied", "header-denied"} {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "destination", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("denied copy %q created object: %v", key, err)
		}
	}
	if parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID}, nil).Output["Parts"].([]any); len(parts) != 1 {
		t.Fatalf("rejected part mutated upload: %#v", parts)
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedBucketOwnerAcrossBucketScopedOperations(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("id,name\n1,Ada\n"))
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart"}, nil).Output["UploadId"].(string)
	tests := []struct {
		operation string
		input     map[string]any
		body      []byte
	}{
		{"PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil},
		{"CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "missing/k"}, nil},
		{"DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{}}, nil},
		{"UploadPart", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1}, []byte("part")},
		{"UploadPartCopy", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1, "CopySource": "missing/k"}, nil},
		{"CompleteMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": []any{}}}, nil},
		{"AbortMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil},
		{"ListParts", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil},
		{"SelectObjectContent", map[string]any{"Bucket": "bucket", "Key": "k", "Expression": "SELECT * FROM S3Object"}, nil},
		{"PutObjectAnnotation", map[string]any{"Bucket": "bucket", "Key": "k", "AnnotationId": "a"}, nil},
	}
	errors := map[string]any{}
	for _, test := range tests {
		test.input["ExpectedBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, test.body)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
		delete(test.input, "ExpectedBucketOwner")
		test.input["Bucket"] = "missing"
		_, err = invoke(t, p, test.operation, test.input, test.body)
		fault = asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", test.operation, fault)
		}
		errors[test.operation+"Missing"] = fault.Code
	}
	if versioning := mustInvoke(t, p, "GetBucketVersioning", map[string]any{"Bucket": "bucket"}, nil).Output; len(versioning) != 0 {
		t.Fatalf("rejected versioning persisted: %#v", versioning)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "copy"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("rejected copy persisted: %v", err)
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil))); body != "id,name\n1,Ada\n" {
		t.Fatalf("rejected delete changed source: %q", body)
	}
	if parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil).Output["Parts"].([]any); len(parts) != 0 {
		t.Fatalf("rejected multipart operations persisted parts: %#v", parts)
	}
	if _, err := invoke(t, p, "GetObjectAnnotation", map[string]any{"Bucket": "bucket", "Key": "k", "AnnotationId": "a"}, nil); asFault(t, err).Code != "NoSuchAnnotation" {
		t.Fatalf("rejected annotation persisted: %v", err)
	}
	golden.AssertJSON(t, errors)
}

func TestLocalStackUnsupportedOperations(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"GetBucketPolicyStatus", map[string]any{"Bucket": "bucket"}},
		{"GetObjectTorrent", map[string]any{"Bucket": "bucket", "Key": "key"}},
	} {
		t.Run(test.operation, func(t *testing.T) {
			_, err := invoke(t, p, test.operation, test.input, nil)
			fault := asFault(t, err)
			if fault.Code != "MirrorNotImplemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fields["operation"] != test.operation {
				t.Fatalf("fault = %#v", fault)
			}
		})
	}
}

func TestTagValidationAndBucketSemantics(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{}
	for _, operation := range []string{"PutBucketTagging", "GetBucketTagging", "DeleteBucketTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "missing", "TagSet": []any{}}, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		characterization[operation+"MissingBucket"] = fault.Code
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "bucket"}, nil)
	if asFault(t, err).Code != "NoSuchTagSet" {
		t.Fatalf("untagged bucket = %v", err)
	}
	characterization["untaggedBucket"] = asFault(t, err).Code
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("body"))
	valid := []any{map[string]any{"Key": "team α", "Value": ""}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": valid}, nil)

	tags := func(count int) []any {
		out := make([]any, count)
		for i := range out {
			out[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "value"}
		}
		return out
	}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": tags(10)}, nil)
	mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "bucket", "TagSet": tags(50)}, nil)
	characterization["acceptedObjectTags"] = 10
	characterization["acceptedBucketTags"] = 50
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": valid}, nil)
	mustInvoke(t, p, "DeleteBucketTagging", map[string]any{"Bucket": "bucket"}, nil)
	for _, test := range []struct {
		name string
		set  any
		code string
	}{
		{"missing-tag-set", nil, "MalformedXML"},
		{"missing-value", []any{map[string]any{"Key": "key"}}, "MalformedXML"},
		{"duplicate-key", []any{map[string]any{"Key": "key", "Value": "one"}, map[string]any{"Key": "key", "Value": "two"}}, "InvalidTag"},
		{"reserved-key", []any{map[string]any{"Key": "aws:team", "Value": "one"}}, "InvalidTag"},
		{"empty-key", []any{map[string]any{"Key": "", "Value": "one"}}, "InvalidTag"},
		{"long-key", []any{map[string]any{"Key": strings.Repeat("k", 129), "Value": "one"}}, "InvalidTag"},
		{"invalid-key", []any{map[string]any{"Key": "team?", "Value": "one"}}, "InvalidTag"},
		{"long-value", []any{map[string]any{"Key": "team", "Value": strings.Repeat("v", 257)}}, "InvalidTag"},
		{"too-many-object-tags", tags(11), "InvalidTag"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": test.set}, nil)
			fault := asFault(t, err)
			if fault.Code != test.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
			characterization[test.name] = fault.Code
			got := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
			if set := asSliceForTest(got.Output["TagSet"]); len(set) != 1 || asMapForTest(set[0])["Key"] != "team α" {
				t.Fatalf("rejected write changed tags: %#v", got.Output)
			}
		})
	}
	if _, err := invoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "bucket", "TagSet": tags(51)}, nil); asFault(t, err).Code != "InvalidTag" {
		t.Fatalf("too many bucket tags = %v", err)
	}
	for _, operation := range []string{"PutObject", "CreateMultipartUpload"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": operation, "Tagging": "key=one&key=two"}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s duplicate header = %#v", operation, fault)
		}
		characterization[operation+"DuplicateHeader"] = fault.Code
	}
	headerTags := url.Values{}
	for i := range 11 {
		headerTags.Set(fmt.Sprintf("key%d", i), "value")
	}
	rejectedKeys := []string{"PutObject", "copy"}
	for _, test := range []struct{ name, tagging string }{
		{"malformed-header", "%zz=value"},
		{"invalid-header-key", "team%3F=value"},
		{"too-many-header-tags", headerTags.Encode()},
	} {
		key := "rejected-" + test.name
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key, "Tagging": test.tagging}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s = %#v", test.name, fault)
		}
		characterization[test.name] = fault.Code
		rejectedKeys = append(rejectedKeys, key)
	}
	_, err = invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/source", "TaggingDirective": "REPLACE", "Tagging": "key=one&key=two"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("copy duplicate header = %#v", fault)
	}
	for _, key := range rejectedKeys {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("rejected %s created object: %v", key, err)
		}
	}
	characterization["storedTags"] = mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil).Output["TagSet"]
	golden.AssertJSON(t, characterization)
}

func TestCopyObjectConditions(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("source"))
	_ = deps.Clock.Advance(2 * time.Second)
	etag := source.Headers.Get("ETag")
	copyObject := func(key string, input map[string]any, headers map[string]string) (*spi.Response, error) {
		t.Helper()
		in := map[string]any{"Bucket": "bucket", "Key": key, "CopySource": "bucket/source"}
		for name, value := range input {
			in[name] = value
		}
		httpReq := httptest.NewRequest(http.MethodPut, "/bucket/"+key, nil)
		httpReq.Header.Set("x-amz-copy-source", "bucket/source")
		for name, value := range headers {
			httpReq.Header.Set(name, value)
		}
		return p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Input: in, Identity: ident(), HTTP: httpReq})
	}
	wantPrecondition := func(err error) {
		t.Helper()
		fault := asFault(t, err)
		if fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
			t.Fatalf("fault = %#v", fault)
		}
	}

	_, err := copyObject("wrong-etag", nil, map[string]string{"x-amz-copy-source-if-match": `"wrong"`})
	wantPrecondition(err)
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "wrong-etag"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("failed copy wrote destination")
	}

	before := time.Unix(-1, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(1, 0).UTC().Format(http.TimeFormat)
	farFuture := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	if _, err := copyObject("matched", nil, map[string]string{
		"x-amz-copy-source-if-match":            etag,
		"x-amz-copy-source-if-unmodified-since": before,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = copyObject("none-matched", nil, map[string]string{
		"x-amz-copy-source-if-none-match":     etag,
		"x-amz-copy-source-if-modified-since": before,
	})
	wantPrecondition(err)
	if _, err := copyObject("future-modified", nil, map[string]string{"x-amz-copy-source-if-modified-since": farFuture}); err != nil {
		t.Fatal(err)
	}
	_, err = copyObject("match-future-modified", nil, map[string]string{
		"x-amz-copy-source-if-match":          etag,
		"x-amz-copy-source-if-modified-since": farFuture,
	})
	wantPrecondition(err)
	_, err = copyObject("none-mismatch-modified", nil, map[string]string{
		"x-amz-copy-source-if-none-match":     `"wrong"`,
		"x-amz-copy-source-if-modified-since": after,
	})
	wantPrecondition(err)
	if _, err := copyObject("match-unmodified", nil, map[string]string{
		"x-amz-copy-source-if-match":            etag,
		"x-amz-copy-source-if-none-match":       etag,
		"x-amz-copy-source-if-modified-since":   before,
		"x-amz-copy-source-if-unmodified-since": before,
	}); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name, header, value string
		ok                  bool
	}{
		{"modified", "x-amz-copy-source-if-modified-since", before, true},
		{"not-modified", "x-amz-copy-source-if-modified-since", after, false},
		{"unmodified", "x-amz-copy-source-if-unmodified-since", after, true},
		{"changed", "x-amz-copy-source-if-unmodified-since", before, false},
	} {
		_, err := copyObject(test.name, nil, map[string]string{test.header: test.value})
		if test.ok && err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if !test.ok {
			wantPrecondition(err)
		}
	}

	destination := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, []byte("old"))
	_, err = copyObject("destination", map[string]any{"IfNoneMatch": "*"}, nil)
	wantPrecondition(err)
	_, err = copyObject("destination", map[string]any{"IfMatch": `"wrong"`}, nil)
	wantPrecondition(err)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, nil)); string(got) != "old" {
		t.Fatalf("failed condition replaced destination with %q", got)
	}
	if _, err := copyObject("destination", map[string]any{"IfMatch": destination.Headers.Get("ETag")}, nil); err != nil {
		t.Fatal(err)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, nil)); string(got) != "source" {
		t.Fatalf("conditional copy = %q", got)
	}
}

func TestCopySourcePreconditionsCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "copy-conditions"}, nil)
	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "copy-conditions", "Key": "source"}, []byte("source"))
	_ = deps.Clock.Advance(2 * time.Second)
	etag := put.Headers.Get("ETag")
	past := time.Unix(-1, 0).UTC().Format(http.TimeFormat)
	modified := time.Unix(0, 0).UTC().Format(http.TimeFormat)
	future := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat)
	cases := map[string]map[string]any{
		"if-match":              {"CopySourceIfMatch": `"wrong"`},
		"if-unmodified-since":   {"CopySourceIfUnmodifiedSince": past},
		"if-none-match":         {"CopySourceIfNoneMatch": etag},
		"if-modified-since":     {"CopySourceIfModifiedSince": modified},
		"future-modified-since": {"CopySourceIfModifiedSince": future},
		"all-positive": {"CopySourceIfMatch": etag, "CopySourceIfNoneMatch": `"wrong"`,
			"CopySourceIfModifiedSince": past, "CopySourceIfUnmodifiedSince": modified},
	}
	outcomes := map[string]string{}
	for name, conditions := range cases {
		input := map[string]any{"Bucket": "copy-conditions", "Key": name, "CopySource": "copy-conditions/source"}
		for key, value := range conditions {
			input[key] = value
		}
		_, err := invoke(t, p, "CopyObject", input, nil)
		if err == nil {
			outcomes[name] = "success"
		} else {
			outcomes[name] = asFault(t, err).Code
		}
	}
	golden.AssertJSON(t, outcomes)
}

func TestObjectReadConditions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "conditional"}, []byte("body"))
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "conditional"}, nil)
	modified, err := http.ParseTime(head.Headers.Get("Last-Modified"))
	if err != nil {
		t.Fatal(err)
	}
	past, future := modified.Add(-time.Hour).Format(http.TimeFormat), modified.Add(time.Hour).Format(http.TimeFormat)
	etag := put.Headers.Get("ETag")
	call := func(operation string, conditions map[string]any) (*spi.Response, error) {
		t.Helper()
		input := map[string]any{"Bucket": "bucket", "Key": "conditional"}
		if operation == "GetObjectAttributes" {
			input["ObjectAttributes"] = []string{"ETag"}
		}
		for key, value := range conditions {
			input[key] = value
		}
		response, err := invoke(t, p, operation, input, nil)
		if response != nil && response.Stream != nil {
			_ = response.Stream.Close()
		}
		return response, err
	}
	for _, operation := range []string{"GetObject", "HeadObject", "GetObjectAttributes"} {
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong"`},
			{"IfUnmodifiedSince": past},
		} {
			_, err := call(operation, conditions)
			if fault := asFault(t, err); fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
				t.Fatalf("%s %#v fault = %#v", operation, conditions, fault)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfNoneMatch": etag},
			{"IfNoneMatch": "*"},
			{"IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status != http.StatusNotModified {
				t.Fatalf("%s %#v = %#v %v", operation, conditions, response, err)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong", ` + etag, "IfUnmodifiedSince": past},
			{"IfNoneMatch": `"wrong"`, "IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status == http.StatusNotModified {
				t.Fatalf("%s precedence %#v: %#v %v", operation, conditions, response, err)
			}
		}
	}
}

func TestCopyObjectSourceVersions(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	key := "reports/a b+c?.json"
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	_ = deps.Clock.Advance(time.Second)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key}, []byte("second"))
	versioned := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key, "VersionId": firstVersion}, nil)
	if versioned.Headers.Get("ETag") != first.Headers.Get("ETag") || versioned.Headers.Get("x-amz-version-id") != firstVersion || string(readStream(t, versioned)) != "first" {
		t.Fatalf("versioned get headers = %v", versioned.Headers)
	}
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": key, "VersionId": firstVersion}, nil); head.Headers.Get("ETag") != first.Headers.Get("ETag") || head.Headers.Get("x-amz-version-id") != firstVersion || head.Headers.Get("Content-Length") != "5" {
		t.Fatalf("versioned head headers = %v", head.Headers)
	}
	source := "bucket/" + url.PathEscape(key)

	copyVersion := mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "version-copy", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	if got := copyVersion.Headers.Get("x-amz-copy-source-version-id"); got != firstVersion {
		t.Fatalf("source version header = %q want %q", got, firstVersion)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "version-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version copy = %q", got)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "current-copy", "CopySource": source}, nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "current-copy"}, nil)); string(got) != "second" {
		t.Fatalf("current copy = %q", got)
	}

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "part-copy"}, nil)
	uploadID := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "part-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version part copy = %q", got)
	}

	deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": key}, nil)
	markerVersion := deleted.Headers.Get("x-amz-version-id")
	for _, operation := range []string{"GetObject", "HeadObject"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": key}, nil)
		if fault := asFault(t, err); fault.HTTPStatus != http.StatusNotFound || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s current marker fault = %#v", operation, fault)
		}
		_, err = invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": key, "VersionId": markerVersion}, nil)
		if fault := asFault(t, err); fault.Code != "MethodNotAllowed" || fault.HTTPStatus != http.StatusMethodNotAllowed || fault.Headers.Get("Last-Modified") == "" || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s explicit marker fault = %#v", operation, fault)
		}
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "deleted", "CopySource": source}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("copied current delete marker")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "restored", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	for _, invalid := range []struct {
		source, code string
	}{
		{"bucket/bad%zz", "InvalidArgument"},
		{source + "?versionId=missing", "NoSuchKey"},
		{source + "?versionId=", "InvalidArgument"},
		{source + "?versionId=" + markerVersion, "InvalidRequest"},
	} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "invalid", "CopySource": invalid.source}, nil)
		if fault := asFault(t, err); fault.Code != invalid.code {
			t.Fatalf("%q fault = %#v", invalid.source, fault)
		}
	}
}

func TestDeleteObjectRestoresPreviousVersion(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=first"}, []byte("first"))
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=second"}, []byte("second"))
	third := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=third"}, []byte("third"))

	deletedSecond := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": second.Headers.Get("x-amz-version-id")}, nil)
	if deletedSecond.Headers.Get("x-amz-version-id") != second.Headers.Get("x-amz-version-id") || string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil))) != "third" {
		t.Fatalf("noncurrent delete = %#v", deletedSecond)
	}

	deletedThird := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": third.Headers.Get("x-amz-version-id")}, nil)
	restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	restoredBody := string(readStream(t, restored))
	if deletedThird.Headers.Get("x-amz-version-id") != third.Headers.Get("x-amz-version-id") || restored.Headers.Get("x-amz-version-id") != first.Headers.Get("x-amz-version-id") || restoredBody != "first" {
		t.Fatalf("restored object = %#v", restored)
	}
	tags := asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "key"}, nil).Output["TagSet"])
	if len(tags) != 1 || tags[0].(map[string]any)["Value"] != "first" {
		t.Fatalf("restored tags = %#v", tags)
	}

	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	deletedMarker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": marker.Headers.Get("x-amz-version-id")}, nil)
	markerRestoredBody := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)))
	if deletedMarker.Headers.Get("x-amz-delete-marker") != "true" || deletedMarker.Headers.Get("x-amz-version-id") != marker.Headers.Get("x-amz-version-id") || markerRestoredBody != "first" {
		t.Fatalf("delete marker restore = %#v", deletedMarker)
	}

	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": "missing"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "versionId" {
		t.Fatalf("missing version fault = %#v", fault)
	}
	golden.AssertJSON(t, map[string]any{
		"deletedNoncurrentVersion": deletedSecond.Headers.Get("x-amz-version-id"),
		"deletedCurrentVersion":    deletedThird.Headers.Get("x-amz-version-id"),
		"restoredVersion":          restored.Headers.Get("x-amz-version-id"),
		"restoredBody":             restoredBody,
		"restoredTags":             tags,
		"deletedMarker":            deletedMarker.Headers.Get("x-amz-delete-marker"),
		"deletedMarkerVersion":     deletedMarker.Headers.Get("x-amz-version-id"),
		"markerRestoredBody":       markerRestoredBody,
	})
}

func TestSuspendedVersioningReplacesNullVersion(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("unversioned"))
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	enabled := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("enabled"))
	enabledVersion := enabled.Headers.Get("x-amz-version-id")

	beforeSuspension := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": "null"}, nil)
	if body := string(readStream(t, beforeSuspension)); body != "unversioned" {
		t.Fatalf("converted null version = %q", body)
	}
	converted := asSliceForTest(mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "bucket"}, nil).Output["Versions"])
	if len(converted) != 2 || asMapForTest(converted[0])["VersionId"] != enabledVersion || asMapForTest(converted[0])["IsLatest"] != true || asMapForTest(converted[1])["VersionId"] != "null" || asMapForTest(converted[1])["IsLatest"] != false {
		t.Fatalf("converted versions = %#v", converted)
	}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Suspended"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("first null"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("second null"))

	listed := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "bucket"}, nil).Output
	suspendedVersions := listed
	versions := asSliceForTest(listed["Versions"])
	if len(versions) != 2 || asMapForTest(versions[0])["VersionId"] != "null" || asMapForTest(versions[1])["VersionId"] != enabledVersion {
		t.Fatalf("suspended versions = %#v", listed)
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": "null"}, nil))); body != "second null" {
		t.Fatalf("replacement null version = %q", body)
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": enabledVersion}, nil))); body != "enabled" {
		t.Fatalf("preserved enabled version = %q", body)
	}

	deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	if deleted.Headers.Get("x-amz-delete-marker") != "true" || deleted.Headers.Get("x-amz-version-id") != "null" {
		t.Fatalf("suspended delete = %#v", deleted.Headers)
	}
	listed = mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "bucket"}, nil).Output
	if markers := asSliceForTest(listed["DeleteMarkers"]); len(markers) != 1 || asMapForTest(markers[0])["VersionId"] != "null" {
		t.Fatalf("suspended delete marker = %#v", listed)
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": "null"}, nil)
	restoredBody := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)))
	if restoredBody != "enabled" {
		t.Fatalf("restored enabled version = %q", restoredBody)
	}
	golden.AssertJSON(t, map[string]any{"suspended": suspendedVersions, "deleted": listed, "restoredBody": restoredBody})
}

func TestDeleteObjectDirectoryPreconditionsAreNotImplemented(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("body"))

	var faults []any
	for _, tc := range []struct {
		name, header, value string
	}{
		{"etag", "If-Match", `"841a2d689ad86bd1611447453c22c6fc"`},
		{"size", "x-amz-if-match-size", "4"},
		{"modified", "x-amz-if-match-last-modified-time", "Sun, 06 Nov 1994 08:49:37 GMT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpRequest := httptest.NewRequest(http.MethodDelete, "http://127.0.0.1/bucket/key", nil)
			httpRequest.Header.Set(tc.header, tc.value)
			_, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "DeleteObject", Input: map[string]any{"Bucket": "bucket", "Key": "key"}, HTTP: httpRequest})
			fault := asFault(t, err)
			if fault.Code != "NotImplemented" || fault.Message != "A header you provided implies functionality that is not implemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fields["Header"] != tc.header {
				t.Fatalf("fault = %#v", fault)
			}
			faults = append(faults, map[string]any{"name": tc.name, "code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "header": fault.Fields["Header"]})
			if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil))); body != "body" {
				t.Fatalf("object changed after rejected delete: %q", body)
			}
		})
	}
	golden.AssertJSON(t, faults)
}

func TestDeleteObjectMissingKeyVersionIsIdempotent(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	var results []any
	for _, tc := range []struct{ status, version string }{{"Enabled", "missing-version"}, {"Suspended", "null"}} {
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": tc.status}, nil)
		deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "missing", "VersionId": tc.version}, nil)
		if deleted.Status != http.StatusNoContent || len(deleted.Headers) != 0 {
			t.Fatalf("%s missing version delete = %#v", tc.status, deleted)
		}
		results = append(results, map[string]any{"status": tc.status, "version": tc.version, "httpStatus": deleted.Status, "headers": deleted.Headers})
	}
	golden.AssertJSON(t, results)
}

func TestDeleteObjectRejectsVersionOnUnversionedMissingKey(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "missing", "VersionId": "missing-version"}, nil)
	fault := asFault(t, err)
	if fault.Code != "InvalidArgument" || fault.Message != "Invalid version id specified" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "versionId" || fault.Fields["ArgumentValue"] != "missing-version" {
		t.Fatalf("fault = %#v", fault)
	}
	deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "missing", "VersionId": "null"}, nil)
	if deleted.Status != http.StatusNoContent || len(deleted.Headers) != 0 {
		t.Fatalf("null version delete = %#v", deleted)
	}
	golden.AssertJSON(t, map[string]any{"invalid": map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}, "null": map[string]any{"status": deleted.Status, "headers": deleted.Headers}})
}

func TestDeleteObjectsVersionAndQuietSemantics(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("first"))
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("second"))

	deleted := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{
		map[string]any{"Key": "key", "VersionId": second.Headers.Get("x-amz-version-id")},
	}}, nil)
	item := deleted.Output["Deleted"].([]any)[0].(map[string]any)
	if item["VersionId"] != second.Headers.Get("x-amz-version-id") || item["DeleteMarker"] != nil {
		t.Fatalf("deleted version %#v", item)
	}
	restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	restoredBody := string(readStream(t, restored))
	if restored.Headers.Get("x-amz-version-id") != first.Headers.Get("x-amz-version-id") || restoredBody != "first" {
		t.Fatalf("restored version %#v", restored)
	}

	quietVersion := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "quiet"}, []byte("quiet")).Headers.Get("x-amz-version-id")
	result := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Quiet": true, "Objects": []any{
		map[string]any{"Key": "key", "VersionId": "missing"},
		map[string]any{"Key": "quiet", "VersionId": quietVersion},
	}}, nil)
	if result.Output["Deleted"] != nil {
		t.Fatalf("quiet response %#v", result.Output)
	}
	failure := result.Output["Errors"].([]any)[0].(map[string]any)
	if failure["Code"] != "NoSuchVersion" || failure["VersionId"] != "missing" {
		t.Fatalf("failure %#v", failure)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "quiet", "VersionId": quietVersion}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("quiet delete did not run: %v", err)
	}
	_, err := invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{}}, nil)
	emptyFault := asFault(t, err)
	if emptyFault.Code != "MalformedXML" {
		t.Fatalf("empty delete: %v", err)
	}
	objects := make([]any, 1001)
	for index := range objects {
		objects[index] = map[string]any{"Key": fmt.Sprintf("limit-%d", index)}
	}
	if _, err := invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Quiet": true, "Objects": objects[:1000]}, nil); err != nil {
		t.Fatalf("maximum delete: %v", err)
	}
	_, err = invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": objects}, nil)
	oversizedFault := asFault(t, err)
	if oversizedFault.Code != "MalformedXML" {
		t.Fatalf("oversized delete: %v", err)
	}
	checksumBody := []byte("<Delete><Object><Key>checksum</Key></Object></Delete>")
	checksumDigest := md5.Sum(checksumBody)
	checksumFault := func(contentMD5, algorithm string) string {
		httpReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/bucket?delete", bytes.NewReader(checksumBody))
		if contentMD5 != "" {
			httpReq.Header.Set("Content-MD5", contentMD5)
		}
		if algorithm != "" {
			httpReq.Header.Set("x-amz-sdk-checksum-algorithm", algorithm)
		}
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "DeleteObjects", Input: map[string]any{
			"Bucket": "bucket", "Objects": []any{map[string]any{"Key": "checksum"}}, "_body": string(checksumBody),
		}, HTTP: httpReq})
		return asFault(t, err).Code
	}
	golden.AssertJSON(t, map[string]any{
		"verbose":      deleted.Output,
		"quiet":        result.Output,
		"restoredBody": restoredBody,
		"empty":        emptyFault.Code,
		"oversized":    oversizedFault.Code,
		"checksums": map[string]any{
			"missing":                 checksumFault("", ""),
			"mismatched":              checksumFault("AA==", ""),
			"algorithm without value": checksumFault(base64.StdEncoding.EncodeToString(checksumDigest[:]), "CRC32"),
		},
	})
}

func TestObjectLockPreventsPermanentDeletion(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket", "ObjectLockEnabledForBucket": true}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("first")).Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("second")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "LegalHold": map[string]any{"Status": "ON"}}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": second}, nil)
	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first}, nil)
	legalHoldFault := asFault(t, err)
	if legalHoldFault.Code != "AccessDenied" {
		t.Fatalf("legal hold delete: %v", err)
	}
	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	if marker.Headers.Get("x-amz-delete-marker") != "true" {
		t.Fatalf("simple delete did not create marker: %#v", marker.Headers)
	}

	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "LegalHold": map[string]any{"Status": "OFF"}}, nil)
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "Retention": map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "9999-01-01T00:00:00Z"}}, nil)
	locked := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{map[string]any{"Key": "key", "VersionId": first}}}, nil)
	if failure := locked.Output["Errors"].([]any)[0].(map[string]any); failure["Code"] != "AccessDenied" {
		t.Fatalf("governance delete: %#v", failure)
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "BypassGovernanceRetention": true}, nil)

	compliance := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "compliance"}, []byte("locked")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "compliance", "VersionId": compliance, "Retention": map[string]any{"Mode": "COMPLIANCE", "RetainUntilDate": "9999-01-01T00:00:00Z"}}, nil)
	_, err = invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "compliance", "VersionId": compliance, "BypassGovernanceRetention": true}, nil)
	complianceFault := asFault(t, err)
	if complianceFault.Code != "AccessDenied" {
		t.Fatalf("compliance bypass: %v", err)
	}

	expired := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "expired"}, []byte("expired")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "expired", "VersionId": expired, "Retention": map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "1960-01-01T00:00:00Z"}}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "expired", "VersionId": expired}, nil)

	golden.AssertJSON(t, map[string]any{
		"legalHold":            legalHoldFault.Code,
		"otherVersion":         "deleted",
		"simpleDeleteMarker":   marker.Headers.Get("x-amz-delete-marker"),
		"governance":           locked.Output,
		"governanceWithBypass": "deleted",
		"complianceWithBypass": complianceFault.Code,
		"expired":              "deleted",
	})
}

func TestObjectLockAppliesRetentionOnWrite(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket", "ObjectLockEnabledForBucket": true}, nil)
	mustInvoke(t, p, "PutObjectLockConfiguration", map[string]any{"Bucket": "bucket", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": 2}}}}, nil)

	version := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "default"}, []byte("locked")).Headers.Get("x-amz-version-id")
	retention := mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "bucket", "Key": "default", "VersionId": version}, nil)
	if got := asMapForTest(retention.Output["Retention"]); got["Mode"] != "GOVERNANCE" || got["RetainUntilDate"] != "1970-01-03T00:00:00Z" {
		t.Fatalf("default retention: %#v", retention.Output)
	}
	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "default", "VersionId": version}, nil)
	if fault := asFault(t, err); fault.Code != "AccessDenied" {
		t.Fatalf("default retention delete: %v", err)
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "default", "VersionId": version, "BypassGovernanceRetention": true}, nil)
	mustInvoke(t, p, "PutObjectLockConfiguration", map[string]any{"Bucket": "bucket", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "COMPLIANCE", "Years": 1}}}}, nil)
	yearVersion := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "year"}, []byte("locked")).Headers.Get("x-amz-version-id")
	yearRetention := mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "bucket", "Key": "year", "VersionId": yearVersion}, nil)
	if got := asMapForTest(yearRetention.Output["Retention"]); got["Mode"] != "COMPLIANCE" || got["RetainUntilDate"] != "1971-01-01T00:00:00Z" {
		t.Fatalf("year retention: %#v", yearRetention.Output)
	}
	copyVersion := mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/year"}, nil).Headers.Get("x-amz-version-id")
	copyRetention := mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "bucket", "Key": "copy", "VersionId": copyVersion}, nil)
	if got := asMapForTest(copyRetention.Output["Retention"]); got["Mode"] != "COMPLIANCE" || got["RetainUntilDate"] != "1971-01-01T00:00:00Z" {
		t.Fatalf("copy retention: %#v", copyRetention.Output)
	}

	explicit := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "explicit", "ObjectLockMode": "COMPLIANCE", "ObjectLockRetainUntilDate": "1970-01-02T00:00:00Z", "ObjectLockLegalHoldStatus": "ON"}, []byte("locked")).Headers.Get("x-amz-version-id")
	explicitRetention := mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "bucket", "Key": "explicit", "VersionId": explicit}, nil)
	if got := asMapForTest(explicitRetention.Output["Retention"]); got["Mode"] != "COMPLIANCE" || got["RetainUntilDate"] != "1970-01-02T00:00:00Z" {
		t.Fatalf("explicit retention: %#v", explicitRetention.Output)
	}
	legalHold := mustInvoke(t, p, "GetObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "explicit", "VersionId": explicit}, nil)
	if got := asMapForTest(legalHold.Output["LegalHold"]); got["Status"] != "ON" {
		t.Fatalf("write legal hold: %#v", legalHold.Output)
	}

	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "invalid", "ObjectLockMode": "GOVERNANCE"}, nil)
	invalidFault := asFault(t, err)
	if invalidFault.Code != "InvalidArgument" {
		t.Fatalf("unpaired retention headers: %v", err)
	}
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "invalid-legal", "ObjectLockLegalHoldStatus": "MAYBE"}, nil)
	invalidLegalFault := asFault(t, err)
	if invalidLegalFault.Code != "InvalidArgument" {
		t.Fatalf("invalid legal hold status: %v", err)
	}
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "invalid-mode", "ObjectLockMode": "INVALID", "ObjectLockRetainUntilDate": "1970-01-02T00:00:00Z"}, nil)
	invalidModeFault := asFault(t, err)
	if invalidModeFault.Code != "InvalidArgument" {
		t.Fatalf("invalid retention mode: %v", err)
	}
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "lowercase-mode", "ObjectLockMode": "governance", "ObjectLockRetainUntilDate": "1970-01-02T00:00:00Z"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("lowercase retention mode: %v", err)
	}
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "past", "ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": "1960-01-01T00:00:00Z"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("past retention deadline: %v", err)
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "plain"}, nil)
	_, err = invoke(t, p, "PutObject", map[string]any{"Bucket": "plain", "Key": "locked", "ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": "1970-01-02T00:00:00Z"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("retention on plain bucket: %v", err)
	}
	golden.AssertJSON(t, map[string]any{"default": retention.Output, "year": yearRetention.Output, "copy": copyRetention.Output, "explicit": explicitRetention.Output, "legalHold": legalHold.Output, "unpaired": invalidFault.Code, "invalidLegalHold": invalidLegalFault.Code, "invalidMode": invalidModeFault.Code})
}

func TestObjectLockCapturesMultipartRetentionAtInitiation(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket", "ObjectLockEnabledForBucket": true}, nil)
	configuration := func(days int) map[string]any {
		return map[string]any{"Bucket": "bucket", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": days}}}}
	}
	mustInvoke(t, p, "PutObjectLockConfiguration", configuration(2), nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart"}, nil).Output["UploadId"].(string)
	if err := deps.Clock.Advance(24 * time.Hour); err != nil {
		t.Fatal(err)
	}
	mustInvoke(t, p, "PutObjectLockConfiguration", configuration(4), nil)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte("part"))
	completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	retention := mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "bucket", "Key": "multipart", "VersionId": completed.Headers.Get("x-amz-version-id")}, nil)
	if got := asMapForTest(retention.Output["Retention"]); got["RetainUntilDate"] != "1970-01-03T00:00:00Z" {
		t.Fatalf("multipart retention: %#v", retention.Output)
	}
}

func TestObjectLockBucketGuards(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "plain"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "plain", "Key": "key"}, []byte("plain"))
	_, err := invoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "plain", "Key": "key", "LegalHold": map[string]any{"Status": "ON"}}, nil)
	legalHoldFault := asFault(t, err)
	if legalHoldFault.Code != "InvalidRequest" {
		t.Fatalf("legal hold without bucket configuration: %v", err)
	}
	_, err = invoke(t, p, "DeleteObject", map[string]any{"Bucket": "plain", "Key": "key", "BypassGovernanceRetention": false}, nil)
	bypassFault := asFault(t, err)
	if bypassFault.Code != "InvalidArgument" {
		t.Fatalf("bypass without bucket configuration: %v", err)
	}
	_, err = invoke(t, p, "PutBucketObjectLockConfiguration", map[string]any{"Bucket": "plain", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled"}}, nil)
	plainConfigurationFault := asFault(t, err)
	if plainConfigurationFault.Code != "InvalidBucketState" {
		t.Fatalf("configure object lock without versioning: %v", err)
	}

	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "locked", "ObjectLockEnabledForBucket": true}, nil)
	versioning := mustInvoke(t, p, "GetBucketVersioning", map[string]any{"Bucket": "locked"}, nil)
	if versioning.Output["Status"] != "Enabled" {
		t.Fatalf("object lock did not enable versioning: %#v", versioning.Output)
	}
	defaultConfiguration := mustInvoke(t, p, "GetBucketObjectLockConfiguration", map[string]any{"Bucket": "locked"}, nil)
	_, err = invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "locked", "Status": "Suspended"}, nil)
	suspendFault := asFault(t, err)
	if suspendFault.Code != "InvalidBucketState" {
		t.Fatalf("suspend object-lock versioning: %v", err)
	}
	regionalName := "locked-123456789012-us-east-1-an"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": regionalName, "BucketNamespace": "account-regional", "ObjectLockEnabledForBucket": true}, nil)
	regionalVersioning := mustInvoke(t, p, "GetBucketVersioning", map[string]any{"Bucket": regionalName}, nil)
	if regionalVersioning.Output["Status"] != "Enabled" {
		t.Fatalf("account-regional object lock did not enable versioning: %#v", regionalVersioning.Output)
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "configured"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "configured", "Status": "Enabled"}, nil)
	_, err = invoke(t, p, "PutBucketObjectLockConfiguration", map[string]any{"Bucket": "configured", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": 1, "Years": 1}}}}, nil)
	invalidConfigurationFault := asFault(t, err)
	if invalidConfigurationFault.Code != "MalformedXML" {
		t.Fatalf("invalid object lock configuration: %v", err)
	}
	_, err = invoke(t, p, "PutBucketObjectLockConfiguration", map[string]any{"Bucket": "configured", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": 0}}}}, nil)
	if fault := asFault(t, err); fault.Code != "MalformedXML" {
		t.Fatalf("zero object lock duration: %v", err)
	}
	mustInvoke(t, p, "PutBucketObjectLockConfiguration", map[string]any{"Bucket": "configured", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": 1}}}}, nil)
	configured := mustInvoke(t, p, "GetBucketObjectLockConfiguration", map[string]any{"Bucket": "configured"}, nil)
	_, err = invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "configured", "Status": "Suspended"}, nil)
	configuredSuspendFault := asFault(t, err)
	if configuredSuspendFault.Code != "InvalidBucketState" {
		t.Fatalf("suspend configured object-lock versioning: %v", err)
	}
	golden.AssertJSON(t, map[string]any{
		"plainLegalHold":       legalHoldFault.Code,
		"plainBypass":          bypassFault.Code,
		"plainConfiguration":   plainConfigurationFault.Code,
		"lockedVersion":        versioning.Output,
		"lockedConfiguration":  defaultConfiguration.Output,
		"lockedSuspend":        suspendFault.Code,
		"regionalVersion":      regionalVersioning.Output,
		"invalidConfiguration": invalidConfigurationFault.Code,
		"configured":           configured.Output,
		"configuredSuspend":    configuredSuspendFault.Code,
	})
}

func TestVersionedObjectTaggingCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source", "Tagging": "stage=first&team=storage"}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("second"))
	secondVersion := second.Headers.Get("x-amz-version-id")

	firstTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil)
	if firstTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(firstTags.Output["TagSet"])) != 2 {
		t.Fatalf("first version tags = %#v headers %v", firstTags.Output, firstTags.Headers)
	}
	if current := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "source"}, nil); current.Headers.Get("x-amz-tagging-count") != "" {
		t.Fatalf("new untagged version inherited tags: %v", current.Headers)
	}
	if old := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil); old.Headers.Get("x-amz-tagging-count") != "2" {
		t.Fatalf("old version tag count = %v", old.Headers)
	} else {
		_ = old.Stream.Close()
	}

	currentTag := []any{map[string]any{"Key": "stage", "Value": "second"}}
	putCurrent := mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": currentTag}, nil)
	if putCurrent.Headers.Get("x-amz-version-id") != secondVersion {
		t.Fatalf("current tag version = %v", putCurrent.Headers)
	}
	explicitTag := []any{map[string]any{"Key": "stage", "Value": "retagged"}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion, "TagSet": explicitTag}, nil)

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source?versionId=" + firstVersion}, nil)
	copiedTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	if tags := asSliceForTest(copiedTags.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "retagged" {
		t.Fatalf("version copy tags = %#v", copiedTags.Output)
	}

	deletedTags := mustInvoke(t, p, "DeleteObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil)
	if deletedTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil).Output["TagSet"])) != 0 {
		t.Fatalf("deleted version tags = %v", deletedTags.Headers)
	}
	current := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
	if tags := asSliceForTest(current.Output["TagSet"]); current.Headers.Get("x-amz-version-id") != secondVersion || len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("current tags changed with old version: %#v headers %v", current.Output, current.Headers)
	}

	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "source"}, nil).Headers.Get("x-amz-version-id")
	retained := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": secondVersion}, nil)
	if tags := asSliceForTest(retained.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("delete marker lost version tags: %#v", retained.Output)
	}
	_, currentErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
	_, markerErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": marker}, nil)
	for _, operation := range []string{"GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": "missing", "TagSet": currentTag}, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchKey" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing object fault = %#v", operation, fault)
		}
	}

	golden.AssertJSON(t, map[string]any{
		"firstVersionTags": firstTags.Output["TagSet"],
		"currentTags":      current.Output["TagSet"],
		"retainedTags":     retained.Output["TagSet"],
		"copiedTags":       copiedTags.Output["TagSet"],
		"currentMarker":    asFault(t, currentErr).Code,
		"explicitMarker":   asFault(t, markerErr).Code,
	})
}

func TestUploadPartCopyConditionsAndRange(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := bytes.Repeat([]byte("0123456789"), 600000)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "large"}, body)
	createUpload := func(key string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key}, nil)
		return response.Output["UploadId"].(string)
	}

	_, err := invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("rejected"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceIfMatch": `"wrong"`,
	}, nil)
	if fault := asFault(t, err); fault.Code != "PreconditionFailed" {
		t.Fatalf("condition fault = %#v", fault)
	}

	uploadID := createUpload("range")
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": "bucket/large",
		"CopySourceIfMatch": source.Headers.Get("ETag"), "CopySourceRange": "bytes=10-19",
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "range"}, nil)); string(got) != "0123456789" {
		t.Fatalf("range copy = %q", got)
	}

	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("invalid-range"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceRange": "bytes=7000000-7000001",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("range fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("malformed-range"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceRange": "0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("malformed range fault = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "small"}, []byte("small"))
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("too-small"), "PartNumber": 1, "CopySource": "bucket/small", "CopySourceRange": "bytes=0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("small range fault = %#v", fault)
	}
}

func TestListObjectsV2Prefix(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "a/1", "StorageClass": "STANDARD_IA"}, []byte("1"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "a/2"}, []byte("2"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "z/9"}, []byte("9"))
	resp := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "bucket", "Prefix": "a/"}, nil)
	contents, _ := resp.Output["Contents"].([]any)
	keys := map[string]bool{}
	for _, item := range contents {
		m, _ := item.(map[string]any)
		keys[m["Key"].(string)] = true
		if _, err := time.Parse(time.RFC3339, m["LastModified"].(string)); err != nil || m["StorageClass"] == "" || m["Key"] == "a/1" && m["StorageClass"] != "STANDARD_IA" {
			t.Fatalf("object metadata: %#v", m)
		}
	}
	if !keys["a/1"] || !keys["a/2"] || keys["z/9"] || len(keys) != 2 {
		t.Fatalf("prefix list: %v", keys)
	}
}

func TestListObjectOwnerIdentityCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-owner"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-owner", "Key": "key"}, []byte("body"))
	owner := func(output map[string]any) map[string]any {
		t.Helper()
		contents := output["Contents"].([]any)
		return asMapForTest(asMapForTest(contents[0])["Owner"])
	}
	v1 := owner(mustInvoke(t, p, "ListObjects", map[string]any{"Bucket": "list-owner"}, nil).Output)
	if v1["ID"] != "123456789012" || v1["DisplayName"] != nil {
		t.Fatalf("ListObjects owner = %#v", v1)
	}
	v2 := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "list-owner"}, nil).Output["Contents"].([]any)[0].(map[string]any)
	if v2["Owner"] != nil {
		t.Fatalf("ListObjectsV2 default owner = %#v", v2["Owner"])
	}
	v2Owner := owner(mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "list-owner", "FetchOwner": true}, nil).Output)
	if v2Owner["ID"] != "123456789012" || v2Owner["DisplayName"] != nil {
		t.Fatalf("ListObjectsV2 requested owner = %#v", v2Owner)
	}
	golden.AssertJSON(t, map[string]any{"v1": v1, "v2Default": v2["Owner"], "v2FetchOwner": v2Owner})
}

func TestListObjectsBucketRegionCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "east"}, nil)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "west", "LocationConstraint": "us-west-2"}, nil)
	for _, bucket := range []string{"east", "west"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": "key"}, []byte("body"))
	}
	characterization := map[string]any{}
	for _, operation := range []string{"ListObjects", "ListObjectsV2"} {
		west := mustInvoke(t, p, operation, map[string]any{"Bucket": "west"}, nil).Output["BucketRegion"]
		if west != "us-west-2" {
			t.Fatalf("%s west BucketRegion = %#v", operation, west)
		}
		east := mustInvoke(t, p, operation, map[string]any{"Bucket": "east"}, nil).Output["BucketRegion"]
		if east != nil {
			t.Fatalf("%s east BucketRegion = %#v", operation, east)
		}
		characterization[operation] = map[string]any{"east": east, "west": west}
	}
	golden.AssertJSON(t, characterization)
}

func TestListObjectChecksumMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-checksums"}, nil)
	body := []byte("checksummed")
	sum := sha256.Sum256(body)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-checksums", "Key": "checksummed", "ChecksumSHA256": base64.StdEncoding.EncodeToString(sum[:])}, body)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-checksums", "Key": "plain"}, body)
	characterization := map[string]any{}
	for _, operation := range []string{"ListObjects", "ListObjectsV2"} {
		contents := mustInvoke(t, p, operation, map[string]any{"Bucket": "list-checksums"}, nil).Output["Contents"].([]any)
		checksummed, plain := asMapForTest(contents[0]), asMapForTest(contents[1])
		if !reflect.DeepEqual(checksummed["ChecksumAlgorithm"], []any{"SHA256"}) || checksummed["ChecksumType"] != "FULL_OBJECT" {
			t.Fatalf("%s checksummed object = %#v", operation, checksummed)
		}
		if plain["ChecksumAlgorithm"] != nil || plain["ChecksumType"] != nil {
			t.Fatalf("%s plain object = %#v", operation, plain)
		}
		characterization[operation] = []any{
			map[string]any{"key": checksummed["Key"], "algorithm": checksummed["ChecksumAlgorithm"], "type": checksummed["ChecksumType"]},
			map[string]any{"key": plain["Key"], "algorithm": plain["ChecksumAlgorithm"], "type": plain["ChecksumType"]},
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestListEncodingTypeValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-encoding"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "list-encoding", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-encoding", "Key": "versioned"}, []byte("body"))
	for _, operation := range []string{"ListObjects", "ListObjectsV2", "ListObjectVersions", "ListMultipartUploads"} {
		t.Run(operation, func(t *testing.T) {
			for _, value := range []string{"value", "", "URL"} {
				_, err := invoke(t, p, operation, map[string]any{"Bucket": "list-encoding", "EncodingType": value}, nil)
				fault := asFault(t, err)
				if fault.Code != "InvalidArgument" || fault.Message != "Invalid Encoding Method specified in Request" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "encoding-type" || fault.Fields["ArgumentValue"] != value {
					t.Fatalf("encoding %q fault = %#v", value, fault)
				}
			}
			for _, input := range []map[string]any{{"Bucket": "list-encoding"}, {"Bucket": "list-encoding", "EncodingType": "url"}} {
				if _, err := invoke(t, p, operation, input, nil); err != nil {
					t.Fatalf("valid encoding %#v: %v", input, err)
				}
			}
		})
	}
	_, err := invoke(t, p, "ListObjects", map[string]any{"Bucket": "list-encoding", "encoding-type": "value"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("lowercase input fault = %#v", fault)
	}
	request := httptest.NewRequest(http.MethodGet, "https://list-encoding.s3.us-east-1.amazonaws.com/?list-type=2&encoding-type=", nil)
	_, err = p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "ListObjectsV2", Input: map[string]any{}, Identity: ident(), HTTP: request})
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Fields["ArgumentValue"] != "" {
		t.Fatalf("empty query fault = %#v", fault)
	}
}

func TestListEncodingTypeCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-encoding-golden"}, nil)
	got := map[string]any{}
	for _, operation := range []string{"ListObjects", "ListObjectsV2", "ListObjectVersions", "ListMultipartUploads"} {
		for _, value := range []string{"value", ""} {
			_, err := invoke(t, p, operation, map[string]any{"Bucket": "list-encoding-golden", "EncodingType": value}, nil)
			fault := asFault(t, err)
			got[operation+":"+value] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
		}
	}
	golden.AssertJSON(t, got)
}

func TestListURLResponseEncoding(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-url"}, nil)
	for _, key := range []string{"folder/a b/file+one", "folder/root ?"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-url", "Key": key}, []byte(key))
	}
	first := mustInvoke(t, p, "ListObjects", map[string]any{"Bucket": "list-url", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": 1, "EncodingType": "url"}, nil).Output
	prefixes := asSliceForTest(first["CommonPrefixes"])
	if len(prefixes) != 1 || asMapForTest(prefixes[0])["Prefix"] != "folder/a%20b/" || first["NextMarker"] != "folder/a%20b/" || first["Prefix"] != "folder/" || first["Delimiter"] != "/" {
		t.Fatalf("encoded V1 page = %#v", first)
	}
	v2 := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "list-url", "Prefix": "folder/a b/", "StartAfter": "folder/a b/", "EncodingType": "url"}, nil).Output
	contents := asSliceForTest(v2["Contents"])
	if len(contents) != 1 || asMapForTest(contents[0])["Key"] != "folder/a%20b/file%2Bone" || v2["Prefix"] != "folder/a%20b/" || v2["StartAfter"] != "folder/a%20b/" || v2["EncodingType"] != "url" {
		t.Fatalf("encoded V2 page = %#v", v2)
	}
	golden.AssertJSON(t, map[string]any{"v1": first, "v2": v2})
}

func TestListObjectVersionsURLResponseEncoding(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-url"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-url", "Status": "Enabled"}, nil)
	for _, key := range []string{"folder/a b/file+one", "folder/root ?"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-url", "Key": key}, []byte(key))
	}
	first := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-url", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": 1, "EncodingType": "url"}, nil).Output
	if prefixes := asSliceForTest(first["CommonPrefixes"]); len(prefixes) != 1 || asMapForTest(prefixes[0])["Prefix"] != "folder/a%20b/" || first["NextKeyMarker"] != "folder/a%20b/" {
		t.Fatalf("encoded version prefix page = %#v", first)
	}
	versions := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-url", "Prefix": "folder/a b/", "EncodingType": "url"}, nil).Output
	rows := asSliceForTest(versions["Versions"])
	if len(rows) != 1 || asMapForTest(rows[0])["Key"] != "folder/a%20b/file%2Bone" || versions["Prefix"] != "folder/a%20b/" || versions["EncodingType"] != "url" {
		t.Fatalf("encoded versions = %#v", versions)
	}
	golden.AssertJSON(t, map[string]any{"first": first, "versions": versions})
}

func TestListObjectVersionsURLMarkerRoundTrip(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-url-markers"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-url-markers", "Status": "Enabled"}, nil)
	for _, key := range []string{"folder/a key", "folder/a!key"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-url-markers", "Key": key}, []byte(key))
	}
	first := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-url-markers", "Prefix": "folder/", "MaxKeys": 1, "EncodingType": "url"}, nil).Output
	firstRows := asSliceForTest(first["Versions"])
	if len(firstRows) != 1 || asMapForTest(firstRows[0])["Key"] != "folder/a%20key" || first["NextKeyMarker"] != "folder/a%20key" || first["NextVersionIdMarker"] == nil {
		t.Fatalf("first encoded page = %#v", first)
	}
	second := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-url-markers", "Prefix": "folder/", "MaxKeys": 1, "EncodingType": "url", "KeyMarker": first["NextKeyMarker"], "VersionIdMarker": first["NextVersionIdMarker"]}, nil).Output
	secondRows := asSliceForTest(second["Versions"])
	if len(secondRows) != 1 || asMapForTest(secondRows[0])["Key"] != "folder/a%21key" || second["KeyMarker"] != "folder/a%20key" {
		t.Fatalf("second encoded page = %#v", second)
	}
	golden.AssertJSON(t, map[string]any{"first": first, "second": second})
}

func TestListZeroMaxKeysUsesDefault(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-zero-max"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "list-zero-max", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-zero-max", "Key": "key"}, []byte("body"))
	got := map[string]any{}
	for _, tc := range []struct{ operation, collection string }{{"ListObjects", "Contents"}, {"ListObjectsV2", "Contents"}, {"ListObjectVersions", "Versions"}} {
		t.Run(tc.operation, func(t *testing.T) {
			out := mustInvoke(t, p, tc.operation, map[string]any{"Bucket": "list-zero-max", "MaxKeys": 0}, nil).Output
			if out["MaxKeys"] != 1000 || len(asSliceForTest(out[tc.collection])) != 1 || out["IsTruncated"] != false {
				t.Fatalf("zero max keys = %#v", out)
			}
			got[tc.operation] = out
		})
	}
	golden.AssertJSON(t, got)
}

func TestListObjectsPaginationIncludesCommonPrefixes(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-pagination"}, nil)
	for _, key := range []string{"folder/aSubfolder/subFile1", "folder/aSubfolder/subFile2", "folder/file1", "folder/file2"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-pagination", "Key": key}, []byte("content"))
	}
	input := map[string]any{"Bucket": "list-pagination", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": 1}
	first := mustInvoke(t, p, "ListObjects", input, nil).Output
	firstPrefixes := asSliceForTest(first["CommonPrefixes"])
	if len(firstPrefixes) != 1 || asMapForTest(firstPrefixes[0])["Prefix"] != "folder/aSubfolder/" || len(asSliceForTest(first["Contents"])) != 0 || first["NextMarker"] != "folder/aSubfolder/" || first["KeyCount"] != 1 || first["IsTruncated"] != true {
		t.Fatalf("first V1 page = %#v", first)
	}
	secondInput := maps.Clone(input)
	secondInput["Marker"] = first["NextMarker"]
	second := mustInvoke(t, p, "ListObjects", secondInput, nil).Output
	secondContents := asSliceForTest(second["Contents"])
	if len(secondContents) != 1 || asMapForTest(secondContents[0])["Key"] != "folder/file1" || second["NextMarker"] != "folder/file1" || second["Marker"] != "folder/aSubfolder/" {
		t.Fatalf("second V1 page = %#v", second)
	}
	lastInput := maps.Clone(input)
	lastInput["Marker"] = second["NextMarker"]
	last := mustInvoke(t, p, "ListObjects", lastInput, nil).Output
	lastContents := asSliceForTest(last["Contents"])
	if len(lastContents) != 1 || asMapForTest(lastContents[0])["Key"] != "folder/file2" || last["IsTruncated"] != false || last["NextMarker"] != nil {
		t.Fatalf("last V1 page = %#v", last)
	}
	manualInput := maps.Clone(input)
	manualInput["Marker"] = "folder/aSubfolder/subFile1"
	manual := mustInvoke(t, p, "ListObjects", manualInput, nil).Output
	if got := asSliceForTest(manual["Contents"]); len(got) != 1 || asMapForTest(got[0])["Key"] != "folder/file1" {
		t.Fatalf("manual V1 marker = %#v", manual)
	}
	withoutDelimiter := mustInvoke(t, p, "ListObjects", map[string]any{"Bucket": "list-pagination", "MaxKeys": 1}, nil).Output
	if withoutDelimiter["IsTruncated"] != true || withoutDelimiter["NextMarker"] != nil {
		t.Fatalf("V1 page without delimiter = %#v", withoutDelimiter)
	}
	v2First := mustInvoke(t, p, "ListObjectsV2", input, nil).Output
	v2Next := v2First["NextContinuationToken"]
	if v2Next != "Zm9sZGVyL2ZpbGUx" || v2First["KeyCount"] != 1 {
		t.Fatalf("first V2 page = %#v", v2First)
	}
	v2Input := maps.Clone(input)
	v2Input["ContinuationToken"] = v2Next
	v2Second := mustInvoke(t, p, "ListObjectsV2", v2Input, nil).Output
	if got := asSliceForTest(v2Second["Contents"]); len(got) != 1 || asMapForTest(got[0])["Key"] != "folder/file1" || v2Second["ContinuationToken"] != v2Next || v2Second["NextContinuationToken"] != "Zm9sZGVyL2ZpbGUy" {
		t.Fatalf("second V2 page = %#v", v2Second)
	}
	for _, test := range []struct {
		query, token, want string
	}{{"", "NextMarker", "folder/aSubfolder/"}, {"?list-type=2", "NextContinuationToken", "Zm9sZGVyL2ZpbGUx"}} {
		request := httptest.NewRequest(http.MethodGet, "https://list-pagination.s3.us-east-1.amazonaws.com/"+test.query, nil)
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "ListObjectsV2", Input: input, Identity: ident(), HTTP: request})
		if err != nil || response.Output[test.token] != test.want {
			t.Fatalf("route %q = %#v, %v", test.query, response, err)
		}
	}
}

func TestListObjectsPaginationCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-characterization"}, nil)
	for _, key := range []string{"folder/aSubfolder/subFile1", "folder/aSubfolder/subFile2", "folder/file1", "folder/file2"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "list-characterization", "Key": key}, []byte("content"))
	}
	input := map[string]any{"Bucket": "list-characterization", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": 1}
	v1First := mustInvoke(t, p, "ListObjects", input, nil).Output
	v1NextInput := maps.Clone(input)
	v1NextInput["Marker"] = v1First["NextMarker"]
	v2First := mustInvoke(t, p, "ListObjectsV2", input, nil).Output
	v2NextInput := maps.Clone(input)
	v2NextInput["ContinuationToken"] = v2First["NextContinuationToken"]
	golden.AssertJSON(t, map[string]any{
		"v1-first": v1First,
		"v1-next":  mustInvoke(t, p, "ListObjects", v1NextInput, nil).Output,
		"v2-first": v2First,
		"v2-next":  mustInvoke(t, p, "ListObjectsV2", v2NextInput, nil).Output,
	})
}

func TestListObjectsV2OpaqueContinuationTokens(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "opaque-tokens"}, nil)
	for _, key := range []string{"a", "b", "c"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "opaque-tokens", "Key": key}, []byte(key))
	}
	first := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "opaque-tokens", "MaxKeys": 1}, nil).Output
	if contents := asSliceForTest(first["Contents"]); len(contents) != 1 || asMapForTest(contents[0])["Key"] != "a" || first["NextContinuationToken"] != "Yg==" {
		t.Fatalf("first page = %#v", first)
	}
	second := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "opaque-tokens", "MaxKeys": 1, "ContinuationToken": first["NextContinuationToken"]}, nil).Output
	if contents := asSliceForTest(second["Contents"]); len(contents) != 1 || asMapForTest(contents[0])["Key"] != "b" || second["ContinuationToken"] != "Yg==" || second["NextContinuationToken"] != "Yw==" {
		t.Fatalf("second page = %#v", second)
	}
	for _, token := range []string{"", "not-base64"} {
		_, err := invoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "opaque-tokens", "ContinuationToken": token}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.Message != "The continuation token provided is incorrect" || fault.Fields["ArgumentName"] != "continuation-token" {
			t.Fatalf("token %q fault = %#v", token, fault)
		}
	}
}

func TestListObjectVersionsPagination(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-list"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-list", "Status": "Enabled"}, nil)
	for _, key := range []string{"folder/a/one", "folder/a/two", "folder/file1", "folder/file2"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list", "Key": key}, []byte(key))
	}
	for range 5 {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list", "Key": "versions/key"}, []byte("version"))
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "version-list", "Key": "deleted"}, nil)

	input := map[string]any{"Bucket": "version-list", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": 1}
	first := mustInvoke(t, p, "ListObjectVersions", input, nil).Output
	if got := asSliceForTest(first["CommonPrefixes"]); len(got) != 1 || asMapForTest(got[0])["Prefix"] != "folder/a/" || len(asSliceForTest(first["Versions"])) != 0 || first["NextKeyMarker"] != "folder/a/" || first["NextVersionIdMarker"] != nil || first["IsTruncated"] != true {
		t.Fatalf("first prefix page = %#v", first)
	}
	nextInput := maps.Clone(input)
	nextInput["KeyMarker"] = first["NextKeyMarker"]
	next := mustInvoke(t, p, "ListObjectVersions", nextInput, nil).Output
	if got := asSliceForTest(next["Versions"]); len(got) != 1 || asMapForTest(got[0])["Key"] != "folder/file1" || next["NextKeyMarker"] != "folder/file1" {
		t.Fatalf("next prefix page = %#v", next)
	}

	versionInput := map[string]any{"Bucket": "version-list", "Prefix": "versions/", "MaxKeys": 3}
	page := mustInvoke(t, p, "ListObjectVersions", versionInput, nil).Output
	versions := asSliceForTest(page["Versions"])
	if len(versions) != 3 || page["NextKeyMarker"] != "versions/key" || page["NextVersionIdMarker"] == nil || asMapForTest(versions[0])["IsLatest"] != true {
		t.Fatalf("first version page = %#v", page)
	}
	for _, item := range versions {
		row := asMapForTest(item)
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", row["LastModified"].(string)); err != nil || row["StorageClass"] != "STANDARD" || row["Owner"] == nil {
			t.Fatalf("version row = %#v", row)
		}
	}
	pageInput := maps.Clone(versionInput)
	pageInput["KeyMarker"], pageInput["VersionIdMarker"] = page["NextKeyMarker"], page["NextVersionIdMarker"]
	last := mustInvoke(t, p, "ListObjectVersions", pageInput, nil).Output
	if got := asSliceForTest(last["Versions"]); len(got) != 2 || last["IsTruncated"] != false {
		t.Fatalf("last version page = %#v", last)
	}
	keyOnly := maps.Clone(versionInput)
	keyOnly["KeyMarker"], keyOnly["MaxKeys"] = "versions/key", 100
	if got := asSliceForTest(mustInvoke(t, p, "ListObjectVersions", keyOnly, nil).Output["Versions"]); len(got) != 0 {
		t.Fatalf("key-only marker retained versions = %#v", got)
	}
	_, err := invoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list", "VersionIdMarker": "orphan"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "version-id-marker" {
		t.Fatalf("orphan version marker = %#v", fault)
	}
	encoded := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list", "Prefix": "folder/", "EncodingType": "url"}, nil).Output
	if encoded["Prefix"] != "folder/" || encoded["EncodingType"] != "url" {
		t.Fatalf("encoded version list = %#v", encoded)
	}
	all := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list"}, nil).Output
	if len(asSliceForTest(all["DeleteMarkers"])) != 1 {
		t.Fatalf("delete markers = %#v", all)
	}
}

func TestListObjectVersionsResumesAfterDeletedVersionMarker(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "deleted-version-marker"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "deleted-version-marker", "Status": "Enabled"}, nil)
	for range 3 {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "deleted-version-marker", "Key": "key"}, []byte("body"))
	}

	first := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "deleted-version-marker", "Prefix": "key", "MaxKeys": 1}, nil).Output
	marker := first["NextVersionIdMarker"]
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "deleted-version-marker", "Key": "key", "VersionId": marker}, nil)
	resumed := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "deleted-version-marker", "Prefix": "key", "KeyMarker": "key", "VersionIdMarker": marker}, nil).Output
	versions := asSliceForTest(resumed["Versions"])
	if len(versions) != 2 {
		t.Fatalf("versions after deleted marker = %#v", resumed)
	}
	golden.AssertJSON(t, map[string]any{"first": first, "resumed": resumed})
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "deleted-version-marker", "Key": "key", "VersionId": asMapForTest(versions[0])["VersionId"]}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "deleted-version-marker", "Key": "key"}, []byte("newer"))
	chained := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "deleted-version-marker", "Prefix": "key", "KeyMarker": "key", "VersionIdMarker": marker}, nil).Output
	if rows := asSliceForTest(chained["Versions"]); len(rows) != 1 || asMapForTest(rows[0])["VersionId"] != asMapForTest(versions[1])["VersionId"] {
		t.Fatalf("versions after chained deletion = %#v", chained)
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "deleted-version-marker", "Key": "key", "VersionId": asMapForTest(versions[1])["VersionId"]}, nil)
	terminal := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "deleted-version-marker", "Prefix": "key", "KeyMarker": "key", "VersionIdMarker": marker}, nil).Output
	if rows := asSliceForTest(terminal["Versions"]); len(rows) != 0 {
		t.Fatalf("versions after deleting resume target = %#v", terminal)
	}
}

func TestListObjectVersionChecksumMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-checksums"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-checksums", "Status": "Enabled"}, nil)
	body := []byte("checksummed")
	sum := sha256.Sum256(body)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-checksums", "Key": "key", "ChecksumSHA256": base64.StdEncoding.EncodeToString(sum[:])}, body)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-checksums", "Key": "key"}, []byte("plain"))
	versions := asSliceForTest(mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-checksums"}, nil).Output["Versions"])
	if len(versions) != 2 {
		t.Fatalf("versions = %#v", versions)
	}
	withChecksum := 0
	characterization := []any{}
	for _, value := range versions {
		row := asMapForTest(value)
		characterization = append(characterization, map[string]any{"latest": row["IsLatest"], "algorithm": row["ChecksumAlgorithm"], "type": row["ChecksumType"]})
		if row["ChecksumAlgorithm"] == nil {
			if row["ChecksumType"] != nil {
				t.Fatalf("plain version = %#v", row)
			}
			continue
		}
		withChecksum++
		if !reflect.DeepEqual(row["ChecksumAlgorithm"], []any{"SHA256"}) || row["ChecksumType"] != "FULL_OBJECT" {
			t.Fatalf("checksummed version = %#v", row)
		}
	}
	if withChecksum != 1 {
		t.Fatalf("checksummed versions = %d: %#v", withChecksum, versions)
	}
	golden.AssertJSON(t, characterization)
}

func TestListObjectVersionsCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-list-golden"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-list-golden", "Status": "Enabled"}, nil)
	for range 3 {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list-golden", "Key": "prefix/key"}, []byte("body"))
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list-golden", "Key": "prefix/other"}, []byte("body"))
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "version-list-golden", "Key": "prefix/other"}, nil)
	input := map[string]any{"Bucket": "version-list-golden", "Prefix": "prefix/", "MaxKeys": 2}
	first := mustInvoke(t, p, "ListObjectVersions", input, nil).Output
	nextInput := maps.Clone(input)
	nextInput["KeyMarker"], nextInput["VersionIdMarker"] = first["NextKeyMarker"], first["NextVersionIdMarker"]
	golden.AssertJSON(t, map[string]any{"first": first, "next": mustInvoke(t, p, "ListObjectVersions", nextInput, nil).Output})
}

func TestMultipartETagForm(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	if id == "" {
		t.Fatal("missing UploadId")
	}
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("BBB"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second)), nil)
	etag, _ := done.Output["ETag"].(string)
	if !regexp.MustCompile(`^"[0-9a-f]{32}-2"$`).MatchString(etag) {
		t.Fatalf("multipart etag form: %q", etag)
	}
	object := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	if object.Headers.Get("ETag") != etag || mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil).Headers.Get("ETag") != etag {
		t.Fatal("multipart ETag was not persisted")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/k", "CopySourceIfMatch": etag}, nil)
	got := readStream(t, object)
	if len(got) != len(firstBody)+3 || !bytes.Equal(got[:len(firstBody)], firstBody) || string(got[len(firstBody):]) != "BBB" {
		t.Fatalf("assembled %d bytes", len(got))
	}
}

func TestMultipartPartReads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k", "ChecksumAlgorithm": "SHA256"}, nil)
	id := created.Output["UploadId"].(string)
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPartWithChecksum(1, first, "ChecksumSHA256", "x-amz-checksum-sha256"), completedPartWithChecksum(2, second, "ChecksumSHA256", "x-amz-checksum-sha256")), nil)
	version := done.Headers.Get("x-amz-version-id")
	if version == "" {
		t.Fatal("missing multipart version")
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("newer"))

	input := map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": 2, "ChecksumMode": "ENABLED"}
	get := mustInvoke(t, p, "GetObject", input, nil)
	if body := readStream(t, get); string(body) != "tail" {
		t.Fatalf("part body = %q", body)
	}
	if get.Status != http.StatusPartialContent || get.Headers.Get("Content-Length") != "4" || get.Headers.Get("Content-Range") != "bytes 5242880-5242883/5242884" || get.Headers.Get("x-amz-mp-parts-count") != "2" {
		t.Fatalf("part headers = status %d %v", get.Status, get.Headers)
	}
	if get.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") || get.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("part checksum = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", input, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "4" || head.Headers.Get("Content-Range") != get.Headers.Get("Content-Range") || head.Headers.Get("x-amz-mp-parts-count") != "2" || head.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("head part = status %d %v", head.Status, head.Headers)
	}

	whole := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "PartNumber": 1}, nil)
	if body := readStream(t, whole); string(body) != "newer" || whole.Status != http.StatusPartialContent || whole.Headers.Get("x-amz-mp-parts-count") != "" {
		t.Fatalf("ordinary part one = %q status %d %v", body, whole.Status, whole.Headers)
	}
	for _, number := range []int{0, 3, 10001} {
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": number}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPartNumber" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("part %d fault = %#v", number, fault)
		}
	}
	_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": 1, "Range": "bytes=0-1"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("part and range fault = %#v", fault)
	}

	golden.AssertJSON(t, map[string]any{
		"get":  map[string]any{"body": "tail", "status": get.Status, "length": get.Headers.Get("Content-Length"), "range": get.Headers.Get("Content-Range"), "parts": get.Headers.Get("x-amz-mp-parts-count"), "checksum": get.Headers.Get("x-amz-checksum-sha256")},
		"head": map[string]any{"status": head.Status, "length": head.Headers.Get("Content-Length"), "range": head.Headers.Get("Content-Range"), "parts": head.Headers.Get("x-amz-mp-parts-count"), "checksum": head.Headers.Get("x-amz-checksum-sha256")},
	})
}

func TestObjectByteRanges(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("0123456789")
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	checksum := base64.StdEncoding.EncodeToString(sum)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "range", "ChecksumCRC32": checksum}, body)
	get := func(value string) (*spi.Response, []byte, error) {
		response, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "range", "Range": value, "ChecksumMode": "ENABLED"}, nil)
		if err != nil {
			return response, nil, err
		}
		return response, readStream(t, response), nil
	}
	for _, test := range []struct {
		value, body, contentRange string
		checksum                  bool
	}{
		{"bytes=2-5", "2345", "bytes 2-5/10", false},
		{"bytes=7-", "789", "bytes 7-9/10", false},
		{"bytes=-3", "789", "bytes 7-9/10", false},
		{"bytes=-20", "0123456789", "bytes 0-9/10", true},
		{"bytes=8-99", "89", "bytes 8-9/10", false},
	} {
		response, got, err := get(test.value)
		if err != nil || response.Status != http.StatusPartialContent || string(got) != test.body || response.Headers.Get("Content-Range") != test.contentRange || response.Headers.Get("Content-Length") != strconv.Itoa(len(test.body)) || response.Headers.Get("Accept-Ranges") != "bytes" {
			t.Fatalf("range %q = %q %#v %v", test.value, got, response, err)
		}
		if hasChecksum := response.Headers.Get("x-amz-checksum-crc32") != ""; hasChecksum != test.checksum {
			t.Fatalf("range %q checksum headers = %v", test.value, response.Headers)
		}
	}
	for _, value := range []string{"2-5", "items=0-1", "bytes=bad", "bytes=5-2", "bytes=0-1,3-4"} {
		response, got, err := get(value)
		if err != nil || response.Status != http.StatusOK || string(got) != string(body) || response.Headers.Get("Content-Range") != "" {
			t.Fatalf("ignored range %q = %q %#v %v", value, got, response, err)
		}
	}
	for _, value := range []string{"bytes=10-", "bytes=-0"} {
		_, _, err := get(value)
		fault := asFault(t, err)
		if fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable || fault.Headers.Get("Content-Range") != "bytes */10" {
			t.Fatalf("range %q fault = %#v", value, fault)
		}
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "range", "Range": "bytes=-3", "ChecksumMode": "ENABLED"}, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "3" || head.Headers.Get("Content-Range") != "bytes 7-9/10" || head.Headers.Get("Accept-Ranges") != "bytes" || head.Headers.Get("x-amz-checksum-crc32") != "" {
		t.Fatalf("head range = %#v", head)
	}
}

func TestGetObjectAttributesContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "missing", "Key": "k", "ObjectAttributes": []string{"ETag"}}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("missing attributes bucket = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, []byte("body"))
	standard := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "standard", "ObjectAttributes": []string{"StorageClass"}}, nil)
	if len(standard.Output) != 1 || standard.Output["StorageClass"] != "STANDARD" {
		t.Fatalf("standard storage class attributes = %#v", standard.Output)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "composite", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA"}, nil)
	id := created.Output["UploadId"].(string)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, bytes.Repeat([]byte("A"), 5<<20))
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, bytes.Repeat([]byte("B"), 5<<20))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 3}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPartWithChecksum(1, first, "ChecksumSHA256", "x-amz-checksum-sha256"), completedPartWithChecksum(2, second, "ChecksumSHA256", "x-amz-checksum-sha256"), completedPartWithChecksum(3, third, "ChecksumSHA256", "x-amz-checksum-sha256")), nil)
	version := done.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "composite"}, []byte("newer"))

	attrs := []string{"ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"}
	page := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": attrs, "MaxParts": 2,
	}, nil)
	if page.Output["ETag"] != done.Output["ETag"] || page.Output["ObjectSize"] != 10<<20+4 || page.Output["StorageClass"] != "STANDARD_IA" || page.Headers.Get("x-amz-version-id") != version || page.Headers.Get("Last-Modified") == "" {
		t.Fatalf("object attributes = %#v %v", page.Output, page.Headers)
	}
	checksum := asMapForTest(page.Output["Checksum"])
	if checksum["ChecksumSHA256"] != strings.SplitN(done.Output["ChecksumSHA256"].(string), "-", 2)[0] || checksum["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("object checksum = %#v", checksum)
	}
	objectParts := asMapForTest(page.Output["ObjectParts"])
	listed := objectParts["Parts"].([]any)
	if objectParts["TotalPartsCount"] != 3 || objectParts["IsTruncated"] != true || objectParts["MaxParts"] != 2 || objectParts["PartNumberMarker"] != "0" || objectParts["NextPartNumberMarker"] != "2" || len(listed) != 2 || asMapForTest(listed[0])["PartNumber"] != 1 || asMapForTest(listed[1])["ChecksumSHA256"] != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("object parts page = %#v", objectParts)
	}
	lastPage := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []any{"ObjectParts"}, "PartNumberMarker": "2", "MaxParts": 2,
	}, nil).Output
	lastParts := asMapForTest(lastPage["ObjectParts"])
	if lastParts["IsTruncated"] != false || lastParts["PartNumberMarker"] != "2" || lastParts["NextPartNumberMarker"] != "3" || len(lastParts["Parts"].([]any)) != 1 || asMapForTest(lastParts["Parts"].([]any)[0])["PartNumber"] != 3 {
		t.Fatalf("object parts final page = %#v", lastParts)
	}
	emptyPage := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectParts"}, "PartNumberMarker": "10", "MaxParts": 2,
	}, nil).Output
	emptyParts := asMapForTest(emptyPage["ObjectParts"])
	if emptyParts["IsTruncated"] != false || emptyParts["PartNumberMarker"] != "10" || emptyParts["NextPartNumberMarker"] != "0" || emptyParts["Parts"] != nil || emptyParts["TotalPartsCount"] != 3 {
		t.Fatalf("object parts empty page = %#v", emptyParts)
	}
	selected := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectSize"}}, nil)
	if len(selected.Output) != 1 || selected.Output["ObjectSize"] == nil {
		t.Fatalf("selected attributes = %#v", selected.Output)
	}
	for field, value := range map[string]any{"MaxParts": 1001, "PartNumberMarker": "invalid"} {
		_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectParts"}, field: value}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid %s fault = %#v", field, fault)
		}
	}

	full := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	fullID := full.Output["UploadId"].(string)
	fullFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 1}, bytes.Repeat([]byte("C"), 5<<20))
	fullSecond := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 2}, []byte("end"))
	fullDone := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(fullID, completedPart(1, fullFirst), completedPart(2, fullSecond)), nil)
	fullAttrs := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "full", "ObjectAttributes": []string{"Checksum", "ObjectParts"}}, nil).Output
	if fullChecksum := asMapForTest(fullAttrs["Checksum"]); fullChecksum["ChecksumCRC32"] != fullDone.Output["ChecksumCRC32"] || fullChecksum["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("full checksum attributes = %#v", fullChecksum)
	}
	if fullParts := asMapForTest(fullAttrs["ObjectParts"]); len(fullParts) != 1 || fullParts["TotalPartsCount"] != 2 {
		t.Fatalf("full object parts = %#v", fullParts)
	}

	golden.AssertJSON(t, map[string]any{"standard": standard.Output, "page": page.Output, "lastPage": lastPage, "emptyPage": emptyPage, "full": fullAttrs})
}

func TestWriteChecksumValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	md5sum, sha1sum, sha256sum, sha512sum := md5.Sum(body), sha1.Sum(body), sha256.Sum256(body), sha512.Sum512(body)
	crc32sum, crc32csum := make([]byte, 4), make([]byte, 4)
	binary.BigEndian.PutUint32(crc32sum, crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(crc32csum, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	b64 := func(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }
	checksums := map[string]string{
		"ContentMD5":        b64(md5sum[:]),
		"ChecksumMD5":       b64(md5sum[:]),
		"ChecksumCRC32":     b64(crc32sum),
		"ChecksumCRC32C":    b64(crc32csum),
		"ChecksumCRC64NVME": "rosUhgp5mIg=",
		"ChecksumSHA1":      b64(sha1sum[:]),
		"ChecksumSHA256":    b64(sha256sum[:]),
		"ChecksumSHA512":    b64(sha512sum[:]),
		"ChecksumXXHASH64":  "jLhB20DmroM=",
		"ChecksumXXHASH3":   "ctyxi2ehff8=",
		"ChecksumXXHASH128": "MxGUd+3l3NXpcWQnaB1YYA==",
	}
	responseHeaders := map[string]string{
		"ChecksumMD5": "x-amz-checksum-md5", "ChecksumCRC32": "x-amz-checksum-crc32", "ChecksumCRC32C": "x-amz-checksum-crc32c",
		"ChecksumCRC64NVME": "x-amz-checksum-crc64nvme", "ChecksumSHA1": "x-amz-checksum-sha1",
		"ChecksumSHA256": "x-amz-checksum-sha256", "ChecksumSHA512": "x-amz-checksum-sha512", "ChecksumXXHASH64": "x-amz-checksum-xxhash64",
		"ChecksumXXHASH3": "x-amz-checksum-xxhash3", "ChecksumXXHASH128": "x-amz-checksum-xxhash128",
	}
	for name, value := range checksums {
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": name, name: value}, body)
		if header := responseHeaders[name]; header != "" {
			if put.Headers.Get(header) != value || put.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s put checksum headers = %v", name, put.Headers)
			}
			get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": name, "ChecksumMode": "ENABLED"}, nil)
			if get.Headers.Get(header) != value || get.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s get checksum headers = %v", name, get.Headers)
			}
			_ = get.Stream.Close()
			if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": name, "ChecksumMode": "ENABLED"}, nil); head.Headers.Get(header) != value {
				t.Fatalf("%s head checksum headers = %v", name, head.Headers)
			}
		}
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": name + "-bad", name: "AA=="}, body)
		if fault := asFault(t, err); fault.Code != "BadDigest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s fault = %#v", name, fault)
		}
	}
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "malformed", "ChecksumMD5": "!"}, body)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("malformed checksum fault = %#v", fault)
	}
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumAlgorithm": "MD5"}, nil)
	uploadID := created.Output["UploadId"].(string)
	_, err = invoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("upload checksum fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": checksums["ChecksumMD5"]}, body)
	if part.Headers.Get("x-amz-checksum-md5") != checksums["ChecksumMD5"] {
		t.Fatalf("upload checksum headers = %v", part.Headers)
	}
	complete := completeInput(uploadID, completedPartWithChecksum(1, part, "ChecksumMD5", "x-amz-checksum-md5"))
	complete["ChecksumMD5"] = "AA=="
	partDigest := md5.Sum(body)
	compositeDigest := md5.Sum(partDigest[:])
	composite := base64.StdEncoding.EncodeToString(compositeDigest[:]) + "-1"
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumMD5"] != composite || done.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("complete checksum output = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-md5") != composite || head.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("multipart checksum metadata = %v", head.Headers)
	}
}

func TestUploadPartContentMD5(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-md5"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-md5", "Key": "key"}, nil).Output["UploadId"].(string)
	body := []byte("content-md5")
	for _, test := range []struct {
		value, code, message string
		fields               map[string]any
	}{
		{"!", "InvalidDigest", "The Content-MD5 you specified was invalid.", map[string]any{"Content_MD5": "!"}},
		{"AA==", "InvalidDigest", "The Content-MD5 you specified was invalid.", map[string]any{"Content_MD5": "AA=="}},
		{"AAAAAAAAAAAAAAAAAAAAAA==", "BadDigest", "The Content-MD5 you specified did not match what we received.", map[string]any{"ExpectedDigest": "AAAAAAAAAAAAAAAAAAAAAA=="}},
	} {
		_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5", "Key": "key", "UploadId": uploadID, "PartNumber": 1, "ContentMD5": test.value}, body)
		fault := asFault(t, err)
		if fault.Code != test.code || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("Content-MD5 %q fault = %#v", test.value, fault)
		}
		for key, value := range test.fields {
			if fault.Fields[key] != value {
				t.Fatalf("Content-MD5 %q field %s = %#v", test.value, key, fault)
			}
		}
		if test.code == "BadDigest" {
			sum := md5.Sum(body)
			if fault.Fields["CalculatedDigest"] != base64.StdEncoding.EncodeToString(sum[:]) {
				t.Fatalf("Content-MD5 calculated digest = %#v", fault)
			}
		}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "upload-part-md5", "Key": "key", "UploadId": uploadID}, nil)
	if len(listed.Output["Parts"].([]any)) != 0 {
		t.Fatalf("rejected digest stored parts = %#v", listed.Output)
	}
	sum := md5.Sum(body)
	mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5", "Key": "key", "UploadId": uploadID, "PartNumber": 1, "ContentMD5": base64.StdEncoding.EncodeToString(sum[:])}, body)
	_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5", "Key": "key", "UploadId": "missing", "PartNumber": 1, "ContentMD5": "!"}, body)
	if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.Fields["UploadId"] != "missing" {
		t.Fatalf("missing upload digest precedence = %#v", fault)
	}
}

func TestUploadPartContentMD5Characterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-md5-golden"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-md5-golden", "Key": "key"}, nil).Output["UploadId"].(string)
	results := map[string]any{}
	for name, value := range map[string]string{"malformed": "!", "mismatch": "AAAAAAAAAAAAAAAAAAAAAA=="} {
		_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5-golden", "Key": "key", "UploadId": uploadID, "PartNumber": 1, "ContentMD5": value}, []byte("content-md5"))
		fault := asFault(t, err)
		results[name] = map[string]any{"code": fault.Code, "message": fault.Message, "fields": fault.Fields}
	}
	_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5-golden", "Key": "key", "UploadId": "missing", "PartNumber": 1, "ContentMD5": "!"}, []byte("content-md5"))
	fault := asFault(t, err)
	results["missing-upload"] = map[string]any{"code": fault.Code, "message": fault.Message, "fields": fault.Fields}
	golden.AssertJSON(t, results)
}

func TestUploadPartChecksumFaults(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-checksum"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-checksum", "Key": "key", "ChecksumAlgorithm": "CRC32"}, nil).Output["UploadId"].(string)
	body := []byte("checksum")
	for _, test := range []struct {
		input         map[string]any
		code, message string
	}{
		{map[string]any{"ChecksumCRC32": "!"}, "InvalidRequest", "Value for x-amz-checksum-crc32 header is invalid."},
		{map[string]any{"ChecksumCRC32": "AA=="}, "InvalidRequest", "Value for x-amz-checksum-crc32 header is invalid."},
		{map[string]any{"ChecksumCRC32": "AAAAAA=="}, "BadDigest", "The CRC32 you specified did not match the calculated checksum."},
		{map[string]any{"ChecksumSHA256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}, "InvalidRequest", "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: sha256"},
		{map[string]any{"ChecksumAlgorithm": "SHA256"}, "InvalidRequest", "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: sha256"},
	} {
		test.input["Bucket"], test.input["Key"], test.input["UploadId"], test.input["PartNumber"] = "upload-part-checksum", "key", uploadID, 1
		_, err := invoke(t, p, "UploadPart", test.input, body)
		if fault := asFault(t, err); fault.Code != test.code || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("checksum input %#v fault = %#v", test.input, fault)
		}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "upload-part-checksum", "Key": "key", "UploadId": uploadID}, nil)
	if len(listed.Output["Parts"].([]any)) != 0 {
		t.Fatalf("rejected checksums stored parts = %#v", listed.Output)
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-checksum", "Key": "key", "UploadId": uploadID, "PartNumber": 1, "ChecksumCRC32": base64.StdEncoding.EncodeToString(sum)}, body)
}

func TestUploadPartChecksumFaultCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-checksum-golden"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-checksum-golden", "Key": "key", "ChecksumAlgorithm": "CRC32"}, nil).Output["UploadId"].(string)
	results := map[string]any{}
	for name, input := range map[string]map[string]any{
		"malformed":          {"ChecksumCRC32": "!"},
		"mismatch":           {"ChecksumCRC32": "AAAAAA=="},
		"header-algorithm":   {"ChecksumSHA256": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
		"declared-algorithm": {"ChecksumAlgorithm": "SHA256"},
	} {
		input["Bucket"], input["Key"], input["UploadId"], input["PartNumber"] = "upload-part-checksum-golden", "key", uploadID, 1
		_, err := invoke(t, p, "UploadPart", input, []byte("checksum"))
		fault := asFault(t, err)
		results[name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	}
	golden.AssertJSON(t, results)
}

func TestMultipartChecksumContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "missing", "Key": "k"}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("create missing bucket fault = %#v", fault)
	}
	wantCreateFault := func(input map[string]any, code string) {
		t.Helper()
		input["Bucket"], input["Key"] = "bucket", code
		_, err := invoke(t, p, "CreateMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != code || fault.HTTPStatus < http.StatusBadRequest {
			t.Fatalf("create checksum fault = %#v want %s", fault, code)
		}
	}
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "SHA256", "ChecksumType": "FULL_OBJECT"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC64NVME", "ChecksumType": "COMPOSITE"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC32", "ChecksumType": "invalid"}, "InvalidArgument")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "XXHASH64", "ChecksumType": "FULL_OBJECT"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumType": "FULL_OBJECT"}, "InvalidRequest")

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	if created.Headers.Get("x-amz-checksum-algorithm") != "CRC32" || created.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("create checksum headers = %v", created.Headers)
	}
	id := created.Output["UploadId"].(string)
	body := []byte("full object")
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumAlgorithm": "SHA1"}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("requested part algorithm fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumSHA1": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("part algorithm fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1}, body)
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id}, nil)
	if listed.Output["ChecksumAlgorithm"] != "CRC32" || listed.Output["ChecksumType"] != "FULL_OBJECT" || listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] == "" {
		t.Fatalf("listed checksum contract = %#v", listed.Output)
	}
	complete := completeInput(id, completedPart(1, part))
	complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "InvalidPart" {
		t.Fatalf("manifest checksum fault = %#v", fault)
	}
	delete(complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any), "ChecksumCRC32")
	complete["ChecksumType"] = "COMPOSITE"
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "The upload was created using the FULL_OBJECT checksum mode. The complete request must use the same checksum mode." || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("complete checksum type fault = %#v", fault)
	}
	complete["ChecksumType"] = "FULL_OBJECT"
	complete["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum fault = %#v", fault)
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	want := base64.StdEncoding.EncodeToString(sum)
	complete["ChecksumCRC32"] = want
	delete(complete, "ChecksumType")
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The crc32 you specified did not match the calculated checksum." || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("implicit full-object checksum type fault = %#v", fault)
	}
	complete["ChecksumType"] = "FULL_OBJECT"
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumCRC32"] != want || done.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("complete checksum = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-crc32") != want || head.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("stored checksum = %v", head.Headers)
	}

	missing := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "missing-part-checksum", "ChecksumAlgorithm": "CRC32"}, nil).Output["UploadId"].(string)
	missingPart := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "missing-part-checksum", "UploadId": missing, "PartNumber": 1}, body)
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(missing, completedPart(1, missingPart)), nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "The upload was created using a crc32 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request." || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing part checksum fault = %#v", fault)
	}
	alternate := completedPart(1, missingPart).(map[string]any)
	alternate["ChecksumSHA256"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(missing, alternate), nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The sha256 you specified for part 1 did not match what we received." || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("alternate part checksum fault = %#v", fault)
	}
	ignored := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "ignored-composite", "ChecksumAlgorithm": "CRC32"}, nil).Output["UploadId"].(string)
	ignoredPart := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "ignored-composite", "UploadId": ignored, "PartNumber": 1}, body)
	ignoredInput := completeInput(ignored, completedPartWithChecksum(1, ignoredPart, "ChecksumCRC32", "x-amz-checksum-crc32"))
	ignoredInput["ChecksumCRC32"] = "AA=="
	ignoredDone := mustInvoke(t, p, "CompleteMultipartUpload", ignoredInput, nil)
	if ignoredDone.Output["ChecksumCRC32"] == "AA==" || ignoredDone.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("ignored composite checksum = %#v", ignoredDone.Output)
	}
	alternateObject := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "alternate-object", "ChecksumAlgorithm": "SHA256"}, nil).Output["UploadId"].(string)
	alternateObjectPart := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "alternate-object", "UploadId": alternateObject, "PartNumber": 1}, body)
	alternateObjectInput := completeInput(alternateObject, completedPartWithChecksum(1, alternateObjectPart, "ChecksumSHA256", "x-amz-checksum-sha256"))
	alternateObjectInput["ChecksumCRC32"] = "AAAAAA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", alternateObjectInput, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The sha256 you specified did not match the calculated checksum." || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("alternate object checksum fault = %#v", fault)
	}

	composite := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "gap", "ChecksumAlgorithm": "SHA256"}, nil)
	compositeID := composite.Output["UploadId"].(string)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "gap", "UploadId": compositeID, "PartNumber": 2}, []byte("second"))
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(compositeID, completedPart(2, second)), nil)
	if fault := asFault(t, err); fault.Code != "InternalError" || fault.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("nonconsecutive composite fault = %#v", fault)
	}
}

func TestXXHashMultipartChecksums(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	for _, tc := range []struct {
		algorithm, input, header, part, composite string
	}{
		{"XXHASH64", "ChecksumXXHASH64", "x-amz-checksum-xxhash64", "jLhB20DmroM=", "aIYCMYPSWcc=-1"},
		{"XXHASH3", "ChecksumXXHASH3", "x-amz-checksum-xxhash3", "ctyxi2ehff8=", "ksPmtVIgSbU=-1"},
		{"XXHASH128", "ChecksumXXHASH128", "x-amz-checksum-xxhash128", "MxGUd+3l3NXpcWQnaB1YYA==", "qhtapxAN/tUuBHXli2H9nQ==-1"},
	} {
		t.Run(tc.algorithm, func(t *testing.T) {
			_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": tc.algorithm + "-full", "ChecksumAlgorithm": tc.algorithm, "ChecksumType": "FULL_OBJECT"}, nil)
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("full-object fault = %#v", fault)
			}
			created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumAlgorithm": tc.algorithm}, nil)
			id := created.Output["UploadId"].(string)
			part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "UploadId": id, "PartNumber": 1, tc.input: tc.part}, body)
			if part.Headers.Get(tc.header) != tc.part {
				t.Fatalf("part headers = %v", part.Headers)
			}
			complete := completeInput(id, completedPartWithChecksum(1, part, tc.input, tc.header))
			complete[tc.input] = tc.composite
			done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
			if done.Output[tc.input] != tc.composite || done.Output["ChecksumType"] != "COMPOSITE" {
				t.Fatalf("complete output = %#v", done.Output)
			}
			head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumMode": "ENABLED"}, nil)
			if head.Headers.Get(tc.header) != tc.composite || head.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
				t.Fatalf("head headers = %v", head.Headers)
			}
		})
	}
}

func TestXXHashChecksumCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	snapshot := map[string]any{}
	for _, tc := range []struct{ algorithm, input, header, value string }{
		{"XXHASH64", "ChecksumXXHASH64", "x-amz-checksum-xxhash64", "jLhB20DmroM="},
		{"XXHASH3", "ChecksumXXHASH3", "x-amz-checksum-xxhash3", "ctyxi2ehff8="},
		{"XXHASH128", "ChecksumXXHASH128", "x-amz-checksum-xxhash128", "MxGUd+3l3NXpcWQnaB1YYA=="},
	} {
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, tc.input: tc.value}, body)
		get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumMode": "ENABLED"}, nil)
		_ = get.Stream.Close()
		snapshot[tc.algorithm] = map[string]any{"put": put.Headers.Get(tc.header), "get": get.Headers.Get(tc.header), "type": get.Headers.Get("x-amz-checksum-type")}
	}
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumAlgorithm": "XXHASH128"}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": id, "PartNumber": 1}, body)
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPartWithChecksum(1, part, "ChecksumXXHASH128", "x-amz-checksum-xxhash128")), nil)
	snapshot["multipart"] = map[string]any{"part": part.Headers.Get("x-amz-checksum-xxhash128"), "complete": done.Output["ChecksumXXHASH128"], "type": done.Output["ChecksumType"]}
	golden.AssertJSON(t, snapshot)
}

func TestMultipartChecksumCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "snapshot", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA", "Tagging": "env=snapshot", "CacheControl": "max-age=120", "ContentType": "application/json", "Metadata": map[string]any{"Env": "snapshot"}, "WebsiteRedirectLocation": "/snapshot"}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "snapshot", "UploadId": id, "PartNumber": 1}, []byte("snapshot"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "snapshot", "UploadId": id}, nil)
	mismatch := completeInput(id, completedPartWithChecksum(1, part, "ChecksumSHA256", "x-amz-checksum-sha256"))
	mismatch["ChecksumType"] = "FULL_OBJECT"
	_, err := invoke(t, p, "CompleteMultipartUpload", mismatch, nil)
	fault := asFault(t, err)
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	missing := asFault(t, err)
	completion := completeInput(id, completedPartWithChecksum(1, part, "ChecksumSHA256", "x-amz-checksum-sha256"))
	completion["ChecksumSHA256"] = "AA=="
	done := mustInvoke(t, p, "CompleteMultipartUpload", completion, nil)
	alternateID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "alternate", "ChecksumAlgorithm": "SHA256"}, nil).Output["UploadId"].(string)
	alternatePart := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "alternate", "UploadId": alternateID, "PartNumber": 1}, []byte("snapshot"))
	alternateInput := completeInput(alternateID, completedPartWithChecksum(1, alternatePart, "ChecksumSHA256", "x-amz-checksum-sha256"))
	alternateInput["ChecksumCRC32"] = "AAAAAA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", alternateInput, nil)
	alternate := asFault(t, err)
	fullID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil).Output["UploadId"].(string)
	fullPart := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": fullID, "PartNumber": 1}, []byte("snapshot"))
	fullInput := completeInput(fullID, completedPart(1, fullPart))
	fullSum := make([]byte, 4)
	binary.BigEndian.PutUint32(fullSum, crc32.ChecksumIEEE([]byte("snapshot")))
	fullInput["ChecksumCRC32"] = base64.StdEncoding.EncodeToString(fullSum)
	_, err = invoke(t, p, "CompleteMultipartUpload", fullInput, nil)
	fullObjectType := asFault(t, err)
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "snapshot"}, nil)
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "snapshot"}, nil).Output["TagSet"]
	golden.AssertJSON(t, map[string]any{
		"create":         map[string]any{"algorithm": created.Output["ChecksumAlgorithm"], "type": created.Output["ChecksumType"], "storageClass": "STANDARD_IA", "tags": "env=snapshot"},
		"part":           map[string]any{"checksum": part.Headers.Get("x-amz-checksum-sha256")},
		"list":           map[string]any{"algorithm": listed.Output["ChecksumAlgorithm"], "type": listed.Output["ChecksumType"], "part": listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumSHA256"]},
		"mismatch":       map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus},
		"missing":        map[string]any{"code": missing.Code, "message": missing.Message, "status": missing.HTTPStatus},
		"alternate":      map[string]any{"code": alternate.Code, "message": alternate.Message, "status": alternate.HTTPStatus},
		"fullObjectType": map[string]any{"code": fullObjectType.Code, "message": fullObjectType.Message, "status": fullObjectType.HTTPStatus},
		"complete":       map[string]any{"checksum": done.Output["ChecksumSHA256"], "supplied": "AA==", "type": done.Output["ChecksumType"]},
		"object":         map[string]any{"cacheControl": head.Headers.Get("Cache-Control"), "contentType": head.Headers.Get("Content-Type"), "metadata": head.Headers.Get("x-amz-meta-env"), "redirect": head.Headers.Get("x-amz-website-redirect-location"), "storageClass": head.Headers.Get("x-amz-storage-class"), "tags": tags},
	})
}

func TestMultipartWithoutChecksum(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "plain"}, nil)
	if created.Output["ChecksumAlgorithm"] != nil || created.Output["ChecksumType"] != nil {
		t.Fatalf("create checksum = %#v", created.Output)
	}
	uploadID := created.Output["UploadId"].(string)
	body := []byte("plain")
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "plain", "UploadId": uploadID, "PartNumber": 1, "ChecksumAlgorithm": "CRC32", "ChecksumCRC32": base64.StdEncoding.EncodeToString(sum)}, body)
	if part.Headers.Get("x-amz-checksum-crc32") == "" || part.Headers.Get("x-amz-checksum-crc64nvme") != "" || part.Headers.Get("x-amz-checksum-type") != "" {
		t.Fatalf("part checksum headers = %v", part.Headers)
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "plain", "UploadId": uploadID}, nil)
	if listed.Output["ChecksumAlgorithm"] != nil || listed.Output["ChecksumType"] != nil || listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] != nil {
		t.Fatalf("list checksum = %#v", listed.Output)
	}
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if done.Output["ChecksumCRC64NVME"] != nil || done.Output["ChecksumType"] != nil {
		t.Fatalf("complete checksum = %#v", done.Output)
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "plain", "ChecksumMode": "ENABLED"}, nil))); body != "plain" {
		t.Fatalf("body = %q", body)
	}
	golden.AssertJSON(t, map[string]any{
		"create":   map[string]any{"algorithm": created.Output["ChecksumAlgorithm"], "type": created.Output["ChecksumType"]},
		"part":     map[string]any{"crc32": part.Headers.Get("x-amz-checksum-crc32")},
		"list":     map[string]any{"algorithm": listed.Output["ChecksumAlgorithm"], "type": listed.Output["ChecksumType"], "partCRC32": listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"]},
		"complete": map[string]any{"crc64nvme": done.Output["ChecksumCRC64NVME"], "type": done.Output["ChecksumType"]},
	})
}

func TestMultipartCreationAttributes(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{
		"Bucket": "bucket", "Key": "attributes", "StorageClass": "STANDARD_IA", "Tagging": "team=storage&env=test",
		"CacheControl": "max-age=60", "ContentDisposition": `attachment; filename="multipart.txt"`, "ContentEncoding": "gzip", "ContentLanguage": "en-US", "ContentType": "text/plain", "Expires": "Wed, 21 Oct 2026 07:28:00 GMT",
		"Metadata": map[string]any{"Team": "storage"}, "WebsiteRedirectLocation": "/multipart",
	}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "attributes", "UploadId": id, "PartNumber": 1}, []byte("body"))
	complete := completeInput(id, completedPart(1, part))
	complete["StorageClass"], complete["Tagging"], complete["ContentType"], complete["Metadata"], complete["WebsiteRedirectLocation"] = "STANDARD", "ignored=true", "application/json", map[string]any{"Team": "ignored"}, "/ignored"
	mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)

	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil)
	if head.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart storage class = %v", head.Headers)
	}
	if head.Headers.Get("Cache-Control") != "max-age=60" || head.Headers.Get("Content-Disposition") != `attachment; filename="multipart.txt"` || head.Headers.Get("Content-Encoding") != "gzip" || head.Headers.Get("Content-Language") != "en-US" || head.Headers.Get("Content-Type") != "text/plain" || head.Headers.Get("Expires") != "Wed, 21 Oct 2026 07:28:00 GMT" || head.Headers.Get("x-amz-meta-team") != "storage" || head.Headers.Get("x-amz-website-redirect-location") != "/multipart" {
		t.Fatalf("multipart metadata = %v", head.Headers)
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil)
	if get.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart get storage class = %v", get.Headers)
	}
	_ = get.Stream.Close()
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil).Output["TagSet"].([]any)
	if len(tags) != 2 || asMapForTest(tags[0])["Key"] != "env" || asMapForTest(tags[0])["Value"] != "test" || asMapForTest(tags[1])["Key"] != "team" || asMapForTest(tags[1])["Value"] != "storage" {
		t.Fatalf("multipart tags = %#v", tags)
	}
	standard := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, []byte("body"))
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, nil); standard.Headers.Get("x-amz-storage-class") != "" || head.Headers.Get("x-amz-storage-class") != "" {
		t.Fatalf("standard storage class headers = put %v head %v", standard.Headers, head.Headers)
	}
}

func TestCompleteMultipartUploadManifest(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	create := func(key string) string {
		t.Helper()
		return mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key}, nil).Output["UploadId"].(string)
	}
	wantFault := func(uploadID, code string, parts ...any) *spi.Fault {
		t.Helper()
		_, err := invoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, parts...), nil)
		fault := asFault(t, err)
		if fault.Code != code || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete fault = %#v want %s", fault, code)
		}
		return fault
	}

	noncontiguous := create("noncontiguous")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 1}, []byte("omitted"))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 3}, []byte("third"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(noncontiguous, completedPart(3, third)), nil)
	if !regexp.MustCompile(`-1"$`).MatchString(done.Headers.Get("ETag")) {
		t.Fatalf("selected part ETag = %q", done.Headers.Get("ETag"))
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "noncontiguous"}, nil)); string(got) != "third" {
		t.Fatalf("noncontiguous completion = %q", got)
	}

	wrongETag := create("wrong-etag")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": wrongETag, "PartNumber": 1}, []byte("one"))
	if fault := wantFault(wrongETag, "InvalidPart", map[string]any{"PartNumber": 1, "ETag": `"wrong"`}); fault.Message != "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag." || fault.Fields["ETag"] != "wrong" || fault.Fields["PartNumber"] != "1" || fault.Fields["UploadId"] != wrongETag {
		t.Fatalf("wrong ETag fault = %#v", fault)
	}
	missing := create("missing")
	if fault := wantFault(missing, "InvalidPart", map[string]any{"PartNumber": 9, "ETag": `"missing"`}); fault.Message != "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag." || fault.Fields["ETag"] != "missing" || fault.Fields["PartNumber"] != "9" || fault.Fields["UploadId"] != missing {
		t.Fatalf("missing part fault = %#v", fault)
	}
	checksumMismatch := create("checksum-mismatch")
	checksumPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": checksumMismatch, "PartNumber": 1}, []byte("checksum"))
	checksumManifest := asMapForTest(completedPart(1, checksumPart))
	checksumManifest["ChecksumCRC64NVME"] = "AA=="
	if fault := wantFault(checksumMismatch, "InvalidPart", checksumManifest); fault.Message != "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag." || fault.Fields["ETag"] != strings.Trim(checksumPart.Headers.Get("ETag"), `"`) || fault.Fields["PartNumber"] != "1" || fault.Fields["UploadId"] != checksumMismatch {
		t.Fatalf("part checksum fault = %#v", fault)
	}

	badOrder := create("order")
	large := bytes.Repeat([]byte("A"), 5<<20)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 2}, large)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 1}, []byte("last"))
	if fault := wantFault(badOrder, "InvalidPartOrder", completedPart(2, second), completedPart(1, first)); fault.Message != "The list of parts was not in ascending order. Parts must be ordered by part number." || fault.Fields["UploadId"] != badOrder {
		t.Fatalf("part order fault = %#v", fault)
	}

	tooSmall := create("small")
	smallFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 1}, []byte("small"))
	smallLast := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 2}, []byte("last"))
	if fault := wantFault(tooSmall, "EntityTooSmall", completedPart(1, smallFirst), completedPart(2, smallLast)); fault.Message != "Your proposed upload is smaller than the minimum allowed size" || fault.Fields["ETag"] != strings.Trim(smallFirst.Headers.Get("ETag"), `"`) || fault.Fields["PartNumber"] != "1" || fault.Fields["MinSizeAllowed"] != 5<<20 || fault.Fields["ProposedSize"] != 5 {
		t.Fatalf("small part fault = %#v", fault)
	}

	zeroIgnored := create("zero-ignored")
	zeroIgnoredPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": zeroIgnored, "PartNumber": 1}, []byte("body"))
	zeroIgnoredInput := completeInput(zeroIgnored, completedPart(1, zeroIgnoredPart))
	zeroIgnoredInput["MpuObjectSize"] = "0"
	mustInvoke(t, p, "CompleteMultipartUpload", zeroIgnoredInput, nil)

	sized := create("sized")
	sizedPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": sized, "PartNumber": 1}, []byte("sized"))
	sizedInput := completeInput(sized, completedPart(1, sizedPart))
	sizedInput["MpuObjectSize"] = "4"
	_, err := invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "The provided 'x-amz-mp-object-size' header value 4 does not match what was computed: 5" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("invalid object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "5"
	mustInvoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	zero := create("zero")
	zeroPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": zero, "PartNumber": 1}, []byte{})
	zeroInput := completeInput(zero, completedPart(1, zeroPart))
	zeroInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("zero object size fault = %#v", fault)
	}
	zeroInput["MpuObjectSize"] = "0"
	mustInvoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
	if fault := wantFault(create("empty"), "InvalidRequest"); fault.Message != "You must specify at least one part" {
		t.Fatalf("empty manifest fault = %#v", fault)
	}
}

func TestMultipartObjectSizeCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-size-golden"}, nil)
	create := func(key string) (string, *spi.Response) {
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-size-golden", "Key": key}, nil).Output["UploadId"].(string)
		return uploadID, mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte("sized"))
	}
	zeroID, zeroPart := create("zero")
	zero := completeInput(zeroID, completedPart(1, zeroPart))
	zero["MpuObjectSize"] = "0"
	accepted := mustInvoke(t, p, "CompleteMultipartUpload", zero, nil)
	mismatchID, mismatchPart := create("mismatch")
	mismatch := completeInput(mismatchID, completedPart(1, mismatchPart))
	mismatch["MpuObjectSize"] = "4"
	_, err := invoke(t, p, "CompleteMultipartUpload", mismatch, nil)
	fault := asFault(t, err)
	golden.AssertJSON(t, map[string]any{"zero": accepted.Output["ETag"], "mismatch": map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}})
}

func TestCompleteMultipartUploadPreconditionFaults(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "complete-preconditions"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "complete-preconditions", "Key": "key"}, nil).Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "complete-preconditions", "Key": "key", "UploadId": uploadID, "PartNumber": 1}, []byte("part"))
	tests := []struct {
		name               string
		conditions         map[string]any
		header, additional string
	}{
		{"combined", map[string]any{"IfMatch": `"etag"`, "IfNoneMatch": "*"}, "If-Match,If-None-Match", "Multiple conditional request headers present in the request"},
		{"if-none-match", map[string]any{"IfNoneMatch": `"etag"`}, "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
		{"if-match-star", map[string]any{"IfMatch": "*"}, "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
	}
	characterization := map[string]any{}
	for index, test := range tests {
		input := completeInput(uploadID, completedPart(1, part))
		for name, value := range test.conditions {
			input[name] = value
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		if fault.Code != "NotImplemented" || fault.Message != "A header you provided implies functionality that is not implemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fault != "server" || fault.Fields["Header"] != test.header || fault.Fields["additionalMessage"] != test.additional {
			t.Fatalf("case %d fault = %#v", index, fault)
		}
		characterization[test.name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "complete-preconditions", "Key": "key", "UploadId": uploadID}, nil)
	if len(listed.Output["Parts"].([]any)) != 1 {
		t.Fatalf("rejected completions changed upload = %#v", listed.Output)
	}
	golden.AssertJSON(t, characterization)
}

func TestWritePreconditionFaults(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "write-preconditions"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-preconditions", "Key": "source"}, []byte("source"))
	tests := []struct {
		name               string
		conditions         map[string]any
		header, additional string
	}{
		{"combined", map[string]any{"IfMatch": `"etag"`, "IfNoneMatch": "*"}, "If-Match,If-None-Match", "Multiple conditional request headers present in the request"},
		{"if-none-match", map[string]any{"IfNoneMatch": `"etag"`}, "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
		{"if-match-star", map[string]any{"IfMatch": "*"}, "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
	}
	characterization := map[string]any{}
	for _, operation := range []string{"PutObject", "CopyObject"} {
		for _, test := range tests {
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-preconditions", "Key": "destination"}, []byte("old"))
				input := map[string]any{"Bucket": "write-preconditions", "Key": "destination"}
				if operation == "CopyObject" {
					input["CopySource"] = "write-preconditions/source"
				}
				for name, value := range test.conditions {
					input[name] = value
				}
				_, err := invoke(t, p, operation, input, []byte("new"))
				fault := asFault(t, err)
				if fault.Code != "NotImplemented" || fault.Message != "A header you provided implies functionality that is not implemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fault != "server" || fault.Fields["Header"] != test.header || fault.Fields["additionalMessage"] != test.additional {
					t.Fatalf("fault = %#v", fault)
				}
				characterization[operation+"/"+test.name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
				if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "write-preconditions", "Key": "destination"}, nil))); body != "old" {
					t.Fatalf("rejected write stored %q", body)
				}
			})
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestWritePreconditionFaultDetails(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "write-precondition-details"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-precondition-details", "Key": "source"}, []byte("source"))
	characterization := map[string]any{}
	for _, operation := range []string{"PutObject", "CopyObject"} {
		for _, test := range []struct {
			name, condition, value, code, message, field, detail string
			status                                               int
			existing                                             bool
		}{
			{"missing-if-match", "IfMatch", `"missing"`, "NoSuchKey", "The specified key does not exist.", "Key", "destination", http.StatusNotFound, false},
			{"wrong-if-match", "IfMatch", `"wrong"`, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", "Condition", "If-Match", http.StatusPreconditionFailed, true},
			{"if-none-match", "IfNoneMatch", "*", "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", "Condition", "If-None-Match", http.StatusPreconditionFailed, true},
		} {
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				if test.existing {
					mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-precondition-details", "Key": "destination"}, []byte("old"))
				} else {
					mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "write-precondition-details", "Key": "destination"}, nil)
				}
				input := map[string]any{"Bucket": "write-precondition-details", "Key": "destination", test.condition: test.value}
				if operation == "CopyObject" {
					input["CopySource"] = "write-precondition-details/source"
				}
				_, err := invoke(t, p, operation, input, []byte("new"))
				fault := asFault(t, err)
				if fault.Code != test.code || fault.Message != test.message || fault.HTTPStatus != test.status || fault.Fault != "client" || fault.Fields[test.field] != test.detail {
					t.Fatalf("fault = %#v", fault)
				}
				characterization[operation+"/"+test.name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
				if test.existing {
					if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "write-precondition-details", "Key": "destination"}, nil))); body != "old" {
						t.Fatalf("rejected write stored %q", body)
					}
				}
			})
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestCompleteMultipartUploadConditionalConflicts(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	const bucket = "complete-conditional-conflicts"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	put := func(key, body string) string {
		t.Helper()
		return mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": key}, []byte(body)).Headers.Get("ETag")
	}
	upload := func(key string) (string, map[string]any) {
		t.Helper()
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": bucket, "Key": key}, nil).Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": bucket, "Key": key, "UploadId": uploadID, "PartNumber": 1}, []byte("part"))
		input := completeInput(uploadID, completedPart(1, part))
		input["Bucket"], input["Key"] = bucket, key
		return uploadID, input
	}
	characterization := map[string]any{}
	wantFault := func(name, uploadID string, input map[string]any, code, message string, status int, fields map[string]any) {
		t.Helper()
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		if fault.Code != code || fault.Message != message || fault.HTTPStatus != status || fault.Fault != "client" || !maps.Equal(fault.Fields, fields) {
			t.Fatalf("fault = %#v", fault)
		}
		characterization[name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": bucket, "Key": input["Key"], "UploadId": uploadID}, nil)
		if len(listed.Output["Parts"].([]any)) != 1 {
			t.Fatalf("rejected completion changed upload = %#v", listed.Output)
		}
	}

	uploadID, input := upload("missing")
	input["IfMatch"] = `"missing"`
	wantFault("missing-if-match", uploadID, input, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound, map[string]any{"Key": "missing"})

	put("mismatch", "old")
	uploadID, input = upload("mismatch")
	input["IfMatch"] = `"wrong"`
	wantFault("mismatched-if-match", uploadID, input, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed, map[string]any{"Condition": "If-Match"})

	uploadID, input = upload("created-after-initiation")
	put("created-after-initiation", "object")
	input["IfNoneMatch"] = "*"
	wantFault("created-after-initiation", uploadID, input, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed, map[string]any{"Condition": "If-None-Match"})

	put("deleted-after-initiation", "object")
	uploadID, input = upload("deleted-after-initiation")
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": "deleted-after-initiation"}, nil)
	input["IfNoneMatch"] = "*"
	wantFault("deleted-after-initiation", uploadID, input, "ConditionalRequestConflict", "The conditional request cannot succeed due to a conflicting operation against this resource.", http.StatusConflict, map[string]any{"Condition": "If-None-Match", "Key": "deleted-after-initiation"})

	put("changed-after-initiation", "old")
	uploadID, input = upload("changed-after-initiation")
	_ = deps.Clock.Advance(2 * time.Second)
	input["IfMatch"] = put("changed-after-initiation", "new")
	wantFault("changed-after-initiation", uploadID, input, "ConditionalRequestConflict", "The conditional request cannot succeed due to a conflicting operation against this resource.", http.StatusConflict, map[string]any{"Condition": "If-Match", "Key": "changed-after-initiation"})

	etag := put("unchanged", "old")
	_, input = upload("unchanged")
	input["IfMatch"] = etag
	mustInvoke(t, p, "CompleteMultipartUpload", input, nil)
	_, input = upload("absent")
	input["IfNoneMatch"] = "*"
	mustInvoke(t, p, "CompleteMultipartUpload", input, nil)
	golden.AssertJSON(t, characterization)
}

func TestMultipartCompletionFaultCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "completion-fault-golden"}, nil)
	create := func(key string) string {
		t.Helper()
		return mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "completion-fault-golden", "Key": key}, nil).Output["UploadId"].(string)
	}
	capture := func(uploadID string, parts ...any) map[string]any {
		t.Helper()
		_, err := invoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, parts...), nil)
		fault := asFault(t, err)
		fields := map[string]any{}
		for key, value := range fault.Fields {
			fields[key] = value
		}
		if _, ok := fields["UploadId"]; ok {
			fields["UploadId"] = "<upload-id>"
		}
		return map[string]any{"code": fault.Code, "message": fault.Message, "fields": fields}
	}
	results := map[string]any{"empty": capture(create("empty"))}
	missing := create("missing")
	results["missing"] = capture(missing, map[string]any{"PartNumber": 9, "ETag": `"missing"`})
	order := create("order")
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": order, "PartNumber": 2}, bytes.Repeat([]byte("A"), 5<<20))
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": order, "PartNumber": 1}, []byte("last"))
	results["order"] = capture(order, completedPart(2, second), completedPart(1, first))
	small := create("small")
	smallFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": small, "PartNumber": 1}, []byte("small"))
	smallLast := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": small, "PartNumber": 2}, []byte("last"))
	results["small"] = capture(small, completedPart(1, smallFirst), completedPart(2, smallLast))
	golden.AssertJSON(t, results)
}

func TestMultipartZeroLimitsUseDefaults(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-zero-limits"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-zero-limits", "Key": "key"}, nil)
	uploadID := created.Output["UploadId"]
	mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-zero-limits", "Key": "key", "UploadId": uploadID, "PartNumber": 1}, []byte("part"))
	got := map[string]any{}
	t.Run("ListMultipartUploads", func(t *testing.T) {
		response, err := invoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "multipart-zero-limits", "MaxUploads": 0}, nil)
		if err != nil || response.Output["MaxUploads"] != 1000 || len(asSliceForTest(response.Output["Uploads"])) != 1 {
			t.Fatalf("zero max uploads = %#v, %v", response, err)
		}
		got["uploads"] = response.Output
	})
	t.Run("ListPartsHTTP", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "http://s3.localhost/multipart-zero-limits/key?uploadId="+url.QueryEscape(fmt.Sprint(uploadID))+"&max-parts=0", nil)
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "ListParts", Input: map[string]any{"Bucket": "multipart-zero-limits", "Key": "key", "UploadId": uploadID}, Identity: ident(), HTTP: request})
		if err != nil || response.Output["MaxParts"] != 1000 || len(asSliceForTest(response.Output["Parts"])) != 1 {
			t.Fatalf("zero max parts = %#v, %v", response, err)
		}
		got["parts"] = response.Output
	})
	golden.AssertJSON(t, got)
}

func TestListPartsAndMultipartUploads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k", "ChecksumAlgorithm": "CRC64NVME"}, nil)
	id, _ := created.Output["UploadId"].(string)
	empty := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": id, "MaxParts": 0}, nil).Output
	if len(empty["Parts"].([]any)) != 0 || empty["MaxParts"] != 1000 || empty["NextPartNumberMarker"] != 0 || asMapForTest(empty["Initiator"])["ID"] != "123456789012" || asMapForTest(empty["Initiator"])["DisplayName"] != "webfile" || asMapForTest(empty["Owner"])["ID"] != "123456789012" {
		t.Fatalf("empty ListParts %v", empty)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": id, "PartNumber": 1}, []byte("AAA"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": id}, nil)
	parts, _ := listed.Output["Parts"].([]any)
	if len(parts) != 1 || listed.Output["ChecksumAlgorithm"] != "CRC64NVME" || listed.Output["ChecksumType"] != "FULL_OBJECT" || listed.Output["NextPartNumberMarker"] != 1 {
		t.Fatalf("ListParts %v", listed.Output)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", asMapForTest(parts[0])["LastModified"].(string)); err != nil {
		t.Fatalf("part timestamp = %#v", parts[0])
	}
	paged := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "paged", "StorageClass": "STANDARD_IA", "ChecksumAlgorithm": "CRC32"}, nil)
	pagedID := paged.Output["UploadId"].(string)
	for _, number := range []int{3, 1, 2} {
		input := map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "PartNumber": number}
		if number == 3 {
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE([]byte("CCC")))
			input["ChecksumCRC32"] = base64.StdEncoding.EncodeToString(sum)
		}
		mustInvoke(t, p, "UploadPart", input, bytes.Repeat([]byte{byte('A' + number - 1)}, 3))
	}
	firstPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "MaxParts": 2}, nil)
	firstParts := firstPage.Output["Parts"].([]any)
	if len(firstParts) != 2 || firstParts[0].(map[string]any)["PartNumber"] != 1 || firstParts[1].(map[string]any)["PartNumber"] != 2 || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextPartNumberMarker"] != 2 || firstPage.Output["StorageClass"] != "STANDARD_IA" || firstPage.Output["ChecksumAlgorithm"] != "CRC32" || firstPage.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("ListParts first page %v", firstPage.Output)
	}
	secondPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "PartNumberMarker": 2, "MaxParts": 2}, nil)
	last := secondPage.Output["Parts"].([]any)[0].(map[string]any)
	if last["PartNumber"] != 3 || last["LastModified"] == "" || last["ChecksumCRC32"] == nil || secondPage.Output["IsTruncated"] != false || secondPage.Output["PartNumberMarker"] != 2 || secondPage.Output["NextPartNumberMarker"] != 3 {
		t.Fatalf("ListParts second page %v", secondPage.Output)
	}
	beyond := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "PartNumberMarker": 10, "MaxParts": 1}, nil).Output
	if len(beyond["Parts"].([]any)) != 0 || beyond["PartNumberMarker"] != 10 || beyond["NextPartNumberMarker"] != 0 || beyond["IsTruncated"] != false {
		t.Fatalf("ListParts beyond final part %v", beyond)
	}
	for _, input := range []map[string]any{
		{"Bucket": "bucket", "Key": "paged", "UploadId": "missing"},
		{"Bucket": "bucket", "Key": "wrong", "UploadId": pagedID},
	} {
		_, err := invoke(t, p, "ListParts", input, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("ListParts missing upload fault = %#v", fault)
		}
	}
	_, err := invoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "MaxParts": 1001}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("ListParts max fault = %#v", fault)
	}
	ups := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket"}, nil)
	uploads, _ := ups.Output["Uploads"].([]any)
	if len(uploads) != 2 {
		t.Fatalf("ListMultipartUploads %v", ups.Output)
	}
	firstUpload, secondUpload := asMapForTest(uploads[0]), asMapForTest(uploads[1])
	if firstUpload["Key"] != "k" || firstUpload["ChecksumAlgorithm"] != "CRC64NVME" || firstUpload["ChecksumType"] != "FULL_OBJECT" || asMapForTest(firstUpload["Initiator"])["DisplayName"] != "webfile" || secondUpload["Key"] != "paged" || secondUpload["ChecksumAlgorithm"] != "CRC32" || secondUpload["ChecksumType"] != "COMPOSITE" || asMapForTest(secondUpload["Initiator"])["DisplayName"] != "webfile" {
		t.Fatalf("ListMultipartUploads metadata %v", uploads)
	}
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	mustInvoke(t, p, "AbortMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID}, nil)
	after := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket"}, nil)
	uploads, _ = after.Output["Uploads"].([]any)
	if len(uploads) != 0 {
		t.Fatalf("completed upload still listed: %v", after.Output)
	}
}

func TestListPartsCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "parts-golden"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "parts-golden", "Key": "object", "ChecksumAlgorithm": "CRC64NVME"}, nil).Output["UploadId"].(string)
	input := map[string]any{"Bucket": "parts-golden", "Key": "object", "UploadId": uploadID}
	empty := mustInvoke(t, p, "ListParts", maps.Clone(input), nil).Output
	for _, part := range []struct {
		number int
		body   string
	}{{1, "one"}, {3, "three"}} {
		partInput := maps.Clone(input)
		partInput["PartNumber"] = part.number
		mustInvoke(t, p, "UploadPart", partInput, []byte(part.body))
		_ = deps.Clock.Advance(time.Second)
	}
	firstInput := maps.Clone(input)
	firstInput["MaxParts"] = 1
	first := mustInvoke(t, p, "ListParts", firstInput, nil).Output
	nextInput := maps.Clone(firstInput)
	nextInput["PartNumberMarker"] = first["NextPartNumberMarker"]
	next := mustInvoke(t, p, "ListParts", nextInput, nil).Output
	beyondInput := maps.Clone(firstInput)
	beyondInput["PartNumberMarker"] = 10
	beyond := mustInvoke(t, p, "ListParts", beyondInput, nil).Output
	golden.AssertJSON(t, map[string]any{"empty": empty, "first": first, "next": next, "beyond": beyond})
}

func TestListMultipartUploadsPaginationAndDelimiter(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	create := func(key, storageClass string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key, "StorageClass": storageClass}, nil)
		_ = deps.Clock.Advance(time.Second)
		return response.Output["UploadId"].(string)
	}
	create("photos/2026/b.jpg", "STANDARD")
	firstSame := create("same", "STANDARD_IA")
	create("alpha", "STANDARD")
	secondSame := create("same", "STANDARD")
	spaceUpload := create("space key", "STANDARD")

	firstPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "MaxUploads": 3}, nil)
	first := firstPage.Output["Uploads"].([]any)
	if len(first) != 3 || first[0].(map[string]any)["Key"] != "alpha" || first[1].(map[string]any)["Key"] != "photos/2026/b.jpg" || first[2].(map[string]any)["UploadId"] != firstSame || first[2].(map[string]any)["StorageClass"] != "STANDARD_IA" || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextKeyMarker"] != "same" || firstPage.Output["NextUploadIdMarker"] != firstSame {
		t.Fatalf("first multipart page = %v", firstPage.Output)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000Z", first[0].(map[string]any)["Initiated"].(string)); err != nil {
		t.Fatalf("multipart initiation timestamp = %v", first[0])
	}
	secondPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{
		"Bucket": "bucket", "KeyMarker": "same", "UploadIdMarker": firstSame, "MaxUploads": 3,
	}, nil)
	second := secondPage.Output["Uploads"].([]any)
	if len(second) != 2 || second[0].(map[string]any)["UploadId"] != secondSame || second[1].(map[string]any)["Key"] != "space key" || secondPage.Output["IsTruncated"] != false || secondPage.Output["NextKeyMarker"] != "space key" || secondPage.Output["NextUploadIdMarker"] != spaceUpload {
		t.Fatalf("second multipart page = %v", secondPage.Output)
	}
	uploadMarkerOnly := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "UploadIdMarker": firstSame, "MaxUploads": 1}, nil).Output
	if uploadMarkerOnly["UploadIdMarker"] != "" || asMapForTest(asSliceForTest(uploadMarkerOnly["Uploads"])[0])["Key"] != "alpha" {
		t.Fatalf("upload marker without key = %#v", uploadMarkerOnly)
	}
	_, err := invoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "KeyMarker": "alpha", "UploadIdMarker": firstSame}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Message != "Invalid uploadId marker" || fault.Fields["ArgumentName"] != "upload-id-marker" || fault.Fields["ArgumentValue"] != firstSame {
		t.Fatalf("mismatched upload marker = %#v", fault)
	}
	for _, key := range []string{"folder/a/one", "folder/a/two", "folder/file1", "folder/file2"} {
		create(key, "STANDARD")
	}
	prefixPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "Prefix": "folder/", "Delimiter": "/", "MaxUploads": 1}, nil).Output
	if prefixes := asSliceForTest(prefixPage["CommonPrefixes"]); len(prefixes) != 1 || asMapForTest(prefixes[0])["Prefix"] != "folder/a/" || prefixPage["IsTruncated"] != true || prefixPage["NextKeyMarker"] != "" || prefixPage["NextUploadIdMarker"] != "" {
		t.Fatalf("multipart common prefix page = %#v", prefixPage)
	}
	grouped := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "Prefix": "photos/", "Delimiter": "/"}, nil)
	groups := grouped.Output["CommonPrefixes"].([]any)
	if len(grouped.Output["Uploads"].([]any)) != 0 || len(groups) != 1 || groups[0].(map[string]any)["Prefix"] != "photos/2026/" {
		t.Fatalf("grouped multipart uploads = %v", grouped.Output)
	}
	encoded := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "Prefix": "space", "EncodingType": "url"}, nil)
	if encoded.Output["Uploads"].([]any)[0].(map[string]any)["Key"] != "space%20key" || encoded.Output["EncodingType"] != "url" {
		t.Fatalf("encoded multipart uploads = %v", encoded.Output)
	}
	for _, test := range []struct {
		input      map[string]any
		code       string
		httpStatus int
	}{
		{map[string]any{"Bucket": "missing"}, "NoSuchBucket", http.StatusNotFound},
	} {
		_, err := invoke(t, p, "ListMultipartUploads", test.input, nil)
		if fault := asFault(t, err); fault.Code != test.code || fault.HTTPStatus != test.httpStatus {
			t.Fatalf("invalid multipart listing fault = %#v", fault)
		}
	}
}

func TestListMultipartUploadsCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-list-golden"}, nil)
	ids := map[string]string{}
	for _, key := range []string{"folder/a/one", "folder/a/two", "folder/file1", "folder/file2"} {
		ids[key] = mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-list-golden", "Key": key, "ChecksumAlgorithm": "CRC64NVME"}, nil).Output["UploadId"].(string)
	}
	first := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "multipart-list-golden", "Prefix": "folder/", "Delimiter": "/", "MaxUploads": 1}, nil).Output
	next := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "multipart-list-golden", "Prefix": "folder/", "Delimiter": "/", "MaxUploads": 1, "KeyMarker": "folder/a/"}, nil).Output
	_, err := invoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "multipart-list-golden", "KeyMarker": "folder/file1", "UploadIdMarker": ids["folder/file2"]}, nil)
	fault := asFault(t, err)
	golden.AssertJSON(t, map[string]any{"first": first, "next": next, "invalid": map[string]any{"code": fault.Code, "message": fault.Message, "fields": fault.Fields}})
}

func TestMultipartOperationsRejectMissingUpload(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, operation := range []string{"UploadPart", "CompleteMultipartUpload", "ListParts", "AbortMultipartUpload"} {
		for _, input := range []map[string]any{
			{"Bucket": "bucket", "Key": "k", "UploadId": "missing", "PartNumber": 1},
			{"Bucket": "bucket", "Key": "wrong", "UploadId": uploadID, "PartNumber": 1},
			{"Bucket": "wrong", "Key": "k", "UploadId": uploadID, "PartNumber": 1},
		} {
			if operation == "CompleteMultipartUpload" {
				input["MultipartUpload"] = map[string]any{"Parts": []any{}}
			}
			_, err := invoke(t, p, operation, input, []byte("part"))
			expected := "NoSuchUpload"
			if input["Bucket"] == "wrong" {
				expected = "NoSuchBucket"
			}
			fault := asFault(t, err)
			if fault.Code != expected || fault.HTTPStatus != http.StatusNotFound {
				t.Fatalf("%s fault = %#v", operation, fault)
			}
			if expected == "NoSuchUpload" && (fault.Message != "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed." || fault.Fields["UploadId"] != input["UploadId"]) {
				t.Fatalf("%s modeled fault = %#v", operation, fault)
			}
		}
	}
	mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": uploadID}, nil)
}

func TestNoSuchUploadCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-fault-golden"}, nil)
	results := map[string]any{}
	for _, operation := range []string{"UploadPart", "CompleteMultipartUpload", "ListParts", "AbortMultipartUpload"} {
		input := map[string]any{"Bucket": "multipart-fault-golden", "Key": "key", "UploadId": "missing", "PartNumber": 1}
		if operation == "CompleteMultipartUpload" {
			input["MultipartUpload"] = map[string]any{"Parts": []any{}}
		}
		_, err := invoke(t, p, operation, input, []byte("part"))
		fault := asFault(t, err)
		results[operation] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "fields": fault.Fields}
	}
	golden.AssertJSON(t, results)
}

func TestMultipartPartNumberBounds(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, input := range []map[string]any{
		{"UploadId": uploadID},
		{"UploadId": uploadID, "PartNumber": -1},
		{"UploadId": uploadID, "PartNumber": 0},
		{"UploadId": uploadID, "PartNumber": 10001},
	} {
		_, err := invoke(t, p, "UploadPart", input, []byte("part"))
		fault := asFault(t, err)
		want := 0
		if number, ok := input["PartNumber"]; ok {
			want = number.(int)
		}
		if fault.Code != "InvalidArgument" || fault.Message != "Part number must be an integer between 1 and 10000, inclusive" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "partNumber" || fault.Fields["ArgumentValue"] != want {
			t.Fatalf("UploadPart %#v fault = %#v", input, fault)
		}
	}
	_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": "missing", "PartNumber": 0}, []byte("part"))
	if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.Fields["UploadId"] != "missing" {
		t.Fatalf("missing upload precedence fault = %#v", fault)
	}
	last := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 10000}, []byte("last"))
	for _, number := range []int{0, 10001} {
		input := completeInput(uploadID, map[string]any{"PartNumber": number, "ETag": last.Headers.Get("ETag")})
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPart" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete part %d fault = %#v", number, fault)
		}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": uploadID}, nil)
	if listed.Output["Parts"].([]any)[0].(map[string]any)["PartNumber"] != 10000 {
		t.Fatalf("valid boundary part = %v", listed.Output)
	}
}

func TestMultipartPartNumberFaultCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "part-number-golden"}, nil)
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "part-number-golden", "Key": "key"}, nil).Output["UploadId"].(string)
	results := []any{}
	for _, number := range []int{-1, 0, 10001} {
		_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "part-number-golden", "Key": "key", "UploadId": uploadID, "PartNumber": number}, []byte("part"))
		fault := asFault(t, err)
		results = append(results, map[string]any{"code": fault.Code, "message": fault.Message, "fields": fault.Fields})
	}
	_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "part-number-golden", "Key": "key", "UploadId": "missing", "PartNumber": 0}, []byte("part"))
	fault := asFault(t, err)
	results = append(results, map[string]any{"code": fault.Code, "message": fault.Message, "fields": fault.Fields})
	golden.AssertJSON(t, results)
}

func TestMissingBucket404(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "nope", "Key": "k"}, []byte("x"))
	f := asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("put missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("list missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "HeadBucket", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("head missing bucket: %+v", f)
	}
}

func TestReplicationTargetsVersionMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	for _, bucket := range []string{"source", "destination"} {
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket, "ObjectLockEnabledForBucket": true}, nil)
	}
	mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{
		"Status": "Enabled", "Destination": map[string]any{"Bucket": "arn:aws:s3:::destination"},
	}}}}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "key", "Tagging": "stage=first"}, []byte("first")).Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "key", "Tagging": "stage=second"}, []byte("second")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "source", "Key": "key", "TagSet": []any{map[string]any{"Key": "stage", "Value": "updated"}}}, nil)
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "source", "Key": "key", "LegalHold": map[string]any{"Status": "ON"}}, nil)

	for _, tc := range []struct {
		version, body, tag, hold string
	}{{first, "first", "first", ""}, {second, "second", "updated", "ON"}} {
		got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "destination", "Key": "key", "VersionId": tc.version}, nil)
		if body := string(readStream(t, got)); body != tc.body {
			t.Fatalf("version %s body=%q want=%q", tc.version, body, tc.body)
		}
		tags := asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "key", "VersionId": tc.version}, nil).Output["TagSet"])
		if len(tags) != 1 || asMapForTest(tags[0])["Value"] != tc.tag {
			t.Fatalf("version %s tags=%#v", tc.version, tags)
		}
		hold := asMapForTest(mustInvoke(t, p, "GetObjectLegalHold", map[string]any{"Bucket": "destination", "Key": "key", "VersionId": tc.version}, nil).Output["LegalHold"])
		if status, _ := hold["Status"].(string); status != tc.hold {
			t.Fatalf("version %s legal hold=%#v", tc.version, hold)
		}
	}
}

func TestReplicationConfigurationValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{}
	for _, bucket := range []string{"source", "destination"} {
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
	}
	legacy := map[string]any{
		"Role":  "arn:aws:iam::000000000000:role/replication",
		"Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "arn:aws:s3:::destination"}}},
	}
	_, err := invoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": legacy}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "Versioning must be 'Enabled' on the bucket to apply a replication configuration" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("replication without versioning: %+v", fault)
	} else {
		characterization["versioning disabled"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus}
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "source", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}, nil)

	manyRules := make([]any, 1001)
	for i := range manyRules {
		manyRules[i] = map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "arn:aws:s3:::destination"}}
	}
	for _, tc := range []struct {
		name, code    string
		configuration map[string]any
	}{
		{"missing role", "MalformedXML", map[string]any{"Rules": legacy["Rules"]}},
		{"missing rules", "MalformedXML", map[string]any{"Role": legacy["Role"]}},
		{"too many rules", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": manyRules}},
		{"invalid status", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Status": "Pending", "Destination": map[string]any{"Bucket": "destination"}}}}},
		{"missing destination", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{}}}}},
		{"filter missing priority", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Status": "Enabled", "Filter": map[string]any{"Prefix": "logs/"}, "DeleteMarkerReplication": map[string]any{"Status": "Disabled"}, "Destination": map[string]any{"Bucket": "destination"}}}}},
		{"filter missing delete marker setting", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Prefix": "logs/"}, "Destination": map[string]any{"Bucket": "destination"}}}}},
		{"invalid delete marker status", "MalformedXML", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Prefix": "logs/"}, "DeleteMarkerReplication": map[string]any{"Status": "Pending"}, "Destination": map[string]any{"Bucket": "destination"}}}}},
		{"tag filter replicates delete markers", "InvalidRequest", map[string]any{"Role": legacy["Role"], "Rules": []any{map[string]any{"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Tag": map[string]any{"Key": "environment", "Value": "test"}}, "DeleteMarkerReplication": map[string]any{"Status": "Enabled"}, "Destination": map[string]any{"Bucket": "destination"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": tc.configuration}, nil)
			if fault := asFault(t, err); fault.Code != tc.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault=%+v want=%s/400", fault, tc.code)
			} else {
				characterization[tc.name] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus}
			}
		})
	}
	mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": legacy}, nil)
	mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": map[string]any{
		"Role": legacy["Role"], "Rules": []any{map[string]any{
			"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Tag": map[string]any{"Key": "environment", "Value": "test"}},
			"DeleteMarkerReplication": map[string]any{"Status": "Disabled"}, "Destination": map[string]any{"Bucket": "destination"},
		}},
	}}, nil)
	characterization["valid"] = mustInvoke(t, p, "GetBucketReplication", map[string]any{"Bucket": "source"}, nil).Output
	golden.AssertJSON(t, characterization)
}

func TestReplicationDestinationValidationAndRuleIDs(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "source", "Status": "Enabled"}, nil)
	configuration := func(destination string) map[string]any {
		return map[string]any{
			"Role":  "arn:aws:iam::000000000000:role/replication",
			"Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "arn:aws:s3:::" + destination}}},
		}
	}
	put := func(destination string) *spi.Fault {
		_, err := invoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": configuration(destination)}, nil)
		return asFault(t, err)
	}
	if fault := put("destination"); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing destination: %+v", fault)
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	if fault := put("destination"); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("unversioned destination: %+v", fault)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Suspended"}, nil)
	if fault := put("destination"); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("suspended destination: %+v", fault)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": configuration("destination")}, nil)
	stored := asMapForTest(mustInvoke(t, p, "GetBucketReplication", map[string]any{"Bucket": "source"}, nil).Output["ReplicationConfiguration"])
	rule := asMapForTest(asSliceForTest(stored["Rules"])[0])
	if id, _ := rule["ID"].(string); len(id) != 8 {
		t.Fatalf("generated rule ID %q", id)
	}
	rule["ID"] = "explicit"
	mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "source", "ReplicationConfiguration": stored}, nil)
	stored = asMapForTest(mustInvoke(t, p, "GetBucketReplication", map[string]any{"Bucket": "source"}, nil).Output["ReplicationConfiguration"])
	if got := asMapForTest(asSliceForTest(stored["Rules"])[0])["ID"]; got != "explicit" {
		t.Fatalf("explicit rule ID = %v", got)
	}
}

func TestPostObjectMultipartUpload(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-object"}, nil)
	mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": "post-object", "LifecycleConfiguration": map[string]any{"Rules": []any{map[string]any{"ID": "post", "Filter": map[string]any{"Prefix": "uploads/"}, "Status": "Enabled", "Expiration": map[string]any{"Days": 1}}}}}, nil)
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	_ = writer.WriteField("key", "uploads/${filename}")
	_ = writer.WriteField("success_action_status", "201")
	_ = writer.WriteField("Content-Type", "text/plain")
	_ = writer.WriteField("x-amz-meta-owner", "mirror")
	_ = writer.WriteField("x-amz-meta-non-ascii", "ÄMÄZÕÑ S3")
	_ = writer.WriteField("x-amz-meta-q-encoded", "=?UTF-8?Q?actually-ascii?=")
	_ = writer.WriteField("x-amz-meta-b-encoded", "=?UTF-8?B?YWJj?=")
	_ = writer.WriteField("x-amz-meta-control", "\x00\x01\x02\x03")
	file, err := writer.CreateFormFile("file", "hello world.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte("browser upload"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-object", &payload)
	httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-object"},
		Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != http.StatusCreated || response.Output["Key"] != "uploads/hello world.txt" || response.Headers.Get("ETag") == "" || response.Headers.Get("x-amz-expiration") != `expiry-date="Sat, 03 Jan 1970 00:00:00 GMT", rule-id="post"` {
		t.Fatalf("post response: %#v", response)
	}
	golden.AssertJSON(t, map[string]any{"status": response.Status, "headers": response.Headers, "output": response.Output})
	got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "post-object", "Key": "uploads/hello world.txt"}, nil)
	if body := string(readStream(t, got)); body != "browser upload" || got.Headers.Get("Content-Type") != "text/plain" || got.Headers.Get("x-amz-meta-owner") != "mirror" || got.Headers.Get("x-amz-meta-non-ascii") != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || got.Headers.Get("x-amz-meta-q-encoded") != "=?UTF-8?Q?actually-ascii?=" || got.Headers.Get("x-amz-meta-b-encoded") != "=?UTF-8?B?YWJj?=" || got.Headers.Get("x-amz-meta-control") != "=?UTF-8?B?AAECAw==?=" {
		t.Fatalf("stored body=%q headers=%v", body, got.Headers)
	}
}

func TestPostObjectRejectsMalformedForms(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-invalid"}, nil)
	call := func(contentType string, payload []byte) *spi.Fault {
		t.Helper()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-invalid", bytes.NewReader(payload))
		if contentType != "" {
			httpRequest.Header.Set("Content-Type", contentType)
		}
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-invalid"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		return asFault(t, err)
	}
	if fault := call("text/plain", []byte("body")); fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
		t.Fatalf("non-multipart: %+v", fault)
	}
	if fault := call("multipart/form-data", []byte("body")); fault.Code != "MalformedPOSTRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing boundary: %+v", fault)
	}
	form := func(key string, file bool) (string, []byte) {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		if key != "" {
			_ = writer.WriteField("key", key)
		}
		if file {
			part, _ := writer.CreateFormFile("file", "file.txt")
			_, _ = part.Write([]byte("body"))
		}
		_ = writer.Close()
		return writer.FormDataContentType(), payload.Bytes()
	}
	for _, tc := range []struct {
		name string
		key  string
		file bool
	}{{"missing key", "", true}, {"missing file", "key", false}} {
		t.Run(tc.name, func(t *testing.T) {
			contentType, payload := form(tc.key, tc.file)
			if fault := call(contentType, payload); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault: %+v", fault)
			}
		})
	}
}

func TestPostObjectSuccessRedirect(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-redirect"}, nil)
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	_ = writer.WriteField("key", "redirected")
	_ = writer.WriteField("success_action_redirect", "https://example.test/done?state=ok")
	file, _ := writer.CreateFormFile("file", "file.txt")
	_, _ = file.Write([]byte("body"))
	_ = writer.Close()
	httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-redirect", &payload)
	httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-redirect"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
	if err != nil {
		t.Fatal(err)
	}
	location, err := url.Parse(response.Headers.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	query := location.Query()
	if response.Status != http.StatusSeeOther || location.Host != "example.test" || query.Get("state") != "ok" || query.Get("bucket") != "post-redirect" || query.Get("key") != "redirected" || query.Get("etag") == "" {
		t.Fatalf("redirect response: %#v", response)
	}
}

func TestPostObjectPolicyValidation(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-policy"}, nil)
	encodePolicy := func(expiration string, conditions []any) string {
		raw, _ := json.Marshal(map[string]any{"expiration": expiration, "conditions": conditions})
		return base64.StdEncoding.EncodeToString(raw)
	}
	post := func(key string, fields map[string]string, body string) (*spi.Response, error) {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", key)
		for field, value := range fields {
			_ = writer.WriteField(field, value)
		}
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte(body))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-policy", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		return p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-policy"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
	}
	v4 := map[string]string{
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
		"x-amz-credential": "test/20260827/us-east-1/s3/aws4_request",
		"x-amz-date":       "20260827T000000Z",
		"x-amz-signature":  "signature",
		"Content-Type":     "text/plain",
	}
	policyFields := func(policy string) map[string]string {
		fields := maps.Clone(v4)
		fields["policy"] = policy
		return fields
	}
	fields := maps.Clone(v4)
	fields["policy"] = encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{
		map[string]any{"bucket": "post-policy"},
		[]any{"eq", "$key", "uploads/item"},
		[]any{"starts-with", "$Content-Type", "text/"},
		[]any{"content-length-range", 1, 10},
	})
	if _, err := post("uploads/item", fields, "body"); err != nil {
		t.Fatal(err)
	}
	if got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "post-policy", "Key": "uploads/item"}, nil); string(readStream(t, got)) != "body" {
		t.Fatal("valid policy did not store object")
	}
	legacy := map[string]string{
		"AWSAccessKeyId": "test",
		"signature":      "signature",
		"policy":         encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{map[string]any{"bucket": "post-policy"}}),
	}
	if _, err := post("uploads/legacy", legacy, "legacy"); err != nil {
		t.Fatal(err)
	}
	characterization := map[string]any{
		"accepted":        map[string]any{"key": "uploads/item", "size": 4},
		"accepted legacy": map[string]any{"key": "uploads/legacy", "size": 6},
	}

	missingSignature := policyFields(encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), nil))
	delete(missingSignature, "x-amz-date")
	missingLegacySignature := maps.Clone(legacy)
	delete(missingLegacySignature, "AWSAccessKeyId")
	cases := []struct {
		name   string
		fields map[string]string
		code   string
	}{
		{"expired", policyFields(encodePolicy(deps.Clock.Now().Add(-time.Second).Format(time.RFC3339Nano), nil)), "AccessDenied"},
		{"missing signature field", missingSignature, "InvalidArgument"},
		{"missing legacy signature field", missingLegacySignature, "InvalidArgument"},
		{"no signature fields", map[string]string{"policy": encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), nil)}, "AccessDenied"},
		{"malformed policy", policyFields("not-base64"), "SignatureDoesNotMatch"},
		{"failed condition", policyFields(encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{map[string]any{"bucket": "wrong"}})), "AccessDenied"},
		{"too small", policyFields(encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{[]any{"content-length-range", 5, 10}})), "EntityTooSmall"},
		{"too large", policyFields(encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{[]any{"content-length-range", 0, 3}})), "EntityTooLarge"},
		{"invalid simple condition", policyFields(encodePolicy(deps.Clock.Now().Add(time.Hour).Format(time.RFC3339Nano), []any{map[string]any{"bucket": "post-policy", "key": "rejected"}})), "InvalidPolicyDocument"},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprintf("rejected-%d", index)
			_, err := post(key, tc.fields, "body")
			if fault := asFault(t, err); fault.Code != tc.code {
				t.Fatalf("fault = %+v", fault)
			} else {
				characterization[tc.name] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus, "message": fault.Message, "fields": fault.Fields}
			}
			if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-policy", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
				t.Fatal("rejected policy stored object")
			}
		})
	}
	golden.AssertJSON(t, characterization)
}

func TestPostObjectPolicySignatureCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-signature"}, nil)
	policy := base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[{"bucket":"post-signature"}]}`))
	hmac256 := func(key []byte, value string) []byte {
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write([]byte(value))
		return mac.Sum(nil)
	}
	signV4 := func(fields map[string]string, secret string) {
		credential := strings.Split(fields["x-amz-credential"], "/")
		dateKey := hmac256([]byte("AWS4"+secret), credential[1])
		regionKey := hmac256(dateKey, credential[2])
		serviceKey := hmac256(regionKey, credential[3])
		fields["x-amz-signature"] = hex.EncodeToString(hmac256(hmac256(serviceKey, "aws4_request"), fields["policy"]))
	}
	post := func(key string, fields map[string]string, validate bool) error {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", key)
		for field, value := range fields {
			_ = writer.WriteField(field, value)
		}
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-signature", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-signature"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest, S3ValidateSignatures: validate})
		return err
	}
	v4 := map[string]string{
		"policy":           policy,
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
		"x-amz-credential": "test/20990101/us-east-1/s3/aws4_request",
		"x-amz-date":       "20990101T000000Z",
	}
	signV4(v4, "test")
	if err := post("v4", v4, true); err != nil {
		t.Fatal(err)
	}
	tamperedV4 := maps.Clone(v4)
	tamperedV4["x-amz-signature"] = "00"
	if fault := asFault(t, post("rejected-v4", tamperedV4, true)); fault.Code != "SignatureDoesNotMatch" || fault.HTTPStatus != http.StatusForbidden {
		t.Fatalf("tampered SigV4 fault = %+v", fault)
	}
	if err := post("validation-disabled", tamperedV4, false); err != nil {
		t.Fatalf("default-off signature validation rejected request: %v", err)
	}
	v2 := map[string]string{"policy": policy, "AWSAccessKeyId": "test"}
	mac := hmac.New(sha1.New, []byte("test"))
	_, _ = mac.Write([]byte(policy))
	v2["signature"] = base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if err := post("v2", v2, true); err != nil {
		t.Fatal(err)
	}
	tamperedV2 := maps.Clone(v2)
	tamperedV2["signature"] = "tampered"
	if fault := asFault(t, post("rejected-v2", tamperedV2, true)); fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered SigV2 fault = %+v", fault)
	}
	temporary := "temporary"
	if err := deps.Store.Scope("_mirror", "global").Collection("stsk").Put(context.Background(), temporary, []byte(ident().Account)); err != nil {
		t.Fatal(err)
	}
	temporaryV4 := maps.Clone(v4)
	temporaryV4["x-amz-credential"] = temporary + "/20990101/us-east-1/s3/aws4_request"
	temporaryV4["x-amz-security-token"] = deps.Rand.Derive(temporary + "tok").Hex(32)
	signV4(temporaryV4, deps.Rand.Derive(temporary).Hex(40))
	if err := post("temporary", temporaryV4, true); err != nil {
		t.Fatal(err)
	}
	missingToken := maps.Clone(temporaryV4)
	delete(missingToken, "x-amz-security-token")
	if fault := asFault(t, post("rejected-token", missingToken, true)); fault.Code != "InvalidToken" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing token fault = %+v", fault)
	}
	for _, key := range []string{"rejected-v4", "rejected-v2", "rejected-token"} {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-signature", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("rejected upload %q stored object", key)
		}
	}
	golden.AssertJSON(t, map[string]any{"accepted": []string{"temporary", "v2", "v4", "validation-disabled"}, "rejected": map[string]string{"SigV2": "SignatureDoesNotMatch", "SigV4": "SignatureDoesNotMatch", "temporary token": "InvalidToken"}})
}

func TestPostObjectTagging(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-tags"}, nil)
	post := func(key, tagging string) error {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", key)
		_ = writer.WriteField("tagging", tagging)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-tags", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-tags"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		return err
	}
	valid := "<Tagging><TagSet><Tag><Key>one</Key><Value>1</Value></Tag><Tag><Key>two</Key><Value>2</Value></Tag></TagSet></Tagging>"
	if err := post("valid", valid); err != nil {
		t.Fatal(err)
	}
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "post-tags", "Key": "valid"}, nil).Output["TagSet"].([]any)
	if len(tags) != 2 || asMapForTest(tags[0])["Key"] != "one" || asMapForTest(tags[1])["Key"] != "two" {
		t.Fatalf("tags = %#v", tags)
	}
	characterization := map[string]any{"valid": tags}
	wrongRoot := "<InvalidXmlTagging><TagSet><Tag><Key>ignored</Key><Value>tag</Value></Tag></TagSet></InvalidXmlTagging>"
	if err := post("wrong-root", wrongRoot); err != nil {
		t.Fatal(err)
	}
	if tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "post-tags", "Key": "wrong-root"}, nil).Output["TagSet"].([]any); len(tags) != 0 {
		t.Fatalf("wrong-root tags = %#v", tags)
	} else {
		characterization["wrong root"] = tags
	}
	duplicate := "<Tagging><TagSet><Tag><Key>same</Key><Value>first</Value></Tag><Tag><Key>same</Key><Value>last</Value></Tag></TagSet></Tagging>"
	if err := post("duplicate", duplicate); err != nil {
		t.Fatal(err)
	}
	if tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "post-tags", "Key": "duplicate"}, nil).Output["TagSet"].([]any); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "last" {
		t.Fatalf("duplicate tags = %#v", tags)
	} else {
		characterization["duplicate"] = tags
	}
	if fault := asFault(t, post("malformed", "not-xml")); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("fault = %+v", fault)
	} else {
		characterization["malformed"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus}
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-tags", "Key": "malformed"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("malformed tagging stored object")
	}
	missingValue := "<Tagging><TagSet><Tag><Key>key</Key></Tag></TagSet></Tagging>"
	if fault := asFault(t, post("missing-value", missingValue)); fault.Code != "MalformedXML" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing value fault = %+v", fault)
	} else {
		characterization["missing value"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus}
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-tags", "Key": "missing-value"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("missing tag value stored object")
	}
	golden.AssertJSON(t, characterization)
}

func TestPostObjectExpires(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-expires"}, nil)
	post := func(key, expires string) error {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", key)
		_ = writer.WriteField("Expires", expires)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-expires", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-expires"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		return err
	}
	expires := "Thu, 27 Aug 2026 12:00:00 GMT"
	if err := post("valid", expires); err != nil {
		t.Fatal(err)
	}
	got := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "post-expires", "Key": "valid"}, nil).Headers.Get("Expires")
	if got != expires {
		t.Fatalf("Expires = %q", got)
	}
	characterization := map[string]any{"valid": got}
	fault := asFault(t, post("invalid", "tomorrow"))
	if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "Expires" {
		t.Fatalf("fault = %+v", fault)
	}
	characterization["invalid"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus, "fields": fault.Fields}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-expires", "Key": "invalid"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("invalid Expires stored object")
	}
	golden.AssertJSON(t, characterization)
}

func TestPostObjectChecksums(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-checksums"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "post-checksums", "Status": "Enabled"}, nil)
	post := func(key, algorithm, checksum string) (*spi.Response, error) {
		t.Helper()
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", key)
		_ = writer.WriteField("x-amz-checksum-algorithm", algorithm)
		if checksum != "" {
			_ = writer.WriteField("x-amz-checksum-"+strings.ToLower(algorithm), checksum)
		}
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("123456789"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-checksums", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		return p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-checksums"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
	}
	body := []byte("123456789")
	crc32sum, crc32csum := make([]byte, 4), make([]byte, 4)
	binary.BigEndian.PutUint32(crc32sum, crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(crc32csum, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	sha1sum, sha256sum := sha1.Sum(body), sha256.Sum256(body)
	b64 := func(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }
	want := map[string]string{
		"CRC32": b64(crc32sum), "CRC32C": b64(crc32csum), "CRC64NVME": "rosUhgp5mIg=",
		"SHA1": b64(sha1sum[:]), "SHA256": b64(sha256sum[:]),
	}
	characterization := map[string]any{}
	for algorithm, checksum := range want {
		response, err := post(strings.ToLower(algorithm), algorithm, "")
		if err != nil {
			t.Fatal(err)
		}
		header := "x-amz-checksum-" + strings.ToLower(algorithm)
		if response.Headers.Get(header) != checksum || response.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" || response.Headers.Get("x-amz-version-id") == "" {
			t.Fatalf("headers = %v", response.Headers)
		}
		head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "post-checksums", "Key": strings.ToLower(algorithm), "ChecksumMode": "ENABLED"}, nil)
		if head.Headers.Get(header) != checksum {
			t.Fatalf("stored checksum = %q", head.Headers.Get(header))
		}
		characterization[algorithm] = map[string]any{"checksum": checksum, "type": response.Headers.Get("x-amz-checksum-type"), "versioned": response.Headers.Get("x-amz-version-id") != ""}
	}
	if _, err := post("provided", "CRC32", want["CRC32"]); err != nil {
		t.Fatal(err)
	}
	_, err := post("invalid", "CRC32", "AAAAAA==")
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("fault = %+v", fault)
	} else {
		characterization["invalid value"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus, "message": fault.Message}
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-checksums", "Key": "invalid"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("invalid checksum stored object")
	}
	_, err = post("unsupported", "SHA512", "")
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("unsupported fault = %+v", fault)
	} else {
		characterization["unsupported algorithm"] = map[string]any{"code": fault.Code, "status": fault.HTTPStatus}
	}
	golden.AssertJSON(t, characterization)
}

func TestObjectCreatedEventNames(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	var events []string
	cancel := deps.Bus.(*bus.Memory).Subscribe("s3:events", func(_ context.Context, payload []byte) {
		var envelope map[string]any
		_ = json.Unmarshal(payload, &envelope)
		records := envelope["Records"].([]any)
		events = append(events, records[0].(map[string]any)["eventName"].(string))
	})
	defer cancel()
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "events"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "events", "Key": "source"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "events", "Key": "copy", "CopySource": "events/source"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "events", "Key": "multipart"}, nil)
	uploadID := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte("part"))
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)
	_ = writer.WriteField("key", "post")
	file, _ := writer.CreateFormFile("file", "post.txt")
	_, _ = file.Write([]byte("post"))
	_ = writer.Close()
	httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/events", &payload)
	httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "events"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest}); err != nil {
		t.Fatal(err)
	}
	want := []string{"ObjectCreated:Put", "ObjectCreated:Copy", "ObjectCreated:CompleteMultipartUpload", "ObjectCreated:Post"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}
}

func TestReplicationFiltersStatusMetadataAndDeleteMarker(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	west := ident()
	west.Region = "us-west-2"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source", "ObjectLockEnabledForBucket": true}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "source", "Status": "Enabled"}, nil)
	mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": "destination", "LocationConstraint": "us-west-2", "ObjectLockEnabledForBucket": true}, nil)
	mustInvokeAs(t, p, west, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutObjectLockConfiguration", map[string]any{"Bucket": "source", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "GOVERNANCE", "Days": 2}}}}, nil)
	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source",
		"ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{
			"Priority": 1, "Status": "Enabled",
			"Filter": map[string]any{"And": map[string]any{
				"Prefix": "logs/",
				"Tags":   []any{map[string]any{"Key": "environment", "Value": "test"}},
			}},
			"DeleteMarkerReplication": map[string]any{"Status": "Disabled"},
			"Destination":             map[string]any{"Bucket": "arn:aws:s3:::destination", "StorageClass": "STANDARD_IA"},
		}}},
	}, nil)

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "other/file", "Tagging": "environment=test"}, []byte("skip"))
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "other/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("unmatched object was replicated: %v", err)
	}

	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/file", "Tagging": "environment=test"}, []byte("replicated"))
	version := put.Headers.Get("x-amz-version-id")
	if got := put.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("source replication status %q", got)
	}
	sourceVersion := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "source", "Key": "logs/file", "VersionId": version}, nil)
	_ = sourceVersion.Stream.Close()
	if got := sourceVersion.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("source version replication status %q", got)
	}
	dst := mustInvokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file", "VersionId": version}, nil)
	if got := string(readStream(t, dst)); got != "replicated" {
		t.Fatalf("replica body %q", got)
	}
	if got := dst.Headers.Get("x-amz-replication-status"); got != "REPLICA" {
		t.Fatalf("destination replication status %q", got)
	}
	if got := dst.Headers.Get("x-amz-storage-class"); got != "STANDARD_IA" {
		t.Fatalf("destination storage class %q", got)
	}
	replicatedRetention := mustInvokeAs(t, p, west, "GetObjectRetention", map[string]any{"Bucket": "destination", "Key": "logs/file", "VersionId": version}, nil)
	if got := asMapForTest(replicatedRetention.Output["Retention"]); got["Mode"] != "GOVERNANCE" || got["RetainUntilDate"] != "1970-01-03T00:00:00Z" {
		t.Fatalf("replica retention %v", replicatedRetention.Output)
	}

	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "source", "Key": "logs/file", "TagSet": []any{
		map[string]any{"Key": "environment", "Value": "test"},
		map[string]any{"Key": "owner", "Value": "mirror"},
	}}, nil)
	tags := mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	gotTags := tags.Output["TagSet"].([]any)
	if len(gotTags) != 2 || gotTags[1].(map[string]any)["Value"] != "mirror" {
		t.Fatalf("replica tags %v", gotTags)
	}
	versionTags := mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "logs/file", "VersionId": version}, nil)
	if got := asSliceForTest(versionTags.Output["TagSet"]); len(got) != 2 || got[1].(map[string]any)["Value"] != "mirror" {
		t.Fatalf("replica version tags %v", got)
	}
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "source", "Key": "logs/file", "LegalHold": map[string]any{"Status": "ON"}}, nil)
	legalHold := mustInvokeAs(t, p, west, "GetObjectLegalHold", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	if got := asMapForTest(legalHold.Output["LegalHold"])["Status"]; got != "ON" {
		t.Fatalf("replica legal hold %v", legalHold.Output)
	}

	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source",
		"ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{
			"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Prefix": "logs/"},
			"DeleteMarkerReplication": map[string]any{"Status": "Enabled"},
			"Destination":             map[string]any{"Bucket": "arn:aws:s3:::destination", "StorageClass": "STANDARD_IA"},
		}}},
	}, nil)
	del := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "source", "Key": "logs/file"}, nil)
	deleteVersion := del.Headers.Get("x-amz-version-id")
	if got := del.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("delete-marker replication status %q", got)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("replica delete marker not visible: %v", err)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file", "VersionId": deleteVersion}, nil); asFault(t, err).Code != "MethodNotAllowed" {
		t.Fatalf("replica delete-marker version not visible: %v", err)
	}
	listed := mustInvokeAs(t, p, west, "ListObjectVersions", map[string]any{"Bucket": "destination"}, nil)
	if len(asSliceForTest(listed.Output["Versions"])) != 1 || len(asSliceForTest(listed.Output["DeleteMarkers"])) != 1 {
		t.Fatalf("replica versions %#v", listed.Output)
	}
	mustInvokeAs(t, p, west, "DeleteObject", map[string]any{"Bucket": "destination", "Key": "logs/file", "VersionId": deleteVersion}, nil)
	restored := mustInvokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	restoredBody := string(readStream(t, restored))
	if restoredBody != "replicated" || restored.Headers.Get("x-amz-version-id") != version {
		t.Fatalf("restored replica body=%q headers=%v", restoredBody, restored.Headers)
	}
	golden.AssertJSON(t, map[string]any{"objectVersion": version, "deleteVersion": deleteVersion, "listed": listed.Output, "restoredBody": restoredBody})

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/batch", "Tagging": "environment=test"}, []byte("batch"))
	deleted := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "source", "Objects": []any{map[string]any{"Key": "logs/batch"}}}, nil)
	if got := deleted.Output["Deleted"].([]any)[0].(map[string]any)["DeleteMarker"]; got != true {
		t.Fatalf("batch delete marker %v", deleted.Output)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/batch"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("batch replica delete marker not visible: %v", err)
	}

	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source", "ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{
			"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Prefix": "plain/"},
			"DeleteMarkerReplication": map[string]any{"Status": "Disabled"},
			"Destination":             map[string]any{"Bucket": "arn:aws:s3:::destination"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file", "Tagging": "owner=mirror"}, []byte("tagged"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file"}, []byte("untagged"))
	if tags := asSliceForTest(mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "plain/file"}, nil).Output["TagSet"]); len(tags) != 0 {
		t.Fatalf("replica inherited overwritten tags: %#v", tags)
	}
}

func asMapForTest(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func asSliceForTest(value any) []any {
	result, _ := value.([]any)
	return result
}
