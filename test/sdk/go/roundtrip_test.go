package sdk_test

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestAWSSDKRoundTripS3DynamoDBSQS(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3", "aws.dynamodb", "aws.sqs"}
	cfg.Seed = "sdk-rt"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	awscfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}

	s3c := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("hello-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32,
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if string(body) != "hello-sdk" {
		t.Fatalf("s3 body %q", body)
	}
	verified, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get checksum: %v", err)
	}
	_, _ = io.Copy(io.Discard, verified.Body)
	_ = verified.Body.Close()
	if aws.ToString(put.ChecksumCRC32) == "" || aws.ToString(verified.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("get checksum %q want %q", aws.ToString(verified.ChecksumCRC32), aws.ToString(put.ChecksumCRC32))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: got.ETag,
	}); err != nil {
		t.Fatalf("conditional copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("rejected"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: aws.String(`"wrong"`),
	}); err == nil {
		t.Fatal("conditional copy with wrong ETag succeeded")
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("newer")),
	}); err != nil {
		t.Fatalf("put newer version: %v", err)
	}
	original, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("get original version: %v", err)
	}
	originalBody, _ := io.ReadAll(original.Body)
	_ = original.Body.Close()
	if string(originalBody) != "hello-sdk" || aws.ToString(original.VersionId) != aws.ToString(put.VersionId) || aws.ToString(original.ETag) != aws.ToString(put.ETag) || aws.ToString(original.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("original version body=%q version=%q etag=%q checksum=%q", originalBody, aws.ToString(original.VersionId), aws.ToString(original.ETag), aws.ToString(original.ChecksumCRC32))
	}
	head, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil || aws.ToString(head.VersionId) != aws.ToString(put.VersionId) || aws.ToString(head.ETag) != aws.ToString(put.ETag) || aws.ToString(head.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("head original version: %#v %v", head, err)
	}
	versionCopy, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("version-copy"), CopySource: aws.String("sdk/k?versionId=" + aws.ToString(put.VersionId)),
	})
	if err != nil {
		t.Fatalf("copy version: %v", err)
	}
	if aws.ToString(versionCopy.CopySourceVersionId) != aws.ToString(put.VersionId) {
		t.Fatalf("copy source version %q want %q", aws.ToString(versionCopy.CopySourceVersionId), aws.ToString(put.VersionId))
	}
	versioned, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("version-copy")})
	if err != nil {
		t.Fatalf("get version copy: %v", err)
	}
	versionedBody, _ := io.ReadAll(versioned.Body)
	_ = versioned.Body.Close()
	if string(versionedBody) != "hello-sdk" {
		t.Fatalf("version copy body %q", versionedBody)
	}
	large := bytes.Repeat([]byte("0123456789"), 600000)
	largePut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("large"), Body: bytes.NewReader(large),
	})
	if err != nil {
		t.Fatalf("put large source: %v", err)
	}
	upload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"),
	})
	if err != nil {
		t.Fatalf("create multipart copy: %v", err)
	}
	listedUploads, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String("sdk"), Prefix: aws.String("range-"), MaxUploads: aws.Int32(1),
	})
	if err != nil || len(listedUploads.Uploads) != 1 || aws.ToString(listedUploads.Uploads[0].Key) != "range-copy" || aws.ToString(listedUploads.Uploads[0].UploadId) != aws.ToString(upload.UploadId) {
		t.Fatalf("list multipart uploads: %#v %v", listedUploads, err)
	}
	part, err := s3c.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1),
		CopySource: aws.String("sdk/large"), CopySourceIfMatch: largePut.ETag, CopySourceRange: aws.String("bytes=10-19"),
	})
	if err != nil {
		t.Fatalf("upload part copy: %v", err)
	}
	listedParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, MaxParts: aws.Int32(1),
	})
	if err != nil || len(listedParts.Parts) != 1 || aws.ToInt32(listedParts.Parts[0].PartNumber) != 1 || aws.ToString(listedParts.Parts[0].ETag) != aws.ToString(part.CopyPartResult.ETag) || aws.ToString(listedParts.UploadId) != aws.ToString(upload.UploadId) {
		t.Fatalf("list parts: %#v %v", listedParts, err)
	}
	completed, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.CopyPartResult.ETag}}},
	})
	if err != nil {
		t.Fatalf("complete multipart copy: %v", err)
	}
	rangeCopy, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy")})
	if err != nil {
		t.Fatalf("get range copy: %v", err)
	}
	rangeBody, _ := io.ReadAll(rangeCopy.Body)
	_ = rangeCopy.Body.Close()
	if string(rangeBody) != "0123456789" {
		t.Fatalf("range copy body %q", rangeBody)
	}
	if aws.ToString(rangeCopy.ETag) != aws.ToString(completed.ETag) {
		t.Fatalf("persisted multipart ETag %q want %q", aws.ToString(rangeCopy.ETag), aws.ToString(completed.ETag))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("multipart-copy"), CopySource: aws.String("sdk/range-copy"), CopySourceIfMatch: completed.ETag,
	}); err != nil {
		t.Fatalf("copy multipart ETag: %v", err)
	}

	ddb := dynamodb.NewFromConfig(awscfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(ts.URL) })
	if _, err := ddb.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("T"),
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ddb.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("T"),
		Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
	}); err != nil {
		t.Fatalf("put item: %v", err)
	}
	item, err := ddb.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("T"),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
	})
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	idAttr, ok := item.Item["id"].(*ddbtypes.AttributeValueMemberS)
	if !ok || idAttr.Value != "1" {
		t.Fatalf("ddb item %#v", item.Item)
	}

	sqsc := sqs.NewFromConfig(awscfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(ts.URL) })
	if _, err := sqsc.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("q")}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := sqsc.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(ts.URL + "/000000000000/q"), MessageBody: aws.String("hello-sqs-sdk"),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	recv, err := sqsc.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(ts.URL + "/000000000000/q"), MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if len(recv.Messages) != 1 || aws.ToString(recv.Messages[0].Body) != "hello-sqs-sdk" {
		t.Fatalf("recv %#v", recv.Messages)
	}
}
