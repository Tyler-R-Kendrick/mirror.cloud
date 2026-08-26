package sdk_test

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	if _, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchTagSet") {
		t.Fatalf("untagged bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("hello-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32, Tagging: aws.String("stage=original"), ContentType: aws.String("text/plain"), CacheControl: aws.String("max-age=60"), Metadata: map[string]string{"owner": "mirror"}, WebsiteRedirectLocation: aws.String("/old"),
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
	if aws.ToString(got.ContentType) != "text/plain" || aws.ToString(got.CacheControl) != "max-age=60" || got.Metadata["owner"] != "mirror" || aws.ToString(got.WebsiteRedirectLocation) != "/old" {
		t.Fatalf("s3 metadata %#v", got)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ExpectedBucketOwner: aws.String("000000000000")}); err != nil {
		t.Fatalf("matching expected owner: %v", err)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "StatusCode: 403") {
		t.Fatalf("mismatched expected owner: %v", err)
	}
	if _, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk"), ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("versioning mismatched expected owner: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("missing"), Key: aws.String("k")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("delete missing bucket: %v", err)
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
		Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: got.ETag, ExpectedSourceBucketOwner: aws.String("000000000000"),
	}); err != nil {
		t.Fatalf("conditional copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("source-owner-denied"), CopySource: aws.String("sdk/k"), ExpectedSourceBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("mismatched expected source owner: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("missing-source"), CopySource: aws.String("missing/k")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("copy missing source bucket: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("rejected"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: aws.String(`"wrong"`),
	}); err == nil {
		t.Fatal("conditional copy with wrong ETag succeeded")
	}
	newer, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("newer")),
	})
	if err != nil {
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
	past := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfMatch: put.ETag, IfUnmodifiedSince: &past}); err != nil {
		t.Fatalf("conditional head precedence: %v", err)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfMatch: aws.String(`"wrong"`)}); err == nil {
		t.Fatal("conditional head with wrong ETag succeeded")
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfNoneMatch: put.ETag}); err == nil {
		t.Fatal("conditional get with matching If-None-Match succeeded")
	}
	ranged, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=-3"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("suffix range: %v", err)
	}
	suffixBody, _ := io.ReadAll(ranged.Body)
	_ = ranged.Body.Close()
	if string(suffixBody) != "sdk" || aws.ToString(ranged.ContentRange) != "bytes 6-8/9" || aws.ToInt64(ranged.ContentLength) != 3 || aws.ToString(ranged.ChecksumCRC32) != "" {
		t.Fatalf("suffix range body=%q output=%#v", suffixBody, ranged)
	}
	headRange, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=1-3")})
	if err != nil || aws.ToInt64(headRange.ContentLength) != 3 {
		t.Fatalf("head range: %#v %v", headRange, err)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=9-")}); err == nil {
		t.Fatal("unsatisfiable range succeeded")
	}
	originalTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId})
	if err != nil || aws.ToString(originalTags.VersionId) != aws.ToString(put.VersionId) || len(originalTags.TagSet) != 1 || aws.ToString(originalTags.TagSet[0].Key) != "stage" || aws.ToString(originalTags.TagSet[0].Value) != "original" {
		t.Fatalf("get original tags: %#v %v", originalTags, err)
	}
	tagged, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{{Key: aws.String("stage"), Value: aws.String("newer")}}},
	})
	if err != nil || aws.ToString(tagged.VersionId) != aws.ToString(newer.VersionId) {
		t.Fatalf("tag newer version: %#v %v", tagged, err)
	}
	if _, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{{Key: aws.String("duplicate"), Value: aws.String("one")}, {Key: aws.String("duplicate"), Value: aws.String("two")}}},
	}); err == nil || !strings.Contains(err.Error(), "InvalidTag") {
		t.Fatalf("duplicate object tags: %v", err)
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
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("bad-metadata-directive"), CopySource: aws.String("sdk/k"), MetadataDirective: s3types.MetadataDirective("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid metadata directive: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("bad-tagging-directive"), CopySource: aws.String("sdk/k"), TaggingDirective: s3types.TaggingDirective("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid tagging directive: %v", err)
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
	versionCopyTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("version-copy")})
	if err != nil || len(versionCopyTags.TagSet) != 1 || aws.ToString(versionCopyTags.TagSet[0].Value) != "original" {
		t.Fatalf("version copy tags: %#v %v", versionCopyTags, err)
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
	if _, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("list parts mismatched expected owner: %v", err)
	}
	if _, err := s3c.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), CopySource: aws.String("sdk/large"), ExpectedSourceBucketOwner: aws.String("999999999999"),
	}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("multipart mismatched expected source owner: %v", err)
	}
	part, err := s3c.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1),
		CopySource: aws.String("sdk/large"), CopySourceIfMatch: largePut.ETag, CopySourceRange: aws.String("bytes=10-19"), ExpectedSourceBucketOwner: aws.String("000000000000"),
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
	checksumUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		StorageClass: s3types.StorageClassStandardIa, Tagging: aws.String("env=test&team=storage"),
	})
	if err != nil {
		t.Fatalf("create checksum multipart: %v", err)
	}
	checksumPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("checksum-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil || aws.ToString(checksumPart.ChecksumSHA256) == "" {
		t.Fatalf("upload checksum part: %#v %v", checksumPart, err)
	}
	checksumParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
	})
	if err != nil || checksumParts.ChecksumAlgorithm != s3types.ChecksumAlgorithmSha256 || len(checksumParts.Parts) != 1 || aws.ToString(checksumParts.Parts[0].ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("list checksum parts: %#v %v", checksumParts, err)
	}
	checksumComplete, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: checksumPart.ETag, ChecksumSHA256: checksumPart.ChecksumSHA256}}},
	})
	if err != nil || !strings.HasSuffix(aws.ToString(checksumComplete.ChecksumSHA256), "-1") {
		t.Fatalf("complete checksum multipart: %#v %v", checksumComplete, err)
	}
	checksumHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil || aws.ToString(checksumHead.ChecksumSHA256) != aws.ToString(checksumComplete.ChecksumSHA256) || checksumHead.StorageClass != s3types.StorageClassStandardIa {
		t.Fatalf("head checksum multipart: %#v %v", checksumHead, err)
	}
	checksumPartGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), PartNumber: aws.Int32(1), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("get multipart part: %v", err)
	}
	checksumPartBody, _ := io.ReadAll(checksumPartGet.Body)
	_ = checksumPartGet.Body.Close()
	if string(checksumPartBody) != "checksum-sdk" || aws.ToInt32(checksumPartGet.PartsCount) != 1 || aws.ToString(checksumPartGet.ContentRange) != "bytes 0-11/12" || aws.ToString(checksumPartGet.ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("get multipart part: %#v body=%q", checksumPartGet, checksumPartBody)
	}
	checksumPartHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), PartNumber: aws.Int32(1), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil || aws.ToInt32(checksumPartHead.PartsCount) != 1 || aws.ToInt64(checksumPartHead.ContentLength) != 12 || aws.ToString(checksumPartHead.ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("head multipart part: %#v %v", checksumPartHead, err)
	}
	checksumAttributes, err := s3c.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), MaxParts: aws.Int32(1),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesEtag, s3types.ObjectAttributesChecksum, s3types.ObjectAttributesObjectParts, s3types.ObjectAttributesStorageClass, s3types.ObjectAttributesObjectSize},
	})
	wantAttributeChecksum := strings.SplitN(aws.ToString(checksumComplete.ChecksumSHA256), "-", 2)[0]
	if err != nil || aws.ToString(checksumAttributes.ETag) != aws.ToString(checksumComplete.ETag) || aws.ToInt64(checksumAttributes.ObjectSize) != 12 || checksumAttributes.StorageClass != s3types.StorageClassStandardIa || checksumAttributes.LastModified == nil || checksumAttributes.Checksum == nil || aws.ToString(checksumAttributes.Checksum.ChecksumSHA256) != wantAttributeChecksum || checksumAttributes.ObjectParts == nil || aws.ToInt32(checksumAttributes.ObjectParts.TotalPartsCount) != 1 || len(checksumAttributes.ObjectParts.Parts) != 1 || aws.ToInt32(checksumAttributes.ObjectParts.Parts[0].PartNumber) != 1 || aws.ToInt64(checksumAttributes.ObjectParts.Parts[0].Size) != 12 || aws.ToString(checksumAttributes.ObjectParts.Parts[0].ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("get object attributes: etag=%q size=%d class=%q modified=%v checksum=%q parts=%#v err=%v", aws.ToString(checksumAttributes.ETag), aws.ToInt64(checksumAttributes.ObjectSize), checksumAttributes.StorageClass, checksumAttributes.LastModified, aws.ToString(checksumAttributes.Checksum.ChecksumSHA256), checksumAttributes.ObjectParts, err)
	}
	checksumTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart")})
	if err != nil || len(checksumTags.TagSet) != 2 || aws.ToString(checksumTags.TagSet[0].Key) != "env" || aws.ToString(checksumTags.TagSet[0].Value) != "test" || aws.ToString(checksumTags.TagSet[1].Key) != "team" || aws.ToString(checksumTags.TagSet[1].Value) != "storage" {
		t.Fatalf("multipart creation tags: %#v %v", checksumTags, err)
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
