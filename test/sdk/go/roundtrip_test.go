package sdk_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http/httptest"
	"reflect"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

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
	for _, name := range []string{"ab", "192.168.5.4", "reserved--table-s3"} {
		if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
			t.Fatalf("invalid bucket name %q: %v", name, err)
		}
	}
	createTags := []s3types.Tag{{Key: aws.String("team"), Value: aws.String("storage")}, {Key: aws.String("env"), Value: aws.String("test")}}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{Tags: createTags}, ObjectOwnership: s3types.ObjectOwnershipBucketOwnerPreferred}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if tagged, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String("sdk")}); err != nil || !reflect.DeepEqual(tagged.TagSet, createTags) {
		t.Fatalf("create bucket tags: %#v %v", tagged, err)
	}
	if _, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucketPolicy") {
		t.Fatalf("default bucket policy: %v", err)
	}
	bucketPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::sdk/*"}]}`
	if _, err := s3c.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String("sdk"), Policy: aws.String(bucketPolicy)}); err != nil {
		t.Fatalf("put bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(got.Policy) != bucketPolicy {
		t.Fatalf("bucket policy round trip: %#v %v", got, err)
	}
	if _, err := s3c.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String("sdk"), Policy: aws.String(" " + bucketPolicy)}); err == nil || !strings.Contains(err.Error(), "MalformedPolicy") {
		t.Fatalf("invalid bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(got.Policy) != bucketPolicy {
		t.Fatalf("invalid policy replaced configuration: %#v %v", got, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket policy: %v", err)
		}
	}
	if _, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucketPolicy") {
		t.Fatalf("get deleted bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || got.ServerSideEncryptionConfiguration != nil {
		t.Fatalf("default bucket encryption: %#v %v", got, err)
	}
	bucketEncryptionKey := "arn:aws:kms:us-east-1:000000000000:key/sdk-default"
	kmsIdentity := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Enabled")
	bucketEncryption := &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{
		ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAwsKms, KMSMasterKeyID: aws.String(bucketEncryptionKey)},
		BucketKeyEnabled:                   aws.Bool(true),
	}}}
	if _, err := s3c.PutBucketEncryption(context.Background(), &s3.PutBucketEncryptionInput{Bucket: aws.String("sdk"), ServerSideEncryptionConfiguration: bucketEncryption}); err != nil {
		t.Fatalf("put bucket encryption: %v", err)
	}
	gotEncryption, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")})
	if err != nil || gotEncryption.ServerSideEncryptionConfiguration == nil || len(gotEncryption.ServerSideEncryptionConfiguration.Rules) != 1 || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault == nil || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAwsKms || aws.ToString(gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID) != bucketEncryptionKey || !aws.ToBool(gotEncryption.ServerSideEncryptionConfiguration.Rules[0].BucketKeyEnabled) {
		t.Fatalf("bucket encryption round trip: %#v %v", gotEncryption, err)
	}
	inheritedEncryption, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-bucket-encryption"), Body: strings.NewReader("encrypted")})
	if err != nil || inheritedEncryption.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(inheritedEncryption.SSEKMSKeyId) != bucketEncryptionKey || !aws.ToBool(inheritedEncryption.BucketKeyEnabled) {
		t.Fatalf("inherited bucket encryption: %#v %v", inheritedEncryption, err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-explicit-kms"), Body: strings.NewReader("encrypted"), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String(bucketEncryptionKey)}); err != nil {
		t.Fatalf("explicit kms put: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-missing-kms"), Body: strings.NewReader("rejected"), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("arn:aws:kms:us-east-1:000000000000:key/missing")}); err == nil || !strings.Contains(err.Error(), "KMS.NotFoundException") {
		t.Fatalf("missing kms key: %v", err)
	}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Disabled")
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-explicit-kms")}); err == nil || !strings.Contains(err.Error(), "KMS.DisabledException") {
		t.Fatalf("disabled kms read: %v", err)
	}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Enabled")
	invalidEncryption := &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAes256, KMSMasterKeyID: aws.String(bucketEncryptionKey)}}}}
	if _, err := s3c.PutBucketEncryption(context.Background(), &s3.PutBucketEncryptionInput{Bucket: aws.String("sdk"), ServerSideEncryptionConfiguration: invalidEncryption}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket encryption: %v", err)
	}
	if gotEncryption, err = s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || gotEncryption.ServerSideEncryptionConfiguration == nil || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAwsKms {
		t.Fatalf("invalid encryption replaced configuration: %#v %v", gotEncryption, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketEncryption(context.Background(), &s3.DeleteBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket encryption: %v", err)
		}
	}
	if got, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || got.ServerSideEncryptionConfiguration != nil {
		t.Fatalf("deleted bucket encryption: %#v %v", got, err)
	}
	bucketACL, err := s3c.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String("sdk")})
	if err != nil || bucketACL.Owner == nil || aws.ToString(bucketACL.Owner.ID) != "000000000000" || len(bucketACL.Grants) != 1 {
		t.Fatalf("default bucket ACL: %#v %v", bucketACL, err)
	}
	if _, err := s3c.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String("sdk"), ACL: s3types.BucketCannedACLPublicRead}); err != nil {
		t.Fatalf("put bucket ACL: %v", err)
	}
	if bucketACL, err = s3c.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String("sdk")}); err != nil || len(bucketACL.Grants) != 2 || aws.ToString(bucketACL.Grants[1].Grantee.URI) != "http://acs.amazonaws.com/groups/global/AllUsers" {
		t.Fatalf("public bucket ACL: %#v %v", bucketACL, err)
	}
	objectACLKey := "sdk-object-acl"
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey), Body: strings.NewReader("acl"), ACL: s3types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatalf("put object ACL: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey)}); err != nil || len(objectACL.Grants) != 2 {
		t.Fatalf("public object ACL: %#v %v", objectACL, err)
	}
	privatePolicy := &s3types.AccessControlPolicy{
		Owner:  &s3types.Owner{ID: bucketACL.Owner.ID, DisplayName: bucketACL.Owner.DisplayName},
		Grants: []s3types.Grant{{Grantee: &s3types.Grantee{ID: bucketACL.Owner.ID, Type: s3types.TypeCanonicalUser}, Permission: s3types.PermissionFullControl}},
	}
	if _, err := s3c.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey), AccessControlPolicy: privatePolicy}); err != nil {
		t.Fatalf("put object ACP: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey)}); err != nil || len(objectACL.Grants) != 1 || objectACL.Grants[0].Permission != s3types.PermissionFullControl {
		t.Fatalf("private object ACP: %#v %v", objectACL, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-acl-marker")}); err != nil {
		t.Fatalf("create acl marker bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-acl-marker"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable acl marker versioning: %v", err)
	}
	object, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), Body: bytes.NewReader([]byte("body"))})
	if err != nil {
		t.Fatalf("put acl marker object: %v", err)
	}
	marker, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object")})
	if err != nil || !aws.ToBool(marker.DeleteMarker) || aws.ToString(marker.VersionId) == "" {
		t.Fatalf("create acl delete marker: %#v %v", marker, err)
	}
	for _, versionID := range []*string{nil, marker.VersionId} {
		if _, err := s3c.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID, ACL: s3types.ObjectCannedACLPublicRead}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
			t.Fatalf("put delete marker acl version=%v: %v", versionID, err)
		}
	}
	if _, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object")}); err == nil || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("get current delete marker acl: %v", err)
	}
	if _, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: marker.VersionId}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
		t.Fatalf("get explicit delete marker acl: %v", err)
	}
	for _, versionID := range []*string{marker.VersionId, object.VersionId} {
		if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID}); err != nil {
			t.Fatalf("delete acl marker version %v: %v", versionID, err)
		}
	}
	if _, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("sdk-acl-marker")}); err != nil {
		t.Fatalf("delete acl marker bucket: %v", err)
	}
	multipartACL, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), ACL: s3types.ObjectCannedACLPublicReadWrite})
	if err != nil {
		t.Fatalf("create multipart ACL: %v", err)
	}
	multipartACLPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), UploadId: multipartACL.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("acl-part")})
	if err != nil {
		t.Fatalf("upload multipart ACL part: %v", err)
	}
	if _, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), UploadId: multipartACL.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: multipartACLPart.ETag}}}}); err != nil {
		t.Fatalf("complete multipart ACL: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl")}); err != nil || len(objectACL.Grants) != 3 {
		t.Fatalf("multipart object ACL: %#v %v", objectACL, err)
	}
	ownership, err := s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")})
	if err != nil || ownership.OwnershipControls == nil {
		t.Fatalf("create bucket ownership rules: %#v %v", ownership.OwnershipControls, err)
	}
	if len(ownership.OwnershipControls.Rules) != 1 {
		t.Fatalf("create bucket ownership rule count = %d", len(ownership.OwnershipControls.Rules))
	}
	if got := ownership.OwnershipControls.Rules[0].ObjectOwnership; got != s3types.ObjectOwnershipBucketOwnerPreferred {
		t.Fatalf("create bucket ownership = %q", got)
	}
	controls := &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnershipObjectWriter}}}
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: controls}); err != nil {
		t.Fatalf("put bucket ownership controls: %v", err)
	}
	ownership, err = s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")})
	if err != nil || ownership.OwnershipControls == nil || len(ownership.OwnershipControls.Rules) != 1 || ownership.OwnershipControls.Rules[0].ObjectOwnership != s3types.ObjectOwnershipObjectWriter {
		t.Fatalf("put bucket ownership round trip: %#v %v", ownership, err)
	}
	invalidControls := &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnership("invalid")}}}
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: invalidControls}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid ownership controls: %v", err)
	}
	if _, err := s3c.DeleteBucketOwnershipControls(context.Background(), &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("delete bucket ownership controls: %v", err)
	}
	if _, err := s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "OwnershipControlsNotFoundError") {
		t.Fatalf("get deleted ownership controls: %v", err)
	}
	controls.Rules[0].ObjectOwnership = s3types.ObjectOwnershipBucketOwnerPreferred
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: controls}); err != nil {
		t.Fatalf("restore bucket ownership controls: %v", err)
	}
	publicAccessBlock := &s3types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(true)}
	if _, err := s3c.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{Bucket: aws.String("sdk"), PublicAccessBlockConfiguration: publicAccessBlock}); err != nil {
		t.Fatalf("put public access block: %v", err)
	}
	blocked, err := s3c.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String("sdk")})
	if err != nil || blocked.PublicAccessBlockConfiguration == nil || !aws.ToBool(blocked.PublicAccessBlockConfiguration.BlockPublicAcls) || aws.ToBool(blocked.PublicAccessBlockConfiguration.BlockPublicPolicy) {
		t.Fatalf("public access block round trip: %#v %v", blocked, err)
	}
	if _, err := s3c.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("delete public access block: %v", err)
	}
	if _, err := s3c.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchPublicAccessBlockConfiguration") {
		t.Fatalf("get deleted public access block: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled != nil {
		t.Fatalf("default bucket logging: %#v %v", logging, err)
	}
	loggingStatus := &s3types.BucketLoggingStatus{LoggingEnabled: &s3types.LoggingEnabled{TargetBucket: aws.String("sdk"), TargetPrefix: aws.String("logs/")}}
	if _, err := s3c.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String("sdk"), BucketLoggingStatus: loggingStatus}); err != nil {
		t.Fatalf("put bucket logging: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled == nil || aws.ToString(logging.LoggingEnabled.TargetBucket) != "sdk" || aws.ToString(logging.LoggingEnabled.TargetPrefix) != "logs/" {
		t.Fatalf("bucket logging round trip: %#v %v", logging, err)
	}
	if _, err := s3c.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String("sdk"), BucketLoggingStatus: &s3types.BucketLoggingStatus{}}); err != nil {
		t.Fatalf("disable bucket logging: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled != nil {
		t.Fatalf("disabled bucket logging: %#v %v", logging, err)
	}
	if _, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchCORSConfiguration") {
		t.Fatalf("default bucket CORS: %v", err)
	}
	cors := &s3types.CORSConfiguration{CORSRules: []s3types.CORSRule{{AllowedMethods: []string{"GET", "HEAD"}, AllowedOrigins: []string{"https://example.test"}, ExposeHeaders: []string{"ETag"}, MaxAgeSeconds: aws.Int32(300)}}}
	if _, err := s3c.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{Bucket: aws.String("sdk"), CORSConfiguration: cors}); err != nil {
		t.Fatalf("put bucket CORS: %v", err)
	}
	if got, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err != nil || len(got.CORSRules) != 1 || !reflect.DeepEqual(got.CORSRules[0].AllowedMethods, []string{"GET", "HEAD"}) || !reflect.DeepEqual(got.CORSRules[0].AllowedOrigins, []string{"https://example.test"}) || aws.ToInt32(got.CORSRules[0].MaxAgeSeconds) != 300 {
		t.Fatalf("bucket CORS round trip: %#v %v", got, err)
	}
	invalidCors := &s3types.CORSConfiguration{CORSRules: []s3types.CORSRule{{AllowedMethods: []string{"OPTIONS"}, AllowedOrigins: []string{"*"}}}}
	if _, err := s3c.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{Bucket: aws.String("sdk"), CORSConfiguration: invalidCors}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("invalid bucket CORS: %v", err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket CORS: %v", err)
		}
	}
	if _, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchCORSConfiguration") {
		t.Fatalf("get deleted bucket CORS: %v", err)
	}
	if _, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchWebsiteConfiguration") {
		t.Fatalf("default bucket website: %v", err)
	}
	website := &s3types.WebsiteConfiguration{
		IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
		ErrorDocument: &s3types.ErrorDocument{Key: aws.String("error.html")},
		RoutingRules: []s3types.RoutingRule{{
			Condition: &s3types.Condition{KeyPrefixEquals: aws.String("docs/")},
			Redirect:  &s3types.Redirect{Protocol: s3types.ProtocolHttps, ReplaceKeyPrefixWith: aws.String("manual/")},
		}},
	}
	if _, err := s3c.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String("sdk"), WebsiteConfiguration: website}); err != nil {
		t.Fatalf("put bucket website: %v", err)
	}
	gotWebsite, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")})
	if err != nil || gotWebsite.IndexDocument == nil || aws.ToString(gotWebsite.IndexDocument.Suffix) != "index.html" || gotWebsite.ErrorDocument == nil || aws.ToString(gotWebsite.ErrorDocument.Key) != "error.html" || len(gotWebsite.RoutingRules) != 1 || gotWebsite.RoutingRules[0].Redirect == nil || gotWebsite.RoutingRules[0].Redirect.Protocol != s3types.ProtocolHttps || aws.ToString(gotWebsite.RoutingRules[0].Redirect.ReplaceKeyPrefixWith) != "manual/" {
		t.Fatalf("bucket website round trip: %#v %v", gotWebsite, err)
	}
	invalidWebsite := &s3types.WebsiteConfiguration{IndexDocument: &s3types.IndexDocument{Suffix: aws.String("dir/index.html")}}
	if _, err := s3c.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String("sdk"), WebsiteConfiguration: invalidWebsite}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket website: %v", err)
	}
	if gotWebsite, err = s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err != nil || gotWebsite.IndexDocument == nil || aws.ToString(gotWebsite.IndexDocument.Suffix) != "index.html" {
		t.Fatalf("invalid website replaced configuration: %#v %v", gotWebsite, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket website: %v", err)
		}
	}
	if _, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchWebsiteConfiguration") {
		t.Fatalf("get deleted bucket website: %v", err)
	}
	if _, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("default bucket lifecycle: %v", err)
	}
	lifecycle := &s3types.BucketLifecycleConfiguration{Rules: []s3types.LifecycleRule{{
		ID: aws.String("expire-images"), Status: s3types.ExpirationStatusEnabled,
		Filter:      &s3types.LifecycleRuleFilter{And: &s3types.LifecycleRuleAndOperator{Prefix: aws.String("images/"), Tags: []s3types.Tag{{Key: aws.String("class"), Value: aws.String("temporary")}}}},
		Expiration:  &s3types.LifecycleExpiration{Days: aws.Int32(7)},
		Transitions: []s3types.Transition{{Days: aws.Int32(1), StorageClass: s3types.TransitionStorageClassGlacier}},
	}}}
	putLifecycle, err := s3c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String("sdk"), LifecycleConfiguration: lifecycle,
		TransitionDefaultMinimumObjectSize: s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass,
	})
	if err != nil || putLifecycle.TransitionDefaultMinimumObjectSize != s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass {
		t.Fatalf("put bucket lifecycle: %#v %v", putLifecycle, err)
	}
	gotLifecycle, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")})
	if err != nil || gotLifecycle.TransitionDefaultMinimumObjectSize != s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass || len(gotLifecycle.Rules) != 1 || aws.ToString(gotLifecycle.Rules[0].ID) != "expire-images" || gotLifecycle.Rules[0].Filter == nil || gotLifecycle.Rules[0].Filter.And == nil || aws.ToString(gotLifecycle.Rules[0].Filter.And.Prefix) != "images/" || len(gotLifecycle.Rules[0].Transitions) != 1 || gotLifecycle.Rules[0].Transitions[0].StorageClass != s3types.TransitionStorageClassGlacier {
		t.Fatalf("bucket lifecycle round trip: %#v %v", gotLifecycle, err)
	}
	putExpiring, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt"), Body: strings.NewReader("photo"), Tagging: aws.String("class=temporary")})
	if err != nil || !strings.Contains(aws.ToString(putExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("put lifecycle expiration: %#v %v", putExpiring, err)
	}
	getExpiring, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt")})
	if err != nil || !strings.Contains(aws.ToString(getExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("get lifecycle expiration: %#v %v", getExpiring, err)
	}
	_ = getExpiring.Body.Close()
	headExpiring, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt")})
	if err != nil || !strings.Contains(aws.ToString(headExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("head lifecycle expiration: %#v %v", headExpiring, err)
	}
	invalidLifecycle := &s3types.BucketLifecycleConfiguration{Rules: []s3types.LifecycleRule{{ID: aws.String("invalid"), Status: s3types.ExpirationStatusEnabled, Filter: &s3types.LifecycleRuleFilter{Prefix: aws.String("a"), Tag: &s3types.Tag{Key: aws.String("k"), Value: aws.String("v")}}}}}
	if _, err := s3c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String("sdk"), LifecycleConfiguration: invalidLifecycle}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid bucket lifecycle: %v", err)
	}
	if gotLifecycle, err = s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(gotLifecycle.Rules[0].ID) != "expire-images" {
		t.Fatalf("invalid lifecycle replaced configuration: %#v %v", gotLifecycle, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket lifecycle: %v", err)
		}
	}
	if _, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("get deleted bucket lifecycle: %v", err)
	}
	analytics := &s3types.AnalyticsConfiguration{
		Id: aws.String("analysis"), Filter: &s3types.AnalyticsFilterMemberPrefix{Value: "logs/"},
		StorageClassAnalysis: &s3types.StorageClassAnalysis{DataExport: &s3types.StorageClassAnalysisDataExport{
			OutputSchemaVersion: s3types.StorageClassAnalysisSchemaVersionV1,
			Destination: &s3types.AnalyticsExportDestination{S3BucketDestination: &s3types.AnalyticsS3BucketDestination{
				Bucket: aws.String("arn:aws:s3:::sdk"), Format: s3types.AnalyticsS3ExportFileFormatCsv,
			}},
		}},
	}
	if _, err := s3c.PutBucketAnalyticsConfiguration(context.Background(), &s3.PutBucketAnalyticsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("analysis"), AnalyticsConfiguration: analytics}); err != nil {
		t.Fatalf("put analytics configuration: %v", err)
	}
	gotAnalytics, err := s3c.GetBucketAnalyticsConfiguration(context.Background(), &s3.GetBucketAnalyticsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("analysis")})
	if err != nil || gotAnalytics.AnalyticsConfiguration == nil || aws.ToString(gotAnalytics.AnalyticsConfiguration.Id) != "analysis" {
		t.Fatalf("analytics round trip: %#v %v", gotAnalytics, err)
	}
	listedAnalytics, err := s3c.ListBucketAnalyticsConfigurations(context.Background(), &s3.ListBucketAnalyticsConfigurationsInput{Bucket: aws.String("sdk")})
	if err != nil || len(listedAnalytics.AnalyticsConfigurationList) != 1 || aws.ToString(listedAnalytics.AnalyticsConfigurationList[0].Id) != "analysis" {
		t.Fatalf("analytics list: %#v %v", listedAnalytics, err)
	}

	inventory := &s3types.InventoryConfiguration{
		Id: aws.String("inventory"), IsEnabled: aws.Bool(true), IncludedObjectVersions: s3types.InventoryIncludedObjectVersionsAll,
		Destination: &s3types.InventoryDestination{S3BucketDestination: &s3types.InventoryS3BucketDestination{Bucket: aws.String("arn:aws:s3:::sdk"), Format: s3types.InventoryFormatCsv}},
		Schedule:    &s3types.InventorySchedule{Frequency: s3types.InventoryFrequencyDaily}, OptionalFields: []s3types.InventoryOptionalField{s3types.InventoryOptionalFieldSize, s3types.InventoryOptionalFieldETag},
	}
	if _, err := s3c.PutBucketInventoryConfiguration(context.Background(), &s3.PutBucketInventoryConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("inventory"), InventoryConfiguration: inventory}); err != nil {
		t.Fatalf("put inventory configuration: %v", err)
	}
	gotInventory, err := s3c.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("inventory")})
	if err != nil || gotInventory.InventoryConfiguration == nil || !aws.ToBool(gotInventory.InventoryConfiguration.IsEnabled) || !reflect.DeepEqual(gotInventory.InventoryConfiguration.OptionalFields, inventory.OptionalFields) {
		t.Fatalf("inventory round trip: %#v %v", gotInventory, err)
	}

	tiering := &s3types.IntelligentTieringConfiguration{Id: aws.String("tiering"), Status: s3types.IntelligentTieringStatusEnabled, Tierings: []s3types.Tiering{{Days: aws.Int32(90), AccessTier: s3types.IntelligentTieringAccessTierArchiveAccess}}}
	if _, err := s3c.PutBucketIntelligentTieringConfiguration(context.Background(), &s3.PutBucketIntelligentTieringConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("tiering"), IntelligentTieringConfiguration: tiering}); err != nil {
		t.Fatalf("put intelligent tiering configuration: %v", err)
	}
	listedTiering, err := s3c.ListBucketIntelligentTieringConfigurations(context.Background(), &s3.ListBucketIntelligentTieringConfigurationsInput{Bucket: aws.String("sdk")})
	if err != nil || len(listedTiering.IntelligentTieringConfigurationList) != 1 || len(listedTiering.IntelligentTieringConfigurationList[0].Tierings) != 1 || aws.ToInt32(listedTiering.IntelligentTieringConfigurationList[0].Tierings[0].Days) != 90 {
		t.Fatalf("intelligent tiering list: %#v %v", listedTiering, err)
	}

	metrics := &s3types.MetricsConfiguration{Id: aws.String("metrics"), Filter: &s3types.MetricsFilterMemberPrefix{Value: "images/"}}
	if _, err := s3c.PutBucketMetricsConfiguration(context.Background(), &s3.PutBucketMetricsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("metrics"), MetricsConfiguration: metrics}); err != nil {
		t.Fatalf("put metrics configuration: %v", err)
	}
	gotMetrics, err := s3c.GetBucketMetricsConfiguration(context.Background(), &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("metrics")})
	if err != nil || gotMetrics.MetricsConfiguration == nil || aws.ToString(gotMetrics.MetricsConfiguration.Id) != "metrics" {
		t.Fatalf("metrics round trip: %#v %v", gotMetrics, err)
	}
	if got, err := s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(got.QueueConfigurations) != 0 || len(got.TopicConfigurations) != 0 || len(got.LambdaFunctionConfigurations) != 0 || got.EventBridgeConfiguration != nil {
		t.Fatalf("default bucket notifications: %#v %v", got, err)
	}
	notifications := &s3types.NotificationConfiguration{
		QueueConfigurations: []s3types.QueueConfiguration{{
			QueueArn: aws.String("arn:aws:sqs:us-east-1:111111111111:queue"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:*")},
			Filter: &s3types.NotificationConfigurationFilter{Key: &s3types.S3KeyFilter{FilterRules: []s3types.FilterRule{{Name: s3types.FilterRuleName("prefix"), Value: aws.String("images/")}}}},
		}},
		TopicConfigurations:          []s3types.TopicConfiguration{{Id: aws.String("topic"), TopicArn: aws.String("arn:aws:sns:us-east-1:111111111111:topic"), Events: []s3types.Event{s3types.Event("s3:ObjectRemoved:*")}}},
		LambdaFunctionConfigurations: []s3types.LambdaFunctionConfiguration{{LambdaFunctionArn: aws.String("arn:aws:lambda:us-east-1:111111111111:function:handler"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:Put")}}},
		EventBridgeConfiguration:     &s3types.EventBridgeConfiguration{},
	}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: notifications, SkipDestinationValidation: aws.Bool(true)}); err != nil {
		t.Fatalf("put bucket notifications: %v", err)
	}
	gotNotifications, err := s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")})
	if err != nil || len(gotNotifications.QueueConfigurations) != 1 || len(aws.ToString(gotNotifications.QueueConfigurations[0].Id)) != 8 || gotNotifications.QueueConfigurations[0].Filter == nil || gotNotifications.QueueConfigurations[0].Filter.Key == nil || gotNotifications.QueueConfigurations[0].Filter.Key.FilterRules[0].Name != s3types.FilterRuleName("Prefix") || len(gotNotifications.TopicConfigurations) != 1 || aws.ToString(gotNotifications.TopicConfigurations[0].Id) != "topic" || len(gotNotifications.LambdaFunctionConfigurations) != 1 || gotNotifications.EventBridgeConfiguration == nil {
		t.Fatalf("bucket notifications round trip: %#v %v", gotNotifications, err)
	}
	invalidNotifications := &s3types.NotificationConfiguration{QueueConfigurations: []s3types.QueueConfiguration{{QueueArn: aws.String("arn:aws:sns:us-east-1:111111111111:queue"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:*")}}}}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: invalidNotifications}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket notifications: %v", err)
	}
	if gotNotifications, err = s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(gotNotifications.QueueConfigurations) != 1 || aws.ToString(gotNotifications.QueueConfigurations[0].QueueArn) != "arn:aws:sqs:us-east-1:111111111111:queue" {
		t.Fatalf("invalid notifications replaced configuration: %#v %v", gotNotifications, err)
	}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: &s3types.NotificationConfiguration{}}); err != nil {
		t.Fatalf("clear bucket notifications: %v", err)
	}
	if gotNotifications, err = s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(gotNotifications.QueueConfigurations) != 0 || gotNotifications.EventBridgeConfiguration != nil {
		t.Fatalf("cleared bucket notifications: %#v %v", gotNotifications, err)
	}
	if payment, err := s3c.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String("sdk")}); err != nil || payment.Payer != s3types.PayerBucketOwner {
		t.Fatalf("default request payer: %#v %v", payment, err)
	}
	if _, err := s3c.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String("sdk"), RequestPaymentConfiguration: &s3types.RequestPaymentConfiguration{Payer: s3types.PayerRequester}}); err != nil {
		t.Fatalf("put request payer: %v", err)
	}
	if payment, err := s3c.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String("sdk")}); err != nil || payment.Payer != s3types.PayerRequester {
		t.Fatalf("request payer round trip: %#v %v", payment, err)
	}
	invalidPayer := s3types.Payer("Invalid")
	if _, err := s3c.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String("sdk"), RequestPaymentConfiguration: &s3types.RequestPaymentConfiguration{Payer: invalidPayer}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid request payer: %v", err)
	}
	if acceleration, err := s3c.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String("sdk")}); err != nil || acceleration.Status != "" {
		t.Fatalf("default acceleration: %#v %v", acceleration, err)
	}
	if _, err := s3c.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String("sdk"), AccelerateConfiguration: &s3types.AccelerateConfiguration{Status: s3types.BucketAccelerateStatusEnabled}}); err != nil {
		t.Fatalf("put acceleration: %v", err)
	}
	if acceleration, err := s3c.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String("sdk")}); err != nil || acceleration.Status != s3types.BucketAccelerateStatusEnabled {
		t.Fatalf("acceleration round trip: %#v %v", acceleration, err)
	}
	invalidAcceleration := s3types.BucketAccelerateStatus("Invalid")
	if _, err := s3c.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String("sdk"), AccelerateConfiguration: &s3types.AccelerateConfiguration{Status: invalidAcceleration}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid acceleration: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("recreate us-east-1 bucket: %v", err)
	}
	accountRegional := "sdk-account-000000000000-us-east-1-an"
	accountRegionalInput := &s3.CreateBucketInput{Bucket: aws.String(accountRegional), BucketNamespace: s3types.BucketNamespaceAccountRegional}
	if created, err := s3c.CreateBucket(context.Background(), accountRegionalInput); err != nil || aws.ToString(created.Location) != "/"+accountRegional {
		t.Fatalf("create account-regional bucket: %#v %v", created, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), accountRegionalInput); err == nil || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("recreate account-regional bucket: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-account-999999999999-us-east-1-an"), BucketNamespace: s3types.BucketNamespaceAccountRegional}); err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
		t.Fatalf("foreign account-regional suffix: %v", err)
	}
	otherCfg := awscfg
	otherCfg.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("999999999999", "test", ""))
	other := s3.NewFromConfig(otherCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	if _, err := other.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("cross-account bucket collision: %v", err)
	}
	westCfg := awscfg
	westCfg.Region = "us-west-2"
	west := s3.NewFromConfig(westCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	if _, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraintUsWest2}}); err == nil || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("cross-region bucket collision: %v", err)
	}
	if _, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-west-missing")}); err == nil || !strings.Contains(err.Error(), "IllegalLocationConstraintException") {
		t.Fatalf("missing regional location constraint: %v", err)
	}
	if created, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-west"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraintUsWest2}}); err != nil || aws.ToString(created.Location) != ts.URL+"/sdk-west/" {
		t.Fatalf("matching regional location constraint: %#v %v", created, err)
	}
	if location, err := west.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String("sdk-west")}); err != nil || location.LocationConstraint != s3types.BucketLocationConstraintUsWest2 {
		t.Fatalf("stored regional location: %#v %v", location, err)
	}
	if head, err := s3c.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String("sdk-west")}); err != nil || head.AccessPointAlias == nil || aws.ToBool(head.AccessPointAlias) || aws.ToString(head.BucketRegion) != "us-west-2" || aws.ToString(head.BucketArn) != "arn:aws:s3:::sdk-west" {
		t.Fatalf("cross-region head: %#v %v", head, err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-west"), Key: aws.String("cross-region"), Body: strings.NewReader("body")}); err != nil {
		t.Fatalf("cross-region put: %v", err)
	}
	if listed, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-west")}); err != nil || len(listed.Contents) != 1 {
		t.Fatalf("cross-region list: %#v %v", listed, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-invalid-location"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraint("moon-west-1")}}); err == nil || !strings.Contains(err.Error(), "InvalidLocationConstraint") {
		t.Fatalf("invalid location constraint: %v", err)
	}
	unpaginatedBuckets, err := s3c.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil || len(unpaginatedBuckets.Buckets) != 3 {
		t.Fatalf("unpaginated buckets: %#v %v", unpaginatedBuckets, err)
	}
	for _, bucket := range unpaginatedBuckets.Buckets {
		if bucket.CreationDate == nil || bucket.BucketRegion != nil {
			t.Fatalf("unpaginated bucket: %#v", bucket)
		}
	}
	paginator := s3.NewListBucketsPaginator(s3c, &s3.ListBucketsInput{MaxBuckets: aws.Int32(1), Prefix: aws.String("sdk")})
	var pagedNames []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil || aws.ToString(page.Prefix) != "sdk" {
			t.Fatalf("paginated buckets: %#v %v", page, err)
		}
		for _, bucket := range page.Buckets {
			if aws.ToString(bucket.BucketRegion) == "" {
				t.Fatalf("paginated bucket: %#v", bucket)
			}
			pagedNames = append(pagedNames, aws.ToString(bucket.Name))
		}
	}
	if got := strings.Join(pagedNames, ","); got != "sdk,sdk-account-000000000000-us-east-1-an,sdk-west" {
		t.Fatalf("paginated bucket names = %s", got)
	}
	regionalBuckets, err := west.ListBuckets(context.Background(), &s3.ListBucketsInput{BucketRegion: aws.String("us-west-2"), Prefix: aws.String("sdk")})
	if err != nil || len(regionalBuckets.Buckets) != 1 || aws.ToString(regionalBuckets.Buckets[0].Name) != "sdk-west" || aws.ToString(regionalBuckets.Buckets[0].BucketRegion) != "us-west-2" {
		t.Fatalf("regional buckets: %#v %v", regionalBuckets, err)
	}
	if _, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(accountRegional)}); err == nil || !strings.Contains(err.Error(), "NoSuchTagSet") {
		t.Fatalf("untagged bucket: %v", err)
	}
	if versioning, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk")}); err != nil || versioning.Status != "" || versioning.MFADelete != "" {
		t.Fatalf("unset versioning: %#v %v", versioning, err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{}}); err == nil || !strings.Contains(err.Error(), "IllegalVersioningConfigurationException") {
		t.Fatalf("missing versioning status: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatus("Invalid")}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid versioning status: %v", err)
	}
	replicationConfiguration := &s3types.ReplicationConfiguration{
		Role:  aws.String("arn:aws:iam::000000000000:role/replication"),
		Rules: []s3types.ReplicationRule{{Priority: aws.Int32(1), Status: s3types.ReplicationRuleStatusEnabled, Filter: &s3types.ReplicationRuleFilter{Prefix: aws.String("replica/")}, DeleteMarkerReplication: &s3types.DeleteMarkerReplication{Status: s3types.DeleteMarkerReplicationStatusDisabled}, Destination: &s3types.Destination{Bucket: aws.String("arn:aws:s3:::sdk-west")}}},
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("replication without versioning: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("replication to unversioned destination: %v", err)
	}
	if _, err := west.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-west"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable replica versioning: %v", err)
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err != nil {
		t.Fatalf("configure replication: %v", err)
	}
	configuration, err := s3c.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String("sdk")})
	if err != nil || configuration.ReplicationConfiguration == nil || len(configuration.ReplicationConfiguration.Rules) != 1 {
		t.Fatalf("get replication configuration: %v", err)
	}
	replicaPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("replica/versioned"), Body: bytes.NewReader([]byte("replica-sdk")), Tagging: aws.String("stage=replicated")})
	if err != nil {
		t.Fatalf("put replicated version: %v", err)
	}
	replicaGet, err := west.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-west"), Key: aws.String("replica/versioned"), VersionId: replicaPut.VersionId})
	if err != nil {
		t.Fatalf("get replicated version: %v", err)
	}
	replicaBody, _ := io.ReadAll(replicaGet.Body)
	_ = replicaGet.Body.Close()
	if string(replicaBody) != "replica-sdk" || aws.ToString(replicaGet.VersionId) != aws.ToString(replicaPut.VersionId) || replicaGet.ReplicationStatus != s3types.ReplicationStatusReplica {
		t.Fatalf("replicated version body=%q output=%#v", replicaBody, replicaGet)
	}
	put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("hello-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32, Tagging: aws.String("stage=original"), ContentType: aws.String("text/plain"), CacheControl: aws.String("max-age=60"), Metadata: map[string]string{"owner": "mirror"}, WebsiteRedirectLocation: aws.String("/old"),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "BucketNotEmpty") {
		t.Fatalf("delete non-empty bucket: %v", err)
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
	overrideExpiry := time.Date(2015, time.October, 21, 7, 28, 0, 0, time.UTC)
	overridden, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), ResponseCacheControl: aws.String("max-age=74"),
		ResponseContentDisposition: aws.String(`attachment; filename="foo.jpg"`), ResponseContentEncoding: aws.String("identity"),
		ResponseContentLanguage: aws.String("de-DE"), ResponseContentType: aws.String("image/jpeg"), ResponseExpires: aws.Time(overrideExpiry),
	})
	if err != nil {
		t.Fatalf("get response overrides: %v", err)
	}
	overriddenBody, _ := io.ReadAll(overridden.Body)
	_ = overridden.Body.Close()
	if string(overriddenBody) != "hello-sdk" || aws.ToString(overridden.CacheControl) != "max-age=74" || aws.ToString(overridden.ContentDisposition) != `attachment; filename="foo.jpg"` || aws.ToString(overridden.ContentEncoding) != "identity" || aws.ToString(overridden.ContentLanguage) != "de-DE" || aws.ToString(overridden.ContentType) != "image/jpeg" || overridden.Expires == nil || !overridden.Expires.Equal(overrideExpiry) {
		t.Fatalf("response overrides %#v body=%q", overridden, overriddenBody)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("rfc2047"), Body: bytes.NewReader([]byte("metadata")),
		Metadata: map[string]string{"non-ascii": "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=", "fake-encoded": "=?UTF-8?Q?actually-ascii?="},
	}); err != nil {
		t.Fatalf("put rfc2047 metadata: %v", err)
	}
	rfc2047, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("rfc2047")})
	if err != nil || rfc2047.Metadata["non-ascii"] != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || rfc2047.Metadata["fake-encoded"] != "actually-ascii" {
		t.Fatalf("rfc2047 metadata: %#v %v", rfc2047, err)
	}
	customerKey := []byte("0123456789abcdef0123456789abcdef")
	customerKeyDigest := md5.Sum(customerKey)
	customerKey64, customerKeyMD5 := base64.StdEncoding.EncodeToString(customerKey), base64.StdEncoding.EncodeToString(customerKeyDigest[:])
	customerPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted"), Body: bytes.NewReader([]byte("sse-c-sdk")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(customerPut.SSECustomerAlgorithm) != "AES256" || aws.ToString(customerPut.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("put customer encryption: %#v %v", customerPut, err)
	}
	customerGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get customer encryption: %v", err)
	}
	customerBody, _ := io.ReadAll(customerGet.Body)
	_ = customerGet.Body.Close()
	if string(customerBody) != "sse-c-sdk" || aws.ToString(customerGet.SSECustomerAlgorithm) != "AES256" || aws.ToString(customerGet.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("get customer encryption: body=%q output=%#v", customerBody, customerGet)
	}
	customerCopy, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted-copy"), CopySource: aws.String("sdk/customer-encrypted"), CopySourceSSECustomerAlgorithm: aws.String("AES256"), CopySourceSSECustomerKey: aws.String(customerKey64), CopySourceSSECustomerKeyMD5: aws.String(customerKeyMD5), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(customerCopy.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("copy customer encryption: %#v %v", customerCopy, err)
	}
	customerCopyGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted-copy"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get copied customer encryption: %v", err)
	}
	customerCopyBody, _ := io.ReadAll(customerCopyGet.Body)
	_ = customerCopyGet.Body.Close()
	if string(customerCopyBody) != "sse-c-sdk" {
		t.Fatalf("copied customer encryption body=%q", customerCopyBody)
	}
	multipartCustomer, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(multipartCustomer.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("create multipart customer encryption: %#v %v", multipartCustomer, err)
	}
	multipartPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("multipart-sse-c-sdk")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(multipartPart.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("upload multipart customer encryption: %#v %v", multipartPart, err)
	}
	multipartCompleted, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{ETag: multipartPart.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatalf("complete multipart customer encryption: %#v %v", multipartCompleted, err)
	}
	multipartCustomerGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get multipart customer encryption: %v", err)
	}
	multipartCustomerBody, _ := io.ReadAll(multipartCustomerGet.Body)
	_ = multipartCustomerGet.Body.Close()
	if string(multipartCustomerBody) != "multipart-sse-c-sdk" || aws.ToString(multipartCustomerGet.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("get multipart customer encryption: body=%q output=%#v", multipartCustomerBody, multipartCustomerGet)
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
	xxhash128 := "MxGUd+3l3NXpcWQnaB1YYA=="
	xxhashPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash"), Body: bytes.NewReader([]byte("123456789")), ChecksumXXHASH128: aws.String(xxhash128),
	})
	if err != nil || aws.ToString(xxhashPut.ChecksumXXHASH128) != xxhash128 {
		t.Fatalf("put xxhash checksum: %#v %v", xxhashPut, err)
	}
	xxhashGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("xxhash"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get xxhash checksum: %v", err)
	}
	xxhashBody, _ := io.ReadAll(xxhashGet.Body)
	_ = xxhashGet.Body.Close()
	if string(xxhashBody) != "123456789" || aws.ToString(xxhashGet.ChecksumXXHASH128) != xxhash128 {
		t.Fatalf("get xxhash checksum: %#v body=%q", xxhashGet, xxhashBody)
	}
	xxhashUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), ChecksumAlgorithm: s3types.ChecksumAlgorithmXxhash3,
	})
	if err != nil {
		t.Fatalf("create xxhash multipart: %v", err)
	}
	xxhash3 := "ctyxi2ehff8="
	xxhashPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), UploadId: xxhashUpload.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("123456789")), ChecksumXXHASH3: aws.String(xxhash3),
	})
	if err != nil || aws.ToString(xxhashPart.ChecksumXXHASH3) != xxhash3 {
		t.Fatalf("upload xxhash part: %#v %v", xxhashPart, err)
	}
	xxhashComposite := "ksPmtVIgSbU=-1"
	xxhashComplete, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), UploadId: xxhashUpload.UploadId, ChecksumXXHASH3: aws.String(xxhashComposite),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: xxhashPart.ETag, ChecksumXXHASH3: xxhashPart.ChecksumXXHASH3}}},
	})
	if err != nil || aws.ToString(xxhashComplete.ChecksumXXHASH3) != xxhashComposite {
		t.Fatalf("complete xxhash multipart: %#v %v", xxhashComplete, err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: got.ETag, ExpectedSourceBucketOwner: aws.String("000000000000"),
	}); err != nil {
		t.Fatalf("conditional copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/copied")}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("unchanged self-copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/copied"), MetadataDirective: s3types.MetadataDirectiveReplace, Metadata: map[string]string{"owner": "self"}}); err != nil {
		t.Fatalf("metadata-replacing self-copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("source-owner-denied"), CopySource: aws.String("sdk/k"), ExpectedSourceBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("mismatched expected source owner: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("missing-source"), CopySource: aws.String("missing/k")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("copy missing source bucket: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-class"), Body: bytes.NewReader([]byte("body")), StorageClass: s3types.StorageClass("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid put storage class: %v", err)
	}
	if _, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-multipart-class"), StorageClass: s3types.StorageClassOutposts}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid multipart storage class: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-copy-class"), CopySource: aws.String("sdk/k"), StorageClass: s3types.StorageClass("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid copy storage class: %v", err)
	}
	for _, key := range []string{"invalid-class", "invalid-multipart-class", "invalid-copy-class"} {
		if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key)}); err == nil || !strings.Contains(err.Error(), "NotFound") {
			t.Fatalf("invalid storage class created %q: %v", key, err)
		}
	}
	longKey := strings.Repeat("é", 513)
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(longKey), Body: bytes.NewReader([]byte("body"))}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized put key: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String(longKey), CopySource: aws.String("missing/source")}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized copy key: %v", err)
	}
	if _, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String(longKey)}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized multipart key: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive"), Body: bytes.NewReader([]byte("cold")), StorageClass: s3types.StorageClassGlacier}); err != nil {
		t.Fatalf("put archive: %v", err)
	}
	archiveHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")})
	if err != nil || archiveHead.StorageClass != s3types.StorageClassGlacier || aws.ToString(archiveHead.Restore) != "" {
		t.Fatalf("head unrestored archive: %#v %v", archiveHead, err)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")}); err == nil || !strings.Contains(err.Error(), "InvalidObjectState") {
		t.Fatalf("get unrestored archive: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive-copy"), CopySource: aws.String("sdk/archive")}); err == nil || !strings.Contains(err.Error(), "InvalidObjectState") {
		t.Fatalf("copy unrestored archive: %v", err)
	}
	if _, err := s3c.RestoreObject(context.Background(), &s3.RestoreObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive"), RestoreRequest: &s3types.RestoreRequest{Days: aws.Int32(1)}}); err != nil {
		t.Fatalf("restore archive: %v", err)
	}
	restoredArchive, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")})
	if err != nil {
		t.Fatalf("get restored archive: %v", err)
	}
	archiveBody, _ := io.ReadAll(restoredArchive.Body)
	_ = restoredArchive.Body.Close()
	if string(archiveBody) != "cold" || aws.ToString(restoredArchive.Restore) == "" {
		t.Fatalf("restored archive body=%q restore=%q", archiveBody, aws.ToString(restoredArchive.Restore))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive-copy"), CopySource: aws.String("sdk/archive")}); err != nil {
		t.Fatalf("copy restored archive: %v", err)
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
	deletedNewer, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: newer.VersionId})
	if err != nil || aws.ToString(deletedNewer.VersionId) != aws.ToString(newer.VersionId) || aws.ToBool(deletedNewer.DeleteMarker) {
		t.Fatalf("delete current version: %#v %v", deletedNewer, err)
	}
	restoredCurrent, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("get restored current version: %v", err)
	}
	restoredCurrentBody, _ := io.ReadAll(restoredCurrent.Body)
	_ = restoredCurrent.Body.Close()
	if string(restoredCurrentBody) != "hello-sdk" || aws.ToString(restoredCurrent.VersionId) != aws.ToString(put.VersionId) {
		t.Fatalf("restored current version body=%q output=%#v", restoredCurrentBody, restoredCurrent)
	}
	multiFirst, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete"), Body: bytes.NewReader([]byte("first"))})
	if err != nil {
		t.Fatalf("put first multi-delete version: %v", err)
	}
	multiSecond, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete"), Body: bytes.NewReader([]byte("second"))})
	if err != nil {
		t.Fatalf("put second multi-delete version: %v", err)
	}
	multiDeleted, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: aws.String("multi-delete"), VersionId: multiSecond.VersionId}}}})
	if err != nil || len(multiDeleted.Deleted) != 1 || aws.ToString(multiDeleted.Deleted[0].VersionId) != aws.ToString(multiSecond.VersionId) || aws.ToBool(multiDeleted.Deleted[0].DeleteMarker) {
		t.Fatalf("multi-delete current version: %#v %v", multiDeleted, err)
	}
	multiRestored, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete")})
	if err != nil {
		t.Fatalf("get multi-delete restored version: %v", err)
	}
	multiRestoredBody, _ := io.ReadAll(multiRestored.Body)
	_ = multiRestored.Body.Close()
	if string(multiRestoredBody) != "first" || aws.ToString(multiRestored.VersionId) != aws.ToString(multiFirst.VersionId) {
		t.Fatalf("multi-delete restored body=%q output=%#v", multiRestoredBody, multiRestored)
	}
	quietDeleted, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Quiet: aws.Bool(true), Objects: []s3types.ObjectIdentifier{
		{Key: aws.String("multi-delete"), VersionId: aws.String("missing")},
		{Key: aws.String("multi-delete"), VersionId: multiFirst.VersionId},
	}}})
	if err != nil || len(quietDeleted.Deleted) != 0 || len(quietDeleted.Errors) != 1 || aws.ToString(quietDeleted.Errors[0].Code) != "NoSuchVersion" || aws.ToString(quietDeleted.Errors[0].VersionId) != "missing" {
		t.Fatalf("quiet multi-delete: %#v %v", quietDeleted, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-lock"), ObjectLockEnabledForBucket: aws.Bool(true)}); err != nil {
		t.Fatalf("create object-lock bucket: %v", err)
	}
	lockVersioning, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk-lock")})
	if err != nil || lockVersioning.Status != s3types.BucketVersioningStatusEnabled {
		t.Fatalf("object-lock bucket versioning: %#v %v", lockVersioning, err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-lock"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusSuspended}}); err == nil || !strings.Contains(err.Error(), "InvalidBucketState") {
		t.Fatalf("suspend object-lock bucket versioning: %v", err)
	}
	if _, err := s3c.PutObjectLockConfiguration(context.Background(), &s3.PutObjectLockConfigurationInput{Bucket: aws.String("sdk-lock"), ObjectLockConfiguration: &s3types.ObjectLockConfiguration{ObjectLockEnabled: s3types.ObjectLockEnabledEnabled, Rule: &s3types.ObjectLockRule{DefaultRetention: &s3types.DefaultRetention{Mode: s3types.ObjectLockRetentionModeGovernance, Days: aws.Int32(7)}}}}); err != nil {
		t.Fatalf("put object-lock configuration: %v", err)
	}
	lockConfiguration, err := s3c.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String("sdk-lock")})
	if err != nil || lockConfiguration.ObjectLockConfiguration == nil {
		t.Fatalf("get object-lock configuration: %#v %v", lockConfiguration, err)
	}
	if got := lockConfiguration.ObjectLockConfiguration; got.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled || got.Rule == nil || got.Rule.DefaultRetention == nil || aws.ToInt32(got.Rule.DefaultRetention.Days) != 7 {
		t.Fatalf("get object-lock configuration: %#v", got)
	}
	lockedVersion, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), Body: bytes.NewReader([]byte("locked"))})
	if err != nil {
		t.Fatalf("put locked object: %v", err)
	}
	defaultRetention, err := s3c.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId})
	if err != nil || defaultRetention.Retention == nil || defaultRetention.Retention.Mode != s3types.ObjectLockRetentionModeGovernance || defaultRetention.Retention.RetainUntilDate == nil || time.Until(*defaultRetention.Retention.RetainUntilDate) < 6*24*time.Hour {
		t.Fatalf("default object retention: %#v %v", defaultRetention, err)
	}
	explicitUntil := defaultRetention.Retention.RetainUntilDate.Add(-6 * 24 * time.Hour)
	explicitVersion, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("explicit-lock"), Body: bytes.NewReader([]byte("locked")), ObjectLockMode: s3types.ObjectLockModeCompliance, ObjectLockRetainUntilDate: &explicitUntil, ObjectLockLegalHoldStatus: s3types.ObjectLockLegalHoldStatusOn})
	if err != nil {
		t.Fatalf("put explicitly locked object: %v", err)
	}
	explicitRetention, err := s3c.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String("sdk-lock"), Key: aws.String("explicit-lock"), VersionId: explicitVersion.VersionId})
	if err != nil || explicitRetention.Retention == nil || explicitRetention.Retention.Mode != s3types.ObjectLockRetentionModeCompliance || explicitRetention.Retention.RetainUntilDate == nil || !explicitRetention.Retention.RetainUntilDate.Equal(explicitUntil) {
		t.Fatalf("explicit object retention: %#v %v", explicitRetention, err)
	}
	if _, err := s3c.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId, LegalHold: &s3types.ObjectLockLegalHold{Status: s3types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatalf("put legal hold: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("delete legal-held version: %v", err)
	}
	if marker, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked")}); err != nil || !aws.ToBool(marker.DeleteMarker) {
		t.Fatalf("delete-marker over legal hold: %#v %v", marker, err)
	}
	tooMany := make([]s3types.ObjectIdentifier, 1001)
	for index := range tooMany {
		tooMany[index].Key = aws.String(fmt.Sprintf("too-many-%d", index))
	}
	if _, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Objects: tooMany}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("oversized multi-delete: %v", err)
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
