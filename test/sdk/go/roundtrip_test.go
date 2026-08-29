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
