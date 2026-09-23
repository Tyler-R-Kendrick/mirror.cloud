package conformance

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func isWriteOp(op string) bool {
	for _, p := range []string{
		"Create", "Put", "Delete", "Send", "Update", "Tag", "Attach", "Set", "Purge",
		"Copy", "Upload", "Complete", "Abort", "Batch", "Transact", "Label", "Add",
		"Remove", "Restore", "Change", "Detach", "Untag", "Publish",
	} {
		if strings.HasPrefix(op, p) {
			return true
		}
	}
	return false
}

func TestListedWriteOpsAreNotEmptySuccess(t *testing.T) {
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}

	t.Run("s3", func(t *testing.T) {
		p := s3.New(spitest.Deps(t))
		seen := map[string]bool{}
		inv := func(op string, in map[string]any, body io.Reader, copySrc string) *spi.Response {
			return call(t, p, ctx, id, seen, op, in, body, copySrc)
		}
		inv("CreateBucket", map[string]any{"Bucket": "bucket", "ObjectLockEnabledForBucket": true}, nil, "")
		inv("PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, bytes.NewReader([]byte("v1")), "")
		inv("PutObject", map[string]any{"Bucket": "bucket", "Key": "src"}, bytes.NewReader([]byte("SRC")), "")
		inv("CopyObject", map[string]any{"Bucket": "bucket", "Key": "dst", "CopySource": "bucket/src"}, nil, "")
		got := inv("GetObject", map[string]any{"Bucket": "bucket", "Key": "dst"}, nil, "")
		raw, _ := io.ReadAll(got.Stream)
		if string(raw) != "SRC" {
			t.Fatalf("copy %q", raw)
		}
		mpu := inv("CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "m"}, nil, "")
		uid := str(mpu.Output["UploadId"])
		inv("UploadPart", map[string]any{"Bucket": "bucket", "Key": "m", "UploadId": uid, "PartNumber": float64(1)}, bytes.NewReader([]byte("AB")), "")
		copied := inv("UploadPartCopy", map[string]any{"Bucket": "bucket", "Key": "m", "UploadId": uid, "PartNumber": float64(2), "CopySource": "bucket/src"}, nil, "")
		parts := inv("ListParts", map[string]any{"Bucket": "bucket", "Key": "m", "UploadId": uid}, nil, "")
		if n := len(asSlice(parts.Output["Parts"])); n != 2 {
			t.Fatalf("ListParts %v", parts.Output)
		}
		mlist := inv("ListMultipartUploads", map[string]any{"Bucket": "bucket"}, nil, "")
		if n := len(asSlice(mlist.Output["Uploads"])); n < 1 {
			t.Fatalf("ListMultipartUploads %v", mlist.Output)
		}
		inv("CompleteMultipartUpload", map[string]any{
			"Bucket": "bucket", "Key": "m", "UploadId": uid,
			"MultipartUpload": map[string]any{"Parts": []any{map[string]any{"PartNumber": 2, "ETag": copied.Headers.Get("ETag")}}},
		}, nil, "")
		inv("PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil, "")
		inv("PutBucketTagging", map[string]any{"Bucket": "bucket", "TagSet": []any{map[string]any{"Key": "a", "Value": "bucket"}}}, nil, "")
		inv("PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "k", "TagSet": []any{}}, nil, "")
		inv("PutBucketNotificationConfiguration", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketAcl", map[string]any{"Bucket": "bucket", "ACL": "private"}, nil, "")
		inv("PutObjectAcl", map[string]any{"Bucket": "bucket", "Key": "k", "ACL": "private"}, nil, "")
		inv("PutBucketPolicy", map[string]any{"Bucket": "bucket", "Policy": `{"Version":"2012-10-17"}`}, nil, "")
		inv("DeleteBucketPolicy", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketCors", map[string]any{"Bucket": "bucket", "CORSConfiguration": map[string]any{"CORSRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}}}, nil, "")
		inv("DeleteBucketCors", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketWebsite", map[string]any{"Bucket": "bucket", "WebsiteConfiguration": map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}}}, nil, "")
		inv("DeleteBucketWebsite", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketLogging", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketLifecycleConfiguration", map[string]any{"Bucket": "bucket", "LifecycleConfiguration": map[string]any{"Rules": []any{map[string]any{"ID": "expire", "Filter": map[string]any{}, "Status": "Enabled", "Expiration": map[string]any{"Days": 1}}}}}, nil, "")
		inv("DeleteBucketLifecycle", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketReplication", map[string]any{"Bucket": "bucket", "ReplicationConfiguration": map[string]any{
			"Role":  "arn:aws:iam::000000000000:role/replication",
			"Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "arn:aws:s3:::bucket"}}},
		}}, nil, "")
		inv("PutBucketEncryption", map[string]any{"Bucket": "bucket", "ServerSideEncryptionConfiguration": map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256"}}}}}, nil, "")
		inv("DeleteBucketEncryption", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutObjectLockConfiguration", map[string]any{"Bucket": "bucket", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled"}}, nil, "")
		inv("PutBucketObjectLockConfiguration", map[string]any{"Bucket": "bucket", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled"}}, nil, "")
		inv("PutBucketRequestPayment", map[string]any{"Bucket": "bucket", "RequestPaymentConfiguration": map[string]any{"Payer": "BucketOwner"}}, nil, "")
		inv("PutBucketAccelerateConfiguration", map[string]any{"Bucket": "bucket", "AccelerateConfiguration": map[string]any{"Status": "Enabled"}}, nil, "")
		inv("PutPublicAccessBlock", map[string]any{"Bucket": "bucket", "PublicAccessBlockConfiguration": map[string]any{}}, nil, "")
		inv("DeletePublicAccessBlock", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("PutBucketOwnershipControls", map[string]any{"Bucket": "bucket", "OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "ObjectWriter"}}}}, nil, "")
		inv("DeleteBucketOwnershipControls", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("DeleteBucketTagging", map[string]any{"Bucket": "bucket"}, nil, "")
		inv("DeleteObjectTagging", map[string]any{"Bucket": "bucket", "Key": "k"}, nil, "")
		inv("PutObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "k"}, nil, "")
		inv("PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "k"}, nil, "")
		inv("PutObject", map[string]any{"Bucket": "bucket", "Key": "archive", "StorageClass": "GLACIER"}, bytes.NewReader([]byte("cold")), "")
		inv("RestoreObject", map[string]any{"Bucket": "bucket", "Key": "archive", "RestoreRequest": map[string]any{"Days": 1}}, nil, "")
		inv("PutBucketAnalyticsConfiguration", map[string]any{"Bucket": "bucket", "Id": "a", "AnalyticsConfiguration": map[string]any{"Id": "a"}}, nil, "")
		inv("DeleteBucketAnalyticsConfiguration", map[string]any{"Bucket": "bucket", "Id": "a"}, nil, "")
		inv("PutBucketInventoryConfiguration", map[string]any{"Bucket": "bucket", "Id": "i", "InventoryConfiguration": map[string]any{
			"Id": "i", "IsEnabled": true, "IncludedObjectVersions": "All",
			"Destination": map[string]any{"S3BucketDestination": map[string]any{"Bucket": "arn:aws:s3:::bucket", "Format": "CSV"}},
			"Schedule":    map[string]any{"Frequency": "Daily"},
		}}, nil, "")
		inv("DeleteBucketInventoryConfiguration", map[string]any{"Bucket": "bucket", "Id": "i"}, nil, "")
		inv("PutBucketMetricsConfiguration", map[string]any{"Bucket": "bucket", "Id": "m", "MetricsConfiguration": map[string]any{"Id": "m"}}, nil, "")
		inv("DeleteBucketMetricsConfiguration", map[string]any{"Bucket": "bucket", "Id": "m"}, nil, "")
		inv("PutBucketIntelligentTieringConfiguration", map[string]any{"Bucket": "bucket", "Id": "t", "IntelligentTieringConfiguration": map[string]any{"Id": "t", "Status": "Enabled"}}, nil, "")
		inv("DeleteBucketIntelligentTieringConfiguration", map[string]any{"Bucket": "bucket", "Id": "t"}, nil, "")
		del := inv("DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{map[string]any{"Key": "k"}}}, nil, "")
		if n := len(del.Output["Deleted"].([]any)); n != 1 {
			t.Fatalf("DeleteObjects %v", del.Output)
		}
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetObject", Input: map[string]any{"Bucket": "bucket", "Key": "k"}}); err == nil {
			t.Fatal("DeleteObjects left k")
		}
		mpu2 := inv("CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "abort"}, nil, "")
		inv("AbortMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "abort", "UploadId": str(mpu2.Output["UploadId"])}, nil, "")
		inv("DeleteObject", map[string]any{"Bucket": "bucket", "Key": "src"}, nil, "")
		inv("CreateBucket", map[string]any{"Bucket": "gone"}, nil, "")
		inv("DeleteBucket", map[string]any{"Bucket": "gone"}, nil, "")
		fatS3 := map[string]any{"Bucket": "bucket", "Key": "k"}
		for _, op := range p.Operations() {
			if isWriteOp(op) && !seen[op] {
				inv(op, fatS3, nil, "")
			}
		}
		assertWritesCovered(t, p.Operations(), seen)
	})

	t.Run("sqs", func(t *testing.T) {
		deps := spitest.Deps(t)
		p := bundled.Handler("aws.sqs", deps)
		seen := map[string]bool{}
		inv := func(op string, in map[string]any) *spi.Response {
			return call(t, p, ctx, id, seen, op, in, nil, "")
		}
		inv("CreateQueue", map[string]any{"QueueName": "q"})
		inv("SendMessage", map[string]any{"QueueName": "q", "MessageBody": "a"})
		inv("SendMessageBatch", map[string]any{"QueueName": "q", "Entries": []any{map[string]any{"Id": "1", "MessageBody": "bucket"}}})
		recv := inv("ReceiveMessage", map[string]any{"QueueName": "q"})
		msgs, _ := recv.Output["Messages"].([]any)
		if len(msgs) == 0 {
			t.Fatal("no messages")
		}
		rh := str(asMap(msgs[0])["ReceiptHandle"])
		inv("ChangeMessageVisibility", map[string]any{"QueueName": "q", "ReceiptHandle": rh, "VisibilityTimeout": "5"})
		inv("ChangeMessageVisibilityBatch", map[string]any{"QueueName": "q", "Entries": []any{map[string]any{"Id": "1", "ReceiptHandle": rh, "VisibilityTimeout": "1"}}})
		inv("DeleteMessage", map[string]any{"QueueName": "q", "ReceiptHandle": rh})
		inv("DeleteMessageBatch", map[string]any{"QueueName": "q", "Entries": []any{map[string]any{"Id": "x", "ReceiptHandle": "nope"}}})
		inv("SetQueueAttributes", map[string]any{"QueueName": "q", "Attributes": map[string]any{"VisibilityTimeout": "5"}})
		inv("TagQueue", map[string]any{"QueueName": "q", "Tags": map[string]any{"k": "v"}})
		inv("UntagQueue", map[string]any{"QueueName": "q", "TagKeys": []any{"k"}})
		inv("PurgeQueue", map[string]any{"QueueName": "q"})
		inv("DeleteQueue", map[string]any{"QueueName": "q"})
		if err := deps.Clock.Advance(time.Minute); err != nil {
			t.Fatal(err)
		}
		inv("CreateQueue", map[string]any{"QueueName": "q"})
		fat := map[string]any{
			"QueueName": "q", "QueueUrl": "http://q", "TaskHandle": "t1",
			"Label": "l", "AWSAccountIds": []any{"111111111111"}, "Actions": []any{"SendMessage"},
			"SourceArn": "arn:aws:sqs:us-east-1:000000000000:q",
		}
		for _, op := range p.Operations() {
			if isWriteOp(op) && !seen[op] {
				inv(op, fat)
			}
		}
		assertWritesCovered(t, p.Operations(), seen)
	})

	t.Run("dynamodb", func(t *testing.T) {
		deps := spitest.Deps(t)
		p := dynamodb.New(deps)
		seen := map[string]bool{}
		inv := func(op string, in map[string]any) *spi.Response {
			return call(t, p, ctx, id, seen, op, in, nil, "")
		}
		inv("CreateTable", map[string]any{"TableName": "T", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}}})
		inv("PutItem", map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "1"}}})
		inv("UpdateItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "1"}}, "UpdateExpression": "SET n = :x", "ExpressionAttributeValues": map[string]any{":x": map[string]any{"N": "3"}}})
		inv("BatchWriteItem", map[string]any{"RequestItems": map[string]any{"T": []any{map[string]any{"PutRequest": map[string]any{"Item": map[string]any{"id": map[string]any{"S": "2"}}}}}}})
		inv("BatchGetItem", map[string]any{"RequestItems": map[string]any{"T": map[string]any{"Keys": []any{map[string]any{"id": map[string]any{"S": "2"}}}}}})
		inv("TransactWriteItems", map[string]any{"TransactItems": []any{map[string]any{"Put": map[string]any{"TableName": "T", "Item": map[string]any{"id": map[string]any{"S": "3"}}}}}})
		tg := inv("TransactGetItems", map[string]any{"TransactItems": []any{map[string]any{"Get": map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "3"}}}}}})
		responses := asSlice(tg.Output["Responses"])
		if len(responses) != 1 || asMap(asMap(responses[0])["Item"])["id"] == nil {
			t.Fatalf("transact get empty %v", tg.Output)
		}
		q := inv("Query", map[string]any{
			"TableName":                 "T",
			"KeyConditionExpression":    "id = :id",
			"ExpressionAttributeValues": map[string]any{":id": map[string]any{"S": "2"}},
		})
		qitems, _ := q.Output["Items"].([]any)
		if len(qitems) != 1 {
			t.Fatalf("query not keyed: %v", q.Output)
		}
		inv("UpdateTable", map[string]any{"TableName": "T", "GlobalSecondaryIndexUpdates": []any{}})
		inv("TagResource", map[string]any{"ResourceArn": "arn:t", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
		inv("UntagResource", map[string]any{"ResourceArn": "arn:t"})
		inv("UpdateTimeToLive", map[string]any{"TableName": "T", "TimeToLiveSpecification": map[string]any{"Enabled": true, "AttributeName": "exp"}})
		inv("UpdateContinuousBackups", map[string]any{"TableName": "T", "PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": true}})
		inv("PutResourcePolicy", map[string]any{"ResourceArn": "arn:t", "Policy": "{}"})
		inv("DeleteResourcePolicy", map[string]any{"ResourceArn": "arn:t"})
		bak := inv("CreateBackup", map[string]any{"TableName": "T", "BackupName": "bucket"})
		bArn := str(asMap(bak.Output["BackupDetails"])["BackupArn"])
		inv("RestoreTableFromBackup", map[string]any{"BackupArn": bArn, "TargetTableName": "Tr"})
		inv("DeleteBackup", map[string]any{"BackupArn": bArn})
		if _, err := bundled.Handler("aws.kinesis", deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStream", Input: map[string]any{"StreamName": "s"}}); err != nil {
			t.Fatal(err)
		}
		streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/s"
		inv("EnableKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN})
		inv("UpdateKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN, "UpdateKinesisStreamingConfiguration": map[string]any{"ApproximateCreationDateTimePrecision": "MICROSECOND"}})
		inv("DisableKinesisStreamingDestination", map[string]any{"TableName": "T", "StreamArn": streamARN})
		inv("DeleteItem", map[string]any{"TableName": "T", "Key": map[string]any{"id": map[string]any{"S": "1"}}})
		inv("CreateTable", map[string]any{"TableName": "gone"})
		inv("DeleteTable", map[string]any{"TableName": "gone"})
		fat := map[string]any{"TableName": "T", "GlobalTableName": "GT", "ExportArn": "arn:e", "ImportArn": "arn:i",
			"Statement": "SELECT * FROM T", "ReplicationGroup": []any{map[string]any{"RegionName": "us-east-1"}},
			"ContributorInsightsAction": "ENABLE", "S3Bucket": "bucket", "SourceTableName": "T", "TargetTableName": "Tpitr",
			"TableCreationParameters": map[string]any{"TableName": "Timp"}, "StreamArn": streamARN}
		for _, op := range p.Operations() {
			if isWriteOp(op) && !seen[op] {
				inv(op, fat)
			}
		}
		assertWritesCovered(t, p.Operations(), seen)
	})

	t.Run("sns-iam-ssm-secrets", func(t *testing.T) {
		snsP := sns.New(spitest.Deps(t))
		seen := map[string]bool{}
		inv := func(p invoker, op string, in map[string]any) *spi.Response {
			return call(t, p, ctx, id, seen, op, in, nil, "")
		}
		created := inv(snsP, "CreateTopic", map[string]any{"Name": "t"})
		arn := str(created.Output["TopicArn"])
		inv(snsP, "SetTopicAttributes", map[string]any{"TopicArn": arn, "AttributeName": "DisplayName", "AttributeValue": "n"})
		inv(snsP, "Publish", map[string]any{"TopicArn": arn, "Message": "hi"})
		inv(snsP, "PublishBatch", map[string]any{"TopicArn": arn, "Message": "hi"})
		sub := inv(snsP, "Subscribe", map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:000000000000:q"})
		inv(snsP, "ConfirmSubscription", map[string]any{"Token": "tok"})
		inv(snsP, "TagResource", map[string]any{"ResourceArn": arn, "Tags": []any{}})
		inv(snsP, "UntagResource", map[string]any{"ResourceArn": arn})
		inv(snsP, "Unsubscribe", map[string]any{"SubscriptionArn": str(sub.Output["SubscriptionArn"])})
		inv(snsP, "DeleteTopic", map[string]any{"TopicArn": arn})
		// Recreate the resources used by the remaining write-operation sweep. The
		// sweep is intended to verify successful state changes, while real SNS
		// operations correctly reject deleted topics and subscriptions.
		created = inv(snsP, "CreateTopic", map[string]any{"Name": "t"})
		arn = str(created.Output["TopicArn"])
		sub = inv(snsP, "Subscribe", map[string]any{"TopicArn": arn, "Protocol": "sqs", "Endpoint": "arn:aws:sqs:us-east-1:000000000000:q"})
		fatSNS := map[string]any{
			"Name": "t", "TopicArn": arn, "PhoneNumber": "+15555550100", "EndpointArn": "arn:e",
			"Label": "allow", "AWSAccountIds": []any{"111111111111"}, "ActionName": []any{"Publish"},
			"Platform": "GCM", "PlatformApplicationArn": "arn:aws:sns:us-east-1:000000000000:app/GCM/t",
			"Token": "tok", "SubscriptionArn": str(sub.Output["SubscriptionArn"]),
			"ResourceArn": arn, "AttributeName": "RawMessageDelivery", "AttributeValue": "true",
			"Attributes": map[string]any{"Enabled": "true"}, "DataProtectionPolicy": `{"Name":"p"}`,
		}
		for _, op := range snsP.Operations() {
			// DeletePlatformApplication precedes endpoint creation in the operation
			// inventory; recreate the fixture app before the endpoint sweep reaches it.
			if op == "CreatePlatformEndpoint" {
				inv(snsP, "CreatePlatformApplication", map[string]any{"Name": "t", "Platform": "GCM"})
				endpoint := inv(snsP, "CreatePlatformEndpoint", map[string]any{"PlatformApplicationArn": fatSNS["PlatformApplicationArn"], "Token": "tok"})
				fatSNS["EndpointArn"] = str(endpoint.Output["EndpointArn"])
			}
			if isWriteOp(op) && !seen[op] {
				input := fatSNS
				if op == "CreatePlatformApplication" {
					input = map[string]any{}
					for key, value := range fatSNS {
						input[key] = value
					}
					input["Attributes"] = map[string]any{"PlatformCredential": "credential"}
				}
				if op == "SetSMSAttributes" {
					input = map[string]any{}
					for key, value := range fatSNS {
						input[key] = value
					}
					input["Attributes"] = map[string]any{"DefaultSenderID": "Mirror"}
				}
				inv(snsP, op, input)
			}
		}
		assertWritesCovered(t, snsP.Operations(), seen)

		seen = map[string]bool{}
		ssmP, err := bundled.New("aws.ssm", spitest.Deps(t))
		if err != nil {
			t.Fatal(err)
		}
		inv(ssmP, "PutParameter", map[string]any{"Name": "/a", "Value": "1", "Type": "String"})
		inv(ssmP, "LabelParameterVersion", map[string]any{"Name": "/a", "Labels": []any{"live"}})
		inv(ssmP, "UnlabelParameterVersion", map[string]any{"Name": "/a", "ParameterVersion": 1, "Labels": []any{"live"}})
		inv(ssmP, "AddTagsToResource", map[string]any{"ResourceType": "Parameter", "ResourceId": "/a", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
		inv(ssmP, "RemoveTagsFromResource", map[string]any{"ResourceType": "Parameter", "ResourceId": "/a", "TagKeys": []any{"k"}})
		inv(ssmP, "CreateDocument", map[string]any{"Name": "d", "Content": "{}"})
		inv(ssmP, "UpdateDocument", map[string]any{"Name": "d", "Content": "{}"})
		inv(ssmP, "UpdateDocumentDefaultVersion", map[string]any{"Name": "d", "DocumentVersion": "1"})
		assoc := inv(ssmP, "CreateAssociation", map[string]any{"Name": "d"})
		aid := str(asMap(assoc.Output["AssociationDescription"])["AssociationId"])
		inv(ssmP, "UpdateAssociation", map[string]any{"AssociationId": aid})
		inv(ssmP, "DeleteAssociation", map[string]any{"AssociationId": aid})
		cmd := inv(ssmP, "SendCommand", map[string]any{"DocumentName": "d"})
		inv(ssmP, "CancelCommand", map[string]any{"CommandId": str(asMap(cmd.Output["Command"])["CommandId"])})
		bl := inv(ssmP, "CreatePatchBaseline", map[string]any{"Name": "pb"})
		bid := str(bl.Output["BaselineId"])
		inv(ssmP, "UpdatePatchBaseline", map[string]any{"BaselineId": bid})
		inv(ssmP, "RegisterDefaultPatchBaseline", map[string]any{"BaselineId": bid})
		inv(ssmP, "DeletePatchBaseline", map[string]any{"BaselineId": bid})
		mw := inv(ssmP, "CreateMaintenanceWindow", map[string]any{"Name": "mw", "Schedule": "rate(1 day)", "Duration": 2, "Cutoff": 1, "AllowUnassociatedTargets": false})
		wid := str(mw.Output["WindowId"])
		inv(ssmP, "UpdateMaintenanceWindow", map[string]any{"WindowId": wid})
		tgt := inv(ssmP, "RegisterTargetWithMaintenanceWindow", map[string]any{"WindowId": wid, "ResourceType": "INSTANCE", "Targets": []any{map[string]any{"Key": "InstanceIds", "Values": []any{"i-1"}}}})
		inv(ssmP, "DeregisterTargetFromMaintenanceWindow", map[string]any{"WindowId": wid, "WindowTargetId": str(tgt.Output["WindowTargetId"])})
		tk := inv(ssmP, "RegisterTaskWithMaintenanceWindow", map[string]any{"WindowId": wid, "TaskArn": "AWS-RunShellScript", "TaskType": "RUN_COMMAND"})
		inv(ssmP, "DeregisterTaskFromMaintenanceWindow", map[string]any{"WindowId": wid, "WindowTaskId": str(tk.Output["WindowTaskId"])})
		inv(ssmP, "DeleteMaintenanceWindow", map[string]any{"WindowId": wid})
		auto := inv(ssmP, "StartAutomationExecution", map[string]any{"DocumentName": "AWS-Hello"})
		inv(ssmP, "StopAutomationExecution", map[string]any{"AutomationExecutionId": str(auto.Output["AutomationExecutionId"])})
		ops := inv(ssmP, "CreateOpsItem", map[string]any{"Title": "t", "Source": "m", "Description": "d"})
		oid := str(ops.Output["OpsItemId"])
		inv(ssmP, "UpdateOpsItem", map[string]any{"OpsItemId": oid})
		inv(ssmP, "DeleteOpsItem", map[string]any{"OpsItemId": oid})
		inv(ssmP, "CreateResourceDataSync", map[string]any{"SyncName": "s"})
		inv(ssmP, "DeleteResourceDataSync", map[string]any{"SyncName": "s"})
		inv(ssmP, "UpdateServiceSetting", map[string]any{"SettingId": "/x", "SettingValue": "v"})
		inv(ssmP, "ResetServiceSetting", map[string]any{"SettingId": "/x"})
		inv(ssmP, "DeleteDocument", map[string]any{"Name": "d"})
		inv(ssmP, "DeleteParameter", map[string]any{"Name": "/a"})
		inv(ssmP, "PutParameter", map[string]any{"Name": "/b", "Value": "1", "Type": "String"})
		inv(ssmP, "DeleteParameters", map[string]any{"Names": []any{"/b"}})
		assertWritesCovered(t, ssmP.Operations(), seen)

		seen = map[string]bool{}
		sm := bundled.Handler("aws.secretsmanager", spitest.Deps(t))
		inv(sm, "CreateSecret", map[string]any{"Name": "n", "SecretString": "v"})
		inv(sm, "PutSecretValue", map[string]any{"SecretId": "n", "SecretString": "v2"})
		inv(sm, "UpdateSecret", map[string]any{"SecretId": "n", "SecretString": "v3"})
		inv(sm, "TagResource", map[string]any{"SecretId": "n", "Tags": []any{}})
		inv(sm, "UntagResource", map[string]any{"SecretId": "n", "TagKeys": []any{}})
		inv(sm, "DeleteSecret", map[string]any{"SecretId": "n"})
		inv(sm, "RestoreSecret", map[string]any{"SecretId": "n"})
		fatSM := map[string]any{"SecretId": "n", "Name": "n", "ResourcePolicy": `{"Version":"2012-10-17"}`,
			"AddReplicaRegions": []any{}, "RemoveReplicaRegions": []any{}, "VersionStage": "AWSCURRENT"}
		for _, op := range sm.Operations() {
			if isWriteOp(op) && !seen[op] {
				inv(sm, op, fatSM)
			}
		}
		assertWritesCovered(t, sm.Operations(), seen)
	})
}

type invoker interface {
	Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error)
}

func call(t *testing.T, p invoker, ctx context.Context, id spi.Identity, seen map[string]bool, op string, in map[string]any, body io.Reader, copySrc string) *spi.Response {
	t.Helper()
	seen[op] = true
	req := &spi.Request{Identity: id, Operation: op, Input: in}
	if body != nil {
		req.Body = io.NopCloser(body)
	}
	if copySrc != "" {
		h := httptest.NewRequest("PUT", "/x", nil)
		h.Header.Set("x-amz-copy-source", copySrc)
		req.HTTP = h
	}
	resp, err := p.Invoke(ctx, req)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if resp == nil {
		t.Fatalf("%s nil resp", op)
	}
	return resp
}

func assertWritesCovered(t *testing.T, ops []string, seen map[string]bool) {
	t.Helper()
	var missing []string
	for _, op := range ops {
		if isWriteOp(op) && !seen[op] {
			missing = append(missing, op)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("listed write ops not exercised (would allow empty-success): %v", missing)
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}
