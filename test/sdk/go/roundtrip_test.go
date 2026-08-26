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
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("hello-sdk")),
	}); err != nil {
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
