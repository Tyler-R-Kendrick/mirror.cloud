package sdk_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestAWSSDKSQSListQueuesContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	for _, name := range []string{"sdk-a-0", "sdk-a-1", "sdk-a-2", "sdk-b-0"} {
		if _, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := client.ListQueues(context.Background(), &sqs.ListQueuesInput{QueueNamePrefix: aws.String("sdk-a-"), MaxResults: aws.Int32(2)})
	if err != nil || len(first.QueueUrls) != 2 || aws.ToString(first.NextToken) == "" {
		t.Fatalf("first page %#v %v", first, err)
	}
	second, err := client.ListQueues(context.Background(), &sqs.ListQueuesInput{QueueNamePrefix: aws.String("sdk-a-"), MaxResults: aws.Int32(2), NextToken: first.NextToken})
	if err != nil || len(second.QueueUrls) != 1 || second.NextToken != nil {
		t.Fatalf("second page %#v %v", second, err)
	}
	empty, err := client.ListQueues(context.Background(), &sqs.ListQueuesInput{QueueNamePrefix: aws.String("missing")})
	if err != nil || len(empty.QueueUrls) != 0 {
		t.Fatalf("empty page %#v %v", empty, err)
	}
}

func TestAWSSDKSQSQueueMetadataContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-metadata")})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: created.QueueUrl,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameQueueArn,
			types.QueueAttributeNameCreatedTimestamp,
			types.QueueAttributeNameVisibilityTimeout,
		},
	})
	if err != nil || len(result.Attributes) != 3 || result.Attributes["QueueArn"] != "arn:aws:sqs:us-east-1:000000000000:sdk-metadata" ||
		result.Attributes["CreatedTimestamp"] == "" || result.Attributes["VisibilityTimeout"] != "30" {
		t.Fatalf("metadata %#v, %v", result, err)
	}
}

func TestAWSSDKSQSRecentlyDeletedContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-deleted")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: created.QueueUrl}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-deleted")}); err == nil || !strings.Contains(err.Error(), "QueueDeletedRecently") {
		t.Fatalf("recently deleted error %v", err)
	}
}

func TestAWSSDKSQSEmptyMessageContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-empty-body")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("")})
	if err == nil || !strings.Contains(err.Error(), "MissingParameter") || !strings.Contains(err.Error(), "The request must contain the parameter MessageBody.") {
		t.Fatalf("empty message error %v", err)
	}
}

func TestAWSSDKSQSMaxNumberOfMessagesContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	awsConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-max-messages")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 11})
	if err == nil || !strings.Contains(err.Error(), "InvalidParameterValue") || !strings.Contains(err.Error(), "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided.") {
		t.Fatalf("max messages error %v", err)
	}
}
