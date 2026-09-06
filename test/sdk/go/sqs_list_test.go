package sdk_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strconv"
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

func TestAWSSDKSQSSendMessageBatchContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch")})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("1"), MessageBody: aws.String("message-0")},
		{Id: aws.String("2"), MessageBody: aws.String("message-1")},
	}})
	if err != nil || len(result.Successful) != 2 || len(result.Failed) != 0 {
		t.Fatalf("batch %#v error %v", result, err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message-2")}); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	response, receiveErr := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 3})
	if receiveErr != nil || len(response.Messages) != 3 {
		t.Fatalf("receive %#v error %v", response, receiveErr)
	}
	for _, message := range response.Messages {
		seen[aws.ToString(message.Body)] = true
	}
	if !seen["message-0"] || !seen["message-1"] || !seen["message-2"] {
		t.Fatalf("batch bodies %#v", seen)
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

func TestAWSSDKSQSReceiveEmptyQueueContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-empty-receive")})
	if err != nil {
		t.Fatal(err)
	}
	for _, wait := range []int32{0, 1} {
		response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 1, WaitTimeSeconds: wait})
		if err != nil || response.Messages != nil {
			t.Fatalf("wait=%d response %#v error %v", wait, response, err)
		}
	}
}

func TestAWSSDKSQSReceiveWaitTimeContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-wait-time")})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message")}); err != nil {
			t.Fatal(err)
		}
	}
	for _, value := range []int32{-1, 21} {
		_, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, WaitTimeSeconds: value})
		want := fmt.Sprintf("Value %d for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided.", value)
		if err == nil || !strings.Contains(err.Error(), "InvalidParameterValue") || !strings.Contains(err.Error(), want) {
			t.Fatalf("wait=%d error %v", value, err)
		}
	}
	for range 2 {
		response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, WaitTimeSeconds: 0})
		if err != nil || len(response.Messages) != 1 || aws.ToString(response.Messages[0].Body) != "message" {
			t.Fatalf("short poll %#v error %v", response, err)
		}
	}
}

func TestAWSSDKSQSMessageTimestampContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-timestamps")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message")}); err != nil {
		t.Fatal(err)
	}
	response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll}})
	if err != nil || len(response.Messages) != 1 {
		t.Fatalf("receive %#v error %v", response, err)
	}
	attributes := response.Messages[0].Attributes
	sent, sentErr := strconv.ParseInt(attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64)
	first, firstErr := strconv.ParseInt(attributes[string(types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp)], 10, 64)
	if sentErr != nil || firstErr != nil || first < sent || first-sent > 1000 {
		t.Fatalf("timestamp attributes %#v", attributes)
	}
}

func TestAWSSDKSQSMultipleQueuesContract(t *testing.T) {
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
	queues := make([]string, 2)
	for index := range queues {
		created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("sdk-queue-%d", index))})
		if err != nil {
			t.Fatal(err)
		}
		queues[index] = aws.ToString(created.QueueUrl)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: &queues[0], MessageBody: aws.String("message")}); err != nil {
		t.Fatal(err)
	}
	empty, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: &queues[1]})
	if err != nil || empty.Messages != nil {
		t.Fatalf("queue-1 %#v error %v", empty, err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: &queues[0]})
	if err != nil || len(received.Messages) != 1 || aws.ToString(received.Messages[0].Body) != "message" {
		t.Fatalf("queue-0 %#v error %v", received, err)
	}
}

func TestAWSSDKSQSEncodedContentContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-encoded")})
	if err != nil {
		t.Fatal(err)
	}
	want := `"&quot;&quot;` + "\r"
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: &want}); err != nil {
		t.Fatal(err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(received.Messages) != 1 || aws.ToString(received.Messages[0].Body) != want {
		t.Fatalf("encoded response %#v error %v", received, err)
	}
}
