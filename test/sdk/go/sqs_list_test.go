package sdk_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestAWSSDKSQSPublishGetDeleteMessageBatchContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-publish-get-delete-batch")})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]types.SendMessageBatchRequestEntry, 3)
	for i := range entries {
		entries[i] = types.SendMessageBatchRequestEntry{Id: aws.String(fmt.Sprintf("message-%d", i)), MessageBody: aws.String(fmt.Sprintf("body-%d", i))}
	}
	sent, err := client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	if err != nil || len(sent.Successful) != len(entries) {
		t.Fatalf("send %#v error %v", sent, err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 10})
	if err != nil || len(received.Messages) != len(entries) {
		t.Fatalf("receive %#v error %v", received, err)
	}
	deleteEntries := make([]types.DeleteMessageBatchRequestEntry, len(received.Messages))
	for i, message := range received.Messages {
		deleteEntries[i] = types.DeleteMessageBatchRequestEntry{Id: message.MessageId, ReceiptHandle: message.ReceiptHandle}
	}
	deleted, err := client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: created.QueueUrl, Entries: deleteEntries})
	if err != nil || len(deleted.Successful) != len(entries) {
		t.Fatalf("delete %#v error %v", deleted, err)
	}
	remaining, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 10})
	if err != nil || len(remaining.Messages) != 0 {
		t.Fatalf("remaining %#v error %v", remaining, err)
	}
}

func TestAWSSDKSQSInvalidBatchEntryIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-invalid-batch-id")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{{Id: aws.String("message:invalid"), MessageBody: aws.String("message")}}})
	if err == nil || !strings.Contains(err.Error(), "InvalidBatchEntryId") || !strings.Contains(err.Error(), "can only contain alphanumeric characters") {
		t.Fatalf("invalid batch id error %v", err)
	}
}

func TestAWSSDKSQSDeleteMessageBatchInvalidEntryIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-delete-invalid-batch-id")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.DeleteMessageBatchRequestEntry{{Id: aws.String("message:invalid"), ReceiptHandle: aws.String("handle")}}})
	if err == nil || !strings.Contains(err.Error(), "InvalidBatchEntryId") || !strings.Contains(err.Error(), "can only contain alphanumeric characters") {
		t.Fatalf("invalid delete batch id error %v", err)
	}
}

func TestAWSSDKSQSDeleteMessageBatchTooManyEntriesContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-delete-too-many-batch")})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]types.DeleteMessageBatchRequestEntry, 20)
	for i := range entries {
		entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(fmt.Sprintf("message-%d", i)), ReceiptHandle: aws.String("handle")}
	}
	_, err = client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	if err == nil || !strings.Contains(err.Error(), "TooManyEntriesInBatchRequest") || !strings.Contains(err.Error(), "Maximum number of entries per request are 10") {
		t.Fatalf("delete too many entries error %v", err)
	}
}

func TestAWSSDKSQSDeleteMessageBatchEmptyContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-delete-empty-batch")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteMessageBatch(context.Background(), &sqs.DeleteMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.DeleteMessageBatchRequestEntry{}})
	if err == nil || !strings.Contains(err.Error(), "EmptyBatchRequest") || !strings.Contains(err.Error(), "There should be at least one DeleteMessageBatchRequestEntry") {
		t.Fatalf("empty delete batch error %v", err)
	}
}

func TestAWSSDKSQSChangeMessageVisibilityBatchTooManyEntriesContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-visibility-too-many")})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 20)
	for i := range entries {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{Id: aws.String(fmt.Sprintf("message-%d", i)), ReceiptHandle: aws.String("handle"), VisibilityTimeout: 123}
	}
	_, err = client.ChangeMessageVisibilityBatch(context.Background(), &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	if err == nil || !strings.Contains(err.Error(), "TooManyEntriesInBatchRequest") || !strings.Contains(err.Error(), "Maximum number of entries per request are 10") {
		t.Fatalf("visibility too many entries error %v", err)
	}
}

func TestAWSSDKSQSFIFOBatchMissingDeduplicationIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch-missing-dedup.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("message-1"), MessageBody: aws.String("message-1"), MessageGroupId: aws.String("test-group"), MessageDeduplicationId: aws.String("dedup-1")},
		{Id: aws.String("message-2"), MessageBody: aws.String("message-2"), MessageGroupId: aws.String("test-group")},
	}})
	if err == nil || !strings.Contains(err.Error(), "ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly") {
		t.Fatalf("missing deduplication id error %v", err)
	}
}

func TestAWSSDKSQSFIFOBatchMissingMessageGroupIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch-missing-group.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("message-1"), MessageBody: aws.String("message-1"), MessageGroupId: aws.String("test-group"), MessageDeduplicationId: aws.String("dedup-1")},
		{Id: aws.String("message-2"), MessageBody: aws.String("message-2"), MessageDeduplicationId: aws.String("dedup-2")},
	}})
	if err == nil || !strings.Contains(err.Error(), "MissingParameter") || !strings.Contains(err.Error(), "MessageGroupId") {
		t.Fatalf("missing message group id error %v", err)
	}
}

func TestAWSSDKSQSTooManyBatchEntriesContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-too-many-batch")})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]types.SendMessageBatchRequestEntry, 20)
	for i := range entries {
		entries[i] = types.SendMessageBatchRequestEntry{Id: aws.String(fmt.Sprintf("message-%d", i)), MessageBody: aws.String("message")}
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	if err == nil || !strings.Contains(err.Error(), "TooManyEntriesInBatchRequest") || !strings.Contains(err.Error(), "Maximum number of entries per request are 10") {
		t.Fatalf("too many entries error %v", err)
	}
}

func TestAWSSDKSQSSendMessageBatchInvalidContentsPartialFailureContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch-invalid-contents")})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]types.SendMessageBatchRequestEntry, 10)
	for i := range entries {
		entries[i] = types.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), MessageBody: aws.String(strconv.Itoa(i))}
	}
	entries[9] = types.SendMessageBatchRequestEntry{Id: aws.String("9"), MessageBody: aws.String("\x01")}
	result, err := client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	if err != nil || len(result.Successful) != 9 || len(result.Failed) != 1 || aws.ToString(result.Failed[0].Code) != "InvalidMessageContents" {
		t.Fatalf("invalid contents batch result %#v error %v", result, err)
	}
}

func TestAWSSDKSQSEmptyMessageBatchContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-empty-batch")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{}})
	if err == nil || !strings.Contains(err.Error(), "EmptyBatchRequest") {
		t.Fatalf("empty batch error %v", err)
	}
}

func TestAWSSDKSQSMessageSizeContract(t *testing.T) {
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
	attributes := map[string]types.MessageAttributeValue{"k": {DataType: aws.String("String"), StringValue: aws.String("x")}}
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-oversized")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String(strings.Repeat("a", (1<<20)-7)), MessageAttributes: attributes})
	if err == nil || !strings.Contains(err.Error(), "Message must be shorter than 1048576 bytes") {
		t.Fatalf("default size error %v", err)
	}
	updated, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-maximum"), Attributes: map[string]string{"MaximumMessageSize": "1024"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: updated.QueueUrl, MessageBody: aws.String(strings.Repeat("a", 1024))}); err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: updated.QueueUrl, MessageBody: aws.String(strings.Repeat("a", 1017)), MessageAttributes: attributes})
	if err == nil || !strings.Contains(err.Error(), "Message must be shorter than 1024 bytes") {
		t.Fatalf("updated size error %v", err)
	}
	_, err = client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("1"), MessageBody: aws.String(strings.Repeat("a", (1<<20)-8)), MessageAttributes: attributes},
		{Id: aws.String("2"), MessageBody: aws.String("a")},
	}})
	if err == nil || !strings.Contains(err.Error(), "BatchRequestTooLong") || !strings.Contains(err.Error(), "1048577") {
		t.Fatalf("oversized batch error %v", err)
	}
	batchQueue, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch-maximum"), Attributes: map[string]string{"MaximumMessageSize": "2048"}})
	if err != nil {
		t.Fatal(err)
	}
	batchResult, err := client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: batchQueue.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("1"), MessageBody: aws.String(strings.Repeat("a", 2040)), MessageAttributes: attributes},
		{Id: aws.String("2"), MessageBody: aws.String("a")},
	}})
	if err != nil || len(batchResult.Successful) != 2 {
		t.Fatalf("updated batch %#v error %v", batchResult, err)
	}
}

func TestAWSSDKSQSBatchPerEntryMaximumSizeContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-batch-entry-maximum"), Attributes: map[string]string{"MaximumMessageSize": "1024"}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.SendMessageBatch(context.Background(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []types.SendMessageBatchRequestEntry{
		{Id: aws.String("valid"), MessageBody: aws.String(strings.Repeat("a", 1024))},
		{Id: aws.String("oversized"), MessageBody: aws.String(strings.Repeat("a", 1025))},
	}})
	if err != nil || len(result.Successful) != 1 || len(result.Failed) != 1 || result.Failed[0].Code == nil || !strings.Contains(*result.Failed[0].Code, "InvalidParameterValue") {
		t.Fatalf("batch per-entry size %#v error %v", result, err)
	}
}

func TestAWSSDKSQSStandardMessageGroupIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-standard-group")})
	if err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"", strings.Repeat("a", 129), "group 123"} {
		_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String(group)})
		if err == nil || !strings.Contains(err.Error(), "MessageGroupId can only include alphanumeric and punctuation characters") {
			t.Fatalf("group %q error %v", group, err)
		}
	}
}

func TestAWSSDKSQSFIFODeduplicationIDContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-dedup-invalid.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", strings.Repeat("a", 129), "group 123"} {
		_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String("group-1"), MessageDeduplicationId: aws.String(value)})
		if err == nil || !strings.Contains(err.Error(), "MessageDeduplicationId can only include alphanumeric and punctuation characters") {
			t.Fatalf("deduplication id %q error %v", value, err)
		}
	}
}

func TestAWSSDKSQSFIFOZeroDelayUsesQueueDelayContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-delay-zero.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "DelaySeconds": "2"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String("group-1"), DelaySeconds: 0}); err != nil {
		t.Fatal(err)
	}
	initial, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, WaitTimeSeconds: 0})
	if err != nil || len(initial.Messages) != 0 {
		t.Fatalf("initial %#v error %v", initial, err)
	}
	time.Sleep(2100 * time.Millisecond)
	after, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, WaitTimeSeconds: 0})
	if err != nil || len(after.Messages) != 1 || aws.ToString(after.Messages[0].Body) != "message" {
		t.Fatalf("after %#v error %v", after, err)
	}
}

func TestAWSSDKSQSFIFOPerMessageDelayContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-invalid-delay.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String("group-1"), DelaySeconds: 2})
	if err == nil || !strings.Contains(err.Error(), "not valid for this queue type") {
		t.Fatalf("FIFO per-message delay error %v", err)
	}
}

func TestAWSSDKSQSTagQueueContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-tagged")})
	if err != nil {
		t.Fatal(err)
	}
	tags := map[string]string{"tag1": "value1", "tag2": "value2", "tag3": ""}
	if _, err := client.TagQueue(context.Background(), &sqs.TagQueueInput{QueueUrl: created.QueueUrl, Tags: tags}); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != len(tags) || listed.Tags["tag1"] != "value1" || listed.Tags["tag3"] != "" {
		t.Fatalf("listed tags %#v error %v", listed, err)
	}
	if _, err := client.TagQueue(context.Background(), &sqs.TagQueueInput{QueueUrl: created.QueueUrl, Tags: map[string]string{"tag1": "VALUE1", "tag4": "value4"}}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != 4 || listed.Tags["tag1"] != "VALUE1" || listed.Tags["tag2"] != "value2" || listed.Tags["tag3"] != "" || listed.Tags["tag4"] != "value4" {
		t.Fatalf("overwritten tags %#v error %v", listed, err)
	}
	if _, err := client.UntagQueue(context.Background(), &sqs.UntagQueueInput{QueueUrl: created.QueueUrl, TagKeys: []string{"tag1", "tag3", "tag4", "missing"}}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != 1 || listed.Tags["tag2"] != "value2" {
		t.Fatalf("partial tags %#v error %v", listed, err)
	}
	if _, err := client.UntagQueue(context.Background(), &sqs.UntagQueueInput{QueueUrl: created.QueueUrl, TagKeys: []string{"tag2"}}); err != nil {
		t.Fatal(err)
	}
	listed, err = client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != 0 {
		t.Fatalf("empty tags %#v error %v", listed, err)
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
	queueWait, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-queue-wait"), Attributes: map[string]string{"ReceiveMessageWaitTimeSeconds": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, func() {
		_, _ = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: queueWait.QueueUrl, MessageBody: aws.String("queue-wait")})
	})
	response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: queueWait.QueueUrl})
	if err != nil || len(response.Messages) != 1 || aws.ToString(response.Messages[0].Body) != "queue-wait" {
		t.Fatalf("queue wait %#v error %v", response, err)
	}
}

func TestAWSSDKSQSInvalidReceiptHandleContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-invalid-receipt")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{QueueUrl: created.QueueUrl, ReceiptHandle: aws.String("garbage"), VisibilityTimeout: 60})
	if err == nil || !strings.Contains(err.Error(), "ReceiptHandleIsInvalid") || !strings.Contains(err.Error(), `The input receipt handle "garbage" is not a valid receipt handle.`) {
		t.Fatalf("invalid receipt handle error %v", err)
	}
}

func TestAWSSDKSQSInvalidMessageContentsContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-invalid-contents")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("Invalid-\x00")})
	if err == nil || !strings.Contains(err.Error(), "InvalidMessageContents") {
		t.Fatalf("invalid contents error %v", err)
	}
}

func TestAWSSDKSQSMessageRetentionContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-retention"), Attributes: map[string]string{"MessageRetentionPeriod": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("expires")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, WaitTimeSeconds: 0})
	if err != nil || len(response.Messages) != 0 {
		t.Fatalf("retention %#v error %v", response, err)
	}
}

func TestAWSSDKSQSSuccessivePurgeContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-purge")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PurgeQueue(context.Background(), &sqs.PurgeQueueInput{QueueUrl: created.QueueUrl}); err != nil {
		t.Fatal(err)
	}
	_, err = client.PurgeQueue(context.Background(), &sqs.PurgeQueueInput{QueueUrl: created.QueueUrl})
	if err == nil || !strings.Contains(err.Error(), "PurgeQueueInProgress") || !strings.Contains(err.Error(), "Only one PurgeQueue operation on sdk-purge is allowed every 60 seconds.") {
		t.Fatalf("successive purge error %v", err)
	}
}

func TestAWSSDKSQSApproximateMessageStatesContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-states")})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []struct {
		body  string
		delay int32
	}{{"visible", 0}, {"delayed-1", 2}, {"delayed-2", 2}} {
		if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String(message.body), DelaySeconds: message.delay}); err != nil {
			t.Fatal(err)
		}
	}
	attrs, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesDelayed", "ApproximateNumberOfMessagesNotVisible"}})
	if err != nil || attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] != "1" || attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesDelayed)] != "2" || attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] != "0" {
		t.Fatalf("initial attributes %#v error %v", attrs.Attributes, err)
	}
	if _, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, VisibilityTimeout: 5}); err != nil {
		t.Fatal(err)
	}
	attrs, err = client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}})
	if err != nil || attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)] != "0" || attrs.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] != "1" {
		t.Fatalf("in-flight attributes %#v error %v", attrs.Attributes, err)
	}
}

func TestAWSSDKSQSReceiptHandleRotationContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-rotate"), Attributes: map[string]string{"VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message")}); err != nil {
		t.Fatal(err)
	}
	first, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("first receive %#v error %v", first, err)
	}
	second, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(second.Messages) != 1 || aws.ToString(first.Messages[0].ReceiptHandle) == aws.ToString(second.Messages[0].ReceiptHandle) {
		t.Fatalf("receipt handles did not rotate first=%#v second=%#v error=%v", first.Messages, second.Messages, err)
	}
	if _, err := client.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{QueueUrl: created.QueueUrl, ReceiptHandle: first.Messages[0].ReceiptHandle, VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	third, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(third.Messages) != 1 {
		t.Fatalf("prior receipt handle was not usable: %#v error %v", third.Messages, err)
	}
}

func TestAWSSDKSQSFIFOExpiredReceiptDeleteContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-expired.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String("group")}); err != nil {
		t.Fatal(err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(received.Messages) != 1 {
		t.Fatalf("receive %#v error %v", received, err)
	}
	_, err = client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: created.QueueUrl, ReceiptHandle: received.Messages[0].ReceiptHandle})
	if err == nil || !strings.Contains(err.Error(), "receipt handle has expired") {
		t.Fatalf("expired FIFO delete error %v", err)
	}
}

func TestAWSSDKSQSFIFOMessageGroupReuseContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-reuse-group.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("first"), MessageGroupId: aws.String("g1")}); err != nil {
		t.Fatal(err)
	}
	first, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(first.Messages) != 1 {
		t.Fatalf("first receive %#v error %v", first, err)
	}
	if _, err := client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: created.QueueUrl, ReceiptHandle: first.Messages[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	empty, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(empty.Messages) != 0 {
		t.Fatalf("empty receive %#v error %v", empty, err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("second"), MessageGroupId: aws.String("g1")}); err != nil {
		t.Fatal(err)
	}
	final, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl})
	if err != nil || len(final.Messages) != 1 || aws.ToString(final.Messages[0].Body) != "second" {
		t.Fatalf("final receive %#v error %v", final, err)
	}
}

func TestAWSSDKSQSFIFOPartialGroupVisibilityContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-partial-group.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "30"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g1-m2", "g1"}, {"g2-m1", "g2"}} {
		if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String(message.body), MessageGroupId: aws.String(message.group)}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 2})
	if err != nil || len(first.Messages) != 2 {
		t.Fatalf("first receive %#v error %v", first, err)
	}
	if _, err := client.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{QueueUrl: created.QueueUrl, ReceiptHandle: first.Messages[0].ReceiptHandle, VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	second, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 3})
	if err != nil || len(second.Messages) != 2 || aws.ToString(second.Messages[0].Body) != "g2-m1" || aws.ToString(second.Messages[1].Body) != "g1-m1" {
		t.Fatalf("partial visibility receive %#v error %v", second, err)
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

func TestAWSSDKSQSFIFOMessageAttributesContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-fifo-attrs.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true", "VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageGroupId: aws.String("group-1"), MessageDeduplicationId: aws.String("dedup-1"), MessageAttributes: map[string]types.MessageAttributeValue{"kind": {DataType: aws.String("String"), StringValue: aws.String("fifo")}}}); err != nil {
		t.Fatal(err)
	}
	for want := int32(1); want <= 2; want++ {
		response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}, MessageAttributeNames: []string{"All"}, WaitTimeSeconds: 0})
		if err != nil || len(response.Messages) != 1 || aws.ToString(response.Messages[0].MessageAttributes["kind"].StringValue) != "fifo" || response.Messages[0].Attributes["ApproximateReceiveCount"] != fmt.Sprint(want) {
			t.Fatalf("receive %d %#v error %v", want, response, err)
		}
	}
}

func TestAWSSDKSQSFIFOSequenceNumberContract(t *testing.T) {
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
	fifo, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-sequence.fifo"), Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	previous := "0"
	for index := 1; index <= 3; index++ {
		result, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: fifo.QueueUrl, MessageBody: aws.String(fmt.Sprintf("message-%d", index)), MessageGroupId: aws.String("group"), MessageDeduplicationId: aws.String(fmt.Sprintf("dedup-%d", index))})
		if err != nil || result.SequenceNumber == nil || aws.ToString(result.SequenceNumber) <= previous {
			t.Fatalf("sequence %d %#v error %v", index, result, err)
		}
		previous = aws.ToString(result.SequenceNumber)
	}
	standard, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-sequence-standard")})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: standard.QueueUrl, MessageBody: aws.String("message")})
	if err != nil || result.SequenceNumber != nil {
		t.Fatalf("standard sequence %#v error %v", result, err)
	}
}

func TestAWSSDKSQSFIFODeduplicationScopeContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-dedup-scope.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("group-1"), MessageGroupId: aws.String("group-1"), MessageDeduplicationId: aws.String("same-dedup")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("group-2"), MessageGroupId: aws.String("group-2"), MessageDeduplicationId: aws.String("same-dedup")})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("duplicate"), MessageGroupId: aws.String("group-1"), MessageDeduplicationId: aws.String("same-dedup")})
	if err != nil || aws.ToString(duplicate.MessageId) != aws.ToString(first.MessageId) {
		t.Fatalf("duplicate %#v first %#v error %v", duplicate, first, err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 10, VisibilityTimeout: 0})
	if err != nil || len(received.Messages) != 2 {
		t.Fatalf("first=%#v second=%#v received=%#v error=%v", first, second, received, err)
	}
	createdQueueScope, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-dedup-scope-update.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "DeduplicationScope": "queue"}})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: createdQueueScope.QueueUrl, MessageBody: aws.String("first"), MessageGroupId: aws.String("group-1"), MessageDeduplicationId: aws.String("same-dedup")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: createdQueueScope.QueueUrl, Attributes: map[string]string{"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}}); err != nil {
		t.Fatal(err)
	}
	updated, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: createdQueueScope.QueueUrl, MessageBody: aws.String("updated"), MessageGroupId: aws.String("group-2"), MessageDeduplicationId: aws.String("same-dedup")})
	if err != nil || aws.ToString(updated.MessageId) != aws.ToString(initial.MessageId) {
		t.Fatalf("scope update initial=%#v updated=%#v error=%v", initial, updated, err)
	}
}

func TestAWSSDKSQSMessageAttributeDigestContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-attribute-digest")})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageAttributes: map[string]types.MessageAttributeValue{
		"binary": {DataType: aws.String("Binary"), BinaryValue: []byte{0, 1, 2}},
		"string": {DataType: aws.String("String"), StringValue: aws.String("value")},
	}})
	if err != nil || sent.MD5OfMessageAttributes == nil || len(aws.ToString(sent.MD5OfMessageAttributes)) != 32 {
		t.Fatalf("send %#v error %v", sent, err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageAttributeNames: []string{"All"}, WaitTimeSeconds: 0})
	if err != nil || len(received.Messages) != 1 || aws.ToString(received.Messages[0].MD5OfMessageAttributes) != aws.ToString(sent.MD5OfMessageAttributes) || len(received.Messages[0].MessageAttributes) != 2 {
		t.Fatalf("receive %#v error %v", received, err)
	}
}

func TestAWSSDKSQSMessageSystemAttributeDigestContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-system-attribute-digest")})
	if err != nil {
		t.Fatal(err)
	}
	attributes := map[string]types.MessageAttributeValue{"timestamp": {DataType: aws.String("Number"), StringValue: aws.String("1493147359900")}}
	without, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("test"), MessageAttributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	with, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("test"), MessageAttributes: attributes, MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String("Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1")}}})
	if err != nil || with.MD5OfMessageSystemAttributes == nil || aws.ToString(with.MD5OfMessageAttributes) != aws.ToString(without.MD5OfMessageAttributes) || aws.ToString(with.MD5OfMessageSystemAttributes) != "5ae4d5d7636402d80f4eb6d213245a88" {
		t.Fatalf("without=%#v with=%#v error=%v", without, with, err)
	}
}

func TestAWSSDKSQSTraceHeaderPropagationContract(t *testing.T) {
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
	trace := "Root=1-3152b799-8954dae64eda91bc9a23a7e8;Parent=7fa8c0f79203be72;Sampled=1"
	awsConfig.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		request = request.Clone(request.Context())
		request.Header = request.Header.Clone()
		request.Header.Set("X-Amzn-Trace-Id", trace)
		return http.DefaultTransport.RoundTrip(request)
	})}
	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(server.URL) })
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-trace-header")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("test")}); err != nil {
		t.Fatal(err)
	}
	received, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeName("AWSTraceHeader")}, MessageAttributeNames: []string{"All"}, VisibilityTimeout: 0})
	if err != nil || len(received.Messages) != 1 || received.Messages[0].Attributes["AWSTraceHeader"] != trace {
		t.Fatalf("trace receive %#v error %v", received, err)
	}
}

func TestAWSSDKSQSMessageAttributeValidationContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-attribute-validation")})
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"empty": "", "reserved": "Valid"} {
		attributeName := "ErrorDetails"
		if name == "reserved" {
			attributeName = "aWs.Invalid"
		}
		_, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("test"), MessageAttributes: map[string]types.MessageAttributeValue{attributeName: {DataType: aws.String("String"), StringValue: aws.String(value)}}})
		if err == nil || !strings.Contains(err.Error(), "InvalidParameterValue") {
			t.Fatalf("%s error %v", name, err)
		}
	}
}

func TestAWSSDKSQSMessageAttributeNameFiltersContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-attribute-filters"), Attributes: map[string]string{"VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	attributes := map[string]types.MessageAttributeValue{
		"Help.Me": {DataType: aws.String("String"), StringValue: aws.String("Me")},
		"Hello":   {DataType: aws.String("String"), StringValue: aws.String("There")},
		"General": {DataType: aws.String("String"), StringValue: aws.String("Kenobi")},
	}
	if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("message"), MessageAttributes: attributes}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		want []string
	}{
		{"exact", []string{"Hello"}},
		{"prefix", []string{"Hel.*"}},
		{"all", []string{"*"}},
	} {
		response, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageAttributeNames: tc.want, WaitTimeSeconds: 0})
		if err != nil || len(response.Messages) != 1 {
			t.Fatalf("%s %#v error %v", tc.name, response, err)
		}
		got := response.Messages[0].MessageAttributes
		if tc.name == "all" && len(got) != 3 || tc.name == "exact" && len(got) != 1 || tc.name == "prefix" && len(got) != 2 || tc.name != "all" && got["Hello"].StringValue == nil {
			t.Fatalf("%s attributes %#v", tc.name, got)
		}
	}
}

func TestAWSSDKSQSFIFOApproximateMessageCountContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-fifo-count.fifo"), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g1-m2", "g1"}, {"g1-m3", "g1"}, {"g2-m1", "g2"}, {"g3-m1", "g3"}} {
		if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String(message.body), MessageGroupId: aws.String(message.group)}); err != nil {
			t.Fatal(err)
		}
	}
	if before, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}}); err != nil || before.Attributes["ApproximateNumberOfMessages"] != "5" {
		t.Fatalf("before %#v error %v", before, err)
	}
	if _, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 4, WaitTimeSeconds: 0}); err != nil {
		t.Fatal(err)
	}
	after, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages}})
	if err != nil || after.Attributes["ApproximateNumberOfMessages"] != "1" {
		t.Fatalf("after %#v error %v", after, err)
	}
}

func TestAWSSDKSQSFIFOContentBasedDeduplicationStrategyContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-dedup-strategy.fifo"), Attributes: map[string]string{"FifoQueue": "true", "SqsManagedSseEnabled": "true", "ContentBasedDeduplication": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
	if err != nil || before.Attributes["ContentBasedDeduplication"] != "true" || before.Attributes["SqsManagedSseEnabled"] != "true" {
		t.Fatalf("before %#v error %v", before, err)
	}
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: created.QueueUrl, Attributes: map[string]string{"ContentBasedDeduplication": "false"}}); err != nil {
		t.Fatal(err)
	}
	after, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
	if err != nil || after.Attributes["ContentBasedDeduplication"] != "false" || after.Attributes["SqsManagedSseEnabled"] != "true" {
		t.Fatalf("after %#v error %v", after, err)
	}
}

func TestAWSSDKSQSRedrivePolicyClearingContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-redrive-policy")})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:123456789012:dlq","maxReceiveCount":"42"}`
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: created.QueueUrl, Attributes: map[string]string{"RedrivePolicy": policy, "Policy": policy}}); err != nil {
		t.Fatal(err)
	}
	set, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
	if err != nil || set.Attributes["RedrivePolicy"] != policy || set.Attributes["Policy"] != policy {
		t.Fatalf("set %#v error %v", set, err)
	}
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: created.QueueUrl, Attributes: map[string]string{"RedrivePolicy": "", "Policy": ""}}); err != nil {
		t.Fatal(err)
	}
	cleared, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll}})
	if err != nil {
		t.Fatal(err)
	}
	if _, present := cleared.Attributes["RedrivePolicy"]; present {
		t.Fatalf("policy was not cleared %#v", cleared)
	}
	if _, present := cleared.Attributes["Policy"]; present {
		t.Fatalf("policy was not cleared %#v", cleared)
	}
}

func TestAWSSDKSQSListDeadLetterSourceQueuesContract(t *testing.T) {
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
	_, err = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-dead-letter")})
	if err != nil {
		t.Fatal(err)
	}
	arn := "arn:aws:sqs:us-east-1:000000000000:sdk-dead-letter"
	for _, name := range []string{"sdk-source-a", "sdk-source-b"} {
		source, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: source.QueueUrl, Attributes: map[string]string{"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"42"}`, arn)}}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := client.ListDeadLetterSourceQueues(context.Background(), &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: aws.String("http://queue/000000000000/sdk-dead-letter")})
	if err != nil || len(result.QueueUrls) != 2 || !strings.Contains(result.QueueUrls[0], "sdk-source-") || !strings.Contains(result.QueueUrls[1], "sdk-source-") {
		t.Fatalf("sources %#v error %v", result, err)
	}
}

func TestAWSSDKSQSDeadLetterMaxReceiveCountContract(t *testing.T) {
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
	dlq, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-max-receive-dlq")})
	if err != nil {
		t.Fatal(err)
	}
	policy := fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:sdk-max-receive-dlq","maxReceiveCount":"1"}`)
	source, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-max-receive-source"), Attributes: map[string]string{"RedrivePolicy": policy, "VisibilityTimeout": "0"}})
	if err != nil {
		t.Fatal(err)
	}
	sent, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: source.QueueUrl, MessageBody: aws.String("poison")})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: source.QueueUrl, VisibilityTimeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: source.QueueUrl, VisibilityTimeout: 0})
	if err != nil {
		t.Fatal(err)
	}
	moved, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: dlq.QueueUrl})
	if err != nil || len(first.Messages) != 1 || len(second.Messages) != 0 || len(moved.Messages) != 1 || aws.ToString(moved.Messages[0].MessageId) != aws.ToString(sent.MessageId) {
		t.Fatalf("first=%#v second=%#v moved=%#v sent=%#v error=%v", first, second, moved, sent, err)
	}
}

func TestAWSSDKSQSSetFifoAttributeValidationContract(t *testing.T) {
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
	standard, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-standard-attribute")})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"true", "false"} {
		_, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: standard.QueueUrl, Attributes: map[string]string{"FifoQueue": value}})
		if err == nil || !strings.Contains(err.Error(), "InvalidAttributeName") {
			t.Fatalf("standard FifoQueue=%s error %v", value, err)
		}
	}
	fifo, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-fifo-attribute.fifo"), Attributes: map[string]string{"FifoQueue": "true"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: fifo.QueueUrl, Attributes: map[string]string{"FifoQueue": "true"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{QueueUrl: fifo.QueueUrl, Attributes: map[string]string{"FifoQueue": "false"}}); err == nil || !strings.Contains(err.Error(), "InvalidAttributeName") {
		t.Fatalf("fifo false error %v", err)
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

func TestAWSSDKSQSCreateQueueTagsContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-create-tags"), Tags: map[string]string{"tag1": "value1", "tag2": "value2"}})
	if err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != 2 || listed.Tags["tag1"] != "value1" || listed.Tags["tag2"] != "value2" {
		t.Fatalf("created tags %#v error %v", listed, err)
	}
}

func TestAWSSDKSQSTagCaseSensitivityContract(t *testing.T) {
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
	created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-tag-case")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.TagQueue(context.Background(), &sqs.TagQueueInput{QueueUrl: created.QueueUrl, Tags: map[string]string{"MyTag": "value1", "mytag": "value2"}}); err != nil {
		t.Fatal(err)
	}
	listed, err := client.ListQueueTags(context.Background(), &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	if err != nil || len(listed.Tags) != 2 || listed.Tags["MyTag"] != "value1" || listed.Tags["mytag"] != "value2" {
		t.Fatalf("case-sensitive tags %#v error %v", listed, err)
	}
}

func TestAWSSDKSQSCreateQueueIdempotencyContract(t *testing.T) {
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
	first, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-idempotent"), Attributes: map[string]string{"VisibilityTimeout": "69"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-idempotent")})
	if err != nil || aws.ToString(first.QueueUrl) != aws.ToString(second.QueueUrl) {
		t.Fatalf("idempotent first=%#v second=%#v error=%v", first, second, err)
	}
	_, err = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-idempotent"), Attributes: map[string]string{"VisibilityTimeout": "70"}})
	if err == nil || !strings.Contains(err.Error(), "different value for attribute VisibilityTimeout") {
		t.Fatalf("conflicting attributes error %v", err)
	}
	_, err = client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("sdk-standard-invalid"), Attributes: map[string]string{"FifoQueue": "false"}})
	if err == nil || !strings.Contains(err.Error(), "Unknown Attribute FifoQueue") {
		t.Fatalf("invalid fifo attribute error %v", err)
	}
}

func TestAWSSDKSQSFIFOQueueNameValidationContract(t *testing.T) {
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
	cases := []struct {
		name, want string
		attrs      map[string]string
	}{
		{name: "sdk-fifo-missing-attribute.fifo", want: "FifoQueue must be specified as true"},
		{name: "sdk-fifo-false-attribute.fifo", attrs: map[string]string{"FifoQueue": "false"}, want: "FifoQueue must be specified as true"},
		{name: "sdk-standard-with-fifo", attrs: map[string]string{"FifoQueue": "true"}, want: "Queue name must end in .fifo for FIFO queues"},
		{name: "sdk-queue-with-slash/name", want: "Queue name must be 1 to 80 characters"},
	}
	for _, tc := range cases {
		_, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(tc.name), Attributes: tc.attrs})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("queue %s validation error %v", tc.name, err)
		}
	}
}

func TestAWSSDKSQSFIFOGroupDeletionOrderingContract(t *testing.T) {
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
	populate := func(queueURL *string) {
		t.Helper()
		for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g2-m1", "g2"}, {"g1-m2", "g1"}, {"g2-m2", "g2"}, {"g1-m3", "g1"}, {"g1-m4", "g1"}, {"g3-m1", "g3"}} {
			if _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{QueueUrl: queueURL, MessageBody: aws.String(message.body), MessageGroupId: aws.String(message.group)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	create := func(name string) *string {
		t.Helper()
		created, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}})
		if err != nil {
			t.Fatal(err)
		}
		return created.QueueUrl
	}
	fullURL := create("sdk-delete-order.fifo")
	populate(fullURL)
	first, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: fullURL, MaxNumberOfMessages: 2})
	if err != nil || len(first.Messages) != 2 {
		t.Fatalf("first %#v error %v", first, err)
	}
	for _, message := range first.Messages {
		if _, err := client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: fullURL, ReceiptHandle: message.ReceiptHandle}); err != nil {
			t.Fatal(err)
		}
	}
	remaining, err := client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: fullURL, MaxNumberOfMessages: 10})
	if err != nil || len(remaining.Messages) != 5 || aws.ToString(remaining.Messages[0].Body) != "g2-m1" {
		t.Fatalf("full delete %#v error %v", remaining, err)
	}
	partialURL := create("sdk-partial-delete-order.fifo")
	populate(partialURL)
	first, err = client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: partialURL, MaxNumberOfMessages: 2})
	if err != nil || len(first.Messages) != 2 {
		t.Fatalf("partial first %#v error %v", first, err)
	}
	if _, err := client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: partialURL, ReceiptHandle: first.Messages[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	remaining, err = client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: partialURL, MaxNumberOfMessages: 10})
	if err != nil || len(remaining.Messages) != 3 || aws.ToString(remaining.Messages[0].Body) != "g2-m1" {
		t.Fatalf("partial delete %#v error %v", remaining, err)
	}
}
