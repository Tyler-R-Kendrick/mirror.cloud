package sdk_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

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
