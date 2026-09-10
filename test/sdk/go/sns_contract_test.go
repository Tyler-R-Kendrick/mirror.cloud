package sdk_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
)

func TestAWSSDKSNSPublishLifecycleContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.sns"}
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
	client := sns.NewFromConfig(awsConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(server.URL) })
	topic, err := client.CreateTopic(context.Background(), &sns.CreateTopicInput{Name: aws.String("sdk-topic")})
	if err != nil || topic.TopicArn == nil {
		t.Fatalf("create topic %#v: %v", topic, err)
	}
	defer client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: topic.TopicArn})
	published, err := client.Publish(context.Background(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("sdk-message")})
	if err != nil || published.MessageId == nil || aws.ToString(published.MessageId) == "" {
		t.Fatalf("publish %#v: %v", published, err)
	}
	listed, err := client.ListTopics(context.Background(), &sns.ListTopicsInput{})
	if err != nil || len(listed.Topics) != 1 || aws.ToString(listed.Topics[0].TopicArn) != aws.ToString(topic.TopicArn) {
		t.Fatalf("list topics %#v: %v", listed, err)
	}
	_, err = client.Subscribe(context.Background(), &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sms"), Endpoint: aws.String("+15555550111"),
		Attributes: map[string]string{"FilterPolicy": "not-json"},
	})
	if err == nil {
		t.Fatal("sdk accepted invalid filter policy")
	}
	_, err = client.ConfirmSubscription(context.Background(), &sns.ConfirmSubscriptionInput{
		TopicArn: topic.TopicArn, Token: aws.String("randomtoken"),
	})
	if err == nil {
		t.Fatal("sdk accepted unissued confirm token")
	}
}
