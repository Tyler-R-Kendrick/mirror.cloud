package sdk_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
)

func TestAWSSDKDynamoDBGlobalTableContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.dynamodb"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	base, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("ap-south-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := func(region string) *dynamodb.Client {
		regional := base
		regional.Region = region
		return dynamodb.NewFromConfig(regional, func(options *dynamodb.Options) { options.BaseEndpoint = aws.String(server.URL) })
	}
	ap, us := client("ap-south-1"), client("us-east-1")
	if _, err := ap.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName:            aws.String("GlobalSDK"),
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		BillingMode:          ddbtypes.BillingModePayPerRequest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ap.UpdateTable(context.Background(), &dynamodb.UpdateTableInput{
		TableName:      aws.String("GlobalSDK"),
		ReplicaUpdates: []ddbtypes.ReplicationGroupUpdate{{Create: &ddbtypes.CreateReplicationGroupMemberAction{RegionName: aws.String("us-east-1")}}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ap.PutItem(context.Background(), &dynamodb.PutItemInput{TableName: aws.String("GlobalSDK"), Item: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "replicated"}}}); err != nil {
		t.Fatal(err)
	}
	item, err := us.GetItem(context.Background(), &dynamodb.GetItemInput{TableName: aws.String("GlobalSDK"), Key: map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "replicated"}}})
	if err != nil || len(item.Item) != 1 {
		t.Fatalf("replicated item: %#v %v", item, err)
	}
	table, err := us.DescribeTable(context.Background(), &dynamodb.DescribeTableInput{TableName: aws.String("GlobalSDK")})
	if err != nil || len(table.Table.Replicas) != 1 || aws.ToString(table.Table.Replicas[0].RegionName) != "us-east-1" {
		t.Fatalf("replica description: %#v %v", table, err)
	}
}
