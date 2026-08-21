package spine

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestHTTPProvenOpsMatchPackOperations(t *testing.T) {
	// Names exercised by TestBootedServerDynamoDB* / TestBootedServerSQS* on runtime.Boot().Handler().
	// If a pack advertises an op that is not here, it is not HTTP-proven — demote or add a Boot test.
	ddbHTTP := []string{
		"CreateTable", "DeleteTable", "DescribeTable", "ListTables", "UpdateTable",
		"PutItem", "GetItem", "DeleteItem", "UpdateItem", "BatchGetItem", "BatchWriteItem",
		"Query", "Scan", "TransactGetItems", "TransactWriteItems",
		"TagResource", "UntagResource", "ListTagsOfResource",
		"DescribeTimeToLive", "UpdateTimeToLive",
		"DescribeContinuousBackups", "UpdateContinuousBackups",
		"DescribeEndpoints", "DescribeLimits",
		"PutResourcePolicy", "GetResourcePolicy", "DeleteResourcePolicy",
		"CreateBackup", "ListBackups", "DescribeBackup", "DeleteBackup", "RestoreTableFromBackup",
		"EnableKinesisStreamingDestination", "DisableKinesisStreamingDestination", "DescribeKinesisStreamingDestination",
		"BatchExecuteStatement", "CreateGlobalTable", "DescribeContributorInsights", "DescribeExport",
		"DescribeGlobalTable", "DescribeGlobalTableSettings", "DescribeImport", "DescribeTableReplicaAutoScaling",
		"ExecuteStatement", "ExecuteTransaction", "ExportTableToPointInTime", "ImportTable",
		"ListContributorInsights", "ListExports", "ListGlobalTables", "ListImports",
		"RestoreTableToPointInTime", "SearchVectors", "UpdateContributorInsights", "UpdateGlobalTable",
		"UpdateGlobalTableSettings", "UpdateKinesisStreamingDestination", "UpdateTableReplicaAutoScaling",
		"ListStreams", "DescribeStream", "GetShardIterator", "GetRecords",
	}

	sqsHTTP := []string{
		"CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
		"SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags",
		"AddPermission", "RemovePermission", "ListDeadLetterSourceQueues",
		"StartMessageMoveTask", "CancelMessageMoveTask", "ListMessageMoveTasks",
	}

	assertSame(t, "dynamodb", dynamodb.New(spitest.Deps(t)).Operations(), ddbHTTP)
	assertSame(t, "sqs", sqs.New(spitest.Deps(t)).Operations(), sqsHTTP)
}

func assertSame(t *testing.T, label string, got, want []string) {
	t.Helper()
	gm, wm := map[string]bool{}, map[string]bool{}
	for _, s := range got {
		gm[s] = true
	}
	for _, s := range want {
		wm[s] = true
	}
	for s := range gm {
		if !wm[s] {
			t.Errorf("%s advertised %s without HTTP Boot proof", label, s)
		}
	}
	for s := range wm {
		if !gm[s] {
			t.Errorf("%s HTTP proof lists %s not in Operations()", label, s)
		}
	}
}
