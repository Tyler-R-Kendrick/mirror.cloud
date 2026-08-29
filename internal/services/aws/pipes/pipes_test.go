package pipes

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestPipesHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	defer p.Close()
	if n := len(p.Operations()); n != 10 {
		t.Fatalf("pipes Operations() %d want 10", n)
	}
}

func TestBootedServerPipesCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.pipes"}
	cfg.Seed = "pipes-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/pipes/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSPipes."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreatePipe", `{"Name":"p1","Source":"arn:aws:sqs:us-east-1:000000000000:src","Target":"arn:aws:sqs:us-east-1:000000000000:dst","RoleArn":"arn:aws:iam::000000000000:role/x"}`)
	if created["Name"] != "p1" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribePipe", `{"Name":"p1"}`)
	if got["Name"] != "p1" || got["CurrentState"] != "RUNNING" {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListPipes", `{}`)
	raw, _ := json.Marshal(listed)
	if !strings.Contains(string(raw), "p1") {
		t.Fatalf("list %s", raw)
	}
	stopped := call("StopPipe", `{"Name":"p1"}`)
	if stopped["CurrentState"] != "STOPPED" {
		t.Fatalf("stop %v", stopped)
	}
	call("DeletePipe", `{"Name":"p1"}`)
	gone := call("ListPipes", `{}`)
	raw, _ = json.Marshal(gone)
	if strings.Contains(string(raw), `"Name":"p1"`) {
		t.Fatalf("still present %s", raw)
	}
}

func TestPipesSQSDeliveryStateAndFiltering(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	for _, name := range []string{"source", "target", "filtered-source", "filtered-target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}

	pipe := pipeInput("delivery", "source", "target")
	pipe["DesiredState"] = "STOPPED"
	pipe["SourceParameters"] = map[string]any{"SqsQueueParameters": map[string]any{"BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", pipe)
	for _, body := range []string{"one", "two"} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": body})
	}
	p.drain(context.Background())
	if got := storedMessages(t, deps, id, "target"); len(got) != 0 {
		t.Fatalf("stopped pipe delivered %d messages", len(got))
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "delivery"})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "source")) == 0 && len(storedMessages(t, deps, id, "target")) == 2
	})
	if got := storedMessages(t, deps, id, "source"); len(got) != 0 {
		t.Fatalf("successful source messages retained: %v", got)
	}
	bodies := map[string]bool{}
	for _, message := range storedMessages(t, deps, id, "target") {
		var record map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &record); err != nil {
			t.Fatal(err)
		}
		bodies[record["body"].(string)] = true
		if record["eventSource"] != "aws:sqs" || record["eventSourceARN"] != queueARN(id, "source") || record["awsRegion"] != id.Region {
			t.Fatalf("source record %#v", record)
		}
	}
	if !bodies["one"] || !bodies["two"] {
		t.Fatalf("delivered bodies %v", bodies)
	}

	filtered := pipeInput("filter", "filtered-source", "filtered-target")
	filtered["DesiredState"] = "STOPPED"
	filtered["FilterCriteria"] = map[string]any{"Filters": []any{map[string]any{"Pattern": `{"body":{"kind":["keep"]}}`}}}
	invoke(t, p, id, "CreatePipe", filtered)
	for _, body := range []string{`{"kind":"keep"}`, `{"kind":"drop"}`} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "filtered-source", "MessageBody": body})
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "filter"})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "filtered-source")) == 0 && len(storedMessages(t, deps, id, "filtered-target")) == 1
	})
	var record map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "filtered-target")[0]["body"].(string)), &record); err != nil {
		t.Fatal(err)
	}
	if record["body"] != `{"kind":"keep"}` {
		t.Fatalf("filtered record %#v", record)
	}
}

func TestPipesRetriesFailedTargetWithoutDeletingSource(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "source"})
	invoke(t, p, id, "CreatePipe", pipeInput("retry", "source", "late"))
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": "retry-me"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "late"})
	if err := deps.Clock.(*clock.Controllable).Advance(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "source")) == 0 && len(storedMessages(t, deps, id, "late")) == 1
	})
}

func TestPipesKinesisDeliveryAndCheckpoint(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	stream, queue := kinesis.New(deps), sqs.New(deps)
	invoke(t, stream, id, "CreateStream", map[string]any{"StreamName": "events"})
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "target"})
	invoke(t, stream, id, "PutRecord", map[string]any{"StreamName": "events", "PartitionKey": "old", "Data": []byte("before")})

	input := pipeInput("kinesis", "unused", "target")
	input["Source"] = "arn:aws:kinesis:us-east-1:123456789012:stream/events"
	input["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "LATEST", "BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	checkpoint := deps.Store.Scope(id.Account, id.Region).Collection("pipecheckpoint")
	eventually(t, func() bool {
		_, ok, _ := checkpoint.Get(context.Background(), "kinesis")
		return ok
	})
	invoke(t, stream, id, "PutRecord", map[string]any{"StreamName": "events", "PartitionKey": "new", "Data": []byte("after")})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "target")) == 1 })

	var event map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "target")[0]["body"].(string)), &event); err != nil {
		t.Fatal(err)
	}
	if event["data"] != base64.StdEncoding.EncodeToString([]byte("after")) || event["partitionKey"] != "new" || event["eventSource"] != "aws:kinesis" || event["eventID"] != "shardId-000000000000:1" || event["invokeIdentityArn"] != input["RoleArn"] {
		t.Fatalf("Kinesis event %#v", event)
	}
	p.drain(context.Background())
	if got := len(storedMessages(t, deps, id, "target")); got != 1 {
		t.Fatalf("checkpoint redelivered %d records", got)
	}
	invoke(t, p, id, "DeletePipe", map[string]any{"Name": "kinesis"})
	if _, ok, _ := checkpoint.Get(context.Background(), "kinesis"); ok {
		t.Fatal("deleted pipe retained checkpoint")
	}

	bad := pipeInput("bad-kinesis", "unused", "target")
	bad["Source"] = input["Source"]
	bad["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "EARLIEST"}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
	bad["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "BatchSize": 10001}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
	for key, value := range map[string]any{
		"MaximumBatchingWindowInSeconds": 301,
		"MaximumRecordAgeInSeconds":      -2,
		"MaximumRetryAttempts":           10001,
		"ParallelizationFactor":          11,
	} {
		bad["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", key: value}}
		assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
	}
	bad["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "OnPartialBatchItemFailure": "INVALID"}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
	bad["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "DeadLetterConfig": map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:invalid.fifo"}}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
}

func TestPipesKinesisPartialBatchCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	stream, function := kinesis.New(deps), lambda.New(deps)
	invoke(t, stream, id, "CreateStream", map[string]any{"StreamName": "partial"})
	for _, data := range []string{"done", "retry"} {
		invoke(t, stream, id, "PutRecord", map[string]any{"StreamName": "partial", "PartitionKey": data, "Data": []byte(data)})
	}
	partial := "def lambda_handler(event, context):\n    return {'batchItemFailures': [{'itemIdentifier': event[-1]['eventID']}]}\n"
	invoke(t, function, id, "CreateFunction", map[string]any{
		"FunctionName": "kinesis-partial", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": lambdaCode(partial),
	})
	input := pipeInput("partial-kinesis", "unused", "unused")
	input["Source"] = "arn:aws:kinesis:us-east-1:123456789012:stream/partial"
	input["Target"] = "arn:aws:lambda:us-east-1:123456789012:function:kinesis-partial"
	input["DesiredState"] = "STOPPED"
	input["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	checkpoint := deps.Store.Scope(id.Account, id.Region).Collection("pipecheckpoint")
	position := func() string {
		raw, _, _ := checkpoint.Get(context.Background(), "partial-kinesis")
		decoded, _ := base64.StdEncoding.DecodeString(string(raw))
		return string(decoded)
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "partial-kinesis"})
	eventually(t, func() bool { return position() == "partial|1" })
	invoke(t, p, id, "StopPipe", map[string]any{"Name": "partial-kinesis"})

	success := "def lambda_handler(event, context):\n    return {}\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "kinesis-partial", "ZipFile": lambdaCode(success)["ZipFile"]})
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "partial-kinesis"})
	eventually(t, func() bool { return position() == "partial|2" })
}

func TestPipesKinesisRetryAgeAndDeadLetterPolicy(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	stream, queue := kinesis.New(deps), sqs.New(deps)
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "stream-dlq"})
	dlq := queueARN(id, "stream-dlq")

	invoke(t, stream, id, "CreateStream", map[string]any{"StreamName": "retry-policy"})
	invoke(t, stream, id, "PutRecord", map[string]any{"StreamName": "retry-policy", "PartitionKey": "retry", "Data": []byte("retry")})
	retry := pipeInput("retry-policy", "unused", "missing-target")
	retry["Source"] = "arn:aws:kinesis:us-east-1:123456789012:stream/retry-policy"
	retry["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{
		"StartingPosition": "TRIM_HORIZON", "MaximumRetryAttempts": 1, "DeadLetterConfig": map[string]any{"Arn": dlq},
	}}
	p := New(deps)
	if p.drainKinesis(context.Background(), id, retry, retry["Source"].(string)) {
		t.Fatal("failed record reported more work")
	}
	attempts := deps.Store.Scope(id.Account, id.Region).Collection("pipeattempt:retry-policy")
	if raw, ok, _ := attempts.Get(context.Background(), "shardId-000000000000:0"); !ok || string(raw) != "1" {
		t.Fatalf("persisted attempts %q, %v", raw, ok)
	}
	p.Close()

	p = New(deps)
	defer p.Close()
	p.drainKinesis(context.Background(), id, retry, retry["Source"].(string))
	if messages := storedMessages(t, deps, id, "stream-dlq"); len(messages) != 1 || !strings.Contains(messages[0]["body"].(string), `"eventID":"shardId-000000000000:0"`) {
		t.Fatalf("retry DLQ messages %#v", messages)
	}
	if _, ok, _ := attempts.Get(context.Background(), "shardId-000000000000:0"); ok {
		t.Fatal("exhausted retry retained attempt state")
	}

	invoke(t, stream, id, "CreateStream", map[string]any{"StreamName": "age-policy"})
	invoke(t, stream, id, "PutRecord", map[string]any{"StreamName": "age-policy", "PartitionKey": "old", "Data": []byte("old")})
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "age-target"})
	if err := deps.Clock.Advance(61 * time.Second); err != nil {
		t.Fatal(err)
	}
	age := pipeInput("age-policy", "unused", "age-target")
	age["Source"] = "arn:aws:kinesis:us-east-1:123456789012:stream/age-policy"
	age["SourceParameters"] = map[string]any{"KinesisStreamParameters": map[string]any{
		"StartingPosition": "TRIM_HORIZON", "MaximumRecordAgeInSeconds": 60, "DeadLetterConfig": map[string]any{"Arn": dlq},
	}}
	p.drainKinesis(context.Background(), id, age, age["Source"].(string))
	if got := len(storedMessages(t, deps, id, "age-target")); got != 0 {
		t.Fatalf("expired record reached target %d times", got)
	}
	if got := len(storedMessages(t, deps, id, "stream-dlq")); got != 2 {
		t.Fatalf("DLQ message count %d want 2", got)
	}
}

func TestPipesDynamoDBStreamDeliveryAndCheckpoint(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	database, queue := dynamodb.New(deps), sqs.New(deps)
	created := invoke(t, database, id, "CreateTable", map[string]any{
		"TableName": "Events", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
		"StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"},
	})
	streamARN := stringValue(created.Output["TableDescription"].(map[string]any)["LatestStreamArn"])
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "ddb-target"})
	invoke(t, database, id, "PutItem", map[string]any{"TableName": "Events", "Item": map[string]any{"id": map[string]any{"S": "old"}}})

	input := pipeInput("dynamodb", "unused", "ddb-target")
	input["Source"] = streamARN
	input["SourceParameters"] = map[string]any{"DynamoDBStreamParameters": map[string]any{"StartingPosition": "LATEST", "BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	checkpoint := deps.Store.Scope(id.Account, id.Region).Collection("pipecheckpoint")
	eventually(t, func() bool {
		_, ok, _ := checkpoint.Get(context.Background(), "dynamodb")
		return ok
	})
	invoke(t, database, id, "PutItem", map[string]any{"TableName": "Events", "Item": map[string]any{"id": map[string]any{"S": "new"}, "value": map[string]any{"N": "2"}}})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "ddb-target")) == 1 })

	var event map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "ddb-target")[0]["body"].(string)), &event); err != nil {
		t.Fatal(err)
	}
	detail := event["dynamodb"].(map[string]any)
	if _, ok := detail["ApproximateCreationDateTime"]; !ok {
		t.Fatalf("DynamoDB stream event missing creation time %#v", event)
	}
	if event["eventSource"] != "aws:dynamodb" || event["eventVersion"] != "1.0" || event["eventSourceARN"] != strings.Split(streamARN, "/stream/")[0] || event["eventName"] != "INSERT" || detail["SequenceNumber"] != "000000000000002" {
		t.Fatalf("DynamoDB stream event %#v", event)
	}
	p.drain(context.Background())
	if got := len(storedMessages(t, deps, id, "ddb-target")); got != 1 {
		t.Fatalf("checkpoint redelivered %d records", got)
	}
	assertFault(t, p, id, "UpdatePipe", map[string]any{"Name": "dynamodb", "Source": strings.Replace(streamARN, "Events", "Other", 1)}, "ValidationException")

	bad := pipeInput("bad-dynamodb", "unused", "ddb-target")
	bad["Source"] = streamARN
	bad["SourceParameters"] = map[string]any{"DynamoDBStreamParameters": map[string]any{"StartingPosition": "AT_TIMESTAMP"}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
	bad["SourceParameters"] = map[string]any{"DynamoDBStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "BatchSize": 10001}}
	assertFault(t, p, id, "CreatePipe", bad, "ValidationException")
}

func TestPipesDynamoDBPartialBatchCheckpoint(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	database, function := dynamodb.New(deps), lambda.New(deps)
	created := invoke(t, database, id, "CreateTable", map[string]any{
		"TableName": "Partial", "KeySchema": []any{map[string]any{"AttributeName": "id", "KeyType": "HASH"}},
		"StreamSpecification": map[string]any{"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"},
	})
	for _, key := range []string{"done", "retry"} {
		invoke(t, database, id, "PutItem", map[string]any{"TableName": "Partial", "Item": map[string]any{"id": map[string]any{"S": key}}})
	}
	partial := "def lambda_handler(event, context):\n    return {'batchItemFailures': [{'itemIdentifier': event[-1]['eventID']}]}\n"
	invoke(t, function, id, "CreateFunction", map[string]any{
		"FunctionName": "ddb-partial", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": lambdaCode(partial),
	})
	input := pipeInput("partial-dynamodb", "unused", "unused")
	input["Source"] = created.Output["TableDescription"].(map[string]any)["LatestStreamArn"]
	input["Target"] = "arn:aws:lambda:us-east-1:123456789012:function:ddb-partial"
	input["DesiredState"] = "STOPPED"
	input["SourceParameters"] = map[string]any{"DynamoDBStreamParameters": map[string]any{"StartingPosition": "TRIM_HORIZON", "BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	checkpoint := deps.Store.Scope(id.Account, id.Region).Collection("pipecheckpoint")
	position := func() string {
		raw, _, _ := checkpoint.Get(context.Background(), "partial-dynamodb")
		decoded, _ := base64.StdEncoding.DecodeString(string(raw))
		return string(decoded)
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "partial-dynamodb"})
	eventually(t, func() bool { return position() == "Partial|2" })
	invoke(t, p, id, "StopPipe", map[string]any{"Name": "partial-dynamodb"})

	success := "def lambda_handler(event, context):\n    return {}\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "ddb-partial", "ZipFile": lambdaCode(success)["ZipFile"]})
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "partial-dynamodb"})
	eventually(t, func() bool { return position() == "Partial|3" })
}

func TestPipesStepFunctionsTarget(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue, machine := sqs.New(deps), states.New(deps)
	for _, name := range []string{"states-source", "async-states-source", "failed-states-source"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	created := invoke(t, machine, id, "CreateStateMachine", map[string]any{
		"name": "pipe-target", "type": "EXPRESS", "roleArn": "arn:aws:iam::123456789012:role/states",
		"definition": `{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`,
	})
	input := pipeInput("states", "states-source", "unused")
	input["Target"] = created.Output["stateMachineArn"]
	input["DesiredState"] = "STOPPED"
	input["TargetParameters"] = map[string]any{
		"InputTemplate":          `{"body":<$.body>}`,
		"StateMachineParameters": map[string]any{"InvocationType": "REQUEST_RESPONSE"},
	}
	invoke(t, p, id, "CreatePipe", input)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "states-source", "MessageBody": `{"value":1}`})
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "states"})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "states-source")) == 0 })
	executions := storedStateExecutions(t, deps, id)
	if len(executions) != 1 || !strings.Contains(stringValue(executions[0]["input"]), `"value":1`) {
		t.Fatalf("state executions %#v", executions)
	}
	standard := invoke(t, machine, id, "CreateStateMachine", map[string]any{
		"name": "pipe-async", "roleArn": "arn:aws:iam::123456789012:role/states",
		"definition": `{"StartAt":"Done","States":{"Done":{"Type":"Pass","End":true}}}`,
	})
	async := pipeInput("async-states", "async-states-source", "unused")
	async["Target"] = standard.Output["stateMachineArn"]
	async["TargetParameters"] = map[string]any{"StateMachineParameters": map[string]any{"InvocationType": "FIRE_AND_FORGET"}}
	invoke(t, p, id, "CreatePipe", async)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "async-states-source", "MessageBody": "async"})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "async-states-source")) == 0 })

	failed := invoke(t, machine, id, "CreateStateMachine", map[string]any{
		"name": "pipe-failure", "type": "EXPRESS", "roleArn": "arn:aws:iam::123456789012:role/states",
		"definition": `{"StartAt":"Failed","States":{"Failed":{"Type":"Fail","Error":"Nope","Cause":"retry"}}}`,
	})
	failing := pipeInput("failed-states", "failed-states-source", "unused")
	failing["Target"] = failed.Output["stateMachineArn"]
	invoke(t, p, id, "CreatePipe", failing)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "failed-states-source", "MessageBody": "retry"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "failed-states-source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if messages := storedMessages(t, deps, id, "failed-states-source"); len(messages) != 1 {
		t.Fatalf("failed synchronous invocation consumed source: %#v", messages)
	}
	invalid := pipeInput("invalid-states", "states-source", "unused")
	invalid["Target"] = created.Output["stateMachineArn"]
	invalid["TargetParameters"] = map[string]any{"StateMachineParameters": map[string]any{"InvocationType": "INVALID"}}
	assertFault(t, p, id, "CreatePipe", invalid, "ValidationException")
}

func TestPipesStepFunctionsEnrichment(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue, machine := sqs.New(deps), states.New(deps)
	for _, name := range []string{"enrich-states-source", "enrich-states-target", "failed-enrich-states-source", "failed-enrich-states-target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	created := invoke(t, machine, id, "CreateStateMachine", map[string]any{
		"name": "pipe-enrichment", "type": "EXPRESS", "roleArn": "arn:aws:iam::123456789012:role/states",
		"definition": `{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","Result":[{"body":{"value":6},"id":"enriched"}],"End":true}}}`,
	})
	input := pipeInput("states-enrichment", "enrich-states-source", "enrich-states-target")
	input["Enrichment"] = created.Output["stateMachineArn"]
	input["EnrichmentParameters"] = map[string]any{"InputTemplate": `{"body":<$.body>,"id":<$.messageId>}`}
	input["TargetParameters"] = map[string]any{"InputTemplate": `{"value":<$.body.value>,"id":<$.id>}`}
	invoke(t, p, id, "CreatePipe", input)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "enrich-states-source", "MessageBody": `{"value":3}`})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "enrich-states-source")) == 0 && len(storedMessages(t, deps, id, "enrich-states-target")) == 1
	})
	var enriched map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "enrich-states-target")[0]["body"].(string)), &enriched); err != nil {
		t.Fatal(err)
	}
	if enriched["value"] != float64(6) || enriched["id"] != "enriched" {
		t.Fatalf("enriched output %#v", enriched)
	}
	executions := storedStateExecutions(t, deps, id)
	if len(executions) != 1 || !strings.Contains(stringValue(executions[0]["input"]), `"value":3`) {
		t.Fatalf("enrichment input %#v", executions)
	}

	failed := invoke(t, machine, id, "CreateStateMachine", map[string]any{
		"name": "failed-enrichment", "type": "EXPRESS", "roleArn": "arn:aws:iam::123456789012:role/states",
		"definition": `{"StartAt":"Fail","States":{"Fail":{"Type":"Fail","Error":"Nope","Cause":"retry"}}}`,
	})
	failing := pipeInput("failed-states-enrichment", "failed-enrich-states-source", "failed-enrich-states-target")
	failing["Enrichment"] = failed.Output["stateMachineArn"]
	invoke(t, p, id, "CreatePipe", failing)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "failed-enrich-states-source", "MessageBody": "retry"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "failed-enrich-states-source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	if messages := storedMessages(t, deps, id, "failed-enrich-states-target"); len(messages) != 0 {
		t.Fatalf("failed enrichment invoked target: %#v", messages)
	}
}

func TestPipesAPIGatewayEnrichment(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue, function, gateway := sqs.New(deps), lambda.New(deps), apigateway.New(deps)
	for _, name := range []string{"api-source", "api-target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	source := "def lambda_handler(event, context):\n    import json\n    body=json.loads(event['body'])\n    return {'statusCode': 200, 'body': json.dumps([{'value': body[0]['value'] * 3, 'path': event['path'], 'query': event['queryStringParameters']['kind'], 'header': event['headers']['X-Test']}])}\n"
	invoke(t, function, id, "CreateFunction", map[string]any{
		"FunctionName": "api-enrichment", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": lambdaCode(source),
	})
	api := invoke(t, gateway, id, "CreateRestApi", map[string]any{"name": "pipe-enrichment"}).Output
	apiID, root := stringValue(api["id"]), stringValue(api["rootResourceId"])
	invoke(t, gateway, id, "PutMethod", map[string]any{"restApiId": apiID, "resourceId": root, "httpMethod": "POST", "authorizationType": "NONE"})
	invoke(t, gateway, id, "PutIntegration", map[string]any{
		"restApiId": apiID, "resourceId": root, "httpMethod": "POST", "type": "AWS_PROXY", "integrationHttpMethod": "POST",
		"uri": "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:api-enrichment/invocations",
	})
	invoke(t, gateway, id, "CreateDeployment", map[string]any{"restApiId": apiID, "stageName": "prod"})

	input := pipeInput("api-enrichment", "api-source", "api-target")
	input["Enrichment"] = "arn:aws:execute-api:us-east-1:123456789012:" + apiID + "/prod/POST/*"
	input["EnrichmentParameters"] = map[string]any{
		"InputTemplate": `{"value":<$.body.value>}`,
		"HttpParameters": map[string]any{
			"PathParameterValues": []any{"$.body.kind"}, "QueryStringParameters": map[string]any{"kind": "$.body.kind"}, "HeaderParameters": map[string]any{"X-Test": "$.body.value"},
		},
	}
	input["TargetParameters"] = map[string]any{"InputTemplate": `{"value":<$.value>,"path":<$.path>,"query":<$.query>,"header":<$.header>}`}
	invoke(t, p, id, "CreatePipe", input)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "api-source", "MessageBody": `{"value":2,"kind":"widgets"}`})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "api-source")) == 0 && len(storedMessages(t, deps, id, "api-target")) == 1
	})
	var enriched map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "api-target")[0]["body"].(string)), &enriched); err != nil {
		t.Fatal(err)
	}
	if enriched["value"] != float64(6) || enriched["path"] != "/widgets" || enriched["query"] != "widgets" || enriched["header"] != "2" {
		t.Fatalf("API Gateway enrichment %#v", enriched)
	}

	failure := "def lambda_handler(event, context):\n    return {'statusCode': 500, 'body': 'failed'}\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "api-enrichment", "ZipFile": lambdaCode(failure)["ZipFile"]})
	if _, err := p.invokeAPIGateway(context.Background(), id, input, stringValue(input["Enrichment"]), []byte(`[{"value":3}]`), map[string]any{"body": map[string]any{"kind": "retry", "value": 3}}); err == nil {
		t.Fatal("API Gateway enrichment accepted a 5xx response")
	}
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "api-source", "MessageBody": `{"value":3,"kind":"retry"}`})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "api-source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	if messages := storedMessages(t, deps, id, "api-target"); len(messages) != 1 {
		t.Fatalf("failed API enrichment invoked target: %#v", messages)
	}
}

func TestPipesAPIDestinationEnrichmentAndTarget(t *testing.T) {
	type call struct{ path, shared, dynamic, apiKey, body string }
	calls := make(chan call, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls <- call{r.URL.Path, r.URL.Query().Get("shared"), r.URL.Query().Get("dynamic"), r.Header.Get("X-Api-Key"), string(body)}
		if r.URL.Path == "/fail" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/enrich/") {
			_, _ = w.Write([]byte(`[{"value":9,"kind":"enriched"}]`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue, eventbridge := sqs.New(deps), events.New(deps)
	defer eventbridge.Close()
	for _, name := range []string{"destination-source", "destination-target", "api-target-source", "destination-failed-source"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	connection := invoke(t, eventbridge, id, "CreateConnection", map[string]any{
		"Name": "pipe-connection", "AuthorizationType": "API_KEY",
		"AuthParameters": map[string]any{
			"ApiKeyAuthParameters":     map[string]any{"ApiKeyName": "X-Api-Key", "ApiKeyValue": "secret"},
			"InvocationHttpParameters": map[string]any{"QueryStringParameters": []any{map[string]any{"Key": "shared", "Value": "connection"}}},
		},
	}).Output["ConnectionArn"]
	destination := invoke(t, eventbridge, id, "CreateApiDestination", map[string]any{
		"Name": "pipe-destination", "ConnectionArn": connection, "InvocationEndpoint": server.URL + "/enrich/*", "HttpMethod": "POST",
	}).Output["ApiDestinationArn"]

	input := pipeInput("api-destination-enrichment", "destination-source", "destination-target")
	input["Enrichment"] = destination
	input["EnrichmentParameters"] = map[string]any{
		"InputTemplate": `{"value":<$.body.value>}`,
		"HttpParameters": map[string]any{
			"PathParameterValues":   []any{"$.body.kind"},
			"QueryStringParameters": map[string]any{"shared": "pipe", "dynamic": "$.body.value"},
		},
	}
	input["TargetParameters"] = map[string]any{"InputTemplate": `{"value":<$.value>,"kind":<$.kind>}`}
	invoke(t, p, id, "CreatePipe", input)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "destination-source", "MessageBody": `{"value":3,"kind":"widgets"}`})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "destination-target")) == 1 })
	firstCall := <-calls
	if firstCall.path != "/enrich/widgets" || firstCall.shared != "connection" || firstCall.dynamic != "3" || firstCall.apiKey != "secret" || !strings.Contains(firstCall.body, `"value":3`) {
		t.Fatalf("API destination enrichment call %#v", firstCall)
	}
	if body := storedMessages(t, deps, id, "destination-target")[0]["body"]; body != `{"value":9,"kind":"enriched"}` {
		t.Fatalf("API destination enrichment body %v", body)
	}

	target := pipeInput("api-destination-target", "api-target-source", "unused")
	target["Target"] = destination
	target["TargetParameters"] = map[string]any{
		"InputTemplate":  `{"value":<$.body.value>}`,
		"HttpParameters": map[string]any{"PathParameterValues": []any{"$.body.kind"}, "QueryStringParameters": map[string]any{"dynamic": "$.body.value"}},
	}
	invoke(t, p, id, "CreatePipe", target)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "api-target-source", "MessageBody": `{"value":4,"kind":"target"}`})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "api-target-source")) == 0 })
	secondCall := <-calls
	if secondCall.path != "/enrich/target" || secondCall.dynamic != "4" || secondCall.apiKey != "secret" || !strings.Contains(secondCall.body, `"value":4`) {
		t.Fatalf("API destination target call %#v", secondCall)
	}

	failedDestination := invoke(t, eventbridge, id, "CreateApiDestination", map[string]any{
		"Name": "failed-destination", "ConnectionArn": connection, "InvocationEndpoint": server.URL + "/fail", "HttpMethod": "POST",
	}).Output["ApiDestinationArn"]
	failed := pipeInput("failed-api-destination", "destination-failed-source", "unused")
	failed["Enrichment"] = failedDestination
	invoke(t, p, id, "CreatePipe", failed)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "destination-failed-source", "MessageBody": "retry"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "destination-failed-source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
	if failedCall := <-calls; failedCall.path != "/fail" {
		t.Fatalf("failed API destination call %#v", failedCall)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if messages := storedMessages(t, deps, id, "destination-failed-source"); len(messages) != 1 {
		t.Fatalf("failed API destination consumed source: %#v", messages)
	}
}

func TestPipesControlPlaneValidationUpdatesAndTags(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	if p.ServiceID() != "aws.pipes" || p.Tier() == "" {
		t.Fatalf("identity %q %q", p.ServiceID(), p.Tier())
	}
	assertFault(t, p, id, "CreatePipe", map[string]any{"Name": "bad"}, "ValidationException")
	badState := pipeInput("bad-state", "source", "target")
	badState["DesiredState"] = "PAUSED"
	assertFault(t, p, id, "CreatePipe", badState, "ValidationException")

	input := pipeInput("control", "source", "target")
	input["DesiredState"] = "STOPPED"
	created := invoke(t, p, id, "CreatePipe", input)
	arn := created.Output["Arn"].(string)
	assertFault(t, p, id, "CreatePipe", input, "ConflictException")
	assertFault(t, p, id, "DescribePipe", map[string]any{"Name": "missing"}, "NotFoundException")
	assertFault(t, p, id, "UpdatePipe", map[string]any{"Name": "control", "DesiredState": "PAUSED"}, "ValidationException")
	updated := invoke(t, p, id, "UpdatePipe", map[string]any{"Name": "control", "DesiredState": "RUNNING", "Target": queueARN(id, "other")})
	if updated.Output["CurrentState"] != "RUNNING" {
		t.Fatalf("update %#v", updated.Output)
	}
	described := invoke(t, p, id, "DescribePipe", map[string]any{"Name": "control"})
	if described.Output["Target"] != queueARN(id, "other") || described.Output["CurrentState"] != "RUNNING" {
		t.Fatalf("describe %#v", described.Output)
	}
	if listed := invoke(t, p, id, "ListPipes", map[string]any{}).Output["Pipes"].([]any); len(listed) != 1 {
		t.Fatalf("list %#v", listed)
	}

	invoke(t, p, id, "TagResource", map[string]any{"resourceArn": arn, "tags": map[string]any{"keep": "1", "remove": "2"}})
	invoke(t, p, id, "TagResource", map[string]any{"resourceArn": arn, "tags": map[string]any{"keep": "updated"}})
	invoke(t, p, id, "UntagResource", map[string]any{"resourceArn": arn, "tagKeys": []any{"remove"}})
	tags := invoke(t, p, id, "ListTagsForResource", map[string]any{"resourceArn": arn}).Output["tags"].(map[string]any)
	if len(tags) != 1 || tags["keep"] != "updated" {
		t.Fatalf("tags %#v", tags)
	}

	invoke(t, p, id, "DeletePipe", map[string]any{"Name": "control"})
	assertFault(t, p, id, "DeletePipe", map[string]any{"Name": "control"}, "NotFoundException")
	assertFault(t, p, id, "Unknown", map[string]any{}, "MirrorNotImplemented")
	if sourceBatchSize(map[string]any{}) != 10 || sourceBatchSize(map[string]any{"SourceParameters": map[string]any{"SqsQueueParameters": map[string]any{"BatchSize": float64(99)}}}) != 10 {
		t.Fatal("invalid SQS batch size bounds")
	}
	if err := New(spi.Deps{}).Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPipesTargetInputTemplate(t *testing.T) {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	for _, name := range []string{"source", "target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	input := pipeInput("transform", "source", "target")
	input["DesiredState"] = "STOPPED"
	input["TargetParameters"] = map[string]any{"InputTemplate": `{"kind":<$.body.kind>,"second":<$.body.items[1]>,"all":<$.body.items[*]>,"summary":"<$.body.kind>-<aws.pipes.pipe-name>","event":<aws.pipes.event.json>}`}
	invoke(t, p, id, "CreatePipe", input)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": `{"kind":"keep","items":[1,2]}`})
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "transform"})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "target")) == 1 })
	var transformed map[string]any
	if err := json.Unmarshal([]byte(storedMessages(t, deps, id, "target")[0]["body"].(string)), &transformed); err != nil {
		t.Fatal(err)
	}
	event := transformed["event"].(map[string]any)
	if transformed["kind"] != "keep" || transformed["second"] != float64(2) || transformed["summary"] != "keep-transform" || len(transformed["all"].([]any)) != 2 || event["body"].(map[string]any)["kind"] != "keep" {
		t.Fatalf("transformed %#v", transformed)
	}
	// The pipe deletes the source message after it has forwarded it, so the
	// target arriving does not mean the source has drained yet: asserting that
	// synchronously fails whenever the runner interleaves the two steps. Both
	// halves are the same delivery, so both are waited for.
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "source")) == 0 })
}

func TestPipesLambdaPartialBatchResponse(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	function := lambda.New(deps)
	invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": "source"})
	partial := "def lambda_handler(event, context):\n    return {'batchItemFailures': [{'itemIdentifier': event[-1]['messageId']}]}\n"
	invoke(t, function, id, "CreateFunction", map[string]any{
		"FunctionName": "partial", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": lambdaCode(partial),
	})
	input := pipeInput("partial", "source", "unused")
	input["Target"] = "arn:aws:lambda:us-east-1:123456789012:function:partial"
	input["DesiredState"] = "STOPPED"
	input["SourceParameters"] = map[string]any{"SqsQueueParameters": map[string]any{"BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	for _, body := range []string{"done", "retry"} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": body})
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "partial"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "source")
		return len(messages) == 1 && messages[0]["body"] == "retry" && messages[0]["receiveCount"] == float64(1)
	})
	invoke(t, p, id, "StopPipe", map[string]any{"Name": "partial"})
	pipe := invoke(t, p, id, "DescribePipe", map[string]any{"Name": "partial"}).Output

	invalid := "def lambda_handler(event, context):\n    return {'batchItemFailures': [{'itemIdentifier': 'unknown'}]}\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "partial", "ZipFile": lambdaCode(invalid)["ZipFile"]})
	_ = deps.Clock.(*clock.Controllable).Advance(30 * time.Second)
	received := invoke(t, queue, id, "ReceiveMessage", map[string]any{"QueueName": "source", "MaxNumberOfMessages": 2}).Output["Messages"].([]any)
	p.processBatch(context.Background(), id, pipe, queueARN(id, "source"), "source", received)
	if messages := storedMessages(t, deps, id, "source"); len(messages) != 1 || messages[0]["receiveCount"] != float64(2) {
		t.Fatalf("invalid partial response deleted source: %#v", messages)
	}

	success := "def lambda_handler(event, context):\n    return {}\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "partial", "ZipFile": lambdaCode(success)["ZipFile"]})
	_ = deps.Clock.(*clock.Controllable).Advance(30 * time.Second)
	received = invoke(t, queue, id, "ReceiveMessage", map[string]any{"QueueName": "source"}).Output["Messages"].([]any)
	p.processBatch(context.Background(), id, pipe, queueARN(id, "source"), "source", received)
	if messages := storedMessages(t, deps, id, "source"); len(messages) != 0 {
		t.Fatalf("successful retry retained source: %#v", messages)
	}
}

func TestPipesLambdaEnrichment(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	deps := spitest.Deps(t)
	p := New(deps)
	defer p.Close()
	queue := sqs.New(deps)
	function := lambda.New(deps)
	for _, name := range []string{"source", "target", "empty-source", "empty-target", "failed-source", "failed-target"} {
		invoke(t, queue, id, "CreateQueue", map[string]any{"QueueName": name})
	}
	enrich := "def lambda_handler(event, context):\n    return [{'messageId': item['messageId'], 'value': item['value'] * 2} for item in event]\n"
	invoke(t, function, id, "CreateFunction", map[string]any{
		"FunctionName": "enrich", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler", "Code": lambdaCode(enrich),
	})
	input := pipeInput("enrich", "source", "target")
	input["DesiredState"] = "STOPPED"
	input["Enrichment"] = "arn:aws:lambda:us-east-1:123456789012:function:enrich"
	input["EnrichmentParameters"] = map[string]any{"InputTemplate": `{"messageId":<$.messageId>,"value":<$.body.value>}`}
	input["TargetParameters"] = map[string]any{"InputTemplate": `{"id":<$.messageId>,"value":<$.value>}`}
	input["SourceParameters"] = map[string]any{"SqsQueueParameters": map[string]any{"BatchSize": 2}}
	invoke(t, p, id, "CreatePipe", input)
	for _, body := range []string{`{"value":1}`, `{"value":2}`} {
		invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "source", "MessageBody": body})
	}
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "enrich"})
	eventually(t, func() bool {
		return len(storedMessages(t, deps, id, "source")) == 0 && len(storedMessages(t, deps, id, "target")) == 2
	})
	values := map[float64]bool{}
	for _, message := range storedMessages(t, deps, id, "target") {
		var output map[string]any
		if err := json.Unmarshal([]byte(message["body"].(string)), &output); err != nil {
			t.Fatal(err)
		}
		values[output["value"].(float64)] = output["id"] != ""
	}
	if !values[2] || !values[4] {
		t.Fatalf("enriched values %#v", values)
	}

	empty := "def lambda_handler(event, context):\n    return []\n"
	invoke(t, function, id, "UpdateFunctionCode", map[string]any{"FunctionName": "enrich", "ZipFile": lambdaCode(empty)["ZipFile"]})
	filtered := pipeInput("empty", "empty-source", "empty-target")
	filtered["DesiredState"] = "STOPPED"
	filtered["Enrichment"] = input["Enrichment"]
	filtered["Target"] = "arn:aws:lambda:us-east-1:123456789012:function:must-not-run"
	invoke(t, p, id, "CreatePipe", filtered)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "empty-source", "MessageBody": "filtered"})
	invoke(t, p, id, "StartPipe", map[string]any{"Name": "empty"})
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "empty-source")) == 0 })

	failed := pipeInput("failed", "failed-source", "failed-target")
	failed["Enrichment"] = "arn:aws:lambda:us-east-1:123456789012:function:missing"
	failed["DesiredState"] = "STOPPED"
	invoke(t, p, id, "CreatePipe", failed)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "failed-source", "MessageBody": "retry"})
	received := invoke(t, queue, id, "ReceiveMessage", map[string]any{"QueueName": "failed-source"}).Output["Messages"].([]any)
	pipe := invoke(t, p, id, "DescribePipe", map[string]any{"Name": "failed"}).Output
	p.processBatch(context.Background(), id, pipe, queueARN(id, "failed-source"), "failed-source", received)
	if messages := storedMessages(t, deps, id, "failed-source"); len(messages) != 1 || messages[0]["receiveCount"] != float64(1) {
		t.Fatalf("failed enrichment acknowledged source: %#v", messages)
	}
	if messages := storedMessages(t, deps, id, "failed-target"); len(messages) != 0 {
		t.Fatalf("failed enrichment invoked target: %#v", messages)
	}
}

func lambdaCode(source string) map[string]any {
	return map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(source))}
}

func pipeInput(name, source, target string) map[string]any {
	id := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	return map[string]any{
		"Name": name, "Source": queueARN(id, source), "Target": queueARN(id, target),
		"RoleArn": "arn:aws:iam::123456789012:role/pipes",
	}
}

func queueARN(id spi.Identity, name string) string {
	return "arn:aws:sqs:" + id.Region + ":" + id.Account + ":" + name
}

func invoke(t *testing.T, handler spi.Handler, id spi.Identity, operation string, input map[string]any) *spi.Response {
	t.Helper()
	response, err := handler.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	if err != nil {
		t.Fatalf("%s: %v", operation, err)
	}
	return response
}

func assertFault(t *testing.T, handler spi.Handler, id spi.Identity, operation string, input map[string]any, code string) {
	t.Helper()
	_, err := handler.Invoke(context.Background(), &spi.Request{Identity: id, Operation: operation, Input: input})
	fault, _ := err.(*spi.Fault)
	if fault == nil || fault.Code != code {
		t.Fatalf("%s fault %v, want %s", operation, err, code)
	}
}

func storedMessages(t *testing.T, deps spi.Deps, id spi.Identity, queue string) []map[string]any {
	t.Helper()
	kvs, _, err := deps.Store.Scope(id.Account, id.Region).Collection("msgs:"+queue).List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	messages := make([]map[string]any, 0, len(kvs))
	for _, kv := range kvs {
		var message map[string]any
		if err := json.Unmarshal(kv.Value, &message); err != nil {
			t.Fatal(err)
		}
		messages = append(messages, message)
	}
	return messages
}

func storedStateExecutions(t *testing.T, deps spi.Deps, id spi.Identity) []map[string]any {
	t.Helper()
	kvs, _, err := deps.Store.Scope(id.Account, id.Region).Collection("ex").List(context.Background(), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	executions := make([]map[string]any, 0, len(kvs))
	for _, kv := range kvs {
		var execution map[string]any
		if err := json.Unmarshal(kv.Value, &execution); err != nil {
			t.Fatal(err)
		}
		executions = append(executions, execution)
	}
	return executions
}

// eventually waits for work the runtime does on its own goroutine after the
// test advances the controllable clock.
//
// The wait is on the wall clock for something driven by simulated time, which
// is the real fragility: the clock jump is synchronous and the work is not, so
// the test can only poll. The deadline is therefore generous rather than tight
// -- a passing run reaches its condition in milliseconds and pays nothing,
// while a loaded machine running under -race no longer fails a correct
// implementation for being slow.
//
// Making this deterministic needs the runtime to expose a point where due work
// is known to be flushed. That is a change to a pack scheduled for extraction,
// so a longer deadline is the proportionate fix -- not a solution to the
// underlying design, and not pretending to be one.
func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("the expected state did not arrive within 60s of advancing the clock")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
