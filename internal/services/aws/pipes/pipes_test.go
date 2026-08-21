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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
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
	eventually(t, func() bool { return len(storedMessages(t, deps, id, "target")) == 2 })
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
	if len(storedMessages(t, deps, id, "source")) != 0 {
		t.Fatal("transformed source message retained")
	}
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
	invoke(t, p, id, "CreatePipe", failed)
	invoke(t, queue, id, "SendMessage", map[string]any{"QueueName": "failed-source", "MessageBody": "retry"})
	eventually(t, func() bool {
		messages := storedMessages(t, deps, id, "failed-source")
		return len(messages) == 1 && messages[0]["receiveCount"] == float64(1)
	})
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

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("condition not met")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
