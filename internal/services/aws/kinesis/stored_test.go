package kinesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// kinesisStored boots the service in process over a store that already holds a stream under each of
// names, as an earlier build's CreateStream stored whatever name it was given.
func kinesisStored(t *testing.T, names ...string) func(op string, in map[string]any) string {
	t.Helper()
	deps := spitest.Deps(t)
	p := bundled.Handler("aws.kinesis", deps)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	call := func(op string, in map[string]any) string {
		out, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.kinesis", Operation: op, Input: in, Identity: id})
		var f *spi.Fault
		if errors.As(err, &f) {
			return fmt.Sprint(f.HTTPStatus, " ", f.Code, ": ", f.Message)
		} else if err != nil {
			return "500 " + err.Error()
		}
		body, _ := json.Marshal(out.Output)
		return "200 " + string(body)
	}
	if got := call("CreateStream", map[string]any{"StreamName": "seed", "ShardCount": 2}); !strings.HasPrefix(got, "200") {
		t.Fatal(got)
	}
	streams := deps.Store.Scope(id.Account, id.Region).Collection("kinesis")
	raw, _, _ := streams.Get(context.Background(), "seed")
	for _, name := range names {
		record := spi.JSONObject(raw)
		record["StreamName"] = name
		if err := streams.Put(context.Background(), name, []byte(spi.JSONText(record))); err != nil {
			t.Fatal(err)
		}
	}
	return call
}

// A stream that is stored is reached by its exact name, whatever the name. CreateStream took any name
// before it held names to the reference's pattern, so a store may hold a stream named with whitespace, a
// slash, a space, 200 characters or letters outside ASCII. The rule that an address which names nothing
// is one not given trimmed the name before it looked, so a stream named " " could be listed and counted
// against the shard limit but not read, written, tagged or deleted. A name is now never judged or altered
// before the lookup: a blank one is "not given" only when it is empty, or when no stream has it. Every
// operation that takes a StreamName answers such a stream by its name, and those that take a StreamARN by
// an ARN whose last segment is the name.
func TestKinesisAStoredStreamIsReachedByItsExactName(t *testing.T) {
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	names := []string{" ", "\t ", "a b", "a/b", "s/", strings.Repeat("n", 200), "é"}
	asked := 0
	for _, name := range names {
		for _, op := range service.Operations {
			members := service.Shapes[op.Input].Members
			for _, by := range []string{"StreamName", "StreamARN"} {
				arn := "arn:aws:kinesis:us-east-1:000000000000:stream/" + name
				if _, takes := members[by]; !takes || op.Name == "CreateStream" || op.Name == "GetRecords" || strings.HasSuffix(op.Name, "StreamConsumer") ||
					by == "StreamARN" && strings.Contains(name, "/") { // an ARN's last segment is not that stream's name
					continue
				}
				call := kinesisStored(t, name)
				request := map[string]any{}
				_ = json.Unmarshal([]byte(kinesisRequest(t, op.Name, kinesisARN)), &request)
				request[by] = map[string]string{"StreamName": name, "StreamARN": arn}[by]
				if got := call(op.Name, request); !strings.HasPrefix(got, "200 ") {
					t.Errorf("%s of the stored stream %q by %s answered %s", op.Name, name, by, got)
				}
				asked++
			}
		}
		// Read, written, tagged and deleted, in one life.
		call := kinesisStored(t, name)
		if arn := "arn:aws:kinesis:us-east-1:000000000000:stream/" + name; !strings.Contains(name, "/") { // and its consumer, by either address
			for _, step := range []struct {
				op string
				in map[string]any
			}{
				{"RegisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "c"}},
				{"DescribeStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "c"}},
				{"DescribeStreamConsumer", map[string]any{"ConsumerARN": arn + "/consumer/c"}},
				{"DeregisterStreamConsumer", map[string]any{"StreamARN": arn, "ConsumerName": "c"}},
			} {
				if got := call(step.op, step.in); !strings.HasPrefix(got, "200 ") {
					t.Errorf("%s of the stored stream %q's consumer, by %v, answered %s", step.op, name, step.in, got)
				}
			}
		}
		in := func(more map[string]any) map[string]any {
			more["StreamName"] = name
			return more
		}
		if got := call("DescribeStreamSummary", in(map[string]any{})); !strings.Contains(got, `"OpenShardCount":2`) {
			t.Errorf("%q described: %s", name, got)
		}
		call("PutRecord", in(map[string]any{"PartitionKey": "k", "Data": "aGk="}))
		iterator := map[string]any{}
		_ = json.Unmarshal([]byte(strings.TrimPrefix(call("GetShardIterator", in(map[string]any{"ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"})), "200 ")), &iterator)
		if got := call("GetRecords", map[string]any{"ShardIterator": iterator["ShardIterator"]}); !strings.Contains(got, `"PartitionKey":"k"`) {
			t.Errorf("%q read: %s", name, got)
		}
		call("AddTagsToStream", in(map[string]any{"Tags": map[string]any{"a": "b"}}))
		if got := call("ListTagsForStream", in(map[string]any{})); !strings.Contains(got, `"a":"b"`) {
			t.Errorf("%q tagged: %s", name, got)
		}
		if got := call("UpdateShardCount", in(map[string]any{"TargetShardCount": 3, "ScalingType": "UNIFORM_SCALING"})); !strings.HasPrefix(got, "200 ") {
			t.Errorf("%q counted: %s", name, got)
		}
		if got := call("DeleteStream", in(map[string]any{})); got != "200 {}" {
			t.Errorf("%q deleted: %s", name, got)
		}
		if got := call("ListStreams", map[string]any{}); got != `200 {"HasMoreStreams":false,"StreamNames":["seed"]}` {
			t.Errorf("after %q was deleted the streams are %s", name, got)
		}
		// Gone, it is not found by its name; a blank name that no stream has is no address at all.
		want := "400 ResourceNotFoundException: Stream " + name + " under account 000000000000 not found."
		if strings.TrimSpace(name) == "" {
			want = "400 InvalidArgumentException: Either StreamName or StreamARN must be specified."
		}
		for _, op := range []string{"DescribeStreamSummary", "DeleteStream", "GetShardIterator"} {
			if got := call(op, in(map[string]any{"ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"})); got != want {
				t.Errorf("%s of %q once it is gone answered %s; want %s", op, name, got, want)
			}
		}
	}
	if asked < 200 {
		t.Errorf("%d requests were sent; want every operation, by name and by ARN, for each stored name", asked)
	}
}
