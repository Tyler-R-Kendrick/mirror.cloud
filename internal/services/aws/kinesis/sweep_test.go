package kinesis

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// kinesisSound is a sound value for each member the model's operations take, of a stream named s.
const kinesisARN = "arn:aws:kinesis:us-east-1:000000000000:stream/s"

var kinesisSound = map[string]any{"StreamName": "s", "StreamARN": kinesisARN, "ResourceARN": kinesisARN, "ShardCount": 2, "TargetShardCount": 2, "ScalingType": "UNIFORM_SCALING",
	"RetentionPeriodHours": 48, "StreamModeDetails": map[string]any{"StreamMode": "PROVISIONED"}, "ShardToSplit": "shardId-000000000000", "NewStartingHashKey": "10",
	"ShardToMerge": "shardId-000000000000", "AdjacentShardToMerge": "shardId-000000000001", "PartitionKey": "k", "Data": "aGk=", "ShardId": "shardId-000000000000",
	"ShardIteratorType": "TRIM_HORIZON", "Tags": map[string]any{"a": "b"}, "TagKeys": []any{"a"}, "ConsumerName": "c", "Policy": "{}", "EncryptionType": "KMS", "KeyId": "alias/k",
	"ShardLevelMetrics": []any{"IncomingBytes"}, "MaxRecordSizeInKiB": 1024, "WarmThroughputMiBps": 1, "Records": []any{map[string]any{"PartitionKey": "k", "Data": "aGk="}}}

// No request makes the service fail. Every operation of the model is sent a request that is well formed
// but for one member, which is in turn null, an object, a list, a Boolean, a string, a number no integer
// holds, zero and a negative number; whatever the service answers, it is not a failure of its own (5xx).
// A stream exists throughout, on-demand in a second pass, so the checks that read a stream's record are
// reached too.
func TestKinesisNoMalformedMemberIsAFailureOfTheService(t *testing.T) {
	malformed := []string{`null`, `{}`, `[]`, `true`, `"x"`, `1e30`, `0`, `-1`, `1.5`, `{"StreamMode":null}`, `[null]`, `""`}
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	sent, failures := 0, []string{}
	for _, onDemand := range []bool{false, true} {
		try := kinesisTries(t)
		if status, out := try("CreateStream", `{"StreamName":"s","ShardCount":2}`); status != 200 {
			t.Fatal(status, out)
		}
		if onDemand {
			if status, out := try("UpdateStreamMode", `{"StreamARN":"`+kinesisARN+`","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`); status != 200 {
				t.Fatal(status, out)
			}
		}
		for _, op := range service.Operations {
			members := slices.Sorted(maps.Keys(service.Shapes[op.Input].Members))
			whole := map[string]any{}
			for _, member := range members {
				if v, known := kinesisSound[member]; known {
					whole[member] = v
				}
			}
			// And the spellings the bundle reads that an operation's model does not declare.
			for _, member := range slices.Compact(slices.Sorted(slices.Values(slices.Concat(members, []string{"StreamName", "StreamARN", "StreamArn", "ResourceARN", "ResourceArn", "ConsumerARN", "ConsumerArn", "ConsumerName"})))) {
				for _, bad := range malformed {
					request := maps.Clone(whole)
					request[member] = json.RawMessage(bad)
					body, _ := json.Marshal(request)
					status, out := try(op.Name, string(body))
					if sent++; status >= 500 {
						failures = append(failures, fmt.Sprintf("%s %s (on-demand: %v) answered %d %v", op.Name, body, onDemand, status, out["message"]))
					}
				}
			}
		}
	}
	if len(failures) > 0 || sent < 6000 {
		t.Errorf("%d of %d requests were answered as a failure of the service:\n%s", len(failures), sent, strings.Join(failures, "\n"))
	}
}

// A member that cannot be what the model declares is the caller's mistake, named. The bundle's expressions
// convert a stream's name and ARN with string() and a count with int(), which have no answer for null, an
// object or a list, so each of these was InternalFailure (500). A member that is null is another matter:
// it is absent, as awsJson1.1 has a server read it, so the member is missing where it is required and
// simply not there where it is not. And a count below one, which the reference
// rules out ("Valid Range: Minimum value of 1", CreateStream's ShardCount and UpdateShardCount's
// TargetShardCount), was taken as no count: CreateStream made the default, UpdateShardCount answered 200
// and changed nothing. The messages are a reading; the reference prints neither.
func TestKinesisAMemberThatCannotBeReadIsRefused(t *testing.T) {
	try := kinesisTries(t)
	answer := func(op, body string) string {
		status, out := try(op, body)
		return strings.TrimSuffix(fmt.Sprint(status, " ", out["__type"], ": ", out["message"]), " <nil>: <nil>")
	}
	summary := func(name string) string {
		_, out := try("DescribeStreamSummary", `{"StreamName":"`+name+`"}`)
		d, _ := out["StreamDescriptionSummary"].(map[string]any)
		return fmt.Sprint(d["OpenShardCount"])
	}
	answer("CreateStream", `{"StreamName":"s","ShardCount":2}`)
	const arn = `"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/s"`
	text := func(member string) string { return "400 ValidationException: " + member + " must be a string" }
	whole := func(member string) string { return "400 ValidationException: " + member + " must be an integer" }
	least := func(member, value string) string {
		return "400 ValidationException: 1 validation error detected: Value '" + value + "' at '" + member + "' failed to satisfy constraint: Member must have value greater than or equal to 1"
	}
	rows := []struct{ op, body, want string }{}
	for _, bad := range []string{`{}`, `[]`} {
		rows = append(rows,
			struct{ op, body, want string }{"UpdateStreamMode", `{"StreamARN":` + bad + `,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, text("StreamARN")},
			struct{ op, body, want string }{"CreateStream", `{"StreamName":` + bad + `,"ShardCount":1}`, text("StreamName")},
			struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":` + bad + `,"TargetShardCount":3,"ScalingType":"UNIFORM_SCALING"}`, text("StreamName")},
			struct{ op, body, want string }{"ListTagsForStream", `{"StreamName":` + bad + `}`, text("StreamName")})
	}
	for _, bad := range []string{`"x"`, `true`, `{}`, `[]`, `1e30`, `1.5`, `""`, `9223372036854775807`} {
		rows = append(rows,
			struct{ op, body, want string }{"CreateStream", `{"StreamName":"n","ShardCount":` + bad + `}`, whole("ShardCount")},
			struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING","TargetShardCount":` + bad + `}`, whole("TargetShardCount")})
	}
	for _, low := range []string{"0", "-1"} {
		rows = append(rows,
			struct{ op, body, want string }{"CreateStream", `{"StreamName":"n","ShardCount":` + low + `}`, least("shardCount", low)},
			struct{ op, body, want string }{"CreateStream", `{"StreamName":"n","ShardCount":` + low + `,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, least("shardCount", low)},
			struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING","TargetShardCount":` + low + `}`, least("targetShardCount", low)},
			// The count is judged before the stream is looked for, as a request's shape is.
			struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"ghost","ScalingType":"UNIFORM_SCALING","TargetShardCount":` + low + `}`, least("targetShardCount", low)})
	}
	rows = append(rows,
		// A member that is null is absent, so a required one is missing and an optional one is not there.
		struct{ op, body, want string }{"UpdateStreamMode", `{"StreamARN":null,"StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, "400 ValidationException: StreamARN is required"},
		struct{ op, body, want string }{"CreateStream", `{"StreamName":null,"ShardCount":1}`, "400 ValidationException: StreamName is required"},
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":null,"TargetShardCount":3,"ScalingType":"UNIFORM_SCALING"}`, "400 InvalidArgumentException: Either StreamName or StreamARN must be specified."},
		struct{ op, body, want string }{"ListTagsForStream", `{"StreamName":null}`, "400 InvalidArgumentException: Either StreamName or StreamARN must be specified."},
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING","TargetShardCount":null}`, "400 ValidationException: TargetShardCount is required"},
		struct{ op, body, want string }{"CreateStream", `{"StreamName":"nulled","ShardCount":null}`, "200"},
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","StreamARN":null,"ScalingType":"UNIFORM_SCALING","TargetShardCount":"9223372036854775807"}`, "400 LimitExceededException: This request would exceed the shard limit for the account."},
		// What is sound still is: a count of one, a whole number written with a fraction of zero, and the
		// members left out.
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING","TargetShardCount":1}`, "200"},
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING","TargetShardCount":2.0}`, "200"},
		struct{ op, body, want string }{"UpdateShardCount", `{"StreamName":"s","ScalingType":"UNIFORM_SCALING"}`, "400 ValidationException: TargetShardCount is required"},
		struct{ op, body, want string }{"UpdateStreamMode", `{` + arn + `,"StreamModeDetails":{"StreamMode":"PROVISIONED"}}`, "200"},
		struct{ op, body, want string }{"CreateStream", `{"StreamName":"one"}`, "200"})
	for _, row := range rows {
		if got := answer(row.op, row.body); got != row.want {
			t.Errorf("%s %s answered %s; want %s", row.op, row.body, got, row.want)
		}
	}
	if got := summary("s"); got != "2" {
		t.Errorf("after the refusals the stream has %s shards; want the 2 the last sound call gave it", got)
	}
	if status, _ := try("DescribeStreamSummary", `{"StreamName":"n"}`); status != 400 {
		t.Errorf("a refused CreateStream made a stream: DescribeStreamSummary answered %d", status)
	}
}
