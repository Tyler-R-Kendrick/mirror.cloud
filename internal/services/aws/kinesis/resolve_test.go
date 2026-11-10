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

// kinesisWith boots the service with the stream s, its consumer c, a tag and a policy.
func kinesisWith(t *testing.T) func(op, body string) (int, map[string]any) {
	t.Helper()
	try := kinesisTries(t)
	for _, call := range [][2]string{{"CreateStream", `{"StreamName":"s","ShardCount":2}`}, {"RegisterStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"c"}`}, {"AddTagsToStream", `{"StreamName":"s","Tags":{"a":"b"}}`}, {"PutResourcePolicy", `{"ResourceARN":"` + kinesisARN + `","Policy":"{}"}`}} {
		if status, out := try(call[0], call[1]); status != 200 {
			t.Fatal(call[0], status, out)
		}
	}
	return try
}

// kinesisRequest is a sound request of op for the stream at arn, addressed by the members in by and no
// other of the members that address a stream or a consumer.
func kinesisRequest(t *testing.T, op, arn string, by ...string) string {
	t.Helper()
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	request := map[string]any{}
	for _, o := range service.Operations {
		if o.Name != op {
			continue
		}
		for member := range service.Shapes[o.Input].Members {
			v, known := kinesisSound[member]
			if addresses := slices.Contains([]string{"StreamName", "StreamARN", "ResourceARN", "ConsumerARN", "ConsumerName", "StreamId", "NextToken", "ExclusiveStartShardId"}, member); !known || addresses && !slices.Contains(by, member) {
				continue
			}
			switch member {
			case "StreamName":
				v = arn[strings.LastIndex(arn, "/")+1:]
			case "StreamARN", "ResourceARN":
				v = arn
			case "ConsumerARN":
				v = arn + "/consumer/c"
			}
			request[member] = v
		}
	}
	body, _ := json.Marshal(request)
	return string(body)
}

// A stream is addressed by its name or by its ARN: "When you invoke this API, you must use either the
// StreamARN or the StreamName parameter, or both" (API reference, on each operation that takes the two).
// DescribeStream, DescribeStreamSummary, DeleteStream and the record plane looked for StreamName alone,
// so a stream that exists, addressed by its ARN, was not found; and the not-found message of a stream
// addressed so had no name in it ("Stream  under account ... not found."). Every operation now answers a
// stream addressed by its ARN as it answers the same stream by name, names the stream it does not find
// from whichever was given, and refuses a request that gives neither. That refusal is
// InvalidArgumentException, "A specified parameter exceeds its restrictions, is not supported, or can't
// be used", which every one of these operations lists; its message is a reading.
func TestKinesisAStreamIsAddressedByItsNameOrItsARN(t *testing.T) {
	const ghost = "arn:aws:kinesis:us-east-1:000000000000:stream/ghost"
	const missing = "400 ResourceNotFoundException: Stream ghost under account 000000000000 not found."
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	checked := 0
	for _, op := range service.Operations {
		members := service.Shapes[op.Input].Members
		_, byName := members["StreamName"]
		if _, byARN := members["StreamARN"]; !byARN || op.Name == "GetRecords" || strings.HasSuffix(op.Name, "StreamConsumer") {
			continue // a consumer's are below; GetRecords is its iterator's
		}
		checked++
		answer := func(try func(op, body string) (int, map[string]any), body string) string {
			status, out := try(op.Name, body)
			if status < 300 {
				delete(out, "SequenceNumber")
				delete(out, "ShardIterator")
				b, _ := json.Marshal(out)
				return fmt.Sprint(status, " ", string(b))
			}
			return fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
		}
		byARNAnswer := answer(kinesisWith(t), kinesisRequest(t, op.Name, kinesisARN, "StreamARN"))
		if !strings.HasPrefix(byARNAnswer, "200 ") {
			t.Errorf("%s of a stream that exists, by its ARN, answered %s", op.Name, byARNAnswer)
		}
		try := kinesisWith(t)
		if got := answer(try, kinesisRequest(t, op.Name, ghost, "StreamARN")); got != missing {
			t.Errorf("%s of a stream that does not exist, by its ARN, answered %s", op.Name, got)
		}
		if byName {
			if got := answer(kinesisWith(t), kinesisRequest(t, op.Name, kinesisARN, "StreamName")); got != byARNAnswer {
				t.Errorf("%s by name answered %s; by ARN %s", op.Name, got, byARNAnswer)
			}
			if got := answer(try, kinesisRequest(t, op.Name, ghost, "StreamName")); got != missing {
				t.Errorf("%s of a stream that does not exist, by its name, answered %s", op.Name, got)
			}
			if got := answer(try, kinesisRequest(t, op.Name, ghost, "StreamName", "StreamARN")); got != missing {
				t.Errorf("%s of a stream that does not exist, by both, answered %s", op.Name, got)
			}
		}
		if got := answer(try, kinesisRequest(t, op.Name, ghost)); members["StreamARN"].Required && got != "400 ValidationException: StreamARN is required" ||
			!members["StreamARN"].Required && got != "400 InvalidArgumentException: Either StreamName or StreamARN must be specified." {
			t.Errorf("%s of no stream at all answered %s", op.Name, got)
		}
		// Nothing was made for the stream that does not exist.
		if status, out := try("ListStreams", `{}`); status != 200 || fmt.Sprint(out["StreamNames"]) != "[s]" {
			t.Errorf("after %s of a stream that does not exist the streams are %v", op.Name, out["StreamNames"])
		}
		for _, look := range [][2]string{{"DescribeStreamSummary", `{"StreamName":"ghost"}`}, {"ListTagsForStream", `{"StreamName":"ghost"}`}, {"GetShardIterator", `{"StreamName":"ghost","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`}} {
			if got := func() string {
				status, out := try(look[0], look[1])
				return fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
			}(); got != missing {
				t.Errorf("after %s of a stream that does not exist, %s of it answered %s", op.Name, look[0], got)
			}
		}
	}
	if checked < 20 {
		t.Errorf("%d operations take a StreamARN; want the service's twenty and more", checked)
	}
}

// What is addressed by a resource's ARN, or is a consumer, is found or is not: the tags and the policy
// of a stream that does not exist, a consumer of one, and a consumer that does not exist, answered 200 and
// most of them wrote a record. Each lists ResourceNotFoundException. A consumer is named by its ARN, or by
// its stream's ARN and its own name ("ConsumerName ... StreamARN", DescribeStreamConsumer and
// DeregisterStreamConsumer), and the message names it either way; a request that gives neither is
// InvalidArgumentException. The wording of a consumer's message is a reading.
func TestKinesisWhatAnARNNamesIsFoundOrIsNot(t *testing.T) {
	try := kinesisWith(t)
	answer := func(op, body string) string {
		status, out := try(op, body)
		if status < 300 {
			return "200"
		}
		return fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
	}
	const ghost = "arn:aws:kinesis:us-east-1:000000000000:stream/ghost"
	const noStream = "400 ResourceNotFoundException: Stream ghost under account 000000000000 not found."
	consumer := func(arn string) string {
		return "400 ResourceNotFoundException: Consumer " + arn + " under account 000000000000 not found."
	}
	const neither = "400 InvalidArgumentException: Either ConsumerARN, or StreamARN and ConsumerName, must be specified."
	for _, row := range []struct{ op, body, want string }{
		{"TagResource", `{"ResourceARN":"` + ghost + `","Tags":{"a":"b"}}`, noStream},
		{"UntagResource", `{"ResourceARN":"` + ghost + `","TagKeys":["a"]}`, noStream},
		{"ListTagsForResource", `{"ResourceARN":"` + ghost + `"}`, noStream},
		{"PutResourcePolicy", `{"ResourceARN":"` + ghost + `","Policy":"{}"}`, noStream},
		{"GetResourcePolicy", `{"ResourceARN":"` + ghost + `"}`, noStream},
		{"DeleteResourcePolicy", `{"ResourceARN":"` + ghost + `"}`, noStream},
		{"TagResource", `{"ResourceARN":"not-an-arn","Tags":{"a":"b"}}`, "400 ResourceNotFoundException: Stream not-an-arn under account 000000000000 not found."},
		{"TagResource", `{"Tags":{"a":"b"}}`, "400 ValidationException: ResourceARN is required"},
		{"TagResource", `{"ResourceARN":"` + kinesisARN + `/consumer/none","Tags":{"a":"b"}}`, consumer(kinesisARN + "/consumer/none")},
		{"TagResource", `{"ResourceARN":"` + ghost + `/consumer/c","Tags":{"a":"b"}}`, noStream},
		{"GetResourcePolicy", `{"ResourceARN":"` + kinesisARN + `/consumer/none"}`, consumer(kinesisARN + "/consumer/none")},
		{"UntagResource", `{"ResourceARN":"` + kinesisARN + `/consumer/none","TagKeys":["a"]}`, consumer(kinesisARN + "/consumer/none")},
		{"ListTagsForResource", `{"ResourceARN":"` + kinesisARN + `/consumer/none"}`, consumer(kinesisARN + "/consumer/none")},
		{"PutResourcePolicy", `{"ResourceARN":"` + kinesisARN + `/consumer/none","Policy":"{}"}`, consumer(kinesisARN + "/consumer/none")},
		{"DeleteResourcePolicy", `{"ResourceARN":"` + kinesisARN + `/consumer/none"}`, consumer(kinesisARN + "/consumer/none")},
		{"TagResource", `{"ResourceARN":"` + kinesisARN + `","Tags":{"a":"b"}}`, "200"},
		{"TagResource", `{"ResourceARN":"` + kinesisARN + `/consumer/c","Tags":{"a":"b"}}`, "200"},
		{"ListTagsForResource", `{"ResourceARN":"` + kinesisARN + `"}`, "200"},
		{"UntagResource", `{"ResourceARN":"` + kinesisARN + `","TagKeys":["a"]}`, "200"},
		{"PutResourcePolicy", `{"ResourceARN":"` + kinesisARN + `/consumer/c","Policy":"{}"}`, "200"},
		{"GetResourcePolicy", `{"ResourceARN":"` + kinesisARN + `"}`, "200"},
		{"DeleteResourcePolicy", `{"ResourceARN":"` + kinesisARN + `"}`, "200"},
		{"RegisterStreamConsumer", `{"StreamARN":"` + ghost + `","ConsumerName":"c"}`, noStream},
		{"RegisterStreamConsumer", `{"ConsumerName":"c"}`, "400 ValidationException: StreamARN is required"},
		{"ListStreamConsumers", `{"StreamARN":"` + ghost + `"}`, noStream},
		{"ListStreamConsumers", `{}`, "400 ValidationException: StreamARN is required"},
		{"ListStreamConsumers", `{"StreamARN":"` + kinesisARN + `"}`, "200"},
		{"DescribeStreamConsumer", `{}`, neither},
		{"DescribeStreamConsumer", `{"StreamARN":"` + kinesisARN + `"}`, neither},
		{"DescribeStreamConsumer", `{"ConsumerName":"c"}`, neither},
		{"DescribeStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"none"}`, consumer(kinesisARN + "/consumer/none")},
		{"DescribeStreamConsumer", `{"StreamARN":"` + ghost + `","ConsumerName":"c"}`, consumer(ghost + "/consumer/c")},
		{"DescribeStreamConsumer", `{"ConsumerARN":"` + kinesisARN + `/consumer/none"}`, consumer(kinesisARN + "/consumer/none")},
		{"DescribeStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"c"}`, "200"},
		{"DescribeStreamConsumer", `{"ConsumerARN":"` + kinesisARN + `/consumer/c"}`, "200"},
		{"SubscribeToShard", `{"ConsumerARN":"` + kinesisARN + `/consumer/none","ShardId":"shardId-000000000000","StartingPosition":{"Type":"LATEST"}}`, consumer(kinesisARN + "/consumer/none")},
		{"SubscribeToShard", `{"ConsumerARN":"` + kinesisARN + `/consumer/c","ShardId":"shardId-000000000000","StartingPosition":{"Type":"LATEST"}}`, "200"},
		{"DeregisterStreamConsumer", `{}`, neither},
		{"DeregisterStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"none"}`, consumer(kinesisARN + "/consumer/none")},
		{"DeregisterStreamConsumer", `{"ConsumerARN":"` + kinesisARN + `/consumer/none"}`, consumer(kinesisARN + "/consumer/none")},
		{"DeregisterStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"c"}`, "200"},
		{"DescribeStreamConsumer", `{"ConsumerARN":"` + kinesisARN + `/consumer/c"}`, consumer(kinesisARN + "/consumer/c")},
		{"DeregisterStreamConsumer", `{"ConsumerARN":"` + kinesisARN + `/consumer/c"}`, consumer(kinesisARN + "/consumer/c")},
	} {
		if got := answer(row.op, row.body); got != row.want {
			t.Errorf("%s %s answered %s; want %s", row.op, row.body, got, row.want)
		}
	}
	// A consumer described by its stream and its name is the one described by its ARN.
	try("RegisterStreamConsumer", `{"StreamARN":"`+kinesisARN+`","ConsumerName":"d"}`)
	_, byName := try("DescribeStreamConsumer", `{"StreamARN":"`+kinesisARN+`","ConsumerName":"d"}`)
	_, byARN := try("DescribeStreamConsumer", `{"ConsumerARN":"`+kinesisARN+`/consumer/d"}`)
	if a, b := fmt.Sprint(byName), fmt.Sprint(byARN); a != b || !strings.Contains(a, "ConsumerName:d") {
		t.Errorf("a consumer by its stream and name is %s; by its ARN %s", a, b)
	}
}

// The record plane finds a stream by its ARN and refuses one that is not there. PutRecords to a stream
// that does not exist answered 200 with FailedRecordCount 0 and an InternalFailure for each record; an
// iterator was handed out for any name; and records were read through the iterator of a stream since
// deleted, or made again. PutRecords, GetShardIterator and GetRecords each list ResourceNotFoundException.
func TestKinesisTheRecordPlaneKnowsItsStream(t *testing.T) {
	try := kinesisWith(t)
	call := func(op, body string) map[string]any {
		t.Helper()
		status, out := try(op, body)
		if status != 200 {
			t.Fatalf("%s %s answered %d %v", op, body, status, out)
		}
		return out
	}
	const missing = "400 ResourceNotFoundException: Stream ghost under account 000000000000 not found."
	for op, body := range map[string]string{
		"PutRecords":       `{"StreamName":"ghost","Records":[{"PartitionKey":"k","Data":"aGk="}]}`,
		"PutRecord":        `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/ghost","PartitionKey":"k","Data":"aGk="}`,
		"GetShardIterator": `{"StreamName":"ghost","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`,
	} {
		if status, out := try(op, body); fmt.Sprint(status, " ", out["__type"], ": ", out["message"]) != missing {
			t.Errorf("%s of a stream that does not exist answered %d %v", op, status, out)
		}
	}
	call("PutRecord", `{"StreamARN":"`+kinesisARN+`","PartitionKey":"k","Data":"b25l"}`)
	call("PutRecords", `{"StreamARN":"`+kinesisARN+`","Records":[{"PartitionKey":"k","Data":"dHdv"}]}`)
	iterator := fmt.Sprint(call("GetShardIterator", `{"StreamARN":"`+kinesisARN+`","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`)["ShardIterator"])
	read := call("GetRecords", `{"ShardIterator":"`+iterator+`"}`)
	if records, _ := read["Records"].([]any); len(records) != 2 {
		t.Errorf("two records put by the stream's ARN are read as %v", read["Records"])
	}
	call("DeleteStream", `{"StreamARN":"`+kinesisARN+`"}`)
	if status, out := try("GetRecords", `{"ShardIterator":"`+iterator+`"}`); status != 400 || out["__type"] != "ResourceNotFoundException" || out["message"] != "Stream s under account 000000000000 not found." {
		t.Errorf("GetRecords through the iterator of a stream since deleted answered %d %v", status, out)
	}
	// Deleted by its ARN, its records went with it: made again, the stream is empty.
	call("CreateStream", `{"StreamName":"s"}`)
	if records, _ := call("GetRecords", `{"ShardIterator":"`+iterator+`"}`)["Records"].([]any); len(records) != 0 {
		t.Errorf("a stream deleted by its ARN and made again still has %v", records)
	}
	if status, out := try("GetRecords", `{"ShardIterator":"bm90LWFuLWl0ZXJhdG9y"}`); status != 400 || out["__type"] != "InvalidArgumentException" {
		t.Errorf("GetRecords with what is no iterator answered %d %v", status, out)
	}
}

// A member that is null is absent: awsJson1.1 has a server ignore it (Smithy's compliance test
// AwsJson11ServersDontDeserializeNullStructureValues sends {"string": null} and expects no member), and
// the bundle's rule for a malformed member had refused it instead. Every operation, with each member
// given as null in turn, answers exactly as without that member: as before where it is optional, and
// with whatever a missing member is answered where it is required (the record plane requires none).
func TestKinesisANullMemberIsAbsent(t *testing.T) {
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	asked, differ := 0, []string{}
	for _, op := range service.Operations {
		members := service.Shapes[op.Input].Members
		whole := map[string]any{}
		for member := range members {
			if v, known := kinesisSound[member]; known && member != "StreamARN" && member != "ConsumerARN" && member != "ResourceARN" {
				whole[member] = v
			}
		}
		for _, member := range []string{"StreamARN", "ResourceARN"} { // by name where there is one, else by ARN
			if _, takes := members[member]; takes && (whole["StreamName"] == nil || members[member].Required) {
				whole[member] = kinesisSound[member]
			}
		}
		if _, takes := members["ConsumerARN"]; takes && members["ConsumerARN"].Required {
			whole["ConsumerARN"] = kinesisARN + "/consumer/c"
		}
		answer := func(request map[string]any) string {
			body, _ := json.Marshal(request)
			status, out := kinesisWith(t)(op.Name, string(body))
			text, _ := json.Marshal(out)
			return fmt.Sprint(status, " ", string(text))
		}
		for _, member := range slices.Sorted(maps.Keys(members)) {
			absent, null := maps.Clone(whole), maps.Clone(whole)
			delete(absent, member)
			null[member] = nil
			if a, n := answer(absent), answer(null); a != n {
				differ = append(differ, fmt.Sprintf("%s without %s answered %s; with it null %s", op.Name, member, a, n))
			}
			asked++
		}
	}
	if len(differ) > 0 || asked < 150 {
		t.Errorf("of %d members given as null, these were not read as absent:\n%s", asked, strings.Join(differ, "\n"))
	}
}
