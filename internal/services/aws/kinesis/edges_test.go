package kinesis

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
)

// An address that names nothing is answered as an address that was not given, and nothing is written for
// it. An empty string, whitespace, or an ARN that ends at "stream/" or in a slash resolves to no name;
// such a StreamARN on RegisterStreamConsumer made a consumer "arn:...:stream//consumer/z", which
// ListStreamConsumers then listed, and elsewhere the answer was a not-found with a blank where the name
// goes ("Stream  under account ... not found."). Now: "<Member> is required" where the model requires
// the member, as for one left out, and InvalidArgumentException where it is one of several. No answer
// names a blank. That an empty address is a missing one is a reading: the reference gives each of these
// members a minimum length of 1 and says no more.
func TestKinesisAnAddressThatNamesNothingIsNotGiven(t *testing.T) {
	blank := regexp.MustCompile(`(Stream|Consumer)\s+under account`)
	service := specboot.Bundle().ServiceByID("aws.kinesis")
	asked := 0
	for _, op := range service.Operations {
		members := service.Shapes[op.Input].Members
		for _, member := range []string{"StreamName", "StreamARN", "ResourceARN", "ConsumerARN"} {
			if _, takes := members[member]; !takes || op.Name == "CreateStream" {
				continue // CreateStream's name is judged by its pattern, below
			}
			for _, value := range []string{"", " ", "\t ", "arn:aws:kinesis:us-east-1:000000000000:stream/", kinesisARN + "/"} {
				try := kinesisWith(t)
				request := map[string]any{}
				_ = json.Unmarshal([]byte(kinesisRequest(t, op.Name, kinesisARN)), &request)
				request[member] = value
				body, _ := json.Marshal(request)
				status, out := try(op.Name, string(body))
				got := fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
				asked++
				nothing := strings.TrimSpace(value) == "" || member == "StreamARN"
				switch {
				case blank.MatchString(got):
					t.Errorf("%s %s answered %s: no name where the name goes", op.Name, body, got)
				case nothing && members[member].Required && got != "400 ValidationException: "+member+" is required":
					t.Errorf("%s %s answered %s; want its required %s missing", op.Name, body, got, member)
				case nothing && !members[member].Required && !strings.HasPrefix(got, "400 InvalidArgumentException: Either ") && !(op.Name == "GetRecords"):
					t.Errorf("%s %s answered %s; want the answer for no address at all", op.Name, body, got)
				}
				// Nothing was written: the stream and its consumer are the only ones.
				_, streams := try("ListStreams", `{}`)
				_, consumers := try("ListStreamConsumers", `{"StreamARN":"`+kinesisARN+`"}`)
				listed, _ := consumers["Consumers"].([]any)
				if fmt.Sprint(streams["StreamNames"]) != "[s]" || len(listed) != 1 {
					t.Errorf("after %s %s there are the streams %v and the consumers %v", op.Name, body, streams["StreamNames"], listed)
				}
			}
		}
	}
	if asked < 250 {
		t.Errorf("%d requests were sent; want every operation's address members, five values each", asked)
	}
}

// The three the review reproduced, and their neighbours, by name.
func TestKinesisAnEmptyAddressByName(t *testing.T) {
	try := kinesisWith(t)
	answer := func(op, body string) string {
		status, out := try(op, body)
		if status < 300 {
			return "200"
		}
		return fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
	}
	const either = "400 InvalidArgumentException: Either StreamName or StreamARN must be specified."
	for _, row := range []struct{ op, body, want string }{
		{"RegisterStreamConsumer", `{"StreamARN":"","ConsumerName":"z"}`, "400 ValidationException: StreamARN is required"},
		{"RegisterStreamConsumer", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/","ConsumerName":"z"}`, "400 ValidationException: StreamARN is required"},
		{"RegisterStreamConsumer", `{"StreamARN":"` + kinesisARN + `/","ConsumerName":"z"}`, "400 ValidationException: StreamARN is required"},
		{"ListStreamConsumers", `{"StreamARN":""}`, "400 ValidationException: StreamARN is required"},
		{"UpdateStreamMode", `{"StreamARN":"","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, "400 ValidationException: StreamARN is required"},
		{"UpdateStreamMode", `{"StreamARN":" ","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, "400 ValidationException: StreamARN is required"},
		{"TagResource", `{"ResourceARN":"","Tags":{"a":"b"}}`, "400 ValidationException: ResourceARN is required"},
		{"UntagResource", `{"ResourceARN":"","TagKeys":["a"]}`, "400 ValidationException: ResourceARN is required"},
		{"ListTagsForResource", `{"ResourceARN":""}`, "400 ValidationException: ResourceARN is required"},
		{"PutResourcePolicy", `{"ResourceARN":"","Policy":"{}"}`, "400 ValidationException: ResourceARN is required"},
		{"GetResourcePolicy", `{"ResourceARN":" "}`, "400 ValidationException: ResourceARN is required"},
		{"DeleteResourcePolicy", `{"ResourceARN":""}`, "400 ValidationException: ResourceARN is required"},
		{"SubscribeToShard", `{"ConsumerARN":"","ShardId":"shardId-000000000000","StartingPosition":{"Type":"LATEST"}}`, "400 ValidationException: ConsumerARN is required"},
		{"DescribeStream", `{"StreamName":" "}`, either},
		{"DescribeStream", `{"StreamARN":"` + kinesisARN + `/"}`, either},
		{"PutRecord", `{"StreamName":" ","PartitionKey":"k","Data":"aGk="}`, either},
		{"PutRecord", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/","PartitionKey":"k","Data":"aGk="}`, either},
		{"DescribeStreamConsumer", `{"ConsumerARN":" "}`, "400 InvalidArgumentException: Either ConsumerARN, or StreamARN and ConsumerName, must be specified."},
		// What names something is still found, or not found by its name.
		{"UpdateStreamMode", `{"StreamARN":"` + kinesisARN + `","StreamModeDetails":{"StreamMode":"ON_DEMAND"}}`, "200"},
		{"RegisterStreamConsumer", `{"StreamARN":"` + kinesisARN + `","ConsumerName":"z"}`, "200"},
		{"RegisterStreamConsumer", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/ghost","ConsumerName":"z"}`, "400 ResourceNotFoundException: Stream ghost under account 000000000000 not found."},
		{"ListStreamConsumers", `{"StreamARN":"` + kinesisARN + `"}`, "200"},
	} {
		if got := answer(row.op, row.body); got != row.want {
			t.Errorf("%s %s answered %s; want %s", row.op, row.body, got, row.want)
		}
	}
	_, consumers := try("ListStreamConsumers", `{"StreamARN":"`+kinesisARN+`"}`)
	names := []string{}
	for _, c := range consumers["Consumers"].([]any) {
		names = append(names, fmt.Sprint(c.(map[string]any)["ConsumerARN"]))
	}
	slices.Sort(names)
	if fmt.Sprint(names) != "["+kinesisARN+"/consumer/c "+kinesisARN+"/consumer/z]" {
		t.Errorf("the consumers are %v; want c and the z registered for the stream", names)
	}
}

// An iterator that is not one is refused as an iterator. GetRecords with one that decodes to no stream
// name answered "Either StreamName or StreamARN must be specified.", which is another operation's
// complaint. InvalidArgumentException, "A specified parameter exceeds its restrictions, is not supported,
// or can't be used", is the one GetRecords lists for it; ExpiredIteratorException is "The provided
// iterator exceeds the maximum age allowed", which a malformed one does not. The message is a reading.
func TestKinesisWhatIsNoIteratorIsRefusedAsOne(t *testing.T) {
	try := kinesisWith(t)
	for _, iterator := range []string{"|5", "|", "no-separator", " |0"} {
		for _, encoded := range []string{base64.StdEncoding.EncodeToString([]byte(iterator)), iterator} {
			status, out := try("GetRecords", `{"ShardIterator":"`+encoded+`"}`)
			if got := fmt.Sprint(status, " ", out["__type"], ": ", out["message"]); got != "400 InvalidArgumentException: Invalid ShardIterator." {
				t.Errorf("GetRecords with the iterator %q (%q) answered %s", encoded, iterator, got)
			}
		}
	}
	if status, out := try("GetRecords", `{"ShardIterator":"`+base64.StdEncoding.EncodeToString([]byte("ghost|0"))+`"}`); status != 400 || out["__type"] != "ResourceNotFoundException" {
		t.Errorf("an iterator of a stream that does not exist answered %d %v", status, out)
	}
	if status, out := try("GetRecords", `{"ShardIterator":"`+base64.StdEncoding.EncodeToString([]byte("s|0"))+`"}`); status != 200 {
		t.Errorf("an iterator of the stream answered %d %v", status, out)
	}
}

// A stream's name is of the reference's pattern, and is never cut. CreateStream's StreamName has "Length
// Constraints: Minimum length of 1. Maximum length of 128. Pattern: [a-zA-Z0-9_.-]+" (API reference), and
// ValidationException is the one for a request that "failed data validation"; any name was taken. The
// record plane cuts an ARN at its last slash to find the name, and cut a name the same way, so a stream
// named "a/b" was made and then not found as "b". A name is now taken whole, and such a name is refused
// when the stream is made. The messages are a reading, in the service's form for a constraint.
func TestKinesisAStreamNameIsOfThePatternAndWhole(t *testing.T) {
	try := kinesisWith(t)
	answer := func(op, body string) string {
		status, out := try(op, body)
		if status < 300 {
			return "200"
		}
		return fmt.Sprint(status, " ", out["__type"], ": ", out["message"])
	}
	pattern := func(name string) string {
		return "400 ValidationException: 1 validation error detected: Value '" + name + "' at 'streamName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+"
	}
	long := strings.Repeat("n", 129)
	for _, row := range []struct{ op, body, want string }{
		{"CreateStream", `{"StreamName":"a/b"}`, pattern("a/b")},
		{"CreateStream", `{"StreamName":"a b"}`, pattern("a b")},
		{"CreateStream", `{"StreamName":""}`, pattern("")},
		{"CreateStream", `{"StreamName":"é"}`, pattern("é")},
		{"CreateStream", `{"StreamName":"a\nb"}`, pattern("a\nb")},
		{"CreateStream", `{"StreamName":"` + long + `"}`, "400 ValidationException: 1 validation error detected: Value '" + long + "' at 'streamName' failed to satisfy constraint: Member must have length less than or equal to 128"},
		{"CreateStream", `{"StreamName":"` + long[1:] + `"}`, "200"},
		{"CreateStream", `{"StreamName":"A.b_c-9"}`, "200"},
		{"CreateStream", `{"StreamName":"b"}`, "200"},
		{"CreateStream", `{}`, "400 ValidationException: StreamName is required"},
		// A name is the stream's whole name: "a/b" is not the stream b, by any road.
		{"PutRecord", `{"StreamName":"a/b","PartitionKey":"k","Data":"aGk="}`, "400 ResourceNotFoundException: Stream a/b under account 000000000000 not found."},
		{"GetShardIterator", `{"StreamName":"a/b","ShardId":"shardId-000000000000","ShardIteratorType":"TRIM_HORIZON"}`, "400 ResourceNotFoundException: Stream a/b under account 000000000000 not found."},
		{"DescribeStream", `{"StreamName":"a/b"}`, "400 ResourceNotFoundException: Stream a/b under account 000000000000 not found."},
		{"PutRecord", `{"StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/b","PartitionKey":"k","Data":"aGk="}`, "200"},
		{"PutRecord", `{"StreamName":"b","StreamARN":"arn:aws:kinesis:us-east-1:000000000000:stream/ghost","PartitionKey":"k","Data":"aGk="}`, "200"},
	} {
		if got := answer(row.op, row.body); got != row.want {
			t.Errorf("%s %s answered %s; want %s", row.op, row.body, got, row.want)
		}
	}
}
