package behavior

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestSQSQueueListing(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	call := func(action, payload string) (int, []byte) {
		t.Helper()
		request, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(payload))
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
		request.Header.Set("Content-Type", "application/x-amz-json-1.0")
		request.Header.Set("X-Amz-Target", "AmazonSQS."+action)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, body
	}
	t.Run("Given prefixed queues When listing pages Then only matching queues are returned", func(t *testing.T) {
		for _, name := range []string{"bdd-a-0", "bdd-a-1", "bdd-b-0"} {
			if status, body := call("CreateQueue", `{"QueueName":"`+name+`"}`); status != http.StatusOK {
				t.Fatalf("create %s: %d %s", name, status, body)
			}
		}
		status, body := call("ListQueues", `{"QueueNamePrefix":"bdd-a-","MaxResults":1}`)
		var first map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &first) != nil || len(first["QueueUrls"].([]any)) != 1 || first["NextToken"] == nil || bytes.Contains(body, []byte("bdd-b-0")) {
			t.Fatalf("first page %d %s", status, body)
		}
		token, _ := json.Marshal(first["NextToken"])
		status, body = call("ListQueues", `{"QueueNamePrefix":"bdd-a-","MaxResults":10,"NextToken":`+string(token)+`}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte("bdd-a-1")) || bytes.Contains(body, []byte("NextToken")) {
			t.Fatalf("second page %d %s", status, body)
		}
		if status, body := call("ListQueues", `{"QueueNamePrefix":"missing"}`); status != http.StatusOK || bytes.Contains(body, []byte("QueueUrls")) {
			t.Fatalf("empty list %d %s", status, body)
		}
	})
	t.Run("Given a queue When requesting metadata Then only selected attributes are returned", func(t *testing.T) {
		status, body := call("CreateQueue", `{"QueueName":"bdd-metadata"}`)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create %d %s", status, body)
		}
		queueURL, _ := json.Marshal(created["QueueUrl"])
		status, body = call("GetQueueAttributes", `{"QueueUrl":`+string(queueURL)+`,"AttributeNames":["QueueArn","CreatedTimestamp","VisibilityTimeout"]}`)
		var result map[string]map[string]string
		if status != http.StatusOK || json.Unmarshal(body, &result) != nil || len(result["Attributes"]) != 3 ||
			result["Attributes"]["QueueArn"] != "arn:aws:sqs:us-east-1:000000000000:bdd-metadata" ||
			result["Attributes"]["CreatedTimestamp"] == "" || result["Attributes"]["VisibilityTimeout"] != "30" {
			t.Fatalf("metadata %d %s", status, body)
		}
	})
	t.Run("Given a deleted queue When recreating immediately Then the deletion window is enforced", func(t *testing.T) {
		status, body := call("CreateQueue", `{"QueueName":"bdd-deleted"}`)
		var created map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &created) != nil {
			t.Fatalf("create %d %s", status, body)
		}
		queueURL, _ := json.Marshal(created["QueueUrl"])
		if status, body = call("DeleteQueue", `{"QueueUrl":`+string(queueURL)+`}`); status != http.StatusOK {
			t.Fatalf("delete %d %s", status, body)
		}
		status, body = call("CreateQueue", `{"QueueName":"bdd-deleted"}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("QueueDeletedRecently")) ||
			!bytes.Contains(body, []byte("You must wait 60 seconds after deleting a queue before you can create another with the same name.")) {
			t.Fatalf("recreate %d %s", status, body)
		}
	})
	t.Run("Given a sent message When receiving it Then its body and digest round trip", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-roundtrip"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-roundtrip","MessageBody":"message"}`)
		var sent map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &sent) != nil {
			t.Fatalf("send %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-roundtrip","VisibilityTimeout":0}`)
		var received map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &received) != nil || len(received["Messages"].([]any)) != 1 {
			t.Fatalf("receive %d %s", status, body)
		}
		message := received["Messages"].([]any)[0].(map[string]any)
		if message["Body"] != "message" || message["MD5OfBody"] != sent["MD5OfMessageBody"] || message["MD5OfBody"] != "78e731027d8fd50ed642340b7c9a63b3" {
			t.Fatalf("round trip sent=%#v received=%#v", sent, message)
		}
	})
	t.Run("Given an empty message body When sending Then the required parameter fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-empty-body"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-empty-body","MessageBody":""}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"MissingParameter"`)) ||
			!bytes.Contains(body, []byte("The request must contain the parameter MessageBody.")) {
			t.Fatalf("empty message %d %s", status, body)
		}
	})
	t.Run("Given an oversized receive batch When receiving Then the invalid parameter fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-max-messages"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-max-messages","MaxNumberOfMessages":11}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) ||
			!bytes.Contains(body, []byte("Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided.")) {
			t.Fatalf("max messages %d %s", status, body)
		}
	})
	t.Run("Given an empty queue When short or long polling Then messages are omitted", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-empty-receive"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, input := range []string{
			`{"QueueUrl":"http://queue/000000000000/bdd-empty-receive","MaxNumberOfMessages":1}`,
			`{"QueueUrl":"http://queue/000000000000/bdd-empty-receive","MaxNumberOfMessages":1,"WaitTimeSeconds":1}`,
		} {
			status, body := call("ReceiveMessage", input)
			if status != http.StatusOK || bytes.Contains(body, []byte(`"Messages"`)) {
				t.Fatalf("empty receive %d %s", status, body)
			}
		}
	})
	t.Run("Given receive wait times When polling Then bounds and short polls match AWS", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-wait-time"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for range 2 {
			if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-wait-time","MessageBody":"message"}`); status != http.StatusOK {
				t.Fatalf("send %d %s", status, body)
			}
		}
		for _, value := range []int{-1, 21} {
			status, body := call("ReceiveMessage", fmt.Sprintf(`{"QueueUrl":"http://queue/000000000000/bdd-wait-time","WaitTimeSeconds":%d}`, value))
			want := fmt.Sprintf("Value %d for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided.", value)
			var response map[string]any
			if status != http.StatusBadRequest || json.Unmarshal(body, &response) != nil || response["__type"] != "InvalidParameterValue" || response["message"] != want {
				t.Fatalf("wait=%d response %d %s", value, status, body)
			}
		}
		for _, input := range []string{
			`{"QueueUrl":"http://queue/000000000000/bdd-wait-time"}`,
			`{"QueueUrl":"http://queue/000000000000/bdd-wait-time","WaitTimeSeconds":0}`,
		} {
			status, body := call("ReceiveMessage", input)
			var response map[string]any
			if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 || response["Messages"].([]any)[0].(map[string]any)["Body"] != "message" {
				t.Fatalf("short poll %d %s", status, body)
			}
		}
	})
	t.Run("Given a received message When requesting all system attributes Then timestamps are numeric", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-timestamps"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-timestamps","MessageBody":"message"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-timestamps","MessageSystemAttributeNames":["All"]}`)
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 {
			t.Fatalf("receive %d %s", status, body)
		}
		attributes := response["Messages"].([]any)[0].(map[string]any)["Attributes"].(map[string]any)
		sent, sentErr := strconv.ParseInt(attributes["SentTimestamp"].(string), 10, 64)
		first, firstErr := strconv.ParseInt(attributes["ApproximateFirstReceiveTimestamp"].(string), 10, 64)
		if sentErr != nil || firstErr != nil || first < sent || first-sent > 1000 {
			t.Fatalf("timestamp attributes %#v", attributes)
		}
	})
	t.Run("Given two queues When sending to one Then the other remains empty", func(t *testing.T) {
		for _, name := range []string{"bdd-queue-0", "bdd-queue-1"} {
			if status, body := call("CreateQueue", fmt.Sprintf(`{"QueueName":%q}`, name)); status != http.StatusOK {
				t.Fatalf("create %s: %d %s", name, status, body)
			}
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-queue-0","MessageBody":"message"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		if status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-queue-1"}`); status != http.StatusOK || bytes.Contains(body, []byte(`"Messages"`)) {
			t.Fatalf("queue-1 %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-queue-0"}`)
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 || response["Messages"].([]any)[0].(map[string]any)["Body"] != "message" {
			t.Fatalf("queue-0 %d %s", status, body)
		}
	})
}
