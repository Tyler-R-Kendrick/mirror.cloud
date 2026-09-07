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
	"time"

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
	t.Run("Given a queue name with a slash When creating Then InvalidParameterValue is returned", func(t *testing.T) {
		status, body := call("CreateQueue", `{"QueueName":"queue/name/"}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) {
			t.Fatalf("invalid queue name %d %s", status, body)
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
	t.Run("Given a redrive policy When clearing it Then the attribute is omitted", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-redrive-policy"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":"42"}`
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-redrive-policy", "Attributes": map[string]string{"RedrivePolicy": policy, "Policy": policy}})
		if status, body := call("SetQueueAttributes", string(payload)); status != http.StatusOK {
			t.Fatalf("set %d %s", status, body)
		}
		payload, _ = json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-redrive-policy", "Attributes": map[string]string{"RedrivePolicy": "", "Policy": ""}})
		if status, body := call("SetQueueAttributes", string(payload)); status != http.StatusOK {
			t.Fatalf("clear %d %s", status, body)
		}
		status, body := call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-redrive-policy","AttributeNames":["All"]}`)
		if status != http.StatusOK || bytes.Contains(body, []byte("RedrivePolicy")) || bytes.Contains(body, []byte("Policy")) {
			t.Fatalf("redrive policy remained %d %s", status, body)
		}
	})
	t.Run("Given an invalid redrive policy When creating Then InvalidParameterValue is returned", func(t *testing.T) {
		payload, _ := json.Marshal(map[string]any{"QueueName": "bdd-invalid-redrive", "Attributes": map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"dummy","maxReceiveCount":"42"}`}})
		status, body := call("CreateQueue", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) {
			t.Fatalf("invalid redrive %d %s", status, body)
		}
	})
	t.Run("Given source queues with a dead-letter target When listing sources Then both sources are returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-dead-letter"}`); status != http.StatusOK {
			t.Fatalf("dead letter create %d %s", status, body)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:bdd-dead-letter","maxReceiveCount":"42"}`
		for _, name := range []string{"bdd-source-a", "bdd-source-b"} {
			payload, _ := json.Marshal(map[string]any{"QueueName": name, "Attributes": map[string]string{"RedrivePolicy": policy}})
			if status, body := call("CreateQueue", string(payload)); status != http.StatusOK {
				t.Fatalf("source create %s %d %s", name, status, body)
			}
		}
		status, body := call("ListDeadLetterSourceQueues", `{"QueueUrl":"http://queue/000000000000/bdd-dead-letter"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte("bdd-source-a")) || !bytes.Contains(body, []byte("bdd-source-b")) {
			t.Fatalf("sources %d %s", status, body)
		}
	})
	t.Run("Given a max receive count When a message is retried Then it moves to the dead-letter queue", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-max-receive-dlq"}`); status != http.StatusOK {
			t.Fatalf("dlq create %d %s", status, body)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:bdd-max-receive-dlq","maxReceiveCount":"1"}`
		payload, _ := json.Marshal(map[string]any{"QueueName": "bdd-max-receive-source", "Attributes": map[string]string{"RedrivePolicy": policy, "VisibilityTimeout": "0"}})
		if status, body := call("CreateQueue", string(payload)); status != http.StatusOK {
			t.Fatalf("source create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-max-receive-source","MessageBody":"poison"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		if status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-max-receive-source","VisibilityTimeout":0}`); status != http.StatusOK || !bytes.Contains(body, []byte("poison")) {
			t.Fatalf("first receive %d %s", status, body)
		}
		if status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-max-receive-source","VisibilityTimeout":0}`); status != http.StatusOK || bytes.Contains(body, []byte("poison")) {
			t.Fatalf("source retry %d %s", status, body)
		}
		if status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-max-receive-dlq"}`); status != http.StatusOK || !bytes.Contains(body, []byte("poison")) {
			t.Fatalf("dlq receive %d %s", status, body)
		}
	})
	t.Run("Given queue types When changing FifoQueue Then unsupported mutations are rejected", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-standard-attribute"}`); status != http.StatusOK {
			t.Fatalf("standard create %d %s", status, body)
		}
		for _, value := range []string{"true", "false"} {
			payload := `{"QueueUrl":"http://queue/000000000000/bdd-standard-attribute","Attributes":{"FifoQueue":"` + value + `"}}`
			if status, body := call("SetQueueAttributes", payload); status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidAttributeName")) {
				t.Fatalf("standard %s %d %s", value, status, body)
			}
		}
		if status, body := call("CreateQueue", `{"QueueName":"bdd-fifo-attribute.fifo","Attributes":{"FifoQueue":"true"}}`); status != http.StatusOK {
			t.Fatalf("fifo create %d %s", status, body)
		}
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-attribute.fifo","Attributes":{"FifoQueue":"true"}}`); status != http.StatusOK {
			t.Fatalf("fifo true %d %s", status, body)
		}
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-attribute.fifo","Attributes":{"FifoQueue":"false"}}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidAttributeName")) {
			t.Fatalf("fifo false %d %s", status, body)
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
	t.Run("Given FIFO naming mismatches When creating Then parameter faults are returned", func(t *testing.T) {
		for _, tc := range []struct {
			payload, message string
		}{
			{`{"QueueName":"bdd-fifo-missing.fifo"}`, "FifoQueue must be specified as true"},
			{`{"QueueName":"bdd-fifo-false.fifo","Attributes":{"FifoQueue":"false"}}`, "FifoQueue must be specified as true"},
			{`{"QueueName":"bdd-standard-fifo","Attributes":{"FifoQueue":"true"}}`, "Queue name must end in .fifo for FIFO queues"},
		} {
			if status, body := call("CreateQueue", tc.payload); status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte(tc.message)) {
				t.Fatalf("validation %s %d %s", tc.payload, status, body)
			}
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
	t.Run("Given disallowed message characters When sending Then invalid contents are rejected", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-invalid-contents"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-invalid-contents", "MessageBody": "Invalid-\x00"})
		status, body := call("SendMessage", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidMessageContents"`)) || !bytes.Contains(body, []byte("The message contains characters outside the allowed set.")) {
			t.Fatalf("invalid contents %d %s", status, body)
		}
	})
	t.Run("Given a retention period When it expires Then the message is removed", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-retention","Attributes":{"MessageRetentionPeriod":"1"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-retention","MessageBody":"expires"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		time.Sleep(1100 * time.Millisecond)
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-retention","WaitTimeSeconds":0}`)
		if status != http.StatusOK || bytes.Contains(body, []byte(`"Messages"`)) {
			t.Fatalf("retention %d %s", status, body)
		}
	})
	t.Run("Given a recently purged queue When purging again Then purge-in-progress is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-purge"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("PurgeQueue", `{"QueueUrl":"http://queue/000000000000/bdd-purge"}`); status != http.StatusOK {
			t.Fatalf("first purge %d %s", status, body)
		}
		status, body := call("PurgeQueue", `{"QueueUrl":"http://queue/000000000000/bdd-purge"}`)
		var response map[string]any
		if status != http.StatusForbidden || json.Unmarshal(body, &response) != nil || response["__type"] != "AWS.SimpleQueueService.PurgeQueueInProgress" || response["message"] != "Only one PurgeQueue operation on bdd-purge is allowed every 60 seconds." {
			t.Fatalf("successive purge %d %s", status, body)
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
	t.Run("Given a queue receive wait attribute When polling without a request wait Then the queue wait is used", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-queue-wait","Attributes":{"ReceiveMessageWaitTimeSeconds":"1"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		time.AfterFunc(100*time.Millisecond, func() {
			_, _ = call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-queue-wait","MessageBody":"queue-wait"}`)
		})
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-queue-wait"}`)
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 || response["Messages"].([]any)[0].(map[string]any)["Body"] != "queue-wait" {
			t.Fatalf("queue wait %d %s", status, body)
		}
	})
	t.Run("Given an illegal receipt handle When changing visibility Then the receipt-handle fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-invalid-receipt"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("ChangeMessageVisibility", `{"QueueUrl":"http://queue/000000000000/bdd-invalid-receipt","ReceiptHandle":"garbage","VisibilityTimeout":60}`)
		var response map[string]any
		if status != http.StatusBadRequest || json.Unmarshal(body, &response) != nil || response["__type"] != "ReceiptHandleIsInvalid" || response["message"] != `The input receipt handle "garbage" is not a valid receipt handle.` {
			t.Fatalf("invalid receipt %d %s", status, body)
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
	t.Run("Given message attributes When selecting names Then exact and wildcard filters are honored", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-attribute-filters","Attributes":{"VisibilityTimeout":"0"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-filters","MessageBody":"message","MessageAttributes":{"Help.Me":{"DataType":"String","StringValue":"Me"},"Hello":{"DataType":"String","StringValue":"There"},"General":{"DataType":"String","StringValue":"Kenobi"}}}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		for _, filter := range []string{"[\"Hello\"]", "[\"Hel.*\"]", "[\"*\"]", "[]"} {
			status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-filters","MessageAttributeNames":`+filter+`}`)
			var response map[string]any
			if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 {
				t.Fatalf("filter %s %d %s", filter, status, body)
			}
			attrs := response["Messages"].([]any)[0].(map[string]any)["MessageAttributes"].(map[string]any)
			want := 0
			if filter == `["Hello"]` {
				want = 1
			} else if filter == `["Hel.*"]` {
				want = 2
			} else if filter == `["*"]` {
				want = 3
			}
			if len(attrs) != want {
				t.Fatalf("filter %s attrs %#v", filter, attrs)
			}
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
	t.Run("Given encoded message content When round-tripping Then bytes are preserved", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-encoded"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		want := `"&quot;&quot;` + "\r"
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-encoded", "MessageBody": want})
		if status, body := call("SendMessage", string(payload)); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-encoded"}`)
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Messages"].([]any)) != 1 || response["Messages"].([]any)[0].(map[string]any)["Body"] != want {
			t.Fatalf("encoded response %d %s", status, body)
		}
	})
	t.Run("Given a message batch When receiving Then every entry is delivered", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-batch","Entries":[{"Id":"1","MessageBody":"message-0"},{"Id":"2","MessageBody":"message-1"}]}`)
		var sent map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &sent) != nil || len(sent["Successful"].([]any)) != 2 {
			t.Fatalf("batch %d %s", status, body)
		}
		if _, exists := sent["Failed"]; exists {
			t.Fatalf("successful batch included failures %s", body)
		}
		seen := map[string]bool{}
		for range 2 {
			status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-batch"}`)
			var received map[string]any
			if status != http.StatusOK || json.Unmarshal(body, &received) != nil || len(received["Messages"].([]any)) != 1 {
				t.Fatalf("receive %d %s", status, body)
			}
			seen[received["Messages"].([]any)[0].(map[string]any)["Body"].(string)] = true
		}
		if !seen["message-0"] || !seen["message-1"] {
			t.Fatalf("batch bodies %#v", seen)
		}
	})
	t.Run("Given string and binary message attributes When sending Then their digest and values are retained", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-attribute-digest"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-digest","MessageBody":"message","MessageAttributes":{"binary":{"DataType":"Binary","BinaryValue":"AAEC"},"string":{"DataType":"String","StringValue":"value"}}}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte("MD5OfMessageAttributes")) {
			t.Fatalf("send digest %d %s", status, body)
		}
		var sent map[string]any
		if json.Unmarshal(body, &sent) != nil {
			t.Fatalf("send json %s", body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-digest","MessageAttributeNames":["All"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte("MD5OfMessageAttributes")) || !bytes.Contains(body, []byte("value")) {
			t.Fatalf("receive digest %d %s", status, body)
		}
	})
	t.Run("Given system attributes When sending Then their digest is separate", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-system-attribute-digest"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		messageAttributes := `{"timestamp":{"DataType":"Number","StringValue":"1493147359900"}}`
		withoutPayload := `{"QueueUrl":"http://queue/000000000000/bdd-system-attribute-digest","MessageBody":"test","MessageAttributes":` + messageAttributes + `}`
		status, body := call("SendMessage", withoutPayload)
		var without map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &without) != nil || without["MD5OfMessageSystemAttributes"] != nil {
			t.Fatalf("without system digest %d %s", status, body)
		}
		withPayload := `{"QueueUrl":"http://queue/000000000000/bdd-system-attribute-digest","MessageBody":"test","MessageAttributes":` + messageAttributes + `,"MessageSystemAttributes":{"AWSTraceHeader":{"DataType":"String","StringValue":"Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"}}}`
		status, body = call("SendMessage", withPayload)
		var with map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &with) != nil || with["MD5OfMessageAttributes"] != without["MD5OfMessageAttributes"] || with["MD5OfMessageSystemAttributes"] != "5ae4d5d7636402d80f4eb6d213245a88" {
			t.Fatalf("with system digest %d %s", status, body)
		}
	})
	t.Run("Given an X-Amzn-Trace-Id header When sending Then AWSTraceHeader is retained", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-trace-header"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		trace := "Root=1-3152b799-8954dae64eda91bc9a23a7e8;Parent=7fa8c0f79203be72;Sampled=1"
		request, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"QueueUrl":"http://queue/000000000000/bdd-trace-header","MessageBody":"test"}`))
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
		request.Header.Set("Content-Type", "application/x-amz-json-1.0")
		request.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		request.Header.Set("X-Amzn-Trace-Id", trace)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("send with trace %d", response.StatusCode)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-trace-header","AttributeNames":["AWSTraceHeader"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(trace)) {
			t.Fatalf("trace receive %d %s", status, body)
		}
	})
	t.Run("Given an empty or reserved message attribute When sending Then validation fails", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-attribute-validation"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, attributes := range []string{`{"ErrorDetails":{"DataType":"String","StringValue":""}}`, `{"aWs.Invalid":{"DataType":"String","StringValue":"value"}}`} {
			status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-validation","MessageBody":"test","MessageAttributes":`+attributes+`}`)
			if status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidParameterValue")) {
				t.Fatalf("validation %d %s", status, body)
			}
		}
	})
	t.Run("Given a batch and a single send When receiving three Then all bodies are present", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-mixed"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-batch-mixed","Entries":[{"Id":"1","MessageBody":"message-0"},{"Id":"2","MessageBody":"message-1"}]}`); status != http.StatusOK {
			t.Fatalf("batch %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-batch-mixed","MessageBody":"message-2"}`); status != http.StatusOK {
			t.Fatalf("single %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-batch-mixed","MaxNumberOfMessages":3}`)
		var received map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &received) != nil || len(received["Messages"].([]any)) != 3 {
			t.Fatalf("receive %d %s", status, body)
		}
		seen := map[string]bool{}
		for _, raw := range received["Messages"].([]any) {
			seen[raw.(map[string]any)["Body"].(string)] = true
		}
		if !seen["message-0"] || !seen["message-1"] || !seen["message-2"] {
			t.Fatalf("mixed batch bodies %#v", seen)
		}
	})
	t.Run("Given a published batch When receiving and deleting Then the queue is empty", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-publish-get-delete"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-publish-get-delete","Entries":[{"Id":"1","MessageBody":"body-0"},{"Id":"2","MessageBody":"body-1"},{"Id":"3","MessageBody":"body-2"}]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Successful"`)) {
			t.Fatalf("send %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-publish-get-delete","MaxNumberOfMessages":10}`)
		var received map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &received) != nil || len(received["Messages"].([]any)) != 3 {
			t.Fatalf("receive %d %s", status, body)
		}
		deleteEntries := make([]map[string]any, 3)
		for i, raw := range received["Messages"].([]any) {
			message := raw.(map[string]any)
			deleteEntries[i] = map[string]any{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-publish-get-delete", "Entries": deleteEntries})
		status, body = call("DeleteMessageBatch", string(payload))
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Successful"`)) {
			t.Fatalf("delete %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-publish-get-delete","MaxNumberOfMessages":10}`)
		if status != http.StatusOK || bytes.Contains(body, []byte(`"Messages"`)) {
			t.Fatalf("remaining %d %s", status, body)
		}
	})
	t.Run("Given an empty message batch When sending Then EmptyBatchRequest is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-empty-batch"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-empty-batch","Entries":[]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"AWS.SimpleQueueService.EmptyBatchRequest"`)) {
			t.Fatalf("empty batch %d %s", status, body)
		}
	})
	t.Run("Given an oversized message When sending Then InvalidParameterValue is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-oversized"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-oversized", "MessageBody": strings.Repeat("a", (1<<20)-7), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}})
		status, body := call("SendMessage", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("Message must be shorter than 1048576 bytes")) {
			t.Fatalf("oversized %d %s", status, body)
		}
	})
	t.Run("Given a reduced maximum message size When sending Then the new limit applies", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-maximum","Attributes":{"MaximumMessageSize":"1024"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-maximum", "MessageBody": strings.Repeat("a", 1017), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}})
		status, body := call("SendMessage", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("Message must be shorter than 1024 bytes")) {
			t.Fatalf("reduced maximum %d %s", status, body)
		}
	})
	t.Run("Given an oversized batch When sending Then BatchRequestTooLong is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-size"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-batch-size", "Entries": []any{
			map[string]any{"Id": "1", "MessageBody": strings.Repeat("a", (1<<20)-8), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}},
			map[string]any{"Id": "2", "MessageBody": "a"},
		}})
		status, body := call("SendMessageBatch", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("BatchRequestTooLong")) || !bytes.Contains(body, []byte("1048577")) {
			t.Fatalf("oversized batch %d %s", status, body)
		}
	})
	t.Run("Given a reduced queue limit When sending a batch Then the batch limit remains one MiB", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-maximum","Attributes":{"MaximumMessageSize":"2048"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-batch-maximum", "Entries": []any{
			map[string]any{"Id": "1", "MessageBody": strings.Repeat("a", 2040), "MessageAttributes": map[string]any{"k": map[string]any{"DataType": "String", "StringValue": "x"}}},
			map[string]any{"Id": "2", "MessageBody": "a"},
		}})
		status, body := call("SendMessageBatch", string(payload))
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Successful"].([]any)) != 2 {
			t.Fatalf("updated batch %d %s", status, body)
		}
	})
	t.Run("Given a reduced per-message limit When sending a batch Then only the oversized entry fails", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-entry-maximum","Attributes":{"MaximumMessageSize":"1024"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-batch-entry-maximum", "Entries": []any{
			map[string]any{"Id": "valid", "MessageBody": strings.Repeat("a", 1024)},
			map[string]any{"Id": "oversized", "MessageBody": strings.Repeat("a", 1025)},
		}})
		status, body := call("SendMessageBatch", string(payload))
		var response map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response["Successful"].([]any)) != 1 || len(response["Failed"].([]any)) != 1 || !bytes.Contains(body, []byte("InvalidParameterValue")) {
			t.Fatalf("per-entry maximum %d %s", status, body)
		}
	})
	t.Run("Given a standard queue When sending an invalid message group Then the request is rejected", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-standard-group"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, group := range []string{"", strings.Repeat("a", 129), "group 123"} {
			payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-standard-group", "MessageBody": "message", "MessageGroupId": group})
			status, body := call("SendMessage", string(payload))
			if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("MessageGroupId can only include alphanumeric and punctuation characters")) {
				t.Fatalf("group %q response %d %s", group, status, body)
			}
		}
	})
	t.Run("Given queue tags When tagging and untagging Then the final empty set is omitted", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-tagged"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("TagQueue", `{"QueueUrl":"http://queue/000000000000/bdd-tagged","Tags":{"tag1":"value1","tag2":"value2","tag3":""}}`); status != http.StatusOK {
			t.Fatalf("tag %d %s", status, body)
		}
		status, body := call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-tagged"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"tag1":"value1"`)) || !bytes.Contains(body, []byte(`"tag3":""`)) {
			t.Fatalf("list %d %s", status, body)
		}
		if status, body = call("UntagQueue", `{"QueueUrl":"http://queue/000000000000/bdd-tagged","TagKeys":["tag1","tag3"]}`); status != http.StatusOK {
			t.Fatalf("untag partial %d %s", status, body)
		}
		status, body = call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-tagged"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"tag2":"value2"`)) || bytes.Contains(body, []byte(`"tag1"`)) {
			t.Fatalf("partial list %d %s", status, body)
		}
		if status, body = call("UntagQueue", `{"QueueUrl":"http://queue/000000000000/bdd-tagged","TagKeys":["tag2"]}`); status != http.StatusOK {
			t.Fatalf("untag final %d %s", status, body)
		}
		status, body = call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-tagged"}`)
		if status != http.StatusOK || bytes.Contains(body, []byte(`"Tags"`)) {
			t.Fatalf("empty list %d %s", status, body)
		}
	})
	t.Run("Given existing queue tags When tagging again Then untouched tags remain", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-tag-overwrite"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, payload := range []string{
			`{"QueueUrl":"http://queue/000000000000/bdd-tag-overwrite","Tags":{"tag1":"value1","tag2":"value2"}}`,
			`{"QueueUrl":"http://queue/000000000000/bdd-tag-overwrite","Tags":{"tag1":"VALUE1","tag3":"value3"}}`,
		} {
			if status, body := call("TagQueue", payload); status != http.StatusOK {
				t.Fatalf("tag %d %s", status, body)
			}
		}
		status, body := call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-tag-overwrite"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"tag1":"VALUE1"`)) || !bytes.Contains(body, []byte(`"tag2":"value2"`)) || !bytes.Contains(body, []byte(`"tag3":"value3"`)) {
			t.Fatalf("overwrite %d %s", status, body)
		}
	})
	t.Run("Given tags on CreateQueue When listing tags Then they are available immediately", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-create-tags","Tags":{"tag1":"value1","tag2":"value2"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-create-tags"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"tag1":"value1"`)) || !bytes.Contains(body, []byte(`"tag2":"value2"`)) {
			t.Fatalf("created tags %d %s", status, body)
		}
	})
	t.Run("Given tag keys differing only by case When listing Then both are retained", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-tag-case"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("TagQueue", `{"QueueUrl":"http://queue/000000000000/bdd-tag-case","Tags":{"MyTag":"value1","mytag":"value2"}}`); status != http.StatusOK {
			t.Fatalf("tag %d %s", status, body)
		}
		status, body := call("ListQueueTags", `{"QueueUrl":"http://queue/000000000000/bdd-tag-case"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"MyTag":"value1"`)) || !bytes.Contains(body, []byte(`"mytag":"value2"`)) {
			t.Fatalf("case-sensitive tags %d %s", status, body)
		}
	})
	t.Run("Given an existing queue When creating it again Then the URL is stable and conflicts are rejected", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-idempotent","Attributes":{"VisibilityTimeout":"69"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("CreateQueue", `{"QueueName":"bdd-idempotent"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte("bdd-idempotent")) {
			t.Fatalf("idempotent %d %s", status, body)
		}
		status, body = call("CreateQueue", `{"QueueName":"bdd-idempotent","Attributes":{"VisibilityTimeout":"70"}}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("QueueAlreadyExists")) || !bytes.Contains(body, []byte("VisibilityTimeout")) {
			t.Fatalf("conflict %d %s", status, body)
		}
	})
	t.Run("Given a batch entry with punctuation When sending Then the invalid batch id fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-invalid-batch-id"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-invalid-batch-id","Entries":[{"Id":"message:invalid","MessageBody":"message"}]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBatchEntryId")) || !bytes.Contains(body, []byte("can only contain alphanumeric characters, hyphens and underscores")) {
			t.Fatalf("invalid batch id %d %s", status, body)
		}
	})
	t.Run("Given a FIFO batch with a missing deduplication id Then the batch fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-missing-dedup.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-batch-missing-dedup.fifo","Entries":[{"Id":"message-1","MessageBody":"message-1","MessageGroupId":"test-group","MessageDeduplicationId":"dedup-1"},{"Id":"message-2","MessageBody":"message-2","MessageGroupId":"test-group"}]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly")) {
			t.Fatalf("missing deduplication id %d %s", status, body)
		}
	})
	t.Run("Given a FIFO batch with a missing message group id Then the batch fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-missing-group.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-batch-missing-group.fifo","Entries":[{"Id":"message-1","MessageBody":"message-1","MessageGroupId":"test-group","MessageDeduplicationId":"dedup-1"},{"Id":"message-2","MessageBody":"message-2","MessageDeduplicationId":"dedup-2"}]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"MissingParameter"`)) || !bytes.Contains(body, []byte("MessageGroupId")) {
			t.Fatalf("missing message group id %d %s", status, body)
		}
	})
	t.Run("Given more than ten batch entries When sending Then the entry-count fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-too-many-batch"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		entries := make([]map[string]any, 20)
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "MessageBody": "message"}
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-too-many-batch", "Entries": entries})
		status, body := call("SendMessageBatch", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("TooManyEntriesInBatchRequest")) || !bytes.Contains(body, []byte("Maximum number of entries per request are 10. You have sent 20.")) {
			t.Fatalf("too many entries %d %s", status, body)
		}
	})
	t.Run("Given a delete batch entry with punctuation When deleting Then the invalid batch id fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-delete-invalid-batch-id"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("DeleteMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-delete-invalid-batch-id","Entries":[{"Id":"message:invalid","ReceiptHandle":"handle"}]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBatchEntryId")) || !bytes.Contains(body, []byte("can only contain alphanumeric characters, hyphens and underscores")) {
			t.Fatalf("invalid delete batch id %d %s", status, body)
		}
	})
	t.Run("Given an empty delete batch When deleting Then the empty batch fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-delete-empty-batch"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("DeleteMessageBatch", `{"QueueUrl":"http://queue/000000000000/bdd-delete-empty-batch","Entries":[]}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("EmptyBatchRequest")) || !bytes.Contains(body, []byte("There should be at least one DeleteMessageBatchRequestEntry")) {
			t.Fatalf("empty delete batch %d %s", status, body)
		}
	})
	t.Run("Given more than ten delete batch entries Then the entry-count fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-delete-too-many-batch"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		entries := make([]map[string]any, 20)
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "ReceiptHandle": "handle"}
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-delete-too-many-batch", "Entries": entries})
		status, body := call("DeleteMessageBatch", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("TooManyEntriesInBatchRequest")) || !bytes.Contains(body, []byte("Maximum number of entries per request are 10. You have sent 20.")) {
			t.Fatalf("delete too many entries %d %s", status, body)
		}
	})
	t.Run("Given one invalid body in a send batch Then only that entry fails", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-batch-invalid-contents"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		entries := make([]map[string]any, 10)
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("%d", i), "MessageBody": fmt.Sprintf("%d", i)}
		}
		entries[9]["MessageBody"] = "\x01"
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-batch-invalid-contents", "Entries": entries})
		status, body := call("SendMessageBatch", string(payload))
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Successful"`)) || !bytes.Contains(body, []byte(`"Failed"`)) || !bytes.Contains(body, []byte("InvalidMessageContents")) {
			t.Fatalf("partial failure %d %s", status, body)
		}
	})
	t.Run("Given more than ten visibility entries Then the entry-count fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-visibility-too-many"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		entries := make([]map[string]any, 20)
		for i := range entries {
			entries[i] = map[string]any{"Id": fmt.Sprintf("message-%d", i), "ReceiptHandle": "handle", "VisibilityTimeout": 123}
		}
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-visibility-too-many", "Entries": entries})
		status, body := call("ChangeMessageVisibilityBatch", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("TooManyEntriesInBatchRequest")) || !bytes.Contains(body, []byte("Maximum number of entries per request are 10. You have sent 20.")) {
			t.Fatalf("visibility too many entries %d %s", status, body)
		}
	})
	t.Run("Given a FIFO queue When sending invalid deduplication ids Then the validation fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-dedup-invalid.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		validPayload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-dedup-invalid.fifo", "MessageBody": "valid", "MessageGroupId": "group-1", "MessageDeduplicationId": strings.Repeat("a", 128)})
		if status, body := call("SendMessage", string(validPayload)); status != http.StatusOK {
			t.Fatalf("valid deduplication id %d %s", status, body)
		}
		for _, value := range []string{"", strings.Repeat("a", 129), "group 123"} {
			payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-dedup-invalid.fifo", "MessageBody": "message", "MessageGroupId": "group-1", "MessageDeduplicationId": value})
			status, body := call("SendMessage", string(payload))
			if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("MessageDeduplicationId can only include alphanumeric and punctuation characters")) {
				t.Fatalf("deduplication id %q %d %s", value, status, body)
			}
		}
	})
	t.Run("Given a FIFO queue delay When sending with zero delay Then the queue delay is applied", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-delay-zero.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","DelaySeconds":"2"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-delay-zero.fifo","MessageBody":"message","MessageGroupId":"group-1","DelaySeconds":0}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-delay-zero.fifo","WaitTimeSeconds":0}`)
		if status != http.StatusOK || bytes.Contains(body, []byte(`"Messages"`)) {
			t.Fatalf("early receive %d %s", status, body)
		}
		time.Sleep(2500 * time.Millisecond)
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-delay-zero.fifo","WaitTimeSeconds":0}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Body":"message"`)) {
			t.Fatalf("delayed receive %d %s", status, body)
		}
	})
	t.Run("Given a FIFO queue When sending with a per-message delay Then the request is rejected", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-invalid-delay.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-invalid-delay.fifo","MessageBody":"message","MessageGroupId":"group-1","DelaySeconds":2}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("not valid for this queue type")) {
			t.Fatalf("FIFO delay %d %s", status, body)
		}
	})
	t.Run("Given a FIFO queue When sending messages Then sequence numbers increase", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-sequence.fifo","Attributes":{"FifoQueue":"true"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		previous := 0
		for index := 1; index <= 3; index++ {
			payload := fmt.Sprintf(`{"QueueUrl":"http://queue/000000000000/bdd-sequence.fifo","MessageBody":"message-%d","MessageGroupId":"group","MessageDeduplicationId":"dedup-%d"}`, index, index)
			status, body := call("SendMessage", payload)
			var sent map[string]any
			if status != http.StatusOK || json.Unmarshal(body, &sent) != nil {
				t.Fatalf("send %d %s", status, body)
			}
			sequence, err := strconv.Atoi(fmt.Sprint(sent["SequenceNumber"]))
			if err != nil || sequence <= previous {
				t.Fatalf("sequence %#v after %d", sent, previous)
			}
			previous = sequence
		}
		if status, body := call("CreateQueue", `{"QueueName":"bdd-sequence-standard"}`); status != http.StatusOK {
			t.Fatalf("standard create %d %s", status, body)
		}
		status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-sequence-standard","MessageBody":"message"}`)
		if status != http.StatusOK || bytes.Contains(body, []byte("SequenceNumber")) {
			t.Fatalf("standard sequence %d %s", status, body)
		}
	})
	t.Run("Given message-group deduplication scope When groups reuse an id Then both messages arrive", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-dedup-scope.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false","DeduplicationScope":"messageGroup","FifoThroughputLimit":"perMessageGroupId"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for index, group := range []string{"group-1", "group-2"} {
			payload := fmt.Sprintf(`{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope.fifo","MessageBody":"message-%d","MessageGroupId":"%s","MessageDeduplicationId":"same-dedup"}`, index+1, group)
			if status, body := call("SendMessage", payload); status != http.StatusOK {
				t.Fatalf("send %d %s", status, body)
			}
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope.fifo","MaxNumberOfMessages":10}`)
		if status != http.StatusOK || bytes.Count(body, []byte(`"MessageId"`)) != 2 {
			t.Fatalf("dedup scope %d %s", status, body)
		}
	})
	t.Run("Given a queue deduplication scope When the scope is updated Then existing semantics remain stable", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-dedup-scope-update.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"false","DeduplicationScope":"queue"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope-update.fifo","MessageBody":"first","MessageGroupId":"group-1","MessageDeduplicationId":"same-dedup"}`); status != http.StatusOK {
			t.Fatalf("first %d %s", status, body)
		}
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope-update.fifo","Attributes":{"DeduplicationScope":"messageGroup","FifoThroughputLimit":"perMessageGroupId"}}`); status != http.StatusOK {
			t.Fatalf("update %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope-update.fifo","MessageBody":"second","MessageGroupId":"group-2","MessageDeduplicationId":"same-dedup"}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"MessageId"`)) {
			t.Fatalf("duplicate %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-scope-update.fifo","MaxNumberOfMessages":10}`)
		if status != http.StatusOK || bytes.Count(body, []byte(`"MessageId"`)) != 1 {
			t.Fatalf("updated scope receive %d %s", status, body)
		}
	})
	t.Run("Given a FIFO message When receiving all attributes twice Then receive metadata mutates", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-fifo-attrs.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"0"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-attrs.fifo","MessageBody":"message","MessageGroupId":"group-1","MessageDeduplicationId":"dedup-1","MessageAttributes":{"kind":{"DataType":"String","StringValue":"fifo"}}}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		for want := 1; want <= 2; want++ {
			status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-attrs.fifo","AttributeNames":["All"],"MessageAttributeNames":["All"],"WaitTimeSeconds":0}`)
			if status != http.StatusOK || !bytes.Contains(body, []byte(`"StringValue":"fifo"`)) || !bytes.Contains(body, []byte(fmt.Sprintf(`"ApproximateReceiveCount":"%d"`, want))) {
				t.Fatalf("receive %d %d %s", want, status, body)
			}
		}
	})
	t.Run("Given FIFO messages When receiving a batch Then approximate count excludes in-flight messages", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-fifo-count.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, message := range []string{"g1-m1", "g1-m2", "g1-m3", "g2-m1", "g3-m1"} {
			payload := `{"QueueUrl":"http://queue/000000000000/bdd-fifo-count.fifo","MessageBody":"` + message + `","MessageGroupId":"` + message[:2] + `"}`
			if status, body := call("SendMessage", payload); status != http.StatusOK {
				t.Fatalf("send %s %d %s", message, status, body)
			}
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-count.fifo","MaxNumberOfMessages":4,"WaitTimeSeconds":0}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Messages"`)) {
			t.Fatalf("receive %d %s", status, body)
		}
		status, body = call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-fifo-count.fifo","AttributeNames":["ApproximateNumberOfMessages"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessages":"1"`)) {
			t.Fatalf("count %d %s", status, body)
		}
	})
	t.Run("Given FIFO deduplication is enabled When updating the strategy Then the attribute changes", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-dedup-strategy.fifo","Attributes":{"FifoQueue":"true","SqsManagedSseEnabled":"true","ContentBasedDeduplication":"true"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-strategy.fifo","Attributes":{"ContentBasedDeduplication":"false"}}`)
		if status != http.StatusOK {
			t.Fatalf("set %d %s", status, body)
		}
		status, body = call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-dedup-strategy.fifo","AttributeNames":["All"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"ContentBasedDeduplication":"false"`)) || !bytes.Contains(body, []byte(`"SqsManagedSseEnabled":"true"`)) {
			t.Fatalf("get %d %s", status, body)
		}
	})
	t.Run("Given visible and delayed messages When reading queue metrics Then each state is counted", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-message-states"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, payload := range []string{`{"QueueUrl":"http://queue/000000000000/bdd-message-states","MessageBody":"visible"}`, `{"QueueUrl":"http://queue/000000000000/bdd-message-states","MessageBody":"delayed","DelaySeconds":2}`} {
			if status, body := call("SendMessage", payload); status != http.StatusOK {
				t.Fatalf("send %d %s", status, body)
			}
		}
		status, body := call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-message-states","AttributeNames":["ApproximateNumberOfMessages","ApproximateNumberOfMessagesDelayed","ApproximateNumberOfMessagesNotVisible"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessages":"1"`)) || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessagesDelayed":"1"`)) || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessagesNotVisible":"0"`)) {
			t.Fatalf("metrics %d %s", status, body)
		}
	})
	t.Run("Given a message received twice When visibility expires Then the receipt handle rotates", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-rotate"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-rotate","MessageBody":"message"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-rotate","VisibilityTimeout":0}`)
		var first map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &first) != nil {
			t.Fatalf("first receive %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-rotate","VisibilityTimeout":0}`)
		var second map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &second) != nil {
			t.Fatalf("second receive %d %s", status, body)
		}
		firstHandle := first["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		secondHandle := second["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		if firstHandle == secondHandle {
			t.Fatalf("receipt handle did not rotate: %v", firstHandle)
		}
		change, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-rotate", "ReceiptHandle": firstHandle, "VisibilityTimeout": 0})
		if status, body = call("ChangeMessageVisibility", string(change)); status != http.StatusOK {
			t.Fatalf("prior handle change %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-rotate","VisibilityTimeout":0}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Body":"message"`)) {
			t.Fatalf("prior handle receive %d %s", status, body)
		}
	})
	t.Run("Given an expired FIFO receipt When deleting Then the expiry fault is returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-expired.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"0"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-expired.fifo","MessageBody":"message","MessageGroupId":"group"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-expired.fifo"}`)
		var received map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &received) != nil {
			t.Fatalf("receive %d %s", status, body)
		}
		handle := received["Messages"].([]any)[0].(map[string]any)["ReceiptHandle"]
		payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-expired.fifo", "ReceiptHandle": handle})
		status, body = call("DeleteMessage", string(payload))
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte(`"__type":"InvalidParameterValue"`)) || !bytes.Contains(body, []byte("receipt handle has expired")) {
			t.Fatalf("expired delete %d %s", status, body)
		}
	})
	t.Run("Given an emptied FIFO group When sending again Then the group remains usable", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-reuse-group.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-reuse-group.fifo","MessageBody":"first","MessageGroupId":"g1"}`); status != http.StatusOK {
			t.Fatalf("first send %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-reuse-group.fifo"}`)
		var received map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &received) != nil {
			t.Fatalf("first receive %d %s", status, body)
		}
		message := received["Messages"].([]any)[0].(map[string]any)
		deletePayload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-reuse-group.fifo", "ReceiptHandle": message["ReceiptHandle"]})
		if status, body = call("DeleteMessage", string(deletePayload)); status != http.StatusOK {
			t.Fatalf("delete %d %s", status, body)
		}
		if status, body = call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-reuse-group.fifo","MessageBody":"second","MessageGroupId":"g1"}`); status != http.StatusOK {
			t.Fatalf("second send %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-reuse-group.fifo"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Body":"second"`)) {
			t.Fatalf("final receive %d %s", status, body)
		}
	})
	t.Run("Given a partially visible FIFO group When receiving Then other groups are prioritized", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-partial-group.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true","VisibilityTimeout":"30"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		for _, payload := range []string{
			`{"QueueUrl":"http://queue/000000000000/bdd-partial-group.fifo","MessageBody":"g1-m1","MessageGroupId":"g1"}`,
			`{"QueueUrl":"http://queue/000000000000/bdd-partial-group.fifo","MessageBody":"g1-m2","MessageGroupId":"g1"}`,
			`{"QueueUrl":"http://queue/000000000000/bdd-partial-group.fifo","MessageBody":"g2-m1","MessageGroupId":"g2"}`,
		} {
			if status, body := call("SendMessage", payload); status != http.StatusOK {
				t.Fatalf("send %d %s", status, body)
			}
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-partial-group.fifo","MaxNumberOfMessages":2}`)
		var first map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &first) != nil {
			t.Fatalf("first receive %d %s", status, body)
		}
		firstMessage := first["Messages"].([]any)[0].(map[string]any)
		change, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/bdd-partial-group.fifo", "ReceiptHandle": firstMessage["ReceiptHandle"], "VisibilityTimeout": 0})
		if status, body = call("ChangeMessageVisibility", string(change)); status != http.StatusOK {
			t.Fatalf("change visibility %d %s", status, body)
		}
		status, body = call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-partial-group.fifo","MaxNumberOfMessages":3}`)
		g2, g1 := bytes.Index(body, []byte(`"Body":"g2-m1"`)), bytes.Index(body, []byte(`"Body":"g1-m1"`))
		if status != http.StatusOK || g2 < 0 || g1 < 0 || g2 > g1 {
			t.Fatalf("partial group order %d %s", status, body)
		}
	})
	t.Run("Given FIFO messages When deleting all or part of a group Then ordering remains isolated", func(t *testing.T) {
		create := func(name string) {
			t.Helper()
			if status, body := call("CreateQueue", `{"QueueName":"`+name+`","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`); status != http.StatusOK {
				t.Fatalf("create %s %d %s", name, status, body)
			}
		}
		send := func(name, body, group string) {
			t.Helper()
			payload := `{"QueueUrl":"http://queue/000000000000/` + name + `","MessageBody":"` + body + `","MessageGroupId":"` + group + `"}`
			if status, response := call("SendMessage", payload); status != http.StatusOK {
				t.Fatalf("send %s %d %s", name, status, response)
			}
		}
		drain := func(name string, max int) []any {
			t.Helper()
			status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/`+name+`","MaxNumberOfMessages":`+strconv.Itoa(max)+`}`)
			var response map[string]any
			if status != http.StatusOK || json.Unmarshal(body, &response) != nil {
				t.Fatalf("receive %s %d %s", name, status, body)
			}
			return response["Messages"].([]any)
		}
		delete := func(name string, message map[string]any) {
			t.Helper()
			payload, _ := json.Marshal(map[string]any{"QueueUrl": "http://queue/000000000000/" + name, "ReceiptHandle": message["ReceiptHandle"]})
			if status, body := call("DeleteMessage", string(payload)); status != http.StatusOK {
				t.Fatalf("delete %s %d %s", name, status, body)
			}
		}
		populate := func(name string) {
			for _, message := range []struct{ body, group string }{{"g1-m1", "g1"}, {"g2-m1", "g2"}, {"g1-m2", "g1"}, {"g2-m2", "g2"}, {"g1-m3", "g1"}, {"g1-m4", "g1"}, {"g3-m1", "g3"}} {
				send(name, message.body, message.group)
			}
		}
		full := "bdd-fifo-delete-order.fifo"
		create(full)
		populate(full)
		first := drain(full, 2)
		for _, raw := range first {
			delete(full, raw.(map[string]any))
		}
		remaining := drain(full, 10)
		if len(remaining) != 5 || remaining[0].(map[string]any)["Body"] != "g2-m1" {
			t.Fatalf("full delete ordering %#v", remaining)
		}
		partial := "bdd-fifo-partial-delete-order.fifo"
		create(partial)
		populate(partial)
		first = drain(partial, 2)
		delete(partial, first[0].(map[string]any))
		remaining = drain(partial, 10)
		if len(remaining) != 3 || remaining[0].(map[string]any)["Body"] != "g2-m1" {
			t.Fatalf("partial delete ordering %#v", remaining)
		}
	})
	t.Run("Given both SSE modes When updating a queue Then AWS rejects the conflicting attributes", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-sse-kms"}`); status != http.StatusOK {
			t.Fatalf("create KMS %d %s", status, body)
		}
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-sse-kms","Attributes":{"KmsMasterKeyId":"testKeyId","KmsDataKeyReusePeriodSeconds":"6000","SqsManagedSseEnabled":"false"}}`); status != http.StatusOK {
			t.Fatalf("set KMS %d %s", status, body)
		}
		if status, body := call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-sse-kms","AttributeNames":["KmsMasterKeyId","KmsDataKeyReusePeriodSeconds","SqsManagedSseEnabled"]}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"KmsMasterKeyId":"testKeyId"`)) || !bytes.Contains(body, []byte(`"SqsManagedSseEnabled":"false"`)) {
			t.Fatalf("get KMS %d %s", status, body)
		}
		if status, body := call("CreateQueue", `{"QueueName":"bdd-sse-exclusive"}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-sse-exclusive","Attributes":{"KmsMasterKeyId":"testKeyId","SqsManagedSseEnabled":"true"}}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("one type of server-side encryption")) {
			t.Fatalf("SSE conflict %d %s", status, body)
		}
		status, body = call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-sse-exclusive","AttributeNames":["All"]}`)
		if status != http.StatusOK || bytes.Contains(body, []byte(`"KmsMasterKeyId":"testKeyId"`)) {
			t.Fatalf("conflicting attributes persisted %d %s", status, body)
		}
	})
	t.Run("Given queue attributes When creating and updating Then defaults and changes are returned", func(t *testing.T) {
		if status, body := call("CreateQueue", `{"QueueName":"bdd-attribute-update","Attributes":{"MessageRetentionPeriod":"604800","ReceiveMessageWaitTimeSeconds":"10","VisibilityTimeout":"20"}}`); status != http.StatusOK {
			t.Fatalf("create %d %s", status, body)
		}
		status, body := call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-update","AttributeNames":["All"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"MaximumMessageSize":"1048576"`)) || !bytes.Contains(body, []byte(`"SqsManagedSseEnabled":"true"`)) || !bytes.Contains(body, []byte(`"ReceiveMessageWaitTimeSeconds":"10"`)) {
			t.Fatalf("initial attributes %d %s", status, body)
		}
		status, body = call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-update","Attributes":{"MaximumMessageSize":"2048","VisibilityTimeout":"69","DelaySeconds":"420"}}`)
		if status != http.StatusOK {
			t.Fatalf("update %d %s", status, body)
		}
		status, body = call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-attribute-update","AttributeNames":["All"]}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"MaximumMessageSize":"2048"`)) || !bytes.Contains(body, []byte(`"VisibilityTimeout":"69"`)) || !bytes.Contains(body, []byte(`"DelaySeconds":"420"`)) {
			t.Fatalf("updated attributes %d %s", status, body)
		}
	})
	t.Run("Given a move task When source or destination is invalid Then the exact resource fault is returned", func(t *testing.T) {
		for _, name := range []string{"bdd-move-plain", "bdd-move-destination", "bdd-move-dlq", "bdd-move-source"} {
			if status, body := call("CreateQueue", `{"QueueName":"`+name+`"}`); status != http.StatusOK {
				t.Fatalf("create %s %d %s", name, status, body)
			}
		}
		if status, body := call("StartMessageMoveTask", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-plain","DestinationArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-destination"}`); status != http.StatusBadRequest || !bytes.Contains(body, []byte("Dead Letter Queue")) {
			t.Fatalf("source validation %d %s", status, body)
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-dlq","maxReceiveCount":"1"}`
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-move-source","Attributes":{"RedrivePolicy":`+strconv.Quote(policy)+`}}`); status != http.StatusOK {
			t.Fatalf("set policy %d %s", status, body)
		}
		status, body := call("StartMessageMoveTask", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-dlq","DestinationArn":"arn:aws:sqs:us-east-1:000000000000:missing-destination"}`)
		if status != http.StatusNotFound || !bytes.Contains(body, []byte("DestinationArn")) {
			t.Fatalf("destination validation %d %s", status, body)
		}
	})
	t.Run("Given DLQ messages When starting a move task Then messages and task counters are reported", func(t *testing.T) {
		for _, name := range []string{"bdd-move-workflow-source", "bdd-move-workflow-dlq", "bdd-move-workflow-destination"} {
			if status, body := call("CreateQueue", `{"QueueName":"`+name+`"}`); status != http.StatusOK {
				t.Fatalf("create %s %d %s", name, status, body)
			}
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-workflow-dlq","maxReceiveCount":"1"}`
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-move-workflow-source","Attributes":{"RedrivePolicy":`+strconv.Quote(policy)+`}}`); status != http.StatusOK {
			t.Fatalf("set policy %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-move-workflow-dlq","MessageBody":"workflow"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		status, body := call("StartMessageMoveTask", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-workflow-dlq","DestinationArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-workflow-destination"}`)
		var started map[string]any
		if status != http.StatusOK || json.Unmarshal(body, &started) != nil || started["TaskHandle"] == "" {
			t.Fatalf("start %d %s", status, body)
		}
		status, body = call("ListMessageMoveTasks", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-workflow-dlq"}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessagesMoved":1`)) || !bytes.Contains(body, []byte(`"ApproximateNumberOfMessagesToMove":1`)) || !bytes.Contains(body, []byte(`"Status":"COMPLETED"`)) {
			t.Fatalf("list %d %s", status, body)
		}
	})
	t.Run("Given a redriven message When destination is omitted Then it returns to its source queue", func(t *testing.T) {
		for _, name := range []string{"bdd-move-default-source", "bdd-move-default-dlq"} {
			if status, body := call("CreateQueue", `{"QueueName":"`+name+`"}`); status != http.StatusOK {
				t.Fatalf("create %s %d %s", name, status, body)
			}
		}
		policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-default-dlq","maxReceiveCount":"1"}`
		if status, body := call("SetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-move-default-source","Attributes":{"RedrivePolicy":`+strconv.Quote(policy)+`}}`); status != http.StatusOK {
			t.Fatalf("set policy %d %s", status, body)
		}
		if status, body := call("SendMessage", `{"QueueUrl":"http://queue/000000000000/bdd-move-default-source","MessageBody":"return-me"}`); status != http.StatusOK {
			t.Fatalf("send %d %s", status, body)
		}
		for range 2 {
			if status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-move-default-source","VisibilityTimeout":0}`); status != http.StatusOK {
				t.Fatalf("receive %d %s", status, body)
			}
		}
		if status, body := call("StartMessageMoveTask", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:bdd-move-default-dlq"}`); status != http.StatusOK {
			t.Fatalf("start %d %s", status, body)
		}
		status, body := call("ReceiveMessage", `{"QueueUrl":"http://queue/000000000000/bdd-move-default-source","VisibilityTimeout":0}`)
		if status != http.StatusOK || !bytes.Contains(body, []byte(`"Body":"return-me"`)) {
			t.Fatalf("returned message %d %s", status, body)
		}
	})
	t.Run("Given an invalid move task handle When cancelling Then TaskHandle validation is returned", func(t *testing.T) {
		status, body := call("CancelMessageMoveTask", `{"TaskHandle":"foobared"}`)
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("TaskHandle is invalid")) {
			t.Fatalf("cancel validation %d %s", status, body)
		}
	})
}

func TestSQSAdvertisedQueueURLBDD(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.AdvertiseURL = "https://external.example/sqs/"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(rt.Handler())
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"QueueName":"bdd-advertised"}`))
	request.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	request.Header.Set("Content-Type", "application/x-amz-json-1.0")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"QueueUrl":"https://external.example/sqs/000000000000/bdd-advertised"`)) {
		t.Fatalf("advertised queue URL %d %s", response.StatusCode, body)
	}
	sendRequest, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"QueueUrl":"https://external.example/sqs/000000000000/bdd-advertised","MessageBody":"external"}`))
	sendRequest.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	sendRequest.Header.Set("Content-Type", "application/x-amz-json-1.0")
	sendResponse, err := http.DefaultClient.Do(sendRequest)
	if err != nil || sendResponse.StatusCode != http.StatusOK {
		t.Fatalf("external send response=%v error=%v", sendResponse, err)
	}
	sendResponse.Body.Close()
	receiveRequest, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"QueueUrl":"https://external.example/sqs/000000000000/bdd-advertised"}`))
	receiveRequest.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
	receiveRequest.Header.Set("Content-Type", "application/x-amz-json-1.0")
	receiveResponse, err := http.DefaultClient.Do(receiveRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer receiveResponse.Body.Close()
	receivedBody, _ := io.ReadAll(receiveResponse.Body)
	if receiveResponse.StatusCode != http.StatusOK || !bytes.Contains(receivedBody, []byte(`"Body":"external"`)) {
		t.Fatalf("external receive %d %s", receiveResponse.StatusCode, receivedBody)
	}
}

func TestSQSQueueArnPartitionBDD(t *testing.T) {
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
		request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-gov-west-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
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
	if status, body := call("CreateQueue", `{"QueueName":"bdd-gov-arn"}`); status != http.StatusOK {
		t.Fatalf("create %d %s", status, body)
	}
	if status, body := call("GetQueueAttributes", `{"QueueUrl":"http://queue/000000000000/bdd-gov-arn","AttributeNames":["QueueArn"]}`); status != http.StatusOK || !bytes.Contains(body, []byte(`"QueueArn":"arn:aws-us-gov:sqs:us-gov-west-1:000000000000:bdd-gov-arn"`)) {
		t.Fatalf("partitioned ARN %d %s", status, body)
	}
}
