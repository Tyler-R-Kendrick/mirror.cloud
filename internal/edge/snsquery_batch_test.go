package edge_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A query-protocol PublishBatch names its entries PublishBatchRequestEntries,
// each as PublishBatchRequestEntries.member.N.* with its message attributes
// under MessageAttributes.entry.M.Name / .Value.*:
// https://docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html
// The pack read only Entries, a name no client sends, so every real
// PublishBatch was answered EmptyBatchRequest.
//
// The answer is per entry: "The result of publishing each message is reported
// individually in the response", as Successful.member.N (Id, MessageId) and
// Failed.member.N. Each entry is delivered as its own notification, with its
// own subject and its attributes as Type and Value
// (https://docs.aws.amazon.com/sns/latest/dg/http-notification-json.html), and
// to a raw-delivery queue as SQS message attributes.
func TestSNSQueryPublishBatchReachesItsEntries(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sns", "aws.sqs"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	call := func(service, version string, form url.Values) string {
		t.Helper()
		form.Set("Version", version)
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = service + ".us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/"+service+"/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: %d %s", service, form.Get("Action"), res.StatusCode, body)
		}
		return string(body)
	}
	sqs := func(form url.Values) string { return call("sqs", "2012-11-05", form) }
	sns := func(form url.Values) string { return call("sns", "2010-03-31", form) }

	topicARN := between(sns(url.Values{"Action": {"CreateTopic"}, "Name": {"batched"}}), "<TopicArn>", "</TopicArn>")
	queue := func(name string) (queueURL, queueARN string) {
		queueURL = between(sqs(url.Values{"Action": {"CreateQueue"}, "QueueName": {name}}), "<QueueUrl>", "</QueueUrl>")
		attrs := sqs(url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL}, "AttributeName.1": {"QueueArn"}})
		return queueURL, between(attrs, "<Name>QueueArn</Name><Value>", "</Value>")
	}
	enveloped, envelopedARN := queue("batch-enveloped")
	raw, rawARN := queue("batch-raw")
	sns(url.Values{"Action": {"Subscribe"}, "TopicArn": {topicARN}, "Protocol": {"sqs"}, "Endpoint": {envelopedARN}})
	sns(url.Values{
		"Action": {"Subscribe"}, "TopicArn": {topicARN}, "Protocol": {"sqs"}, "Endpoint": {rawARN},
		"Attributes.entry.1.key": {"RawMessageDelivery"}, "Attributes.entry.1.value": {"true"},
	})

	binary := base64.StdEncoding.EncodeToString([]byte{0, 1, 0xfe, 0xff})
	body := sns(url.Values{
		"Action": {"PublishBatch"}, "TopicArn": {topicARN},
		"PublishBatchRequestEntries.member.1.Id":                                          {"first"},
		"PublishBatchRequestEntries.member.1.Message":                                     {"one"},
		"PublishBatchRequestEntries.member.1.Subject":                                     {"first subject"},
		"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Name":              {"kind"},
		"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.DataType":    {"String"},
		"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.StringValue": {"order"},
		"PublishBatchRequestEntries.member.2.Id":                                          {"second"},
		"PublishBatchRequestEntries.member.2.Message":                                     {"two"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.1.Name":              {"price"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.1.Value.DataType":    {"Number"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.1.Value.StringValue": {"7"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.2.Name":              {"blob"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.2.Value.DataType":    {"Binary"},
		"PublishBatchRequestEntries.member.2.MessageAttributes.entry.2.Value.BinaryValue": {binary},
	})
	var answer struct {
		Successful []struct{ Id, MessageId string } `xml:"PublishBatchResult>Successful>member"`
		Failed     []struct{ Id, Code string }      `xml:"PublishBatchResult>Failed>member"`
	}
	if err := xml.Unmarshal([]byte(body), &answer); err != nil {
		t.Fatalf("PublishBatch answer %v: %s", err, body)
	}
	published := map[string]string{}
	for _, entry := range answer.Successful {
		published[entry.Id] = entry.MessageId
	}
	if len(answer.Successful) != 2 || published["first"] == "" || published["second"] == "" || published["first"] == published["second"] {
		t.Errorf("Successful = %+v, want first and second, each with its own MessageId: %s", answer.Successful, body)
	}
	if len(answer.Failed) != 0 || !strings.Contains(body, "<Failed></Failed>") {
		t.Errorf("Failed = %+v, want the element, and empty: %s", answer.Failed, body)
	}

	type attribute struct {
		Name        string
		DataType    string `xml:"Value>DataType"`
		StringValue string `xml:"Value>StringValue"`
		BinaryValue string `xml:"Value>BinaryValue"`
	}
	type message struct {
		Body       string
		Attributes []attribute `xml:"MessageAttribute"`
	}
	receive := func(queueURL string) []message {
		t.Helper()
		body := sqs(url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {queueURL}, "MaxNumberOfMessages": {"10"}, "MessageAttributeName.1": {"All"}})
		var got struct {
			Messages []message `xml:"ReceiveMessageResult>Message"`
		}
		if err := xml.Unmarshal([]byte(body), &got); err != nil {
			t.Fatalf("ReceiveMessage %s: %v %s", queueURL, err, body)
		}
		return got.Messages
	}

	notifications := map[string]map[string]any{}
	for _, m := range receive(enveloped) {
		var envelope map[string]any
		if json.Unmarshal([]byte(m.Body), &envelope) != nil {
			t.Fatalf("notification body %q", m.Body)
		}
		notifications[envelope["Message"].(string)] = envelope
	}
	want := map[string]struct {
		id, subject string
		attributes  map[string]any
	}{
		"one": {"first", "first subject", map[string]any{"kind": map[string]any{"Type": "String", "Value": "order"}}},
		"two": {"second", "", map[string]any{
			"price": map[string]any{"Type": "Number", "Value": "7"},
			"blob":  map[string]any{"Type": "Binary", "Value": binary},
		}},
	}
	if len(notifications) != len(want) {
		t.Errorf("notifications for %d messages, want %d: %v", len(notifications), len(want), notifications)
	}
	for text, w := range want {
		got := notifications[text]
		if got["MessageId"] != published[w.id] {
			t.Errorf("%s: notification MessageId %v, want the %s entry's %q", text, got["MessageId"], w.id, published[w.id])
		}
		if subject, _ := got["Subject"].(string); subject != w.subject {
			t.Errorf("%s: Subject %q, want %q", text, subject, w.subject)
		}
		if !reflect.DeepEqual(got["MessageAttributes"], w.attributes) {
			t.Errorf("%s: MessageAttributes %#v, want %#v", text, got["MessageAttributes"], w.attributes)
		}
	}

	rawMessages := map[string]map[string]attribute{}
	for _, m := range receive(raw) {
		attrs := map[string]attribute{}
		for _, a := range m.Attributes {
			attrs[a.Name] = a
		}
		rawMessages[m.Body] = attrs
	}
	wantRaw := map[string]map[string]attribute{
		"one": {"kind": {Name: "kind", DataType: "String", StringValue: "order"}},
		"two": {
			"price": {Name: "price", DataType: "Number", StringValue: "7"},
			"blob":  {Name: "blob", DataType: "Binary", BinaryValue: binary},
		},
	}
	if !reflect.DeepEqual(rawMessages, wantRaw) {
		t.Errorf("raw delivery = %#v\nwant %#v", rawMessages, wantRaw)
	}
}

// snsQuery is an edge serving SNS, and a query-protocol call to it that
// answers the status and the body whatever the status is.
func snsQuery(t *testing.T) func(form url.Values) (int, string) {
	t.Helper()
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sns"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	t.Cleanup(ts.Close)
	return func(form url.Values) (int, string) {
		t.Helper()
		form.Set("Version", "2010-03-31")
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "sns.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sns/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}
}

// A FIFO topic answers a SequenceNumber for every message it accepts, from
// Publish and for each Successful PublishBatch entry: "a large, non-consecutive
// number that Amazon SNS assigns to each message ... SequenceNumber continues
// to increase for each MessageGroupId"
// (https://docs.aws.amazon.com/sns/latest/api/API_Publish.html,
// https://docs.aws.amazon.com/sns/latest/api/API_PublishBatchResultEntry.html).
// It increases across the topic, so it increases within every group, and it
// follows from the requests alone. A standard topic answers none.
func TestSNSQueryFIFOPublishAnswersSequenceNumbers(t *testing.T) {
	call := snsQuery(t)
	must := func(form url.Values) string {
		t.Helper()
		status, body := call(form)
		if status != http.StatusOK {
			t.Fatalf("%s: %d %s", form.Get("Action"), status, body)
		}
		return body
	}
	fifo := between(must(url.Values{
		"Action": {"CreateTopic"}, "Name": {"orders.fifo"},
		"Attributes.entry.1.key": {"FifoTopic"}, "Attributes.entry.1.value": {"true"},
	}), "<TopicArn>", "</TopicArn>")
	standard := between(must(url.Values{"Action": {"CreateTopic"}, "Name": {"orders"}}), "<TopicArn>", "</TopicArn>")

	var sequence []string
	for i, group := range []string{"a", "b"} {
		body := must(url.Values{
			"Action": {"Publish"}, "TopicArn": {fifo}, "Message": {"single " + group},
			"MessageGroupId": {group}, "MessageDeduplicationId": {"single-" + group},
		})
		got := between(body, "<SequenceNumber>", "</SequenceNumber>")
		if got == "" {
			t.Fatalf("Publish %d to a FIFO topic answered no SequenceNumber: %s", i, body)
		}
		sequence = append(sequence, got)
	}
	body := must(url.Values{
		"Action": {"PublishBatch"}, "TopicArn": {fifo},
		"PublishBatchRequestEntries.member.1.Id":                     {"first"},
		"PublishBatchRequestEntries.member.1.Message":                {"batch a"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"a"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"batch-a"},
		"PublishBatchRequestEntries.member.2.Id":                     {"second"},
		"PublishBatchRequestEntries.member.2.Message":                {"batch b"},
		"PublishBatchRequestEntries.member.2.MessageGroupId":         {"b"},
		"PublishBatchRequestEntries.member.2.MessageDeduplicationId": {"batch-b"},
	})
	var answer struct {
		Successful []struct{ Id, MessageId, SequenceNumber string } `xml:"PublishBatchResult>Successful>member"`
	}
	if err := xml.Unmarshal([]byte(body), &answer); err != nil || len(answer.Successful) != 2 {
		t.Fatalf("PublishBatch answer %v: %s", err, body)
	}
	for _, entry := range answer.Successful {
		if entry.SequenceNumber == "" {
			t.Fatalf("PublishBatch entry %s answered no SequenceNumber: %s", entry.Id, body)
		}
		sequence = append(sequence, entry.SequenceNumber)
	}
	// Fixed width, so the strings order as the numbers do.
	want := []string{"10000000000000000001", "10000000000000000002", "10000000000000000003", "10000000000000000004"}
	if !reflect.DeepEqual(sequence, want) {
		t.Errorf("SequenceNumbers %v, want %v: increasing across the topic and the same on every run", sequence, want)
	}

	// A duplicate inside the deduplication window is accepted and not delivered
	// again; it answers the first message's MessageId and SequenceNumber and
	// takes no new number.
	first := must(url.Values{
		"Action": {"Publish"}, "TopicArn": {fifo}, "Message": {"single a"},
		"MessageGroupId": {"a"}, "MessageDeduplicationId": {"single-a"},
	})
	if got := between(first, "<SequenceNumber>", "</SequenceNumber>"); got != sequence[0] {
		t.Errorf("a deduplicated Publish answered SequenceNumber %q, want the first message's %q", got, sequence[0])
	}
	next := must(url.Values{
		"Action": {"Publish"}, "TopicArn": {fifo}, "Message": {"after"},
		"MessageGroupId": {"a"}, "MessageDeduplicationId": {"after"},
	})
	if got := between(next, "<SequenceNumber>", "</SequenceNumber>"); got != "10000000000000000005" {
		t.Errorf("Publish after a duplicate answered SequenceNumber %q, want 10000000000000000005", got)
	}

	body = must(url.Values{"Action": {"Publish"}, "TopicArn": {standard}, "Message": {"plain"}})
	if strings.Contains(body, "SequenceNumber") {
		t.Errorf("Publish to a standard topic answered a SequenceNumber: %s", body)
	}
	body = must(url.Values{
		"Action": {"PublishBatch"}, "TopicArn": {standard},
		"PublishBatchRequestEntries.member.1.Id": {"only"}, "PublishBatchRequestEntries.member.1.Message": {"plain"},
	})
	if strings.Contains(body, "SequenceNumber") {
		t.Errorf("PublishBatch to a standard topic answered a SequenceNumber: %s", body)
	}
}

// An entry Id that breaks the rules and two entries that share one are
// request errors with their own codes, not InvalidParameter: "InvalidBatchEntryId:
// The Id of a batch entry in a batch request doesn't abide by the
// specification" and "BatchEntryIdsNotDistinct: Two or more batch entries in
// the request have the same Id", both HTTP 400 and the sender's fault
// (https://docs.aws.amazon.com/sns/latest/api/API_PublishBatch.html#API_PublishBatch_Errors).
func TestSNSQueryPublishBatchIdErrors(t *testing.T) {
	call := snsQuery(t)
	_, body := call(url.Values{"Action": {"CreateTopic"}, "Name": {"ids"}})
	topic := between(body, "<TopicArn>", "</TopicArn>")
	for _, tc := range []struct {
		name, code, message string
		entries             url.Values
	}{
		{"invalid", "InvalidBatchEntryId", "The Id of a batch entry in a batch request doesn't abide by the specification.", url.Values{
			"PublishBatchRequestEntries.member.1.Id": {"bad.id"}, "PublishBatchRequestEntries.member.1.Message": {"x"},
		}},
		{"duplicate", "BatchEntryIdsNotDistinct", "Two or more batch entries in the request have the same Id.", url.Values{
			"PublishBatchRequestEntries.member.1.Id": {"same"}, "PublishBatchRequestEntries.member.1.Message": {"x"},
			"PublishBatchRequestEntries.member.2.Id": {"same"}, "PublishBatchRequestEntries.member.2.Message": {"y"},
		}},
	} {
		form := url.Values{"Action": {"PublishBatch"}, "TopicArn": {topic}}
		for key, values := range tc.entries {
			form[key] = values
		}
		status, body := call(form)
		var got struct {
			Type    string `xml:"Error>Type"`
			Code    string `xml:"Error>Code"`
			Message string `xml:"Error>Message"`
		}
		if err := xml.Unmarshal([]byte(body), &got); err != nil || status != http.StatusBadRequest || got.Type != "Sender" || got.Code != tc.code || got.Message != tc.message {
			t.Errorf("%s Id: %d %+v (%v), want 400 Sender %s %q", tc.name, status, got, err, tc.code, tc.message)
		}
	}
}
