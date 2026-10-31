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
