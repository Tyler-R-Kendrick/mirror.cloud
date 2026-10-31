package edge_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda" // natives the Lambda bundle serves
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A query-protocol Publish names its message attributes as
// MessageAttributes.entry.N.Name and MessageAttributes.entry.N.Value.* (the
// model's xmlNames for the map entry), which the decoder hands the pack as a
// structured MessageAttributeValue per name: DataType with a StringValue or a
// BinaryValue. SNS does not deliver them in that shape. It maps each name to a
// Type and a Value:
//
//   - the notification body an SQS queue without raw delivery (or an HTTP/S
//     endpoint) receives: "This object maps each attribute name to an object
//     with a Type and a Value."
//     https://docs.aws.amazon.com/sns/latest/dg/http-notification-json.html
//   - the event a Lambda function is invoked with, whose Sns.MessageAttributes
//     is {"Test": {"Type": "String", "Value": "TestString"}, "TestBinary":
//     {"Type": "Binary", "Value": "TestBinary"}}.
//     https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html#sns-sample-event
//     Lambda takes no Number and no String.Array: "it's passed as the String
//     data type in the JSON payload that Amazon SNS delivers to Lambda."
//     https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html#SNSMessageAttributes.DataTypes
//
// A Binary value is base64 text in Value, as it was on the wire: JSON has no
// bytes. Raw delivery to SQS is the other case: the queue gets the attributes
// as SQS message attributes, DataType with StringValue or BinaryValue.
func TestSNSQueryPublishDeliversAttributesInTheNotificationShape(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sns", "aws.sqs", "aws.lambda"}
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

	topicARN := between(sns(url.Values{"Action": {"CreateTopic"}, "Name": {"orders"}}), "<TopicArn>", "</TopicArn>")
	parts := strings.Split(topicARN, ":")
	if len(parts) != 6 {
		t.Fatalf("TopicArn %q", topicARN)
	}
	id := spi.Identity{Region: parts[3], Account: parts[4]}

	queue := func(name string) (queueURL, queueARN string) {
		queueURL = between(sqs(url.Values{"Action": {"CreateQueue"}, "QueueName": {name}}), "<QueueUrl>", "</QueueUrl>")
		attrs := sqs(url.Values{"Action": {"GetQueueAttributes"}, "QueueUrl": {queueURL}, "AttributeName.1": {"QueueArn"}})
		return queueURL, between(attrs, "<Name>QueueArn</Name><Value>", "</Value>")
	}
	enveloped, envelopedARN := queue("enveloped")
	raw, rawARN := queue("raw")
	sns(url.Values{"Action": {"Subscribe"}, "TopicArn": {topicARN}, "Protocol": {"sqs"}, "Endpoint": {envelopedARN}})
	sns(url.Values{
		"Action": {"Subscribe"}, "TopicArn": {topicARN}, "Protocol": {"sqs"}, "Endpoint": {rawARN},
		"Attributes.entry.1.key": {"RawMessageDelivery"}, "Attributes.entry.1.value": {"true"},
	})

	eventOut := filepath.Join(t.TempDir(), "event.json")
	code := "import json,os\ndef lambda_handler(event, context):\n open(os.environ['EVENT_OUT'],'w').write(json.dumps(event))\n return event\n"
	if _, err := bundled.Handler("aws.lambda", deps).Invoke(context.Background(), &spi.Request{Identity: id, Operation: "CreateFunction", Input: map[string]any{
		"Role": "arn:aws:iam::000000000000:role/lambda", "FunctionName": "attributes", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
		"Code":        map[string]any{"ZipFile": base64.StdEncoding.EncodeToString([]byte(code))},
		"Environment": map[string]any{"Variables": map[string]any{"EVENT_OUT": eventOut}},
	}}); err != nil {
		t.Fatal(err)
	}
	sns(url.Values{"Action": {"Subscribe"}, "TopicArn": {topicARN}, "Protocol": {"lambda"}, "Endpoint": {"arn:aws:lambda:" + id.Region + ":" + id.Account + ":function:attributes"}})

	binary := base64.StdEncoding.EncodeToString([]byte{0, 1, 0xfe, 0xff})
	sns(url.Values{
		"Action": {"Publish"}, "TopicArn": {topicARN}, "Message": {"placed"},
		"MessageAttributes.entry.1.Name":              {"event"},
		"MessageAttributes.entry.1.Value.DataType":    {"String"},
		"MessageAttributes.entry.1.Value.StringValue": {"order"},
		"MessageAttributes.entry.2.Name":              {"price"},
		"MessageAttributes.entry.2.Value.DataType":    {"Number"},
		"MessageAttributes.entry.2.Value.StringValue": {"12.5"},
		"MessageAttributes.entry.3.Name":              {"blob"},
		"MessageAttributes.entry.3.Value.DataType":    {"Binary"},
		"MessageAttributes.entry.3.Value.BinaryValue": {binary},
	})

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
	receive := func(queueURL string) message {
		t.Helper()
		body := sqs(url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {queueURL}, "MessageAttributeName.1": {"All"}})
		var got struct {
			Messages []message `xml:"ReceiveMessageResult>Message"`
		}
		if err := xml.Unmarshal([]byte(body), &got); err != nil || len(got.Messages) != 1 {
			t.Fatalf("ReceiveMessage %s: %v %s", queueURL, err, body)
		}
		return got.Messages[0]
	}

	var envelope map[string]any
	if body := receive(enveloped).Body; json.Unmarshal([]byte(body), &envelope) != nil {
		t.Fatalf("notification body %q", body)
	}
	wantNotification := map[string]any{
		"event": map[string]any{"Type": "String", "Value": "order"},
		"price": map[string]any{"Type": "Number", "Value": "12.5"},
		"blob":  map[string]any{"Type": "Binary", "Value": binary},
	}
	if got := envelope["MessageAttributes"]; !reflect.DeepEqual(got, wantNotification) {
		t.Errorf("notification MessageAttributes = %#v\nwant %#v", got, wantNotification)
	}

	rawMessage := receive(raw)
	if rawMessage.Body != "placed" {
		t.Errorf("raw delivery body = %q, want the message itself", rawMessage.Body)
	}
	gotRaw := map[string]attribute{}
	for _, a := range rawMessage.Attributes {
		gotRaw[a.Name] = a
	}
	wantRaw := map[string]attribute{
		"event": {Name: "event", DataType: "String", StringValue: "order"},
		"price": {Name: "price", DataType: "Number", StringValue: "12.5"},
		"blob":  {Name: "blob", DataType: "Binary", BinaryValue: binary},
	}
	if !reflect.DeepEqual(gotRaw, wantRaw) {
		t.Errorf("raw delivery MessageAttributes = %#v\nwant %#v", gotRaw, wantRaw)
	}

	rawEvent, err := os.ReadFile(eventOut)
	if err != nil {
		t.Fatalf("the Lambda subscriber was not invoked: %v", err)
	}
	var event struct {
		Records []struct {
			Sns struct{ MessageAttributes map[string]any }
		}
	}
	if json.Unmarshal(rawEvent, &event) != nil || len(event.Records) != 1 {
		t.Fatalf("Lambda event %s", rawEvent)
	}
	wantLambda := map[string]any{
		"event": map[string]any{"Type": "String", "Value": "order"},
		"price": map[string]any{"Type": "String", "Value": "12.5"},
		"blob":  map[string]any{"Type": "Binary", "Value": binary},
	}
	if got := event.Records[0].Sns.MessageAttributes; !reflect.DeepEqual(got, wantLambda) {
		t.Errorf("Lambda Sns.MessageAttributes = %#v\nwant %#v", got, wantLambda)
	}
}
