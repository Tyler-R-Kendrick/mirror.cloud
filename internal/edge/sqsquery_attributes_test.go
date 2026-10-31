package edge_test

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A query-protocol client names SQS queue attributes as Attribute.N.Name and
// Attribute.N.Value (the model's xmlNames for the map entry). Read as
// Attribute.N.key/value they were lost: CreateQueue of a .fifo name answered
// "FifoQueue must be specified as true", and SetQueueAttributes answered
// MissingParameter Attributes.
func TestSQSQueryAttributesArriveUnderTheirEntryNames(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	call := func(form url.Values) (int, string) {
		t.Helper()
		form.Set("Version", "2012-11-05")
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "sqs.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}

	status, body := call(url.Values{
		"Action":            {"CreateQueue"},
		"QueueName":         {"orders.fifo"},
		"Attribute.1.Name":  {"FifoQueue"},
		"Attribute.1.Value": {"true"},
	})
	if status != http.StatusOK || !strings.Contains(body, "<QueueUrl>") {
		t.Fatalf("CreateQueue orders.fifo with FifoQueue=true: %d %s", status, body)
	}

	status, body = call(url.Values{
		"Action":            {"CreateQueue"},
		"QueueName":         {"jobs"},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"45"},
	})
	if status != http.StatusOK {
		t.Fatalf("CreateQueue jobs: %d %s", status, body)
	}
	queueURL := between(body, "<QueueUrl>", "</QueueUrl>")
	visibility := func() string {
		t.Helper()
		status, body := call(url.Values{
			"Action":          {"GetQueueAttributes"},
			"QueueUrl":        {queueURL},
			"AttributeName.1": {"VisibilityTimeout"},
		})
		if status != http.StatusOK {
			t.Fatalf("GetQueueAttributes: %d %s", status, body)
		}
		return between(body, "<Name>VisibilityTimeout</Name><Value>", "</Value>")
	}
	if got := visibility(); got != "45" {
		t.Fatalf("VisibilityTimeout after CreateQueue = %q, want 45", got)
	}

	status, body = call(url.Values{
		"Action":            {"SetQueueAttributes"},
		"QueueUrl":          {queueURL},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"50"},
	})
	if status != http.StatusOK {
		t.Fatalf("SetQueueAttributes: %d %s", status, body)
	}
	if got := visibility(); got != "50" {
		t.Fatalf("VisibilityTimeout after SetQueueAttributes = %q, want 50", got)
	}
}

// SendMessage's MessageAttributes is a flattened map too: member xmlName
// MessageAttribute, key Name, value Value, so a query-protocol client sends
// MessageAttribute.1.Name=kind&MessageAttribute.1.Value.DataType=String&
// MessageAttribute.1.Value.StringValue=order. Read as key/value they were
// dropped without a word: SendMessage answered no MD5OfMessageAttributes and
// the message arrived with no attributes. Read under their names they arrive,
// SendMessage answers the digest AWS documents, and an attribute that names
// no DataType is refused rather than dropped.
func TestSQSQuerySendMessageAttributesArriveUnderTheirEntryNames(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	call := func(form url.Values) (int, string) {
		t.Helper()
		form.Set("Version", "2012-11-05")
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "sqs.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(body)
	}

	status, body := call(url.Values{"Action": {"CreateQueue"}, "QueueName": {"attributes"}})
	if status != http.StatusOK {
		t.Fatalf("CreateQueue: %d %s", status, body)
	}
	queueURL := between(body, "<QueueUrl>", "</QueueUrl>")

	blob := []byte{0, 1, 0xfe, 0xff}
	status, body = call(url.Values{
		"Action": {"SendMessage"}, "QueueUrl": {queueURL}, "MessageBody": {"placed"},
		"MessageAttribute.1.Name":              {"kind"},
		"MessageAttribute.1.Value.DataType":    {"String"},
		"MessageAttribute.1.Value.StringValue": {"order"},
		"MessageAttribute.2.Name":              {"price"},
		"MessageAttribute.2.Value.DataType":    {"Number"},
		"MessageAttribute.2.Value.StringValue": {"12.5"},
		"MessageAttribute.3.Name":              {"blob"},
		"MessageAttribute.3.Value.DataType":    {"Binary"},
		"MessageAttribute.3.Value.BinaryValue": {base64.StdEncoding.EncodeToString(blob)},
	})
	if status != http.StatusOK {
		t.Fatalf("SendMessage: %d %s", status, body)
	}
	// The helper is checked against a published vector before it judges
	// anything: moto's SQS tests pin 235c5c51... for this one attribute.
	if got := documentedAttributesMD5([]queueAttribute{{"timestamp", "Number", []byte("1493147359900")}}); got != "235c5c510d26fb653d073faed50ae77c" {
		t.Fatalf("documentedAttributesMD5 = %s, want the published 235c5c510d26fb653d073faed50ae77c", got)
	}
	want := documentedAttributesMD5([]queueAttribute{
		{"kind", "String", []byte("order")},
		{"price", "Number", []byte("12.5")},
		{"blob", "Binary", blob},
	})
	if got := between(body, "<MD5OfMessageAttributes>", "</MD5OfMessageAttributes>"); got != want {
		t.Errorf("SendMessage MD5OfMessageAttributes = %q, want %q: %s", got, want, body)
	}

	status, body = call(url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {queueURL}, "MessageAttributeName.1": {"All"}})
	if status != http.StatusOK {
		t.Fatalf("ReceiveMessage: %d %s", status, body)
	}
	if got := between(body, "<MD5OfMessageAttributes>", "</MD5OfMessageAttributes>"); got != want {
		t.Errorf("ReceiveMessage MD5OfMessageAttributes = %q, want %q: %s", got, want, body)
	}
	for _, name := range []string{"kind", "price", "blob"} {
		if !strings.Contains(body, "<Name>"+name+"</Name>") {
			t.Errorf("ReceiveMessage carries no attribute %s: %s", name, body)
		}
	}

	status, body = call(url.Values{
		"Action": {"SendMessage"}, "QueueUrl": {queueURL}, "MessageBody": {"untyped"},
		"MessageAttribute.1.Name":              {"kind"},
		"MessageAttribute.1.Value.StringValue": {"order"},
	})
	if status != http.StatusBadRequest || !strings.Contains(body, "<Code>InvalidParameterValue</Code>") {
		t.Fatalf("SendMessage with an attribute that has no DataType: %d %s, want 400 InvalidParameterValue", status, body)
	}
}

type queueAttribute struct {
	name, dataType string
	value          []byte
}

// documentedAttributesMD5 is MD5OfMessageAttributes computed the way AWS
// documents it, independently of the emulator's own implementation:
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-attributes-md5-message-digest-calculation
// Attributes sorted by name; each one's name, then data type, each as a 4-byte
// big-endian length and its UTF-8 bytes; one transport byte, 1 for String and
// Number and 2 for Binary; then the value as a 4-byte length and its bytes
// (UTF-8 for String, raw for Binary). The digest is over the whole buffer.
func documentedAttributesMD5(attrs []queueAttribute) string {
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].name < attrs[j].name })
	digest := md5.New()
	field := func(b []byte) {
		_ = binary.Write(digest, binary.BigEndian, uint32(len(b)))
		digest.Write(b)
	}
	for _, a := range attrs {
		field([]byte(a.name))
		field([]byte(a.dataType))
		if strings.HasPrefix(a.dataType, "Binary") {
			digest.Write([]byte{2})
		} else {
			digest.Write([]byte{1})
		}
		field(a.value)
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func between(s, open, close string) string {
	i := strings.Index(s, open)
	if i < 0 {
		return ""
	}
	rest := s[i+len(open):]
	j := strings.Index(rest, close)
	if j < 0 {
		return ""
	}
	return rest[:j]
}
