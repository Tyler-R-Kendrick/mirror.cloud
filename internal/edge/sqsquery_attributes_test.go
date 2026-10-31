package edge_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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
