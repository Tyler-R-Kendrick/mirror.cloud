package behavior

import (
	"bytes"
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

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
)

func TestSNSTopicPublishBDD(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sns"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	call := func(values url.Values) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(values.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "sns.us-east-1.localhost"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sns/aws4_request, SignedHeaders=host, Signature=00")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, body
	}

	status, body := call(url.Values{"Action": {"CreateTopic"}, "Version": {"2010-03-31"}, "Name": {"bdd-topic"}})
	start, end := bytes.Index(body, []byte("<TopicArn>")), bytes.Index(body, []byte("</TopicArn>"))
	if status != http.StatusOK || start < 0 || end <= start {
		t.Fatalf("create topic %d %s", status, body)
	}
	arn := string(body[start+len("<TopicArn>") : end])
	status, body = call(url.Values{"Action": {"Publish"}, "Version": {"2010-03-31"}, "TargetArn": {arn}, "Message": {"hello"}})
	if status != http.StatusOK || !bytes.Contains(body, []byte("<PublishResult>")) {
		t.Fatalf("target publish %d %s", status, body)
	}
	status, body = call(url.Values{"Action": {"Publish"}, "Version": {"2010-03-31"}, "TopicArn": {"bad"}, "Message": {"hello"}})
	if status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidParameter")) {
		t.Fatalf("malformed publish %d %s", status, body)
	}
	status, body = call(url.Values{
		"Action": {"Subscribe"}, "Version": {"2010-03-31"}, "TopicArn": {arn},
		"Protocol": {"sms"}, "Endpoint": {"+15555550111"},
		"Attributes.entry.1.key": {"FilterPolicy"}, "Attributes.entry.1.value": {"not-json"},
	})
	if status != http.StatusBadRequest || !bytes.Contains(body, []byte("FilterPolicy")) {
		t.Fatalf("invalid filter subscribe %d %s", status, body)
	}
	status, body = call(url.Values{
		"Action": {"ConfirmSubscription"}, "Version": {"2010-03-31"},
		"TopicArn": {arn}, "Token": {"randomtoken"},
	})
	if status != http.StatusBadRequest || !bytes.Contains(body, []byte("Token")) {
		t.Fatalf("unissued confirm token %d %s", status, body)
	}
}
