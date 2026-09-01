package behavior

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
)

func TestStatesWaitFeature(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.states"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/states/aws4_request, SignedHeaders=host, Signature=00"
	call := func(operation, body string) map[string]any {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AWSStepFunctions."+operation)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s: %d %s", operation, res.StatusCode, raw)
		}
		out := map[string]any{}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}

	t.Run("Given a due Wait When execution starts Then it completes", func(t *testing.T) {
		definition, _ := json.Marshal(`{"StartAt":"Wait","States":{"Wait":{"Type":"Wait","Seconds":0,"End":true}}}`)
		created := call("CreateStateMachine", `{"name":"wait-feature","definition":`+string(definition)+`,"roleArn":"arn:aws:iam::000000000000:role/states"}`)
		started := call("StartExecution", `{"stateMachineArn":"`+created["stateMachineArn"].(string)+`","name":"run"}`)
		deadline := time.Now().Add(time.Second)
		for {
			described := call("DescribeExecution", `{"executionArn":"`+started["executionArn"].(string)+`"}`)
			if described["status"] == "SUCCEEDED" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("Wait execution remained %#v", described)
			}
			time.Sleep(time.Millisecond)
		}
	})
}
