package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestBootedServerSQSJSONAndQuery(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.Seed = "sqs-spine"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"

	jsonCall := func(op, body string) (int, map[string]any, []byte) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return res.StatusCode, out, raw
	}
	if code, out, raw := jsonCall("CreateQueue", `{"QueueName":"q"}`); code >= 300 {
		t.Fatalf("create %d %s", code, raw)
	} else if out["QueueUrl"] == nil {
		t.Fatalf("no url %v", out)
	}
	if code, _, raw := jsonCall("SendMessage", `{"QueueName":"q","MessageBody":"hello-json"}`); code >= 300 {
		t.Fatalf("send %d %s", code, raw)
	}
	code, recv, raw := jsonCall("ReceiveMessage", `{"QueueName":"q","MaxNumberOfMessages":1,"WaitTimeSeconds":0,"VisibilityTimeout":30}`)
	if code >= 300 {
		t.Fatalf("recv %d %s", code, raw)
	}
	msgs, _ := recv["Messages"].([]any)
	if len(msgs) != 1 || asM(msgs[0])["Body"] != "hello-json" {
		t.Fatalf("json recv %v", recv)
	}
	code, recv2, _ := jsonCall("ReceiveMessage", `{"QueueName":"q","WaitTimeSeconds":0}`)
	if code != 200 {
		t.Fatalf("inflight %d", code)
	}
	if n := len(asSlice(recv2["Messages"])); n != 0 {
		t.Fatalf("visibility not applied %v", recv2)
	}

	form := url.Values{"Action": {"SendMessage"}, "Version": {"2012-11-05"}, "QueueName": {"q"}, "MessageBody": {"hello-query"}}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	qb, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("query send %d %s", res.StatusCode, qb)
	}
	if !strings.Contains(string(qb), "SendMessage") && !strings.Contains(string(qb), "MessageId") {
		t.Fatalf("query xml %s", qb)
	}
	if h := res.Header.Get("x-mirror-fidelity"); h != "emulate" && h != "" {
		// edge sets fidelity on success
	}
}

func TestBootedServerSQSQueryTags(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.Seed = "sqs-query-tags"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	call := func(form url.Values) []byte {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode >= 300 {
			t.Fatalf("query %v: %d %s", form.Get("Action"), res.StatusCode, body)
		}
		return body
	}
	call(url.Values{"Action": {"CreateQueue"}, "Version": {"2012-11-05"}, "QueueName": {"query-tags"}, "Tag.1.Key": {"first"}, "Tag.1.Value": {"one"}})
	call(url.Values{"Action": {"TagQueue"}, "Version": {"2012-11-05"}, "QueueName": {"query-tags"}, "Tags.member.1.Key": {"second"}, "Tags.member.1.Value": {"two"}})
	body := call(url.Values{"Action": {"ListQueueTags"}, "Version": {"2012-11-05"}, "QueueName": {"query-tags"}})
	if !strings.Contains(string(body), "<first>one</first>") || !strings.Contains(string(body), "<second>two</second>") {
		t.Fatalf("query tags response %s", body)
	}
}

func TestBootedServerSQSJSONOnQueueURL(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.Seed = "sqs-json-queue-url"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"
	request := func(target, endpoint, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS."+target)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	create := request("CreateQueue", ts.URL+"/", `{"QueueName":"json-url"}`)
	defer create.Body.Close()
	var created map[string]any
	if err := json.NewDecoder(create.Body).Decode(&created); err != nil || create.StatusCode >= 300 {
		t.Fatalf("create queue: %d %#v %v", create.StatusCode, created, err)
	}
	queueURL := str(created["QueueUrl"])
	parsed, err := url.Parse(queueURL)
	if err != nil {
		t.Fatal(err)
	}
	receive := request("ReceiveMessage", ts.URL+parsed.Path, `{"QueueUrl":"`+queueURL+`","WaitTimeSeconds":1}`)
	defer receive.Body.Close()
	if receive.StatusCode != http.StatusOK || receive.Header.Get("Content-Type") != "application/x-amz-json-1.0" {
		raw, _ := io.ReadAll(receive.Body)
		t.Fatalf("receive on queue URL: %d %s %q", receive.StatusCode, raw, receive.Header.Get("Content-Type"))
	}
}

func TestBootedServerSQSPathEndpointStrategy(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.Seed = "sqs-path-strategy"
	cfg.AdvertiseURL = "http://localhost:4566"
	cfg.SQSEndpointStrategy = "path"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"QueueName":"path-q"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/eu-north-1/sqs/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil || res.StatusCode >= 300 {
		t.Fatalf("create: %d %#v %v", res.StatusCode, body, err)
	}
	if got := str(body["QueueUrl"]); got != "http://localhost.localstack.cloud:4566/queue/eu-north-1/000000000000/path-q" {
		t.Fatalf("path strategy URL %q", got)
	}
}

func TestBootedServerSQSSection48(t *testing.T) {
	t.Setenv("MIRROR_CLOCK", "controllable")
	cfg := config.Default()
	cfg.Services = []string{"aws.sqs"}
	cfg.Seed = "sqs-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"
	jsonCall := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return out
	}
	dlqOut := jsonCall("CreateQueue", `{"QueueName":"dlq"}`)
	jsonCall("CreateQueue", `{"QueueName":"q","Attributes":{"VisibilityTimeout":"0","RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:dlq\",\"maxReceiveCount\":\"1\"}"}}`)
	urlOut := jsonCall("GetQueueUrl", `{"QueueName":"q"}`)
	if urlOut["QueueUrl"] == nil {
		t.Fatalf("get url %v", urlOut)
	}
	queueURL, err := url.Parse(str(urlOut["QueueUrl"]))
	if err != nil {
		t.Fatal(err)
	}
	queueEndpoint := ts.URL + queueURL.Path
	for _, action := range []string{"FooBar", "CreateQueue", "ListQueues"} {
		queryReq, _ := http.NewRequest(http.MethodGet, queueEndpoint+"?Action="+action, nil)
		queryReq.Header.Set("Authorization", auth)
		queryRes, err := http.DefaultClient.Do(queryReq)
		if err != nil {
			t.Fatal(err)
		}
		queryBody, _ := io.ReadAll(queryRes.Body)
		queryRes.Body.Close()
		if queryRes.StatusCode != http.StatusBadRequest || !strings.Contains(string(queryBody), "<Code>InvalidAction</Code>") || !strings.Contains(string(queryBody), "The action "+action+" is not valid for this endpoint.") {
			t.Fatalf("query invalid action %s: %d %s", action, queryRes.StatusCode, queryBody)
		}
	}
	missingParamReq, _ := http.NewRequest(http.MethodGet, queueEndpoint+"?Action=SendMessage", nil)
	missingParamReq.Header.Set("Authorization", auth)
	missingParamRes, err := http.DefaultClient.Do(missingParamReq)
	if err != nil {
		t.Fatal(err)
	}
	missingParamBody, _ := io.ReadAll(missingParamRes.Body)
	missingParamRes.Body.Close()
	if missingParamRes.StatusCode != http.StatusBadRequest || !strings.Contains(string(missingParamBody), "<Code>MissingParameter</Code>") || !strings.Contains(string(missingParamBody), "The request must contain the parameter MessageBody.") {
		t.Fatalf("query missing parameter: %d %s", missingParamRes.StatusCode, missingParamBody)
	}
	dlqURL, err := url.Parse(str(dlqOut["QueueUrl"]))
	if err != nil {
		t.Fatal(err)
	}
	unauthReq, _ := http.NewRequest(http.MethodGet, ts.URL+dlqURL.Path+"?Action=GetQueueAttributes&AttributeName.1=All", nil)
	unauthRes, err := http.DefaultClient.Do(unauthReq)
	if err != nil {
		t.Fatal(err)
	}
	unauthBody, _ := io.ReadAll(unauthRes.Body)
	unauthRes.Body.Close()
	if unauthRes.StatusCode != http.StatusOK || !strings.Contains(string(unauthBody), "<GetQueueAttributesResponse") || !strings.Contains(string(unauthBody), "<Name>VisibilityTimeout</Name><Value>30") {
		t.Fatalf("query without auth params: %d %s", unauthRes.StatusCode, unauthBody)
	}
	missingNameReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"QueueUrl":"http://queue/000000000000/q"}`))
	missingNameReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	missingNameReq.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueUrl")
	missingNameReq.Header.Set("Authorization", auth)
	missingNameRes, err := http.DefaultClient.Do(missingNameReq)
	if err != nil {
		t.Fatal(err)
	}
	missingNameRes.Body.Close()
	if missingNameRes.StatusCode < 300 {
		t.Fatalf("GetQueueUrl accepted QueueUrl without QueueName: %d", missingNameRes.StatusCode)
	}
	listed := jsonCall("ListQueues", `{}`)
	if len(asSlice(listed["QueueUrls"])) == 0 {
		t.Fatalf("list %v", listed)
	}
	queryListReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/?Action=ListQueues&Version=2012-11-05", nil)
	queryListReq.Header.Set("Authorization", auth)
	queryListReq.Header.Set("Accept", "application/json")
	queryListRes, err := http.DefaultClient.Do(queryListReq)
	if err != nil {
		t.Fatal(err)
	}
	queryListBody, _ := io.ReadAll(queryListRes.Body)
	queryListRes.Body.Close()
	var queryList map[string]any
	if json.Unmarshal(queryListBody, &queryList) != nil || queryListRes.StatusCode != http.StatusOK || queryList["ListQueuesResponse"] == nil {
		t.Fatalf("query JSON list %d %s", queryListRes.StatusCode, queryListBody)
	}
	jsonCall("SetQueueAttributes", `{"QueueName":"q","Attributes":{"DelaySeconds":"0"}}`)
	attrs := jsonCall("GetQueueAttributes", `{"QueueName":"q","AttributeNames":["DelaySeconds","VisibilityTimeout"]}`)
	if asM(attrs["Attributes"])["DelaySeconds"] == nil && asM(attrs["Attributes"])["VisibilityTimeout"] == nil {
		t.Fatalf("attrs %v", attrs)
	}
	jsonCall("TagQueue", `{"QueueName":"q","Tags":{"k":"v"}}`)
	tags := jsonCall("ListQueueTags", `{"QueueName":"q"}`)
	if asM(tags["Tags"])["k"] != "v" {
		t.Fatalf("tags %v", tags)
	}
	jsonCall("UntagQueue", `{"QueueName":"q","TagKeys":["k"]}`)
	jsonCall("SendMessageBatch", `{"QueueName":"q","Entries":[{"Id":"1","MessageBody":"b1"},{"Id":"2","MessageBody":"b2"}]}`)
	recv := jsonCall("ReceiveMessage", `{"QueueName":"q","MaxNumberOfMessages":2,"WaitTimeSeconds":0,"VisibilityTimeout":0,"MessageAttributeNames":["All"]}`)
	if len(asSlice(recv["Messages"])) == 0 {
		t.Fatalf("batch recv %v", recv)
	}
	jsonCall("SendMessage", `{"QueueName":"q","MessageBody":"vis","MessageAttributes":{"color":{"DataType":"String","StringValue":"red"}}}`)
	one := jsonCall("ReceiveMessage", `{"QueueName":"q","WaitTimeSeconds":0,"VisibilityTimeout":30,"MessageAttributeNames":["All"]}`)
	if len(asSlice(one["Messages"])) != 1 {
		t.Fatalf("one %v", one)
	}
	rh := str(asM(asSlice(one["Messages"])[0])["ReceiptHandle"])
	jsonCall("ChangeMessageVisibility", `{"QueueName":"q","ReceiptHandle":"`+rh+`","VisibilityTimeout":"0"}`)
	jsonCall("ChangeMessageVisibilityBatch", `{"QueueName":"q","Entries":[{"Id":"1","ReceiptHandle":"`+rh+`","VisibilityTimeout":"0"}]}`)
	jsonCall("DeleteMessage", `{"QueueName":"q","ReceiptHandle":"`+rh+`"}`)
	jsonCall("SendMessage", `{"QueueName":"q","MessageBody":"delbatch"}`)
	got := jsonCall("ReceiveMessage", `{"QueueName":"q","WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	rh2 := str(asM(asSlice(got["Messages"])[0])["ReceiptHandle"])
	jsonCall("DeleteMessageBatch", `{"QueueName":"q","Entries":[{"Id":"x","ReceiptHandle":"`+rh2+`"}]}`)
	jsonCall("SendMessage", `{"QueueName":"q","MessageBody":"purge-me"}`)
	jsonCall("PurgeQueue", `{"QueueName":"q"}`)
	empty := jsonCall("ReceiveMessage", `{"QueueName":"q","WaitTimeSeconds":0}`)
	if len(asSlice(empty["Messages"])) != 0 {
		t.Fatalf("purge left %v", empty)
	}

	jsonCall("CreateQueue", `{"QueueName":"f.fifo","Attributes":{"FifoQueue":"true","ContentBasedDeduplication":"true"}}`)
	jsonCall("SendMessage", `{"QueueName":"f.fifo","MessageBody":"g1a","MessageGroupId":"g1","MessageDeduplicationId":"d1"}`)
	jsonCall("SendMessage", `{"QueueName":"f.fifo","MessageBody":"g1a","MessageGroupId":"g1","MessageDeduplicationId":"d1"}`)
	jsonCall("SendMessage", `{"QueueName":"f.fifo","MessageBody":"g2a","MessageGroupId":"g2","MessageDeduplicationId":"d2"}`)
	fifo := jsonCall("ReceiveMessage", `{"QueueName":"f.fifo","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	if len(asSlice(fifo["Messages"])) != 2 {
		t.Fatalf("fifo %v", fifo)
	}
	fifoReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/000000000000/f.fifo?Action=GetQueueAttributes&AttributeName.1=All", nil)
	fifoReq.Header.Set("Authorization", auth)
	fifoRes, err := http.DefaultClient.Do(fifoReq)
	if err != nil {
		t.Fatal(err)
	}
	fifoBody, _ := io.ReadAll(fifoRes.Body)
	fifoRes.Body.Close()
	if fifoRes.StatusCode != http.StatusOK || !strings.Contains(string(fifoBody), "<Name>FifoQueue</Name><Value>true</Value>") || !strings.Contains(string(fifoBody), "f.fifo") {
		t.Fatalf("fifo query attributes %d %s", fifoRes.StatusCode, fifoBody)
	}

	jsonCall("CreateQueue", `{"QueueName":"src","Attributes":{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:dlq\",\"maxReceiveCount\":\"1\"}","VisibilityTimeout":"0"}}`)
	jsonCall("SendMessage", `{"QueueName":"src","MessageBody":"poison"}`)
	jsonCall("ReceiveMessage", `{"QueueName":"src","WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	jsonCall("ReceiveMessage", `{"QueueName":"src","WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	dlq := jsonCall("ReceiveMessage", `{"QueueName":"dlq","WaitTimeSeconds":0}`)
	if len(asSlice(dlq["Messages"])) != 1 {
		t.Fatalf("dlq http %v", dlq)
	}
	srcs := jsonCall("ListDeadLetterSourceQueues", `{"QueueName":"dlq"}`)
	joined := fmtJSON(srcs["QueueUrls"])
	if !strings.Contains(joined, "src") {
		t.Fatalf("dlq sources %v", srcs)
	}
	jsonCall("AddPermission", `{"QueueName":"src","Label":"allow-send","AWSAccountIds":["111111111111"],"Actions":["SendMessage"]}`)
	pol := jsonCall("GetQueueAttributes", `{"QueueName":"src","AttributeNames":["Policy"]}`)
	if !strings.Contains(fmtJSON(pol["Attributes"]), "allow-send") {
		t.Fatalf("policy %v", pol)
	}
	jsonCall("RemovePermission", `{"QueueName":"src","Label":"allow-send"}`)
	pol2 := jsonCall("GetQueueAttributes", `{"QueueName":"src","AttributeNames":["Policy"]}`)
	if strings.Contains(fmtJSON(pol2["Attributes"]), "allow-send") {
		t.Fatalf("policy still labeled %v", pol2)
	}
	moved := jsonCall("StartMessageMoveTask", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:dlq","DestinationArn":"arn:aws:sqs:us-east-1:000000000000:src"}`)
	th := str(moved["TaskHandle"])
	if th == "" {
		t.Fatalf("move %v", moved)
	}
	back := jsonCall("ReceiveMessage", `{"QueueName":"src","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	foundPoison := false
	for _, m := range asSlice(back["Messages"]) {
		if str(asM(m)["Body"]) == "poison" {
			foundPoison = true
			break
		}
	}
	if !foundPoison {
		t.Fatalf("moved body missing poison %v", back)
	}
	listedMoves := jsonCall("ListMessageMoveTasks", `{"SourceArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`)
	if len(asSlice(listedMoves["Results"])) == 0 {
		t.Fatalf("list moves %v", listedMoves)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"TaskHandle":"`+th+`"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CancelMessageMoveTask")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cb, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode < 300 {
		t.Fatalf("cancel completed task succeeded %s", cb)
	}
	if res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("cancel fidelity %q", res.Header.Get("x-mirror-fidelity"))
	}

	queryCall := func(vals url.Values) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		qb, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(qb), res.Header
	}
	if code, body, h := queryCall(url.Values{"Action": {"CreateQueue"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}}); code >= 300 {
		t.Fatalf("query create %d %s", code, body)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("query create fidelity %q", h.Get("x-mirror-fidelity"))
	}
	getReq, _ := http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq?Action=GetQueueAttributes&AttributeName.1=All", nil)
	getReq.Header.Set("Authorization", auth)
	getRes, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ := io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusOK || !strings.Contains(string(getBody), "<Name>QueueArn</Name>") || !strings.Contains(string(getBody), "<Name>VisibilityTimeout</Name>") {
		t.Fatalf("query URL attributes %d %s", getRes.StatusCode, getBody)
	}
	getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq?Action=GetQueueAttributes&AttributeName.1=QueueArn", nil)
	getReq.Header.Set("Authorization", auth)
	getRes, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ = io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusOK || !strings.Contains(string(getBody), "<Name>QueueArn</Name>") || strings.Contains(string(getBody), "<Name>VisibilityTimeout</Name>") {
		t.Fatalf("query URL selected attributes %d %s", getRes.StatusCode, getBody)
	}
	getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq?Action=GetQueueAttributes&AttributeName.1=All", nil)
	getReq.Header.Set("Authorization", auth)
	getReq.Header.Set("Accept", "application/json")
	getRes, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ = io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusOK || getRes.Header.Get("Content-Type") != "application/json" || !strings.Contains(string(getBody), `"GetQueueAttributesResponse"`) || !strings.Contains(string(getBody), `"Name":"QueueArn"`) {
		t.Fatalf("query JSON attributes %d %q %s", getRes.StatusCode, getRes.Header.Get("Content-Type"), getBody)
	}
	getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq", nil)
	getReq.Header.Set("Authorization", auth)
	getReq.Header.Set("Accept", "application/json")
	getRes, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ = io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusNotFound || !strings.Contains(string(getBody), "<UnknownOperationException") {
		t.Fatalf("query URL missing action %d %s", getRes.StatusCode, getBody)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"CreateQueue"}, "Version": {"2012-11-05"}, "QueueName": {"queryq2"}}); code != http.StatusOK {
		t.Fatalf("query second create %d %s", code, body)
	}
	for _, tc := range []struct {
		name, want, reject string
	}{
		{"queryq", "queryq", ""},
		{"queryq2", "queryq2", "queryq"},
	} {
		getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq?Action=GetQueueUrl&QueueName="+tc.name+"&QueueOwnerAWSAccountId=000000000000", nil)
		getReq.Header.Set("Authorization", auth)
		getRes, err = http.DefaultClient.Do(getReq)
		if err != nil {
			t.Fatal(err)
		}
		getBody, _ = io.ReadAll(getRes.Body)
		getRes.Body.Close()
		if getRes.StatusCode != http.StatusOK || !strings.Contains(string(getBody), "/"+tc.want+"</") || (tc.reject != "" && strings.Contains(string(getBody), "/"+tc.reject+"</")) {
			t.Fatalf("query URL lookup %s %d %s", tc.name, getRes.StatusCode, getBody)
		}
	}
	getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq2?Action=DeleteQueue", nil)
	getReq.Header.Set("Authorization", auth)
	getRes, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ = io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusOK || !strings.Contains(string(getBody), "<DeleteQueueResponse") {
		t.Fatalf("query URL delete %d %s", getRes.StatusCode, getBody)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"GetQueueAttributes"}, "Version": {"2012-11-05"}, "QueueName": {"missing-query-queue"}, "AttributeName.1": {"All"}}); code != http.StatusBadRequest || !strings.Contains(body, "AWS.SimpleQueueService.NonExistentQueue") || !strings.Contains(body, "for this wsdl version") {
		t.Fatalf("query missing queue %d %s", code, body)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"GetQueueAttributes"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}, "AttributeName.1": {"Foobar"}}); code != http.StatusBadRequest || !strings.Contains(body, "InvalidAttributeName") || !strings.Contains(body, "Unknown Attribute Foobar.") {
		t.Fatalf("query invalid attribute %d %s", code, body)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"SendMessage"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}, "MessageBody": {"hello-query-wire"}}); code >= 300 {
		t.Fatalf("query send %d %s", code, body)
	}
	code, qrecv, qh := queryCall(url.Values{"Action": {"ReceiveMessage"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}, "WaitTimeSeconds": {"0"}, "VisibilityTimeout": {"0"}})
	if code >= 300 {
		t.Fatalf("query recv %d %s", code, qrecv)
	}
	if qh.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("query recv fidelity %q", qh.Get("x-mirror-fidelity"))
	}
	if !strings.Contains(qrecv, "hello-query-wire") {
		t.Fatalf("query recv body %s", qrecv)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"SendMessage"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}, "MessageBody": {"json-query-wire"}}); code >= 300 {
		t.Fatalf("query JSON seed %d %s", code, body)
	}
	getReq, _ = http.NewRequest(http.MethodGet, ts.URL+"/000000000000/queryq?Action=ReceiveMessage&VisibilityTimeout=0", nil)
	getReq.Header.Set("Authorization", auth)
	getReq.Header.Set("Accept", "application/json")
	getRes, err = http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	getBody, _ = io.ReadAll(getRes.Body)
	getRes.Body.Close()
	if getRes.StatusCode != http.StatusOK || getRes.Header.Get("Content-Type") != "application/json" || !strings.Contains(string(getBody), `"ReceiveMessageResponse"`) || !strings.Contains(string(getBody), `"Message":{"Body":"`) {
		t.Fatalf("query JSON receive %d %q %s", getRes.StatusCode, getRes.Header.Get("Content-Type"), getBody)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"DeleteMessage"}, "Version": {"2012-11-05"}, "QueueName": {"queryq"}, "ReceiptHandle": {"nope"}}); code != http.StatusBadRequest || !strings.Contains(body, "ReceiptHandleIsInvalid") {
		t.Fatalf("query delete validation %d %s", code, body)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"ReceiveMessage"}, "Version": {"2012-11-05"}, "QueueName": {"f.fifo"}, "WaitTimeSeconds": {"0"}}); code >= 300 {
		t.Fatalf("query fifo recv %d %s", code, body)
	}

	jsonCall("CreateQueue", `{"QueueName":"waitq"}`)
	done := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"QueueName":"waitq","WaitTimeSeconds":1}`))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS.ReceiveMessage")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- -1
			return
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		done <- res.StatusCode
	}()
	for i := 0; i < 1000000; i++ {
		select {
		case code := <-done:
			if code != 200 {
				t.Fatalf("long poll http %d", code)
			}
			goto polled
		default:
			runtime.Gosched()
			_ = rt.Deps.Clock.Advance(time.Second)
		}
	}
	t.Fatal("long poll http did not return")
polled:
	jsonCall("DeleteQueue", `{"QueueName":"waitq"}`)
	jsonCall("CreateQueue", `{"QueueName":"permq"}`)
	if code, body, h := queryCall(url.Values{
		"Action": {"AddPermission"}, "Version": {"2012-11-05"}, "QueueName": {"permq"},
		"Label": {"qlabel"}, "AWSAccountId.1": {"111111111111"}, "ActionName.1": {"SendMessage"},
	}); code >= 300 {
		t.Fatalf("query AddPermission %d %s", code, body)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("query AddPermission fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, body, _ := queryCall(url.Values{"Action": {"GetQueueAttributes"}, "Version": {"2012-11-05"}, "QueueName": {"permq"}, "AttributeName.1": {"Policy"}}); code >= 300 || !strings.Contains(body, "qlabel") {
		t.Fatalf("query policy %d %s", code, body)
	}
	if code, body, _ := queryCall(url.Values{"Action": {"RemovePermission"}, "Version": {"2012-11-05"}, "QueueName": {"permq"}, "Label": {"qlabel"}}); code >= 300 {
		t.Fatalf("query RemovePermission %d %s", code, body)
	}
}

func asSlice(v any) []any {
	s, _ := v.([]any)
	return s
}
