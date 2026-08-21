package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sns"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestBootedServerSNSSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sns", "aws.sqs"}
	cfg.Seed = "sns-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sns/aws4_request, SignedHeaders=host, Signature=00"
	sqsAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"

	formCall := func(vals url.Values) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, string(b), res.Header
	}
	sqsJSON := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
		req.Header.Set("Authorization", sqsAuth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("sqs %s %d %s", op, res.StatusCode, raw)
		}
		return out
	}

	code, body, h := formCall(url.Values{"Action": {"CreateTopic"}, "Name": {"t"}})
	if code >= 300 || !strings.Contains(body, "TopicArn") {
		t.Fatalf("create %d %s", code, body)
	}
	if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("create fidelity %q", h.Get("x-mirror-fidelity"))
	}
	arnStart := strings.Index(body, "arn:aws:sns:")
	arnEnd := strings.Index(body[arnStart:], "<")
	arn := body[arnStart : arnStart+arnEnd]

	formCall(url.Values{"Action": {"SetTopicAttributes"}, "TopicArn": {arn}, "AttributeName": {"DisplayName"}, "AttributeValue": {"n"}})
	code, gat, _ := formCall(url.Values{"Action": {"GetTopicAttributes"}, "TopicArn": {arn}})
	if code >= 300 || !strings.Contains(gat, "DisplayName") {
		t.Fatalf("get attrs %s", gat)
	}
	code, lt, _ := formCall(url.Values{"Action": {"ListTopics"}})
	if !strings.Contains(lt, arn) {
		t.Fatalf("list topics %s", lt)
	}

	sqsJSON("CreateQueue", `{"QueueName":"q"}`)
	code, sub, _ := formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"q"}, "RawMessageDelivery": {"true"}})
	if code >= 300 || !strings.Contains(sub, "SubscriptionArn") {
		t.Fatalf("sub %d %s", code, sub)
	}
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"yes"}})
	formCall(url.Values{"Action": {"PublishBatch"}, "TopicArn": {arn}, "Message": {"batch"}})

	var notes []string
	var mu sync.Mutex
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		notes = append(notes, string(b))
		mu.Unlock()
		w.WriteHeader(200)
	}))
	defer hs.Close()
	code, pending, _ := formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"http"}, "Endpoint": {hs.URL}})
	if code >= 300 {
		t.Fatalf("http sub %d %s", code, pending)
	}
	token := ""
	mu.Lock()
	for _, n := range notes {
		var m map[string]any
		_ = json.Unmarshal([]byte(n), &m)
		if str(m["Type"]) == "SubscriptionConfirmation" {
			token = str(m["Token"])
		}
	}
	mu.Unlock()
	if token == "" {
		t.Fatalf("no confirmation POST %v", notes)
	}
	code, conf, _ := formCall(url.Values{"Action": {"ConfirmSubscription"}, "Token": {token}})
	if code >= 300 || !strings.Contains(conf, "SubscriptionArn") {
		t.Fatalf("confirm %d %s", code, conf)
	}
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"http-hi"}})
	mu.Lock()
	sawNote := false
	for _, n := range notes {
		if strings.Contains(n, "http-hi") {
			sawNote = true
		}
	}
	mu.Unlock()
	if !sawNote {
		t.Fatalf("no http notification %v", notes)
	}

	formCall(url.Values{"Action": {"ListSubscriptions"}})
	formCall(url.Values{"Action": {"ListSubscriptionsByTopic"}, "TopicArn": {arn}})
	formCall(url.Values{"Action": {"TagResource"}, "ResourceArn": {arn}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}})
	formCall(url.Values{"Action": {"UntagResource"}, "ResourceArn": {arn}})

	recv := sqsJSON("ReceiveMessage", `{"QueueName":"q","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	msgs := asSlice(recv["Messages"])
	if len(msgs) == 0 {
		t.Fatalf("sns->sqs empty %v", recv)
	}
	rawBodies := map[string]bool{}
	for _, m := range msgs {
		rawBodies[str(asM(m)["Body"])] = true
	}
	if !rawBodies["yes"] && !rawBodies["batch"] {
		t.Fatalf("raw delivery missing published bodies %v", recv)
	}

	sqsJSON("CreateQueue", `{"QueueName":"filt"}`)
	formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"filt"}, "FilterPolicy": {`{"event":["order"]}`}, "RawMessageDelivery": {"true"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"skip-other"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"other"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hit-exact"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"order"}})
	filt := sqsJSON("ReceiveMessage", `{"QueueName":"filt","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	if !recvHas(filt, "hit-exact") || recvHas(filt, "skip-other") {
		t.Fatalf("exact filter %v", filt)
	}

	sqsJSON("CreateQueue", `{"QueueName":"pref"}`)
	formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"pref"}, "FilterPolicy": {`{"event":[{"prefix":"ord"}]}`}, "RawMessageDelivery": {"true"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hit-prefix"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"order"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"skip-prefix"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"nope"}})
	pref := sqsJSON("ReceiveMessage", `{"QueueName":"pref","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	if !recvHas(pref, "hit-prefix") || recvHas(pref, "skip-prefix") {
		t.Fatalf("prefix filter %v", pref)
	}

	sqsJSON("CreateQueue", `{"QueueName":"numq"}`)
	formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"numq"}, "FilterPolicy": {`{"n":[{"numeric":[">",5]}]}`}, "RawMessageDelivery": {"true"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hit-num"}, "MessageAttributes.entry.1.Name": {"n"}, "MessageAttributes.entry.1.Value.StringValue": {"9"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"skip-num"}, "MessageAttributes.entry.1.Name": {"n"}, "MessageAttributes.entry.1.Value.StringValue": {"1"}})
	numq := sqsJSON("ReceiveMessage", `{"QueueName":"numq","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	if !recvHas(numq, "hit-num") || recvHas(numq, "skip-num") {
		t.Fatalf("numeric filter %v", numq)
	}

	sqsJSON("CreateQueue", `{"QueueName":"abq"}`)
	formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"abq"}, "FilterPolicy": {`{"event":[{"anything-but":"skip"}]}`}, "RawMessageDelivery": {"true"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"hit-ab"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"keep"}})
	formCall(url.Values{"Action": {"Publish"}, "TopicArn": {arn}, "Message": {"skip-ab"}, "MessageAttributes.entry.1.Name": {"event"}, "MessageAttributes.entry.1.Value.StringValue": {"skip"}})
	abq := sqsJSON("ReceiveMessage", `{"QueueName":"abq","MaxNumberOfMessages":10,"WaitTimeSeconds":0,"VisibilityTimeout":0}`)
	if !recvHas(abq, "hit-ab") || recvHas(abq, "skip-ab") {
		t.Fatalf("anything-but filter %v", abq)
	}

	code, lam, _ := formCall(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"lambda"}, "Endpoint": {"fn"}})
	if code != 501 && !strings.Contains(lam, "MirrorNotImplemented") && !strings.Contains(lam, "not implemented") {
		t.Fatalf("lambda %d %s", code, lam)
	}

	formCall(url.Values{"Action": {"Unsubscribe"}, "SubscriptionArn": {"gone"}})
	formCall(url.Values{"Action": {"DeleteTopic"}, "TopicArn": {arn}})
}

func TestBootedServerSNSExtraEngines(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sns", "aws.sqs"}
	cfg.Seed = "sns-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sns/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) (int, string, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q", vals.Get("Action"), res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, string(b), res.Header
	}
	_, created, _ := call(url.Values{"Action": {"CreateTopic"}, "Name": {"t"}})
	arn := ""
	if i := strings.Index(created, "arn:aws:sns:"); i >= 0 {
		rest := created[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			arn = rest[:j]
		}
	}
	if arn == "" {
		t.Fatalf("no arn %s", created)
	}
	sqsAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"
	sqsReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"QueueName":"q"}`))
	sqsReq.Header.Set("Content-Type", "application/x-amz-json-1.0")
	sqsReq.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	sqsReq.Header.Set("Authorization", sqsAuth)
	sqsRes, err := http.DefaultClient.Do(sqsReq)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, sqsRes.Body)
	sqsRes.Body.Close()
	_, subBody, _ := call(url.Values{"Action": {"Subscribe"}, "TopicArn": {arn}, "Protocol": {"sqs"}, "Endpoint": {"q"}})
	sub := ""
	if i := strings.Index(subBody, "arn:aws:sns:"); i >= 0 {
		rest := subBody[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			sub = rest[:j]
		}
	}
	call(url.Values{"Action": {"SetSubscriptionAttributes"}, "SubscriptionArn": {sub}, "AttributeName": {"RawMessageDelivery"}, "AttributeValue": {"true"}})
	_, ga, _ := call(url.Values{"Action": {"GetSubscriptionAttributes"}, "SubscriptionArn": {sub}})
	if !strings.Contains(ga, "RawMessageDelivery") {
		t.Fatalf("sub attrs %s", ga)
	}
	call(url.Values{"Action": {"AddPermission"}, "TopicArn": {arn}, "Label": {"allow-pub"}, "AWSAccountId.1": {"111111111111"}, "ActionName.1": {"Publish"}})
	_, pol, _ := call(url.Values{"Action": {"GetTopicAttributes"}, "TopicArn": {arn}})
	if !strings.Contains(pol, "allow-pub") {
		t.Fatalf("policy %s", pol)
	}
	call(url.Values{"Action": {"RemovePermission"}, "TopicArn": {arn}, "Label": {"allow-pub"}})
	_, pol2, _ := call(url.Values{"Action": {"GetTopicAttributes"}, "TopicArn": {arn}})
	if strings.Contains(pol2, "allow-pub") {
		t.Fatalf("policy leftover %s", pol2)
	}
	call(url.Values{"Action": {"TagResource"}, "ResourceArn": {arn}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}})
	_, tags, _ := call(url.Values{"Action": {"ListTagsForResource"}, "ResourceArn": {arn}})
	if !strings.Contains(tags, "k") && !strings.Contains(tags, "v") {
		t.Fatalf("tags %s", tags)
	}
	call(url.Values{"Action": {"PutDataProtectionPolicy"}, "ResourceArn": {arn}, "DataProtectionPolicy": {`{"Name":"p"}`}})
	_, dpp, _ := call(url.Values{"Action": {"GetDataProtectionPolicy"}, "ResourceArn": {arn}})
	if !strings.Contains(dpp, "Name") {
		t.Fatalf("dpp %s", dpp)
	}
	_, pa, _ := call(url.Values{"Action": {"CreatePlatformApplication"}, "Name": {"pa"}, "Platform": {"GCM"}})
	app := ""
	if i := strings.Index(pa, "arn:aws:sns:"); i >= 0 {
		rest := pa[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			app = rest[:j]
		}
	}
	if app == "" {
		t.Fatalf("app arn %s", pa)
	}
	_, listed, _ := call(url.Values{"Action": {"ListPlatformApplications"}})
	if !strings.Contains(listed, app) {
		t.Fatalf("list apps %s", listed)
	}
	call(url.Values{"Action": {"SetPlatformApplicationAttributes"}, "PlatformApplicationArn": {app}, "Attributes.entry.1.key": {"Enabled"}, "Attributes.entry.1.value": {"true"}})
	call(url.Values{"Action": {"GetPlatformApplicationAttributes"}, "PlatformApplicationArn": {app}})
	_, ep, _ := call(url.Values{"Action": {"CreatePlatformEndpoint"}, "PlatformApplicationArn": {app}, "Token": {"devtok"}})
	endp := ""
	if i := strings.Index(ep, "arn:aws:sns:"); i >= 0 {
		rest := ep[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			endp = rest[:j]
		}
	}
	_, ea, _ := call(url.Values{"Action": {"GetEndpointAttributes"}, "EndpointArn": {endp}})
	if !strings.Contains(ea, "devtok") {
		t.Fatalf("endpoint %s", ea)
	}
	call(url.Values{"Action": {"SetEndpointAttributes"}, "EndpointArn": {endp}, "Attributes.entry.1.key": {"Enabled"}, "Attributes.entry.1.value": {"false"}})
	_, leps, _ := call(url.Values{"Action": {"ListEndpointsByPlatformApplication"}, "PlatformApplicationArn": {app}})
	if !strings.Contains(leps, endp) {
		t.Fatalf("list endpoints %s", leps)
	}
	call(url.Values{"Action": {"DeleteEndpoint"}, "EndpointArn": {endp}})
	call(url.Values{"Action": {"DeletePlatformApplication"}, "PlatformApplicationArn": {app}})
	call(url.Values{"Action": {"SetSMSAttributes"}, "Attributes.entry.1.key": {"DefaultSenderID"}, "Attributes.entry.1.value": {"MIRROR"}})
	_, sms, _ := call(url.Values{"Action": {"GetSMSAttributes"}})
	if !strings.Contains(sms, "MIRROR") && !strings.Contains(sms, "DefaultSenderID") {
		t.Fatalf("sms attrs %s", sms)
	}
	_, chk, _ := call(url.Values{"Action": {"CheckIfPhoneNumberIsOptedOut"}, "phoneNumber": {"+15555550100"}})
	if strings.Contains(strings.ToLower(chk), "true") && strings.Contains(chk, "isOptedOut") {
		if strings.Contains(chk, ">true<") {
			t.Fatalf("unexpected opt-out %s", chk)
		}
	}
	call(url.Values{"Action": {"OptInPhoneNumber"}, "phoneNumber": {"+15555550100"}})
	call(url.Values{"Action": {"ListPhoneNumbersOptedOut"}})
	call(url.Values{"Action": {"CreateSMSSandboxPhoneNumber"}, "PhoneNumber": {"+15555550100"}})
	call(url.Values{"Action": {"VerifySMSSandboxPhoneNumber"}, "PhoneNumber": {"+15555550100"}, "OneTimePassword": {"123456"}})
	_, sb, _ := call(url.Values{"Action": {"ListSMSSandboxPhoneNumbers"}})
	if !strings.Contains(sb, "+15555550100") {
		t.Fatalf("sandbox list %s", sb)
	}
	_, st, _ := call(url.Values{"Action": {"GetSMSSandboxAccountStatus"}})
	if !strings.Contains(st, "IsInSandbox") && !strings.Contains(st, "true") {
		t.Fatalf("sandbox status %s", st)
	}
	call(url.Values{"Action": {"DeleteSMSSandboxPhoneNumber"}, "PhoneNumber": {"+15555550100"}})
	call(url.Values{"Action": {"ListOriginationNumbers"}})
}

func TestSNSHTTPProvenOps(t *testing.T) {
	want := []string{"CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "ConfirmSubscription", "Unsubscribe", "ListSubscriptions",
		"ListSubscriptionsByTopic", "Publish", "PublishBatch", "TagResource", "UntagResource",
		"AddPermission", "RemovePermission", "GetSubscriptionAttributes", "SetSubscriptionAttributes",
		"ListTagsForResource", "PutDataProtectionPolicy", "GetDataProtectionPolicy",
		"CreatePlatformApplication", "GetPlatformApplicationAttributes", "SetPlatformApplicationAttributes",
		"ListPlatformApplications", "DeletePlatformApplication",
		"CreatePlatformEndpoint", "GetEndpointAttributes", "SetEndpointAttributes",
		"ListEndpointsByPlatformApplication", "DeleteEndpoint",
		"SetSMSAttributes", "GetSMSAttributes",
		"OptInPhoneNumber", "CheckIfPhoneNumberIsOptedOut", "ListPhoneNumbersOptedOut",
		"CreateSMSSandboxPhoneNumber", "VerifySMSSandboxPhoneNumber", "DeleteSMSSandboxPhoneNumber",
		"ListSMSSandboxPhoneNumbers", "GetSMSSandboxAccountStatus", "ListOriginationNumbers"}
	assertSame(t, "sns", sns.New(spitest.Deps(t)).Operations(), want)
}

func recvHas(out map[string]any, body string) bool {
	for _, m := range asSlice(out["Messages"]) {
		if str(asM(m)["Body"]) == body {
			return true
		}
	}
	return false
}
