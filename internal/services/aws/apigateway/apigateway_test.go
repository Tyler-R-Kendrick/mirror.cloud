package apigateway

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
)

func TestBootedServerAPIGatewayLambdaProxy(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.apigateway", "aws.lambda"}
	cfg.Seed = "apigw-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	authGW := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/apigateway/aws4_request, SignedHeaders=host, Signature=00"
	authLam := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=00"

	src := "def lambda_handler(event, context):\n    import json\n    b=event.get('body') or '{}'\n    if isinstance(b,str):\n        b=json.loads(b or '{}')\n    return {'echo': b.get('n', b)}\n"
	pyb64 := base64.StdEncoding.EncodeToString([]byte(src))
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/2015-03-31/functions", strings.NewReader(`{"FunctionName":"echo","Runtime":"python3.12","Handler":"lambda_function.lambda_handler","Code":{"ZipFile":"`+pyb64+`"}}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authLam)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create fn %d %s", res.StatusCode, raw)
	}

	doGW := func(method, path, body string) (int, []byte) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", authGW)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s %d %s", method, path, res.StatusCode, b)
		}
		return res.StatusCode, b
	}
	_, created := doGW(http.MethodPost, "/restapis", `{"name":"demo"}`)
	var api map[string]any
	_ = json.Unmarshal(created, &api)
	id, _ := api["id"].(string)
	root, _ := api["rootResourceId"].(string)
	if id == "" || root == "" {
		t.Fatalf("api %v", api)
	}
	uri := "arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:000000000000:function:echo/invocations"
	doGW(http.MethodPut, "/restapis/"+id+"/resources/"+root+"/methods/POST", `{"authorizationType":"NONE"}`)
	doGW(http.MethodPut, "/restapis/"+id+"/resources/"+root+"/methods/POST/integration", `{"type":"AWS_PROXY","uri":"`+uri+`","httpMethod":"POST"}`)
	doGW(http.MethodPost, "/restapis/"+id+"/deployments", `{"stageName":"prod"}`)

	req, _ = http.NewRequest(http.MethodPost, ts.URL+"/restapis/"+id+"/prod/_user_request_/", strings.NewReader(`{"n":9}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authGW)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || !strings.Contains(string(out), "echo") {
		t.Fatalf("execute %d %s", res.StatusCode, out)
	}
}

func TestBootedServerAPIGatewayRemainder(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.apigateway"}
	cfg.Seed = "apigw-2"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/apigateway/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string, want ...string) string {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s %d %s", method, path, res.StatusCode, b)
		}
		s := string(b)
		for _, w := range want {
			if !strings.Contains(s, w) {
				t.Fatalf("%s %s missing %q in %s", method, path, w, s)
			}
		}
		return s
	}
	created := do(http.MethodPost, "/restapis", `{"name":"demo"}`, "id")
	var api map[string]any
	_ = json.Unmarshal([]byte(created), &api)
	id, _ := api["id"].(string)
	root, _ := api["rootResourceId"].(string)
	do(http.MethodGet, "/restapis/"+id, "", "demo")
	do(http.MethodGet, "/restapis", "", "demo")
	do(http.MethodPatch, "/restapis/"+id, `{"name":"demo2"}`, "demo2")
	resBody := do(http.MethodPost, "/restapis/"+id+"/resources/"+root, `{"pathPart":"pets"}`, "pets")
	var res map[string]any
	_ = json.Unmarshal([]byte(resBody), &res)
	rid, _ := res["id"].(string)
	do(http.MethodGet, "/restapis/"+id+"/resources/"+rid, "", "pets")
	do(http.MethodGet, "/restapis/"+id+"/resources", "", "pets")
	do(http.MethodPut, "/restapis/"+id+"/resources/"+rid+"/methods/GET", `{"authorizationType":"NONE"}`)
	do(http.MethodGet, "/restapis/"+id+"/resources/"+rid+"/methods/GET", "", "NONE")
	do(http.MethodPut, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integration", `{"type":"MOCK","httpMethod":"GET"}`)
	do(http.MethodGet, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integration", "", "MOCK")
	do(http.MethodPut, "/restapis/"+id+"/resources/"+rid+"/methods/GET/methodresponses/200", `{"statusCode":"200"}`, "200")
	do(http.MethodGet, "/restapis/"+id+"/resources/"+rid+"/methods/GET/methodresponses/200", "", "200")
	do(http.MethodPut, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integrationresponses/200", `{"statusCode":"200"}`, "200")
	do(http.MethodGet, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integrationresponses/200", "", "200")
	dep := do(http.MethodPost, "/restapis/"+id+"/deployments", `{"stageName":"prod"}`, "id")
	var d map[string]any
	_ = json.Unmarshal([]byte(dep), &d)
	did, _ := d["id"].(string)
	do(http.MethodGet, "/restapis/"+id+"/deployments/"+did, "", did)
	do(http.MethodGet, "/restapis/"+id+"/deployments", "", did)
	do(http.MethodGet, "/restapis/"+id+"/stages/prod", "", "prod")
	do(http.MethodPost, "/restapis/"+id+"/stages", `{"stageName":"dev","deploymentId":"`+did+`"}`, "dev")
	do(http.MethodPatch, "/restapis/"+id+"/stages/prod", `{"deploymentId":"`+did+`"}`)
	authz := do(http.MethodPost, "/restapis/"+id+"/authorizers", `{"name":"a1","type":"TOKEN"}`, "a1")
	var az map[string]any
	_ = json.Unmarshal([]byte(authz), &az)
	azid, _ := az["id"].(string)
	do(http.MethodGet, "/restapis/"+id+"/authorizers/"+azid, "", "a1")
	do(http.MethodGet, "/restapis/"+id+"/authorizers", "", "a1")
	do(http.MethodDelete, "/restapis/"+id+"/authorizers/"+azid, "")
	key := do(http.MethodPost, "/apikeys", `{"name":"k1"}`, "k1")
	var km map[string]any
	_ = json.Unmarshal([]byte(key), &km)
	kid, _ := km["id"].(string)
	do(http.MethodGet, "/apikeys/"+kid, "", "k1")
	do(http.MethodGet, "/apikeys", "", "k1")
	do(http.MethodDelete, "/apikeys/"+kid, "")
	up := do(http.MethodPost, "/usageplans", `{"name":"u1"}`, "u1")
	var um map[string]any
	_ = json.Unmarshal([]byte(up), &um)
	uid, _ := um["id"].(string)
	do(http.MethodGet, "/usageplans/"+uid, "", "u1")
	do(http.MethodGet, "/usageplans", "", "u1")
	do(http.MethodDelete, "/usageplans/"+uid, "")
	do(http.MethodDelete, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integrationresponses/200", "")
	do(http.MethodDelete, "/restapis/"+id+"/resources/"+rid+"/methods/GET/methodresponses/200", "")
	do(http.MethodDelete, "/restapis/"+id+"/resources/"+rid+"/methods/GET/integration", "")
	do(http.MethodDelete, "/restapis/"+id+"/resources/"+rid+"/methods/GET", "")
	do(http.MethodDelete, "/restapis/"+id+"/stages/prod", "")
	do(http.MethodDelete, "/restapis/"+id+"/deployments/"+did, "")
	do(http.MethodDelete, "/restapis/"+id+"/resources/"+rid, "")
	do(http.MethodDelete, "/restapis/"+id, "")
	lreq, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action=CreateDomainName", strings.NewReader(`{"domainName":"ex.com"}`))
	lreq.Header.Set("Content-Type", "application/json")
	lreq.Header.Set("Authorization", auth)
	lres, err := http.DefaultClient.Do(lreq)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(lres.Body)
	lres.Body.Close()
	if lres.StatusCode >= 300 || lres.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("CreateDomainName %d %s %s", lres.StatusCode, lres.Header.Get("x-mirror-fidelity"), raw)
	}
	if !strings.Contains(string(raw), "ex.com") {
		t.Fatalf("create domain %s", raw)
	}
	gotDN := do(http.MethodPost, "/?Action=GetDomainName", `{"domainName":"ex.com"}`, "ex.com")
	if !strings.Contains(gotDN, "ex.com") {
		t.Fatalf("get domain %s", gotDN)
	}
	do(http.MethodPost, "/?Action=DeleteDomainName", `{"domainName":"ex.com"}`)
	gReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action=GetDomainName", strings.NewReader(`{"domainName":"ex.com"}`))
	gReq.Header.Set("Content-Type", "application/json")
	gReq.Header.Set("Authorization", auth)
	gRes, gErr := http.DefaultClient.Do(gReq)
	if gErr != nil {
		t.Fatal(gErr)
	}
	miss, _ := io.ReadAll(gRes.Body)
	gRes.Body.Close()
	if gRes.StatusCode < 300 && strings.Contains(string(miss), `"domainName":"ex.com"`) {
		t.Fatalf("domain still present %s", miss)
	}
	for _, op := range extraOps() {
		er, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(`{"domainName":"ex.com","name":"n","id":"i1","restApiId":"r"}`))
		er.Header.Set("Content-Type", "application/json")
		er.Header.Set("Authorization", auth)
		eres, err := http.DefaultClient.Do(er)
		if err != nil {
			t.Fatal(err)
		}
		eb, _ := io.ReadAll(eres.Body)
		eres.Body.Close()
		if eres.Header.Get("x-mirror-fidelity") != "emulate" && eres.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, eres.StatusCode, eb)
		}
	}
}

func TestAPIGatewayHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 42+len(extraOps()) {
		t.Fatalf("apigateway Operations() %d want %d", n, 42+len(extraOps()))
	}
}
