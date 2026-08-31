package edge_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
)

type recordingAuthorizer struct{ checks []string }

func s3Envelope(t testing.TB, response *http.Response) map[string]any {
	t.Helper()
	requestID := response.Header.Get("x-amz-request-id")
	hostID := response.Header.Get("x-amz-id-2")
	if requestID == "" || hostID == "" || requestID != response.Header.Get("x-mirror-request-id") || requestID == hostID {
		t.Fatalf("invalid S3 response identifiers: %#v", response.Header)
	}
	return map[string]any{
		"content_type":       response.Header.Get("Content-Type"),
		"has_host_id":        hostID != "",
		"has_request_id":     requestID != "",
		"request_correlated": requestID == response.Header.Get("x-mirror-request-id"),
		"status":             response.StatusCode,
	}
}

func (a *recordingAuthorizer) Authorize(_ context.Context, _ spi.Identity, _, operation, resource string) error {
	a.checks = append(a.checks, operation+" "+resource)
	return nil
}

func (a *recordingAuthorizer) AuthorizeRequest(_ context.Context, req *spi.Request, resource string) error {
	a.checks = append(a.checks, req.Operation+" "+resource)
	return nil
}

func TestS3CopyObjectAuthorizationChecks(t *testing.T) {
	deps := spitest.Deps(t)
	authorizer := &recordingAuthorizer{}
	deps.Authorizer = authorizer
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(path string, body []byte, headers map[string]string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, ts.URL+path, bytes.NewReader(body))
		req.Header.Set("Authorization", auth)
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			t.Fatalf("PUT %s: %s", path, resp.Status)
		}
	}
	do("/source", nil, nil)
	do("/destination", nil, nil)
	do("/source/key", []byte("body"), nil)
	authorizer.checks = nil
	do("/destination/copy", nil, map[string]string{"x-amz-copy-source": "/source/key"})
	want := "GetObject arn:aws:s3:::source/key\nPutObject arn:aws:s3:::destination/copy"
	if got := strings.Join(authorizer.checks, "\n"); got != want {
		t.Fatalf("copy checks:\n%s\nwant:\n%s", got, want)
	}
	authorizer.checks = nil
	do("/destination/tagged", nil, map[string]string{"x-amz-copy-source": "/source/key", "x-amz-tagging-directive": "REPLACE", "x-amz-tagging": "team=data"})
	if got := strings.Join(authorizer.checks, "\n"); !strings.HasSuffix(got, "PutObjectTagging arn:aws:s3:::destination/tagged") {
		t.Fatalf("replace-tagging checks:\n%s", got)
	}
}

func TestS3PutGetAndForeignService501(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := edge.New(cfg, deps, reg, "test")
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	mkb, err := http.NewRequest(http.MethodPut, ts.URL+"/mybucket", nil)
	if err != nil {
		t.Fatal(err)
	}
	mkb.Header.Set("Authorization", auth)
	bres, err := http.DefaultClient.Do(mkb)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, bres.Body)
	bres.Body.Close()
	if bres.StatusCode >= 300 {
		t.Fatalf("create bucket status %d", bres.StatusCode)
	}
	createEnvelope := s3Envelope(t, bres)

	head, _ := http.NewRequest(http.MethodHead, ts.URL+"/mybucket", nil)
	head.Header.Set("Authorization", auth)
	hres, err := http.DefaultClient.Do(head)
	if err != nil {
		t.Fatal(err)
	}
	hres.Body.Close()
	if hres.StatusCode != http.StatusOK || hres.Header.Get("Content-Type") != "application/xml" {
		t.Fatalf("head bucket status %d headers %#v", hres.StatusCode, hres.Header)
	}
	headEnvelope := s3Envelope(t, hres)

	put, err := http.NewRequest(http.MethodPut, ts.URL+"/mybucket/hello.txt", bytes.NewReader([]byte("hi")))
	if err != nil {
		t.Fatal(err)
	}
	put.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("put status %d", res.StatusCode)
	}
	putEnvelope := s3Envelope(t, res)

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/mybucket/hello.txt", nil)
	get.Header.Set("Authorization", put.Header.Get("Authorization"))
	gres, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gres.Body)
	gres.Body.Close()
	if gres.StatusCode != 200 {
		t.Fatalf("get status %d body %s", gres.StatusCode, body)
	}
	if string(body) != "hi" {
		t.Fatalf("got %q", body)
	}
	getEnvelope := s3Envelope(t, gres)

	missing, _ := http.NewRequest(http.MethodGet, ts.URL+"/mybucket/missing", nil)
	missing.Header.Set("Authorization", auth)
	mres, err := http.DefaultClient.Do(missing)
	if err != nil {
		t.Fatal(err)
	}
	missingBody, _ := io.ReadAll(mres.Body)
	mres.Body.Close()
	if mres.StatusCode != http.StatusNotFound || !bytes.Contains(missingBody, []byte("<Code>NoSuchKey</Code>")) {
		t.Fatalf("missing object status %d body %s", mres.StatusCode, missingBody)
	}
	missingEnvelope := s3Envelope(t, mres)

	ddb, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{}`))
	ddb.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
	ddb.Header.Set("Content-Type", "application/x-amz-json-1.0")
	dres, err := http.DefaultClient.Do(ddb)
	if err != nil {
		t.Fatal(err)
	}
	db, _ := io.ReadAll(dres.Body)
	dres.Body.Close()
	if dres.StatusCode != 501 {
		t.Fatalf("expected 501 for dynamodb on s3-only boot, got %d %s", dres.StatusCode, db)
	}
	if dres.Header.Get("x-mirror-not-implemented") == "" && !strings.Contains(string(db), "MirrorNotImplemented") {
		t.Fatalf("not a §4.11 error: %s %v", db, dres.Header)
	}

	unknown, _ := http.NewRequest(http.MethodPost, ts.URL+"/", nil)
	unknown.Header.Set("X-Amz-Target", "UnknownService.UnknownOperation")
	ures, err := http.DefaultClient.Do(unknown)
	if err != nil {
		t.Fatal(err)
	}
	ubody, _ := io.ReadAll(ures.Body)
	ures.Body.Close()
	if ures.StatusCode != http.StatusNotImplemented || !strings.Contains(string(ubody), "unknown service") {
		t.Fatalf("unknown service %d %s", ures.StatusCode, ubody)
	}
	golden.AssertJSON(t, map[string]any{"create": createEnvelope, "get": getEnvelope, "head": headEnvelope, "missing": missingEnvelope, "put": putEnvelope})
}

func TestS3PresignedExpiryFaultCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	results := map[string]any{}
	for name, target := range map[string]string{
		"sigv2": "/bucket/key?AWSAccessKeyId=AKIATEST&Expires=-1&Signature=00",
		"sigv4": "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIATEST%2F19691231%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=19691231T235900Z&X-Amz-Expires=30&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
		var fault struct {
			Code       string `xml:"Code"`
			Message    string `xml:"Message"`
			Expires    string `xml:"Expires"`
			ServerTime string `xml:"ServerTime"`
			AmzExpires string `xml:"X-Amz-Expires"`
			RequestID  string `xml:"RequestId"`
			HostID     string `xml:"HostId"`
		}
		if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
			t.Fatalf("%s decode: %v: %s", name, err, recorder.Body.String())
		}
		if recorder.Code != http.StatusForbidden || fault.Code != "AccessDenied" || fault.Message != "Request has expired" || fault.Expires == "" || fault.ServerTime == "" || fault.RequestID == "" || fault.HostID == "" {
			t.Fatalf("%s response: status=%d headers=%#v fault=%#v", name, recorder.Code, recorder.Header(), fault)
		}
		results[name] = map[string]any{
			"amz_expires":  fault.AmzExpires,
			"code":         fault.Code,
			"content_type": recorder.Header().Get("Content-Type"),
			"expires":      fault.Expires,
			"message":      fault.Message,
			"server_time":  fault.ServerTime,
			"status":       recorder.Code,
		}
	}
	golden.AssertJSON(t, results)
}

func TestS3PresignedAuthFaultCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	results := map[string]any{}
	for name, tc := range map[string]struct {
		target string
		status int
		code   string
	}{
		"sigv2":  {"/bucket/key?AWSAccessKeyId=test&Signature=00", http.StatusForbidden, "AccessDenied"},
		"sigv4":  {"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=00&X-Amz-Expires=60&X-Amz-SignedHeaders=host", http.StatusBadRequest, "AuthorizationQueryParametersError"},
		"sigv4a": {"/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=00", http.StatusBadRequest, "AuthorizationQueryParametersError"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.target, nil))
		var fault struct {
			Code      string `xml:"Code"`
			Message   string `xml:"Message"`
			RequestID string `xml:"RequestId"`
			HostID    string `xml:"HostId"`
		}
		if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
			t.Fatalf("%s decode: %v: %s", name, err, recorder.Body.String())
		}
		if recorder.Code != tc.status || fault.Code != tc.code || fault.Message == "" || fault.RequestID == "" || fault.HostID == "" {
			t.Fatalf("%s response: status=%d headers=%#v fault=%#v", name, recorder.Code, recorder.Header(), fault)
		}
		results[name] = map[string]any{"code": fault.Code, "content_type": recorder.Header().Get("Content-Type"), "message": fault.Message, "status": recorder.Code}
	}
	golden.AssertJSON(t, results)
}

func TestS3PresignedSignatureFaultCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	if err := deps.Clock.Advance(time.Date(2098, 12, 31, 23, 59, 30, 0, time.UTC).Sub(deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.S3ValidatePresignedSignatures = true
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	if err := deps.Store.Scope("_mirror", "global").Collection("stsk").Put(context.Background(), "temporary", []byte("000000000000")); err != nil {
		t.Fatal(err)
	}
	results := map[string]any{}
	for name, tc := range map[string]struct {
		target string
		status int
		code   string
	}{
		"sigv2":  {"/bucket/key?AWSAccessKeyId=test&Expires=4070908800&Signature=AAAAAAAAAAAAAAAAAAAAAAAAAAA%3D", http.StatusForbidden, "SignatureDoesNotMatch"},
		"sigv4":  {"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64), http.StatusForbidden, "SignatureDoesNotMatch"},
		"sigv4a": {"/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Region-Set=us-east-1&X-Amz-SignedHeaders=host&X-Amz-Signature=00", http.StatusForbidden, "SignatureDoesNotMatch"},
		"token":  {"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=temporary%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Security-Token=wrong&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64), http.StatusBadRequest, "InvalidToken"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.target, nil))
		var fault struct{ Code, Message string }
		if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != tc.status || fault.Code != tc.code || fault.Message == "" {
			t.Fatalf("%s status=%d headers=%#v body=%s", name, recorder.Code, recorder.Header(), recorder.Body.String())
		}
		results[name] = map[string]any{"code": fault.Code, "content_type": recorder.Header().Get("Content-Type"), "message": fault.Message, "status": recorder.Code}
	}
	authorization := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	authorization.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	authorization.Header.Set("X-Amz-Date", "20990101T000000Z")
	authorization.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="+strings.Repeat("0", 64))
	authorizationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(authorizationRecorder, authorization)
	var authorizationFault struct{ Code, Message string }
	if err := xml.Unmarshal(authorizationRecorder.Body.Bytes(), &authorizationFault); err != nil {
		t.Fatal(err)
	}
	if authorizationRecorder.Code != http.StatusForbidden || authorizationFault.Code != "SignatureDoesNotMatch" || authorizationFault.Message == "" {
		t.Fatalf("authorization status=%d headers=%#v body=%s", authorizationRecorder.Code, authorizationRecorder.Header(), authorizationRecorder.Body.String())
	}
	results["authorization_v4"] = map[string]any{"code": authorizationFault.Code, "content_type": authorizationRecorder.Header().Get("Content-Type"), "message": authorizationFault.Message, "status": authorizationRecorder.Code}
	authorization = httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	authorization.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	authorization.Header.Set("X-Amz-Date", "20990101T000000Z")
	authorization.Header.Set("X-Amz-Region-Set", "us-east-1")
	authorization.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-region-set,Signature=00")
	authorizationRecorder = httptest.NewRecorder()
	handler.ServeHTTP(authorizationRecorder, authorization)
	if err := xml.Unmarshal(authorizationRecorder.Body.Bytes(), &authorizationFault); err != nil {
		t.Fatal(err)
	}
	if authorizationRecorder.Code != http.StatusForbidden || authorizationFault.Code != "SignatureDoesNotMatch" || authorizationFault.Message == "" {
		t.Fatalf("SigV4A authorization status=%d headers=%#v body=%s", authorizationRecorder.Code, authorizationRecorder.Header(), authorizationRecorder.Body.String())
	}
	results["authorization_v4a"] = map[string]any{"code": authorizationFault.Code, "content_type": authorizationRecorder.Header().Get("Content-Type"), "message": authorizationFault.Message, "status": authorizationRecorder.Code}
	authorization = httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	authorization.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
	authorization.Header.Set("Authorization", "AWS test:AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	authorizationRecorder = httptest.NewRecorder()
	handler.ServeHTTP(authorizationRecorder, authorization)
	if err := xml.Unmarshal(authorizationRecorder.Body.Bytes(), &authorizationFault); err != nil {
		t.Fatal(err)
	}
	if authorizationRecorder.Code != http.StatusForbidden || authorizationFault.Code != "SignatureDoesNotMatch" || authorizationFault.Message == "" {
		t.Fatalf("SigV2 authorization status=%d headers=%#v body=%s", authorizationRecorder.Code, authorizationRecorder.Header(), authorizationRecorder.Body.String())
	}
	results["authorization_v2"] = map[string]any{"code": authorizationFault.Code, "content_type": authorizationRecorder.Header().Get("Content-Type"), "message": authorizationFault.Message, "status": authorizationRecorder.Code}
	validAuthorization := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	validAuthorization.Header.Set("Date", "Thu, 01 Jan 2099 00:00:00 GMT")
	validAuthorization.Header.Set("Authorization", "AWS test:RviC9EQBbB5VsLwWSI+WK0lMbaQ=")
	validAuthorizationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(validAuthorizationRecorder, validAuthorization)
	if validAuthorizationRecorder.Code != http.StatusNotFound || bytes.Contains(validAuthorizationRecorder.Body.Bytes(), []byte("SignatureDoesNotMatch")) {
		t.Fatalf("valid SigV2 authorization rejected: %d %s", validAuthorizationRecorder.Code, validAuthorizationRecorder.Body.String())
	}
	results["authorization_v2_valid_status"] = validAuthorizationRecorder.Code
	valid := httptest.NewRecorder()
	handler.ServeHTTP(valid, httptest.NewRequest(http.MethodGet, "/bucket/key?AWSAccessKeyId=test&Expires=4070908800&Signature=B7s4qHMXncO2jDe59MhIDTHOODk%3D", nil))
	if valid.Code != http.StatusNotFound || bytes.Contains(valid.Body.Bytes(), []byte("SignatureDoesNotMatch")) {
		t.Fatalf("valid SigV2 rejected: %d %s", valid.Code, valid.Body.String())
	}
	results["sigv2_valid_status"] = valid.Code
	portTarget := "/signed/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=8752c43939826ec5e949abb74845c6ac5a92ea98f5114bbdc6db1e78fe2b7e5e"
	for name, tc := range map[string]struct {
		host   string
		status int
	}{
		"gateway_port_443":  {"s3.localhost.localstack.cloud:443", http.StatusNotFound},
		"gateway_port_none": {"s3.localhost.localstack.cloud", http.StatusNotFound},
		"unrelated_port":    {"s3.localhost.localstack.cloud:8443", http.StatusForbidden},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, portTarget, nil)
		request.Host = tc.host
		handler.ServeHTTP(recorder, request)
		if recorder.Code != tc.status {
			t.Fatalf("%s status=%d body=%s", name, recorder.Code, recorder.Body.String())
		}
		results[name] = recorder.Code
	}
	if err := deps.Clock.Advance(16 * time.Minute); err != nil {
		t.Fatal(err)
	}
	skewedAuthorizationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(skewedAuthorizationRecorder, validAuthorization)
	var skewedAuthorizationFault struct{ Code, Message, RequestTime, ServerTime string }
	if err := xml.Unmarshal(skewedAuthorizationRecorder.Body.Bytes(), &skewedAuthorizationFault); err != nil {
		t.Fatal(err)
	}
	if skewedAuthorizationRecorder.Code != http.StatusForbidden || skewedAuthorizationFault.Code != "RequestTimeTooSkewed" || skewedAuthorizationFault.Message == "" || skewedAuthorizationFault.RequestTime == "" || skewedAuthorizationFault.ServerTime == "" {
		t.Fatalf("skewed SigV2 authorization status=%d body=%s", skewedAuthorizationRecorder.Code, skewedAuthorizationRecorder.Body.String())
	}
	results["authorization_skew"] = map[string]any{"code": skewedAuthorizationFault.Code, "request_time": skewedAuthorizationFault.RequestTime, "server_time": skewedAuthorizationFault.ServerTime, "status": skewedAuthorizationRecorder.Code}
	golden.AssertJSON(t, results)
}

func TestS3StreamingSignatureCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	if err := deps.Clock.Advance(time.Date(2098, 12, 31, 23, 59, 30, 0, time.UTC).Sub(deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.S3ValidatePresignedSignatures = true
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	for _, bucket := range []string{"streaming", "trailers", "unsigned", "v4a", "v4a-trailers", "v4a-unsigned"} {
		created := httptest.NewRecorder()
		handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/"+bucket, nil))
		if created.Code != http.StatusOK {
			t.Fatalf("create bucket %s: %d %s", bucket, created.Code, created.Body.String())
		}
	}
	results := map[string]any{}
	for name, payload := range map[string]string{"valid": "hello", "tampered": "jello"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, streamingSignatureRequest(payload))
		result := map[string]any{"status": recorder.Code}
		if recorder.Code != http.StatusOK {
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			result["code"] = fault.Code
		}
		results[name] = result
	}
	for name, tc := range map[string]struct {
		checksum string
		signed   bool
	}{
		"unsigned_trailer_valid":        {"mnG7TA==", false},
		"unsigned_trailer_bad_checksum": {"AAAAAA==", false},
		"unsigned_trailer_signed_chunk": {"mnG7TA==", true},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, streamingUnsignedTrailerRequest(tc.checksum, tc.signed))
		result := map[string]any{"status": recorder.Code}
		if recorder.Code != http.StatusOK {
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			result["code"] = fault.Code
		}
		results[name] = result
	}
	for name, tc := range map[string]struct {
		checksum, signature string
	}{
		"trailer_valid":        {"mnG7TA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa"},
		"trailer_tampered":     {"AAAAAA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa"},
		"trailer_bad_checksum": {"AAAAAA==", "0ef78e944cd5e61df18db7d6b99929d6871152b34d021274be44c9b5b113eeda"},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, streamingTrailerSignatureRequest(tc.checksum, tc.signature))
		result := map[string]any{"status": recorder.Code}
		if recorder.Code != http.StatusOK {
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			result["code"] = fault.Code
		}
		results[name] = result
	}
	for name, tc := range map[string]struct {
		trailer  bool
		payload  string
		checksum string
	}{
		"v4a_stream_valid":     {payload: "hello"},
		"v4a_stream_tampered":  {payload: "jello"},
		"v4a_trailer_valid":    {trailer: true, payload: "hello", checksum: "mnG7TA=="},
		"v4a_trailer_tampered": {trailer: true, payload: "hello", checksum: "AAAAAA=="},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, streamingV4ARequest(tc.trailer, tc.payload, tc.checksum))
		result := map[string]any{"status": recorder.Code}
		if recorder.Code != http.StatusOK {
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			result["code"] = fault.Code
		}
		results[name] = result
	}
	for name, tc := range map[string]struct {
		checksum string
		signed   bool
	}{
		"v4a_unsigned_trailer_valid":        {"mnG7TA==", false},
		"v4a_unsigned_trailer_bad_checksum": {"AAAAAA==", false},
		"v4a_unsigned_trailer_signed_chunk": {"mnG7TA==", true},
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, streamingUnsignedV4ARequest(tc.checksum, tc.signed))
		result := map[string]any{"status": recorder.Code}
		if recorder.Code != http.StatusOK {
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			result["code"] = fault.Code
		}
		results[name] = result
	}
	read := httptest.NewRecorder()
	handler.ServeHTTP(read, httptest.NewRequest(http.MethodGet, "/streaming/object", nil))
	results["stored"] = map[string]any{"body": read.Body.String(), "status": read.Code}
	trailerRead := httptest.NewRecorder()
	handler.ServeHTTP(trailerRead, httptest.NewRequest(http.MethodGet, "/trailers/object", nil))
	results["trailer_stored"] = map[string]any{"body": trailerRead.Body.String(), "status": trailerRead.Code}
	unsignedRead := httptest.NewRecorder()
	handler.ServeHTTP(unsignedRead, httptest.NewRequest(http.MethodGet, "/unsigned/object", nil))
	results["unsigned_trailer_stored"] = map[string]any{"body": unsignedRead.Body.String(), "status": unsignedRead.Code}
	for _, bucket := range []string{"v4a", "v4a-trailers", "v4a-unsigned"} {
		read := httptest.NewRecorder()
		handler.ServeHTTP(read, httptest.NewRequest(http.MethodGet, "/"+bucket+"/object", nil))
		results[bucket+"_stored"] = map[string]any{"body": read.Body.String(), "status": read.Code}
	}
	golden.AssertJSON(t, results)
}

func TestS3MalformedAWSChunkedCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/chunk-errors", nil))
	if created.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", created.Code, created.Body.String())
	}
	valid := "5\r\nhello\r\n0\r\n\r\n"
	results := map[string]any{}
	for name, tc := range map[string]struct {
		decoded string
		body    string
	}{
		"missing decoded length": {body: valid},
		"non-integer length":     {decoded: "test", body: valid},
		"negative length":        {decoded: "-1", body: valid},
		"mismatched length":      {decoded: "4", body: valid},
		"truncated chunk":        {decoded: "5", body: "5\r\nhello"},
		"missing terminal chunk": {decoded: "5", body: "5\r\nhello\r\n"},
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, "/chunk-errors/"+strings.ReplaceAll(name, " ", "-"), strings.NewReader(tc.body))
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			if tc.decoded != "" {
				request.Header.Set("X-Amz-Decoded-Content-Length", tc.decoded)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			var fault struct{ Code string }
			if err := xml.Unmarshal(recorder.Body.Bytes(), &fault); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusForbidden || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			results[name] = map[string]any{"code": fault.Code, "status": recorder.Code}
		})
	}
	golden.AssertJSON(t, results)
}

func TestS3AWSChunkedContentEncodingCharacterization(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := edge.New(cfg, deps, reg, "test").Handler()
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, httptest.NewRequest(http.MethodPut, "/chunk-encoding", nil))
	if created.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", created.Code, created.Body.String())
	}
	results := map[string]any{}
	for name, tc := range map[string]struct {
		encoding string
		want     string
	}{
		"chunked only":         {"aws-chunked", ""},
		"content before chunk": {"gzip, aws-chunked", "gzip"},
		"content after chunk":  {"AWS-CHUNKED, br", "br"},
	} {
		t.Run(name, func(t *testing.T) {
			path := "/chunk-encoding/" + strings.ReplaceAll(name, " ", "-")
			request := httptest.NewRequest(http.MethodPut, path, strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
			request.Header.Set("Content-Encoding", tc.encoding)
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			request.Header.Set("X-Amz-Decoded-Content-Length", "5")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("put: %d %s", recorder.Code, recorder.Body.String())
			}
			read := httptest.NewRecorder()
			handler.ServeHTTP(read, httptest.NewRequest(http.MethodGet, path, nil))
			if read.Code != http.StatusOK || read.Body.String() != "hello" || read.Header().Get("Content-Encoding") != tc.want {
				t.Fatalf("get: %d encoding=%q body=%q", read.Code, read.Header().Get("Content-Encoding"), read.Body.String())
			}
			results[name] = map[string]any{"body": read.Body.String(), "content_encoding": read.Header().Get("Content-Encoding"), "status": read.Code}
		})
	}
	golden.AssertJSON(t, results)
}

func streamingUnsignedV4ARequest(checksum string, signedChunk bool) *http.Request {
	extension := ""
	if signedChunk {
		extension = ";chunk-signature=unexpected"
	}
	raw := "5" + extension + "\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + checksum + "\r\n\r\n"
	request := httptest.NewRequest(http.MethodPut, "/v4a-unsigned/object", strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Region-Set", "us-east-1")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set;x-amz-trailer,Signature=304402201f09d982734f868ab87f6e305473f7ef74a6882095dbf5d0f0b97bede169993402204a4c59017095e2ffaf861e04fc6c73b5d1c9b0d8c041b7fd2acb05d0a4c356f3")
	return request
}

func streamingV4ARequest(trailer bool, payload, checksum string) *http.Request {
	path := "/v4a/object"
	payloadHash := "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set"
	seed := "30450220292f2afead2f51323260a06fdfed3d88e0998b54f024a175f65e19bdbf970425022100e28adec0e230329184badd9bf335b18c8ad5373000bad0c47223b173ecd16d11"
	chunks := []string{"**304502201ba0be85f07d901a715f28fbcd6d4ee4d14ab70abe11f5cfaff93a3c1961e4ae022100f5693b9c34d100107df15bd06cbc5c1a608d467761f97f26e048c240b21cc256", "**304502202bed57aec7b9b53cfebdf5163fbc5c61009c0f0b1e1b50848ac50641c6d0d14a022100806a00edfb80226cf9f2761851cd38cb9f33ee3fdafb597c723086655aad5cb9"}
	trailerBlock := ""
	if trailer {
		path = "/v4a-trailers/object"
		payloadHash += "-TRAILER"
		signedHeaders += ";x-amz-trailer"
		seed = "3046022100dcdd29ee9c78fdb87571b7ee2f202417795100fc3782a87296d8dbcdfd05ee91022100e72c624e7c065de7d9d6bc9f44b805390367f72d041219ea147ec45c4d47d180"
		chunks = []string{"**3045022014ec32c1ce4d72ad9504db7c3584cdf88ef5408590472dfa1333f3696d030a76022100e15554ef66351e5f90b6b9a62e67b0fdf0b2e678ce3c5394252f3e57d93275a6", "**304502210090e80732fa8c16e01818cafdbff64c37e56feced7c512cd43c48481df98377970220145d5e04288392f3bad2740bc847b217751f666baad7ee1a5358c68161b9297d"}
		trailerBlock = "x-amz-checksum-crc32c:" + checksum + "\r\nx-amz-trailer-signature:****30440220053b683045656f9eba0a1a2785bea923cddca5c5cc83b0d1fba03e1aab23fd5502200c01dde330a75c75412925fe9dd44324a60aee6a7491714e1c1ed6944e0a05aa\r\n"
	}
	raw := "5;chunk-signature=" + chunks[0] + "\r\n" + payload + "\r\n0;chunk-signature=" + chunks[1] + "\r\n" + trailerBlock + "\r\n"
	request := httptest.NewRequest(http.MethodPut, path, strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Region-Set", "us-east-1")
	if trailer {
		request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	}
	request.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders="+signedHeaders+",Signature="+seed)
	return request
}

func streamingTrailerSignatureRequest(checksum, trailerSignature string) *http.Request {
	raw := "5;chunk-signature=c83b0404927860c2dfacb114cd53dfe5505c5b4ad4dc605cc4e53806d4bb0d74\r\nhello\r\n0;chunk-signature=ffc89ae66d2e00900ad958aa09d8ea91ab7e1cb1938d6f4a5a30821f8fbe297f\r\nx-amz-checksum-crc32c:" + checksum + "\r\nx-amz-trailer-signature:" + trailerSignature + "\r\n\r\n"
	request := httptest.NewRequest(http.MethodPut, "/trailers/object", strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=378380e9501dea596cd83a9661c42fc2603dbd37872ab598316173a4d9244821")
	return request
}

func streamingUnsignedTrailerRequest(checksum string, signedChunk bool) *http.Request {
	extension := ""
	if signedChunk {
		extension = ";chunk-signature=unexpected"
	}
	raw := "5" + extension + "\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + checksum + "\r\n\r\n"
	request := httptest.NewRequest(http.MethodPut, "/unsigned/object", strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
	return request
}

func streamingSignatureRequest(payload string) *http.Request {
	raw := "5;chunk-signature=87081aa8d08ebfccd3aa73e18ac88541cf2050c23a5a49a9e46d94a70d84f2a4\r\n" + payload + "\r\n0;chunk-signature=eaf2700e23d624c531f0f9a0c7312b66470ab3aee81742bfa00dfc9cf6ca0f4e\r\n\r\n"
	request := httptest.NewRequest(http.MethodPut, "/streaming/object", strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d32bab45d70b05d89ada2e57acc27c4117cf31f7ce3de470cf916b8f89558054")
	return request
}

func FuzzS3ResponseEnvelope(f *testing.F) {
	deps := spitest.Deps(f)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		f.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	f.Cleanup(ts.Close)
	for _, seed := range []struct {
		method uint8
		path   string
	}{{0, "object"}, {1, "space value"}, {2, "unicode-☃"}, {0, "eks"}} {
		f.Add(seed.method, seed.path)
	}
	methods := []string{http.MethodGet, http.MethodHead, http.MethodDelete}
	f.Fuzz(func(t *testing.T, method uint8, path string) {
		request, err := http.NewRequest(methods[int(method)%len(methods)], ts.URL+"/missing-fuzz-bucket/"+url.PathEscape(path), nil)
		if err != nil {
			t.Skip()
		}
		request.Host = "s3.localhost.localstack.cloud"
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		s3Envelope(t, response)
	})
}

func TestHealth(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	reg, err := registry.New(deps, []string{"aws.s3"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	res, err := http.Get(ts.URL + "/_mirror/health")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatal(res.Status)
	}
}

func TestDiagnostics(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	for _, path := range []string{"/_mirror/services", "/_mirror/journal?service=aws.s3", "/_mirror/model/aws.s3"} {
		response, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || !json.Valid(body) {
			t.Fatalf("GET %s: %d %s", path, response.StatusCode, body)
		}
	}
	response, err := http.Post(ts.URL+"/_mirror/clock/advance", "application/json", strings.NewReader(`{"duration":"1s"}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("advance clock: %s", response.Status)
	}
	response, err = http.Get(ts.URL + "/_mirror/missing")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing diagnostic: %s", response.Status)
	}
	response, err = http.Post(ts.URL+"/_mirror/reset", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("reset: %s", response.Status)
	}
}

func TestAWSChunkedDeframe(t *testing.T) {
	// framed: size;chunk-signature=x\r\npayload\r\n0;chunk-signature=x\r\n\r\n
	payload := "hello-chunk"
	framed := "b;chunk-signature=abc\r\n" + payload + "\r\n0;chunk-signature=abc\r\n\r\n"
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	mkb, _ := http.NewRequest(http.MethodPut, ts.URL+"/bkt", nil)
	mkb.Header.Set("Authorization", auth)
	bres, err := http.DefaultClient.Do(mkb)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, bres.Body)
	bres.Body.Close()

	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/bkt/obj", strings.NewReader(framed))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", "11")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/bkt/obj", nil)
	get.Header.Set("Authorization", req.Header.Get("Authorization"))
	gres, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gres.Body)
	gres.Body.Close()
	if string(body) != payload {
		t.Fatalf("stored framing? got %q", body)
	}
}
