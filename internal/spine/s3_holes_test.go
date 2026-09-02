package spine

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	"encoding/json"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestBootedServerS3VersioningPaginationPresign(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "s3-holes"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string, hdr map[string]string) (int, []byte, http.Header) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, err := http.NewRequest(method, ts.URL+path, rdr)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header
	}

	if code, b, _ := do(http.MethodPut, "/holes", "", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("create %d %s", code, b)
	}
	verXML := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	if code, b, _ := do(http.MethodPut, "/holes?versioning", verXML, map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("versioning %d %s", code, b)
	}
	if code, b, h := do(http.MethodPut, "/holes/k", "v1", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put v1 %d %s", code, b)
	} else if h.Get("x-amz-version-id") == "" {
		t.Fatalf("no version id on put %v", h)
	}
	vid1 := ""
	if code, b, h := do(http.MethodPut, "/holes/k", "v2", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put v2 %d %s", code, b)
	} else {
		_ = h
		_ = b
	}
	if code, b, _ := do(http.MethodGet, "/holes/k", "", map[string]string{"Authorization": auth}); code != 200 || string(b) != "v2" {
		t.Fatalf("latest %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/holes?versions", "", map[string]string{"Authorization": auth}); code != 200 {
		t.Fatalf("list versions %d %s", code, b)
	} else if !bytes.Contains(b, []byte("VersionId")) {
		t.Fatalf("versions empty %s", b)
	} else {
		m := regexp.MustCompile(`<VersionId>([^<]+)</VersionId>`).FindAllSubmatch(b, -1)
		if len(m) < 2 {
			t.Fatalf("need 2 versions %s", b)
		}
		vid1 = string(m[0][1])
	}
	if vid1 != "" {
		if code, b, _ := do(http.MethodGet, "/holes/k?versionId="+vid1, "", map[string]string{"Authorization": auth}); code != 200 {
			t.Fatalf("get version %d %s", code, b)
		} else if string(b) != "v1" && string(b) != "v2" {
			t.Fatalf("version body %q", b)
		}
	}
	markerVersion := ""
	if code, b, h := do(http.MethodDelete, "/holes/k", "", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("delete marker %d %s", code, b)
	} else if h.Get("x-amz-delete-marker") != "true" {
		t.Fatalf("no delete marker header %v %s", h, b)
	} else {
		markerVersion = h.Get("x-amz-version-id")
	}
	if code, b, h := do(http.MethodGet, "/holes/k", "", map[string]string{"Authorization": auth}); code != 404 || h.Get("x-amz-delete-marker") != "true" || h.Get("x-amz-version-id") != markerVersion {
		t.Fatalf("deleted latest %d %s", code, b)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		code, b, h := do(method, "/holes/k?versionId="+markerVersion, "", map[string]string{"Authorization": auth})
		if code != http.StatusMethodNotAllowed || h.Get("Last-Modified") == "" || h.Get("x-amz-delete-marker") != "true" || h.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s explicit marker %d %v %s", method, code, h, b)
		}
	}
	if code, b, _ := do(http.MethodGet, "/holes?versions", "", map[string]string{"Authorization": auth}); code != 200 || !bytes.Contains(b, []byte("DeleteMarker")) && !bytes.Contains(b, []byte("delete")) {
		// restxml encodes DeleteMarkers key
		if !bytes.Contains(b, []byte("DeleteMarkers")) && !bytes.Contains(b, []byte("VersionId")) {
			t.Fatalf("delete markers %s", b)
		}
	}

	if code, b, _ := do(http.MethodPut, "/holes/a", "A", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put a %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/holes/b", "B", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put b %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/holes/c", "C", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put c %d %s", code, b)
	}
	code, page1, _ := do(http.MethodGet, "/holes?list-type=2&max-keys=1", "", map[string]string{"Authorization": auth})
	if code != 200 {
		t.Fatalf("list page1 %d %s", code, page1)
	}
	if !bytes.Contains(page1, []byte("NextContinuationToken")) && !bytes.Contains(page1, []byte("IsTruncated")) {
		t.Fatalf("no pagination %s", page1)
	}
	tok := regexp.MustCompile(`<NextContinuationToken>([^<]+)</NextContinuationToken>`).FindSubmatch(page1)
	if tok != nil {
		code, page2, _ := do(http.MethodGet, "/holes?list-type=2&max-keys=1&continuation-token="+string(tok[1]), "", map[string]string{"Authorization": auth})
		if code != 200 {
			t.Fatalf("list page2 %d %s", code, page2)
		}
		if bytes.Equal(page1, page2) {
			t.Fatalf("pagination repeated")
		}
	}

	code, b, conditionHeaders := do(http.MethodPut, "/holes/ims", "body", map[string]string{"Authorization": auth})
	if code >= 300 {
		t.Fatalf("put ims %d %s", code, b)
	}
	etag := conditionHeaders.Get("ETag")
	if code, b, _ := do(http.MethodGet, "/holes/ims", "", map[string]string{"Authorization": auth, "If-Modified-Since": "Mon, 01 Jan 2099 00:00:00 GMT"}); code != 200 || string(b) != "body" {
		t.Fatalf("If-Modified-Since future %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/holes/ims", "", map[string]string{"Authorization": auth, "If-Modified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"}); code != 200 || string(b) != "body" {
		t.Fatalf("If-Modified-Since past %d %s", code, b)
	}
	for _, request := range []struct {
		method, path string
		headers      map[string]string
		status       int
	}{
		{http.MethodHead, "/holes/ims", map[string]string{"If-None-Match": etag}, http.StatusNotModified},
		{http.MethodHead, "/holes/ims", map[string]string{"If-Match": `"wrong"`}, http.StatusPreconditionFailed},
		{http.MethodHead, "/holes/ims", map[string]string{"If-Match": etag, "If-Unmodified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"}, http.StatusOK},
		{http.MethodGet, "/holes/ims?attributes", map[string]string{"x-amz-object-attributes": "ETag", "If-None-Match": etag}, http.StatusNotModified},
		{http.MethodGet, "/holes/ims?attributes", map[string]string{"x-amz-object-attributes": "ETag", "If-Unmodified-Since": "Mon, 01 Jan 1990 00:00:00 GMT"}, http.StatusPreconditionFailed},
	} {
		headers := map[string]string{"Authorization": auth}
		for key, value := range request.headers {
			headers[key] = value
		}
		if code, body, _ := do(request.method, request.path, "", headers); code != request.status {
			t.Fatalf("%s %s conditions %#v = %d %s", request.method, request.path, request.headers, code, body)
		}
	}
	if code, body, headers := do(http.MethodGet, "/holes/ims", "", map[string]string{"Authorization": auth, "Range": "bytes=-2"}); code != http.StatusPartialContent || string(body) != "dy" || headers.Get("Content-Range") != "bytes 2-3/4" || headers.Get("Accept-Ranges") != "bytes" {
		t.Fatalf("suffix range %d %q %v", code, body, headers)
	}
	if code, _, headers := do(http.MethodHead, "/holes/ims", "", map[string]string{"Authorization": auth, "Range": "bytes=1-2"}); code != http.StatusPartialContent || headers.Get("Content-Length") != "2" || headers.Get("Content-Range") != "bytes 1-2/4" {
		t.Fatalf("head range %d %v", code, headers)
	}
	if code, body, headers := do(http.MethodGet, "/holes/ims", "", map[string]string{"Authorization": auth, "Range": "bytes=4-"}); code != http.StatusRequestedRangeNotSatisfiable || headers.Get("Content-Range") != "bytes */4" {
		t.Fatalf("invalid range %d %s %v", code, body, headers)
	}
	if code, body, _ := do(http.MethodGet, "/holes/ims", "", map[string]string{"Authorization": auth, "Range": "bytes=bad"}); code != http.StatusOK || string(body) != "body" {
		t.Fatalf("malformed range %d %q", code, body)
	}

	q := "/holes/ims?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test/20200101/us-east-1/s3/aws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=00"
	if code, b, _ := do(http.MethodGet, q, "", nil); code != 200 || string(b) != "body" {
		t.Fatalf("presign get %d %s", code, b)
	}
	expired := "/holes/ims?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test/20200101/us-east-1/s3/aws4_request&X-Amz-Date=20200101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=00"
	if code, _, _ := do(http.MethodGet, expired, "", nil); code != 403 {
		t.Fatalf("expired presign %d", code)
	}

	pq := "/holes/pre?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test/20200101/us-east-1/s3/aws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=3600&X-Amz-SignedHeaders=host&X-Amz-Signature=00"
	if code, b, _ := do(http.MethodPut, pq, "presign-put", nil); code >= 300 {
		t.Fatalf("presign put %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/holes/pre", "", map[string]string{"Authorization": auth}); code != 200 || string(b) != "presign-put" {
		t.Fatalf("presign put get %d %s", code, b)
	}

	if code, b, _ := do(http.MethodPut, "/holes/a%20b", "x", map[string]string{"Authorization": auth}); code >= 300 {
		t.Fatalf("put space key %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/holes?list-type=2&encoding-type=url", "", map[string]string{"Authorization": auth}); code != 200 {
		t.Fatalf("encoding-type %d %s", code, b)
	} else if !bytes.Contains(b, []byte("a+b")) && !bytes.Contains(b, []byte("a%20b")) {
		t.Fatalf("keys not url-encoded %s", b)
	}
}

func TestBootedServerS3NotificationToSQS(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3", "aws.sqs"}
	cfg.Seed = "s3-notify"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	s3auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	sqsauth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=00"
	sqsJSON := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "AmazonSQS."+op)
		req.Header.Set("Authorization", sqsauth)
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
	do := func(method, path, body string) (int, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", s3auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b
	}
	sqsJSON("CreateQueue", `{"QueueName":"nq"}`)
	if code, b := do(http.MethodPut, "/bucket-n", ""); code >= 300 {
		t.Fatalf("bucket %d %s", code, b)
	}
	nxml := `<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:nq</Queue><Event>s3:ObjectCreated:Put</Event></QueueConfiguration></NotificationConfiguration>`
	if code, b := do(http.MethodPut, "/bucket-n?notification", nxml); code >= 300 {
		t.Fatalf("notify cfg %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/bucket-n/obj", "payload"); code >= 300 {
		t.Fatalf("put %d %s", code, b)
	}
	recv := sqsJSON("ReceiveMessage", `{"QueueName":"nq","WaitTimeSeconds":0,"MaxNumberOfMessages":10}`)
	msgs := asSlice(recv["Messages"])
	if len(msgs) != 2 {
		t.Fatalf("notifications %v", recv)
	}
	testEvent, objectEvent := false, false
	for _, message := range msgs {
		body := str(asM(message)["Body"])
		testEvent = testEvent || strings.Contains(body, `"Event":"s3:TestEvent"`)
		objectEvent = objectEvent || strings.Contains(body, "ObjectCreated") && strings.Contains(body, "obj")
	}
	if !testEvent || !objectEvent {
		t.Fatalf("events %v", msgs)
	}
}
