package spine

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

func TestBootedServerChunkedPutGetAndForeign501(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "spine"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	mk, err := http.NewRequest(http.MethodPut, ts.URL+"/spinek", nil)
	if err != nil {
		t.Fatal(err)
	}
	mk.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(mk)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create bucket %d", res.StatusCode)
	}
	if res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
	}

	payload := "hello-chunk"
	framed := "b;chunk-signature=abc\r\n" + payload + "\r\n0;chunk-signature=abc\r\n\r\n"
	put, _ := http.NewRequest(http.MethodPut, ts.URL+"/spinek/obj", strings.NewReader(framed))
	put.Header.Set("Authorization", auth)
	put.Header.Set("Content-Encoding", "aws-chunked")
	put.Header.Set("X-Amz-Decoded-Content-Length", "11")
	// curl --data-binary defaults to this Content-Type; ParseForm must not eat the body.
	put.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	pres, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, pres.Body)
	pres.Body.Close()
	if pres.StatusCode >= 300 {
		t.Fatalf("put %d", pres.StatusCode)
	}

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/spinek/obj", nil)
	get.Header.Set("Authorization", auth)
	gres, err := http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(gres.Body)
	gres.Body.Close()
	if gres.StatusCode != 200 {
		t.Fatalf("get %d %s", gres.StatusCode, body)
	}
	if !bytes.Equal(body, []byte(payload)) {
		t.Fatalf("stored framing? got %q", body)
	}

	plain := []byte("not-chunked-body")
	formPut, _ := http.NewRequest(http.MethodPut, ts.URL+"/spinek/plain.txt", bytes.NewReader(plain))
	formPut.Header.Set("Authorization", auth)
	formPut.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	fres, err := http.DefaultClient.Do(formPut)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, fres.Body)
	fres.Body.Close()
	if fres.StatusCode >= 300 {
		t.Fatalf("form put %d", fres.StatusCode)
	}
	fget, _ := http.NewRequest(http.MethodGet, ts.URL+"/spinek/plain.txt", nil)
	fget.Header.Set("Authorization", auth)
	fgres, err := http.DefaultClient.Do(fget)
	if err != nil {
		t.Fatal(err)
	}
	fbody, _ := io.ReadAll(fgres.Body)
	fgres.Body.Close()
	if string(fbody) != string(plain) {
		t.Fatalf("form-urlencoded PUT consumed body: got %q", fbody)
	}

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
		t.Fatalf("expected 501, got %d %s", dres.StatusCode, db)
	}
	if dres.Header.Get("x-mirror-not-implemented") == "" && !bytes.Contains(db, []byte("MirrorNotImplemented")) {
		t.Fatalf("not §4.11: %s %v", db, dres.Header)
	}
}

func TestBootedServerS3QuerySemantics(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "spine-query"
	rt, err := runtime.Boot(cfg)
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
		req.Header.Set("Authorization", auth)
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

	if code, b, _ := do(http.MethodPut, "/qb", "", nil); code >= 300 {
		t.Fatalf("create bucket %d %s", code, b)
	}
	if code, b, h := do(http.MethodPut, "/qb/k", "keep-me", nil); code >= 300 {
		t.Fatalf("put k %d %s", code, b)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("put fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, b, _ := do(http.MethodPut, "/qb/src", "SRC-BYTES", nil); code >= 300 {
		t.Fatalf("put src %d %s", code, b)
	}

	tagXML := `<Tagging><TagSet><Tag><Key>env</Key><Value>test</Value></Tag></TagSet></Tagging>`
	if code, b, h := do(http.MethodPut, "/qb?tagging", tagXML, nil); code >= 300 {
		t.Fatalf("put tagging %d %s", code, b)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("tag put fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, b, _ := do(http.MethodGet, "/qb?tagging", "", nil); code != 200 {
		t.Fatalf("get tagging %d %s", code, b)
	} else if !bytes.Contains(b, []byte("env")) || !bytes.Contains(b, []byte("test")) {
		t.Fatalf("tagging round-trip %s", b)
	}

	delXML := `<Delete><Object><Key>k</Key></Object></Delete>`
	if code, b, h := do(http.MethodPost, "/qb?delete", delXML, nil); code >= 300 {
		t.Fatalf("delete objects %d %s", code, b)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("delete fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, b, _ := do(http.MethodGet, "/qb/k", "", nil); code != 404 {
		t.Fatalf("deleted key still there %d %s", code, b)
	}

	code, b, _ := do(http.MethodPost, "/qb/m?uploads", "", nil)
	if code >= 300 {
		t.Fatalf("create mpu %d %s", code, b)
	}
	uid := regexp.MustCompile(`<UploadId>([^<]+)</UploadId>`).FindSubmatch(b)
	if uid == nil {
		t.Fatalf("no UploadId in %s", b)
	}
	uploadID := string(uid[1])
	code, b, part1Headers := do(http.MethodPut, "/qb/m?partNumber=1&uploadId="+uploadID, strings.Repeat("A", 5<<20), nil)
	if code >= 300 {
		t.Fatalf("upload part %d %s", code, b)
	}
	code, b, part2Headers := do(http.MethodPut, "/qb/m?partNumber=2&uploadId="+uploadID, "", map[string]string{"x-amz-copy-source": "qb/src"})
	if code >= 300 {
		t.Fatalf("upload part copy %d %s", code, b)
	} else if part2Headers.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("part copy fidelity %q", part2Headers.Get("x-mirror-fidelity"))
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?uploadId="+uploadID, "", nil); code != 200 {
		t.Fatalf("list parts %d %s", code, b)
	} else if !bytes.Contains(b, []byte("PartNumber")) {
		t.Fatalf("list parts empty %s", b)
	}
	comp := `<CompleteMultipartUpload><Part><ETag>` + part1Headers.Get("ETag") + `</ETag><PartNumber>1</PartNumber></Part><Part><ETag>` + part2Headers.Get("ETag") + `</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`
	size := 5<<20 + len("SRC-BYTES")
	if code, b, _ := do(http.MethodPost, "/qb/m?uploadId="+uploadID, comp, map[string]string{"x-amz-mp-object-size": strconv.Itoa(size - 1)}); code != http.StatusBadRequest || !bytes.Contains(b, []byte("InvalidRequest")) {
		t.Fatalf("complete mpu size mismatch %d %s", code, b)
	}
	if code, b, h := do(http.MethodPost, "/qb/m?uploadId="+uploadID, comp, map[string]string{"x-amz-mp-object-size": strconv.Itoa(size)}); code >= 300 {
		t.Fatalf("complete mpu %d %s", code, b)
	} else if !bytes.Contains(b, []byte("-2")) && !strings.Contains(h.Get("ETag"), "-2") {
		t.Fatalf("multipart etag %s %v", b, h)
	}
}
