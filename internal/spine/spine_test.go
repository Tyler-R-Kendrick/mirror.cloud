package spine

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

	code, b, createHeaders := do(http.MethodPost, "/qb/m?uploads", "", map[string]string{"x-amz-checksum-algorithm": "CRC32", "x-amz-checksum-type": "FULL_OBJECT", "x-amz-storage-class": "STANDARD_IA", "x-amz-tagging": "env=test&team=storage"})
	if code >= 300 {
		t.Fatalf("create mpu %d %s", code, b)
	} else if createHeaders.Get("x-amz-checksum-algorithm") != "CRC32" || createHeaders.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("create mpu checksum headers %v", createHeaders)
	}
	uid := regexp.MustCompile(`<UploadId>([^<]+)</UploadId>`).FindSubmatch(b)
	if uid == nil {
		t.Fatalf("no UploadId in %s", b)
	}
	uploadID := string(uid[1])
	startUpload := func(key string) string {
		t.Helper()
		code, body, _ := do(http.MethodPost, "/qb/"+key+"?uploads", "", nil)
		id := regexp.MustCompile(`<UploadId>([^<]+)</UploadId>`).FindSubmatch(body)
		if code >= 300 || id == nil {
			t.Fatalf("create %s upload %d %s", key, code, body)
		}
		return string(id[1])
	}
	startUpload("z")
	startUpload("a")
	startUpload("photos/2026/x")
	secondM := startUpload("m")
	if code, body, _ := do(http.MethodGet, "/qb?uploads&max-uploads=2", "", nil); code != 200 {
		t.Fatalf("list uploads page 1 %d %s", code, body)
	} else if bytes.Count(body, []byte("<Upload>")) != 2 || bytes.Contains(body, []byte("<member>")) || !bytes.Contains(body, []byte("<Key>a</Key>")) || !bytes.Contains(body, []byte("<Key>m</Key>")) || !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) || !bytes.Contains(body, []byte("<NextKeyMarker>m</NextKeyMarker>")) || !bytes.Contains(body, []byte("<NextUploadIdMarker>"+uploadID+"</NextUploadIdMarker>")) {
		t.Fatalf("list uploads page 1 %s", body)
	}
	if code, body, _ := do(http.MethodGet, "/qb?uploads&key-marker=m&upload-id-marker="+uploadID+"&max-uploads=3", "", nil); code != 200 {
		t.Fatalf("list uploads page 2 %d %s", code, body)
	} else if !bytes.Contains(body, []byte("<UploadId>"+secondM+"</UploadId>")) || !bytes.Contains(body, []byte("<Key>photos/2026/x</Key>")) || !bytes.Contains(body, []byte("<Key>z</Key>")) || !bytes.Contains(body, []byte("<IsTruncated>false</IsTruncated>")) {
		t.Fatalf("list uploads page 2 %s", body)
	}
	if code, body, _ := do(http.MethodGet, "/qb?uploads&prefix=photos/&delimiter=/", "", nil); code != 200 || !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>photos/2026/</Prefix></CommonPrefixes>")) {
		t.Fatalf("list grouped uploads %d %s", code, body)
	}
	for _, number := range []string{"0", "10001", "invalid"} {
		if code, body, _ := do(http.MethodPut, "/qb/m?partNumber="+number+"&uploadId="+uploadID, "part", nil); code != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("upload part %s %d %s", number, code, body)
		}
	}
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
	if code, b, _ := do(http.MethodGet, "/qb/m?uploadId="+uploadID+"&max-parts=1", "", nil); code != 200 {
		t.Fatalf("list parts page 1 %d %s", code, b)
	} else if !bytes.Contains(b, []byte("<Part><ChecksumCRC32>")) || bytes.Contains(b, []byte("<member>")) || !bytes.Contains(b, []byte("<PartNumber>1</PartNumber>")) || !bytes.Contains(b, []byte("<IsTruncated>true</IsTruncated>")) || !bytes.Contains(b, []byte("<NextPartNumberMarker>1</NextPartNumberMarker>")) || !bytes.Contains(b, []byte("<ChecksumAlgorithm>CRC32</ChecksumAlgorithm>")) || !bytes.Contains(b, []byte("<ChecksumType>FULL_OBJECT</ChecksumType>")) {
		t.Fatalf("list parts page 1 %s", b)
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?uploadId="+uploadID+"&part-number-marker=1&max-parts=1", "", nil); code != 200 {
		t.Fatalf("list parts page 2 %d %s", code, b)
	} else if !bytes.Contains(b, []byte("<PartNumber>2</PartNumber>")) || !bytes.Contains(b, []byte("<IsTruncated>false</IsTruncated>")) {
		t.Fatalf("list parts page 2 %s", b)
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?uploadId=missing", "", nil); code != http.StatusNotFound || !bytes.Contains(b, []byte("NoSuchUpload")) {
		t.Fatalf("list missing upload %d %s", code, b)
	}
	for _, request := range []struct{ method, body string }{
		{http.MethodPut, "part"},
		{http.MethodPost, `<CompleteMultipartUpload/>`},
		{http.MethodDelete, ""},
	} {
		path := "/qb/m?uploadId=missing"
		if request.method == http.MethodPut {
			path += "&partNumber=1"
		}
		if code, b, _ := do(request.method, path, request.body, nil); code != http.StatusNotFound || !bytes.Contains(b, []byte("NoSuchUpload")) {
			t.Fatalf("%s missing upload %d %s", request.method, code, b)
		}
	}
	comp := `<CompleteMultipartUpload><Part><ETag>` + part1Headers.Get("ETag") + `</ETag><PartNumber>1</PartNumber></Part><Part><ETag>` + part2Headers.Get("ETag") + `</ETag><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`
	size := 5<<20 + len("SRC-BYTES")
	if code, b, _ := do(http.MethodPost, "/qb/m?uploadId="+uploadID, comp, map[string]string{"x-amz-mp-object-size": strconv.Itoa(size - 1)}); code != http.StatusBadRequest || !bytes.Contains(b, []byte("InvalidRequest")) {
		t.Fatalf("complete mpu size mismatch %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/qb/m?uploadId="+uploadID, comp, map[string]string{"x-amz-mp-object-size": strconv.Itoa(size), "x-amz-checksum-crc32": "AA=="}); code != http.StatusBadRequest || !bytes.Contains(b, []byte("BadDigest")) {
		t.Fatalf("complete mpu checksum mismatch %d %s", code, b)
	}
	checksumBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBytes, crc32.ChecksumIEEE(append(bytes.Repeat([]byte("A"), 5<<20), []byte("SRC-BYTES")...)))
	checksum := base64.StdEncoding.EncodeToString(checksumBytes)
	headers := map[string]string{"x-amz-mp-object-size": strconv.Itoa(size), "x-amz-checksum-crc32": checksum, "x-amz-checksum-type": "FULL_OBJECT"}
	if code, b, h := do(http.MethodPost, "/qb/m?uploadId="+uploadID, comp, headers); code >= 300 {
		t.Fatalf("complete mpu %d %s", code, b)
	} else if !bytes.Contains(b, []byte("-2")) && !strings.Contains(h.Get("ETag"), "-2") {
		t.Fatalf("multipart etag %s %v", b, h)
	} else if !bytes.Contains(b, []byte(checksum)) {
		t.Fatalf("multipart checksum output %s", b)
	}
	if code, b, h := do(http.MethodGet, "/qb/m", "", map[string]string{"x-amz-checksum-mode": "ENABLED"}); code != http.StatusOK || h.Get("x-amz-checksum-crc32") != checksum || h.Get("x-amz-checksum-type") != "FULL_OBJECT" || h.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("get multipart checksum %d %s %v", code, b, h)
	}
	partRange := fmt.Sprintf("bytes %d-%d/%d", 5<<20, size-1, size)
	if code, b, h := do(http.MethodGet, "/qb/m?partNumber=2", "", map[string]string{"x-amz-checksum-mode": "ENABLED"}); code != http.StatusPartialContent || string(b) != "SRC-BYTES" || h.Get("Content-Length") != strconv.Itoa(len("SRC-BYTES")) || h.Get("Content-Range") != partRange || h.Get("x-amz-mp-parts-count") != "2" || h.Get("x-amz-checksum-crc32") != "" {
		t.Fatalf("get multipart part %d %q %v", code, b, h)
	}
	if code, b, h := do(http.MethodHead, "/qb/m?partNumber=2", "", nil); code != http.StatusPartialContent || len(b) != 0 || h.Get("Content-Length") != strconv.Itoa(len("SRC-BYTES")) || h.Get("Content-Range") != partRange || h.Get("x-amz-mp-parts-count") != "2" {
		t.Fatalf("head multipart part %d %q %v", code, b, h)
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?partNumber=3", "", nil); code != http.StatusRequestedRangeNotSatisfiable || !bytes.Contains(b, []byte("InvalidPartNumber")) {
		t.Fatalf("invalid multipart part %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?partNumber=1", "", map[string]string{"Range": "bytes=0-1"}); code != http.StatusBadRequest || !bytes.Contains(b, []byte("InvalidRequest")) {
		t.Fatalf("part and range %d %s", code, b)
	}
	attributeHeaders := map[string]string{"x-amz-object-attributes": "ETag,Checksum,ObjectParts,StorageClass,ObjectSize", "x-amz-max-parts": "1"}
	if code, b, h := do(http.MethodGet, "/qb/m?attributes", "", attributeHeaders); code != http.StatusOK || !bytes.Contains(b, []byte("<GetObjectAttributesResponse>")) || !bytes.Contains(b, []byte("<Checksum><ChecksumCRC32>"+checksum+"</ChecksumCRC32><ChecksumType>FULL_OBJECT</ChecksumType></Checksum>")) || !bytes.Contains(b, []byte("<ObjectParts><PartsCount>2</PartsCount></ObjectParts>")) || !bytes.Contains(b, []byte("<ObjectSize>"+strconv.Itoa(size)+"</ObjectSize>")) || !bytes.Contains(b, []byte("<StorageClass>STANDARD_IA</StorageClass>")) || bytes.Contains(b, []byte("<member>")) || h.Get("Last-Modified") == "" {
		t.Fatalf("get object attributes %d %s %v", code, b, h)
	}
	if code, b, _ := do(http.MethodGet, "/qb/m?tagging", "", nil); code != http.StatusOK || bytes.Contains(b, []byte("<member>")) || !bytes.Contains(b, []byte("<TagSet><Tag><Key>env</Key><Value>test</Value></Tag><Tag><Key>team</Key><Value>storage</Value></Tag></TagSet>")) {
		t.Fatalf("get multipart tags %d %s", code, b)
	}
}

func TestBootedServerS3VersionedTaggingContract(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "spine-versioned-tags"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string, headers map[string]string) (int, []byte, http.Header) {
		t.Helper()
		req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", auth)
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, data, res.Header
	}
	if code, body, _ := do(http.MethodPut, "/tags", "", nil); code != http.StatusOK {
		t.Fatalf("create bucket %d %s", code, body)
	}
	versioning := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
	if code, body, _ := do(http.MethodPut, "/tags?versioning", versioning, nil); code != http.StatusOK {
		t.Fatalf("enable versioning %d %s", code, body)
	}
	code, body, firstHeaders := do(http.MethodPut, "/tags/object", "first", map[string]string{"x-amz-tagging": "stage=first&team=storage"})
	firstVersion := firstHeaders.Get("x-amz-version-id")
	if code != http.StatusOK || firstVersion == "" {
		t.Fatalf("put tagged version %d %s %v", code, body, firstHeaders)
	}
	code, body, secondHeaders := do(http.MethodPut, "/tags/object", "second", nil)
	secondVersion := secondHeaders.Get("x-amz-version-id")
	if code != http.StatusOK || secondVersion == "" {
		t.Fatalf("put untagged version %d %s %v", code, body, secondHeaders)
	}
	if code, _, headers := do(http.MethodHead, "/tags/object", "", nil); code != http.StatusOK || headers.Get("x-amz-tagging-count") != "" {
		t.Fatalf("untagged current head %d %v", code, headers)
	}
	if code, _, headers := do(http.MethodGet, "/tags/object?versionId="+firstVersion, "", nil); code != http.StatusOK || headers.Get("x-amz-tagging-count") != "2" || headers.Get("x-amz-version-id") != firstVersion {
		t.Fatalf("tagged version get %d %v", code, headers)
	}
	if code, body, headers := do(http.MethodGet, "/tags/object?tagging&versionId="+firstVersion, "", nil); code != http.StatusOK || headers.Get("x-amz-version-id") != firstVersion || !bytes.Contains(body, []byte("<Key>stage</Key><Value>first</Value>")) || !bytes.Contains(body, []byte("<Key>team</Key><Value>storage</Value>")) {
		t.Fatalf("get version tags %d %s %v", code, body, headers)
	}
	tagging := `<Tagging><TagSet><Tag><Key>stage</Key><Value>second</Value></Tag></TagSet></Tagging>`
	if code, body, headers := do(http.MethodPut, "/tags/object?tagging", tagging, nil); code != http.StatusOK || headers.Get("x-amz-version-id") != secondVersion {
		t.Fatalf("tag current version %d %s %v", code, body, headers)
	}
	if code, _, headers := do(http.MethodHead, "/tags/object", "", nil); code != http.StatusOK || headers.Get("x-amz-tagging-count") != "1" {
		t.Fatalf("tagged current head %d %v", code, headers)
	}
	if code, body, _ := do(http.MethodGet, "/tags?tagging", "", nil); code != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchTagSet")) {
		t.Fatalf("untagged bucket %d %s", code, body)
	}
	if code, body, _ := do(http.MethodPut, "/tags?tagging", `<Tagging/>`, nil); code != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
		t.Fatalf("missing bucket TagSet %d %s", code, body)
	}
	duplicate := `<Tagging><TagSet><Tag><Key>stage</Key><Value>one</Value></Tag><Tag><Key>stage</Key><Value>two</Value></Tag></TagSet></Tagging>`
	if code, body, _ := do(http.MethodPut, "/tags/object?tagging", duplicate, nil); code != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidTag")) {
		t.Fatalf("duplicate object tags %d %s", code, body)
	}
	if code, body, _ := do(http.MethodPut, "/tags/rejected", "body", map[string]string{"x-amz-tagging": "stage=one&stage=two"}); code != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
		t.Fatalf("duplicate tagging header %d %s", code, body)
	}
	if code, body, _ := do(http.MethodHead, "/tags/rejected", "", nil); code != http.StatusNotFound {
		t.Fatalf("rejected tagged put persisted %d %s", code, body)
	}
}
