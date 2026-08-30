package behavior

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"io"
	"maps"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
)

func TestS3ObjectLifecycle(t *testing.T) {
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
	testIdentity := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/multipart-behavior", "Enabled")
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/kms-bdd", "Enabled")
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/disabled-bdd", "Disabled")

	do := func(method, path string, body []byte, storageClass string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		if method == http.MethodPut && path == "/object-lock" {
			req.Header.Set("x-amz-bucket-object-lock-enabled", "true")
		}
		if method == http.MethodPut && path == "/create-owned" {
			req.Header.Set("x-amz-object-ownership", "BucketOwnerPreferred")
		}
		if method == http.MethodPut && path == "/invalid-create-owned" {
			req.Header.Set("x-amz-object-ownership", "")
		}
		if strings.Contains(path, "?delete") {
			digest := md5.Sum(body)
			req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(digest[:]))
		}
		if storageClass != "" {
			req.Header.Set("x-amz-storage-class", storageClass)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("Given a bucket in another Region When accessed Then S3 resolves it and reports its Region", func(t *testing.T) {
		configuration := []byte(`<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>`)
		res := do(http.MethodPut, "/cross-region-bdd", configuration, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create cross-region bucket %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/cross-region-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/xml" || res.Header.Get("x-amz-access-point-alias") != "false" || res.Header.Get("x-amz-bucket-region") != "us-west-2" || res.Header.Get("x-amz-bucket-arn") != "arn:aws:s3:::cross-region-bdd" || res.Header.Get("x-amz-request-id") == "" || res.Header.Get("x-amz-id-2") == "" {
			t.Fatalf("cross-region head %d %#v", res.StatusCode, res.Header)
		}
		res = do(http.MethodPut, "/cross-region-bdd/key", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("cross-region put %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cross-region-bdd?list-type=2", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-bucket-region") != "us-west-2" || !bytes.Contains(body, []byte("<Key>key</Key>")) {
			t.Fatalf("cross-region list %d %#v %s", res.StatusCode, res.Header, body)
		}
	})

	t.Run("Given an expired presigned URL When requested Then S3 returns a modeled access denial", func(t *testing.T) {
		request, err := http.NewRequest(http.MethodGet, ts.URL+"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F19691231%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=19691231T235900Z&X-Amz-Expires=30&X-Amz-SignedHeaders=host&X-Amz-Signature=00", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || response.Header.Get("Content-Type") != "application/xml" || response.Header.Get("x-amz-request-id") == "" || response.Header.Get("x-amz-id-2") == "" || !bytes.Contains(body, []byte("<Code>AccessDenied</Code>")) || !bytes.Contains(body, []byte("<Message>Request has expired</Message>")) || !bytes.Contains(body, []byte("<X-Amz-Expires>30</X-Amz-Expires>")) {
			t.Fatalf("expired presign %d %#v %s", response.StatusCode, response.Header, body)
		}
	})

	t.Run("Given incomplete presigned authentication When requested Then S3 rejects the query", func(t *testing.T) {
		for name, target := range map[string]string{
			"sigv2": "/bucket/key?AWSAccessKeyId=test&Signature=00",
			"sigv4": "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=00&X-Amz-Expires=60&X-Amz-SignedHeaders=host",
			"sigv4a": "/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
		} {
			response, err := http.Get(ts.URL + target)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.Header.Get("Content-Type") != "application/xml" || response.Header.Get("x-amz-request-id") == "" || response.Header.Get("x-amz-id-2") == "" || !bytes.Contains(body, []byte("Query-string authentication")) {
				t.Fatalf("%s incomplete presign %d %#v %s", name, response.StatusCode, response.Header, body)
			}
		}
	})

	t.Run("Given signature validation is enabled When a signature is tampered Then S3 rejects it", func(t *testing.T) {
		target := "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64)
		response, err := http.Get(ts.URL + target)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatal("default server unexpectedly validated the signature")
		}
		strict := cfg
		strict.S3ValidatePresignedSignatures = true
		strictDeps := spitest.Deps(t)
		if err := strictDeps.Clock.Advance(time.Date(2098, 12, 31, 23, 59, 30, 0, time.UTC).Sub(strictDeps.Clock.Now())); err != nil {
			t.Fatal(err)
		}
		strictReg, err := registry.New(strictDeps, strict.Services, nil)
		if err != nil {
			t.Fatal(err)
		}
		strictServer := httptest.NewServer(edge.New(strict, strictDeps, strictReg, "test").Handler())
		defer strictServer.Close()
		for name, strictTarget := range map[string]string{
			"sigv2": "/bucket/key?AWSAccessKeyId=test&Expires=4070908800&Signature=AAAAAAAAAAAAAAAAAAAAAAAAAAA%3D",
			"sigv4": target,
			"sigv4a": "/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Region-Set=us-east-1&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
		} {
			response, err = http.Get(strictServer.URL + strictTarget)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
				t.Fatalf("%s tampered presign %d %s", name, response.StatusCode, body)
			}
		}
		createPostBucket, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/post-signature", nil)
		response, err = http.DefaultClient.Do(createPostBucket)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("create POST signature bucket %d", response.StatusCode)
		}
		var postPayload bytes.Buffer
		postWriter := multipart.NewWriter(&postPayload)
		_ = postWriter.WriteField("key", "browser-upload")
		_ = postWriter.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T01:00:00Z","conditions":[{"bucket":"post-signature"}]}`)))
		_ = postWriter.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		_ = postWriter.WriteField("x-amz-credential", "test/20990101/us-east-1/s3/aws4_request")
		_ = postWriter.WriteField("x-amz-date", "20990101T000000Z")
		_ = postWriter.WriteField("x-amz-signature", strings.Repeat("0", 64))
		postFile, _ := postWriter.CreateFormFile("file", "file.txt")
		_, _ = postFile.Write([]byte("body"))
		_ = postWriter.Close()
		postRequest, _ := http.NewRequest(http.MethodPost, strictServer.URL+"/post-signature", &postPayload)
		postRequest.Header.Set("Content-Type", postWriter.FormDataContentType())
		response, err = http.DefaultClient.Do(postRequest)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered browser POST policy %d %s", response.StatusCode, body)
		}
		authorization, err := http.NewRequest(http.MethodGet, strictServer.URL+"/bucket/key", nil)
		if err != nil {
			t.Fatal(err)
		}
		authorization.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		authorization.Header.Set("X-Amz-Date", "20990101T000000Z")
		authorization.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="+strings.Repeat("0", 64))
		response, err = http.DefaultClient.Do(authorization)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered authorization %d %s", response.StatusCode, body)
		}
		authorization, err = http.NewRequest(http.MethodGet, strictServer.URL+"/bucket/key", nil)
		if err != nil {
			t.Fatal(err)
		}
		authorization.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
		authorization.Header.Set("Authorization", "AWS test:AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		response, err = http.DefaultClient.Do(authorization)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered SigV2 authorization %d %s", response.StatusCode, body)
		}
		createStreaming, err := http.NewRequest(http.MethodPut, strictServer.URL+"/streaming", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createStreaming)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			payload string
			status  int
		}{"valid stream": {"hello", http.StatusOK}, "tampered stream": {"jello", http.StatusForbidden}} {
			raw := "5;chunk-signature=87081aa8d08ebfccd3aa73e18ac88541cf2050c23a5a49a9e46d94a70d84f2a4\r\n" + tc.payload + "\r\n0;chunk-signature=eaf2700e23d624c531f0f9a0c7312b66470ab3aee81742bfa00dfc9cf6ca0f4e\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/streaming/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d32bab45d70b05d89ada2e57acc27c4117cf31f7ce3de470cf916b8f89558054")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createTrailers, err := http.NewRequest(http.MethodPut, strictServer.URL+"/trailers", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createTrailers)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			checksum, signature string
			status              int
		}{
			"valid trailer":    {"mnG7TA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa", http.StatusOK},
			"tampered trailer": {"AAAAAA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa", http.StatusForbidden},
			"bad checksum":     {"AAAAAA==", "0ef78e944cd5e61df18db7d6b99929d6871152b34d021274be44c9b5b113eeda", http.StatusBadRequest},
		} {
			raw := "5;chunk-signature=c83b0404927860c2dfacb114cd53dfe5505c5b4ad4dc605cc4e53806d4bb0d74\r\nhello\r\n0;chunk-signature=ffc89ae66d2e00900ad958aa09d8ea91ab7e1cb1938d6f4a5a30821f8fbe297f\r\nx-amz-checksum-crc32c:" + tc.checksum + "\r\nx-amz-trailer-signature:" + tc.signature + "\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/trailers/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=378380e9501dea596cd83a9661c42fc2603dbd37872ab598316173a4d9244821")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createUnsigned, err := http.NewRequest(http.MethodPut, strictServer.URL+"/unsigned", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createUnsigned)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			checksum string
			status   int
		}{"valid unsigned trailer": {"mnG7TA==", http.StatusOK}, "bad unsigned checksum": {"AAAAAA==", http.StatusBadRequest}} {
			raw := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + tc.checksum + "\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/unsigned/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		if err := strictDeps.Store.Scope("_mirror", "global").Collection("stsk").Put(context.Background(), "temporary", []byte("000000000000")); err != nil {
			t.Fatal(err)
		}
		tokenTarget := "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=temporary%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Security-Token=wrong&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64)
		response, err = http.Get(strictServer.URL + tokenTarget)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidToken</Code>")) {
			t.Fatalf("invalid session token %d %s", response.StatusCode, body)
		}
	})

	t.Run("Given explicit KMS keys When writing and reading Then S3 validates their regional state", func(t *testing.T) {
		res := do(http.MethodPut, "/kms-validation-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		request := func(method, path, keyID string) (*http.Response, []byte) {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, strings.NewReader("body"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if method == http.MethodPut {
				req.Header.Set("x-amz-server-side-encryption", "aws:kms")
				req.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", keyID)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		enabledARN := "arn:aws:kms:us-east-1:000000000000:key/kms-bdd"
		if response, body := request(http.MethodPut, "/kms-validation-bdd/enabled", enabledARN); response.StatusCode != http.StatusOK {
			t.Fatalf("enabled key %d %s", response.StatusCode, body)
		}
		if response, body := request(http.MethodPut, "/kms-validation-bdd/missing", "arn:aws:kms:us-east-1:000000000000:key/missing"); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.NotFoundException</Code>")) {
			t.Fatalf("missing key %d %s", response.StatusCode, body)
		}
		if response, body := request(http.MethodPut, "/kms-validation-bdd/disabled", "arn:aws:kms:us-east-1:000000000000:key/disabled-bdd"); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.DisabledException</Code>")) {
			t.Fatalf("disabled key %d %s", response.StatusCode, body)
		}
		spitest.SeedKMSKey(t, deps, testIdentity, enabledARN, "Disabled")
		if response, body := request(http.MethodGet, "/kms-validation-bdd/enabled", ""); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.DisabledException</Code>")) {
			t.Fatalf("disabled read %d %s", response.StatusCode, body)
		}
	})

	t.Run("Given signed GetObject response overrides When reading Then S3 returns the requested headers", func(t *testing.T) {
		res := do(http.MethodPut, "/response-overrides-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/response-overrides-bdd/object", []byte("body"), "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object %d", res.StatusCode)
		}
		query := url.Values{
			"response-cache-control":       {"max-age=74"},
			"response-content-disposition": {`attachment; filename="foo.jpg"`},
			"response-content-encoding":    {"identity"},
			"response-content-language":    {"de-DE"},
			"response-content-type":        {"image/jpeg"},
			"response-expires":             {"Wed, 21 Oct 2015 07:28:00 GMT"},
		}
		res = do(http.MethodGet, "/response-overrides-bdd/object?"+query.Encode(), nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "body" || res.Header.Get("Cache-Control") != "max-age=74" || res.Header.Get("Content-Disposition") != `attachment; filename="foo.jpg"` || res.Header.Get("Content-Encoding") != "identity" || res.Header.Get("Content-Language") != "de-DE" || res.Header.Get("Content-Type") != "image/jpeg" || res.Header.Get("Expires") != "Wed, 21 Oct 2015 07:28:00 GMT" {
			t.Fatalf("response overrides %d %v %q", res.StatusCode, res.Header, body)
		}
	})

	t.Run("Given RFC 2047 user metadata When reading Then S3 returns mail-safe decoded values", func(t *testing.T) {
		res := do(http.MethodPut, "/rfc2047-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		req, err := http.NewRequest(http.MethodPut, ts.URL+"/rfc2047-bdd/object", strings.NewReader("body"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("x-amz-meta-non-ascii", "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=")
		req.Header.Set("x-amz-meta-fake-encoded", "=?UTF-8?Q?actually-ascii?=")
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/rfc2047-bdd/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-meta-non-ascii") != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || res.Header.Get("x-amz-meta-fake-encoded") != "actually-ascii" {
			t.Fatalf("head metadata %d %v", res.StatusCode, res.Header)
		}

		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "post")
		_ = writer.WriteField("x-amz-meta-non-ascii", "ÄMÄZÕÑ S3")
		_ = writer.WriteField("x-amz-meta-q-encoded", "=?UTF-8?Q?actually-ascii?=")
		file, _ := writer.CreateFormFile("file", "post.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		req, err = http.NewRequest(http.MethodPost, ts.URL+"/rfc2047-bdd", &payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			t.Fatalf("post object %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/rfc2047-bdd/post", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-meta-non-ascii") != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || res.Header.Get("x-amz-meta-q-encoded") != "=?UTF-8?Q?actually-ascii?=" {
			t.Fatalf("post metadata %d %v", res.StatusCode, res.Header)
		}
	})

	t.Run("Given no bucket When PUT object Then not found or created after bucket", func(t *testing.T) {
		// Given a fresh emulator
		// When the user creates a bucket
		res := do(http.MethodPut, "/demo", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		// And uploads an object
		res = do(http.MethodPut, "/demo/readme.txt", []byte("hello"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("put %d", res.StatusCode)
		}
		// Then GET returns the same bytes
		res = do(http.MethodGet, "/demo/readme.txt", nil, "")
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || string(b) != "hello" {
			t.Fatalf("get %d %q", res.StatusCode, b)
		}
		// And fidelity is declared
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
	})

	t.Run("Given a browser form When POSTing a file Then S3 stores it and returns the requested response", func(t *testing.T) {
		res := do(http.MethodPut, "/post-form", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "forms/${filename}")
		_ = writer.WriteField("success_action_status", "201")
		_ = writer.WriteField("tagging", "<Tagging><TagSet><Tag><Key>source</Key><Value>browser</Value></Tag></TagSet></Tagging>")
		_ = writer.WriteField("Expires", "Thu, 27 Aug 2026 12:00:00 GMT")
		_ = writer.WriteField("x-amz-checksum-algorithm", "SHA256")
		_ = writer.WriteField("x-amz-server-side-encryption", "aws:kms")
		_ = writer.WriteField("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:us-east-1:000000000000:key/browser")
		_ = writer.WriteField("x-amz-server-side-encryption-bucket-key-enabled", "true")
		file, err := writer.CreateFormFile("file", "report.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.Write([]byte("from browser"))
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/post-form", &payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response, _ := io.ReadAll(res.Body)
		res.Body.Close()
		checksum := sha256.Sum256([]byte("from browser"))
		if res.StatusCode != http.StatusCreated || res.Header.Get("ETag") == "" || res.Header.Get("x-amz-checksum-sha256") != base64.StdEncoding.EncodeToString(checksum[:]) || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/browser" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" || !bytes.Contains(response, []byte("<PostResponse>")) || !bytes.Contains(response, []byte("<Key>forms/report.txt</Key>")) {
			t.Fatalf("post form %d headers=%v body=%s", res.StatusCode, res.Header, response)
		}
		spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/browser", "Enabled")
		res = do(http.MethodGet, "/post-form/forms/report.txt", nil, "")
		stored, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(stored) != "from browser" || res.Header.Get("Expires") != "Thu, 27 Aug 2026 12:00:00 GMT" || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/browser" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
			t.Fatalf("stored form object %d %q", res.StatusCode, stored)
		}
		res = do(http.MethodGet, "/post-form/forms/report.txt?tagging", nil, "")
		tags, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(tags, []byte("<Key>source</Key>")) || !bytes.Contains(tags, []byte("<Value>browser</Value>")) {
			t.Fatalf("stored form tags %d %s", res.StatusCode, tags)
		}
	})

	t.Run("Given a customer encryption key When PUTting and GETting an object Then matching headers are required", func(t *testing.T) {
		res := do(http.MethodPut, "/customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		headers := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       base64.StdEncoding.EncodeToString(rawKey),
			"x-amz-server-side-encryption-customer-key-MD5":   base64.StdEncoding.EncodeToString(digest[:]),
		}
		request := func(method string, includeKey bool) (*http.Response, []byte) {
			t.Helper()
			payload := ""
			if method == http.MethodPut {
				payload = "secret"
			}
			req, _ := http.NewRequest(method, ts.URL+"/customer-encryption/object", strings.NewReader(payload))
			req.Header.Set("Authorization", auth)
			if includeKey {
				for key, value := range headers {
					req.Header.Set(key, value)
				}
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		res, _ = request(http.MethodPut, true)
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" || res.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] || res.Header.Get("x-amz-server-side-encryption") != "" {
			t.Fatalf("put customer encryption %d %v", res.StatusCode, res.Header)
		}
		if res, _ = request(http.MethodGet, false); res.StatusCode != http.StatusBadRequest {
			t.Fatalf("get without customer key %d", res.StatusCode)
		}
		res, body := request(http.MethodGet, true)
		if res.StatusCode != http.StatusOK || string(body) != "secret" || res.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] {
			t.Fatalf("get customer encryption %d %q %v", res.StatusCode, body, res.Header)
		}
	})

	t.Run("Given a customer-encrypted source When copying it Then source and destination keys are independent", func(t *testing.T) {
		res := do(http.MethodPut, "/copy-customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		key, keyMD5 := base64.StdEncoding.EncodeToString(rawKey), base64.StdEncoding.EncodeToString(digest[:])
		destinationHeaders := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       key,
			"x-amz-server-side-encryption-customer-key-MD5":   keyMD5,
		}
		request := func(method, path, body string, headers map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		if source, _ := request(http.MethodPut, "/copy-customer-encryption/source", "copy body", destinationHeaders); source.StatusCode != http.StatusOK {
			t.Fatalf("put copy source %d", source.StatusCode)
		}
		if copied, _ := request(http.MethodPut, "/copy-customer-encryption/missing-source-key", "", map[string]string{"x-amz-copy-source": "/copy-customer-encryption/source"}); copied.StatusCode != http.StatusBadRequest {
			t.Fatalf("copy without source key %d", copied.StatusCode)
		}
		copyHeaders := maps.Clone(destinationHeaders)
		copyHeaders["x-amz-copy-source"] = "/copy-customer-encryption/source"
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-algorithm"] = "AES256"
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-key"] = key
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-key-MD5"] = keyMD5
		copied, _ := request(http.MethodPut, "/copy-customer-encryption/copied", "", copyHeaders)
		if copied.StatusCode != http.StatusOK || copied.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("copy customer encryption %d %v", copied.StatusCode, copied.Header)
		}
		stored, body := request(http.MethodGet, "/copy-customer-encryption/copied", "", destinationHeaders)
		if stored.StatusCode != http.StatusOK || string(body) != "copy body" || stored.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("stored customer copy %d %q %v", stored.StatusCode, body, stored.Header)
		}
	})

	t.Run("Given KMS multipart encryption When completing the upload Then every stage preserves its headers", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		keyID := "arn:aws:kms:us-east-1:000000000000:key/multipart-behavior"
		request := func(method, path, body string, headers map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for key, value := range headers {
				req.Header.Set(key, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		assertEncryption := func(name string, response *http.Response) {
			t.Helper()
			if response.Header.Get("x-amz-server-side-encryption") != "aws:kms" || response.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || response.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
				t.Fatalf("%s encryption headers %v", name, response.Header)
			}
		}
		created, createdBody := request(http.MethodPost, "/multipart-encryption/object?uploads", "", map[string]string{"x-amz-server-side-encryption": "aws:kms", "x-amz-server-side-encryption-aws-kms-key-id": keyID, "x-amz-server-side-encryption-bucket-key-enabled": "true"})
		var upload struct {
			ID string `xml:"UploadId"`
		}
		if created.StatusCode != http.StatusOK || xml.Unmarshal(createdBody, &upload) != nil || upload.ID == "" {
			t.Fatalf("create upload %d %s", created.StatusCode, createdBody)
		}
		assertEncryption("create", created)
		part, _ := request(http.MethodPut, "/multipart-encryption/object?partNumber=1&uploadId="+upload.ID, "body", nil)
		if part.StatusCode != http.StatusOK || part.Header.Get("ETag") == "" || part.Header.Get("Content-Type") != "" || part.Header.Get("Content-Length") != "0" {
			t.Fatalf("upload part %d %v", part.StatusCode, part.Header)
		}
		assertEncryption("part", part)
		manifest := "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
		completed, _ := request(http.MethodPost, "/multipart-encryption/object?uploadId="+upload.ID, manifest, nil)
		if completed.StatusCode != http.StatusOK {
			t.Fatalf("complete upload %d", completed.StatusCode)
		}
		assertEncryption("complete", completed)
		stored, body := request(http.MethodGet, "/multipart-encryption/object", "", nil)
		if stored.StatusCode != http.StatusOK || string(body) != "body" {
			t.Fatalf("stored object %d %q", stored.StatusCode, body)
		}
		assertEncryption("get", stored)
	})

	t.Run("Given customer multipart encryption When uploading parts Then matching headers are required", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		headers := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       base64.StdEncoding.EncodeToString(rawKey),
			"x-amz-server-side-encryption-customer-key-MD5":   base64.StdEncoding.EncodeToString(digest[:]),
		}
		request := func(method, path, body string, extra map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for key, value := range extra {
				req.Header.Set(key, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		assertEncryption := func(name string, response *http.Response) {
			t.Helper()
			if response.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" || response.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] || response.Header.Get("x-amz-server-side-encryption") != "" {
				t.Fatalf("%s customer encryption headers %v", name, response.Header)
			}
		}
		created, createdBody := request(http.MethodPost, "/multipart-customer-encryption/object?uploads", "", headers)
		var upload struct {
			ID string `xml:"UploadId"`
		}
		if created.StatusCode != http.StatusOK || xml.Unmarshal(createdBody, &upload) != nil || upload.ID == "" {
			t.Fatalf("create customer upload %d %s", created.StatusCode, createdBody)
		}
		assertEncryption("create", created)
		partPath := "/multipart-customer-encryption/object?partNumber=1&uploadId=" + upload.ID
		if part, _ := request(http.MethodPut, partPath, "body", nil); part.StatusCode != http.StatusBadRequest {
			t.Fatalf("part without customer key %d", part.StatusCode)
		}
		part, _ := request(http.MethodPut, partPath, "body", headers)
		if part.StatusCode != http.StatusOK || part.Header.Get("ETag") == "" {
			t.Fatalf("upload customer part %d %v", part.StatusCode, part.Header)
		}
		assertEncryption("part", part)
		manifest := "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
		completed, _ := request(http.MethodPost, "/multipart-customer-encryption/object?uploadId="+upload.ID, manifest, nil)
		if completed.StatusCode != http.StatusOK {
			t.Fatalf("complete customer upload %d", completed.StatusCode)
		}
		assertEncryption("complete", completed)
		stored, body := request(http.MethodGet, "/multipart-customer-encryption/object", "", headers)
		if stored.StatusCode != http.StatusOK || string(body) != "body" {
			t.Fatalf("stored customer object %d %q", stored.StatusCode, body)
		}
		assertEncryption("get", stored)
	})

	t.Run("Given an expired browser policy When POSTing a file Then S3 rejects it without storing the object", func(t *testing.T) {
		res := do(http.MethodPut, "/post-policy", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "forms/expired.txt")
		_ = writer.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"expiration":"1960-01-01T00:00:00Z","conditions":[]}`)))
		_ = writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		_ = writer.WriteField("x-amz-credential", "test/20260827/us-east-1/s3/aws4_request")
		_ = writer.WriteField("x-amz-date", "20260827T000000Z")
		_ = writer.WriteField("x-amz-signature", "signature")
		file, _ := writer.CreateFormFile("file", "expired.txt")
		_, _ = file.Write([]byte("must not persist"))
		_ = writer.Close()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/post-policy", &payload)
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("AccessDenied")) || !bytes.Contains(fault, []byte("Policy expired")) {
			t.Fatalf("expired form %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/post-policy/forms/expired.txt", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expired form persisted object: %d", res.StatusCode)
		}
	})

	t.Run("Given a non-empty bucket When DELETE bucket Then it is rejected and preserved", func(t *testing.T) {
		res := do(http.MethodDelete, "/demo", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusConflict || !bytes.Contains(fault, []byte("BucketNotEmpty")) {
			t.Fatalf("delete non-empty bucket %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/demo/readme.txt", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "hello" {
			t.Fatalf("object after rejected delete %d %q", res.StatusCode, body)
		}
	})

	t.Run("Given tags in CreateBucketConfiguration When creating a bucket Then the tags are persisted atomically", func(t *testing.T) {
		configuration := []byte(`<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>storage</Value></Tag><Tag><Key>env</Key><Value>test</Value></Tag></Tags></CreateBucketConfiguration>`)
		res := do(http.MethodPut, "/create-tagged", configuration, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("tagged create %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/create-tagged?tagging", nil, "")
		tags, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(tags, []byte("<Key>team</Key>")) || !bytes.Contains(tags, []byte("<Value>test</Value>")) {
			t.Fatalf("created tags %d %s", res.StatusCode, tags)
		}
		res = do(http.MethodPut, "/create-tagged", configuration, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusConflict || !bytes.Contains(fault, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("tagged recreation %d %s", res.StatusCode, fault)
		}
		invalid := []byte(`<CreateBucketConfiguration><Tags><Tag><Key>duplicate</Key><Value>one</Value></Tag><Tag><Key>duplicate</Key><Value>two</Value></Tag></Tags></CreateBucketConfiguration>`)
		res = do(http.MethodPut, "/invalid-create-tags", invalid, "")
		fault, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidTag")) {
			t.Fatalf("invalid create tags %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodHead, "/invalid-create-tags", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid tags reserved bucket: %d", res.StatusCode)
		}
	})

	t.Run("Given object ownership When creating a bucket Then ownership controls are persisted", func(t *testing.T) {
		res := do(http.MethodPut, "/create-owned", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("owned create %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/create-owned?ownershipControls", nil, "")
		controls, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(controls, []byte("<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>")) || bytes.Contains(controls, []byte("<member>")) {
			t.Fatalf("created ownership %d %s", res.StatusCode, controls)
		}
		res = do(http.MethodPut, "/invalid-create-owned", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidArgument")) {
			t.Fatalf("invalid ownership %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodHead, "/invalid-create-owned", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid ownership reserved bucket: %d", res.StatusCode)
		}
	})

	t.Run("Given ownership controls When replacing and deleting them Then S3 validates and persists the rule", func(t *testing.T) {
		res := do(http.MethodPut, "/ownership-controls", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create ownership-controls bucket %d", res.StatusCode)
		}
		valid := []byte(`<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>`)
		res = do(http.MethodPut, "/ownership-controls?ownershipControls", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put ownership controls %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule>")) {
			t.Fatalf("get ownership controls %d %s", res.StatusCode, body)
		}
		multiple := []byte(`<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>`)
		res = do(http.MethodPut, "/ownership-controls?ownershipControls", multiple, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("multiple ownership controls %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ObjectOwnership>ObjectWriter</ObjectOwnership>")) {
			t.Fatalf("controls after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/ownership-controls?ownershipControls", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete ownership controls %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("OwnershipControlsNotFoundError")) {
			t.Fatalf("get deleted ownership controls %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a public access block When replacing and deleting it Then S3 validates and defaults the flags", func(t *testing.T) {
		res := do(http.MethodPut, "/public-access-block", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create public-access-block bucket %d", res.StatusCode)
		}
		valid := []byte(`<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`)
		res = do(http.MethodPut, "/public-access-block?publicAccessBlock", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put public access block %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<BlockPublicAcls>true</BlockPublicAcls>")) || !bytes.Contains(body, []byte("<BlockPublicPolicy>false</BlockPublicPolicy>")) || !bytes.Contains(body, []byte("<IgnorePublicAcls>false</IgnorePublicAcls>")) || !bytes.Contains(body, []byte("<RestrictPublicBuckets>false</RestrictPublicBuckets>")) || bytes.Contains(body, []byte("GetPublicAccessBlockResult")) {
			t.Fatalf("get public access block %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<PublicAccessBlockConfiguration><Unknown>true</Unknown></PublicAccessBlockConfiguration>`)
		res = do(http.MethodPut, "/public-access-block?publicAccessBlock", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid public access block %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<BlockPublicAcls>true</BlockPublicAcls>")) {
			t.Fatalf("public access block after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/public-access-block?publicAccessBlock", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete public access block %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchPublicAccessBlockConfiguration")) {
			t.Fatalf("get deleted public access block %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket logging When enabling and disabling it Then S3 validates and persists the destination", func(t *testing.T) {
		for _, bucket := range []string{"logging-bdd-source", "logging-bdd-target"} {
			res := do(http.MethodPut, "/"+bucket, nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s bucket %d", bucket, res.StatusCode)
			}
		}
		res := do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<LoggingEnabled>")) {
			t.Fatalf("default logging %d %s", res.StatusCode, body)
		}
		valid := []byte(`<BucketLoggingStatus><LoggingEnabled><TargetBucket>logging-bdd-target</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`)
		res = do(http.MethodPut, "/logging-bdd-source?logging", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put logging %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<TargetBucket>logging-bdd-target</TargetBucket>")) || !bytes.Contains(body, []byte("<TargetPrefix>logs/</TargetPrefix>")) || bytes.Contains(body, []byte("GetBucketLoggingResult")) {
			t.Fatalf("get logging %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<BucketLoggingStatus><LoggingEnabled><TargetBucket>missing</TargetBucket></LoggingEnabled></BucketLoggingStatus>`)
		res = do(http.MethodPut, "/logging-bdd-source?logging", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidTargetBucketForLogging")) {
			t.Fatalf("invalid logging %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/logging-bdd-source?logging", []byte(`<BucketLoggingStatus/>`), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("disable logging %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<LoggingEnabled>")) {
			t.Fatalf("disabled logging %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket CORS When replacing and deleting it Then S3 validates and persists the rules", func(t *testing.T) {
		res := do(http.MethodPut, "/cors-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create CORS bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchCORSConfiguration")) {
			t.Fatalf("default CORS %d %s", res.StatusCode, body)
		}
		valid := []byte(`<CORSConfiguration><CORSRule><ID>read</ID><AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>https://example.test</AllowedOrigin><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>`)
		res = do(http.MethodPut, "/cors-bdd?cors", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put CORS %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<AllowedMethod>GET</AllowedMethod>")) || !bytes.Contains(body, []byte("<AllowedOrigin>https://example.test</AllowedOrigin>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketCorsResult")) {
			t.Fatalf("get CORS %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<CORSConfiguration><CORSRule><AllowedMethod>OPTIONS</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>`)
		res = do(http.MethodPut, "/cors-bdd?cors", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidRequest")) {
			t.Fatalf("invalid CORS %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ID>read</ID>")) {
			t.Fatalf("CORS after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/cors-bdd?cors", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete CORS %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchCORSConfiguration")) {
			t.Fatalf("get deleted CORS %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a bucket website When replacing and deleting it Then S3 validates and persists the routing", func(t *testing.T) {
		websiteBucket := func(bucket, method, path string) (*http.Response, []byte) {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = bucket + ".s3-website.localhost.localstack.cloud"
			response, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		website := func(method, path string) (*http.Response, []byte) {
			return websiteBucket("website-bdd", method, path)
		}
		res := do(http.MethodPut, "/website-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create website bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchWebsiteConfiguration")) {
			t.Fatalf("default website %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/")
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<li>Code: NoSuchWebsiteConfiguration</li>")) {
			t.Fatalf("unconfigured website endpoint %d %s", res.StatusCode, body)
		}
		res, body = websiteBucket("missing-website-bdd", http.MethodGet, "/")
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<li>Code: NoSuchBucket</li>")) {
			t.Fatalf("missing website bucket %d %s", res.StatusCode, body)
		}
		valid := []byte(`<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><Protocol>https</Protocol><ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`)
		res = do(http.MethodPut, "/website-bdd?website", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put website %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Suffix>index.html</Suffix>")) || !bytes.Contains(body, []byte("<ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketWebsiteResult")) {
			t.Fatalf("get website %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<WebsiteConfiguration><IndexDocument><Suffix>dir/index.html</Suffix></IndexDocument></WebsiteConfiguration>`)
		res = do(http.MethodPut, "/website-bdd?website", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid website %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Suffix>index.html</Suffix>")) {
			t.Fatalf("website after invalid put %d %s", res.StatusCode, body)
		}
		for _, object := range []struct{ key, body string }{{"index.html", "index"}, {"error.html", "error"}} {
			res = do(http.MethodPut, "/website-bdd/"+object.key, []byte(object.body), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put website object %s = %d", object.key, res.StatusCode)
			}
		}
		res, body = website(http.MethodGet, "/")
		if res.StatusCode != http.StatusOK || string(body) != "index" {
			t.Fatalf("website index %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/missing")
		if res.StatusCode != http.StatusNotFound || string(body) != "error" {
			t.Fatalf("website error document %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/docs/key")
		if res.StatusCode != http.StatusMovedPermanently || res.Header.Get("Location") != "https://website-bdd.s3-website.localhost.localstack.cloud/manual/key" || len(body) != 0 {
			t.Fatalf("website routing rule %d %#v %s", res.StatusCode, res.Header, body)
		}
		res, body = website(http.MethodPost, "/")
		if res.StatusCode != http.StatusMethodNotAllowed || !bytes.Contains(body, []byte("<li>Method: POST</li>")) {
			t.Fatalf("website method %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/website-bdd?website", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete website %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchWebsiteConfiguration")) {
			t.Fatalf("get deleted website %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket lifecycle When configuring objects Then S3 returns LocalStack expiration headers", func(t *testing.T) {
		res := do(http.MethodPut, "/lifecycle-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create lifecycle bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchLifecycleConfiguration")) {
			t.Fatalf("default lifecycle %d %s", res.StatusCode, body)
		}
		valid := []byte(`<LifecycleConfiguration><Rule><ID>expire</ID><Filter></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
		res = do(http.MethodPut, "/lifecycle-bdd?lifecycle", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 || res.Header.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" {
			t.Fatalf("put lifecycle %d %s headers=%v", res.StatusCode, body, res.Header)
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<LifecycleConfiguration")) || !bytes.Contains(body, []byte("<Rule>")) || bytes.Contains(body, []byte("<member>")) || res.Header.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" {
			t.Fatalf("get lifecycle %d %s headers=%v", res.StatusCode, body, res.Header)
		}
		res = do(http.MethodPut, "/lifecycle-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("x-amz-expiration"), `rule-id="expire"`) {
			t.Fatalf("put lifecycle object %d headers=%v", res.StatusCode, res.Header)
		}
		res = do(http.MethodHead, "/lifecycle-bdd/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("x-amz-expiration"), `rule-id="expire"`) {
			t.Fatalf("head lifecycle object %d headers=%v", res.StatusCode, res.Header)
		}
		invalid := []byte(`<LifecycleConfiguration><Rule><ID>invalid</ID><Filter><Prefix>a</Prefix><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></Filter><Status>Enabled</Status></Rule></LifecycleConfiguration>`)
		res = do(http.MethodPut, "/lifecycle-bdd?lifecycle", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid lifecycle %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/lifecycle-bdd?lifecycle", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete lifecycle %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchLifecycleConfiguration")) {
			t.Fatalf("get deleted lifecycle %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given named bucket configurations When managing them Then S3 preserves LocalStack ordering and errors", func(t *testing.T) {
		res := do(http.MethodPut, "/named-configuration-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create named configuration bucket %d", res.StatusCode)
		}
		for _, configurationID := range []string{"z-analysis", "a-analysis"} {
			body := []byte(`<AnalyticsConfiguration><Id>` + configurationID + `</Id><Filter><Prefix>` + configurationID + `/</Prefix></Filter></AnalyticsConfiguration>`)
			res = do(http.MethodPut, "/named-configuration-bdd?analytics&id="+configurationID, body, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put analytics configuration %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/named-configuration-bdd?analytics", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if first, second := bytes.Index(body, []byte("<Id>a-analysis</Id>")), bytes.Index(body, []byte("<Id>z-analysis</Id>")); res.StatusCode != http.StatusOK || first < 0 || second < first || strings.Count(string(body), "<AnalyticsConfiguration>") != 2 || bytes.Contains(body, []byte("<member>")) {
			t.Fatalf("list analytics configurations %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<AnalyticsConfiguration><Id>body-id</Id></AnalyticsConfiguration>`)
		res = do(http.MethodPut, "/named-configuration-bdd?analytics&id=request-id", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("mismatched analytics configuration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodDelete, "/named-configuration-bdd?analytics&id=missing", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchConfiguration")) {
			t.Fatalf("delete missing analytics configuration %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket and object ACLs When replacing policies Then S3 validates and round trips AWS XML", func(t *testing.T) {
		res := do(http.MethodPut, "/acl-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create ACL bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd?acl", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Owner>")) || !bytes.Contains(body, []byte("<Permission>FULL_CONTROL</Permission>")) || bytes.Contains(body, []byte("GetBucketAclResult")) {
			t.Fatalf("default bucket ACL %d %s", res.StatusCode, body)
		}
		public := []byte(`<AccessControlPolicy><Owner><ID>000000000000</ID><DisplayName>mirror</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>000000000000</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>`)
		res = do(http.MethodPut, "/acl-bdd?acl", public, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put bucket ACL %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd?acl", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>")) || strings.Count(string(body), "<Grant>") != 2 {
			t.Fatalf("public bucket ACL %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/acl-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put ACL object %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/acl-bdd/object?acl", []byte(`<AccessControlPolicy/>`), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedACLError")) {
			t.Fatalf("invalid object ACL %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/acl-bdd/object?acl", public, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object ACL %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd/object?acl", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || strings.Count(string(body), "<Grant>") != 2 || bytes.Contains(body, []byte("GetObjectAclResult")) {
			t.Fatalf("public object ACL %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an object delete marker When reading or writing its ACL Then S3 returns LocalStack faults", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/acl-marker-bdd", ""},
			{http.MethodPut, "/acl-marker-bdd?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
			{http.MethodPut, "/acl-marker-bdd/object", "body"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		res := do(http.MethodDelete, "/acl-marker-bdd/object", nil, "")
		res.Body.Close()
		versionID := res.Header.Get("x-amz-version-id")
		if res.StatusCode != http.StatusNoContent || res.Header.Get("x-amz-delete-marker") != "true" || versionID == "" {
			t.Fatalf("delete marker %d %#v", res.StatusCode, res.Header)
		}
		for _, request := range []struct {
			method, path string
			status       int
			code, field  string
		}{
			{http.MethodPut, "/acl-marker-bdd/object?acl", http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>PUT</Method>"},
			{http.MethodGet, "/acl-marker-bdd/object?acl", http.StatusNotFound, "NoSuchKey", "<Key>object</Key>"},
			{http.MethodPut, "/acl-marker-bdd/object?acl&versionId=" + url.QueryEscape(versionID), http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>PUT</Method>"},
			{http.MethodGet, "/acl-marker-bdd/object?acl&versionId=" + url.QueryEscape(versionID), http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>GET</Method>"},
		} {
			res = do(request.method, request.path, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != request.status || !bytes.Contains(body, []byte("<Code>"+request.code+"</Code>")) || !bytes.Contains(body, []byte(request.field)) {
				t.Fatalf("%s %s = %d %s", request.method, request.path, res.StatusCode, body)
			}
		}
	})

	t.Run("Given a bucket policy When replacing it Then S3 validates and returns the exact JSON", func(t *testing.T) {
		res := do(http.MethodPut, "/policy-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create policy bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchBucketPolicy")) {
			t.Fatalf("missing policy %d %s", res.StatusCode, body)
		}
		policy := []byte(`{"Version":"2012-10-17", "Statement":[{"Effect":"Allow","Principal":"*"}]}`)
		res = do(http.MethodPut, "/policy-bdd?policy", policy, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put policy %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" || !bytes.Equal(body, policy) {
			t.Fatalf("get policy %d content-type=%q body=%s", res.StatusCode, res.Header.Get("Content-Type"), body)
		}
		res = do(http.MethodPut, "/policy-bdd?policy", append([]byte{' '}, policy...), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedPolicy")) {
			t.Fatalf("invalid policy %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Equal(body, policy) {
			t.Fatalf("policy after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/policy-bdd?policy", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete policy %d", res.StatusCode)
			}
		}
	})

	t.Run("Given LocalStack-unsupported operations When requested Then S3 reports them explicitly", func(t *testing.T) {
		for _, test := range []struct {
			path      string
			operation string
		}{
			{"/unsupported-bdd?policyStatus", "GetBucketPolicyStatus"},
			{"/unsupported-bdd/key?torrent", "GetObjectTorrent"},
		} {
			res := do(http.MethodGet, test.path, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNotImplemented || res.Header.Get("x-mirror-not-implemented") != "aws.s3."+test.operation || !bytes.Contains(body, []byte("MirrorNotImplemented")) {
				t.Fatalf("%s: %d %#v %s", test.operation, res.StatusCode, res.Header, body)
			}
		}
	})

	t.Run("Given bucket encryption When replacing it Then S3 validates and applies the default", func(t *testing.T) {
		res := do(http.MethodPut, "/encryption-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create encryption bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/encryption-bdd?encryption", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("default encryption %d %s", res.StatusCode, body)
		}
		valid := []byte(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:000000000000:key/bdd</KMSMasterKeyID></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>`)
		res = do(http.MethodPut, "/encryption-bdd?encryption", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put encryption %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/encryption-bdd?encryption", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ServerSideEncryptionConfiguration")) || !bytes.Contains(body, []byte("<SSEAlgorithm>aws:kms</SSEAlgorithm>")) || !bytes.Contains(body, []byte("<BucketKeyEnabled>true</BucketKeyEnabled>")) || bytes.Contains(body, []byte("<member>")) {
			t.Fatalf("get encryption %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>key-id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
		res = do(http.MethodPut, "/encryption-bdd?encryption", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid encryption %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/encryption-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/bdd" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
			t.Fatalf("inherited encryption %d %v", res.StatusCode, res.Header)
		}
		for range 2 {
			res = do(http.MethodDelete, "/encryption-bdd?encryption", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete encryption %d", res.StatusCode)
			}
		}
	})

	t.Run("Given bucket notifications When configuring and clearing them Then matching objects reach the queue", func(t *testing.T) {
		res := do(http.MethodPut, "/notification-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create notification bucket %d", res.StatusCode)
		}
		if err := deps.Store.Scope("000000000000", "us-east-1").Collection("queues").Put(context.Background(), "notification-queue", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		valid := []byte(`<NotificationConfiguration><QueueConfiguration><Id>images</Id><Queue>arn:aws:sqs:us-east-1:000000000000:notification-queue</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>`)
		res = do(http.MethodPut, "/notification-bdd?notification", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put notifications %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/notification-bdd?notification", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<QueueConfiguration>")) || !bytes.Contains(body, []byte("<Name>Prefix</Name>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketNotificationConfigurationResult")) {
			t.Fatalf("get notifications %d %s", res.StatusCode, body)
		}
		for _, key := range []string{"images/photo.jpg", "images/photo.png", "docs/photo.jpg"} {
			res = do(http.MethodPut, "/notification-bdd/"+key, []byte(key), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put notification object %s: %d", key, res.StatusCode)
			}
		}
		messages, _, err := deps.Store.Scope("000000000000", "us-east-1").Collection("msgs:notification-queue").List(context.Background(), "", "", 0)
		matched := false
		for _, message := range messages {
			matched = matched || bytes.Contains(message.Value, []byte("images/photo.jpg"))
		}
		if err != nil || len(messages) != 2 || !matched {
			t.Fatalf("notification messages = %#v, err=%v", messages, err)
		}
		invalid := []byte(`<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:missing</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`)
		res = do(http.MethodPut, "/notification-bdd?notification", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid notifications %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/notification-bdd?notification", []byte(`<NotificationConfiguration/>`), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("clear notifications %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/notification-bdd?notification", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("QueueConfiguration")) {
			t.Fatalf("cleared notifications %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given request payment When changing the payer Then S3 validates and persists it", func(t *testing.T) {
		res := do(http.MethodPut, "/request-payment", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create request-payment bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/request-payment?requestPayment", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Payer>BucketOwner</Payer>")) {
			t.Fatalf("default request payer %d %s", res.StatusCode, body)
		}
		valid := []byte(`<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`)
		res = do(http.MethodPut, "/request-payment?requestPayment", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put request payer %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<RequestPaymentConfiguration><Payer>Invalid</Payer></RequestPaymentConfiguration>`)
		res = do(http.MethodPut, "/request-payment?requestPayment", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid request payer %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/request-payment?requestPayment", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Payer>Requester</Payer>")) || bytes.Contains(body, []byte("GetBucketRequestPaymentResult")) {
			t.Fatalf("request payer after invalid put %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given acceleration When changing its status Then S3 validates and persists it", func(t *testing.T) {
		res := do(http.MethodPut, "/accelerate-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create accelerate bucket %d", res.StatusCode)
		}
		valid := []byte(`<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`)
		res = do(http.MethodPut, "/accelerate-bdd?accelerate", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put acceleration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/accelerate-bdd?accelerate", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) || bytes.Contains(body, []byte("GetBucketAccelerateConfigurationResult")) {
			t.Fatalf("get acceleration %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<AccelerateConfiguration><Status>Invalid</Status></AccelerateConfiguration>`)
		res = do(http.MethodPut, "/accelerate-bdd?accelerate", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid acceleration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/accelerate-bdd?accelerate", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) {
			t.Fatalf("acceleration after invalid put %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/accelerate.with.period", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		res = do(http.MethodPut, "/accelerate.with.period?accelerate", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidRequest")) {
			t.Fatalf("period bucket acceleration %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a globally owned bucket When another identity creates it Then ownership errors are returned", func(t *testing.T) {
		create := func(account, region string) (int, []byte) {
			t.Helper()
			var requestBody io.Reader
			if region != "us-east-1" {
				requestBody = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + region + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(http.MethodPut, ts.URL+"/global-name", requestBody)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Account-Id", account)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, body
		}
		if status, _ := create("111111111111", "us-east-1"); status != http.StatusOK {
			t.Fatalf("initial create %d", status)
		}
		if status, _ := create("111111111111", "us-east-1"); status != http.StatusOK {
			t.Fatalf("idempotent create %d", status)
		}
		if status, body := create("222222222222", "us-east-1"); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyExists")) {
			t.Fatalf("cross-account create %d %s", status, body)
		}
		if status, body := create("111111111111", "us-west-2"); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("cross-region create %d %s", status, body)
		}
	})

	t.Run("Given a regional endpoint When creating a bucket Then its location constraint must match", func(t *testing.T) {
		request := func(method, bucket, region, constraint string) (int, []byte, string) {
			t.Helper()
			var body io.Reader
			if constraint != "" {
				body = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + constraint + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(method, ts.URL+"/"+bucket, body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, response, res.Header.Get("Location")
		}
		if status, body, _ := request(http.MethodPut, "regional-missing", "us-west-2", ""); status != http.StatusBadRequest || !bytes.Contains(body, []byte("IllegalLocationConstraintException")) {
			t.Fatalf("missing constraint %d %s", status, body)
		}
		if status, body, location := request(http.MethodPut, "regional-match", "us-west-2", "us-west-2"); status != http.StatusOK || location != ts.URL+"/regional-match/" {
			t.Fatalf("matching constraint %d %s location=%q", status, body, location)
		}
		if status, body, _ := request(http.MethodGet, "regional-match?location", "us-west-2", ""); status != http.StatusOK || !bytes.Contains(body, []byte(">us-west-2</LocationConstraint>")) {
			t.Fatalf("reported location %d %s", status, body)
		}
	})

	t.Run("Given an account regional namespace When creating a bucket Then it stays in that account and Region", func(t *testing.T) {
		create := func(account, region, name string) (int, []byte, string) {
			t.Helper()
			var body io.Reader
			if region != "us-east-1" {
				body = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + region + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(http.MethodPut, ts.URL+"/"+name, body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Account-Id", account)
			req.Header.Set("X-Mirror-Region", region)
			req.Header.Set("x-amz-bucket-namespace", "account-regional")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, response, res.Header.Get("Location")
		}

		eastName := "behavior-111111111111-us-east-1-an"
		if status, body, location := create("111111111111", "us-east-1", eastName); status != http.StatusOK || location != "/"+eastName {
			t.Fatalf("east create %d %s location=%q", status, body, location)
		}
		if status, body, _ := create("111111111111", "us-east-1", eastName); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("east recreate %d %s", status, body)
		}
		if status, body, _ := create("222222222222", "us-east-1", eastName); status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBucketName")) {
			t.Fatalf("foreign suffix %d %s", status, body)
		}
		westName := "behavior-111111111111-us-west-2-an"
		if status, body, location := create("111111111111", "us-west-2", westName); status != http.StatusOK || location != "/"+westName {
			t.Fatalf("west create %d %s location=%q", status, body, location)
		}
	})

	t.Run("Given buckets in multiple Regions When listing pages Then filters and cursors are honored", func(t *testing.T) {
		request := func(method, path, region, body string) *http.Response {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return res
		}
		for _, bucket := range []struct{ name, region, body string }{
			{"list-behavior-east", "us-east-1", ""},
			{"list-behavior-west", "us-west-2", "<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>"},
		} {
			res := request(http.MethodPut, "/"+bucket.name, bucket.region, bucket.body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s: %d", bucket.name, res.StatusCode)
			}
		}
		type page struct {
			Buckets []struct {
				Name         string `xml:"Name"`
				BucketRegion string `xml:"BucketRegion"`
			} `xml:"Buckets>Bucket"`
			Prefix            string `xml:"Prefix"`
			ContinuationToken string `xml:"ContinuationToken"`
		}
		list := func(path string) page {
			t.Helper()
			res := request(http.MethodGet, path, "us-east-1", "")
			defer res.Body.Close()
			var got page
			if err := xml.NewDecoder(res.Body).Decode(&got); err != nil || res.StatusCode != http.StatusOK {
				t.Fatalf("list %s: %d %#v %v", path, res.StatusCode, got, err)
			}
			return got
		}
		first := list("/?max-buckets=1&prefix=list-behavior")
		if len(first.Buckets) != 1 || first.Buckets[0].Name != "list-behavior-east" || first.Buckets[0].BucketRegion != "us-east-1" || first.Prefix != "list-behavior" || first.ContinuationToken == "" {
			t.Fatalf("first page: %#v", first)
		}
		second := list("/?max-buckets=1&prefix=list-behavior&continuation-token=" + url.QueryEscape(first.ContinuationToken))
		if len(second.Buckets) != 1 || second.Buckets[0].Name != "list-behavior-west" || second.Buckets[0].BucketRegion != "us-west-2" || second.ContinuationToken != "" {
			t.Fatalf("second page: %#v", second)
		}
	})

	t.Run("Given a bucket When versioning changes Then its state matches AWS", func(t *testing.T) {
		res := do(http.MethodPut, "/versioning-state", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/versioning-state?versioning", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<Status>")) {
			t.Fatalf("unset versioning %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/versioning-state?versioning", []byte("<VersioningConfiguration/>"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("IllegalVersioningConfigurationException")) {
			t.Fatalf("missing versioning status %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/versioning-state?versioning", []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("enable versioning %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/versioning-state?versioning", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) {
			t.Fatalf("enabled versioning %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given versioned replication When an object is written Then its version is readable from the replica", func(t *testing.T) {
		for _, bucket := range []string{"replication-source", "replication-destination"} {
			res := do(http.MethodPut, "/"+bucket, nil, "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s: %d", bucket, res.StatusCode)
			}
			res = do(http.MethodPut, "/"+bucket+"?versioning", []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("version %s: %d", bucket, res.StatusCode)
			}
		}
		res := do(http.MethodPut, "/replication-unversioned", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create unversioned destination: %d", res.StatusCode)
		}
		unversionedConfiguration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::replication-unversioned</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(unversionedConfiguration), "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) {
			t.Fatalf("unversioned destination validation: %d %s", res.StatusCode, body)
		}
		invalidConfiguration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Tag><Key>environment</Key><Value>test</Value></Tag></Filter><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replication-destination</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(invalidConfiguration), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) {
			t.Fatalf("tag delete-marker validation: %d %s", res.StatusCode, body)
		}
		configuration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Priority>1</Priority><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix><Tag><Key>environment</Key><Value>test</Value></Tag></And></Filter><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replication-destination</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(configuration), "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("configure replication: %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/replication-source?replication", nil, "")
		gotConfiguration, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(gotConfiguration, []byte("<ReplicationConfiguration")) || !bytes.Contains(gotConfiguration, []byte("<Rule>")) || !bytes.Contains(gotConfiguration, []byte("<Prefix>logs/</Prefix>")) || !bytes.Contains(gotConfiguration, []byte("<Tag><Key>environment</Key><Value>test</Value></Tag>")) || bytes.Contains(gotConfiguration, []byte("<member>")) {
			t.Fatalf("get replication: %d %s", res.StatusCode, gotConfiguration)
		}
		req, err := http.NewRequest(http.MethodPut, ts.URL+"/replication-source/logs/key", strings.NewReader("replicated-version"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("x-amz-tagging", "environment=test")
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		version := res.Header.Get("x-amz-version-id")
		if res.StatusCode != http.StatusOK || version == "" || res.Header.Get("x-amz-replication-status") != "COMPLETED" {
			t.Fatalf("put replicated version: %d %v", res.StatusCode, res.Header)
		}
		res = do(http.MethodGet, "/replication-destination/logs/key?versionId="+url.QueryEscape(version), nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "replicated-version" || res.Header.Get("x-amz-version-id") != version || res.Header.Get("x-amz-replication-status") != "REPLICA" {
			t.Fatalf("get replicated version: %d %q %v", res.StatusCode, body, res.Header)
		}
	})

	t.Run("Given object versions When current version is deleted Then previous version is current", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/version-restore", ""},
			{http.MethodPut, "/version-restore?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
			{http.MethodPut, "/version-restore/key", "old"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode >= 300 {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		newer := do(http.MethodPut, "/version-restore/key", []byte("new"), "")
		io.Copy(io.Discard, newer.Body)
		newer.Body.Close()
		version := newer.Header.Get("x-amz-version-id")
		deleted := do(http.MethodDelete, "/version-restore/key?versionId="+url.QueryEscape(version), nil, "")
		io.Copy(io.Discard, deleted.Body)
		deleted.Body.Close()
		if deleted.StatusCode != http.StatusNoContent || deleted.Header.Get("x-amz-version-id") != version {
			t.Fatalf("delete version = %d %v", deleted.StatusCode, deleted.Header)
		}
		restored := do(http.MethodGet, "/version-restore/key", nil, "")
		body, _ := io.ReadAll(restored.Body)
		restored.Body.Close()
		if restored.StatusCode != http.StatusOK || string(body) != "old" || restored.Header.Get("x-amz-version-id") == version {
			t.Fatalf("restored = %d %q %v", restored.StatusCode, body, restored.Header)
		}
	})

	t.Run("Given a legal hold When deleting Then only a delete marker is allowed", func(t *testing.T) {
		res := do(http.MethodPut, "/object-lock", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create object-lock bucket = %d", res.StatusCode)
		}
		put := do(http.MethodPut, "/object-lock/key", []byte("locked"), "")
		put.Body.Close()
		version := put.Header.Get("x-amz-version-id")
		hold := do(http.MethodPut, "/object-lock/key?legal-hold&versionId="+url.QueryEscape(version), []byte("<LegalHold><Status>ON</Status></LegalHold>"), "")
		hold.Body.Close()
		if hold.StatusCode != http.StatusOK {
			t.Fatalf("put legal hold = %d", hold.StatusCode)
		}
		permanent := do(http.MethodDelete, "/object-lock/key?versionId="+url.QueryEscape(version), nil, "")
		body, _ := io.ReadAll(permanent.Body)
		permanent.Body.Close()
		if permanent.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("AccessDenied")) {
			t.Fatalf("permanent delete = %d %s", permanent.StatusCode, body)
		}
		marker := do(http.MethodDelete, "/object-lock/key", nil, "")
		marker.Body.Close()
		if marker.StatusCode != http.StatusNoContent || marker.Header.Get("x-amz-delete-marker") != "true" {
			t.Fatalf("simple delete = %d %v", marker.StatusCode, marker.Header)
		}
	})

	t.Run("Given default retention When writing Then the new version is protected", func(t *testing.T) {
		configuration := []byte("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>3</Days></DefaultRetention></Rule></ObjectLockConfiguration>")
		configured := do(http.MethodPut, "/object-lock?object-lock", configuration, "")
		configured.Body.Close()
		if configured.StatusCode != http.StatusOK {
			t.Fatalf("configure default retention = %d", configured.StatusCode)
		}
		put := do(http.MethodPut, "/object-lock/default-retention", []byte("locked"), "")
		put.Body.Close()
		version := put.Header.Get("x-amz-version-id")
		retention := do(http.MethodGet, "/object-lock/default-retention?retention&versionId="+url.QueryEscape(version), nil, "")
		body, _ := io.ReadAll(retention.Body)
		retention.Body.Close()
		if retention.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Mode>GOVERNANCE</Mode>")) || !bytes.Contains(body, []byte("<RetainUntilDate>1970-01-04T00:00:00Z</RetainUntilDate>")) {
			t.Fatalf("default retention = %d %s", retention.StatusCode, body)
		}
		deleted := do(http.MethodDelete, "/object-lock/default-retention?versionId="+url.QueryEscape(version), nil, "")
		fault, _ := io.ReadAll(deleted.Body)
		deleted.Body.Close()
		if deleted.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("AccessDenied")) {
			t.Fatalf("delete retained version = %d %s", deleted.StatusCode, fault)
		}
	})

	t.Run("Given object versions When multi-delete is quiet Then only errors are returned", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/multi-delete", ""},
			{http.MethodPut, "/multi-delete?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			res.Body.Close()
			if res.StatusCode >= 300 {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		first := do(http.MethodPut, "/multi-delete/key", []byte("old"), "")
		first.Body.Close()
		firstVersion := first.Header.Get("x-amz-version-id")
		second := do(http.MethodPut, "/multi-delete/key", []byte("new"), "")
		second.Body.Close()
		secondVersion := second.Header.Get("x-amz-version-id")
		probe := []byte("<Delete><Object><Key>key</Key></Object></Delete>")
		probeDigest := md5.Sum(probe)
		for _, test := range []struct{ name, checksum, algorithm, code string }{
			{"missing", "", "", "MissingContentMD5"},
			{"mismatched", "AA==", "", "BadDigest"},
			{"algorithm without value", base64.StdEncoding.EncodeToString(probeDigest[:]), "CRC32", "InvalidRequest"},
		} {
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/multi-delete?delete", bytes.NewReader(probe))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if test.checksum != "" {
				req.Header.Set("Content-MD5", test.checksum)
			}
			if test.algorithm != "" {
				req.Header.Set("x-amz-sdk-checksum-algorithm", test.algorithm)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte(test.code)) {
				t.Fatalf("%s checksum = %d %s", test.name, res.StatusCode, body)
			}
		}
		res := do(http.MethodPost, "/multi-delete?delete", []byte("<Delete><Object><Key>key</Key><VersionId>"+secondVersion+"</VersionId></Object></Delete>"), "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)) || !bytes.Contains(body, []byte("<VersionId>"+secondVersion+"</VersionId>")) || bytes.Contains(body, []byte("<DeleteMarker>")) {
			t.Fatalf("verbose multi-delete = %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/multi-delete/key", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "old" {
			t.Fatalf("restored multi-delete = %d %q", res.StatusCode, body)
		}
		res = do(http.MethodPost, "/multi-delete?delete", []byte("<Delete><Object><Key>key</Key><VersionId>missing</VersionId></Object><Object><Key>key</Key><VersionId>"+firstVersion+"</VersionId></Object><Quiet>true</Quiet></Delete>"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Code>NoSuchVersion</Code>")) || !bytes.Contains(body, []byte("<VersionId>missing</VersionId>")) || bytes.Contains(body, []byte("<Deleted>")) {
			t.Fatalf("quiet multi-delete = %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an invalid bucket name When creating it Then it is rejected", func(t *testing.T) {
		for _, name := range []string{"ab", "192.168.5.4", "reserved--table-s3"} {
			res := do(http.MethodPut, "/"+name, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBucketName")) {
				t.Fatalf("name %q = %d %s", name, res.StatusCode, body)
			}
		}
		res := do(http.MethodPut, "/valid-after-invalid", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("valid create after invalid names %d", res.StatusCode)
		}
	})

	t.Run("Given an invalid storage class When PUT object Then it is rejected", func(t *testing.T) {
		res := do(http.MethodPut, "/classes", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/classes/object", []byte("body"), "glacier")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidStorageClass")) {
			t.Fatalf("invalid storage class %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/classes/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid storage class created object: %d", res.StatusCode)
		}
	})

	t.Run("Given an XXHash checksum When PUT and GET object Then the checksum round trips", func(t *testing.T) {
		res := do(http.MethodPut, "/xxhash-behavior", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		request := func(method, path string, body []byte, checksum string) *http.Response {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if checksum != "" {
				req.Header.Set("x-amz-checksum-xxhash64", checksum)
			} else if method == http.MethodGet {
				req.Header.Set("x-amz-checksum-mode", "ENABLED")
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		checksum := "jLhB20DmroM="
		res = request(http.MethodPut, "/xxhash-behavior/object", []byte("123456789"), checksum)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-checksum-xxhash64") != checksum || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("put xxhash %d %v", res.StatusCode, res.Header)
		}
		res = request(http.MethodGet, "/xxhash-behavior/object", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "123456789" || res.Header.Get("x-amz-checksum-xxhash64") != checksum || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("get xxhash %d %q %v", res.StatusCode, body, res.Header)
		}
		res = request(http.MethodPut, "/xxhash-behavior/bad", []byte("123456789"), "AA==")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("BadDigest")) {
			t.Fatalf("bad xxhash %d %s", res.StatusCode, fault)
		}
	})

	t.Run("Given an oversized object key When PUT object Then it is rejected", func(t *testing.T) {
		res := do(http.MethodPut, "/key-limits", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		path := "/key-limits/" + strings.Repeat("x", 1025)
		res = do(http.MethodPut, path, []byte("body"), "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("KeyTooLongError")) {
			t.Fatalf("oversized object key %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, path, nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("oversized object key created object: %d", res.StatusCode)
		}
	})

	t.Run("Given an archived object When restored Then reads become available", func(t *testing.T) {
		res := do(http.MethodPut, "/cold", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/cold/archive.txt", []byte("cold-data"), "GLACIER")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("put archive %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/cold/archive.txt", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-storage-class") != "GLACIER" || res.Header.Get("x-amz-restore") != "" {
			t.Fatalf("head unrestored archive %d %v", res.StatusCode, res.Header)
		}
		res = do(http.MethodGet, "/cold/archive.txt", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("InvalidObjectState")) {
			t.Fatalf("get unrestored archive %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodPost, "/cold/archive.txt?restore", []byte(`<RestoreRequest><Days>1</Days></RestoreRequest>`), "")
		res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("restore archive %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cold/archive.txt", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "cold-data" || res.Header.Get("x-amz-restore") == "" {
			t.Fatalf("get restored archive %d %q %v", res.StatusCode, body, res.Header)
		}
	})
}

func TestSTSGetCallerIdentity(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sts"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader([]byte("Action=GetCallerIdentity&Version=2011-06-15")))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, b)
	}
	if !bytes.Contains(b, []byte("Account")) && !bytes.Contains(b, []byte("account")) && !bytes.Contains(b, []byte("000000000000")) {
		t.Fatalf("body %s", b)
	}
}
