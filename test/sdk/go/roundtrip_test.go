package sdk_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
)

func TestAWSSDKPresignedSignatureValidation(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.S3ValidatePresignedSignatures = true
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	clockResponse, err := http.Head(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	clockResponse.Body.Close()
	signedAt, err := http.ParseTime(clockResponse.Header.Get("Date"))
	if err != nil {
		t.Fatal(err)
	}
	awscfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(awscfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(ts.URL)
		options.UsePathStyle = true
	})
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("signed")}); err != nil {
		t.Fatal(err)
	}
	policyRaw, _ := json.Marshal(map[string]any{"expiration": signedAt.Add(time.Hour).Format(time.RFC3339), "conditions": []any{map[string]any{"bucket": "signed"}}})
	policy := base64.StdEncoding.EncodeToString(policyRaw)
	postDate := signedAt.UTC().Format("20060102T150405Z")
	postSignature := hex.EncodeToString(hmacSHA256Test(streamingV4SigningKey(postDate[:8]), policy))
	for name, signature := range map[string]string{"valid post policy": postSignature, "tampered post policy": strings.Repeat("0", 64)} {
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", strings.ReplaceAll(name, " ", "-"))
		_ = writer.WriteField("policy", policy)
		_ = writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		_ = writer.WriteField("x-amz-credential", "test/"+postDate[:8]+"/us-east-1/s3/aws4_request")
		_ = writer.WriteField("x-amz-date", postDate)
		_ = writer.WriteField("x-amz-signature", signature)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("browser upload"))
		_ = writer.Close()
		request, _ := http.NewRequest(http.MethodPost, ts.URL+"/signed", &payload)
		request.Header.Set("Content-Type", writer.FormDataContentType())
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		want := http.StatusNoContent
		if name == "tampered post policy" {
			want = http.StatusForbidden
		}
		if response.StatusCode != want {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	if _, err := client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("signed"), Key: aws.String("object"), Body: strings.NewReader("verified")}); err != nil {
		t.Fatal(err)
	}
	tamperedConfig := awscfg
	tamperedConfig.HTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		request = request.Clone(request.Context())
		request.Header = request.Header.Clone()
		before, _, ok := strings.Cut(request.Header.Get("Authorization"), "Signature=")
		if !ok {
			return nil, fmt.Errorf("signed request has no signature")
		}
		request.Header.Set("Authorization", before+"Signature="+strings.Repeat("0", 64))
		return http.DefaultTransport.RoundTrip(request)
	})}
	tamperedClient := s3.NewFromConfig(tamperedConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(ts.URL)
		options.UsePathStyle = true
	})
	if _, err := tamperedClient.ListBuckets(context.Background(), &s3.ListBucketsInput{}); err == nil || !strings.Contains(err.Error(), "SignatureDoesNotMatch") {
		t.Fatalf("tampered authorization: %v", err)
	}
	sigV2, err := http.NewRequest(http.MethodGet, ts.URL+"/signed/object", nil)
	if err != nil {
		t.Fatal(err)
	}
	sigV2Date := signedAt.Format(http.TimeFormat)
	sigV2MAC := hmac.New(sha1.New, []byte("test"))
	_, _ = sigV2MAC.Write([]byte("GET\n\n\n" + sigV2Date + "\n/signed/object"))
	sigV2.Header.Set("Date", sigV2Date)
	sigV2.Header.Set("Authorization", "AWS test:"+base64.StdEncoding.EncodeToString(sigV2MAC.Sum(nil)))
	response, err := http.DefaultClient.Do(sigV2)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || string(body) != "verified" {
		t.Fatalf("valid SigV2 authorization: %d %s", response.StatusCode, body)
	}
	sigV2.Header.Set("Authorization", "AWS test:AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	response, err = http.DefaultClient.Do(sigV2)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
		t.Fatalf("tampered SigV2 authorization: %d %s", response.StatusCode, body)
	}
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("streaming")}); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		payload string
		status  int
	}{"valid stream": {"hello", http.StatusOK}, "tampered stream": {"jello", http.StatusForbidden}} {
		response, err := http.DefaultClient.Do(streamingSignatureRequest(ts.URL, tc.payload, signedAt))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("unsigned")}); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		checksum string
		status   int
	}{"valid unsigned trailer": {"mnG7TA==", http.StatusOK}, "bad unsigned checksum": {"AAAAAA==", http.StatusBadRequest}} {
		response, err := http.DefaultClient.Do(streamingUnsignedTrailerRequest(ts.URL, tc.checksum, signedAt))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("trailers")}); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		checksum, signedChecksum string
		status                   int
	}{
		"valid trailer":    {"mnG7TA==", "mnG7TA==", http.StatusOK},
		"tampered trailer": {"AAAAAA==", "mnG7TA==", http.StatusForbidden},
		"bad checksum":     {"AAAAAA==", "AAAAAA==", http.StatusBadRequest},
	} {
		response, err := http.DefaultClient.Do(streamingTrailerSignatureRequest(ts.URL, tc.checksum, tc.signedChecksum, signedAt))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	presigned, err := s3.NewPresignClient(client).PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("signed"), Key: aws.String("object")}, func(options *s3.PresignOptions) { options.Expires = time.Minute })
	if err != nil {
		t.Fatal(err)
	}
	do := func(rawURL string) (*http.Response, []byte) {
		t.Helper()
		request, err := http.NewRequest(presigned.Method, rawURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header = presigned.SignedHeader.Clone()
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		return response, body
	}
	if response, body := do(presigned.URL); response.StatusCode != http.StatusOK || string(body) != "verified" {
		t.Fatalf("valid presign %d %s", response.StatusCode, body)
	}
	unsigned, _ := http.NewRequest(presigned.Method, presigned.URL, nil)
	unsigned.Header = presigned.SignedHeader.Clone()
	unsigned.Header.Set("X-Amz-User-Agent", "test")
	response, err = http.DefaultClient.Do(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
		t.Fatalf("unsigned x-amz header %d %s", response.StatusCode, body)
	}
	malformedCredential := strings.ReplaceAll(presigned.URL, "%2F", "%252F")
	if response, body := do(malformedCredential); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>AuthorizationQueryParametersError</Code>")) || !bytes.Contains(body, []byte("Credential is mal-formed")) {
		t.Fatalf("malformed credential %d %s", response.StatusCode, body)
	}
	tampered, err := url.Parse(presigned.URL)
	if err != nil {
		t.Fatal(err)
	}
	query := tampered.Query()
	query.Set("X-Amz-Signature", strings.Repeat("0", 64))
	tampered.RawQuery = query.Encode()
	if response, body := do(tampered.String()); response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
		t.Fatalf("tampered presign %d %s", response.StatusCode, body)
	}
	mrap, err := s3.NewPresignClient(s3.NewFromConfig(awscfg)).PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("arn:aws:s3::123456789012:accesspoint:" + strings.Repeat("a", 12) + ".mrap"), Key: aws.String("object")}, func(options *s3.PresignOptions) { options.Expires = time.Minute })
	if err != nil {
		t.Fatal(err)
	}
	mrapURL, err := url.Parse(mrap.URL)
	if err != nil {
		t.Fatal(err)
	}
	doMRAP := func() (*http.Response, []byte) {
		t.Helper()
		request, err := http.NewRequest(mrap.Method, ts.URL+mrapURL.RequestURI(), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = mrapURL.Host
		request.Header = mrap.SignedHeader.Clone()
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		return response, body
	}
	if response, body := doMRAP(); response.StatusCode != http.StatusNotFound || bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
		t.Fatalf("valid SigV4A presign %d %s", response.StatusCode, body)
	}
	mrapQuery := mrapURL.Query()
	mrapQuery.Set("X-Amz-Signature", "00")
	mrapURL.RawQuery = mrapQuery.Encode()
	if response, body := doMRAP(); response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
		t.Fatalf("tampered SigV4A presign %d %s", response.StatusCode, body)
	}
	temporaryKey := "temporary"
	temporarySecret := rt.Deps.Rand.Derive(temporaryKey).Hex(40)
	temporaryToken := rt.Deps.Rand.Derive(temporaryKey + "tok").Hex(32)
	if err := rt.Deps.Store.Scope("_mirror", "global").Collection("stsk").Put(context.Background(), temporaryKey, []byte("000000000000")); err != nil {
		t.Fatal(err)
	}
	temporaryConfig, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"), config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(temporaryKey, temporarySecret, temporaryToken)))
	if err != nil {
		t.Fatal(err)
	}
	temporaryClient := s3.NewFromConfig(temporaryConfig, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(ts.URL)
		options.UsePathStyle = true
	})
	temporaryPresign, err := s3.NewPresignClient(temporaryClient).PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("signed"), Key: aws.String("object")}, func(options *s3.PresignOptions) { options.Expires = time.Minute })
	if err != nil {
		t.Fatal(err)
	}
	temporaryRequest, err := http.NewRequest(temporaryPresign.Method, temporaryPresign.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	temporaryRequest.Header = temporaryPresign.SignedHeader.Clone()
	temporaryResponse, err := http.DefaultClient.Do(temporaryRequest)
	if err != nil {
		t.Fatal(err)
	}
	temporaryBody, _ := io.ReadAll(temporaryResponse.Body)
	temporaryResponse.Body.Close()
	if temporaryResponse.StatusCode != http.StatusOK || string(temporaryBody) != "verified" {
		t.Fatalf("valid session presign %d %s", temporaryResponse.StatusCode, temporaryBody)
	}
	invalidTokenURL, err := url.Parse(temporaryPresign.URL)
	if err != nil {
		t.Fatal(err)
	}
	invalidTokenQuery := invalidTokenURL.Query()
	invalidTokenQuery.Set("X-Amz-Security-Token", "wrong")
	invalidTokenURL.RawQuery = invalidTokenQuery.Encode()
	temporaryRequest.URL = invalidTokenURL
	temporaryResponse, err = http.DefaultClient.Do(temporaryRequest)
	if err != nil {
		t.Fatal(err)
	}
	temporaryBody, _ = io.ReadAll(temporaryResponse.Body)
	temporaryResponse.Body.Close()
	if temporaryResponse.StatusCode != http.StatusBadRequest || !bytes.Contains(temporaryBody, []byte("<Code>InvalidToken</Code>")) {
		t.Fatalf("invalid session token %d %s", temporaryResponse.StatusCode, temporaryBody)
	}
	portClient := s3.NewFromConfig(awscfg, func(options *s3.Options) {
		options.BaseEndpoint = aws.String("http://s3.localhost.localstack.cloud:4566")
		options.UsePathStyle = true
	})
	portPresign, err := s3.NewPresignClient(portClient).PresignGetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("signed"), Key: aws.String("object")}, func(options *s3.PresignOptions) { options.Expires = time.Minute })
	if err != nil {
		t.Fatal(err)
	}
	signedPortURL, err := url.Parse(portPresign.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"s3.localhost.localstack.cloud:443", "s3.localhost.localstack.cloud"} {
		request, err := http.NewRequest(portPresign.Method, ts.URL+signedPortURL.RequestURI(), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = host
		request.Header = portPresign.SignedHeader.Clone()
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != "verified" {
			t.Fatalf("signed gateway host %q: %d %s", host, response.StatusCode, body)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestS3ETagWireCasingContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	for _, target := range []struct {
		path string
		body io.Reader
	}{{"/etag-casing", nil}, {"/etag-casing/object", strings.NewReader("body")}} {
		request, _ := http.NewRequest(http.MethodPut, ts.URL+target.path, target.body)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatal(response.Status)
		}
	}
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "GET /etag-casing/object HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", ts.Listener.Addr()); err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(raw, []byte("\r\nETag: \"")) || bytes.Contains(raw, []byte("\r\nEtag:")) {
		t.Fatalf("raw response:\n%s", raw)
	}
}

func TestS3MultipartContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	do := func(method, path, body string) (*http.Response, []byte) {
		t.Helper()
		request, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			t.Fatalf("%s %s: %d %s", method, path, response.StatusCode, payload)
		}
		return response, payload
	}
	do(http.MethodPut, "/multipart-contract", "")
	_, initiated := do(http.MethodPost, "/multipart-contract/plain?uploads", "")
	var upload struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(initiated, &upload); err != nil {
		t.Fatal(err)
	}
	part, _ := do(http.MethodPut, "/multipart-contract/plain?partNumber=1&uploadId="+url.QueryEscape(upload.UploadID), "plain")
	_, listed := do(http.MethodGet, "/multipart-contract/plain?uploadId="+url.QueryEscape(upload.UploadID), "")
	if bytes.Contains(listed, []byte("ChecksumAlgorithm")) || bytes.Contains(listed, []byte("ChecksumType")) {
		t.Fatalf("list exposed checksum metadata: %s", listed)
	}
	manifest := "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
	_, completed := do(http.MethodPost, "/multipart-contract/plain?uploadId="+url.QueryEscape(upload.UploadID), manifest)
	if bytes.Contains(completed, []byte("ChecksumCRC64NVME")) || bytes.Contains(completed, []byte("ChecksumType")) {
		t.Fatalf("completion exposed checksum metadata: %s", completed)
	}
	_, got := do(http.MethodGet, "/multipart-contract/plain", "")
	if string(got) != "plain" {
		t.Fatalf("body = %q", got)
	}
	request, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/composite?uploads", nil)
	request.Header.Set("x-amz-checksum-algorithm", "CRC32")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	initiated, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || xml.Unmarshal(initiated, &upload) != nil {
		t.Fatalf("initiate composite: %d %s", response.StatusCode, initiated)
	}
	part, _ = do(http.MethodPut, "/multipart-contract/composite?partNumber=1&uploadId="+url.QueryEscape(upload.UploadID), "checked")
	manifest = "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/composite?uploadId="+url.QueryEscape(upload.UploadID), strings.NewReader(manifest))
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	fault, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(fault, []byte("missing for part 1")) {
		t.Fatalf("missing composite checksum: %d %s", response.StatusCode, fault)
	}
	manifest = "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber><ChecksumCRC32>" + part.Header.Get("x-amz-checksum-crc32") + "</ChecksumCRC32></Part></CompleteMultipartUpload>"
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/composite?uploadId="+url.QueryEscape(upload.UploadID), strings.NewReader(manifest))
	request.Header.Set("x-amz-checksum-crc32", "AA==")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	completed, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Contains(completed, []byte("<ChecksumCRC32>")) || bytes.Contains(completed, []byte("<ChecksumCRC32>AA==</ChecksumCRC32>")) {
		t.Fatalf("ignored composite aggregate: %d %s", response.StatusCode, completed)
	}
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/alternate?uploads", nil)
	request.Header.Set("x-amz-checksum-algorithm", "SHA256")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	initiated, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || xml.Unmarshal(initiated, &upload) != nil {
		t.Fatalf("initiate alternate checksum: %d %s", response.StatusCode, initiated)
	}
	part, _ = do(http.MethodPut, "/multipart-contract/alternate?partNumber=1&uploadId="+url.QueryEscape(upload.UploadID), "checked")
	manifest = "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber><ChecksumSHA256>" + part.Header.Get("x-amz-checksum-sha256") + "</ChecksumSHA256></Part></CompleteMultipartUpload>"
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/alternate?uploadId="+url.QueryEscape(upload.UploadID), strings.NewReader(manifest))
	request.Header.Set("x-amz-checksum-crc32", "AAAAAA==")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	fault, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(fault, []byte("The sha256 you specified did not match the calculated checksum.")) {
		t.Fatalf("alternate object checksum: %d %s", response.StatusCode, fault)
	}
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/full?uploads", nil)
	request.Header.Set("x-amz-checksum-algorithm", "CRC32")
	request.Header.Set("x-amz-checksum-type", "FULL_OBJECT")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	initiated, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || xml.Unmarshal(initiated, &upload) != nil {
		t.Fatalf("initiate full-object checksum: %d %s", response.StatusCode, initiated)
	}
	part, _ = do(http.MethodPut, "/multipart-contract/full?partNumber=1&uploadId="+url.QueryEscape(upload.UploadID), "checked")
	manifest = "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/full?uploadId="+url.QueryEscape(upload.UploadID), strings.NewReader(manifest))
	request.Header.Set("x-amz-checksum-crc32", part.Header.Get("x-amz-checksum-crc32"))
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	fault, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(fault, []byte("The crc32 you specified did not match the calculated checksum.")) {
		t.Fatalf("implicit full-object checksum type: %d %s", response.StatusCode, fault)
	}
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/full?uploadId="+url.QueryEscape(upload.UploadID), strings.NewReader(manifest))
	request.Header.Set("x-amz-checksum-crc32", part.Header.Get("x-amz-checksum-crc32"))
	request.Header.Set("x-amz-checksum-type", "FULL_OBJECT")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	completed, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Contains(completed, []byte("<ChecksumType>FULL_OBJECT</ChecksumType>")) {
		t.Fatalf("explicit full-object checksum type: %d %s", response.StatusCode, completed)
	}
	createPart := func(key string) (string, string) {
		_, initiated := do(http.MethodPost, "/multipart-contract/"+key+"?uploads", "")
		if err := xml.Unmarshal(initiated, &upload); err != nil {
			t.Fatal(err)
		}
		part, _ := do(http.MethodPut, "/multipart-contract/"+key+"?partNumber=1&uploadId="+url.QueryEscape(upload.UploadID), "sized")
		return upload.UploadID, part.Header.Get("ETag")
	}
	completeSize := func(key, uploadID, etag, size string) (int, []byte) {
		manifest := "<CompleteMultipartUpload><Part><ETag>" + etag + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
		request, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-contract/"+key+"?uploadId="+url.QueryEscape(uploadID), strings.NewReader(manifest))
		request.Header.Set("x-amz-mp-object-size", size)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		payload, _ := io.ReadAll(response.Body)
		response.Body.Close()
		return response.StatusCode, payload
	}
	zeroID, zeroETag := createPart("zero-size")
	if status, payload := completeSize("zero-size", zeroID, zeroETag, "0"); status != http.StatusOK {
		t.Fatalf("zero object size: %d %s", status, payload)
	}
	mismatchID, mismatchETag := createPart("mismatched-size")
	if status, payload := completeSize("mismatched-size", mismatchID, mismatchETag, "4"); status != http.StatusBadRequest || !bytes.Contains(payload, []byte("header value 4 does not match what was computed: 5")) {
		t.Fatalf("mismatched object size: %d %s", status, payload)
	}
}

func TestAWSChunkedFramingContract(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3"}
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	request, _ := http.NewRequest(http.MethodPut, ts.URL+"/chunk-errors", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	for name, tc := range map[string]struct {
		decoded string
		body    string
	}{
		"missing decoded length": {body: "5\r\nhello\r\n0\r\n\r\n"},
		"non-integer length":     {decoded: "test", body: "5\r\nhello\r\n0\r\n\r\n"},
		"truncated chunk":        {decoded: "5", body: "5\r\nhello"},
	} {
		request, _ := http.NewRequest(http.MethodPut, ts.URL+"/chunk-errors/object", strings.NewReader(tc.body))
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		if tc.decoded != "" {
			request.Header.Set("X-Amz-Decoded-Content-Length", tc.decoded)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	for name, tc := range map[string]struct {
		encoding string
		want     string
	}{"chunked only": {"aws-chunked", ""}, "preserve gzip": {"gzip, aws-chunked", "gzip"}} {
		path := "/chunk-errors/" + strings.ReplaceAll(name, " ", "-")
		request, _ := http.NewRequest(http.MethodPut, ts.URL+path, strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
		request.Header.Set("Content-Encoding", tc.encoding)
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		request.Header.Set("X-Amz-Decoded-Content-Length", "5")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		request, _ = http.NewRequest(http.MethodGet, ts.URL+path, nil)
		request.Header.Set("Accept-Encoding", "identity")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != "hello" || response.Header.Get("Content-Encoding") != tc.want {
			t.Fatalf("%s: %d encoding=%q body=%q", name, response.StatusCode, response.Header.Get("Content-Encoding"), body)
		}
	}
	request, _ = http.NewRequest(http.MethodPost, ts.URL+"/chunk-errors/part?uploads", nil)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var upload struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.NewDecoder(response.Body).Decode(&upload); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	partURL := ts.URL + "/chunk-errors/part?partNumber=1&uploadId=" + url.QueryEscape(upload.UploadID)
	putPart := func(body string) *http.Response {
		request, _ := http.NewRequest(http.MethodPut, partURL, strings.NewReader(body))
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
		request.Header.Set("X-Amz-Decoded-Content-Length", "10")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	response = putPart("\r\nHello Blob\r\n0;chunk-signature=invalid\r\n")
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError || !bytes.Contains(body, []byte("<Code>InternalError</Code>")) {
		t.Fatalf("invalid part: %d %s", response.StatusCode, body)
	}
	response = putPart("a;chunk-signature=first\r\nHello Blob\r\n0;chunk-signature=last\r\n")
	response.Body.Close()
	sum := md5.Sum([]byte("Hello Blob"))
	if response.StatusCode != http.StatusOK || response.Header.Get("ETag") != `"`+hex.EncodeToString(sum[:])+`"` {
		t.Fatalf("valid retry: %d etag=%q", response.StatusCode, response.Header.Get("ETag"))
	}
}

func TestSigV4AStreamingContract(t *testing.T) {
	t.Setenv("MIRROR_CLOCK", "controllable")
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.S3ValidatePresignedSignatures = true
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Deps.Clock.Advance(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Sub(rt.Deps.Clock.Now())); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	for _, bucket := range []string{"v4a", "v4a-trailers", "v4a-unsigned"} {
		request, _ := http.NewRequest(http.MethodPut, ts.URL+"/"+bucket, nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("create %s: %s", bucket, response.Status)
		}
	}
	for name, tc := range map[string]struct {
		trailer  bool
		payload  string
		checksum string
		status   int
	}{
		"valid payload":    {payload: "hello", status: http.StatusOK},
		"tampered payload": {payload: "jello", status: http.StatusForbidden},
		"valid trailer":    {trailer: true, payload: "hello", checksum: "mnG7TA==", status: http.StatusOK},
		"tampered trailer": {trailer: true, payload: "hello", checksum: "AAAAAA==", status: http.StatusForbidden},
	} {
		response, err := http.DefaultClient.Do(sigV4AStreamingFixture(ts.URL, tc.trailer, tc.payload, tc.checksum))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
	for name, tc := range map[string]struct {
		checksum string
		signed   bool
		status   int
	}{
		"valid unsigned trailer":        {"mnG7TA==", false, http.StatusOK},
		"bad unsigned checksum":         {"AAAAAA==", false, http.StatusBadRequest},
		"signed unsigned payload chunk": {"mnG7TA==", true, http.StatusForbidden},
	} {
		response, err := http.DefaultClient.Do(sigV4AUnsignedStreamingFixture(ts.URL, tc.checksum, tc.signed))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != tc.status {
			t.Fatalf("%s: %d %s", name, response.StatusCode, body)
		}
	}
}

func sigV4AUnsignedStreamingFixture(endpoint, checksum string, signedChunk bool) *http.Request {
	extension := ""
	if signedChunk {
		extension = ";chunk-signature=unexpected"
	}
	raw := "5" + extension + "\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + checksum + "\r\n\r\n"
	request, _ := http.NewRequest(http.MethodPut, endpoint+"/v4a-unsigned/object", strings.NewReader(raw))
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

func sigV4AStreamingFixture(endpoint string, trailer bool, payload, checksum string) *http.Request {
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
	request, _ := http.NewRequest(http.MethodPut, endpoint+path, strings.NewReader(raw))
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

func streamingSignatureRequest(endpoint, payload string, signedAt time.Time) *http.Request {
	request, _ := http.NewRequest(http.MethodPut, endpoint+"/streaming/object", nil)
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	request.Header.Set("X-Amz-Date", signedAt.UTC().Format("20060102T150405Z"))
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"
	seed := signStreamingV4Authorization(request, signedHeaders)
	signatures := signStreamingV4Chunks(request.Header.Get("X-Amz-Date"), seed, [][]byte{[]byte("hello"), nil})
	raw := "5;chunk-signature=" + signatures[0] + "\r\n" + payload + "\r\n0;chunk-signature=" + signatures[1] + "\r\n\r\n"
	request.Body = io.NopCloser(strings.NewReader(raw))
	request.ContentLength = int64(len(raw))
	request.Header.Set("Authorization", streamingV4Authorization(request, signedHeaders, seed))
	return request
}

func streamingTrailerSignatureRequest(endpoint, checksum, signedChecksum string, signedAt time.Time) *http.Request {
	request, _ := http.NewRequest(http.MethodPut, endpoint+"/trailers/object", nil)
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", signedAt.UTC().Format("20060102T150405Z"))
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer"
	seed := signStreamingV4Authorization(request, signedHeaders)
	signatures := signStreamingV4Chunks(request.Header.Get("X-Amz-Date"), seed, [][]byte{[]byte("hello"), nil})
	trailerSignature := signStreamingV4Trailer(request.Header.Get("X-Amz-Date"), signatures[1], "x-amz-checksum-crc32c", signedChecksum)
	raw := "5;chunk-signature=" + signatures[0] + "\r\nhello\r\n0;chunk-signature=" + signatures[1] + "\r\nx-amz-checksum-crc32c:" + checksum + "\r\nx-amz-trailer-signature:" + trailerSignature + "\r\n\r\n"
	request.Body = io.NopCloser(strings.NewReader(raw))
	request.ContentLength = int64(len(raw))
	request.Header.Set("Authorization", streamingV4Authorization(request, signedHeaders, seed))
	return request
}

func streamingUnsignedTrailerRequest(endpoint, checksum string, signedAt time.Time) *http.Request {
	raw := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + checksum + "\r\n\r\n"
	request, _ := http.NewRequest(http.MethodPut, endpoint+"/unsigned/object", strings.NewReader(raw))
	request.Host = "s3.localhost.localstack.cloud:4566"
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", signedAt.UTC().Format("20060102T150405Z"))
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer"
	signature := signStreamingV4Authorization(request, signedHeaders)
	request.Header.Set("Authorization", streamingV4Authorization(request, signedHeaders, signature))
	return request
}

func signStreamingV4Authorization(request *http.Request, signedHeaders string) string {
	var canonicalHeaders strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		value := request.Header.Get(name)
		if name == "host" {
			value = request.Host
		}
		canonicalHeaders.WriteString(name + ":" + value + "\n")
	}
	canonical := strings.Join([]string{request.Method, request.URL.EscapedPath(), "", canonicalHeaders.String(), signedHeaders, request.Header.Get("X-Amz-Content-Sha256")}, "\n")
	requestHash := sha256.Sum256([]byte(canonical))
	date := request.Header.Get("X-Amz-Date")
	scope := date[:8] + "/us-east-1/s3/aws4_request"
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", date, scope, hex.EncodeToString(requestHash[:])}, "\n")
	return hex.EncodeToString(hmacSHA256Test(streamingV4SigningKey(date[:8]), stringToSign))
}

func streamingV4Authorization(request *http.Request, signedHeaders, signature string) string {
	date := request.Header.Get("X-Amz-Date")
	return "AWS4-HMAC-SHA256 Credential=test/" + date[:8] + "/us-east-1/s3/aws4_request,SignedHeaders=" + signedHeaders + ",Signature=" + signature
}

func signStreamingV4Chunks(date, previous string, chunks [][]byte) []string {
	empty := sha256.Sum256(nil)
	scope := date[:8] + "/us-east-1/s3/aws4_request"
	key := streamingV4SigningKey(date[:8])
	signatures := make([]string, len(chunks))
	for i, chunk := range chunks {
		hash := sha256.Sum256(chunk)
		stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256-PAYLOAD", date, scope, previous, hex.EncodeToString(empty[:]), hex.EncodeToString(hash[:])}, "\n")
		signatures[i] = hex.EncodeToString(hmacSHA256Test(key, stringToSign))
		previous = signatures[i]
	}
	return signatures
}

func signStreamingV4Trailer(date, previous, name, value string) string {
	hash := sha256.Sum256([]byte(name + ":" + value + "\n"))
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256-TRAILER", date, date[:8] + "/us-east-1/s3/aws4_request", previous, hex.EncodeToString(hash[:])}, "\n")
	return hex.EncodeToString(hmacSHA256Test(streamingV4SigningKey(date[:8]), stringToSign))
}

func streamingV4SigningKey(date string) []byte {
	dateKey := hmacSHA256Test([]byte("AWS4test"), date)
	regionKey := hmacSHA256Test(dateKey, "us-east-1")
	serviceKey := hmacSHA256Test(regionKey, "s3")
	return hmacSHA256Test(serviceKey, "aws4_request")
}

func hmacSHA256Test(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(value))
	return mac.Sum(nil)
}

func TestAWSSDKRoundTripS3DynamoDBSQS(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.s3", "aws.dynamodb", "aws.sqs"}
	cfg.Seed = "sdk-rt"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	awscfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}

	s3c := s3.NewFromConfig(awscfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	for operation, err := range map[string]error{
		"GetObject": func() error {
			_, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("does-not-exist"), Key: aws.String("foobar")})
			return err
		}(),
		"DeleteBucket": func() error {
			_, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("does-not-exist")})
			return err
		}(),
		"GetBucketNotificationConfiguration": func() error {
			_, err := s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("does-not-exist")})
			return err
		}(),
	} {
		if err == nil || !strings.Contains(err.Error(), "NoSuchBucket") || !strings.Contains(err.Error(), "StatusCode: 404") {
			t.Fatalf("%s missing bucket: %v", operation, err)
		}
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-list-pagination")}); err != nil {
		t.Fatal(err)
	}
	invalidEncoding := s3types.EncodingType("value")
	for operation, err := range map[string]error{
		"ListObjects": func() error {
			_, err := s3c.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("sdk-list-pagination"), EncodingType: invalidEncoding})
			return err
		}(),
		"ListObjectsV2": func() error {
			_, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), EncodingType: invalidEncoding})
			return err
		}(),
		"ListObjectVersions": func() error {
			_, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-list-pagination"), EncodingType: invalidEncoding})
			return err
		}(),
		"ListMultipartUploads": func() error {
			_, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String("sdk-list-pagination"), EncodingType: invalidEncoding})
			return err
		}(),
	} {
		if err == nil || !strings.Contains(err.Error(), "InvalidArgument") || !strings.Contains(err.Error(), "Invalid Encoding Method specified in Request") {
			t.Fatalf("%s invalid encoding: %v", operation, err)
		}
	}
	listKeys := []string{"folder/aSubfolder/subFile1", "folder/aSubfolder/subFile2", "folder/file1", "folder/file2"}
	for _, key := range listKeys {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-list-pagination"), Key: aws.String(key), Body: strings.NewReader("content")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-list-pagination"), Key: aws.String("encoded/a b+"), Body: strings.NewReader("content")}); err != nil {
		t.Fatal(err)
	}
	encodedList, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("encoded/"), EncodingType: s3types.EncodingTypeUrl})
	if err != nil || len(encodedList.Contents) != 1 || aws.ToString(encodedList.Contents[0].Key) != "encoded/a%20b%2B" || aws.ToString(encodedList.Prefix) != "encoded/" || encodedList.EncodingType != s3types.EncodingTypeUrl {
		t.Fatalf("encoded list: %#v %v", encodedList, err)
	}
	zeroList, err := s3c.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("encoded/"), MaxKeys: aws.Int32(0)})
	if err != nil || len(zeroList.Contents) != 1 || aws.ToInt32(zeroList.MaxKeys) != 1000 {
		t.Fatalf("zero max V1 list: %#v %v", zeroList, err)
	}
	zeroListV2, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("encoded/"), MaxKeys: aws.Int32(0)})
	if err != nil || len(zeroListV2.Contents) != 1 || aws.ToInt32(zeroListV2.MaxKeys) != 1000 {
		t.Fatalf("zero max V2 list: %#v %v", zeroListV2, err)
	}
	listKeys = append(listKeys, "encoded/a b+")
	firstList, err := s3c.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
	if err != nil || len(firstList.CommonPrefixes) != 1 || aws.ToString(firstList.CommonPrefixes[0].Prefix) != "folder/aSubfolder/" || len(firstList.Contents) != 0 || aws.ToString(firstList.NextMarker) != "folder/aSubfolder/" || !aws.ToBool(firstList.IsTruncated) {
		t.Fatalf("first list page: %#v %v", firstList, err)
	}
	secondList, err := s3c.ListObjects(context.Background(), &s3.ListObjectsInput{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), Marker: firstList.NextMarker})
	if err != nil || len(secondList.Contents) != 1 || aws.ToString(secondList.Contents[0].Key) != "folder/file1" || secondList.Contents[0].Owner == nil || aws.ToString(secondList.Contents[0].Owner.ID) != "000000000000" || secondList.Contents[0].Owner.DisplayName != nil || aws.ToString(secondList.Marker) != "folder/aSubfolder/" || aws.ToString(secondList.NextMarker) != "folder/file1" {
		t.Fatalf("second list page: %#v %v", secondList, err)
	}
	firstListV2, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
	if err != nil || len(firstListV2.CommonPrefixes) != 1 || aws.ToInt32(firstListV2.KeyCount) != 1 || aws.ToString(firstListV2.NextContinuationToken) != "Zm9sZGVyL2ZpbGUx" {
		t.Fatalf("first list V2 page: %#v %v", firstListV2, err)
	}
	secondListV2, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1), ContinuationToken: firstListV2.NextContinuationToken})
	if err != nil || len(secondListV2.Contents) != 1 || aws.ToString(secondListV2.Contents[0].Key) != "folder/file1" || aws.ToString(secondListV2.ContinuationToken) != "Zm9sZGVyL2ZpbGUx" || aws.ToString(secondListV2.NextContinuationToken) != "Zm9sZGVyL2ZpbGUy" {
		t.Fatalf("second list V2 page: %#v %v", secondListV2, err)
	}
	defaultOwnerV2, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/file"), MaxKeys: aws.Int32(1)})
	if err != nil || len(defaultOwnerV2.Contents) != 1 || defaultOwnerV2.Contents[0].Owner != nil {
		t.Fatalf("default V2 owner: %#v %v", defaultOwnerV2, err)
	}
	fetchedOwnerV2, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("folder/file"), MaxKeys: aws.Int32(1), FetchOwner: aws.Bool(true)})
	if err != nil || len(fetchedOwnerV2.Contents) != 1 || fetchedOwnerV2.Contents[0].Owner == nil || aws.ToString(fetchedOwnerV2.Contents[0].Owner.ID) != "000000000000" || fetchedOwnerV2.Contents[0].Owner.DisplayName != nil {
		t.Fatalf("fetched V2 owner: %#v %v", fetchedOwnerV2, err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-list-pagination"), Key: aws.String("checksum"), Body: strings.NewReader("content"), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256}); err != nil {
		t.Fatal(err)
	}
	checksummed, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-list-pagination"), Prefix: aws.String("checksum")})
	if err != nil || len(checksummed.Contents) != 1 || len(checksummed.Contents[0].ChecksumAlgorithm) != 1 || checksummed.Contents[0].ChecksumAlgorithm[0] != s3types.ChecksumAlgorithmSha256 || checksummed.Contents[0].ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("listed checksum: %#v %v", checksummed, err)
	}
	listKeys = append(listKeys, "checksum")
	for _, key := range listKeys {
		if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-list-pagination"), Key: aws.String(key)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("sdk-list-pagination")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-version-list")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-version-list"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String("checksummed"), Body: strings.NewReader("body"), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256}); err != nil {
		t.Fatal(err)
	}
	checksummedVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("checksummed")})
	if err != nil || len(checksummedVersions.Versions) != 1 || len(checksummedVersions.Versions[0].ChecksumAlgorithm) != 1 || checksummedVersions.Versions[0].ChecksumAlgorithm[0] != s3types.ChecksumAlgorithmSha256 || checksummedVersions.Versions[0].ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("checksummed versions: %#v %v", checksummedVersions, err)
	}
	zeroVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("checksummed"), MaxKeys: aws.Int32(0)})
	if err != nil || len(zeroVersions.Versions) != 1 || aws.ToInt32(zeroVersions.MaxKeys) != 1000 {
		t.Fatalf("zero max version list: %#v %v", zeroVersions, err)
	}
	for _, key := range []string{"encoded/a b+", "encoded/a!b+"} {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String(key), Body: strings.NewReader("body")}); err != nil {
			t.Fatal(err)
		}
	}
	encodedVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("encoded/"), MaxKeys: aws.Int32(1), EncodingType: s3types.EncodingTypeUrl})
	if err != nil || len(encodedVersions.Versions) != 1 || aws.ToString(encodedVersions.Versions[0].Key) != "encoded/a%20b%2B" || aws.ToString(encodedVersions.Prefix) != "encoded/" || aws.ToString(encodedVersions.NextKeyMarker) != "encoded/a%20b%2B" || aws.ToString(encodedVersions.NextVersionIdMarker) == "" {
		t.Fatalf("encoded versions: %#v %v", encodedVersions, err)
	}
	encodedNext, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("encoded/"), MaxKeys: aws.Int32(1), EncodingType: s3types.EncodingTypeUrl, KeyMarker: encodedVersions.NextKeyMarker, VersionIdMarker: encodedVersions.NextVersionIdMarker})
	if err != nil || len(encodedNext.Versions) != 1 || aws.ToString(encodedNext.Versions[0].Key) != "encoded/a%21b%2B" || aws.ToString(encodedNext.KeyMarker) != "encoded/a%20b%2B" {
		t.Fatalf("encoded next version page: %#v %v", encodedNext, err)
	}
	for _, key := range []string{"folder/a/one", "folder/file1", "folder/file2"} {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String(key), Body: strings.NewReader("body")}); err != nil {
			t.Fatal(err)
		}
	}
	firstVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("folder/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1)})
	if err != nil || len(firstVersions.CommonPrefixes) != 1 || aws.ToString(firstVersions.CommonPrefixes[0].Prefix) != "folder/a/" || aws.ToString(firstVersions.NextKeyMarker) != "folder/a/" || len(firstVersions.Versions) != 0 {
		t.Fatalf("first version list page: %#v %v", firstVersions, err)
	}
	for range 5 {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String("versions/key"), Body: strings.NewReader("body")}); err != nil {
			t.Fatal(err)
		}
	}
	versionPage, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("versions/"), MaxKeys: aws.Int32(3)})
	if err != nil || len(versionPage.Versions) != 3 || aws.ToString(versionPage.NextKeyMarker) != "versions/key" || aws.ToString(versionPage.NextVersionIdMarker) == "" || !aws.ToBool(versionPage.Versions[0].IsLatest) {
		t.Fatalf("version list page: %#v %v", versionPage, err)
	}
	lastVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("versions/"), MaxKeys: aws.Int32(5), KeyMarker: versionPage.NextKeyMarker, VersionIdMarker: versionPage.NextVersionIdMarker})
	if err != nil || len(lastVersions.Versions) != 2 || aws.ToBool(lastVersions.IsTruncated) {
		t.Fatalf("last version list page: %#v %v", lastVersions, err)
	}
	for range 3 {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String("deleted-marker/key"), Body: strings.NewReader("body")}); err != nil {
			t.Fatal(err)
		}
	}
	deletedMarkerPage, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("deleted-marker/key"), MaxKeys: aws.Int32(1)})
	if err != nil || aws.ToString(deletedMarkerPage.NextVersionIdMarker) == "" {
		t.Fatalf("deleted marker first page: %#v %v", deletedMarkerPage, err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-version-list"), Key: aws.String("deleted-marker/key"), VersionId: deletedMarkerPage.NextVersionIdMarker}); err != nil {
		t.Fatal(err)
	}
	resumedVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), Prefix: aws.String("deleted-marker/key"), KeyMarker: aws.String("deleted-marker/key"), VersionIdMarker: deletedMarkerPage.NextVersionIdMarker})
	if err != nil || len(resumedVersions.Versions) != 2 {
		t.Fatalf("deleted marker next page: %#v %v", resumedVersions, err)
	}
	if _, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-version-list"), VersionIdMarker: aws.String("orphan")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("orphan version marker: %v", err)
	}
	for _, name := range []string{"ab", "192.168.5.4", "reserved--table-s3"} {
		if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(name)}); err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
			t.Fatalf("invalid bucket name %q: %v", name, err)
		}
	}
	createTags := []s3types.Tag{{Key: aws.String("team"), Value: aws.String("storage")}, {Key: aws.String("env"), Value: aws.String("test")}}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-invalid-ownership"), ObjectOwnership: s3types.ObjectOwnership("RandomValue")}); err == nil || !strings.Contains(err.Error(), "Invalid x-amz-object-ownership header: RandomValue") {
		t.Fatalf("invalid create ownership: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{Tags: createTags}, ObjectOwnership: s3types.ObjectOwnershipBucketOwnerPreferred}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if tagged, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String("sdk")}); err != nil || !reflect.DeepEqual(tagged.TagSet, createTags) {
		t.Fatalf("create bucket tags: %#v %v", tagged, err)
	}
	if _, err := s3c.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{Bucket: aws.String("sdk"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{}}}); err != nil {
		t.Fatalf("clear bucket tags: %v", err)
	}
	if _, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchTagSet") {
		t.Fatalf("cleared bucket tags: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("empty-tags"), Body: strings.NewReader("body")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("empty-tags"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{}}}); err != nil {
		t.Fatalf("clear object tags: %v", err)
	}
	if tagged, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("empty-tags")}); err != nil || len(tagged.TagSet) != 0 {
		t.Fatalf("cleared object tags: %#v %v", tagged, err)
	}
	if _, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucketPolicy") {
		t.Fatalf("default bucket policy: %v", err)
	}
	bucketPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::sdk/*"}]}`
	if _, err := s3c.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String("sdk"), Policy: aws.String(bucketPolicy)}); err != nil {
		t.Fatalf("put bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(got.Policy) != bucketPolicy {
		t.Fatalf("bucket policy round trip: %#v %v", got, err)
	}
	if _, err := s3c.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{Bucket: aws.String("sdk"), Policy: aws.String(" " + bucketPolicy)}); err == nil || !strings.Contains(err.Error(), "MalformedPolicy") {
		t.Fatalf("invalid bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(got.Policy) != bucketPolicy {
		t.Fatalf("invalid policy replaced configuration: %#v %v", got, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket policy: %v", err)
		}
	}
	if _, err := s3c.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucketPolicy") {
		t.Fatalf("get deleted bucket policy: %v", err)
	}
	if got, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || got.ServerSideEncryptionConfiguration == nil || len(got.ServerSideEncryptionConfiguration.Rules) != 1 || got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault == nil || got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAes256 || aws.ToBool(got.ServerSideEncryptionConfiguration.Rules[0].BucketKeyEnabled) {
		t.Fatalf("default bucket encryption: %#v %v", got, err)
	}
	bucketEncryptionKey := "arn:aws:kms:us-east-1:000000000000:key/sdk-default"
	kmsIdentity := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Enabled")
	bucketEncryption := &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{
		ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAwsKms, KMSMasterKeyID: aws.String(bucketEncryptionKey)},
		BucketKeyEnabled:                   aws.Bool(true),
	}}}
	if _, err := s3c.PutBucketEncryption(context.Background(), &s3.PutBucketEncryptionInput{Bucket: aws.String("sdk"), ServerSideEncryptionConfiguration: bucketEncryption}); err != nil {
		t.Fatalf("put bucket encryption: %v", err)
	}
	gotEncryption, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")})
	if err != nil || gotEncryption.ServerSideEncryptionConfiguration == nil || len(gotEncryption.ServerSideEncryptionConfiguration.Rules) != 1 || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault == nil || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAwsKms || aws.ToString(gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID) != bucketEncryptionKey || !aws.ToBool(gotEncryption.ServerSideEncryptionConfiguration.Rules[0].BucketKeyEnabled) {
		t.Fatalf("bucket encryption round trip: %#v %v", gotEncryption, err)
	}
	inheritedEncryption, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-bucket-encryption"), Body: strings.NewReader("encrypted")})
	if err != nil || inheritedEncryption.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(inheritedEncryption.SSEKMSKeyId) != bucketEncryptionKey || !aws.ToBool(inheritedEncryption.BucketKeyEnabled) {
		t.Fatalf("inherited bucket encryption: %#v %v", inheritedEncryption, err)
	}
	kmsUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-multipart"), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc64nvme})
	if err != nil {
		t.Fatalf("create KMS multipart upload: %v", err)
	}
	kmsPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-multipart"), UploadId: kmsUpload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("part")})
	if err != nil {
		t.Fatalf("upload KMS multipart part: %v", err)
	}
	kmsCompleted, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-multipart"), UploadId: kmsUpload.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: kmsPart.ETag}}}})
	if err != nil || kmsCompleted.ChecksumCRC64NVME != nil || kmsCompleted.ChecksumType != "" {
		t.Fatalf("KMS completion checksum response: %#v %v", kmsCompleted, err)
	}
	kmsHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-multipart"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil || kmsHead.ChecksumCRC64NVME == nil || kmsHead.ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("persisted KMS multipart checksum: %#v %v", kmsHead, err)
	}
	if put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-explicit-kms"), Body: strings.NewReader("encrypted"), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32, ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("sdk-default")}); err != nil || aws.ToString(put.SSEKMSKeyId) != bucketEncryptionKey {
		t.Fatalf("explicit kms put: %#v %v", put, err)
	}
	kmsCopy, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-copy"), CopySource: aws.String("sdk/sdk-explicit-kms"), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String(bucketEncryptionKey), BucketKeyEnabled: aws.Bool(true)})
	if err != nil || kmsCopy.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(kmsCopy.SSEKMSKeyId) != bucketEncryptionKey || !aws.ToBool(kmsCopy.BucketKeyEnabled) || kmsCopy.CopyObjectResult == nil || aws.ToString(kmsCopy.CopyObjectResult.ChecksumCRC32) == "" || kmsCopy.CopyObjectResult.ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("kms copy: %#v %v", kmsCopy, err)
	}
	kmsCopyGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-kms-copy"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get kms copy: %v", err)
	}
	kmsCopyBody, _ := io.ReadAll(kmsCopyGet.Body)
	_ = kmsCopyGet.Body.Close()
	if string(kmsCopyBody) != "encrypted" || kmsCopyGet.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(kmsCopyGet.SSEKMSKeyId) != bucketEncryptionKey || !aws.ToBool(kmsCopyGet.BucketKeyEnabled) || aws.ToString(kmsCopyGet.ChecksumCRC32) != aws.ToString(kmsCopy.CopyObjectResult.ChecksumCRC32) || kmsCopyGet.ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("stored kms copy: body=%q output=%#v", kmsCopyBody, kmsCopyGet)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-missing-kms"), Body: strings.NewReader("rejected"), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms, SSEKMSKeyId: aws.String("arn:aws:kms:us-east-1:000000000000:key/missing")}); err == nil || !strings.Contains(err.Error(), "KMS.NotFoundException") {
		t.Fatalf("missing kms key: %v", err)
	}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Disabled")
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-explicit-kms")}); err == nil || !strings.Contains(err.Error(), "KMS.DisabledException") {
		t.Fatalf("disabled kms read: %v", err)
	}
	spitest.SeedKMSKey(t, rt.Deps, kmsIdentity, bucketEncryptionKey, "Enabled")
	invalidEncryption := &s3types.ServerSideEncryptionConfiguration{Rules: []s3types.ServerSideEncryptionRule{{ApplyServerSideEncryptionByDefault: &s3types.ServerSideEncryptionByDefault{SSEAlgorithm: s3types.ServerSideEncryptionAes256, KMSMasterKeyID: aws.String(bucketEncryptionKey)}}}}
	if _, err := s3c.PutBucketEncryption(context.Background(), &s3.PutBucketEncryptionInput{Bucket: aws.String("sdk"), ServerSideEncryptionConfiguration: invalidEncryption}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket encryption: %v", err)
	}
	if gotEncryption, err = s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || gotEncryption.ServerSideEncryptionConfiguration == nil || gotEncryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAwsKms {
		t.Fatalf("invalid encryption replaced configuration: %#v %v", gotEncryption, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketEncryption(context.Background(), &s3.DeleteBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket encryption: %v", err)
		}
	}
	if got, err := s3c.GetBucketEncryption(context.Background(), &s3.GetBucketEncryptionInput{Bucket: aws.String("sdk")}); err != nil || got.ServerSideEncryptionConfiguration == nil || len(got.ServerSideEncryptionConfiguration.Rules) != 1 || got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault == nil || got.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.SSEAlgorithm != s3types.ServerSideEncryptionAes256 {
		t.Fatalf("deleted bucket encryption: %#v %v", got, err)
	}
	bucketACL, err := s3c.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String("sdk")})
	if err != nil || bucketACL.Owner == nil || aws.ToString(bucketACL.Owner.ID) != "000000000000" || bucketACL.Owner.DisplayName != nil || len(bucketACL.Grants) != 1 || bucketACL.Grants[0].Grantee.DisplayName != nil {
		t.Fatalf("default bucket ACL: %#v %v", bucketACL, err)
	}
	if _, err := s3c.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{Bucket: aws.String("sdk"), ACL: s3types.BucketCannedACLPublicRead}); err != nil {
		t.Fatalf("put bucket ACL: %v", err)
	}
	if bucketACL, err = s3c.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{Bucket: aws.String("sdk")}); err != nil || len(bucketACL.Grants) != 2 || aws.ToString(bucketACL.Grants[1].Grantee.URI) != "http://acs.amazonaws.com/groups/global/AllUsers" {
		t.Fatalf("public bucket ACL: %#v %v", bucketACL, err)
	}
	objectACLKey := "sdk-object-acl"
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey), Body: strings.NewReader("acl"), ACL: s3types.ObjectCannedACLPublicRead}); err != nil {
		t.Fatalf("put object ACL: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey)}); err != nil || len(objectACL.Grants) != 2 {
		t.Fatalf("public object ACL: %#v %v", objectACL, err)
	}
	privatePolicy := &s3types.AccessControlPolicy{
		Owner:  &s3types.Owner{ID: bucketACL.Owner.ID, DisplayName: bucketACL.Owner.DisplayName},
		Grants: []s3types.Grant{{Grantee: &s3types.Grantee{ID: bucketACL.Owner.ID, Type: s3types.TypeCanonicalUser}, Permission: s3types.PermissionFullControl}},
	}
	if _, err := s3c.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey), AccessControlPolicy: privatePolicy}); err != nil {
		t.Fatalf("put object ACP: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String(objectACLKey)}); err != nil || len(objectACL.Grants) != 1 || objectACL.Grants[0].Permission != s3types.PermissionFullControl {
		t.Fatalf("private object ACP: %#v %v", objectACL, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-acl-marker")}); err != nil {
		t.Fatalf("create acl marker bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-acl-marker"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable acl marker versioning: %v", err)
	}
	object, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), Body: bytes.NewReader([]byte("body"))})
	if err != nil {
		t.Fatalf("put acl marker object: %v", err)
	}
	marker, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object")})
	if err != nil || !aws.ToBool(marker.DeleteMarker) || aws.ToString(marker.VersionId) == "" {
		t.Fatalf("create acl delete marker: %#v %v", marker, err)
	}
	for _, versionID := range []*string{nil, marker.VersionId} {
		if _, err := s3c.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID, ACL: s3types.ObjectCannedACLPublicRead}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
			t.Fatalf("put delete marker acl version=%v: %v", versionID, err)
		}
	}
	if _, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object")}); err == nil || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("get current delete marker acl: %v", err)
	}
	if _, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: marker.VersionId}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
		t.Fatalf("get explicit delete marker acl: %v", err)
	}
	for _, versionID := range []*string{nil, marker.VersionId} {
		if _, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID, Tagging: &s3types.Tagging{TagSet: []s3types.Tag{}}}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
			t.Fatalf("put delete marker tags version=%v: %v", versionID, err)
		}
		if _, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
			t.Fatalf("get delete marker tags version=%v: %v", versionID, err)
		}
		if _, err := s3c.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID}); err == nil || !strings.Contains(err.Error(), "MethodNotAllowed") {
			t.Fatalf("delete delete marker tags version=%v: %v", versionID, err)
		}
	}
	for _, versionID := range []*string{marker.VersionId, object.VersionId} {
		if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-acl-marker"), Key: aws.String("object"), VersionId: versionID}); err != nil {
			t.Fatalf("delete acl marker version %v: %v", versionID, err)
		}
	}
	if _, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("sdk-acl-marker")}); err != nil {
		t.Fatalf("delete acl marker bucket: %v", err)
	}
	multipartACL, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), ACL: s3types.ObjectCannedACLPublicReadWrite, CacheControl: aws.String("max-age=60"), ContentType: aws.String("text/plain"), Metadata: map[string]string{"team": "storage"}, WebsiteRedirectLocation: aws.String("/multipart")})
	if err != nil {
		t.Fatalf("create multipart ACL: %v", err)
	}
	multipartACLPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), UploadId: multipartACL.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("acl-part")})
	if err != nil {
		t.Fatalf("upload multipart ACL part: %v", err)
	}
	if _, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl"), UploadId: multipartACL.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: multipartACLPart.ETag}}}}); err != nil {
		t.Fatalf("complete multipart ACL: %v", err)
	}
	if objectACL, err := s3c.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl")}); err != nil || len(objectACL.Grants) != 3 {
		t.Fatalf("multipart object ACL: %#v %v", objectACL, err)
	}
	if head, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("sdk-multipart-acl")}); err != nil || aws.ToString(head.CacheControl) != "max-age=60" || aws.ToString(head.ContentType) != "text/plain" || head.Metadata["team"] != "storage" || aws.ToString(head.WebsiteRedirectLocation) != "/multipart" {
		t.Fatalf("multipart object metadata: %#v %v", head, err)
	}
	ownership, err := s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")})
	if err != nil || ownership.OwnershipControls == nil {
		t.Fatalf("create bucket ownership rules: %#v %v", ownership.OwnershipControls, err)
	}
	if len(ownership.OwnershipControls.Rules) != 1 {
		t.Fatalf("create bucket ownership rule count = %d", len(ownership.OwnershipControls.Rules))
	}
	if got := ownership.OwnershipControls.Rules[0].ObjectOwnership; got != s3types.ObjectOwnershipBucketOwnerPreferred {
		t.Fatalf("create bucket ownership = %q", got)
	}
	controls := &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnershipObjectWriter}}}
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: controls}); err != nil {
		t.Fatalf("put bucket ownership controls: %v", err)
	}
	ownership, err = s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")})
	if err != nil || ownership.OwnershipControls == nil || len(ownership.OwnershipControls.Rules) != 1 || ownership.OwnershipControls.Rules[0].ObjectOwnership != s3types.ObjectOwnershipObjectWriter {
		t.Fatalf("put bucket ownership round trip: %#v %v", ownership, err)
	}
	invalidControls := &s3types.OwnershipControls{Rules: []s3types.OwnershipControlsRule{{ObjectOwnership: s3types.ObjectOwnership("invalid")}}}
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: invalidControls}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid ownership controls: %v", err)
	}
	if _, err := s3c.DeleteBucketOwnershipControls(context.Background(), &s3.DeleteBucketOwnershipControlsInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("delete bucket ownership controls: %v", err)
	}
	if _, err := s3c.GetBucketOwnershipControls(context.Background(), &s3.GetBucketOwnershipControlsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "OwnershipControlsNotFoundError") || !strings.Contains(err.Error(), "The bucket ownership controls were not found") {
		t.Fatalf("get deleted ownership controls: %v", err)
	}
	controls.Rules[0].ObjectOwnership = s3types.ObjectOwnershipBucketOwnerPreferred
	if _, err := s3c.PutBucketOwnershipControls(context.Background(), &s3.PutBucketOwnershipControlsInput{Bucket: aws.String("sdk"), OwnershipControls: controls}); err != nil {
		t.Fatalf("restore bucket ownership controls: %v", err)
	}
	publicDefaults, err := s3c.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String("sdk")})
	if err != nil || publicDefaults.PublicAccessBlockConfiguration == nil || !aws.ToBool(publicDefaults.PublicAccessBlockConfiguration.BlockPublicAcls) ||
		!aws.ToBool(publicDefaults.PublicAccessBlockConfiguration.BlockPublicPolicy) || !aws.ToBool(publicDefaults.PublicAccessBlockConfiguration.IgnorePublicAcls) ||
		!aws.ToBool(publicDefaults.PublicAccessBlockConfiguration.RestrictPublicBuckets) {
		t.Fatalf("default public access block: %#v %v", publicDefaults, err)
	}
	publicAccessBlock := &s3types.PublicAccessBlockConfiguration{BlockPublicAcls: aws.Bool(true)}
	if _, err := s3c.PutPublicAccessBlock(context.Background(), &s3.PutPublicAccessBlockInput{Bucket: aws.String("sdk"), PublicAccessBlockConfiguration: publicAccessBlock}); err != nil {
		t.Fatalf("put public access block: %v", err)
	}
	blocked, err := s3c.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String("sdk")})
	if err != nil || blocked.PublicAccessBlockConfiguration == nil || !aws.ToBool(blocked.PublicAccessBlockConfiguration.BlockPublicAcls) || aws.ToBool(blocked.PublicAccessBlockConfiguration.BlockPublicPolicy) {
		t.Fatalf("public access block round trip: %#v %v", blocked, err)
	}
	if _, err := s3c.DeletePublicAccessBlock(context.Background(), &s3.DeletePublicAccessBlockInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("delete public access block: %v", err)
	}
	if _, err := s3c.GetPublicAccessBlock(context.Background(), &s3.GetPublicAccessBlockInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchPublicAccessBlockConfiguration") {
		t.Fatalf("get deleted public access block: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled != nil {
		t.Fatalf("default bucket logging: %#v %v", logging, err)
	}
	loggingStatus := &s3types.BucketLoggingStatus{LoggingEnabled: &s3types.LoggingEnabled{TargetBucket: aws.String("sdk"), TargetPrefix: aws.String("logs/")}}
	if _, err := s3c.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String("sdk"), BucketLoggingStatus: loggingStatus}); err != nil {
		t.Fatalf("put bucket logging: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled == nil || aws.ToString(logging.LoggingEnabled.TargetBucket) != "sdk" || aws.ToString(logging.LoggingEnabled.TargetPrefix) != "logs/" {
		t.Fatalf("bucket logging round trip: %#v %v", logging, err)
	}
	if _, err := s3c.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{Bucket: aws.String("sdk"), BucketLoggingStatus: &s3types.BucketLoggingStatus{}}); err != nil {
		t.Fatalf("disable bucket logging: %v", err)
	}
	if logging, err := s3c.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{Bucket: aws.String("sdk")}); err != nil || logging.LoggingEnabled != nil {
		t.Fatalf("disabled bucket logging: %#v %v", logging, err)
	}
	if _, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchCORSConfiguration") {
		t.Fatalf("default bucket CORS: %v", err)
	}
	cors := &s3types.CORSConfiguration{CORSRules: []s3types.CORSRule{{AllowedMethods: []string{"GET", "HEAD"}, AllowedOrigins: []string{"https://example.test"}, ExposeHeaders: []string{"ETag"}, MaxAgeSeconds: aws.Int32(300)}}}
	if _, err := s3c.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{Bucket: aws.String("sdk"), CORSConfiguration: cors}); err != nil {
		t.Fatalf("put bucket CORS: %v", err)
	}
	if got, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err != nil || len(got.CORSRules) != 1 || !reflect.DeepEqual(got.CORSRules[0].AllowedMethods, []string{"GET", "HEAD"}) || !reflect.DeepEqual(got.CORSRules[0].AllowedOrigins, []string{"https://example.test"}) || aws.ToInt32(got.CORSRules[0].MaxAgeSeconds) != 300 {
		t.Fatalf("bucket CORS round trip: %#v %v", got, err)
	}
	preflight, _ := http.NewRequest(http.MethodOptions, ts.URL+"/object", nil)
	preflight.Host = "sdk.s3.us-east-1.amazonaws.com"
	preflight.Header.Set("Origin", "https://example.test")
	preflight.Header.Set("Access-Control-Request-Method", http.MethodGet)
	preflightResponse, err := http.DefaultClient.Do(preflight)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, preflightResponse.Body)
	preflightResponse.Body.Close()
	if preflightResponse.StatusCode != http.StatusOK || preflightResponse.Header.Get("Access-Control-Allow-Origin") != "https://example.test" || preflightResponse.Header.Get("Access-Control-Allow-Methods") != "GET, HEAD" || preflightResponse.Header.Get("Access-Control-Expose-Headers") != "ETag" || preflightResponse.Header.Get("Access-Control-Max-Age") != "300" {
		t.Fatalf("bucket CORS preflight: %d %#v", preflightResponse.StatusCode, preflightResponse.Header)
	}
	invalidCors := &s3types.CORSConfiguration{CORSRules: []s3types.CORSRule{{AllowedMethods: []string{"OPTIONS"}, AllowedOrigins: []string{"*"}}}}
	if _, err := s3c.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{Bucket: aws.String("sdk"), CORSConfiguration: invalidCors}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("invalid bucket CORS: %v", err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket CORS: %v", err)
		}
	}
	if _, err := s3c.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchCORSConfiguration") {
		t.Fatalf("get deleted bucket CORS: %v", err)
	}
	defaultPreflight, _ := http.NewRequest(http.MethodOptions, ts.URL+"/object", nil)
	defaultPreflight.Host = "sdk.s3.us-east-1.amazonaws.com"
	defaultPreflight.Header.Set("Origin", "https://app.localstack.cloud")
	defaultPreflight.Header.Set("Access-Control-Request-Method", http.MethodGet)
	defaultResponse, err := http.DefaultClient.Do(defaultPreflight)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, defaultResponse.Body)
	defaultResponse.Body.Close()
	if defaultResponse.StatusCode != http.StatusOK || defaultResponse.Header.Get("Access-Control-Allow-Origin") != "https://app.localstack.cloud" || defaultResponse.Header.Get("Access-Control-Allow-Methods") != "HEAD,GET,PUT,POST,DELETE,OPTIONS,PATCH" || defaultResponse.Header.Get("Vary") != "Origin" {
		t.Fatalf("LocalStack default bucket CORS: %d %#v", defaultResponse.StatusCode, defaultResponse.Header)
	}
	if _, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchWebsiteConfiguration") {
		t.Fatalf("default bucket website: %v", err)
	}
	website := &s3types.WebsiteConfiguration{
		IndexDocument: &s3types.IndexDocument{Suffix: aws.String("index.html")},
		ErrorDocument: &s3types.ErrorDocument{Key: aws.String("error.html")},
		RoutingRules: []s3types.RoutingRule{{
			Condition: &s3types.Condition{KeyPrefixEquals: aws.String("docs/")},
			Redirect:  &s3types.Redirect{Protocol: s3types.ProtocolHttps, ReplaceKeyPrefixWith: aws.String("manual/")},
		}},
	}
	if _, err := s3c.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String("sdk"), WebsiteConfiguration: website}); err != nil {
		t.Fatalf("put bucket website: %v", err)
	}
	gotWebsite, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")})
	if err != nil || gotWebsite.IndexDocument == nil || aws.ToString(gotWebsite.IndexDocument.Suffix) != "index.html" || gotWebsite.ErrorDocument == nil || aws.ToString(gotWebsite.ErrorDocument.Key) != "error.html" || len(gotWebsite.RoutingRules) != 1 || gotWebsite.RoutingRules[0].Redirect == nil || gotWebsite.RoutingRules[0].Redirect.Protocol != s3types.ProtocolHttps || aws.ToString(gotWebsite.RoutingRules[0].Redirect.ReplaceKeyPrefixWith) != "manual/" {
		t.Fatalf("bucket website round trip: %#v %v", gotWebsite, err)
	}
	invalidWebsite := &s3types.WebsiteConfiguration{IndexDocument: &s3types.IndexDocument{Suffix: aws.String("dir/index.html")}}
	if _, err := s3c.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{Bucket: aws.String("sdk"), WebsiteConfiguration: invalidWebsite}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket website: %v", err)
	}
	if gotWebsite, err = s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err != nil || gotWebsite.IndexDocument == nil || aws.ToString(gotWebsite.IndexDocument.Suffix) != "index.html" {
		t.Fatalf("invalid website replaced configuration: %#v %v", gotWebsite, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket website: %v", err)
		}
	}
	if _, err := s3c.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchWebsiteConfiguration") {
		t.Fatalf("get deleted bucket website: %v", err)
	}
	if _, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("default bucket lifecycle: %v", err)
	}
	lifecycle := &s3types.BucketLifecycleConfiguration{Rules: []s3types.LifecycleRule{{
		ID: aws.String("expire-images"), Status: s3types.ExpirationStatusEnabled,
		Filter:      &s3types.LifecycleRuleFilter{And: &s3types.LifecycleRuleAndOperator{Prefix: aws.String("images/"), Tags: []s3types.Tag{{Key: aws.String("class"), Value: aws.String("temporary")}}}},
		Expiration:  &s3types.LifecycleExpiration{Days: aws.Int32(7)},
		Transitions: []s3types.Transition{{Days: aws.Int32(1), StorageClass: s3types.TransitionStorageClassGlacier}},
	}}}
	putLifecycle, err := s3c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String("sdk"), LifecycleConfiguration: lifecycle,
		TransitionDefaultMinimumObjectSize: s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass,
	})
	if err != nil || putLifecycle.TransitionDefaultMinimumObjectSize != s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass {
		t.Fatalf("put bucket lifecycle: %#v %v", putLifecycle, err)
	}
	gotLifecycle, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")})
	if err != nil || gotLifecycle.TransitionDefaultMinimumObjectSize != s3types.TransitionDefaultMinimumObjectSizeVariesByStorageClass || len(gotLifecycle.Rules) != 1 || aws.ToString(gotLifecycle.Rules[0].ID) != "expire-images" || gotLifecycle.Rules[0].Filter == nil || gotLifecycle.Rules[0].Filter.And == nil || aws.ToString(gotLifecycle.Rules[0].Filter.And.Prefix) != "images/" || len(gotLifecycle.Rules[0].Transitions) != 1 || gotLifecycle.Rules[0].Transitions[0].StorageClass != s3types.TransitionStorageClassGlacier {
		t.Fatalf("bucket lifecycle round trip: %#v %v", gotLifecycle, err)
	}
	putExpiring, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt"), Body: strings.NewReader("photo"), Tagging: aws.String("class=temporary")})
	if err != nil || !strings.Contains(aws.ToString(putExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("put lifecycle expiration: %#v %v", putExpiring, err)
	}
	getExpiring, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt")})
	if err != nil || !strings.Contains(aws.ToString(getExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("get lifecycle expiration: %#v %v", getExpiring, err)
	}
	_ = getExpiring.Body.Close()
	headExpiring, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("images/temporary.txt")})
	if err != nil || !strings.Contains(aws.ToString(headExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("head lifecycle expiration: %#v %v", headExpiring, err)
	}
	expiringUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("images/multipart.txt"), Tagging: aws.String("class=temporary")})
	if err != nil {
		t.Fatalf("create expiring multipart upload: %v", err)
	}
	expiringPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("images/multipart.txt"), UploadId: expiringUpload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("photo")})
	if err != nil {
		t.Fatalf("upload expiring part: %v", err)
	}
	completedExpiring, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("images/multipart.txt"), UploadId: expiringUpload.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: expiringPart.ETag}}}})
	if err != nil || !strings.Contains(aws.ToString(completedExpiring.Expiration), `rule-id="expire-images"`) {
		t.Fatalf("complete lifecycle expiration: %#v %v", completedExpiring, err)
	}
	invalidLifecycle := &s3types.BucketLifecycleConfiguration{Rules: []s3types.LifecycleRule{{ID: aws.String("invalid"), Status: s3types.ExpirationStatusEnabled, Filter: &s3types.LifecycleRuleFilter{Prefix: aws.String("a"), Tag: &s3types.Tag{Key: aws.String("k"), Value: aws.String("v")}}}}}
	if _, err := s3c.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{Bucket: aws.String("sdk"), LifecycleConfiguration: invalidLifecycle}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid bucket lifecycle: %v", err)
	}
	if gotLifecycle, err = s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err != nil || aws.ToString(gotLifecycle.Rules[0].ID) != "expire-images" {
		t.Fatalf("invalid lifecycle replaced configuration: %#v %v", gotLifecycle, err)
	}
	for range 2 {
		if _, err := s3c.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{Bucket: aws.String("sdk")}); err != nil {
			t.Fatalf("delete bucket lifecycle: %v", err)
		}
	}
	if _, err := s3c.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
		t.Fatalf("get deleted bucket lifecycle: %v", err)
	}
	analytics := &s3types.AnalyticsConfiguration{
		Id: aws.String("analysis"), Filter: &s3types.AnalyticsFilterMemberPrefix{Value: "logs/"},
		StorageClassAnalysis: &s3types.StorageClassAnalysis{DataExport: &s3types.StorageClassAnalysisDataExport{
			OutputSchemaVersion: s3types.StorageClassAnalysisSchemaVersionV1,
			Destination: &s3types.AnalyticsExportDestination{S3BucketDestination: &s3types.AnalyticsS3BucketDestination{
				Bucket: aws.String("arn:aws:s3:::sdk"), Format: s3types.AnalyticsS3ExportFileFormatCsv,
			}},
		}},
	}
	if _, err := s3c.PutBucketAnalyticsConfiguration(context.Background(), &s3.PutBucketAnalyticsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("analysis"), AnalyticsConfiguration: analytics}); err != nil {
		t.Fatalf("put analytics configuration: %v", err)
	}
	gotAnalytics, err := s3c.GetBucketAnalyticsConfiguration(context.Background(), &s3.GetBucketAnalyticsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("analysis")})
	if err != nil || gotAnalytics.AnalyticsConfiguration == nil || aws.ToString(gotAnalytics.AnalyticsConfiguration.Id) != "analysis" {
		t.Fatalf("analytics round trip: %#v %v", gotAnalytics, err)
	}
	listedAnalytics, err := s3c.ListBucketAnalyticsConfigurations(context.Background(), &s3.ListBucketAnalyticsConfigurationsInput{Bucket: aws.String("sdk")})
	if err != nil || len(listedAnalytics.AnalyticsConfigurationList) != 1 || aws.ToString(listedAnalytics.AnalyticsConfigurationList[0].Id) != "analysis" {
		t.Fatalf("analytics list: %#v %v", listedAnalytics, err)
	}

	inventory := &s3types.InventoryConfiguration{
		Id: aws.String("inventory"), IsEnabled: aws.Bool(true), IncludedObjectVersions: s3types.InventoryIncludedObjectVersionsAll,
		Destination: &s3types.InventoryDestination{S3BucketDestination: &s3types.InventoryS3BucketDestination{Bucket: aws.String("arn:aws:s3:::sdk"), Format: s3types.InventoryFormatCsv}},
		Schedule:    &s3types.InventorySchedule{Frequency: s3types.InventoryFrequencyDaily}, OptionalFields: []s3types.InventoryOptionalField{s3types.InventoryOptionalFieldSize, s3types.InventoryOptionalFieldETag},
	}
	if _, err := s3c.PutBucketInventoryConfiguration(context.Background(), &s3.PutBucketInventoryConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("inventory"), InventoryConfiguration: inventory}); err != nil {
		t.Fatalf("put inventory configuration: %v", err)
	}
	gotInventory, err := s3c.GetBucketInventoryConfiguration(context.Background(), &s3.GetBucketInventoryConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("inventory")})
	if err != nil || gotInventory.InventoryConfiguration == nil || !aws.ToBool(gotInventory.InventoryConfiguration.IsEnabled) || !reflect.DeepEqual(gotInventory.InventoryConfiguration.OptionalFields, inventory.OptionalFields) {
		t.Fatalf("inventory round trip: %#v %v", gotInventory, err)
	}

	tiering := &s3types.IntelligentTieringConfiguration{Id: aws.String("tiering"), Status: s3types.IntelligentTieringStatusEnabled, Tierings: []s3types.Tiering{{Days: aws.Int32(90), AccessTier: s3types.IntelligentTieringAccessTierArchiveAccess}}}
	if _, err := s3c.PutBucketIntelligentTieringConfiguration(context.Background(), &s3.PutBucketIntelligentTieringConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("tiering"), IntelligentTieringConfiguration: tiering}); err != nil {
		t.Fatalf("put intelligent tiering configuration: %v", err)
	}
	listedTiering, err := s3c.ListBucketIntelligentTieringConfigurations(context.Background(), &s3.ListBucketIntelligentTieringConfigurationsInput{Bucket: aws.String("sdk")})
	if err != nil || len(listedTiering.IntelligentTieringConfigurationList) != 1 || len(listedTiering.IntelligentTieringConfigurationList[0].Tierings) != 1 || aws.ToInt32(listedTiering.IntelligentTieringConfigurationList[0].Tierings[0].Days) != 90 {
		t.Fatalf("intelligent tiering list: %#v %v", listedTiering, err)
	}

	metrics := &s3types.MetricsConfiguration{Id: aws.String("metrics"), Filter: &s3types.MetricsFilterMemberPrefix{Value: "images/"}}
	if _, err := s3c.PutBucketMetricsConfiguration(context.Background(), &s3.PutBucketMetricsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("metrics"), MetricsConfiguration: metrics}); err != nil {
		t.Fatalf("put metrics configuration: %v", err)
	}
	gotMetrics, err := s3c.GetBucketMetricsConfiguration(context.Background(), &s3.GetBucketMetricsConfigurationInput{Bucket: aws.String("sdk"), Id: aws.String("metrics")})
	if err != nil || gotMetrics.MetricsConfiguration == nil || aws.ToString(gotMetrics.MetricsConfiguration.Id) != "metrics" {
		t.Fatalf("metrics round trip: %#v %v", gotMetrics, err)
	}
	if got, err := s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(got.QueueConfigurations) != 0 || len(got.TopicConfigurations) != 0 || len(got.LambdaFunctionConfigurations) != 0 || got.EventBridgeConfiguration != nil {
		t.Fatalf("default bucket notifications: %#v %v", got, err)
	}
	notifications := &s3types.NotificationConfiguration{
		QueueConfigurations: []s3types.QueueConfiguration{{
			QueueArn: aws.String("arn:aws:sqs:us-east-1:111111111111:queue"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:*")},
			Filter: &s3types.NotificationConfigurationFilter{Key: &s3types.S3KeyFilter{FilterRules: []s3types.FilterRule{{Name: s3types.FilterRuleName("prefix"), Value: aws.String("images/")}}}},
		}},
		TopicConfigurations:          []s3types.TopicConfiguration{{Id: aws.String("topic"), TopicArn: aws.String("arn:aws:sns:us-east-1:111111111111:topic"), Events: []s3types.Event{s3types.Event("s3:ObjectRemoved:*")}}},
		LambdaFunctionConfigurations: []s3types.LambdaFunctionConfiguration{{LambdaFunctionArn: aws.String("arn:aws:lambda:us-east-1:111111111111:function:handler"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:Put")}}},
		EventBridgeConfiguration:     &s3types.EventBridgeConfiguration{},
	}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: notifications, SkipDestinationValidation: aws.Bool(true)}); err != nil {
		t.Fatalf("put bucket notifications: %v", err)
	}
	gotNotifications, err := s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")})
	if err != nil || len(gotNotifications.QueueConfigurations) != 1 || len(aws.ToString(gotNotifications.QueueConfigurations[0].Id)) != 8 || gotNotifications.QueueConfigurations[0].Filter == nil || gotNotifications.QueueConfigurations[0].Filter.Key == nil || gotNotifications.QueueConfigurations[0].Filter.Key.FilterRules[0].Name != s3types.FilterRuleName("Prefix") || len(gotNotifications.TopicConfigurations) != 1 || aws.ToString(gotNotifications.TopicConfigurations[0].Id) != "topic" || len(gotNotifications.LambdaFunctionConfigurations) != 1 || gotNotifications.EventBridgeConfiguration == nil {
		t.Fatalf("bucket notifications round trip: %#v %v", gotNotifications, err)
	}
	invalidNotifications := &s3types.NotificationConfiguration{QueueConfigurations: []s3types.QueueConfiguration{{QueueArn: aws.String("arn:aws:sns:us-east-1:111111111111:queue"), Events: []s3types.Event{s3types.Event("s3:ObjectCreated:*")}}}}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: invalidNotifications}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid bucket notifications: %v", err)
	}
	if gotNotifications, err = s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(gotNotifications.QueueConfigurations) != 1 || aws.ToString(gotNotifications.QueueConfigurations[0].QueueArn) != "arn:aws:sqs:us-east-1:111111111111:queue" {
		t.Fatalf("invalid notifications replaced configuration: %#v %v", gotNotifications, err)
	}
	if _, err := s3c.PutBucketNotificationConfiguration(context.Background(), &s3.PutBucketNotificationConfigurationInput{Bucket: aws.String("sdk"), NotificationConfiguration: &s3types.NotificationConfiguration{}}); err != nil {
		t.Fatalf("clear bucket notifications: %v", err)
	}
	if gotNotifications, err = s3c.GetBucketNotificationConfiguration(context.Background(), &s3.GetBucketNotificationConfigurationInput{Bucket: aws.String("sdk")}); err != nil || len(gotNotifications.QueueConfigurations) != 0 || gotNotifications.EventBridgeConfiguration != nil {
		t.Fatalf("cleared bucket notifications: %#v %v", gotNotifications, err)
	}
	if payment, err := s3c.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String("sdk")}); err != nil || payment.Payer != s3types.PayerBucketOwner {
		t.Fatalf("default request payer: %#v %v", payment, err)
	}
	if _, err := s3c.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String("sdk"), RequestPaymentConfiguration: &s3types.RequestPaymentConfiguration{Payer: s3types.PayerRequester}}); err != nil {
		t.Fatalf("put request payer: %v", err)
	}
	if payment, err := s3c.GetBucketRequestPayment(context.Background(), &s3.GetBucketRequestPaymentInput{Bucket: aws.String("sdk")}); err != nil || payment.Payer != s3types.PayerRequester {
		t.Fatalf("request payer round trip: %#v %v", payment, err)
	}
	invalidPayer := s3types.Payer("Invalid")
	if _, err := s3c.PutBucketRequestPayment(context.Background(), &s3.PutBucketRequestPaymentInput{Bucket: aws.String("sdk"), RequestPaymentConfiguration: &s3types.RequestPaymentConfiguration{Payer: invalidPayer}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid request payer: %v", err)
	}
	if acceleration, err := s3c.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String("sdk")}); err != nil || acceleration.Status != "" {
		t.Fatalf("default acceleration: %#v %v", acceleration, err)
	}
	if _, err := s3c.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String("sdk"), AccelerateConfiguration: &s3types.AccelerateConfiguration{Status: s3types.BucketAccelerateStatusEnabled}}); err != nil {
		t.Fatalf("put acceleration: %v", err)
	}
	if acceleration, err := s3c.GetBucketAccelerateConfiguration(context.Background(), &s3.GetBucketAccelerateConfigurationInput{Bucket: aws.String("sdk")}); err != nil || acceleration.Status != s3types.BucketAccelerateStatusEnabled {
		t.Fatalf("acceleration round trip: %#v %v", acceleration, err)
	}
	invalidAcceleration := s3types.BucketAccelerateStatus("Invalid")
	if _, err := s3c.PutBucketAccelerateConfiguration(context.Background(), &s3.PutBucketAccelerateConfigurationInput{Bucket: aws.String("sdk"), AccelerateConfiguration: &s3types.AccelerateConfiguration{Status: invalidAcceleration}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid acceleration: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk")}); err != nil {
		t.Fatalf("recreate us-east-1 bucket: %v", err)
	}
	accountRegional := "sdk-account-000000000000-us-east-1-an"
	accountRegionalInput := &s3.CreateBucketInput{Bucket: aws.String(accountRegional), BucketNamespace: s3types.BucketNamespaceAccountRegional}
	if created, err := s3c.CreateBucket(context.Background(), accountRegionalInput); err != nil || aws.ToString(created.Location) != "/"+accountRegional || aws.ToString(created.BucketArn) != "arn:aws:s3:::"+accountRegional {
		t.Fatalf("create account-regional bucket: %#v %v", created, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), accountRegionalInput); err == nil || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("recreate account-regional bucket: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-account-999999999999-us-east-1-an"), BucketNamespace: s3types.BucketNamespaceAccountRegional}); err == nil || !strings.Contains(err.Error(), "InvalidBucketName") {
		t.Fatalf("foreign account-regional suffix: %v", err)
	}
	otherCfg := awscfg
	otherCfg.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("999999999999", "test", ""))
	other := s3.NewFromConfig(otherCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	if _, err := other.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("cross-account bucket collision: %v", err)
	}
	westCfg := awscfg
	westCfg.Region = "us-west-2"
	west := s3.NewFromConfig(westCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ts.URL)
		o.UsePathStyle = true
	})
	if _, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraintUsWest2}}); err == nil || !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
		t.Fatalf("cross-region bucket collision: %v", err)
	}
	if _, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-west-missing")}); err == nil || !strings.Contains(err.Error(), "IllegalLocationConstraintException") {
		t.Fatalf("missing regional location constraint: %v", err)
	}
	if created, err := west.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-west"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraintUsWest2}}); err != nil || aws.ToString(created.Location) != ts.URL+"/sdk-west/" || aws.ToString(created.BucketArn) != "arn:aws:s3:::sdk-west" {
		t.Fatalf("matching regional location constraint: %#v %v", created, err)
	}
	if location, err := west.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{Bucket: aws.String("sdk-west")}); err != nil || location.LocationConstraint != s3types.BucketLocationConstraintUsWest2 {
		t.Fatalf("stored regional location: %#v %v", location, err)
	}
	if head, err := s3c.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: aws.String("sdk-west")}); err != nil || head.AccessPointAlias == nil || aws.ToBool(head.AccessPointAlias) || aws.ToString(head.BucketRegion) != "us-west-2" || aws.ToString(head.BucketArn) != "arn:aws:s3:::sdk-west" {
		t.Fatalf("cross-region head: %#v %v", head, err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-west"), Key: aws.String("cross-region"), Body: strings.NewReader("body")}); err != nil {
		t.Fatalf("cross-region put: %v", err)
	}
	if listed, err := s3c.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String("sdk-west")}); err != nil || len(listed.Contents) != 1 {
		t.Fatalf("cross-region list: %#v %v", listed, err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-invalid-location"), CreateBucketConfiguration: &s3types.CreateBucketConfiguration{LocationConstraint: s3types.BucketLocationConstraint("moon-west-1")}}); err == nil || !strings.Contains(err.Error(), "InvalidLocationConstraint") {
		t.Fatalf("invalid location constraint: %v", err)
	}
	unpaginatedBuckets, err := s3c.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err != nil || unpaginatedBuckets.Owner == nil || aws.ToString(unpaginatedBuckets.Owner.ID) != "000000000000" || unpaginatedBuckets.Owner.DisplayName != nil || len(unpaginatedBuckets.Buckets) != 4 {
		t.Fatalf("unpaginated buckets: %#v %v", unpaginatedBuckets, err)
	}
	for _, bucket := range unpaginatedBuckets.Buckets {
		if bucket.CreationDate == nil || bucket.BucketRegion != nil || aws.ToString(bucket.BucketArn) != "arn:aws:s3:::"+aws.ToString(bucket.Name) {
			t.Fatalf("unpaginated bucket: %#v", bucket)
		}
	}
	paginator := s3.NewListBucketsPaginator(s3c, &s3.ListBucketsInput{MaxBuckets: aws.Int32(1), Prefix: aws.String("sdk")})
	var pagedNames []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil || aws.ToString(page.Prefix) != "sdk" {
			t.Fatalf("paginated buckets: %#v %v", page, err)
		}
		for _, bucket := range page.Buckets {
			if aws.ToString(bucket.BucketRegion) == "" || aws.ToString(bucket.BucketArn) != "arn:aws:s3:::"+aws.ToString(bucket.Name) {
				t.Fatalf("paginated bucket: %#v", bucket)
			}
			pagedNames = append(pagedNames, aws.ToString(bucket.Name))
		}
	}
	if got := strings.Join(pagedNames, ","); got != "sdk,sdk-account-000000000000-us-east-1-an,sdk-version-list,sdk-west" {
		t.Fatalf("paginated bucket names = %s", got)
	}
	regionalBuckets, err := west.ListBuckets(context.Background(), &s3.ListBucketsInput{BucketRegion: aws.String("us-west-2"), Prefix: aws.String("sdk")})
	if err != nil || len(regionalBuckets.Buckets) != 1 || aws.ToString(regionalBuckets.Buckets[0].Name) != "sdk-west" || aws.ToString(regionalBuckets.Buckets[0].BucketRegion) != "us-west-2" || aws.ToString(regionalBuckets.Buckets[0].BucketArn) != "arn:aws:s3:::sdk-west" {
		t.Fatalf("regional buckets: %#v %v", regionalBuckets, err)
	}
	if _, err := s3c.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{Bucket: aws.String(accountRegional)}); err == nil || !strings.Contains(err.Error(), "NoSuchTagSet") {
		t.Fatalf("untagged bucket: %v", err)
	}
	if versioning, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk")}); err != nil || versioning.Status != "" || versioning.MFADelete != "" {
		t.Fatalf("unset versioning: %#v %v", versioning, err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{}}); err == nil || !strings.Contains(err.Error(), "IllegalVersioningConfigurationException") {
		t.Fatalf("missing versioning status: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatus("Invalid")}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("invalid versioning status: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-suspended")}); err != nil {
		t.Fatalf("create suspended bucket: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), Body: strings.NewReader("unversioned")}); err != nil {
		t.Fatalf("put unversioned object: %v", err)
	}
	unversionedNull, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: aws.String("null")})
	if err != nil {
		t.Fatalf("get unversioned null object: %v", err)
	}
	unversionedNullBody, _ := io.ReadAll(unversionedNull.Body)
	_ = unversionedNull.Body.Close()
	if string(unversionedNullBody) != "unversioned" || unversionedNull.VersionId != nil {
		t.Fatalf("unversioned null object: body=%q output=%#v", unversionedNullBody, unversionedNull)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: aws.String("missing")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") || !strings.Contains(err.Error(), "Invalid version id specified") {
		t.Fatalf("get invalid unversioned version: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-suspended"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable suspended bucket: %v", err)
	}
	enabledObject, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), Body: strings.NewReader("enabled")})
	if err != nil {
		t.Fatalf("put enabled object: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-suspended"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusSuspended}}); err != nil {
		t.Fatalf("suspend versioning: %v", err)
	}
	for _, body := range []string{"first null", "second null"} {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), Body: strings.NewReader(body)}); err != nil {
			t.Fatalf("put suspended object: %v", err)
		}
	}
	for range 2 {
		copied, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), CopySource: aws.String("sdk-suspended/key"), MetadataDirective: s3types.MetadataDirectiveReplace})
		if err != nil || aws.ToString(copied.VersionId) != "null" || aws.ToString(copied.CopySourceVersionId) != "" {
			t.Fatalf("copy suspended object: %#v %v", copied, err)
		}
	}
	suspendedVersions, err := s3c.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{Bucket: aws.String("sdk-suspended")})
	if err != nil || len(suspendedVersions.Versions) != 2 || aws.ToString(suspendedVersions.Versions[0].VersionId) != "null" || aws.ToString(suspendedVersions.Versions[1].VersionId) != aws.ToString(enabledObject.VersionId) || !aws.ToBool(suspendedVersions.Versions[0].IsLatest) || aws.ToBool(suspendedVersions.Versions[1].IsLatest) {
		t.Fatalf("suspended versions: %#v %v", suspendedVersions, err)
	}
	nullObject, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: aws.String("null")})
	if err != nil {
		t.Fatalf("get null object: %v", err)
	}
	nullBody, _ := io.ReadAll(nullObject.Body)
	_ = nullObject.Body.Close()
	if string(nullBody) != "second null" || aws.ToString(nullObject.VersionId) != "null" {
		t.Fatalf("null object: body=%q output=%#v", nullBody, nullObject)
	}
	suspendedMarker, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key")})
	if err != nil || !aws.ToBool(suspendedMarker.DeleteMarker) || aws.ToString(suspendedMarker.VersionId) != "null" {
		t.Fatalf("suspended delete marker: %#v %v", suspendedMarker, err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: aws.String("null")}); err != nil {
		t.Fatalf("delete null marker: %v", err)
	}
	restoredObject, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key")})
	if err != nil {
		t.Fatalf("get restored object: %v", err)
	}
	restoredBody, _ := io.ReadAll(restoredObject.Body)
	_ = restoredObject.Body.Close()
	if string(restoredBody) != "enabled" || aws.ToString(restoredObject.VersionId) != aws.ToString(enabledObject.VersionId) {
		t.Fatalf("restored object: body=%q output=%#v", restoredBody, restoredObject)
	}
	conditionalDelete, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("conditional-delete"), Body: strings.NewReader("body")})
	if err != nil {
		t.Fatalf("seed conditional delete: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("conditional-delete"), IfMatch: aws.String(`"wrong"`)}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("wrong conditional delete: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("conditional-delete"), IfMatch: conditionalDelete.ETag}); err != nil {
		t.Fatalf("matching conditional delete: %v", err)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("conditional-delete")}); err == nil {
		t.Fatal("matching conditional delete left object")
	}
	for _, precondition := range []struct {
		name  string
		input *s3.DeleteObjectInput
	}{
		{"size", &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), IfMatchSize: aws.Int64(int64(len(restoredBody)))}},
		{"modified", &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), IfMatchLastModifiedTime: restoredObject.LastModified}},
	} {
		if _, err := s3c.DeleteObject(context.Background(), precondition.input); err == nil || !strings.Contains(err.Error(), "NotImplemented") {
			t.Fatalf("delete %s precondition: %v", precondition.name, err)
		}
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key")}); err != nil {
		t.Fatalf("precondition delete changed object: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: enabledObject.VersionId}); err != nil {
		t.Fatalf("delete enabled object version: %v", err)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("key"), VersionId: enabledObject.VersionId}); err == nil || !strings.Contains(err.Error(), "NoSuchVersion") || !strings.Contains(err.Error(), "The specified version does not exist.") {
		t.Fatalf("get deleted object version: %v", err)
	}
	for _, version := range []string{"missing-version", "null"} {
		deleted, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-suspended"), Key: aws.String("missing-key"), VersionId: aws.String(version)})
		if err != nil || deleted.VersionId != nil || deleted.DeleteMarker != nil {
			t.Fatalf("delete missing key version %q: %#v %v", version, deleted, err)
		}
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("missing-unversioned-key"), VersionId: aws.String("missing-version")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("delete invalid unversioned version: %v", err)
	}
	if deleted, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("missing-unversioned-key"), VersionId: aws.String("null")}); err != nil || deleted.VersionId != nil || deleted.DeleteMarker != nil {
		t.Fatalf("delete null unversioned version: %#v %v", deleted, err)
	}
	replicationConfiguration := &s3types.ReplicationConfiguration{
		Role:  aws.String("arn:aws:iam::000000000000:role/replication"),
		Rules: []s3types.ReplicationRule{{Priority: aws.Int32(1), Status: s3types.ReplicationRuleStatusEnabled, Filter: &s3types.ReplicationRuleFilter{Prefix: aws.String("replica/")}, DeleteMarkerReplication: &s3types.DeleteMarkerReplication{Status: s3types.DeleteMarkerReplicationStatusDisabled}, Destination: &s3types.Destination{Bucket: aws.String("arn:aws:s3:::sdk-west")}}},
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("replication without versioning: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String("sdk"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("replication to unversioned destination: %v", err)
	}
	if _, err := west.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-west"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable replica versioning: %v", err)
	}
	if _, err := s3c.PutBucketReplication(context.Background(), &s3.PutBucketReplicationInput{Bucket: aws.String("sdk"), ReplicationConfiguration: replicationConfiguration}); err != nil {
		t.Fatalf("configure replication: %v", err)
	}
	configuration, err := s3c.GetBucketReplication(context.Background(), &s3.GetBucketReplicationInput{Bucket: aws.String("sdk")})
	if err != nil || configuration.ReplicationConfiguration == nil || len(configuration.ReplicationConfiguration.Rules) != 1 {
		t.Fatalf("get replication configuration: %v", err)
	}
	replicaPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("replica/versioned"), Body: bytes.NewReader([]byte("replica-sdk")), Tagging: aws.String("stage=replicated")})
	if err != nil {
		t.Fatalf("put replicated version: %v", err)
	}
	replicaGet, err := west.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk-west"), Key: aws.String("replica/versioned"), VersionId: replicaPut.VersionId})
	if err != nil {
		t.Fatalf("get replicated version: %v", err)
	}
	replicaBody, _ := io.ReadAll(replicaGet.Body)
	_ = replicaGet.Body.Close()
	if string(replicaBody) != "replica-sdk" || aws.ToString(replicaGet.VersionId) != aws.ToString(replicaPut.VersionId) || replicaGet.ReplicationStatus != s3types.ReplicationStatusReplica {
		t.Fatalf("replicated version body=%q output=%#v", replicaBody, replicaGet)
	}
	put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("hello-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32, Tagging: aws.String("stage=original"), ContentType: aws.String("text/plain"), CacheControl: aws.String("no-cache"), ContentLanguage: aws.String("de"), ContentDisposition: aws.String(`attachment; filename="foo.jpg"`), Metadata: map[string]string{"owner": "mirror"}, WebsiteRedirectLocation: aws.String("/old"),
	})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s3c.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "BucketNotEmpty") {
		t.Fatalf("delete non-empty bucket: %v", err)
	}
	got, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if string(body) != "hello-sdk" {
		t.Fatalf("s3 body %q", body)
	}
	if aws.ToString(got.ContentType) != "text/plain" || aws.ToString(got.CacheControl) != "no-cache" || aws.ToString(got.ContentLanguage) != "de" || aws.ToString(got.ContentDisposition) != `attachment; filename="foo.jpg"` || got.Metadata["owner"] != "mirror" || aws.ToString(got.WebsiteRedirectLocation) != "/old" {
		t.Fatalf("s3 metadata %#v", got)
	}
	utf8Put, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("Ā0Ä"), Body: strings.NewReader("abc123"), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32})
	if err != nil || utf8Put.ServerSideEncryption != s3types.ServerSideEncryptionAes256 || aws.ToString(utf8Put.ChecksumCRC32) == "" {
		t.Fatalf("put utf8 key: %#v %v", utf8Put, err)
	}
	utf8Get, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("Ā0Ä"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get utf8 key: %v", err)
	}
	utf8Body, _ := io.ReadAll(utf8Get.Body)
	_ = utf8Get.Body.Close()
	if string(utf8Body) != "abc123" || utf8Get.ServerSideEncryption != s3types.ServerSideEncryptionAes256 || aws.ToString(utf8Get.ChecksumCRC32) != aws.ToString(utf8Put.ChecksumCRC32) {
		t.Fatalf("stored utf8 key: body=%q output=%#v", utf8Body, utf8Get)
	}
	for _, key := range []string{"test@key/", "test%40key/", "test key/", "test+key"} {
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader(key)}); err != nil {
			t.Fatalf("put special key %q: %v", key, err)
		}
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied special key"), CopySource: aws.String(url.QueryEscape("sdk/test key/"))}); err != nil {
		t.Fatalf("copy special key: %v", err)
	}
	specialCopy, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied special key")})
	if err != nil {
		t.Fatalf("get copied special key: %v", err)
	}
	specialCopyBody, _ := io.ReadAll(specialCopy.Body)
	_ = specialCopy.Body.Close()
	if string(specialCopyBody) != "test key/" {
		t.Fatalf("copied special key body = %q", specialCopyBody)
	}
	unicodeMultipart, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("test-unicode_—_file")})
	if err != nil {
		t.Fatalf("create Unicode multipart: %v", err)
	}
	unicodePart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("test-unicode_—_file"), UploadId: unicodeMultipart.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("upload-part-1")})
	if err != nil {
		t.Fatalf("upload Unicode multipart: %v", err)
	}
	unicodeComplete, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("test-unicode_—_file"), UploadId: unicodeMultipart.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: unicodePart.ETag}}}})
	if err != nil || !strings.HasSuffix(aws.ToString(unicodeComplete.Location), "/sdk/test-unicode_%E2%80%94_file") {
		t.Fatalf("complete Unicode multipart: %#v %v", unicodeComplete, err)
	}
	listETag := aws.String(`"wrong", ` + aws.ToString(got.ETag))
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), IfMatch: listETag}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "At least one of the pre-conditions you specified did not hold") {
		t.Fatalf("get If-Match list: %v", err)
	}
	listRead, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), IfNoneMatch: listETag})
	if err != nil {
		t.Fatalf("get If-None-Match list: %v", err)
	}
	_ = listRead.Body.Close()
	futureRead, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), IfModifiedSince: aws.Time(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))})
	if err != nil {
		t.Fatalf("get future If-Modified-Since: %v", err)
	}
	_ = futureRead.Body.Close()
	overrideExpiry := time.Date(2015, time.October, 21, 7, 28, 0, 0, time.UTC)
	overridden, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), ResponseCacheControl: aws.String("max-age=74"),
		ResponseContentDisposition: aws.String(`attachment; filename="foo.jpg"`), ResponseContentEncoding: aws.String("identity"),
		ResponseContentLanguage: aws.String("de-DE"), ResponseContentType: aws.String("image/jpeg"), ResponseExpires: aws.Time(overrideExpiry),
	})
	if err != nil {
		t.Fatalf("get response overrides: %v", err)
	}
	overriddenBody, _ := io.ReadAll(overridden.Body)
	_ = overridden.Body.Close()
	if string(overriddenBody) != "hello-sdk" || aws.ToString(overridden.CacheControl) != "max-age=74" || aws.ToString(overridden.ContentDisposition) != `attachment; filename="foo.jpg"` || aws.ToString(overridden.ContentEncoding) != "identity" || aws.ToString(overridden.ContentLanguage) != "de-DE" || aws.ToString(overridden.ContentType) != "image/jpeg" || overridden.Expires == nil || !overridden.Expires.Equal(overrideExpiry) {
		t.Fatalf("response overrides %#v body=%q", overridden, overriddenBody)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("rfc2047"), Body: bytes.NewReader([]byte("metadata")),
		Metadata: map[string]string{"non-ascii": "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=", "fake-encoded": "=?UTF-8?Q?actually-ascii?=", "TEST_META_1": "foo", "__meta_2": "bar"},
	}); err != nil {
		t.Fatalf("put rfc2047 metadata: %v", err)
	}
	rfc2047, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("rfc2047")})
	if err != nil || rfc2047.Metadata["non-ascii"] != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || rfc2047.Metadata["fake-encoded"] != "actually-ascii" || rfc2047.Metadata["test_meta_1"] != "foo" || rfc2047.Metadata["__meta_2"] != "bar" {
		t.Fatalf("rfc2047 metadata: %#v %v", rfc2047, err)
	}
	unicodeDisposition := `attachment; filename="test_—_file%E2%80%94_é_2.pdf"`
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("unicode-system-metadata"), Body: strings.NewReader(""), CacheControl: aws.String("ÄMÄZÕÑ S3"), ContentLanguage: aws.String("de"), ContentDisposition: aws.String(unicodeDisposition)}); err != nil {
		t.Fatalf("put unicode system metadata: %v", err)
	}
	unicodeMetadata, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("unicode-system-metadata")})
	if err != nil || aws.ToString(unicodeMetadata.CacheControl) != "ÄMÄZÕÑ S3" || aws.ToString(unicodeMetadata.ContentLanguage) != "de" || aws.ToString(unicodeMetadata.ContentDisposition) != unicodeDisposition {
		t.Fatalf("unicode system metadata: %#v %v", unicodeMetadata, err)
	}
	_ = unicodeMetadata.Body.Close()
	customerKey := []byte("0123456789abcdef0123456789abcdef")
	customerKeyDigest := md5.Sum(customerKey)
	customerKey64, customerKeyMD5 := base64.StdEncoding.EncodeToString(customerKey), base64.StdEncoding.EncodeToString(customerKeyDigest[:])
	customerPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted"), Body: bytes.NewReader([]byte("sse-c-sdk")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(customerPut.SSECustomerAlgorithm) != "AES256" || aws.ToString(customerPut.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("put customer encryption: %#v %v", customerPut, err)
	}
	customerGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get customer encryption: %v", err)
	}
	customerBody, _ := io.ReadAll(customerGet.Body)
	_ = customerGet.Body.Close()
	if string(customerBody) != "sse-c-sdk" || aws.ToString(customerGet.SSECustomerAlgorithm) != "AES256" || aws.ToString(customerGet.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("get customer encryption: body=%q output=%#v", customerBody, customerGet)
	}
	customerCopy, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted-copy"), CopySource: aws.String("sdk/customer-encrypted"), CopySourceSSECustomerAlgorithm: aws.String("AES256"), CopySourceSSECustomerKey: aws.String(customerKey64), CopySourceSSECustomerKeyMD5: aws.String(customerKeyMD5), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(customerCopy.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("copy customer encryption: %#v %v", customerCopy, err)
	}
	customerCopyGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("customer-encrypted-copy"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get copied customer encryption: %v", err)
	}
	customerCopyBody, _ := io.ReadAll(customerCopyGet.Body)
	_ = customerCopyGet.Body.Close()
	if string(customerCopyBody) != "sse-c-sdk" {
		t.Fatalf("copied customer encryption body=%q", customerCopyBody)
	}
	multipartCustomer, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(multipartCustomer.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("create multipart customer encryption: %#v %v", multipartCustomer, err)
	}
	const multipartEncryptionMessage = "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters."
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("missing-sse-c"))}); err == nil || !strings.Contains(err.Error(), multipartEncryptionMessage) {
		t.Fatalf("upload multipart without customer encryption: %v", err)
	}
	plainMultipart, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-plain-sse-fault")})
	if err != nil {
		t.Fatalf("create plain multipart for customer encryption fault: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-plain-sse-fault"), UploadId: plainMultipart.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("unexpected-sse-c")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)}); err == nil || !strings.Contains(err.Error(), multipartEncryptionMessage) {
		t.Fatalf("upload plain multipart with customer encryption: %v", err)
	}
	otherCustomerKey := bytes.Repeat([]byte{'b'}, 32)
	otherCustomerDigest := md5.Sum(otherCustomerKey)
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("mismatched-sse-c")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(base64.StdEncoding.EncodeToString(otherCustomerKey)), SSECustomerKeyMD5: aws.String(base64.StdEncoding.EncodeToString(otherCustomerDigest[:]))}); err == nil || !strings.Contains(err.Error(), "The provided encryption parameters did not match the ones used originally.") {
		t.Fatalf("upload multipart with mismatched customer encryption: %v", err)
	}
	multipartPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("multipart-sse-c-sdk")), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil || aws.ToString(multipartPart.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("upload multipart customer encryption: %#v %v", multipartPart, err)
	}
	multipartCompleted, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), UploadId: multipartCustomer.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{ETag: multipartPart.ETag, PartNumber: aws.Int32(1)}}}})
	if err != nil {
		t.Fatalf("complete multipart customer encryption: %#v %v", multipartCompleted, err)
	}
	multipartCustomerGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multipart-customer-encrypted"), SSECustomerAlgorithm: aws.String("AES256"), SSECustomerKey: aws.String(customerKey64), SSECustomerKeyMD5: aws.String(customerKeyMD5)})
	if err != nil {
		t.Fatalf("get multipart customer encryption: %v", err)
	}
	multipartCustomerBody, _ := io.ReadAll(multipartCustomerGet.Body)
	_ = multipartCustomerGet.Body.Close()
	if string(multipartCustomerBody) != "multipart-sse-c-sdk" || aws.ToString(multipartCustomerGet.SSECustomerKeyMD5) != customerKeyMD5 {
		t.Fatalf("get multipart customer encryption: body=%q output=%#v", multipartCustomerBody, multipartCustomerGet)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ExpectedBucketOwner: aws.String("000000000000")}); err != nil {
		t.Fatalf("matching expected owner: %v", err)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "StatusCode: 403") {
		t.Fatalf("mismatched expected owner: %v", err)
	}
	if _, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk"), ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("versioning mismatched expected owner: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("missing"), Key: aws.String("k")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("delete missing bucket: %v", err)
	}
	verified, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get checksum: %v", err)
	}
	_, _ = io.Copy(io.Discard, verified.Body)
	_ = verified.Body.Close()
	if aws.ToString(put.ChecksumCRC32) == "" || aws.ToString(verified.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("get checksum %q want %q", aws.ToString(verified.ChecksumCRC32), aws.ToString(put.ChecksumCRC32))
	}
	xxhash128 := "MxGUd+3l3NXpcWQnaB1YYA=="
	xxhashPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash"), Body: bytes.NewReader([]byte("123456789")), ChecksumXXHASH128: aws.String(xxhash128),
	})
	if err != nil || aws.ToString(xxhashPut.ChecksumXXHASH128) != xxhash128 {
		t.Fatalf("put xxhash checksum: %#v %v", xxhashPut, err)
	}
	xxhashGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("xxhash"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("get xxhash checksum: %v", err)
	}
	xxhashBody, _ := io.ReadAll(xxhashGet.Body)
	_ = xxhashGet.Body.Close()
	if string(xxhashBody) != "123456789" || aws.ToString(xxhashGet.ChecksumXXHASH128) != xxhash128 {
		t.Fatalf("get xxhash checksum: %#v body=%q", xxhashGet, xxhashBody)
	}
	xxhashUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), ChecksumAlgorithm: s3types.ChecksumAlgorithmXxhash3,
	})
	if err != nil {
		t.Fatalf("create xxhash multipart: %v", err)
	}
	xxhash3 := "ctyxi2ehff8="
	xxhashPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), UploadId: xxhashUpload.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("123456789")), ChecksumXXHASH3: aws.String(xxhash3),
	})
	if err != nil || aws.ToString(xxhashPart.ChecksumXXHASH3) != xxhash3 {
		t.Fatalf("upload xxhash part: %#v %v", xxhashPart, err)
	}
	xxhashComposite := "ksPmtVIgSbU=-1"
	xxhashComplete, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("xxhash-multipart"), UploadId: xxhashUpload.UploadId, ChecksumXXHASH3: aws.String(xxhashComposite),
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: xxhashPart.ETag, ChecksumXXHASH3: xxhashPart.ChecksumXXHASH3}}},
	})
	if err != nil || aws.ToString(xxhashComplete.ChecksumXXHASH3) != xxhashComposite {
		t.Fatalf("complete xxhash multipart: %#v %v", xxhashComplete, err)
	}
	copyChecksumBody := []byte("copy-checksum-sdk")
	copyChecksumSum := sha256.Sum256(copyChecksumBody)
	copyChecksumValue := base64.StdEncoding.EncodeToString(copyChecksumSum[:])
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("copy-checksum-source"), Body: bytes.NewReader(copyChecksumBody), ChecksumSHA256: aws.String(copyChecksumValue),
	}); err != nil {
		t.Fatalf("put copy checksum source: %v", err)
	}
	copyChecksum, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copy-checksum"), CopySource: aws.String("sdk/copy-checksum-source")})
	if err != nil || copyChecksum.CopyObjectResult == nil || copyChecksum.CopyObjectResult.LastModified == nil || aws.ToString(copyChecksum.CopyObjectResult.ChecksumSHA256) != copyChecksumValue || copyChecksum.CopyObjectResult.ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("copy checksum result: %#v %v", copyChecksum, err)
	}
	copyChecksumHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copy-checksum"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil || copyChecksumHead.LastModified == nil || !copyChecksum.CopyObjectResult.LastModified.Equal(*copyChecksumHead.LastModified) || aws.ToString(copyChecksumHead.ChecksumSHA256) != copyChecksumValue || copyChecksumHead.ChecksumType != s3types.ChecksumTypeFullObject {
		t.Fatalf("copy checksum head: %#v %v", copyChecksumHead, err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: got.ETag, ExpectedSourceBucketOwner: aws.String("000000000000"),
	}); err != nil {
		t.Fatalf("conditional copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("modified-copy"), CopySource: aws.String("sdk/k"), CopySourceIfModifiedSince: aws.Time(got.LastModified.Add(-time.Minute))}); err != nil {
		t.Fatalf("versioned modified-since copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("modified-copy-rejected"), CopySource: aws.String("sdk/k"), CopySourceIfModifiedSince: got.LastModified}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") {
		t.Fatalf("versioned modified-since boundary: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("unmodified-copy"), CopySource: aws.String("sdk/k"), CopySourceIfUnmodifiedSince: got.LastModified}); err != nil {
		t.Fatalf("versioned unmodified-since copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("unmodified-copy-rejected"), CopySource: aws.String("sdk/k"), CopySourceIfUnmodifiedSince: aws.Time(got.LastModified.Add(-time.Minute))}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") {
		t.Fatalf("versioned unmodified-since boundary: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("listed-copy"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: listETag}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "At least one of the pre-conditions you specified did not hold") {
		t.Fatalf("copy source If-Match list: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("future-copy"), CopySource: aws.String("sdk/k"), CopySourceIfModifiedSince: aws.Time(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)),
	}); err != nil {
		t.Fatalf("future modified-since copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("ordered-rejection"), CopySource: aws.String("sdk/k"), CopySourceIfNoneMatch: aws.String(`"wrong"`), CopySourceIfModifiedSince: got.LastModified,
	}); err == nil || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("ordered copy preconditions: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("short-circuit-copy"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: got.ETag, CopySourceIfNoneMatch: got.ETag,
		CopySourceIfModifiedSince: aws.Time(got.LastModified.Add(-time.Hour)), CopySourceIfUnmodifiedSince: aws.Time(got.LastModified.Add(-time.Hour)),
	}); err != nil {
		t.Fatalf("matching copy short circuit: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/copied")}); err == nil || !strings.Contains(err.Error(), "InvalidRequest") {
		t.Fatalf("unchanged self-copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("copied"), CopySource: aws.String("sdk/copied"), MetadataDirective: s3types.MetadataDirectiveReplace, Metadata: map[string]string{"owner": "self"}}); err != nil {
		t.Fatalf("metadata-replacing self-copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("source-owner-denied"), CopySource: aws.String("sdk/k"), ExpectedSourceBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("mismatched expected source owner: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("missing-source"), CopySource: aws.String("missing/k")}); err == nil || !strings.Contains(err.Error(), "NoSuchBucket") {
		t.Fatalf("copy missing source bucket: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-class"), Body: bytes.NewReader([]byte("body")), StorageClass: s3types.StorageClass("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid put storage class: %v", err)
	}
	if _, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-multipart-class"), StorageClass: s3types.StorageClassOutposts}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid multipart storage class: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("invalid-copy-class"), CopySource: aws.String("sdk/k"), StorageClass: s3types.StorageClass("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidStorageClass") {
		t.Fatalf("invalid copy storage class: %v", err)
	}
	for _, key := range []string{"invalid-class", "invalid-multipart-class", "invalid-copy-class"} {
		if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key)}); err == nil || !strings.Contains(err.Error(), "NotFound") {
			t.Fatalf("invalid storage class created %q: %v", key, err)
		}
	}
	longKey := strings.Repeat("é", 513)
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(longKey), Body: bytes.NewReader([]byte("body"))}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized put key: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String(longKey), CopySource: aws.String("missing/source")}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized copy key: %v", err)
	}
	if _, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String(longKey)}); err == nil || !strings.Contains(err.Error(), "KeyTooLongError") {
		t.Fatalf("oversized multipart key: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive"), Body: bytes.NewReader([]byte("cold")), StorageClass: s3types.StorageClassGlacier}); err != nil {
		t.Fatalf("put archive: %v", err)
	}
	archiveHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")})
	if err != nil || archiveHead.StorageClass != s3types.StorageClassGlacier || aws.ToString(archiveHead.Restore) != "" {
		t.Fatalf("head unrestored archive: %#v %v", archiveHead, err)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")}); err == nil || !strings.Contains(err.Error(), "InvalidObjectState") {
		t.Fatalf("get unrestored archive: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive-copy"), CopySource: aws.String("sdk/archive")}); err == nil || !strings.Contains(err.Error(), "InvalidObjectState") {
		t.Fatalf("copy unrestored archive: %v", err)
	}
	if _, err := s3c.RestoreObject(context.Background(), &s3.RestoreObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive"), RestoreRequest: &s3types.RestoreRequest{Days: aws.Int32(1)}}); err != nil {
		t.Fatalf("restore archive: %v", err)
	}
	restoredArchive, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive")})
	if err != nil {
		t.Fatalf("get restored archive: %v", err)
	}
	archiveBody, _ := io.ReadAll(restoredArchive.Body)
	_ = restoredArchive.Body.Close()
	if string(archiveBody) != "cold" || aws.ToString(restoredArchive.Restore) == "" {
		t.Fatalf("restored archive body=%q restore=%q", archiveBody, aws.ToString(restoredArchive.Restore))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("archive-copy"), CopySource: aws.String("sdk/archive")}); err != nil {
		t.Fatalf("copy restored archive: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("rejected"), CopySource: aws.String("sdk/k"), CopySourceIfMatch: aws.String(`"wrong"`),
	}); err == nil {
		t.Fatal("conditional copy with wrong ETag succeeded")
	}
	newer, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Body: bytes.NewReader([]byte("newer")),
	})
	if err != nil {
		t.Fatalf("put newer version: %v", err)
	}
	original, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("get original version: %v", err)
	}
	originalBody, _ := io.ReadAll(original.Body)
	_ = original.Body.Close()
	if string(originalBody) != "hello-sdk" || aws.ToString(original.VersionId) != aws.ToString(put.VersionId) || aws.ToString(original.ETag) != aws.ToString(put.ETag) || aws.ToString(original.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("original version body=%q version=%q etag=%q checksum=%q", originalBody, aws.ToString(original.VersionId), aws.ToString(original.ETag), aws.ToString(original.ChecksumCRC32))
	}
	head, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil || aws.ToString(head.VersionId) != aws.ToString(put.VersionId) || aws.ToString(head.ETag) != aws.ToString(put.ETag) || aws.ToString(head.ChecksumCRC32) != aws.ToString(put.ChecksumCRC32) {
		t.Fatalf("head original version: %#v %v", head, err)
	}
	past := time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfMatch: put.ETag, IfUnmodifiedSince: &past}); err != nil {
		t.Fatalf("conditional head precedence: %v", err)
	}
	if _, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfMatch: aws.String(`"wrong"`)}); err == nil {
		t.Fatal("conditional head with wrong ETag succeeded")
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, IfNoneMatch: put.ETag}); err == nil {
		t.Fatal("conditional get with matching If-None-Match succeeded")
	}
	ranged, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=-3"), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		t.Fatalf("suffix range: %v", err)
	}
	suffixBody, _ := io.ReadAll(ranged.Body)
	_ = ranged.Body.Close()
	if string(suffixBody) != "sdk" || aws.ToString(ranged.ContentRange) != "bytes 6-8/9" || aws.ToInt64(ranged.ContentLength) != 3 || aws.ToString(ranged.ChecksumCRC32) != "" {
		t.Fatalf("suffix range body=%q output=%#v", suffixBody, ranged)
	}
	headRange, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=1-3")})
	if err != nil || aws.ToInt64(headRange.ContentLength) != 3 {
		t.Fatalf("head range: %#v %v", headRange, err)
	}
	if _, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId, Range: aws.String("bytes=9-")}); err == nil {
		t.Fatal("unsatisfiable range succeeded")
	}
	originalTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: put.VersionId})
	if err != nil || aws.ToString(originalTags.VersionId) != aws.ToString(put.VersionId) || len(originalTags.TagSet) != 1 || aws.ToString(originalTags.TagSet[0].Key) != "stage" || aws.ToString(originalTags.TagSet[0].Value) != "original" {
		t.Fatalf("get original tags: %#v %v", originalTags, err)
	}
	tagged, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{{Key: aws.String("stage"), Value: aws.String("newer")}}},
	})
	if err != nil || aws.ToString(tagged.VersionId) != aws.ToString(newer.VersionId) {
		t.Fatalf("tag newer version: %#v %v", tagged, err)
	}
	deletedNewer, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k"), VersionId: newer.VersionId})
	if err != nil || aws.ToString(deletedNewer.VersionId) != aws.ToString(newer.VersionId) || aws.ToBool(deletedNewer.DeleteMarker) {
		t.Fatalf("delete current version: %#v %v", deletedNewer, err)
	}
	restoredCurrent, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("k")})
	if err != nil {
		t.Fatalf("get restored current version: %v", err)
	}
	restoredCurrentBody, _ := io.ReadAll(restoredCurrent.Body)
	_ = restoredCurrent.Body.Close()
	if string(restoredCurrentBody) != "hello-sdk" || aws.ToString(restoredCurrent.VersionId) != aws.ToString(put.VersionId) {
		t.Fatalf("restored current version body=%q output=%#v", restoredCurrentBody, restoredCurrent)
	}
	multiFirst, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete"), Body: bytes.NewReader([]byte("first"))})
	if err != nil {
		t.Fatalf("put first multi-delete version: %v", err)
	}
	multiSecond, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete"), Body: bytes.NewReader([]byte("second"))})
	if err != nil {
		t.Fatalf("put second multi-delete version: %v", err)
	}
	multiDeleted, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: aws.String("multi-delete"), VersionId: multiSecond.VersionId}}}})
	if err != nil || len(multiDeleted.Deleted) != 1 || aws.ToString(multiDeleted.Deleted[0].VersionId) != aws.ToString(multiSecond.VersionId) || aws.ToBool(multiDeleted.Deleted[0].DeleteMarker) {
		t.Fatalf("multi-delete current version: %#v %v", multiDeleted, err)
	}
	multiRestored, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("multi-delete")})
	if err != nil {
		t.Fatalf("get multi-delete restored version: %v", err)
	}
	multiRestoredBody, _ := io.ReadAll(multiRestored.Body)
	_ = multiRestored.Body.Close()
	if string(multiRestoredBody) != "first" || aws.ToString(multiRestored.VersionId) != aws.ToString(multiFirst.VersionId) {
		t.Fatalf("multi-delete restored body=%q output=%#v", multiRestoredBody, multiRestored)
	}
	quietDeleted, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Quiet: aws.Bool(true), Objects: []s3types.ObjectIdentifier{
		{Key: aws.String("multi-delete"), VersionId: aws.String("missing")},
		{Key: aws.String("multi-delete"), VersionId: multiFirst.VersionId},
	}}})
	if err != nil || len(quietDeleted.Deleted) != 0 || len(quietDeleted.Errors) != 1 || aws.ToString(quietDeleted.Errors[0].Code) != "NoSuchVersion" || aws.ToString(quietDeleted.Errors[0].VersionId) != "missing" {
		t.Fatalf("quiet multi-delete: %#v %v", quietDeleted, err)
	}
	if _, err := s3c.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String("sdk")}); err == nil || !strings.Contains(err.Error(), "ObjectLockConfigurationNotFoundError") || !strings.Contains(err.Error(), "Object Lock configuration does not exist for this bucket") {
		t.Fatalf("missing object-lock configuration: %v", err)
	}
	if _, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), BypassGovernanceRetention: aws.Bool(false), Delete: &s3types.Delete{Objects: []s3types.ObjectIdentifier{{Key: aws.String("multi-delete")}}}}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") || !strings.Contains(err.Error(), "only applicable to Object Lock enabled buckets") {
		t.Fatalf("multi-delete bypass without object lock: %v", err)
	}
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("sdk-lock"), ObjectLockEnabledForBucket: aws.Bool(true)}); err != nil {
		t.Fatalf("create object-lock bucket: %v", err)
	}
	lockVersioning, err := s3c.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{Bucket: aws.String("sdk-lock")})
	if err != nil || lockVersioning.Status != s3types.BucketVersioningStatusEnabled {
		t.Fatalf("object-lock bucket versioning: %#v %v", lockVersioning, err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String("sdk-lock"), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusSuspended}}); err == nil || !strings.Contains(err.Error(), "InvalidBucketState") {
		t.Fatalf("suspend object-lock bucket versioning: %v", err)
	}
	if _, err := s3c.PutObjectLockConfiguration(context.Background(), &s3.PutObjectLockConfigurationInput{Bucket: aws.String("sdk-lock"), ObjectLockConfiguration: &s3types.ObjectLockConfiguration{ObjectLockEnabled: s3types.ObjectLockEnabledEnabled, Rule: &s3types.ObjectLockRule{DefaultRetention: &s3types.DefaultRetention{Mode: s3types.ObjectLockRetentionModeGovernance, Days: aws.Int32(7)}}}}); err != nil {
		t.Fatalf("put object-lock configuration: %v", err)
	}
	lockConfiguration, err := s3c.GetObjectLockConfiguration(context.Background(), &s3.GetObjectLockConfigurationInput{Bucket: aws.String("sdk-lock")})
	if err != nil || lockConfiguration.ObjectLockConfiguration == nil {
		t.Fatalf("get object-lock configuration: %#v %v", lockConfiguration, err)
	}
	if got := lockConfiguration.ObjectLockConfiguration; got.ObjectLockEnabled != s3types.ObjectLockEnabledEnabled || got.Rule == nil || got.Rule.DefaultRetention == nil || aws.ToInt32(got.Rule.DefaultRetention.Days) != 7 {
		t.Fatalf("get object-lock configuration: %#v", got)
	}
	lockedVersion, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), Body: bytes.NewReader([]byte("locked"))})
	if err != nil {
		t.Fatalf("put locked object: %v", err)
	}
	defaultRetention, err := s3c.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId})
	if err != nil || defaultRetention.Retention == nil || defaultRetention.Retention.Mode != s3types.ObjectLockRetentionModeGovernance || defaultRetention.Retention.RetainUntilDate == nil || time.Until(*defaultRetention.Retention.RetainUntilDate) < 6*24*time.Hour {
		t.Fatalf("default object retention: %#v %v", defaultRetention, err)
	}
	explicitUntil := defaultRetention.Retention.RetainUntilDate.Add(-6 * 24 * time.Hour)
	explicitVersion, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("explicit-lock"), Body: bytes.NewReader([]byte("locked")), ObjectLockMode: s3types.ObjectLockModeCompliance, ObjectLockRetainUntilDate: &explicitUntil, ObjectLockLegalHoldStatus: s3types.ObjectLockLegalHoldStatusOn})
	if err != nil {
		t.Fatalf("put explicitly locked object: %v", err)
	}
	explicitRetention, err := s3c.GetObjectRetention(context.Background(), &s3.GetObjectRetentionInput{Bucket: aws.String("sdk-lock"), Key: aws.String("explicit-lock"), VersionId: explicitVersion.VersionId})
	if err != nil || explicitRetention.Retention == nil || explicitRetention.Retention.Mode != s3types.ObjectLockRetentionModeCompliance || explicitRetention.Retention.RetainUntilDate == nil || !explicitRetention.Retention.RetainUntilDate.Equal(explicitUntil) {
		t.Fatalf("explicit object retention: %#v %v", explicitRetention, err)
	}
	if _, err := s3c.PutObjectLegalHold(context.Background(), &s3.PutObjectLegalHoldInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId, LegalHold: &s3types.ObjectLockLegalHold{Status: s3types.ObjectLockLegalHoldStatusOn}}); err != nil {
		t.Fatalf("put legal hold: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked"), VersionId: lockedVersion.VersionId}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("delete legal-held version: %v", err)
	}
	if marker, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk-lock"), Key: aws.String("locked")}); err != nil || !aws.ToBool(marker.DeleteMarker) {
		t.Fatalf("delete-marker over legal hold: %#v %v", marker, err)
	}
	tooMany := make([]s3types.ObjectIdentifier, 1001)
	for index := range tooMany {
		tooMany[index].Key = aws.String(fmt.Sprintf("too-many-%d", index))
	}
	if _, err := s3c.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{Bucket: aws.String("sdk"), Delete: &s3types.Delete{Objects: tooMany}}); err == nil || !strings.Contains(err.Error(), "MalformedXML") {
		t.Fatalf("oversized multi-delete: %v", err)
	}
	if _, err := s3c.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
		Bucket: aws.String("sdk"), Key: aws.String("k"), Tagging: &s3types.Tagging{TagSet: []s3types.Tag{{Key: aws.String("duplicate"), Value: aws.String("one")}, {Key: aws.String("duplicate"), Value: aws.String("two")}}},
	}); err == nil || !strings.Contains(err.Error(), "InvalidTag") {
		t.Fatalf("duplicate object tags: %v", err)
	}
	versionCopy, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("version-copy"), CopySource: aws.String("sdk/k?versionId=" + aws.ToString(put.VersionId)),
	})
	if err != nil {
		t.Fatalf("copy version: %v", err)
	}
	if aws.ToString(versionCopy.CopySourceVersionId) != aws.ToString(put.VersionId) {
		t.Fatalf("copy source version %q want %q", aws.ToString(versionCopy.CopySourceVersionId), aws.ToString(put.VersionId))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("bad-metadata-directive"), CopySource: aws.String("sdk/k"), MetadataDirective: s3types.MetadataDirective("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid metadata directive: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String("bad-tagging-directive"), CopySource: aws.String("sdk/k"), TaggingDirective: s3types.TaggingDirective("INVALID")}); err == nil || !strings.Contains(err.Error(), "InvalidArgument") {
		t.Fatalf("invalid tagging directive: %v", err)
	}
	versioned, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("version-copy")})
	if err != nil {
		t.Fatalf("get version copy: %v", err)
	}
	versionedBody, _ := io.ReadAll(versioned.Body)
	_ = versioned.Body.Close()
	if string(versionedBody) != "hello-sdk" {
		t.Fatalf("version copy body %q", versionedBody)
	}
	versionCopyTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("version-copy")})
	if err != nil || len(versionCopyTags.TagSet) != 1 || aws.ToString(versionCopyTags.TagSet[0].Value) != "original" {
		t.Fatalf("version copy tags: %#v %v", versionCopyTags, err)
	}
	large := bytes.Repeat([]byte("0123456789"), 600000)
	largePut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("large"), Body: bytes.NewReader(large),
	})
	if err != nil {
		t.Fatalf("put large source: %v", err)
	}
	upload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"),
	})
	if err != nil {
		t.Fatalf("create multipart copy: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(10001), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "Part number must be an integer between 1 and 10000, inclusive") {
		t.Fatalf("invalid multipart part number: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), ContentMD5: aws.String("!"), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "The Content-MD5 you specified was invalid") {
		t.Fatalf("malformed upload part Content-MD5: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), ContentMD5: aws.String("AAAAAAAAAAAAAAAAAAAAAA=="), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "The Content-MD5 you specified did not match what we received") {
		t.Fatalf("mismatched upload part Content-MD5: %v", err)
	}
	checksumFaultUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32})
	if err != nil {
		t.Fatalf("create checksum fault upload: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), UploadId: checksumFaultUpload.UploadId, PartNumber: aws.Int32(1), ChecksumCRC32: aws.String("!"), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "Value for x-amz-checksum-crc32 header is invalid") {
		t.Fatalf("malformed upload part checksum: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), UploadId: checksumFaultUpload.UploadId, PartNumber: aws.Int32(1), ChecksumCRC32: aws.String("AAAAAA=="), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "The CRC32 you specified did not match the calculated checksum") {
		t.Fatalf("mismatched upload part checksum: %v", err)
	}
	if _, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), UploadId: checksumFaultUpload.UploadId, PartNumber: aws.Int32(1), ChecksumSHA256: aws.String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="), Body: strings.NewReader("part")}); err == nil || !strings.Contains(err.Error(), "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: sha256") {
		t.Fatalf("mismatched upload part checksum algorithm: %v", err)
	}
	checksumFaultPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), UploadId: checksumFaultUpload.UploadId, PartNumber: aws.Int32(1), ChecksumAlgorithm: s3types.ChecksumAlgorithmCrc32, Body: strings.NewReader("part")})
	if err != nil {
		t.Fatalf("upload checksum part for completion fault: %v", err)
	}
	if _, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-fault"), UploadId: checksumFaultUpload.UploadId, ChecksumType: s3types.ChecksumTypeFullObject, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: checksumFaultPart.ETag, ChecksumCRC32: checksumFaultPart.ChecksumCRC32}}}}); err == nil || !strings.Contains(err.Error(), "The upload was created using the COMPOSITE checksum mode. The complete request must use the same checksum mode.") {
		t.Fatalf("mismatched complete checksum type: %v", err)
	}
	if _, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{}}); err == nil || !strings.Contains(err.Error(), "You must specify at least one part") {
		t.Fatalf("empty multipart completion: %v", err)
	}
	if _, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(9), ETag: aws.String(`\"missing\"`)}}}}); err == nil || !strings.Contains(err.Error(), "One or more of the specified parts could not be found") {
		t.Fatalf("missing multipart completion part: %v", err)
	}
	preconditionUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("complete-precondition-fault")})
	if err != nil {
		t.Fatalf("create multipart precondition upload: %v", err)
	}
	preconditionPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String("complete-precondition-fault"), UploadId: preconditionUpload.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("part")})
	if err != nil {
		t.Fatalf("upload multipart precondition part: %v", err)
	}
	preconditionManifest := &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: preconditionPart.ETag}}}
	for name, conditions := range map[string]struct{ match, noneMatch *string }{
		"combined":      {aws.String(`"etag"`), aws.String("*")},
		"if-none-match": {nil, aws.String(`"etag"`)},
		"if-match-star": {aws.String("*"), nil},
	} {
		_, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("complete-precondition-fault"), UploadId: preconditionUpload.UploadId, MultipartUpload: preconditionManifest, IfMatch: conditions.match, IfNoneMatch: conditions.noneMatch})
		if err == nil || !strings.Contains(err.Error(), "StatusCode: 501") || !strings.Contains(err.Error(), "A header you provided implies functionality that is not implemented") {
			t.Fatalf("%s multipart precondition fault: %v", name, err)
		}
	}
	for name, conditions := range map[string]struct{ match, noneMatch *string }{
		"combined":      {aws.String(`"etag"`), aws.String("*")},
		"if-none-match": {nil, aws.String(`"etag"`)},
		"if-match-star": {aws.String("*"), nil},
	} {
		key := "write-precondition-" + name
		if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("old")}); err != nil {
			t.Fatalf("seed %s write precondition: %v", name, err)
		}
		_, putErr := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("new"), IfMatch: conditions.match, IfNoneMatch: conditions.noneMatch})
		_, copyErr := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), CopySource: aws.String("sdk/k"), IfMatch: conditions.match, IfNoneMatch: conditions.noneMatch})
		for operation, err := range map[string]error{"PutObject": putErr, "CopyObject": copyErr} {
			if err == nil || !strings.Contains(err.Error(), "StatusCode: 501") || !strings.Contains(err.Error(), "A header you provided implies functionality that is not implemented") {
				t.Fatalf("%s %s precondition fault: %v", operation, name, err)
			}
		}
		got, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("get %s after rejected writes: %v", name, err)
		}
		body, _ := io.ReadAll(got.Body)
		_ = got.Body.Close()
		if string(body) != "old" {
			t.Fatalf("%s rejected write body = %q", name, body)
		}
	}
	for _, operation := range []string{"PutObject", "CopyObject"} {
		for _, test := range []struct {
			name, code, status, message string
			match, noneMatch            *string
			existing                    bool
		}{
			{"missing-if-match", "NoSuchKey", "StatusCode: 404", "The specified key does not exist.", aws.String(`"missing"`), nil, false},
			{"wrong-if-match", "PreconditionFailed", "StatusCode: 412", "At least one of the pre-conditions you specified did not hold", aws.String(`"wrong"`), nil, true},
			{"if-none-match", "PreconditionFailed", "StatusCode: 412", "At least one of the pre-conditions you specified did not hold", nil, aws.String("*"), true},
		} {
			key := "write-condition-detail-" + operation + "-" + test.name
			if test.existing {
				if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("old")}); err != nil {
					t.Fatalf("seed %s %s: %v", operation, test.name, err)
				}
			}
			var err error
			if operation == "PutObject" {
				_, err = s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("new"), IfMatch: test.match, IfNoneMatch: test.noneMatch})
			} else {
				_, err = s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), CopySource: aws.String("sdk/k"), IfMatch: test.match, IfNoneMatch: test.noneMatch})
			}
			if err == nil || !strings.Contains(err.Error(), test.status) || !strings.Contains(err.Error(), test.code) || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("%s %s fault: %v", operation, test.name, err)
			}
			if test.existing {
				got, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key)})
				if err != nil {
					t.Fatalf("get %s %s after rejection: %v", operation, test.name, err)
				}
				body, _ := io.ReadAll(got.Body)
				_ = got.Body.Close()
				if string(body) != "old" {
					t.Fatalf("%s %s rejected body = %q", operation, test.name, body)
				}
			}
		}
	}
	for _, versioned := range []bool{false, true} {
		name := "unversioned"
		if versioned {
			name = "versioned"
		}
		bucket := "sdk-if-none-match-" + name
		if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("create %s If-None-Match bucket: %v", name, err)
		}
		if versioned {
			if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
				t.Fatalf("enable %s If-None-Match bucket: %v", name, err)
			}
		}
		input := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("key"), Body: strings.NewReader("first"), IfNoneMatch: aws.String("*")}
		if _, err := s3c.PutObject(context.Background(), input); err != nil {
			t.Fatalf("first %s If-None-Match put: %v", name, err)
		}
		input.Body = strings.NewReader("blocked")
		if _, err := s3c.PutObject(context.Background(), input); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
			t.Fatalf("second %s If-None-Match put: %v", name, err)
		}
		if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String("key")}); err != nil {
			t.Fatalf("delete %s If-None-Match object: %v", name, err)
		}
		input.Body = strings.NewReader("after-delete")
		if _, err := s3c.PutObject(context.Background(), input); err != nil {
			t.Fatalf("%s If-None-Match put after delete: %v", name, err)
		}
	}
	ifMatchBucket := "sdk-if-match-versioned"
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(ifMatchBucket)}); err != nil {
		t.Fatalf("create If-Match bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(ifMatchBucket), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable If-Match bucket: %v", err)
	}
	ifMatchFirst, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(ifMatchBucket), Key: aws.String("key"), Body: strings.NewReader("first")})
	if err != nil {
		t.Fatalf("seed If-Match object: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(ifMatchBucket), Key: aws.String("key"), Body: strings.NewReader("wrong"), IfMatch: aws.String("d41d8cd98f00b204e9800998ecf8427e")}); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("wrong If-Match put: %v", err)
	}
	ifMatchSecond, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(ifMatchBucket), Key: aws.String("key"), Body: strings.NewReader("matched"), IfMatch: ifMatchFirst.ETag})
	if err != nil {
		t.Fatalf("matched If-Match put: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(ifMatchBucket), Key: aws.String("key")}); err != nil {
		t.Fatalf("delete If-Match object: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(ifMatchBucket), Key: aws.String("key"), IfMatch: ifMatchSecond.ETag}); err == nil || !strings.Contains(err.Error(), "StatusCode: 404") || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("delete-marker If-Match put: %v", err)
	}
	copyBucket := "sdk-copy-write-versioned"
	if _, err := s3c.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(copyBucket)}); err != nil {
		t.Fatalf("create copy write bucket: %v", err)
	}
	if _, err := s3c.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{Bucket: aws.String(copyBucket), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable copy write versioning: %v", err)
	}
	copySource, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("source"), Body: strings.NewReader("source")})
	if err != nil {
		t.Fatalf("seed copy write source: %v", err)
	}
	copyInput := &s3.CopyObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("destination"), CopySource: aws.String(copyBucket + "/source"), IfNoneMatch: aws.String("*")}
	if _, err := s3c.CopyObject(context.Background(), copyInput); err != nil {
		t.Fatalf("first If-None-Match copy: %v", err)
	}
	if _, err := s3c.CopyObject(context.Background(), copyInput); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") {
		t.Fatalf("existing If-None-Match copy: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("destination")}); err != nil {
		t.Fatalf("delete copy destination: %v", err)
	}
	afterDelete, err := s3c.CopyObject(context.Background(), copyInput)
	if err != nil {
		t.Fatalf("delete-marker If-None-Match copy: %v", err)
	}
	copyInput.IfNoneMatch, copyInput.IfMatch = nil, aws.String(`"wrong"`)
	if _, err := s3c.CopyObject(context.Background(), copyInput); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") {
		t.Fatalf("wrong If-Match copy: %v", err)
	}
	copyInput.IfMatch = afterDelete.CopyObjectResult.ETag
	matched, err := s3c.CopyObject(context.Background(), copyInput)
	if err != nil {
		t.Fatalf("matching If-Match copy: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("destination")}); err != nil {
		t.Fatalf("delete matched copy destination: %v", err)
	}
	copyInput.IfMatch = matched.CopyObjectResult.ETag
	if _, err := s3c.CopyObject(context.Background(), copyInput); err == nil || !strings.Contains(err.Error(), "StatusCode: 404") || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("delete-marker If-Match copy: %v", err)
	}
	current, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("destination"), Body: strings.NewReader("current")})
	if err != nil {
		t.Fatalf("seed current copy destination: %v", err)
	}
	copyInput.IfMatch = current.ETag
	if _, err := s3c.CopyObject(context.Background(), copyInput); err != nil {
		t.Fatalf("current If-Match copy: %v", err)
	}
	inPlace := &s3.CopyObjectInput{Bucket: aws.String(copyBucket), Key: aws.String("source"), CopySource: aws.String(copyBucket + "/source"), IfNoneMatch: aws.String("*"), StorageClass: s3types.StorageClassStandard}
	if _, err := s3c.CopyObject(context.Background(), inPlace); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") {
		t.Fatalf("in-place If-None-Match copy: %v", err)
	}
	inPlace.IfNoneMatch, inPlace.IfMatch = nil, copySource.ETag
	if _, err := s3c.CopyObject(context.Background(), inPlace); err != nil {
		t.Fatalf("in-place If-Match copy: %v", err)
	}
	for _, operation := range []string{"PutObject", "CopyObject"} {
		key := "write-if-match-list-" + operation
		seed, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("old")})
		if err != nil {
			t.Fatalf("seed %s If-Match list: %v", operation, err)
		}
		condition := aws.String(`"wrong", ` + aws.ToString(seed.ETag))
		if operation == "PutObject" {
			_, err = s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), Body: strings.NewReader("new"), IfMatch: condition})
		} else {
			_, err = s3c.CopyObject(context.Background(), &s3.CopyObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key), CopySource: aws.String("sdk/k"), IfMatch: condition})
		}
		if err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
			t.Fatalf("%s If-Match list fault: %v", operation, err)
		}
		got, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("get %s after If-Match list: %v", operation, err)
		}
		body, _ := io.ReadAll(got.Body)
		_ = got.Body.Close()
		if string(body) != "old" {
			t.Fatalf("%s If-Match list stored %q", operation, body)
		}
	}
	conditionalUpload := func(key string) (string, *s3types.CompletedMultipartUpload) {
		t.Helper()
		created, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("create %s conditional upload: %v", key, err)
		}
		part, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{Bucket: aws.String("sdk"), Key: aws.String(key), UploadId: created.UploadId, PartNumber: aws.Int32(1), Body: strings.NewReader("part")})
		if err != nil {
			t.Fatalf("upload %s conditional part: %v", key, err)
		}
		return aws.ToString(created.UploadId), &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.ETag}}}
	}
	completeConditional := func(key, uploadID string, manifest *s3types.CompletedMultipartUpload, match, noneMatch *string) error {
		t.Helper()
		_, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String(key), UploadId: aws.String(uploadID), MultipartUpload: manifest, IfMatch: match, IfNoneMatch: noneMatch})
		return err
	}
	seedCompleteList, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-list"), Body: strings.NewReader("old")})
	if err != nil {
		t.Fatalf("seed complete If-Match list: %v", err)
	}
	uploadID, manifest := conditionalUpload("complete-if-match-list")
	listCondition := aws.String(`"wrong", ` + aws.ToString(seedCompleteList.ETag))
	if err := completeConditional("complete-if-match-list", uploadID, &s3types.CompletedMultipartUpload{}, listCondition, nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("complete If-Match validation order: %v", err)
	}
	if err := completeConditional("complete-if-match-list", uploadID, manifest, listCondition, nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("complete If-Match list fault: %v", err)
	}
	if parts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-list"), UploadId: aws.String(uploadID)}); err != nil || len(parts.Parts) != 1 {
		t.Fatalf("complete If-Match list upload: %#v %v", parts, err)
	}
	if err := completeConditional("complete-if-match-list", uploadID, manifest, seedCompleteList.ETag, nil); err != nil {
		t.Fatalf("exact complete If-Match: %v", err)
	}
	currentMultipart, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-list")})
	if err != nil || aws.ToString(currentMultipart.ETag) == aws.ToString(seedCompleteList.ETag) {
		t.Fatalf("multipart ETag after exact completion: %#v %v", currentMultipart, err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-list")
	if err := completeConditional("complete-if-match-list", uploadID, manifest, seedCompleteList.ETag, nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("stale original complete If-Match: %v", err)
	}
	if err := completeConditional("complete-if-match-list", uploadID, manifest, currentMultipart.ETag, nil); err != nil {
		t.Fatalf("current multipart complete If-Match: %v", err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-missing")
	if err := completeConditional("complete-if-match-missing", uploadID, manifest, aws.String(`"missing"`), nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 404") || !strings.Contains(err.Error(), "NoSuchKey") || !strings.Contains(err.Error(), "The specified key does not exist") {
		t.Fatalf("missing complete If-Match: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-mismatch"), Body: strings.NewReader("old")}); err != nil {
		t.Fatal(err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-mismatch")
	if err := completeConditional("complete-if-match-mismatch", uploadID, manifest, aws.String(`"wrong"`), nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") || !strings.Contains(err.Error(), "At least one of the pre-conditions you specified did not hold") {
		t.Fatalf("mismatched complete If-Match: %v", err)
	}
	uploadID, manifest = conditionalUpload("complete-if-none-created")
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-none-created"), Body: strings.NewReader("object")}); err != nil {
		t.Fatal(err)
	}
	if err := completeConditional("complete-if-none-created", uploadID, manifest, nil, aws.String("*")); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("created complete If-None-Match: %v", err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-none-deleted"), Body: strings.NewReader("object")}); err != nil {
		t.Fatal(err)
	}
	uploadID, manifest = conditionalUpload("complete-if-none-deleted")
	if err := completeConditional("complete-if-none-deleted", uploadID, manifest, nil, aws.String("*")); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("existing complete If-None-Match: %v", err)
	}
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-none-deleted")}); err != nil {
		t.Fatal(err)
	}
	if err := completeConditional("complete-if-none-deleted", uploadID, manifest, nil, aws.String("*")); err == nil || !strings.Contains(err.Error(), "StatusCode: 409") || !strings.Contains(err.Error(), "ConditionalRequestConflict") || !strings.Contains(err.Error(), "The conditional request cannot succeed due to a conflicting operation against this resource") {
		t.Fatalf("deleted complete If-None-Match: %v", err)
	}
	uploadID, manifest = conditionalUpload("complete-if-none-deleted")
	if err := completeConditional("complete-if-none-deleted", uploadID, manifest, nil, aws.String("*")); err != nil {
		t.Fatalf("restarted complete If-None-Match: %v", err)
	}
	ifMatchPut, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-put"), Body: strings.NewReader("old")})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-put")
	ifMatchReplacement, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-put"), Body: strings.NewReader("new")})
	if err != nil {
		t.Fatal(err)
	}
	if err := completeConditional("complete-if-match-put", uploadID, manifest, ifMatchPut.ETag, nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 412") || !strings.Contains(err.Error(), "PreconditionFailed") {
		t.Fatalf("stale complete If-Match: %v", err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-put")
	if err := completeConditional("complete-if-match-put", uploadID, manifest, ifMatchReplacement.ETag, nil); err != nil {
		t.Fatalf("restarted complete If-Match: %v", err)
	}
	ifMatchDeleted, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-delete"), Body: strings.NewReader("object")})
	if err != nil {
		t.Fatal(err)
	}
	uploadID, manifest = conditionalUpload("complete-if-match-delete")
	if _, err := s3c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("sdk"), Key: aws.String("complete-if-match-delete")}); err != nil {
		t.Fatal(err)
	}
	if err := completeConditional("complete-if-match-delete", uploadID, manifest, ifMatchDeleted.ETag, nil); err == nil || !strings.Contains(err.Error(), "StatusCode: 404") || !strings.Contains(err.Error(), "NoSuchKey") {
		t.Fatalf("deleted complete If-Match: %v", err)
	}
	otherUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy")})
	if err != nil {
		t.Fatalf("create second multipart copy: %v", err)
	}
	zeroUploads, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String("sdk"), Prefix: aws.String("range-"), MaxUploads: aws.Int32(0)})
	if err != nil || len(zeroUploads.Uploads) != 2 || aws.ToInt32(zeroUploads.MaxUploads) != 1000 {
		t.Fatalf("zero max multipart uploads: %#v %v", zeroUploads, err)
	}
	listedUploads, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
		Bucket: aws.String("sdk"), Prefix: aws.String("range-"), MaxUploads: aws.Int32(1),
	})
	if err != nil || len(listedUploads.Uploads) != 1 || aws.ToString(listedUploads.Uploads[0].Key) != "range-copy" || aws.ToString(listedUploads.NextKeyMarker) != "range-copy" || aws.ToString(listedUploads.NextUploadIdMarker) == "" {
		t.Fatalf("list multipart uploads: %#v %v", listedUploads, err)
	}
	nextUploads, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String("sdk"), Prefix: aws.String("range-"), MaxUploads: aws.Int32(1), KeyMarker: listedUploads.NextKeyMarker, UploadIdMarker: listedUploads.NextUploadIdMarker})
	if err != nil || len(nextUploads.Uploads) != 1 || aws.ToString(nextUploads.Uploads[0].UploadId) == aws.ToString(listedUploads.Uploads[0].UploadId) || aws.ToString(nextUploads.NextKeyMarker) != "range-copy" || aws.ToString(nextUploads.NextUploadIdMarker) != aws.ToString(nextUploads.Uploads[0].UploadId) || aws.ToString(nextUploads.Uploads[0].UploadId) != aws.ToString(upload.UploadId) && aws.ToString(nextUploads.Uploads[0].UploadId) != aws.ToString(otherUpload.UploadId) {
		t.Fatalf("next multipart uploads: %#v %v", nextUploads, err)
	}
	if _, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String("sdk"), KeyMarker: aws.String("wrong"), UploadIdMarker: upload.UploadId}); err == nil || !strings.Contains(err.Error(), "Invalid uploadId marker") {
		t.Fatalf("mismatched multipart marker: %v", err)
	}
	if _, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, ExpectedBucketOwner: aws.String("999999999999")}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("list parts mismatched expected owner: %v", err)
	}
	if _, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: aws.String("missing")}); err == nil || !strings.Contains(err.Error(), "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.") {
		t.Fatalf("list parts missing upload: %v", err)
	}
	smallCopySource, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("small-copy-source"), Body: strings.NewReader("0123456789")})
	if err != nil {
		t.Fatalf("put small copy source: %v", err)
	}
	smallCopyHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String("sdk"), Key: aws.String("small-copy-source")})
	if err != nil {
		t.Fatalf("head small copy source: %v", err)
	}
	newCopyUpload := func(key string) *string {
		t.Helper()
		created, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{Bucket: aws.String("sdk"), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("create copy upload %s: %v", key, err)
		}
		return created.UploadId
	}
	for _, tc := range []struct {
		name, copyRange, wantError string
	}{
		{name: "no-range"},
		{name: "small-range", copyRange: "bytes=0-8"},
		{name: "malformed-range", copyRange: "0-8", wantError: "InvalidArgument"},
		{name: "past-end", copyRange: "bytes=0-100", wantError: "Range specified is not valid for source object of size: 10"},
		{name: "after-object", copyRange: "bytes=100-200", wantError: "The specified copy range is invalid for the source object size"},
	} {
		uploadID := newCopyUpload("copy-range-" + tc.name)
		input := &s3.UploadPartCopyInput{Bucket: aws.String("sdk"), Key: aws.String("copy-range-" + tc.name), UploadId: uploadID, PartNumber: aws.Int32(1), CopySource: aws.String("sdk/small-copy-source")}
		if tc.copyRange != "" {
			input.CopySourceRange = aws.String(tc.copyRange)
		}
		_, err := s3c.UploadPartCopy(context.Background(), input)
		if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
			t.Fatalf("upload part copy %s: %v", tc.name, err)
		}
	}
	for _, tc := range []struct {
		name, wantError string
		configure       func(*s3.UploadPartCopyInput)
	}{
		{name: "if-match", configure: func(in *s3.UploadPartCopyInput) { in.CopySourceIfMatch = smallCopySource.ETag }},
		{name: "if-none-match", configure: func(in *s3.UploadPartCopyInput) { in.CopySourceIfNoneMatch = aws.String(`"not-matching"`) }},
		{name: "if-unmodified-since", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfUnmodifiedSince = aws.Time(smallCopyHead.LastModified.Add(time.Second))
		}},
		{name: "if-modified-since", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfModifiedSince = aws.Time(smallCopyHead.LastModified.Add(-time.Second))
		}},
		{name: "future-if-modified-since", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfModifiedSince = aws.Time(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
		}},
		{name: "failed-if-match", wantError: "StatusCode: 412", configure: func(in *s3.UploadPartCopyInput) { in.CopySourceIfMatch = aws.String(`"not-matching"`) }},
		{name: "failed-if-none-match", wantError: "StatusCode: 412", configure: func(in *s3.UploadPartCopyInput) { in.CopySourceIfNoneMatch = smallCopySource.ETag }},
		{name: "failed-if-unmodified-since", wantError: "StatusCode: 412", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfUnmodifiedSince = aws.Time(smallCopyHead.LastModified.Add(-time.Second))
		}},
		{name: "match-overrides-unmodified", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfMatch, in.CopySourceIfUnmodifiedSince = smallCopySource.ETag, aws.Time(smallCopyHead.LastModified.Add(-time.Second))
		}},
		{name: "failed-none-match-and-unmodified", wantError: "StatusCode: 412", configure: func(in *s3.UploadPartCopyInput) {
			in.CopySourceIfNoneMatch, in.CopySourceIfUnmodifiedSince = aws.String(`"not-matching"`), aws.Time(smallCopyHead.LastModified.Add(-time.Second))
		}},
		{name: "failed-if-modified-since", wantError: "StatusCode: 412", configure: func(in *s3.UploadPartCopyInput) { in.CopySourceIfModifiedSince = smallCopyHead.LastModified }},
	} {
		uploadID := newCopyUpload("copy-condition-" + tc.name)
		input := &s3.UploadPartCopyInput{Bucket: aws.String("sdk"), Key: aws.String("copy-condition-" + tc.name), UploadId: uploadID, PartNumber: aws.Int32(1), CopySource: aws.String("sdk/small-copy-source")}
		tc.configure(input)
		_, err := s3c.UploadPartCopy(context.Background(), input)
		if tc.wantError == "" && err != nil || tc.wantError != "" && (err == nil || !strings.Contains(err.Error(), tc.wantError)) {
			t.Fatalf("upload part copy condition %s: %v", tc.name, err)
		}
	}
	if _, err := s3c.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1), CopySource: aws.String("sdk/large"), ExpectedSourceBucketOwner: aws.String("999999999999"),
	}); err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("multipart mismatched expected source owner: %v", err)
	}
	emptyParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: otherUpload.UploadId})
	if err != nil || len(emptyParts.Parts) != 0 || aws.ToString(emptyParts.PartNumberMarker) != "0" || aws.ToString(emptyParts.NextPartNumberMarker) != "0" || aws.ToInt32(emptyParts.MaxParts) != 1000 || emptyParts.Initiator == nil || aws.ToString(emptyParts.Initiator.ID) != "000000000000" || emptyParts.Owner == nil || aws.ToString(emptyParts.Owner.ID) != "000000000000" {
		t.Fatalf("empty list parts: %#v %v", emptyParts, err)
	}
	part, err := s3c.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, PartNumber: aws.Int32(1),
		CopySource: aws.String("sdk/large"), CopySourceIfMatch: largePut.ETag, CopySourceRange: aws.String("bytes=10-19"), ExpectedSourceBucketOwner: aws.String("000000000000"),
	})
	if err != nil {
		t.Fatalf("upload part copy: %v", err)
	}
	zeroParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, MaxParts: aws.Int32(0)})
	if err != nil || len(zeroParts.Parts) != 1 || aws.ToInt32(zeroParts.MaxParts) != 1000 {
		t.Fatalf("zero max parts: %#v %v", zeroParts, err)
	}
	listedParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId, MaxParts: aws.Int32(1),
	})
	if err != nil || len(listedParts.Parts) != 1 || aws.ToInt32(listedParts.Parts[0].PartNumber) != 1 || aws.ToString(listedParts.Parts[0].ETag) != aws.ToString(part.CopyPartResult.ETag) || aws.ToString(listedParts.UploadId) != aws.ToString(upload.UploadId) || aws.ToString(listedParts.PartNumberMarker) != "0" || aws.ToString(listedParts.NextPartNumberMarker) != "1" || aws.ToBool(listedParts.IsTruncated) || listedParts.Parts[0].LastModified == nil || listedParts.Parts[0].LastModified.Nanosecond() != 0 {
		t.Fatalf("list parts: %#v %v", listedParts, err)
	}
	completed, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("range-copy"), UploadId: upload.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: part.CopyPartResult.ETag}}},
	})
	if err != nil {
		t.Fatalf("complete multipart copy: %v", err)
	}
	rangeCopy, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String("sdk"), Key: aws.String("range-copy")})
	if err != nil {
		t.Fatalf("get range copy: %v", err)
	}
	rangeBody, _ := io.ReadAll(rangeCopy.Body)
	_ = rangeCopy.Body.Close()
	if string(rangeBody) != "0123456789" {
		t.Fatalf("range copy body %q", rangeBody)
	}
	if aws.ToString(rangeCopy.ETag) != aws.ToString(completed.ETag) {
		t.Fatalf("persisted multipart ETag %q want %q", aws.ToString(rangeCopy.ETag), aws.ToString(completed.ETag))
	}
	if _, err := s3c.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("multipart-copy"), CopySource: aws.String("sdk/range-copy"), CopySourceIfMatch: completed.ETag,
	}); err != nil {
		t.Fatalf("copy multipart ETag: %v", err)
	}
	checksumUpload, err := s3c.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
		StorageClass: s3types.StorageClassStandardIa, Tagging: aws.String("env=test&team=storage"),
	})
	if err != nil {
		t.Fatalf("create checksum multipart: %v", err)
	}
	checksumPart, err := s3c.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
		PartNumber: aws.Int32(1), Body: bytes.NewReader([]byte("checksum-sdk")), ChecksumAlgorithm: s3types.ChecksumAlgorithmSha256,
	})
	if err != nil || aws.ToString(checksumPart.ChecksumSHA256) == "" {
		t.Fatalf("upload checksum part: %#v %v", checksumPart, err)
	}
	checksumParts, err := s3c.ListParts(context.Background(), &s3.ListPartsInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
	})
	if err != nil || checksumParts.ChecksumAlgorithm != s3types.ChecksumAlgorithmSha256 || checksumParts.Initiator == nil || aws.ToString(checksumParts.Initiator.DisplayName) != "webfile" || len(checksumParts.Parts) != 1 || aws.ToString(checksumParts.Parts[0].ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("list checksum parts: %#v %v", checksumParts, err)
	}
	checksumUploads, err := s3c.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{Bucket: aws.String("sdk"), Prefix: aws.String("checksum-multipart")})
	if err != nil || len(checksumUploads.Uploads) != 1 || checksumUploads.Uploads[0].ChecksumAlgorithm != s3types.ChecksumAlgorithmSha256 || checksumUploads.Uploads[0].ChecksumType != s3types.ChecksumTypeComposite || checksumUploads.Uploads[0].Initiator == nil || aws.ToString(checksumUploads.Uploads[0].Initiator.DisplayName) != "webfile" {
		t.Fatalf("list checksum multipart uploads: %#v %v", checksumUploads, err)
	}
	checksumComplete, err := s3c.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), UploadId: checksumUpload.UploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: []s3types.CompletedPart{{PartNumber: aws.Int32(1), ETag: checksumPart.ETag, ChecksumSHA256: checksumPart.ChecksumSHA256}}},
	})
	if err != nil || !strings.HasSuffix(aws.ToString(checksumComplete.ChecksumSHA256), "-1") {
		t.Fatalf("complete checksum multipart: %#v %v", checksumComplete, err)
	}
	checksumHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil || aws.ToString(checksumHead.ChecksumSHA256) != aws.ToString(checksumComplete.ChecksumSHA256) || checksumHead.StorageClass != s3types.StorageClassStandardIa {
		t.Fatalf("head checksum multipart: %#v %v", checksumHead, err)
	}
	checksumPartGet, err := s3c.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), PartNumber: aws.Int32(1), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("get multipart part: %v", err)
	}
	checksumPartBody, _ := io.ReadAll(checksumPartGet.Body)
	_ = checksumPartGet.Body.Close()
	if string(checksumPartBody) != "checksum-sdk" || aws.ToInt32(checksumPartGet.PartsCount) != 1 || aws.ToString(checksumPartGet.ContentRange) != "bytes 0-11/12" || aws.ToString(checksumPartGet.ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("get multipart part: %#v body=%q", checksumPartGet, checksumPartBody)
	}
	checksumPartHead, err := s3c.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), PartNumber: aws.Int32(1), ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil || aws.ToInt32(checksumPartHead.PartsCount) != 1 || aws.ToInt64(checksumPartHead.ContentLength) != 12 || aws.ToString(checksumPartHead.ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("head multipart part: %#v %v", checksumPartHead, err)
	}
	if _, err := s3c.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String("sdk"), Key: aws.String("standard-attributes"), Body: strings.NewReader("body")}); err != nil {
		t.Fatal(err)
	}
	standardAttributes, err := s3c.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{Bucket: aws.String("sdk"), Key: aws.String("standard-attributes"), ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesStorageClass}})
	if err != nil || standardAttributes.StorageClass != s3types.StorageClassStandard {
		t.Fatalf("standard storage attributes: %#v %v", standardAttributes, err)
	}
	checksumAttributes, err := s3c.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), MaxParts: aws.Int32(1),
		ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesEtag, s3types.ObjectAttributesChecksum, s3types.ObjectAttributesObjectParts, s3types.ObjectAttributesStorageClass, s3types.ObjectAttributesObjectSize},
	})
	wantAttributeChecksum := strings.SplitN(aws.ToString(checksumComplete.ChecksumSHA256), "-", 2)[0]
	if err != nil || aws.ToString(checksumAttributes.ETag) != aws.ToString(checksumComplete.ETag) || aws.ToInt64(checksumAttributes.ObjectSize) != 12 || checksumAttributes.StorageClass != s3types.StorageClassStandardIa || checksumAttributes.LastModified == nil || checksumAttributes.Checksum == nil || aws.ToString(checksumAttributes.Checksum.ChecksumSHA256) != wantAttributeChecksum || checksumAttributes.ObjectParts == nil || aws.ToInt32(checksumAttributes.ObjectParts.TotalPartsCount) != 1 || len(checksumAttributes.ObjectParts.Parts) != 1 || aws.ToInt32(checksumAttributes.ObjectParts.Parts[0].PartNumber) != 1 || aws.ToInt64(checksumAttributes.ObjectParts.Parts[0].Size) != 12 || aws.ToString(checksumAttributes.ObjectParts.Parts[0].ChecksumSHA256) != aws.ToString(checksumPart.ChecksumSHA256) {
		t.Fatalf("get object attributes: etag=%q size=%d class=%q modified=%v checksum=%q parts=%#v err=%v", aws.ToString(checksumAttributes.ETag), aws.ToInt64(checksumAttributes.ObjectSize), checksumAttributes.StorageClass, checksumAttributes.LastModified, aws.ToString(checksumAttributes.Checksum.ChecksumSHA256), checksumAttributes.ObjectParts, err)
	}
	emptyAttributes, err := s3c.GetObjectAttributes(context.Background(), &s3.GetObjectAttributesInput{
		Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart"), MaxParts: aws.Int32(1), PartNumberMarker: aws.String("10"), ObjectAttributes: []s3types.ObjectAttributes{s3types.ObjectAttributesObjectParts},
	})
	if err != nil || emptyAttributes.ObjectParts == nil || len(emptyAttributes.ObjectParts.Parts) != 0 || aws.ToString(emptyAttributes.ObjectParts.PartNumberMarker) != "10" || aws.ToString(emptyAttributes.ObjectParts.NextPartNumberMarker) != "0" || aws.ToBool(emptyAttributes.ObjectParts.IsTruncated) {
		t.Fatalf("empty object attributes page: %#v %v", emptyAttributes, err)
	}
	checksumTags, err := s3c.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{Bucket: aws.String("sdk"), Key: aws.String("checksum-multipart")})
	if err != nil || len(checksumTags.TagSet) != 2 || aws.ToString(checksumTags.TagSet[0].Key) != "env" || aws.ToString(checksumTags.TagSet[0].Value) != "test" || aws.ToString(checksumTags.TagSet[1].Key) != "team" || aws.ToString(checksumTags.TagSet[1].Value) != "storage" {
		t.Fatalf("multipart creation tags: %#v %v", checksumTags, err)
	}

	ddb := dynamodb.NewFromConfig(awscfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(ts.URL) })
	if _, err := ddb.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String("T"),
		KeySchema: []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := ddb.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String("T"),
		Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
	}); err != nil {
		t.Fatalf("put item: %v", err)
	}
	item, err := ddb.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName: aws.String("T"),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "1"}},
	})
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	idAttr, ok := item.Item["id"].(*ddbtypes.AttributeValueMemberS)
	if !ok || idAttr.Value != "1" {
		t.Fatalf("ddb item %#v", item.Item)
	}

	sqsc := sqs.NewFromConfig(awscfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(ts.URL) })
	if _, err := sqsc.CreateQueue(context.Background(), &sqs.CreateQueueInput{QueueName: aws.String("q")}); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	if _, err := sqsc.SendMessage(context.Background(), &sqs.SendMessageInput{
		QueueUrl: aws.String(ts.URL + "/000000000000/q"), MessageBody: aws.String("hello-sqs-sdk"),
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	recv, err := sqsc.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(ts.URL + "/000000000000/q"), MaxNumberOfMessages: 1, WaitTimeSeconds: 0,
	})
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if len(recv.Messages) != 1 || aws.ToString(recv.Messages[0].Body) != "hello-sqs-sdk" {
		t.Fatalf("recv %#v", recv.Messages)
	}
}
