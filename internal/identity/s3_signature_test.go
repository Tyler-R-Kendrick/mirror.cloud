package identity

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVerifyS3PresignedV4AWSExample(t *testing.T) {
	const rawURL = "https://examplebucket.s3.amazonaws.com/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
	request := httptest.NewRequest(http.MethodGet, rawURL, nil)
	if fault := VerifyS3PresignedV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	query := request.URL.Query()
	query.Set("X-Amz-Signature", "00eed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404")
	request.URL.RawQuery = query.Encode()
	if fault := VerifyS3PresignedV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signature accepted: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV4AWSExample(t *testing.T) {
	request := awsAuthorizationExample()
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	request.Header.Set("Range", "bytes=1-9")
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed header accepted: %#v", fault)
	}
	request = awsAuthorizationExample()
	request.Body = io.NopCloser(strings.NewReader("tampered"))
	if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered payload accepted: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV4RejectsMalformed(t *testing.T) {
	for name, mutate := range map[string]func(*http.Request){
		"duplicate field": func(r *http.Request) { r.Header.Set("Authorization", r.Header.Get("Authorization")+",Signature=00") },
		"missing date": func(r *http.Request) {
			r.Header.Del("X-Amz-Date")
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256,Signature=29575ca434952f9bf8b87b84161cfacd0a7809cc36a3c12984abaef2bdc7e4f6")
		},
		"missing payload": func(r *http.Request) {
			r.Header.Del("X-Amz-Content-Sha256")
			r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-date,Signature=1b0e851a1c8d0a8c713d10578e54ae3d418b1d457acf62c7a22dd66c3b50f178")
		},
		"missing header": func(r *http.Request) { r.Header.Del("Range") },
		"wrong service": func(r *http.Request) {
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "/s3/", "/sts/", 1))
			r.Header.Set("Authorization", strings.Replace(r.Header.Get("Authorization"), "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41", "138618539e0e3b441c624f9b403b36cd3c81c715ee31d553b9c17446d453bf4c", 1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			request := awsAuthorizationExample()
			mutate(request)
			if fault := VerifyS3AuthorizationV4(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
				t.Fatalf("malformed authorization accepted: %#v", fault)
			}
		})
	}
	request := awsAuthorizationExample()
	request.Header.Set("Authorization", "Bearer token")
	if fault := VerifyS3AuthorizationV4(request, "unused"); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV2AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg", nil)
	request.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
	request.Header.Set("Authorization", "AWS AKIAIOSFODNN7EXAMPLE:qgk2+6Sv9/oM7G3qLEjTH1a1l1g=")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV2(request, secret); fault != nil {
		t.Fatalf("official AWS example rejected: %#v", fault)
	}
	request.Header.Set("Date", "Tue, 27 Mar 2007 19:36:43 +0000")
	if fault := VerifyS3AuthorizationV2(request, secret); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered date accepted: %#v", fault)
	}
}

func TestVerifyS3AuthorizationV2DateAndGrammar(t *testing.T) {
	request := httptest.NewRequest(http.MethodDelete, "https://localhost/bucket/key", nil)
	request.Header.Set("Date", "ignored")
	request.Header.Set("X-Amz-Date", "Tue, 27 Mar 2007 21:20:26 +0000")
	stringToSign := "DELETE\n\n\n\nx-amz-date:Tue, 27 Mar 2007 21:20:26 +0000\n/bucket/key"
	signature := base64.StdEncoding.EncodeToString(hmacSHA1([]byte("test"), stringToSign))
	request.Header.Set("Authorization", "AWS test:"+signature)
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("x-amz-date request rejected: %#v", fault)
	}
	request.Header.Set("Date", "still ignored")
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("Date overrode x-amz-date: %#v", fault)
	}
	for _, authorization := range []string{"AWS ", "AWS test", "AWS :" + signature, "AWS test:", "AWS test:" + signature + ":extra"} {
		request.Header.Set("Authorization", authorization)
		if fault := VerifyS3AuthorizationV2(request, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
			t.Fatalf("malformed %q accepted: %#v", authorization, fault)
		}
	}
	request.Header.Del("Date")
	request.Header.Del("X-Amz-Date")
	emptyDateSignature := base64.StdEncoding.EncodeToString(hmacSHA1([]byte("test"), "DELETE\n\n\n\n/bucket/key"))
	request.Header.Set("Authorization", "AWS test:"+emptyDateSignature)
	if fault := VerifyS3AuthorizationV2(request, "test"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("missing timestamp accepted: %#v", fault)
	}
	request.Header.Set("Authorization", "Bearer token")
	if fault := VerifyS3AuthorizationV2(request, "test"); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestS3AuthorizationTimeFault(t *testing.T) {
	now := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	request := httptest.NewRequest(http.MethodGet, "https://example.test", nil)
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 signed")
	for name, at := range map[string]time.Time{
		"past boundary":   now.Add(-15 * time.Minute),
		"future boundary": now.Add(15 * time.Minute),
	} {
		request.Header.Set("X-Amz-Date", at.Format("20060102T150405Z"))
		if fault := S3AuthorizationTimeFault(request, now); fault != nil {
			t.Fatalf("%s rejected: %#v", name, fault)
		}
	}
	request.Header.Set("X-Amz-Date", now.Add(-15*time.Minute-time.Second).Format("20060102T150405Z"))
	if fault := S3AuthorizationTimeFault(request, now); fault == nil || fault.Code != "RequestTimeTooSkewed" || fault.HTTPStatus != http.StatusForbidden || fault.Fields["RequestTime"] == nil || fault.Fields["ServerTime"] == nil {
		t.Fatalf("past skew fault = %#v", fault)
	}
	request.Header.Set("X-Amz-Date", now.Add(15*time.Minute+time.Second).Format("20060102T150405Z"))
	if fault := S3AuthorizationTimeFault(request, now); fault == nil || fault.Code != "RequestTimeTooSkewed" {
		t.Fatalf("future skew fault = %#v", fault)
	}
	request.Header.Set("Authorization", "AWS test:signature")
	request.Header.Set("X-Amz-Date", now.Format(http.TimeFormat))
	if fault := S3AuthorizationTimeFault(request, now); fault != nil {
		t.Fatalf("SigV2 x-amz-date rejected: %#v", fault)
	}
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set("X-Amz-Date", "invalid")
	if fault := S3AuthorizationTimeFault(request, now); fault != nil {
		t.Fatalf("unrelated authorization rejected: %#v", fault)
	}
}

func TestVerifyS3StreamingV4AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	request.ContentLength = 66824
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	request.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;content-length;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class,Signature=4f232c4386841ef735655705268965c44a0e4690baa4adea153f7db9fa80a0a9")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV4(request, secret); fault != nil {
		t.Fatalf("official AWS seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{bytes.Repeat([]byte{'a'}, 64*1024), bytes.Repeat([]byte{'a'}, 1024), nil}
	signatures := []string{"ad80c730a21e5b8d04586a2213dd63b9a0e99e0e2307b0ade35a65485a288648", "0055627c9e194cb4542bae2aa5492e3c1575bbb81b612b7d234b86a503ef5497", "b6c6ea8a5354eaf15b3cb7646744f4275b71ea724fed81ceb9323e279d449df9"}
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, nil); fault != nil {
		t.Fatalf("official AWS chunk signatures rejected: %#v", fault)
	}
	if fault := VerifyS3StreamingV4(request, secret, chunks[:2], signatures[:2], nil); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("stream without final chunk accepted: %#v", fault)
	}
	chunks[1][0] = 'b'
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, nil); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed chunk accepted: %#v", fault)
	}
}

func TestVerifyS3StreamingTrailerV4AWSExample(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.amazonaws.com/examplebucket/chunkObject.txt", nil)
	request.ContentLength = 66946
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "66560")
	request.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-storage-class;x-amz-trailer,Signature=106e2a8a18243abcf37539882f36619c00e2dfc72633413f02d3b74544bfeb8e")
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	if fault := VerifyS3AuthorizationV4(request, secret); fault != nil {
		t.Fatalf("official AWS trailer seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{bytes.Repeat([]byte{'a'}, 64*1024), bytes.Repeat([]byte{'a'}, 1024), nil}
	signatures := []string{"b474d8862b1487a5145d686f57f013e54db672cee1c953b3010fb58501ef5aa2", "1c1344b170168f8e65b41376b44b20fe354e373826ccbbe2c1d40a8cae51e5c7", "2ca2aba2005185cf7159c6277faf83795951dd77a3a99e6e65d5c9f85863f992"}
	trailers := http.Header{"X-Amz-Checksum-Crc32c": {"sOO8/Q=="}, "X-Amz-Trailer-Signature": {"d81f82fc3505edab99d459891051a732e8730629a2e4a59689829ca17fe2e435"}}
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault != nil {
		t.Fatalf("official AWS trailer signature rejected: %#v", fault)
	}
	authorization := request.Header.Get("Authorization")
	request.Header.Set("Authorization", strings.Replace(authorization, ";x-amz-trailer", "", 1))
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unsigned trailer declaration accepted: %#v", fault)
	}
	request.Header.Set("Authorization", authorization)
	extra := trailers.Clone()
	extra.Set("X-Amz-Extra", "value")
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, extra); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("undeclared trailer accepted: %#v", fault)
	}
	trailers.Set("X-Amz-Checksum-Crc32c", "AAAAAA==")
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed trailer accepted: %#v", fault)
	}
}

func TestVerifyS3StreamingUnsignedTrailerV4(t *testing.T) {
	request := httptest.NewRequest(http.MethodPut, "https://s3.localhost.localstack.cloud:4566/unsigned/object", nil)
	request.Header.Set("Content-Encoding", "aws-chunked")
	request.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	request.Header.Set("X-Amz-Date", "20990101T000000Z")
	request.Header.Set("X-Amz-Decoded-Content-Length", "5")
	request.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
	if fault := VerifyS3AuthorizationV4(request, "test"); fault != nil {
		t.Fatalf("unsigned trailer seed signature rejected: %#v", fault)
	}
	chunks := [][]byte{[]byte("hello"), nil}
	signatures := []string{"", ""}
	trailers := http.Header{"X-Amz-Checksum-Crc32c": {"mnG7TA=="}}
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault != nil {
		t.Fatalf("unsigned trailer rejected: %#v", fault)
	}
	signatures[0] = "unexpected"
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("unsigned stream with chunk signature accepted: %#v", fault)
	}
	signatures[0] = ""
	trailers.Set("X-Amz-Extra", "value")
	if fault := VerifyS3StreamingV4(request, "test", chunks, signatures, trailers); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("undeclared unsigned trailer accepted: %#v", fault)
	}
}

func awsAuthorizationExample() *http.Request {
	request := httptest.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	request.Header.Set("Range", "bytes=0-9")
	request.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	request.Header.Set("X-Amz-Date", "20130524T000000Z")
	request.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	return request
}

func TestVerifyS3PresignedV2AWSGrammar(t *testing.T) {
	// AWS documents this exact query-auth string-to-sign grammar. The
	// published signature uses unrelated legacy credentials, so this fixture's
	// signature was independently reproduced with Python's stdlib HMAC-SHA1.
	const rawURL = "https://awsexamplebucket1.s3.us-west-1.amazonaws.com/photos/puppy.jpg?AWSAccessKeyId=AKIAIOSFODNN7EXAMPLE&Expires=1175139620&Signature=1No4mq5ETf02z8aet9voy6gui6E%3D"
	request := httptest.NewRequest(http.MethodGet, rawURL, nil)
	if fault := VerifyS3PresignedV2(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault != nil {
		t.Fatalf("AWS grammar fixture rejected: %#v", fault)
	}
	query := request.URL.Query()
	query.Set("Signature", "AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	request.URL.RawQuery = query.Encode()
	if fault := VerifyS3PresignedV2(request, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signature accepted: %#v", fault)
	}
	pathStyle := httptest.NewRequest(http.MethodGet, "https://localhost/bucket/key?acl&AWSAccessKeyId=test&Expires=4070908800&Signature=wUhjtATf5qx6xYeZ1XjoZQHJKjg%3D", nil)
	if fault := VerifyS3PresignedV2(pathStyle, "test"); fault != nil {
		t.Fatalf("path-style subresource rejected: %#v", fault)
	}
}

func TestVerifyS3SessionToken(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://localhost/key?X-Amz-Security-Token=session", nil)
	if fault := VerifyS3SessionToken(request, "session"); fault != nil {
		t.Fatalf("valid query token rejected: %#v", fault)
	}
	request.URL.RawQuery = ""
	request.Header.Set("X-Amz-Security-Token", "session")
	if fault := VerifyS3SessionToken(request, "session"); fault != nil {
		t.Fatalf("valid header token rejected: %#v", fault)
	}
	request.Header.Del("X-Amz-Security-Token")
	if fault := VerifyS3SessionToken(request, "session"); fault == nil || fault.Code != "InvalidToken" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("missing token accepted: %#v", fault)
	}
}
