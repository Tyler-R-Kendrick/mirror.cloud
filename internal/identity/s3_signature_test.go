package identity

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures); fault != nil {
		t.Fatalf("official AWS chunk signatures rejected: %#v", fault)
	}
	chunks[1][0] = 'b'
	if fault := VerifyS3StreamingV4(request, secret, chunks, signatures); fault == nil || fault.Code != "SignatureDoesNotMatch" {
		t.Fatalf("tampered signed chunk accepted: %#v", fault)
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
