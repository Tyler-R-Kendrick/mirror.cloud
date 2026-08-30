package identity

import (
	"net/http"
	"net/http/httptest"
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
