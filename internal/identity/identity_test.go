package identity

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseAndExpiry(t *testing.T) {
	r := httptest.NewRequest("GET", "/x?X-Amz-Credential=AKIATEST/20200101/us-east-1/s3/aws4_request&X-Amz-Date=20200101T000000Z&X-Amz-Expires=60", nil)
	now := time.Date(2020, 1, 1, 0, 0, 30, 0, time.UTC)
	id := Parse(r, "", "", now)
	if id.AccessKeyID != "AKIATEST" {
		t.Fatalf("akid %q", id.AccessKeyID)
	}
	if id.Region != "us-east-1" {
		t.Fatalf("region %q", id.Region)
	}
	if Expired(id) {
		t.Fatal("should not be expired at +30s")
	}
	id2 := Parse(r, "", "", now.Add(2*time.Minute))
	if !Expired(id2) {
		t.Fatal("should be expired at +2m")
	}

	v2 := httptest.NewRequest("GET", "/x?AWSAccessKeyId=AKIATEST&Expires=90&Signature=00", nil)
	if expires, ok := PresignedExpiry(v2); !ok || !expires.Equal(time.Unix(90, 0)) {
		t.Fatalf("sigv2 expiry = %v, %v", expires, ok)
	}
	if !Expired(Parse(v2, "", "", time.Unix(90, 0))) {
		t.Fatal("SigV2 request should expire at its boundary")
	}
}
