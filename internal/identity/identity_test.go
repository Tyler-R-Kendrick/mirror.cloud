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
	if id := Parse(v2, "", "", time.Unix(0, 0)); id.AccessKeyID != "AKIATEST" {
		t.Fatalf("SigV2 access key = %q", id.AccessKeyID)
	}
	tooLong := httptest.NewRequest("GET", "/x?X-Amz-Date=20200101T000000Z&X-Amz-Expires=604801", nil)
	if expires, ok := PresignedExpiry(tooLong); ok {
		t.Fatalf("accepted excessive SigV4 expiry %v", expires)
	}
}

func TestPresignedAuthFault(t *testing.T) {
	for name, target := range map[string]string{
		"none":       "/x?list-type=2",
		"sigv2":      "/x?AWSAccessKeyId=test&Signature=00",
		"sigv2-full": "/x?AWSAccessKeyId=test&Signature=00&Expires=90",
		"sigv4":      "/x?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=00&X-Amz-Expires=60&X-Amz-SignedHeaders=host",
		"sigv4-full": "/x?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=00&X-Amz-Date=20200101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host",
	} {
		fault := PresignedAuthFault(httptest.NewRequest("GET", target, nil))
		switch name {
		case "sigv2":
			if fault == nil || fault.Code != "AccessDenied" || fault.HTTPStatus != 403 {
				t.Fatalf("%s fault %#v", name, fault)
			}
		case "sigv4":
			if fault == nil || fault.Code != "AuthorizationQueryParametersError" || fault.HTTPStatus != 400 {
				t.Fatalf("%s fault %#v", name, fault)
			}
		default:
			if fault != nil {
				t.Fatalf("%s unexpected fault %#v", name, fault)
			}
		}
	}
}
