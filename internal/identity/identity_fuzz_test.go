package identity

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func FuzzParse(f *testing.F) {
	f.Add("AWS4-HMAC-SHA256 Credential=AKIATEST/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00", "")
	f.Add("", "AKIATEST/20200101/us-east-1/s3/aws4_request")
	f.Fuzz(func(t *testing.T, auth, cred string) {
		r := httptest.NewRequest("GET", "/x?X-Amz-Credential="+url.QueryEscape(cred)+"&X-Amz-Date=20200101T000000Z&X-Amz-Expires=60", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		_ = Parse(r, "000000000000", "us-east-1", time.Unix(0, 0).UTC())
	})
}
