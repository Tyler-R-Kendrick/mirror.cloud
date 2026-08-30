package identity

import (
	"net/http"
	"testing"
)

func FuzzVerifyS3PresignedV4(f *testing.F) {
	f.Add("GET", "/test.txt", "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404", "examplebucket.s3.amazonaws.com", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	f.Fuzz(func(t *testing.T, method, path, signature, host, secret string) {
		if method == "" || path == "" || path[0] != '/' {
			t.Skip()
		}
		target := "https://example.test" + path + "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20130524T000000Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=" + signature
		request, err := http.NewRequest(method, target, nil)
		if err != nil {
			t.Skip()
		}
		request.Host = host
		_ = VerifyS3PresignedV4(request, secret)
	})
}
