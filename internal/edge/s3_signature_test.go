package edge

import "testing"

func TestSignedGatewayHost(t *testing.T) {
	for _, tc := range []struct {
		host, bind, want string
	}{
		{"s3.localhost.localstack.cloud:443", "127.0.0.1:4566", "s3.localhost.localstack.cloud:4566"},
		{"s3.localhost.localstack.cloud", "127.0.0.1:4566", "s3.localhost.localstack.cloud:4566"},
		{"s3.localhost.localstack.cloud:8443", "127.0.0.1:4566", ""},
		{"s3.localhost.localstack.cloud", "127.0.0.1:0", ""},
	} {
		if got := signedGatewayHost(tc.host, tc.bind); got != tc.want {
			t.Fatalf("signedGatewayHost(%q, %q) = %q, want %q", tc.host, tc.bind, got, tc.want)
		}
	}
}

func FuzzSignedGatewayHost(f *testing.F) {
	f.Add("s3.localhost.localstack.cloud:443", "127.0.0.1:4566")
	f.Fuzz(func(t *testing.T, host, bind string) {
		_ = signedGatewayHost(host, bind)
	})
}
