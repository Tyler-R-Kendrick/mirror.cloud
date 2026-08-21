package iam_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
)

func TestBootedServerIAMExtraMFA(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.iam"}
	cfg.Seed = "iam-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		return string(b)
	}
	created := call(url.Values{"Action": {"CreateVirtualMFADevice"}, "Version": {"2010-05-08"}, "VirtualMFADeviceName": {"m1"}, "SerialNumber": {"arn:aws:iam::000000000000:mfa/m1"}})
	if !strings.Contains(created, "m1") && !strings.Contains(created, "SerialNumber") {
		t.Fatalf("create mfa %s", created)
	}
	listed := call(url.Values{"Action": {"ListVirtualMFADevices"}, "Version": {"2010-05-08"}})
	if !strings.Contains(listed, "m1") && !strings.Contains(listed, "arn:aws:iam::000000000000:mfa/m1") {
		t.Fatalf("list mfa %s", listed)
	}
	call(url.Values{"Action": {"DeleteVirtualMFADevice"}, "Version": {"2010-05-08"}, "SerialNumber": {"arn:aws:iam::000000000000:mfa/m1"}})
	gone := call(url.Values{"Action": {"ListVirtualMFADevices"}, "Version": {"2010-05-08"}})
	if strings.Contains(gone, "arn:aws:iam::000000000000:mfa/m1") {
		t.Fatalf("mfa still present %s", gone)
	}
	for _, op := range iam.ExtraOps() {
		vals := url.Values{
			"Action": {op}, "Version": {"2010-05-08"},
			"UserName": {"u"}, "RoleName": {"r"}, "SerialNumber": {"arn:aws:iam::000000000000:mfa/m1"},
			"VirtualMFADeviceName": {"m1"}, "ServerCertificateName": {"cert"}, "PolicyArn": {"arn:aws:iam::aws:policy/x"},
			"SSHPublicKeyId": {"pk"}, "CertificateId": {"c1"}, "AWSServiceName": {"autoscaling.amazonaws.com"},
		}
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), b)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, b)
		}
	}
}
