package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerSTSSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.sts"}
	cfg.Seed = "sts-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=00"
	call := func(vals url.Values) (int, string) {
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
		return res.StatusCode, string(b)
	}

	_, ident := call(url.Values{"Action": {"GetCallerIdentity"}, "Version": {"2011-06-15"}})
	if !strings.Contains(ident, "Account") || !strings.Contains(ident, "000000000000") {
		t.Fatalf("identity %s", ident)
	}

	_, ar := call(url.Values{
		"Action": {"AssumeRole"}, "Version": {"2011-06-15"},
		"RoleArn": {"arn:aws:iam::000000000000:role/Admin"}, "RoleSessionName": {"sess"},
	})
	if !strings.Contains(ar, "arn:aws:sts::000000000000:assumed-role/Admin/sess") {
		t.Fatalf("assume arn %s", ar)
	}
	if !strings.Contains(ar, "AccessKeyId") || !strings.Contains(ar, "Expiration") {
		t.Fatalf("assume creds %s", ar)
	}

	_, st := call(url.Values{"Action": {"GetSessionToken"}, "Version": {"2011-06-15"}})
	if !strings.Contains(st, "AccessKeyId") || !strings.Contains(st, "SessionToken") || !strings.Contains(st, "Expiration") {
		t.Fatalf("session %s", st)
	}

	_, fed := call(url.Values{"Action": {"GetFederationToken"}, "Version": {"2011-06-15"}, "Name": {"bob"}})
	if !strings.Contains(fed, "arn:aws:sts::000000000000:federated-user/bob") {
		t.Fatalf("federation %s", fed)
	}
	if !strings.Contains(fed, "AccessKeyId") {
		t.Fatalf("fed creds %s", fed)
	}
	_, saml := call(url.Values{
		"Action": {"AssumeRoleWithSAML"}, "Version": {"2011-06-15"},
		"RoleArn": {"arn:aws:iam::000000000000:role/Admin"}, "SAMLAssertion": {"PHNhbWw+"},
	})
	if !strings.Contains(saml, "assumed-role/Admin/saml") || !strings.Contains(saml, "AccessKeyId") {
		t.Fatalf("saml %s", saml)
	}
	_, web := call(url.Values{
		"Action": {"AssumeRoleWithWebIdentity"}, "Version": {"2011-06-15"},
		"RoleArn": {"arn:aws:iam::000000000000:role/Admin"}, "WebIdentityToken": {"header.payload.sig"},
	})
	if !strings.Contains(web, "assumed-role/Admin/web") {
		t.Fatalf("web %s", web)
	}
	_, root := call(url.Values{"Action": {"AssumeRoot"}, "Version": {"2011-06-15"}})
	if !strings.Contains(root, "AccessKeyId") {
		t.Fatalf("root %s", root)
	}
	_, info := call(url.Values{"Action": {"GetAccessKeyInfo"}, "Version": {"2011-06-15"}, "AccessKeyId": {"AKIATEST"}})
	if !strings.Contains(info, "000000000000") {
		t.Fatalf("key info %s", info)
	}
	enc := "7b22616374696f6e223a2273333a4765744f626a656374227d" // {"action":"s3:GetObject"}
	_, dec := call(url.Values{"Action": {"DecodeAuthorizationMessage"}, "Version": {"2011-06-15"}, "EncodedMessage": {enc}})
	if !strings.Contains(dec, "s3:GetObject") {
		t.Fatalf("decode %s", dec)
	}
	_, wit := call(url.Values{"Action": {"GetWebIdentityToken"}, "Version": {"2011-06-15"}})
	if !strings.Contains(wit, "WebIdentityToken") {
		t.Fatalf("wit %s", wit)
	}
	_, del := call(url.Values{"Action": {"GetDelegatedAccessToken"}, "Version": {"2011-06-15"}})
	if !strings.Contains(del, "SessionToken") && !strings.Contains(del, "AccessKeyId") {
		t.Fatalf("delegated %s", del)
	}
}

func TestSTSHTTPProvenOps(t *testing.T) {
	want := []string{
		"GetCallerIdentity", "AssumeRole", "GetSessionToken", "GetFederationToken",
		"AssumeRoleWithSAML", "AssumeRoleWithWebIdentity", "AssumeRoot",
		"DecodeAuthorizationMessage", "GetAccessKeyInfo", "GetDelegatedAccessToken", "GetWebIdentityToken",
	}
	assertSame(t, "sts", sts.New(spitest.Deps(t)).Operations(), want)
}
