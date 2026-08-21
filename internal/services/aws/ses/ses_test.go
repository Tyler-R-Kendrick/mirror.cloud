package ses

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestSESHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 8 {
		t.Fatalf("ses Operations() %d want 8", n)
	}
}

func TestBootedServerSESSendAndIdentity(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ses"}
	cfg.Seed = "ses-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ses/aws4_request, SignedHeaders=host, Signature=00"
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
	call(url.Values{"Action": {"VerifyEmailIdentity"}, "Version": {"2010-12-01"}, "EmailAddress": {"a@example.com"}})
	listed := call(url.Values{"Action": {"ListIdentities"}, "Version": {"2010-12-01"}})
	if !strings.Contains(listed, "a@example.com") {
		t.Fatalf("list %s", listed)
	}
	got := call(url.Values{"Action": {"GetIdentityVerificationAttributes"}, "Version": {"2010-12-01"}, "Identities.member.1": {"a@example.com"}})
	if !strings.Contains(got, "Success") && !strings.Contains(got, "a@example.com") {
		t.Fatalf("attrs %s", got)
	}
	sent := call(url.Values{"Action": {"SendEmail"}, "Version": {"2010-12-01"}, "Source": {"a@example.com"}, "Destination.ToAddresses.member.1": {"b@example.com"}, "Message.Subject.Data": {"hi"}, "Message.Body.Text.Data": {"body"}})
	if !strings.Contains(sent, "MessageId") {
		t.Fatalf("send %s", sent)
	}
	call(url.Values{"Action": {"DeleteIdentity"}, "Version": {"2010-12-01"}, "Identity": {"a@example.com"}})
	gone := call(url.Values{"Action": {"ListIdentities"}, "Version": {"2010-12-01"}})
	if strings.Contains(gone, "a@example.com") {
		t.Fatalf("identity still present %s", gone)
	}
}
