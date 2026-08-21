package elasticbeanstalk

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

func TestElasticBeanstalkHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 11 {
		t.Fatalf("elasticbeanstalk Operations() %d want 11", n)
	}
}

func TestBootedServerElasticBeanstalkCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.elasticbeanstalk"}
	cfg.Seed = "eb-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/elasticbeanstalk/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateApplication"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}})
	if !strings.Contains(created, "app1") {
		t.Fatalf("create %s", created)
	}
	got := call(url.Values{"Action": {"DescribeApplications"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}})
	if !strings.Contains(got, "app1") {
		t.Fatalf("describe %s", got)
	}
	call(url.Values{"Action": {"CreateApplicationVersion"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}, "VersionLabel": {"v1"}})
	env := call(url.Values{"Action": {"CreateEnvironment"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}, "EnvironmentName": {"e1"}, "VersionLabel": {"v1"}})
	if !strings.Contains(env, "e1") {
		t.Fatalf("env %s", env)
	}
	call(url.Values{"Action": {"TerminateEnvironment"}, "Version": {"2010-12-01"}, "EnvironmentName": {"e1"}})
	call(url.Values{"Action": {"DeleteApplication"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}})
	gone := call(url.Values{"Action": {"DescribeApplications"}, "Version": {"2010-12-01"}, "ApplicationName": {"app1"}})
	if strings.Contains(gone, "<ApplicationName>app1</ApplicationName>") {
		t.Fatalf("still present %s", gone)
	}
}
