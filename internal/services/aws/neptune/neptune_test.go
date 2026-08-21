package neptune

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

func TestNeptuneHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 14 {
		t.Fatalf("neptune Operations() %d want 14", n)
	}
}

func TestBootedServerNeptuneCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.neptune"}
	cfg.Seed = "np-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/neptune/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"n1"}, "Engine": {"neptune"}})
	if !strings.Contains(created, "n1") || !strings.Contains(created, "available") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeDBClusters"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"n1"}})
	if !strings.Contains(desc, "n1") || !strings.Contains(desc, "neptune.amazonaws.com") {
		t.Fatalf("describe %s", desc)
	}
	inst := call(url.Values{"Action": {"CreateDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"ni1"}, "DBClusterIdentifier": {"n1"}, "DBInstanceClass": {"db.r5.large"}})
	if !strings.Contains(inst, "ni1") {
		t.Fatalf("instance %s", inst)
	}
	call(url.Values{"Action": {"DeleteDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"ni1"}})
	call(url.Values{"Action": {"DeleteDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"n1"}})
	gone := call(url.Values{"Action": {"DescribeDBClusters"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"n1"}})
	if strings.Contains(gone, "<DBClusterIdentifier>n1</DBClusterIdentifier>") {
		t.Fatalf("cluster still present %s", gone)
	}
}
