package docdb

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

func TestDocDBHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 14 {
		t.Fatalf("docdb Operations() %d want 14", n)
	}
}

func TestBootedServerDocDBClusterInstance(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.docdb"}
	cfg.Seed = "docdb-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/docdb/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}, "Engine": {"docdb"}, "MasterUsername": {"root"}})
	if !strings.Contains(created, "c1") || !strings.Contains(created, "available") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeDBClusters"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	if !strings.Contains(desc, "c1") || !strings.Contains(desc, "docdb.amazonaws.com") {
		t.Fatalf("describe %s", desc)
	}
	inst := call(url.Values{"Action": {"CreateDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"i1"}, "DBClusterIdentifier": {"c1"}, "DBInstanceClass": {"db.t3.medium"}})
	if !strings.Contains(inst, "i1") {
		t.Fatalf("instance %s", inst)
	}
	call(url.Values{"Action": {"DeleteDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"i1"}})
	call(url.Values{"Action": {"DeleteDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	gone := call(url.Values{"Action": {"DescribeDBClusters"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	if strings.Contains(gone, "<DBClusterIdentifier>c1</DBClusterIdentifier>") && strings.Contains(gone, "available") {
		t.Fatalf("cluster still present %s", gone)
	}
}
