package elasticache

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

func TestBootedServerElastiCacheCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.elasticache"}
	cfg.Seed = "ec-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/elasticache/aws4_request, SignedHeaders=host, Signature=00"
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
		return string(b)
	}
	created := call(url.Values{"Action": {"CreateCacheCluster"}, "Version": {"2015-02-02"}, "CacheClusterId": {"c1"}, "Engine": {"redis"}})
	if !strings.Contains(created, "c1") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeCacheClusters"}, "Version": {"2015-02-02"}, "CacheClusterId": {"c1"}})
	if !strings.Contains(desc, "available") {
		t.Fatalf("describe %s", desc)
	}
	user := call(url.Values{"Action": {"CreateUser"}, "Version": {"2015-02-02"}, "UserId": {"u1"}, "UserName": {"u1"}, "Engine": {"redis"}, "AccessString": {"on ~* +@all"}})
	if !strings.Contains(user, "u1") {
		t.Fatalf("user %s", user)
	}
	call(url.Values{"Action": {"DescribeUsers"}, "Version": {"2015-02-02"}, "UserId": {"u1"}})
	call(url.Values{"Action": {"CreateUserGroup"}, "Version": {"2015-02-02"}, "UserGroupId": {"g1"}, "Engine": {"redis"}})
	call(url.Values{"Action": {"CreateServerlessCache"}, "Version": {"2015-02-02"}, "ServerlessCacheName": {"s1"}, "Engine": {"redis"}})
	call(url.Values{"Action": {"DescribeServerlessCaches"}, "Version": {"2015-02-02"}})
	call(url.Values{"Action": {"CreateCacheParameterGroup"}, "Version": {"2015-02-02"}, "CacheParameterGroupName": {"pg1"}, "CacheParameterGroupFamily": {"redis7"}})
	call(url.Values{"Action": {"DescribeCacheEngineVersions"}, "Version": {"2015-02-02"}})
	call(url.Values{"Action": {"IncreaseReplicaCount"}, "Version": {"2015-02-02"}, "ReplicationGroupId": {"r1"}, "NewReplicaCount": {"2"}})
}

func TestElastiCacheHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 75 {
		t.Fatalf("elasticache Operations() %d want 75", n)
	}
}

func TestBootedServerElastiCacheExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.elasticache"}
	cfg.Seed = "ec-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/elasticache/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(vals url.Values) string {
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", vals.Get("Action"), res.Header.Get("x-mirror-fidelity"), b)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", vals.Get("Action"), res.StatusCode, b)
		}
		return string(b)
	}
	hard := func(vals url.Values) string {
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
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", vals.Get("Action"), res.StatusCode, res.Header.Get("x-mirror-fidelity"), b)
		}
		return string(b)
	}
	created := hard(url.Values{"Action": {"CreateUser"}, "Version": {"2015-02-02"}, "UserId": {"uboot"}, "UserName": {"uboot"}, "Engine": {"redis"}, "AccessString": {"on ~* +@all"}})
	if !strings.Contains(created, "uboot") {
		t.Fatalf("create user %s", created)
	}
	got := hard(url.Values{"Action": {"DescribeUsers"}, "Version": {"2015-02-02"}, "UserId": {"uboot"}})
	if !strings.Contains(got, "uboot") {
		t.Fatalf("describe users %s", got)
	}
	hard(url.Values{"Action": {"DeleteUser"}, "Version": {"2015-02-02"}, "UserId": {"uboot"}})
	gone := hard(url.Values{"Action": {"DescribeUsers"}, "Version": {"2015-02-02"}})
	if strings.Contains(gone, "uboot") {
		t.Fatalf("user still present %s", gone)
	}
	base := url.Values{
		"Version": {"2015-02-02"}, "UserId": {"uboot"}, "UserGroupId": {"g1"},
		"ServerlessCacheName": {"s1"}, "ServerlessCacheSnapshotName": {"ss1"},
		"GlobalReplicationGroupId": {"grg1"}, "CacheParameterGroupName": {"pg1"},
		"CacheSecurityGroupName": {"sg1"}, "ReplicationGroupId": {"r1"},
		"CacheClusterId": {"c1"}, "CacheSubnetGroupName": {"sn1"},
		"ServiceUpdateName": {"su1"}, "SourceSnapshotName": {"src"}, "TargetSnapshotName": {"dst"},
	}
	for _, op := range extraOps() {
		vals := url.Values{}
		for k, v := range base {
			vals[k] = v
		}
		vals.Set("Action", op)
		soft(vals)
	}
}
