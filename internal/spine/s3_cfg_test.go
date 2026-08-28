package spine

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerS3BucketConfigsRoundTrip(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "s3-cfg"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string) (int, []byte) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Authorization", auth)
		if method == http.MethodPut && path == "/cfgb" {
			req.Header.Set("x-amz-bucket-object-lock-enabled", "true")
		}
		if strings.Contains(path, "?attributes") {
			req.Header.Set("x-amz-object-attributes", "ETag,ObjectSize")
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b
	}
	if code, b := do(http.MethodPut, "/cfgb", ""); code >= 300 {
		t.Fatalf("mb %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?encryption", ""); code != 404 || !bytes.Contains(b, []byte("ServerSideEncryptionConfigurationNotFoundError")) && !bytes.Contains(b, []byte("encryption")) {
		t.Fatalf("missing encryption want 404, %d %s", code, b)
	}
	enc := `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`
	if code, b := do(http.MethodPut, "/cfgb?encryption", enc); code >= 300 {
		t.Fatalf("put enc %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?encryption", ""); code != 200 || !bytes.Contains(b, []byte("AES256")) && !bytes.Contains(b, []byte("Document")) && !bytes.Contains(b, []byte("_body")) {
		t.Fatalf("get enc %d %s", code, b)
	}
	pol := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`
	if code, b := do(http.MethodPut, "/cfgb?policy", pol); code >= 300 {
		t.Fatalf("put policy %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?policy", ""); code != 200 || !bytes.Contains(b, []byte("s3:*")) {
		t.Fatalf("get policy %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?policy", ""); code >= 300 && code != 204 {
		t.Fatalf("del policy %d %s", code, b)
	}
	if code, _ := do(http.MethodGet, "/cfgb?policy", ""); code != 404 {
		t.Fatalf("policy after delete %d", code)
	}
	if code, b := do(http.MethodGet, "/cfgb?acl", ""); code != 200 || !bytes.Contains(b, []byte("FULL_CONTROL")) {
		t.Fatalf("default acl %d %s", code, b)
	}
	pab := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>true</IgnorePublicAcls><BlockPublicPolicy>true</BlockPublicPolicy><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	if code, b := do(http.MethodPut, "/cfgb?publicAccessBlock", pab); code >= 300 {
		t.Fatalf("put pab %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?publicAccessBlock", ""); code != 200 {
		t.Fatalf("get pab %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb/k", "body"); code >= 300 {
		t.Fatalf("put obj %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb/k?attributes", ""); code != 200 || !bytes.Contains(b, []byte("ETag")) && !bytes.Contains(b, []byte("ObjectSize")) && !bytes.Contains(b, []byte("etag")) {
		t.Fatalf("attrs %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb?tagging", `<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>`); code >= 300 {
		t.Fatalf("put tagging %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?tagging", ""); code >= 300 && code != 204 {
		t.Fatalf("del tagging %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb/k?legal-hold", `<LegalHold><Status>ON</Status></LegalHold>`); code >= 300 {
		t.Fatalf("legal-hold %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb/k?legal-hold", ""); code != 200 {
		t.Fatalf("get legal-hold %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb/k?retention", `<Retention><Mode>GOVERNANCE</Mode></Retention>`); code >= 300 {
		t.Fatalf("retention %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb?analytics&id=a1", `<AnalyticsConfiguration><Id>a1</Id></AnalyticsConfiguration>`); code >= 300 {
		t.Fatalf("put analytics %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?analytics&id=a1", ""); code != 200 {
		t.Fatalf("get analytics %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?analytics", ""); code != 200 {
		t.Fatalf("list analytics %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?analytics&id=a1", ""); code >= 300 && code != 204 {
		t.Fatalf("del analytics %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb?inventory&id=i1", `<InventoryConfiguration><Id>i1</Id></InventoryConfiguration>`); code >= 300 {
		t.Fatalf("put inv %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?inventory", ""); code != 200 {
		t.Fatalf("list inv %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?inventory&id=i1", ""); code >= 300 && code != 204 {
		t.Fatalf("del inv %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb?metrics&id=m1", `<MetricsConfiguration><Id>m1</Id></MetricsConfiguration>`); code >= 300 {
		t.Fatalf("put metrics %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?metrics", ""); code != 200 {
		t.Fatalf("list metrics %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?metrics&id=m1", ""); code >= 300 && code != 204 {
		t.Fatalf("del metrics %d %s", code, b)
	}
	if code, b := do(http.MethodPut, "/cfgb?intelligent-tiering&id=t1", `<IntelligentTieringConfiguration><Id>t1</Id></IntelligentTieringConfiguration>`); code >= 300 {
		t.Fatalf("put it %d %s", code, b)
	}
	if code, b := do(http.MethodGet, "/cfgb?intelligent-tiering", ""); code != 200 {
		t.Fatalf("list it %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb?intelligent-tiering&id=t1", ""); code >= 300 && code != 204 {
		t.Fatalf("del it %d %s", code, b)
	}
	if code, b := do(http.MethodDelete, "/cfgb/k?tagging", ""); code >= 300 && code != 204 {
		t.Fatalf("del obj tagging %d %s", code, b)
	}
}

func TestS3HTTPProvenOps(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	if n := len(p.Operations()); n != 115 {
		t.Fatalf("s3 Operations() %d want 115", n)
	}
}

func TestBootedServerS3ExtraEngines(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	cfg.Seed = "s3-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string, extra map[string]string) (int, []byte, string) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Authorization", auth)
		for k, v := range extra {
			req.Header.Set(k, v)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		return res.StatusCode, b, res.Header.Get("x-mirror-fidelity")
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x", "", nil); code >= 300 {
		t.Fatalf("mb %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x/old", "hello-s3-rename", nil); code >= 300 {
		t.Fatalf("put old %d %s", code, b)
	}
	if code, b, fid := do(http.MethodPut, "/bucket-x/new", "", map[string]string{"x-amz-rename-source": "old"}); code >= 300 || fid != "emulate" {
		t.Fatalf("rename %d %s fid %s", code, b, fid)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x/new", "", nil); code != 200 || string(b) != "hello-s3-rename" {
		t.Fatalf("renamed get %d %s", code, b)
	}
	if code, _, _ := do(http.MethodGet, "/bucket-x/old", "", nil); code != 404 {
		t.Fatalf("old still there %d", code)
	}
	if code, b, fid := do(http.MethodPost, "/bucket-x?session", "", nil); code >= 300 || fid != "emulate" || !bytes.Contains(b, []byte("AccessKeyId")) {
		t.Fatalf("session %d %s fid %s", code, b, fid)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x?versioning", `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`, nil); code >= 300 {
		t.Fatalf("enable versioning %d %s", code, b)
	}
	lock := `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>`
	if code, b, _ := do(http.MethodPut, "/bucket-x?object-lock", lock, nil); code >= 300 {
		t.Fatalf("put lock %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x?object-lock", "", nil); code != 200 || !bytes.Contains(b, []byte("Enabled")) && !bytes.Contains(b, []byte("_body")) {
		t.Fatalf("get lock %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x?abac", `<AbacStatus>Enabled</AbacStatus>`, nil); code >= 300 {
		t.Fatalf("put abac %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x?abac", "", nil); code != 200 || !bytes.Contains(b, []byte("Enabled")) && !bytes.Contains(b, []byte("_body")) && !bytes.Contains(b, []byte("Abac")) {
		t.Fatalf("get abac %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x?metadataConfiguration", `<JournalTable><Enabled>true</Enabled></JournalTable>`, nil); code >= 300 {
		t.Fatalf("put meta %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x?metadataConfiguration", "", nil); code != 200 {
		t.Fatalf("get meta %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x?metadataTable", `<TableName>t</TableName>`, nil); code >= 300 {
		t.Fatalf("put metatable %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x?metadataTable", "", nil); code != 200 {
		t.Fatalf("get metatable %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x/rows", "alice,1\nbob,2\n", nil); code >= 300 {
		t.Fatalf("put rows %d %s", code, b)
	}
	sel := `Action=SelectObjectContent&Expression=SELECT+%2A+WHERE+s._1+%3D+%27alice%27`
	if code, b, fid := do(http.MethodPost, "/bucket-x/rows?select&select-type=2", "Expression=SELECT * WHERE s._1 = 'alice'", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}); code >= 300 || fid != "emulate" {
		t.Fatalf("select %d %s fid %s", code, b, fid)
	} else if !bytes.Contains(b, []byte("alice")) || bytes.Contains(b, []byte("bob")) {
		_ = sel
		t.Fatalf("select filter %s", b)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x?replication", `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/r</Role><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::bucket-x</Bucket></Destination></Rule></ReplicationConfiguration>`, nil); code >= 300 {
		t.Fatalf("put repl %d %s", code, b)
	}
	if code, b, _ := do(http.MethodDelete, "/bucket-x?replication", "", nil); code >= 300 && code != 204 {
		t.Fatalf("del repl %d %s", code, b)
	}
	if code, _, _ := do(http.MethodGet, "/bucket-x?replication", "", nil); code != 404 {
		t.Fatalf("repl after delete %d", code)
	}
	if code, b, _ := do(http.MethodPut, "/bucket-x/k?annotation", `<Note>hi</Note>`, nil); code >= 300 {
		t.Fatalf("put annot %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x/k?annotation", "", nil); code != 200 {
		t.Fatalf("get annot %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x?annotation", "", nil); code != 200 {
		t.Fatalf("list annot %d %s", code, b)
	}
	if code, b, _ := do(http.MethodGet, "/bucket-x/new?torrent", "", nil); code != 200 || !bytes.Contains(b, []byte("announce")) && !bytes.Contains(b, []byte("Torrent")) {
		t.Fatalf("torrent %d %s", code, b)
	}
	if code, b, fid := do(http.MethodGet, "/?Action=ListDirectoryBuckets", "", nil); code >= 300 || fid != "emulate" {
		t.Fatalf("dir buckets %d %s fid %s", code, b, fid)
	}
	if code, b, _ := do(http.MethodPost, "/?Action=WriteGetObjectResponse", "payload", map[string]string{"x-amz-request-route": "r1", "x-amz-request-token": "t1"}); code >= 300 {
		t.Fatalf("wgor %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/bucket-x/k?Action=UpdateObjectEncryption", "", nil); code >= 300 {
		t.Fatalf("objenc %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/bucket-x?metadataConfiguration&journal", `<Enabled>true</Enabled>`, nil); code >= 300 {
		t.Fatalf("upd journal %d %s", code, b)
	}
	if code, b, _ := do(http.MethodPost, "/bucket-x?metadataConfiguration&inventory", `<Enabled>true</Enabled>`, nil); code >= 300 {
		t.Fatalf("upd inventory %d %s", code, b)
	}
	if code, b, _ := do(http.MethodDelete, "/bucket-x?metadataConfiguration", "", nil); code >= 300 && code != 204 {
		t.Fatalf("del meta %d %s", code, b)
	}
	if code, b, _ := do(http.MethodDelete, "/bucket-x?metadataTable", "", nil); code >= 300 && code != 204 {
		t.Fatalf("del metatable %d %s", code, b)
	}
	if code, b, _ := do(http.MethodDelete, "/bucket-x/k?annotation", "", nil); code >= 300 && code != 204 {
		t.Fatalf("del annot %d %s", code, b)
	}
}
