package redshift

import (
	"context"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerRedshiftCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.redshift"}
	cfg.Seed = "rs-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/redshift/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateCluster"}, "Version": {"2012-12-01"}, "ClusterIdentifier": {"rs1"}, "NodeType": {"dc2.large"}, "MasterUsername": {"awsuser"}})
	if !strings.Contains(created, "rs1") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeClusters"}, "Version": {"2012-12-01"}, "ClusterIdentifier": {"rs1"}})
	if !strings.Contains(desc, "available") {
		t.Fatalf("describe %s", desc)
	}
	v := "2012-12-01"
	call(url.Values{"Action": {"DeleteCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{
		"Action": {"CreateHsmConfiguration"}, "Version": {v}, "HsmConfigurationIdentifier": {"h1"},
		"Description": {"d"}, "HsmIpAddress": {"10.0.0.1"}, "HsmPartitionName": {"p"},
		"HsmPartitionPassword": {"pw"}, "HsmServerPublicCertificate": {"cert"},
	}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("CreateHsmConfiguration %d %s %s", res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
	}
	listed := call(url.Values{"Action": {"DescribeHsmConfigurations"}, "Version": {"2012-12-01"}, "HsmConfigurationIdentifier": {"h1"}})
	if !strings.Contains(listed, "h1") {
		t.Fatalf("describe hsm %s", listed)
	}
	call(url.Values{"Action": {"DeleteHsmConfiguration"}, "Version": {"2012-12-01"}, "HsmConfigurationIdentifier": {"h1"}})
	gone := call(url.Values{"Action": {"DescribeHsmConfigurations"}, "Version": {"2012-12-01"}, "HsmConfigurationIdentifier": {"h1"}})
	if strings.Contains(gone, "<HsmConfigurationIdentifier>h1</HsmConfigurationIdentifier>") {
		t.Fatalf("hsm still present %s", gone)
	}
}

func TestRedshiftCopyDataPlane(t *testing.T) {
	deps := spitest.Deps(t)
	p, clusters := New(deps), bundled.Handler("aws.redshift", deps)
	ctx := context.Background()
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	req := &spi.Request{Identity: identity, Operation: "CreateCluster", Input: map[string]any{
		"NodeType": "dc2.large", "ClusterIdentifier": "warehouse", "DBName": "analytics", "MasterUsername": "firehose", "MasterUserPassword": "secret-password",
	}}
	response, err := clusters.Invoke(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	cluster := response.Output["Cluster"].(map[string]any)
	if cluster["MasterUserPassword"] != nil || cluster["DBName"] != "analytics" {
		t.Fatalf("Redshift cluster description %#v", cluster)
	}
	if err := p.CreateTable(ctx, identity, "warehouse", "analytics", "events", []string{"id", "payload"}); err != nil {
		t.Fatal(err)
	}
	input := CopyInput{
		Cluster: "warehouse", Database: "analytics", Table: "events", Username: "firehose", Password: "secret-password",
		Columns: "id,payload", Options: "delimiter '|'", Data: [][]byte{[]byte("1|one\n"), []byte("2|two\n")},
	}
	if err := p.Copy(ctx, identity, input); err != nil {
		t.Fatal(err)
	}
	input.Options = "JSON 'auto'"
	input.Data = [][]byte{[]byte(`{"id":3,"payload":"three"}` + "\n")}
	if err := p.Copy(ctx, identity, input); err != nil {
		t.Fatal(err)
	}
	rows, err := p.TableRows(ctx, identity, "warehouse", "analytics", "events")
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"id": "1", "payload": "one"}, {"id": "2", "payload": "two"}, {"id": float64(3), "payload": "three"}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("Redshift COPY rows %#v", rows)
	}
	for name, mutate := range map[string]func(*CopyInput){
		"credentials": func(input *CopyInput) { input.Password = "wrong-password" },
		"database":    func(input *CopyInput) { input.Database = "missing" },
		"table":       func(input *CopyInput) { input.Table = "missing" },
		"columns":     func(input *CopyInput) { input.Columns = "missing" },
		"row": func(input *CopyInput) {
			input.Options, input.Data = "delimiter '|'", [][]byte{[]byte("only-one-column\n")}
		},
		"json": func(input *CopyInput) {
			input.Options, input.Data = "JSON 'auto'", [][]byte{[]byte("not-json\n")}
		},
		"delimiter": func(input *CopyInput) {
			input.Options, input.Data = "delimiter '||'", [][]byte{[]byte("1||one\n")}
		},
	} {
		candidate := input
		mutate(&candidate)
		err := p.Copy(ctx, identity, candidate)
		expected := map[string]string{
			"credentials": "credentials are invalid", "database": "database does not exist", "table": "table not found", "columns": "column does not exist",
			"row": "wrong column count", "json": "JSON row is invalid", "delimiter": "delimiter must be one byte",
		}[name]
		if err == nil || !strings.Contains(err.Error(), expected) {
			t.Errorf("invalid Redshift COPY %s returned %v", name, err)
		}
	}
	// The bundle's DeleteCluster takes the cluster's COPY tables with it.
	if _, err := clusters.Invoke(ctx, &spi.Request{Identity: identity, Operation: "DeleteCluster", Input: map[string]any{"ClusterIdentifier": "warehouse"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := p.TableRows(ctx, identity, "warehouse", "analytics", "events"); err == nil {
		t.Fatal("deleted Redshift cluster retained table data")
	}
}
