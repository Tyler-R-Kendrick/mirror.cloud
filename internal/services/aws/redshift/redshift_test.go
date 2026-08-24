package redshift

import (
	"context"
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
	call(url.Values{"Action": {"ModifyCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}, "NodeType": {"dc2.large"}})
	call(url.Values{"Action": {"RebootCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"PauseCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"ResumeCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"ResizeCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}, "NodeType": {"dc2.8xlarge"}})
	call(url.Values{"Action": {"CreateClusterSnapshot"}, "Version": {v}, "SnapshotIdentifier": {"snap1"}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"DescribeClusterSnapshots"}, "Version": {v}})
	call(url.Values{"Action": {"CopyClusterSnapshot"}, "Version": {v}, "SourceSnapshotIdentifier": {"snap1"}, "TargetSnapshotIdentifier": {"snap2"}})
	call(url.Values{"Action": {"RestoreFromClusterSnapshot"}, "Version": {v}, "ClusterIdentifier": {"rs2"}, "SnapshotIdentifier": {"snap1"}})
	call(url.Values{"Action": {"DeleteClusterSnapshot"}, "Version": {v}, "SnapshotIdentifier": {"snap1"}})
	call(url.Values{"Action": {"CreateClusterSubnetGroup"}, "Version": {v}, "ClusterSubnetGroupName": {"sg1"}})
	call(url.Values{"Action": {"DescribeClusterSubnetGroups"}, "Version": {v}})
	call(url.Values{"Action": {"ModifyClusterSubnetGroup"}, "Version": {v}, "ClusterSubnetGroupName": {"sg1"}})
	call(url.Values{"Action": {"DeleteClusterSubnetGroup"}, "Version": {v}, "ClusterSubnetGroupName": {"sg1"}})
	call(url.Values{"Action": {"CreateClusterParameterGroup"}, "Version": {v}, "ParameterGroupName": {"pg1"}, "ParameterGroupFamily": {"redshift-1.0"}})
	call(url.Values{"Action": {"DescribeClusterParameterGroups"}, "Version": {v}})
	call(url.Values{"Action": {"ModifyClusterParameterGroup"}, "Version": {v}, "ParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"DescribeClusterParameters"}, "Version": {v}, "ParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"ResetClusterParameterGroup"}, "Version": {v}, "ParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"DeleteClusterParameterGroup"}, "Version": {v}, "ParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"EnableSnapshotCopy"}, "Version": {v}, "ClusterIdentifier": {"rs1"}, "DestinationRegion": {"us-west-2"}})
	call(url.Values{"Action": {"DisableSnapshotCopy"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"CreateSnapshotCopyGrant"}, "Version": {v}, "SnapshotCopyGrantName": {"g1"}})
	call(url.Values{"Action": {"DescribeSnapshotCopyGrants"}, "Version": {v}})
	call(url.Values{"Action": {"DeleteSnapshotCopyGrant"}, "Version": {v}, "SnapshotCopyGrantName": {"g1"}})
	call(url.Values{"Action": {"CreateEventSubscription"}, "Version": {v}, "SubscriptionName": {"ev1"}, "SnsTopicArn": {"arn:aws:sns:us-east-1:000000000000:t"}})
	call(url.Values{"Action": {"DescribeEventSubscriptions"}, "Version": {v}})
	call(url.Values{"Action": {"DeleteEventSubscription"}, "Version": {v}, "SubscriptionName": {"ev1"}})
	call(url.Values{"Action": {"GetClusterCredentials"}, "Version": {v}, "ClusterIdentifier": {"rs1"}, "DbUser": {"awsuser"}})
	call(url.Values{"Action": {"ModifyClusterIamRoles"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	call(url.Values{"Action": {"CreateTags"}, "Version": {v}, "ResourceName": {"arn:aws:redshift:us-east-1:000000000000:cluster:rs1"}})
	call(url.Values{"Action": {"DescribeTags"}, "Version": {v}, "ResourceName": {"arn:aws:redshift:us-east-1:000000000000:cluster:rs1"}})
	call(url.Values{"Action": {"DeleteTags"}, "Version": {v}, "ResourceName": {"arn:aws:redshift:us-east-1:000000000000:cluster:rs1"}})
	call(url.Values{"Action": {"DeleteCluster"}, "Version": {v}, "ClusterIdentifier": {"rs1"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{
		"Action": {"CreateHsmConfiguration"}, "Version": {v}, "HsmConfigurationIdentifier": {"h1"},
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
	for _, op := range extraOps() {
		_ = call(url.Values{"Action": {op}, "Version": {"2012-12-01"}, "HsmConfigurationIdentifier": {"h1"}, "ClusterIdentifier": {"c1"}, "SnapshotIdentifier": {"s1"}})
	}
}

func TestRedshiftHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 36+len(extraOps()) {
		t.Fatalf("redshift Operations() %d want %d", n, 36+len(extraOps()))
	}
}

func TestRedshiftCopyDataPlane(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	identity := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	req := &spi.Request{Identity: identity, Operation: "CreateCluster", Input: map[string]any{
		"ClusterIdentifier": "warehouse", "DBName": "analytics", "MasterUsername": "firehose", "MasterUserPassword": "secret-password",
	}}
	response, err := p.Invoke(ctx, req)
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
	} {
		candidate := input
		mutate(&candidate)
		if err := p.Copy(ctx, identity, candidate); err == nil {
			t.Errorf("accepted invalid Redshift COPY %s", name)
		}
	}
}
