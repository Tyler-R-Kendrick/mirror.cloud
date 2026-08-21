package rds

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

func TestBootedServerRDSCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.rds"}
	cfg.Seed = "rds-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/rds/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}, "Engine": {"mysql"}, "DBInstanceClass": {"db.t3.micro"}})
	if !strings.Contains(created, "db1") || !strings.Contains(created, "available") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeDBInstances"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	if !strings.Contains(desc, "db1") || !strings.Contains(desc, "rds.amazonaws.com") {
		t.Fatalf("describe %s", desc)
	}
	call(url.Values{"Action": {"CreateDBSnapshot"}, "Version": {"2014-10-31"}, "DBSnapshotIdentifier": {"snap1"}, "DBInstanceIdentifier": {"db1"}})
	snaps := call(url.Values{"Action": {"DescribeDBSnapshots"}, "Version": {"2014-10-31"}})
	if !strings.Contains(snaps, "snap1") {
		t.Fatalf("snaps %s", snaps)
	}
	call(url.Values{"Action": {"CopyDBSnapshot"}, "Version": {"2014-10-31"}, "SourceDBSnapshotIdentifier": {"snap1"}, "TargetDBSnapshotIdentifier": {"snap2"}})
	call(url.Values{"Action": {"StopDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	stopped := call(url.Values{"Action": {"DescribeDBInstances"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	if !strings.Contains(stopped, "stopped") {
		t.Fatalf("stop %s", stopped)
	}
	call(url.Values{"Action": {"StartDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	call(url.Values{"Action": {"RebootDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	call(url.Values{"Action": {"ModifyDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}, "DBInstanceClass": {"db.t3.small"}})
	call(url.Values{"Action": {"CreateDBInstanceReadReplica"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1-rr"}, "SourceDBInstanceIdentifier": {"db1"}})
	call(url.Values{"Action": {"PromoteReadReplica"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1-rr"}})
	call(url.Values{"Action": {"RestoreDBInstanceFromDBSnapshot"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db-restored"}, "DBSnapshotIdentifier": {"snap1"}})
	call(url.Values{"Action": {"CreateDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}, "Engine": {"aurora-mysql"}})
	call(url.Values{"Action": {"DescribeDBClusters"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	call(url.Values{"Action": {"ModifyDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}, "BackupRetentionPeriod": {"7"}})
	call(url.Values{"Action": {"FailoverDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	call(url.Values{"Action": {"CreateDBClusterSnapshot"}, "Version": {"2014-10-31"}, "DBClusterSnapshotIdentifier": {"csnap1"}, "DBClusterIdentifier": {"c1"}})
	call(url.Values{"Action": {"DescribeDBClusterSnapshots"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"RestoreDBClusterFromSnapshot"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c-restored"}, "SnapshotIdentifier": {"csnap1"}, "Engine": {"aurora-mysql"}})
	call(url.Values{"Action": {"DeleteDBClusterSnapshot"}, "Version": {"2014-10-31"}, "DBClusterSnapshotIdentifier": {"csnap1"}})
	call(url.Values{"Action": {"CreateDBSubnetGroup"}, "Version": {"2014-10-31"}, "DBSubnetGroupName": {"sg1"}, "DBSubnetGroupDescription": {"d"}})
	call(url.Values{"Action": {"DescribeDBSubnetGroups"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"DeleteDBSubnetGroup"}, "Version": {"2014-10-31"}, "DBSubnetGroupName": {"sg1"}})
	call(url.Values{"Action": {"CreateDBParameterGroup"}, "Version": {"2014-10-31"}, "DBParameterGroupName": {"pg1"}, "DBParameterGroupFamily": {"mysql8.0"}})
	call(url.Values{"Action": {"DescribeDBParameterGroups"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"ModifyDBParameterGroup"}, "Version": {"2014-10-31"}, "DBParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"DescribeDBParameters"}, "Version": {"2014-10-31"}, "DBParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"ResetDBParameterGroup"}, "Version": {"2014-10-31"}, "DBParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"DeleteDBParameterGroup"}, "Version": {"2014-10-31"}, "DBParameterGroupName": {"pg1"}})
	call(url.Values{"Action": {"CreateDBClusterParameterGroup"}, "Version": {"2014-10-31"}, "DBClusterParameterGroupName": {"cpg1"}, "DBParameterGroupFamily": {"aurora-mysql8.0"}})
	call(url.Values{"Action": {"DescribeDBClusterParameterGroups"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"DeleteDBClusterParameterGroup"}, "Version": {"2014-10-31"}, "DBClusterParameterGroupName": {"cpg1"}})
	call(url.Values{"Action": {"CreateOptionGroup"}, "Version": {"2014-10-31"}, "OptionGroupName": {"og1"}, "EngineName": {"mysql"}, "MajorEngineVersion": {"8.0"}})
	call(url.Values{"Action": {"DescribeOptionGroups"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"DeleteOptionGroup"}, "Version": {"2014-10-31"}, "OptionGroupName": {"og1"}})
	call(url.Values{"Action": {"AddRoleToDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}, "RoleArn": {"arn:aws:iam::000000000000:role/rds"}})
	call(url.Values{"Action": {"RemoveRoleFromDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}, "RoleArn": {"arn:aws:iam::000000000000:role/rds"}})
	call(url.Values{"Action": {"CreateEventSubscription"}, "Version": {"2014-10-31"}, "SubscriptionName": {"ev1"}, "SnsTopicArn": {"arn:aws:sns:us-east-1:000000000000:t"}})
	call(url.Values{"Action": {"DescribeEventSubscriptions"}, "Version": {"2014-10-31"}})
	call(url.Values{"Action": {"DeleteEventSubscription"}, "Version": {"2014-10-31"}, "SubscriptionName": {"ev1"}})
	call(url.Values{"Action": {"AddTagsToResource"}, "Version": {"2014-10-31"}, "ResourceName": {"arn:aws:rds:us-east-1:000000000000:db:db1"}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}})
	call(url.Values{"Action": {"ListTagsForResource"}, "Version": {"2014-10-31"}, "ResourceName": {"arn:aws:rds:us-east-1:000000000000:db:db1"}})
	call(url.Values{"Action": {"RemoveTagsFromResource"}, "Version": {"2014-10-31"}, "ResourceName": {"arn:aws:rds:us-east-1:000000000000:db:db1"}})
	call(url.Values{"Action": {"DeleteDBSnapshot"}, "Version": {"2014-10-31"}, "DBSnapshotIdentifier": {"snap1"}})
	call(url.Values{"Action": {"DeleteDBCluster"}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"c1"}})
	call(url.Values{"Action": {"DeleteDBInstance"}, "Version": {"2014-10-31"}, "DBInstanceIdentifier": {"db1"}})
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(url.Values{
		"Action": {"CreateDBProxy"}, "Version": {"2014-10-31"}, "DBProxyName": {"p1"},
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
		t.Fatalf("CreateDBProxy %d %s %s", res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
	}
	listed := call(url.Values{"Action": {"DescribeDBProxies"}, "Version": {"2014-10-31"}, "DBProxyName": {"p1"}})
	if !strings.Contains(listed, "p1") {
		t.Fatalf("describe proxy %s", listed)
	}
	call(url.Values{"Action": {"DeleteDBProxy"}, "Version": {"2014-10-31"}, "DBProxyName": {"p1"}})
	gone := call(url.Values{"Action": {"DescribeDBProxies"}, "Version": {"2014-10-31"}, "DBProxyName": {"p1"}})
	if strings.Contains(gone, "p1") && strings.Contains(gone, "DBProxyName") {
		if strings.Count(gone, "<DBProxyName>p1</DBProxyName>") > 0 {
			t.Fatalf("proxy still present %s", gone)
		}
	}
	for _, op := range extraOps() {
		body := call(url.Values{
			"Action": {op}, "Version": {"2014-10-31"},
			"DBProxyName": {"p1"}, "DBClusterIdentifier": {"c1"}, "DBInstanceIdentifier": {"db1"},
			"GlobalClusterIdentifier": {"g1"}, "DBSecurityGroupName": {"sg"},
		})
		if body == "" {
			t.Fatalf("%s empty", op)
		}
	}
}

func TestRDSHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 46+len(extraOps()) {
		t.Fatalf("rds Operations() %d want %d", n, 46+len(extraOps()))
	}
}
