package ec2

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

func TestEC2HTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 12 {
		t.Fatalf("ec2 Operations() %d want 12", n)
	}
}

func TestBootedServerEC2ControlPlane(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ec2"}
	cfg.Seed = "ec2-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ec2/aws4_request, SignedHeaders=host, Signature=00"
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
	vpc := call(url.Values{"Action": {"CreateVpc"}, "Version": {"2016-11-15"}, "CidrBlock": {"10.0.0.0/16"}})
	if !strings.Contains(vpc, "vpc-") {
		t.Fatalf("vpc %s", vpc)
	}
	vid := xmlTag(vpc, "VpcId")
	if vid == "" {
		vid = xmlTag(vpc, "vpcId")
	}
	if !strings.Contains(call(url.Values{"Action": {"DescribeVpcs"}, "Version": {"2016-11-15"}}), vid) {
		t.Fatalf("describe vpc missing %s", vid)
	}
	sub := call(url.Values{"Action": {"CreateSubnet"}, "Version": {"2016-11-15"}, "VpcId": {vid}, "CidrBlock": {"10.0.1.0/24"}})
	sid := xmlTag(sub, "SubnetId")
	if sid == "" {
		sid = xmlTag(sub, "subnetId")
	}
	if !strings.Contains(call(url.Values{"Action": {"DescribeSubnets"}, "Version": {"2016-11-15"}}), sid) {
		t.Fatalf("describe subnet missing %s in %s", sid, sub)
	}
	sg := call(url.Values{"Action": {"CreateSecurityGroup"}, "Version": {"2016-11-15"}, "GroupName": {"web"}, "GroupDescription": {"t"}, "VpcId": {vid}})
	gid := xmlTag(sg, "GroupId")
	if gid == "" {
		gid = xmlTag(sg, "groupId")
	}
	if !strings.Contains(call(url.Values{"Action": {"DescribeSecurityGroups"}, "Version": {"2016-11-15"}}), gid) {
		t.Fatalf("describe sg missing %s", gid)
	}
	run := call(url.Values{"Action": {"RunInstances"}, "Version": {"2016-11-15"}, "ImageId": {"ami-1"}, "MinCount": {"1"}, "MaxCount": {"1"}, "SubnetId": {sid}})
	iid := xmlTag(run, "InstanceId")
	if iid == "" {
		t.Fatalf("run %s", run)
	}
	desc := call(url.Values{"Action": {"DescribeInstances"}, "Version": {"2016-11-15"}})
	if !strings.Contains(desc, iid) {
		t.Fatalf("describe instances missing %s %s", iid, desc)
	}
	call(url.Values{"Action": {"TerminateInstances"}, "Version": {"2016-11-15"}, "InstanceId.1": {iid}})
	after := call(url.Values{"Action": {"DescribeInstances"}, "Version": {"2016-11-15"}})
	if strings.Contains(after, iid) {
		t.Fatalf("terminated still present %s", after)
	}
	call(url.Values{"Action": {"DeleteSecurityGroup"}, "Version": {"2016-11-15"}, "GroupId": {gid}})
	call(url.Values{"Action": {"DeleteSubnet"}, "Version": {"2016-11-15"}, "SubnetId": {sid}})
	call(url.Values{"Action": {"DeleteVpc"}, "Version": {"2016-11-15"}, "VpcId": {vid}})
	gone := call(url.Values{"Action": {"DescribeVpcs"}, "Version": {"2016-11-15"}})
	if strings.Contains(gone, vid) {
		t.Fatalf("vpc still present %s", gone)
	}
}

func xmlTag(s, tag string) string {
	open := "<" + tag + ">"
	i := strings.Index(s, open)
	if i < 0 {
		return ""
	}
	rest := s[i+len(open):]
	j := strings.Index(rest, "<")
	if j < 0 {
		return ""
	}
	return rest[:j]
}
