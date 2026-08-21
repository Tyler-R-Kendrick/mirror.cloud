package autoscaling

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

func TestBootedServerAutoScalingCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.autoscaling"}
	cfg.Seed = "asg-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/autoscaling/aws4_request, SignedHeaders=host, Signature=00"
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
	call(url.Values{"Action": {"CreateLaunchConfiguration"}, "Version": {"2011-01-01"}, "LaunchConfigurationName": {"lc1"}, "ImageId": {"ami-1"}, "InstanceType": {"t3.micro"}})
	call(url.Values{"Action": {"CreateAutoScalingGroup"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}, "MinSize": {"1"}, "MaxSize": {"2"}, "DesiredCapacity": {"1"}, "LaunchConfigurationName": {"lc1"}})
	desc := call(url.Values{"Action": {"DescribeAutoScalingGroups"}, "Version": {"2011-01-01"}})
	if !strings.Contains(desc, "g1") {
		t.Fatalf("describe %s", desc)
	}
	pol := call(url.Values{"Action": {"PutScalingPolicy"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}, "PolicyName": {"p1"}, "AdjustmentType": {"ChangeInCapacity"}, "ScalingAdjustment": {"1"}})
	if !strings.Contains(pol, "PolicyARN") {
		t.Fatalf("policy %s", pol)
	}
	call(url.Values{"Action": {"DescribePolicies"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}})
	call(url.Values{"Action": {"PutLifecycleHook"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}, "LifecycleHookName": {"h1"}, "LifecycleTransition": {"autoscaling:EC2_INSTANCE_LAUNCHING"}})
	call(url.Values{"Action": {"DescribeLifecycleHooks"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}})
	ref := call(url.Values{"Action": {"StartInstanceRefresh"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}})
	if !strings.Contains(ref, "InstanceRefreshId") {
		t.Fatalf("refresh %s", ref)
	}
	call(url.Values{"Action": {"DescribeInstanceRefreshes"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}})
	call(url.Values{"Action": {"DescribeAccountLimits"}, "Version": {"2011-01-01"}})
	call(url.Values{"Action": {"ExecutePolicy"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"g1"}, "PolicyName": {"p1"}})
}

func TestAutoScalingHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 66 {
		t.Fatalf("autoscaling Operations() %d want 66", n)
	}
}

func TestBootedServerAutoScalingExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.autoscaling"}
	cfg.Seed = "asg-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/autoscaling/aws4_request, SignedHeaders=host, Signature=00"
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
	hard(url.Values{"Action": {"CreateLaunchConfiguration"}, "Version": {"2011-01-01"}, "LaunchConfigurationName": {"lcboot"}, "ImageId": {"ami-1"}, "InstanceType": {"t3.micro"}})
	hard(url.Values{"Action": {"CreateAutoScalingGroup"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}, "MinSize": {"1"}, "MaxSize": {"2"}, "DesiredCapacity": {"1"}, "LaunchConfigurationName": {"lcboot"}})
	created := hard(url.Values{"Action": {"PutScalingPolicy"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}, "PolicyName": {"pboot"}, "AdjustmentType": {"ChangeInCapacity"}, "ScalingAdjustment": {"1"}})
	if !strings.Contains(created, "PolicyARN") {
		t.Fatalf("put policy %s", created)
	}
	listed := hard(url.Values{"Action": {"DescribePolicies"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}})
	if !strings.Contains(listed, "pboot") {
		t.Fatalf("describe policies %s", listed)
	}
	hard(url.Values{"Action": {"DeletePolicy"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}, "PolicyName": {"pboot"}})
	gone := hard(url.Values{"Action": {"DescribePolicies"}, "Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}})
	if strings.Contains(gone, "<PolicyName>pboot</PolicyName>") {
		t.Fatalf("policy still present %s", gone)
	}
	base := url.Values{
		"Version": {"2011-01-01"}, "AutoScalingGroupName": {"gboot"}, "PolicyName": {"pboot"},
		"LifecycleHookName": {"h1"}, "ScheduledActionName": {"s1"}, "InstanceRefreshId": {"r1"},
		"LaunchConfigurationName": {"lcboot"}, "HealthStatus": {"Healthy"},
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
