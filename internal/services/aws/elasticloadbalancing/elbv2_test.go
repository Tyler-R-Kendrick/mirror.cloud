package elasticloadbalancing

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

func TestBootedServerELBCreateDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.elasticloadbalancing"}
	cfg.Seed = "elb-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/elasticloadbalancing/aws4_request, SignedHeaders=host, Signature=00"
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
	created := call(url.Values{"Action": {"CreateLoadBalancer"}, "Version": {"2015-12-01"}, "Name": {"alb1"}, "Type": {"application"}})
	if !strings.Contains(created, "alb1") || !strings.Contains(created, "LoadBalancerArn") {
		t.Fatalf("create %s", created)
	}
	desc := call(url.Values{"Action": {"DescribeLoadBalancers"}, "Version": {"2015-12-01"}})
	if !strings.Contains(desc, "alb1") {
		t.Fatalf("describe %s", desc)
	}
	tg := call(url.Values{"Action": {"CreateTargetGroup"}, "Version": {"2015-12-01"}, "Name": {"tg1"}, "Port": {"80"}, "Protocol": {"HTTP"}})
	if !strings.Contains(tg, "tg1") {
		t.Fatalf("tg %s", tg)
	}
	lis := call(url.Values{"Action": {"CreateListener"}, "Version": {"2015-12-01"}, "LoadBalancerArn": {"arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/alb1/x"}, "Port": {"443"}, "Protocol": {"HTTPS"}})
	larn := ""
	if i := strings.Index(lis, "arn:aws:elasticloadbalancing:"); i >= 0 {
		rest := lis[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			larn = rest[:j]
		}
	}
	if larn == "" {
		t.Fatalf("listener %s", lis)
	}
	rule := call(url.Values{"Action": {"CreateRule"}, "Version": {"2015-12-01"}, "ListenerArn": {larn}, "Priority": {"10"}})
	if !strings.Contains(rule, "RuleArn") {
		t.Fatalf("rule %s", rule)
	}
	rules := call(url.Values{"Action": {"DescribeRules"}, "Version": {"2015-12-01"}, "ListenerArn": {larn}})
	if !strings.Contains(rules, "RuleArn") {
		t.Fatalf("describe rules %s", rules)
	}
	call(url.Values{"Action": {"AddListenerCertificates"}, "Version": {"2015-12-01"}, "ListenerArn": {larn}, "Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:000000000000:certificate/x"}})
	certs := call(url.Values{"Action": {"DescribeListenerCertificates"}, "Version": {"2015-12-01"}, "ListenerArn": {larn}})
	if !strings.Contains(certs, "certificate") && !strings.Contains(certs, "Certificates") {
		t.Fatalf("certs %s", certs)
	}
	ts1 := call(url.Values{"Action": {"CreateTrustStore"}, "Version": {"2015-12-01"}, "Name": {"ts1"}})
	if !strings.Contains(ts1, "TrustStoreArn") {
		t.Fatalf("trust %s", ts1)
	}
	call(url.Values{"Action": {"DescribeSSLPolicies"}, "Version": {"2015-12-01"}})
	call(url.Values{"Action": {"DescribeAccountLimits"}, "Version": {"2015-12-01"}})
	tarn := ""
	if i := strings.Index(ts1, "arn:aws:elasticloadbalancing:"); i >= 0 {
		rest := ts1[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			tarn = rest[:j]
		}
	}
	rarn := ""
	if i := strings.Index(rule, "arn:aws:elasticloadbalancing:"); i >= 0 {
		rest := rule[i:]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			rarn = rest[:j]
		}
	}
	v := "2015-12-01"
	lb := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/alb1/x"
	call(url.Values{"Action": {"ModifyRule"}, "Version": {v}, "RuleArn": {rarn}, "Priority": {"20"}})
	call(url.Values{"Action": {"SetRulePriorities"}, "Version": {v}, "RulePriorities.member.1.RuleArn": {rarn}, "RulePriorities.member.1.Priority": {"5"}})
	call(url.Values{"Action": {"DescribeListenerAttributes"}, "Version": {v}, "ListenerArn": {larn}})
	call(url.Values{"Action": {"ModifyListenerAttributes"}, "Version": {v}, "ListenerArn": {larn}})
	call(url.Values{"Action": {"ModifyListener"}, "Version": {v}, "ListenerArn": {larn}, "Port": {"8443"}})
	call(url.Values{"Action": {"RemoveListenerCertificates"}, "Version": {v}, "ListenerArn": {larn}})
	call(url.Values{"Action": {"DescribeTargetGroupAttributes"}, "Version": {v}, "TargetGroupArn": {"arn:tg"}})
	call(url.Values{"Action": {"ModifyTargetGroupAttributes"}, "Version": {v}, "TargetGroupArn": {"arn:tg"}})
	call(url.Values{"Action": {"DescribeTrustStores"}, "Version": {v}})
	call(url.Values{"Action": {"ModifyTrustStore"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"AddTrustStoreRevocations"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"DescribeTrustStoreRevocations"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"GetTrustStoreCaCertificatesBundle"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"GetTrustStoreRevocationContent"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"DescribeTrustStoreAssociations"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"RemoveTrustStoreRevocations"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"DeleteSharedTrustStoreAssociation"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"DeleteTrustStore"}, "Version": {v}, "TrustStoreArn": {tarn}})
	call(url.Values{"Action": {"DescribeCapacityReservation"}, "Version": {v}, "LoadBalancerArn": {lb}})
	call(url.Values{"Action": {"ModifyCapacityReservation"}, "Version": {v}, "LoadBalancerArn": {lb}})
	call(url.Values{"Action": {"ModifyIpPools"}, "Version": {v}, "LoadBalancerArn": {lb}})
	call(url.Values{"Action": {"GetResourcePolicy"}, "Version": {v}, "ResourceArn": {lb}})
	call(url.Values{"Action": {"SetIpAddressType"}, "Version": {v}, "LoadBalancerArn": {lb}, "IpAddressType": {"ipv4"}})
	call(url.Values{"Action": {"SetSecurityGroups"}, "Version": {v}, "LoadBalancerArn": {lb}})
	call(url.Values{"Action": {"SetSubnets"}, "Version": {v}, "LoadBalancerArn": {lb}})
	call(url.Values{"Action": {"DeleteRule"}, "Version": {v}, "RuleArn": {rarn}})
}

func TestELBHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 51 {
		t.Fatalf("elb Operations() %d want 51", n)
	}
}
