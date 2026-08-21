package route53

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerRoute53HostedZoneRRSet(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.route53"}
	cfg.Seed = "r53-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/route53/aws4_request, SignedHeaders=host, Signature=00"
	do := func(method, path, body string) (int, string) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req, _ := http.NewRequest(method, ts.URL+path, rdr)
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", "application/xml")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %s %d %s", method, path, res.StatusCode, b)
		}
		return res.StatusCode, string(b)
	}
	_, created := do(http.MethodPost, "/2013-04-01/hostedzone", `<CreateHostedZoneRequest><Name>example.com</Name><CallerReference>c1</CallerReference></CreateHostedZoneRequest>`)
	if !strings.Contains(created, "example.com") {
		t.Fatalf("create %s", created)
	}
	id := ""
	if i := strings.Index(created, "/hostedzone/"); i >= 0 {
		rest := created[i+len("/hostedzone/"):]
		if j := strings.IndexAny(rest, "<"); j > 0 {
			id = rest[:j]
		}
	}
	if id == "" {
		t.Fatalf("no zone id %s", created)
	}
	_, listed := do(http.MethodGet, "/2013-04-01/hostedzone", "")
	if !strings.Contains(listed, "example.com") {
		t.Fatalf("list %s", listed)
	}
	chg := `<ChangeResourceRecordSetsRequest><ChangeBatch><Changes><Change><Action>UPSERT</Action><ResourceRecordSet><Name>www.example.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	do(http.MethodPost, "/2013-04-01/hostedzone/"+id+"/rrset", chg)
	_, rrs := do(http.MethodGet, "/2013-04-01/hostedzone/"+id+"/rrset", "")
	if !strings.Contains(rrs, "www.example.com") || !strings.Contains(rrs, "1.2.3.4") {
		t.Fatalf("rrset %s", rrs)
	}
	_, hc := do(http.MethodPost, "/?Action=CreateHealthCheck", `<CreateHealthCheckRequest><CallerReference>c</CallerReference></CreateHealthCheckRequest>`)
	if !strings.Contains(hc, "HealthCheck") && !strings.Contains(hc, "Id") {
		t.Fatalf("health %s", hc)
	}
	do(http.MethodPost, "/?Action=ListHealthChecks", "")
	do(http.MethodPost, "/?Action=CreateTrafficPolicy", `<CreateTrafficPolicyRequest><Name>p</Name><Document>{}</Document></CreateTrafficPolicyRequest>`)
	_, ans := do(http.MethodPost, "/?Action=TestDNSAnswer", `<TestDNSAnswerRequest><HostedZoneId>`+id+`</HostedZoneId><RecordName>www.example.com</RecordName><RecordType>A</RecordType></TestDNSAnswerRequest>`)
	if !strings.Contains(ans, "1.2.3.4") && !strings.Contains(ans, "127.0.0.1") && !strings.Contains(ans, "NOERROR") {
		t.Fatalf("dns %s", ans)
	}
	do(http.MethodPost, "/?Action=GetChange", `<GetChangeRequest><Id>/change/x</Id></GetChangeRequest>`)
}

func TestRoute53HTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 71 {
		t.Fatalf("route53 Operations() %d want 71", n)
	}
}

func TestBootedServerRoute53ExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.route53"}
	cfg.Seed = "r53-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/route53/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), raw)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/?Action="+op, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", op, res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
		}
		return string(raw)
	}
	created := hard("CreateHealthCheck", "HealthCheckId=hcboot&Name=boot.example.com&FullyQualifiedDomainName=boot.example.com")
	if !strings.Contains(created, "hcboot") && !strings.Contains(created, "HealthCheck") {
		t.Fatalf("create hc %s", created)
	}
	got := hard("GetHealthCheck", "HealthCheckId=hcboot")
	if !strings.Contains(got, "hcboot") && !strings.Contains(got, "HealthCheck") {
		t.Fatalf("get hc %s", got)
	}
	hard("DeleteHealthCheck", "HealthCheckId=hcboot")
	listed := hard("ListHealthChecks", "")
	if strings.Contains(listed, "hcboot") {
		t.Fatalf("hc still present %s", listed)
	}
	payload := "HealthCheckId=hcboot&Id=hcboot&Name=p1&HostedZoneId=Z1&VPCId=vpc-1&ResourceId=r1&Document={}&TrafficPolicyId=p1&CloudWatchLogsLogGroupArn=arn:logs&CountryCode=US&Type=MAX_HEALTH_CHECKS_BY_OWNER&RecordName=www.example.com&RecordType=A"
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
