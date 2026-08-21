package acm

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerACMRequestDescribe(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.acm"}
	cfg.Seed = "acm-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/acm/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CertificateManager."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	got := call("RequestCertificate", `{"DomainName":"example.com"}`)
	arn, _ := got["CertificateArn"].(string)
	if arn == "" {
		t.Fatalf("request %v", got)
	}
	desc := call("DescribeCertificate", `{"CertificateArn":"`+arn+`"}`)
	if desc["Certificate"] == nil {
		t.Fatalf("describe %v", desc)
	}
	listed := call("ListCertificates", `{}`)
	if listed["CertificateSummaryList"] == nil {
		t.Fatalf("list %v", listed)
	}
	pem := call("GetCertificate", `{"CertificateArn":"`+arn+`"}`)
	if !strings.Contains(str(pem["Certificate"]), "BEGIN CERTIFICATE") {
		t.Fatalf("pem %v", pem)
	}
	exp := call("ExportCertificate", `{"CertificateArn":"`+arn+`"}`)
	if str(exp["PrivateKey"]) == "" {
		t.Fatalf("export %v", exp)
	}
	imp := call("ImportCertificate", `{"Certificate":"-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n","PrivateKey":"mirror-private-key"}`)
	iarn, _ := imp["CertificateArn"].(string)
	if iarn == "" {
		t.Fatalf("import %v", imp)
	}
	call("RenewCertificate", `{"CertificateArn":"`+arn+`"}`)
	rev := call("RevokeCertificate", `{"CertificateArn":"`+iarn+`","RevocationReason":"UNSPECIFIED"}`)
	if rec, _ := rev["Certificate"].(map[string]any); str(rec["Status"]) != "REVOKED" && str(rev["CertificateArn"]) == "" {
		t.Fatalf("revoke %v", rev)
	}
	call("ResendValidationEmail", `{"CertificateArn":"`+arn+`","Domain":"example.com","ValidationDomain":"example.com"}`)
	call("UpdateCertificateOptions", `{"CertificateArn":"`+arn+`","Options":{"CertificateTransparencyLoggingPreference":"DISABLED"}}`)
	srch := call("SearchCertificates", `{"DomainName":"example.com"}`)
	if srch["CertificateSummaryList"] == nil {
		t.Fatalf("search %v", srch)
	}
	call("ListCertificateDomainValidations", `{"CertificateArn":"`+arn+`"}`)
	call("TagResource", `{"ResourceArn":"`+arn+`","Tags":[{"Key":"k","Value":"v"}]}`)
	tags := call("ListTagsForResource", `{"ResourceArn":"`+arn+`"}`)
	tb, _ := json.Marshal(tags["Tags"])
	if !strings.Contains(string(tb), "k") {
		t.Fatalf("tags %v", tags)
	}
	call("UntagResource", `{"ResourceArn":"`+arn+`"}`)
	call("PutAccountConfiguration", `{"ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	acct := call("GetAccountConfiguration", `{}`)
	if acct["ExpiryEvents"] == nil && acct["DaysBeforeExpiry"] == nil {
		t.Fatalf("acct %v", acct)
	}
	dv := call("CreateAcmeDomainValidation", `{"DomainName":"example.com"}`)
	dva, _ := dv["DomainValidationArn"].(string)
	call("DescribeAcmeDomainValidation", `{"DomainValidationArn":"`+dva+`"}`)
	call("UpdateAcmeDomainValidation", `{"DomainValidationArn":"`+dva+`","Status":"VALID"}`)
	call("ListAcmeDomainValidations", `{}`)
	call("DeleteAcmeDomainValidation", `{"DomainValidationArn":"`+dva+`"}`)
	ep := call("CreateAcmeEndpoint", `{"EndpointName":"ep1"}`)
	epa, _ := ep["EndpointArn"].(string)
	call("DescribeAcmeEndpoint", `{"EndpointArn":"`+epa+`"}`)
	call("UpdateAcmeEndpoint", `{"EndpointArn":"`+epa+`","Status":"ACTIVE"}`)
	call("ListAcmeEndpoints", `{}`)
	call("DeleteAcmeEndpoint", `{"EndpointArn":"`+epa+`"}`)
	eab := call("CreateAcmeExternalAccountBinding", `{}`)
	eaba, _ := eab["ExternalAccountBindingArn"].(string)
	call("DescribeAcmeExternalAccountBinding", `{"ExternalAccountBindingArn":"`+eaba+`"}`)
	creds := call("GetAcmeExternalAccountBindingCredentials", `{"ExternalAccountBindingArn":"`+eaba+`"}`)
	if str(creds["MacKey"]) == "" {
		t.Fatalf("eab creds %v", creds)
	}
	call("ListAcmeExternalAccountBindings", `{}`)
	call("RevokeAcmeExternalAccountBinding", `{"ExternalAccountBindingArn":"`+eaba+`"}`)
	call("DeleteAcmeExternalAccountBinding", `{"ExternalAccountBindingArn":"`+eaba+`"}`)
	call("DescribeAcmeAccount", `{}`)
	call("ListAcmeAccounts", `{}`)
	call("RevokeAcmeAccount", `{}`)
}

func TestACMHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 40 {
		t.Fatalf("acm Operations() %d want 40", n)
	}
}

func str(v any) string { s, _ := v.(string); return s }
