package spine

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
)

func TestBootedServerIAMDenyDeleteBucket(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.iam", "aws.s3"}
	cfg.Seed = "iam-deny"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	iamAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=00"
	s3Auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	form := func(vals string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", iamAuth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("iam %d %s", res.StatusCode, b)
		}
	}
	form("Action=CreateRole&RoleName=denied")
	form("Action=PutRolePolicy&RoleName=denied&PolicyName=d&PolicyDocument=" + url.QueryEscape(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}`))

	put, _ := http.NewRequest(http.MethodPut, ts.URL+"/deny-b", nil)
	put.Header.Set("Authorization", s3Auth)
	res, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("create bucket %d", res.StatusCode)
	}

	del, _ := http.NewRequest(http.MethodDelete, ts.URL+"/deny-b", nil)
	del.Header.Set("Authorization", s3Auth)
	del.Header.Set("X-Mirror-Role", "denied")
	res, err = http.DefaultClient.Do(del)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatalf("want 403 deny, got %d %s", res.StatusCode, b)
	}
}

func TestBootedServerIAMAllowEngine(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.iam", "aws.s3"}
	cfg.Seed = "iam-allow"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	iamAuth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/iam/aws4_request, SignedHeaders=host, Signature=00"
	s3Auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"
	form := func(vals string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(vals))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", iamAuth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("iam %d %s", res.StatusCode, b)
		}
		return string(b)
	}
	form("Action=CreateRole&RoleName=reader")
	form("Action=PutRolePolicy&RoleName=reader&PolicyName=p&PolicyDocument=" + url.QueryEscape(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`))
	put, _ := http.NewRequest(http.MethodPut, ts.URL+"/allow-b", nil)
	put.Header.Set("Authorization", s3Auth)
	res, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("mb %d", res.StatusCode)
	}
	obj, _ := http.NewRequest(http.MethodPut, ts.URL+"/allow-b/k", strings.NewReader("hi"))
	obj.Header.Set("Authorization", s3Auth)
	res, err = http.DefaultClient.Do(obj)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 {
		t.Fatalf("put %d", res.StatusCode)
	}

	get, _ := http.NewRequest(http.MethodGet, ts.URL+"/allow-b/k", nil)
	get.Header.Set("Authorization", s3Auth)
	get.Header.Set("X-Mirror-Role", "reader")
	res, err = http.DefaultClient.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	gb, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(gb) != "hi" {
		t.Fatalf("get allow %d %s", res.StatusCode, gb)
	}

	put2, _ := http.NewRequest(http.MethodPut, ts.URL+"/allow-b/k2", strings.NewReader("no"))
	put2.Header.Set("Authorization", s3Auth)
	put2.Header.Set("X-Mirror-Role", "reader")
	res, err = http.DefaultClient.Do(put2)
	if err != nil {
		t.Fatal(err)
	}
	pb, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatalf("put should implicit-deny, got %d %s", res.StatusCode, pb)
	}

	sim := form("Action=SimulatePrincipalPolicy&PolicySourceArn=" + url.QueryEscape("arn:aws:iam::000000000000:role/reader") + "&ActionNames.member.1=" + url.QueryEscape("s3:GetObject") + "&ActionNames.member.2=" + url.QueryEscape("s3:PutObject"))
	if !strings.Contains(sim, "allowed") || !strings.Contains(sim, "implicitDeny") {
		t.Fatalf("simulate %s", sim)
	}
	custom := form("Action=SimulateCustomPolicy&PolicyInputList.member.1=" + url.QueryEscape(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}`) + "&ActionNames.member.1=" + url.QueryEscape("s3:PutObject"))
	if !strings.Contains(custom, "allowed") {
		t.Fatalf("custom %s", custom)
	}
}
