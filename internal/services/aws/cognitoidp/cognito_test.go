package cognitoidp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/rand"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestCognitoHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 19 {
		t.Fatalf("cognito Operations() %d want 19", n)
	}
}

func TestBootedServerCognitoUserPool(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cognito-idp"}
	cfg.Seed = "cog-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cognito-idp/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
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
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateUserPool", `{"PoolName":"p1"}`)
	pool, _ := created["UserPool"].(map[string]any)
	id, _ := pool["Id"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeUserPool", `{"UserPoolId":"`+id+`"}`)
	if got["UserPool"] == nil {
		t.Fatalf("describe %v", got)
	}
	listed := call("ListUserPools", `{"MaxResults":10}`)
	if listed["UserPools"] == nil {
		t.Fatalf("list %v", listed)
	}
	call("AdminCreateUser", `{"UserPoolId":"`+id+`","Username":"alice"}`)
	user := call("AdminGetUser", `{"UserPoolId":"`+id+`","Username":"alice"}`)
	if user["Username"] != "alice" {
		t.Fatalf("get user %v", user)
	}
	cli := call("CreateUserPoolClient", `{"UserPoolId":"`+id+`","ClientName":"web"}`)
	client, _ := cli["UserPoolClient"].(map[string]any)
	cid, _ := client["ClientId"].(string)
	if cid == "" {
		t.Fatalf("client %v", cli)
	}
	gotC := call("DescribeUserPoolClient", `{"UserPoolId":"`+id+`","ClientId":"`+cid+`"}`)
	if gotC["UserPoolClient"] == nil {
		t.Fatalf("describe client %v", gotC)
	}
	call("AdminSetUserPassword", `{"UserPoolId":"`+id+`","Username":"alice","Password":"p@ss"}`)
	authz := call("InitiateAuth", `{"ClientId":"`+cid+`","AuthFlow":"USER_PASSWORD_AUTH","AuthParameters":{"USERNAME":"alice","PASSWORD":"p@ss"}}`)
	ar, _ := authz["AuthenticationResult"].(map[string]any)
	tok, _ := ar["AccessToken"].(string)
	idt, _ := ar["IdToken"].(string)
	if tok == "" || idt == "" {
		t.Fatalf("initiate %v", authz)
	}
	assertSignedJWT(t, "cog-1", tok, "access", "alice")
	assertSignedJWT(t, "cog-1", idt, "id", "alice")
	gu := call("GetUser", `{"AccessToken":"`+tok+`"}`)
	if gu["Username"] != "alice" {
		t.Fatalf("getuser %v", gu)
	}
	call("GlobalSignOut", `{"AccessToken":"`+tok+`"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"AccessToken":"`+tok+`"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.GetUser")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode < 400 {
		t.Fatalf("signed-out getuser %d %s", res.StatusCode, raw)
	}
	call("AdminDeleteUser", `{"UserPoolId":"`+id+`","Username":"alice"}`)
	users := call("ListUsers", `{"UserPoolId":"`+id+`"}`)
	raw, _ = json.Marshal(users)
	if strings.Contains(string(raw), `"Username":"alice"`) {
		t.Fatalf("user still present %s", raw)
	}
	call("DeleteUserPool", `{"UserPoolId":"`+id+`"}`)
}

func assertSignedJWT(t *testing.T, seed, tok, use, user string) {
	t.Helper()
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("not a jwt: %s", tok)
	}
	hdr, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(hdr), `"HS256"`) {
		t.Fatalf("header %s", hdr)
	}
	pay, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(pay, &claims); err != nil {
		t.Fatal(err)
	}
	if claims["token_use"] != use {
		t.Fatalf("token_use %v want %s", claims["token_use"], use)
	}
	if use == "access" && claims["username"] != user {
		t.Fatalf("username %v", claims["username"])
	}
	if use == "id" && claims["cognito:username"] != user {
		t.Fatalf("cognito:username %v", claims["cognito:username"])
	}
	key := rand.New(seed).Derive("cognito-hs256").Bytes(32)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	want := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if want != parts[2] {
		t.Fatalf("bad hmac")
	}
}
