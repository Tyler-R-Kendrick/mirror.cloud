package servicecatalog

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

func TestServiceCatalogHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 9 {
		t.Fatalf("servicecatalog Operations() %d want 9", n)
	}
}

func TestBootedServerServiceCatalogCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.servicecatalog"}
	cfg.Seed = "sc-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/servicecatalog/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWS242ServiceCatalogService."+op)
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
	created := call("CreatePortfolio", `{"DisplayName":"p1","ProviderName":"mirror"}`)
	port, _ := created["PortfolioDetail"].(map[string]any)
	id, _ := port["Id"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribePortfolio", `{"Id":"`+id+`"}`)
	if got["PortfolioDetail"] == nil {
		t.Fatalf("get %v", got)
	}
	prod := call("CreateProduct", `{"Name":"prod1","Owner":"mirror"}`)
	if prod["ProductViewDetail"] == nil {
		t.Fatalf("product %v", prod)
	}
	call("DeletePortfolio", `{"Id":"`+id+`"}`)
	listed := call("ListPortfolios", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
