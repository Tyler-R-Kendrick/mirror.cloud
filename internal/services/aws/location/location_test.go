package location

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

func TestLocationHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 8 {
		t.Fatalf("location Operations() %d want 8", n)
	}
}

func TestBootedServerLocationCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.location"}
	cfg.Seed = "geo-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/geo/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "LocationService."+op)
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
	created := call("CreatePlaceIndex", `{"IndexName":"ix1","DataSource":"Esri"}`)
	if created["IndexName"] != "ix1" {
		t.Fatalf("create %v", created)
	}
	got := call("DescribePlaceIndex", `{"IndexName":"ix1"}`)
	if got["IndexName"] != "ix1" {
		t.Fatalf("get %v", got)
	}
	search := call("SearchPlaceIndexForText", `{"IndexName":"ix1","Text":"Seattle"}`)
	if search["Results"] == nil {
		t.Fatalf("search %v", search)
	}
	call("DeletePlaceIndex", `{"IndexName":"ix1"}`)
	listed := call("ListPlaceIndexes", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"ix1"`) {
		t.Fatalf("still present %s", raw)
	}
}
