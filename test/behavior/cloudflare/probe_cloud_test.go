package cloudflare_test

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
)

// TestCloudflareLiveProbe hits Cloudflare's KV namespace list API when
// CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are set, and checks the
// response carries the same id/title fields mirror's list answers.
func TestCloudflareLiveProbe(t *testing.T) {
	token := os.Getenv("CLOUDFLARE_API_TOKEN")
	acct := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	if token == "" || acct == "" {
		t.Skip("set CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID to run live probe")
	}
	url := "https://api.cloudflare.com/client/v4/accounts/" + acct + "/storage/kv/namespaces?per_page=5"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d: %s", resp.StatusCode, raw)
	}
	var body struct {
		Success bool             `json:"success"`
		Result  []map[string]any `json:"result"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	if !body.Success {
		t.Fatalf("success=false: %s", raw)
	}
	for _, ns := range body.Result {
		if _, ok := ns["id"]; !ok {
			t.Fatalf("missing id: %#v", ns)
		}
		if _, ok := ns["title"]; !ok {
			t.Fatalf("missing title: %#v", ns)
		}
	}
}
