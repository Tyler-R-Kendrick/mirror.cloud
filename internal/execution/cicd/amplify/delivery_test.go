package amplify_test

import (
	"context"
	"io"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	amp "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/amplify"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// AWS-AMPLIFY-DELIVERY: CreateApp + CreateBranch preview/main + StartJob
// buildSpec → SUCCEED; preview≠production artifact bodies; HTTP via
// delivery.Publish; failed job does not advance production branch current.
func TestAWS_AMPLIFY_DELIVERY(t *testing.T) {
	amp.Enable()
	t.Cleanup(amp.Disable)

	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	nonce := "amp-delivery-" + t.Name()

	pack, err := bundled.New("aws.amplify", deps)
	if err != nil {
		t.Fatal(err)
	}
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := pack.Invoke(ctx, &spi.Request{
			ServiceID: "aws.amplify", Identity: id, Operation: op, Input: in,
		})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}

	app := call("CreateApp", map[string]any{"name": "web"})
	appID := str(app["app"].(map[string]any)["appId"])
	call("CreateBranch", map[string]any{"appId": appID, "branchName": "main"})
	call("CreateBranch", map[string]any{"appId": appID, "branchName": "preview"})

	prodSpec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n 'prod-" + nonce + "' > index.html\n"
	prevSpec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n 'prev-" + nonce + "' > index.html\n"
	prodJob := call("StartJob", map[string]any{
		"appId": appID, "branchName": "main", "jobType": "RELEASE", "buildSpec": prodSpec,
	})
	prevJob := call("StartJob", map[string]any{
		"appId": appID, "branchName": "preview", "jobType": "RELEASE", "buildSpec": prevSpec,
	})
	prodSum, _ := prodJob["jobSummary"].(map[string]any)
	prevSum, _ := prevJob["jobSummary"].(map[string]any)
	if str(prodSum["status"]) != "SUCCEED" || str(prevSum["status"]) != "SUCCEED" {
		t.Fatalf("amplify jobs prod=%#v prev=%#v", prodSum, prevSum)
	}
	prodArts, _ := prodSum["artifacts"].(map[string]any)
	prevArts, _ := prevSum["artifacts"].(map[string]any)
	if str(prodArts["body"]) != "prod-"+nonce || str(prevArts["body"]) != "prev-"+nonce {
		t.Fatalf("branch isolation broken prod=%#v prev=%#v", prodArts, prevArts)
	}
	if str(prodArts["mirrorBranchCurrent"]) == str(prevArts["mirrorBranchCurrent"]) {
		t.Fatal("preview and production share current blob key")
	}

	prodSite, err := delivery.Publish(map[string][]byte{"index.html": []byte(str(prodArts["body"]))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prodSite.Close() })
	prevSite, err := delivery.Publish(map[string][]byte{"index.html": []byte(str(prevArts["body"]))})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prevSite.Close() })
	if b, _ := prodSite.Get("index.html"); string(b) != "prod-"+nonce {
		t.Fatalf("prod site %q", b)
	}
	if b, _ := prevSite.Get("index.html"); string(b) != "prev-"+nonce {
		t.Fatalf("preview site %q", b)
	}

	// Failed job on main: production branch current blob stays prior SUCCEED body.
	curKey := str(prodArts["mirrorBranchCurrent"])
	before, err := readBlob(ctx, deps, curKey)
	if err != nil || string(before) != "prod-"+nonce {
		t.Fatalf("pre-fail current %q err %v", before, err)
	}
	failSpec := "version: 0.2\nphases:\n  build:\n    commands:\n      - exit 1\n"
	failed := call("StartJob", map[string]any{
		"appId": appID, "branchName": "main", "jobType": "RELEASE", "buildSpec": failSpec,
	})
	failSum, _ := failed["jobSummary"].(map[string]any)
	if str(failSum["status"]) != "FAILED" {
		t.Fatalf("want FAILED got %#v", failSum)
	}
	after, err := readBlob(ctx, deps, curKey)
	if err != nil || string(after) != string(before) {
		t.Fatalf("production advanced after failed job: before=%q after=%q err=%v", before, after, err)
	}
	if arts, _ := failSum["artifacts"].(map[string]any); arts != nil && str(arts["body"]) != "" {
		t.Fatalf("failed job must not publish body: %#v", arts)
	}

	// Without executor, StartJob stays RUNNING (ADV-FALSE-SUCCESS).
	amp.Disable()
	pending := call("StartJob", map[string]any{
		"appId": appID, "branchName": "preview", "jobType": "RELEASE", "buildSpec": prevSpec,
	})
	if str(pending["jobSummary"].(map[string]any)["status"]) == "SUCCEED" {
		t.Fatal("false success without amplify executor")
	}
}

func readBlob(ctx context.Context, deps spi.Deps, key string) ([]byte, error) {
	body, _, err := deps.Blobs.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	return io.ReadAll(body)
}

func str(v any) string { s, _ := v.(string); return s }
