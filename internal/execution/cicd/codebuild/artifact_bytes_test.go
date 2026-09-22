package codebuild_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cbexec "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// AWS-CB-ARTIFACT-BYTES: StartBuild runs buildspec 0.2 and yields nonempty artifact bytes.
func TestAWS_CB_ARTIFACT_BYTES(t *testing.T) {
	cbexec.Enable()
	t.Cleanup(cbexec.Disable)

	deps := spitest.Deps(t)
	pack, err := bundled.New("aws.codebuild", deps)
	if err != nil {
		t.Fatal(err)
	}
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := pack.Invoke(ctx, &spi.Request{ServiceID: "aws.codebuild", Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}

	nonce := "nonce-" + t.Name()
	buildspecYAML := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n '" + nonce + "' > artifact.txt\n"

	call("CreateProject", map[string]any{
		"name":        "artifact-bytes",
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source": map[string]any{
			"type":      "NO_SOURCE",
			"buildspec": buildspecYAML,
		},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:7.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
		"artifacts": map[string]any{
			"type":     "S3",
			"location": "local-bucket/out",
		},
	})

	started := call("StartBuild", map[string]any{"projectName": "artifact-bytes"})
	build, _ := started["build"].(map[string]any)
	if build == nil {
		t.Fatalf("no build in StartBuild: %#v", started)
	}
	if status := str(build["buildStatus"]); status != "SUCCEEDED" {
		t.Fatalf("want SUCCEEDED after execute hook, got %q reason=%v excerpt=%v",
			status, build["buildStatusReason"], build["mirrorLogExcerpt"])
	}
	arts, _ := build["artifacts"].(map[string]any)
	if arts == nil || str(arts["sha256sum"]) == "" {
		t.Fatalf("missing artifact digest: %#v", arts)
	}
	key := str(arts["mirrorBlobKey"])
	if key == "" || deps.Blobs == nil {
		t.Fatal("missing blob key or BlobStore")
	}
	body, _, err := deps.Blobs.Get(ctx, key)
	if err != nil {
		t.Fatalf("blob get: %v", err)
	}
	defer body.Close()
	all, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(all) != nonce {
		t.Fatalf("artifact bytes %q want %q", all, nonce)
	}

	// ADV-FALSE-SUCCESS / ADV-EXEC-OFF: without executor, must not fabricate SUCCEEDED.
	cbexec.Disable()
	call("CreateProject", map[string]any{
		"name":        "no-exec",
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": buildspecYAML},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
	})
	pending := call("StartBuild", map[string]any{"projectName": "no-exec"})
	b2, _ := pending["build"].(map[string]any)
	if str(b2["buildStatus"]) == "SUCCEEDED" {
		t.Fatal("StartBuild must not SUCCEEDED without executor")
	}
	if str(b2["buildStatus"]) != "IN_PROGRESS" {
		t.Fatalf("want IN_PROGRESS without executor, got %q", str(b2["buildStatus"]))
	}
	_ = strings.TrimSpace
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
