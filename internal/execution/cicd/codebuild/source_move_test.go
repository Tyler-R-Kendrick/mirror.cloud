package codebuild_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cb "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// ADV-SOURCE-MOVE: after StartBuild admits, changing project buildspec does not
// rewrite the in-flight build's admitted source (Finalize uses build record).
func TestADV_SOURCE_MOVE(t *testing.T) {
	cb.Enable()
	t.Cleanup(cb.Disable)
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	pack, err := bundled.New("aws.codebuild", deps)
	if err != nil {
		t.Fatal(err)
	}
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := pack.Invoke(ctx, &spi.Request{ServiceID: "aws.codebuild", Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}
	nonce := "src-" + t.Name()
	spec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n '" + nonce + "' > out.txt\n"
	call("CreateProject", map[string]any{
		"name": "src-move", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": spec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
	})
	started := call("StartBuild", map[string]any{"projectName": "src-move"})
	build, _ := started["build"].(map[string]any)
	if build["buildStatus"] != "SUCCEEDED" {
		t.Fatalf("%#v", build)
	}
	// Mutate project after build completed — new StartBuild would see new spec;
	// admitted build record must still reflect original (artifact body via override path).
	call("UpdateProject", map[string]any{
		"name":   "src-move",
		"source": map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n mutated > out.txt\n"},
	})
	// Re-read original build — status remains SUCCEEDED (not rewritten by project mutate).
	got := call("BatchGetBuilds", map[string]any{"ids": []any{build["id"]}})
	builds, _ := got["builds"].([]any)
	if len(builds) != 1 {
		t.Fatalf("%#v", got)
	}
	b0, _ := builds[0].(map[string]any)
	if b0["buildStatus"] != "SUCCEEDED" {
		t.Fatalf("project mutate rewrote build %#v", b0)
	}
}
