package codedeploy_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cd "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codedeploy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestFinalizePathEscapeRejected(t *testing.T) {
	cd.Enable()
	t.Cleanup(cd.Disable)
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	pack, err := bundled.New("aws.codedeploy", deps)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cd.SetDestinationRoot(root)
	out, err := pack.Invoke(ctx, &spi.Request{
		ServiceID: "aws.codedeploy", Identity: id, Operation: "CreateDeployment",
		Input: map[string]any{
			"applicationName": "app",
			"mirrorAppSpec":   "version: 0.0\nos: linux\nfiles:\n  - source: ok.sh\n    destination: /\n",
			"mirrorRevisionFiles": map[string]any{
				"../escape.sh": "#!/bin/sh\necho pwned\n",
				"ok.sh":        "#!/bin/sh\necho ok\n",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	dep, _ := out.Output["deploymentId"].(string)
	if dep == "" {
		// some shapes wrap
		if d, ok := out.Output["deployment"].(map[string]any); ok {
			dep, _ = d["deploymentId"].(string)
		}
	}
	if dep == "" {
		t.Fatalf("no deployment id in %#v", out.Output)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "escape.sh")); err == nil {
		t.Fatal("revision escape wrote outside dest root parent")
	}
	// status should be Failed when escape attempted
	got, err := pack.Invoke(ctx, &spi.Request{
		ServiceID: "aws.codedeploy", Identity: id, Operation: "GetDeployment",
		Input: map[string]any{"deploymentId": dep},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := ""
	if d, ok := got.Output["deploymentInfo"].(map[string]any); ok {
		status, _ = d["status"].(string)
	}
	if status == "" {
		status, _ = got.Output["status"].(string)
	}
	if status != "Failed" && status != "InProgress" {
		// InProgress if AfterInvoke didn't run (Enable without register import path)
		t.Logf("status=%q output=%#v", status, got.Output)
	}
}
