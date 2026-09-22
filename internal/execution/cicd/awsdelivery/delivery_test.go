package awsdelivery_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cb "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	cd "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codedeploy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// AWS-DELIVERY: CreateProject+StartBuild → artifact blob → CreateDeployment
// (local Linux target) → HTTP-served nonce. Build fail blocks production advance.
// Pipeline Approval→Deploy composite lives in TestAWS_PIPE_APPROVAL_DEPLOY.
// Amplify preview/main isolation lives in TestAWS_AMPLIFY_DELIVERY.
func TestAWS_DELIVERY(t *testing.T) {
	cb.Enable()
	cd.Enable()
	t.Cleanup(func() { cb.Disable(); cd.Disable() })

	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	nonce := "aws-delivery-" + t.Name()

	cbPack, err := bundled.New("aws.codebuild", deps)
	if err != nil {
		t.Fatal(err)
	}
	cdPack, err := bundled.New("aws.codedeploy", deps)
	if err != nil {
		t.Fatal(err)
	}
	call := func(p spi.BehaviorPack, svc, op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := p.Invoke(ctx, &spi.Request{ServiceID: svc, Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}

	buildspec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n '" + nonce + "' > artifact.txt\n"
	call(cbPack, "aws.codebuild", "CreateProject", map[string]any{
		"name": "pipe-build", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": buildspec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "S3", "location": "local/out"},
	})
	started := call(cbPack, "aws.codebuild", "StartBuild", map[string]any{"projectName": "pipe-build"})
	build, _ := started["build"].(map[string]any)
	if str(build["buildStatus"]) != "SUCCEEDED" {
		t.Fatalf("build %#v", build)
	}
	arts, _ := build["artifacts"].(map[string]any)
	blobKey := str(arts["mirrorBlobKey"])
	if blobKey == "" {
		t.Fatal("missing artifact blob")
	}
	body, _, err := deps.Blobs.Get(ctx, blobKey)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := io.ReadAll(body)
	body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != nonce {
		t.Fatalf("blob %q want %q", buf, nonce)
	}

	prodRoot := t.TempDir()
	cd.SetDestinationRoot(prodRoot)
	appSpec := "version: 0.0\nos: linux\nfiles:\n  - source: artifact.txt\n    destination: /\nhooks:\n  ApplicationStart:\n    - location: start.sh\n"
	call(cdPack, "aws.codedeploy", "CreateApplication", map[string]any{"applicationName": "app"})
	call(cdPack, "aws.codedeploy", "CreateDeploymentGroup", map[string]any{
		"applicationName": "app", "deploymentGroupName": "dg",
		"serviceRoleArn": "arn:aws:iam::000000000000:role/cd",
	})
	depOut := call(cdPack, "aws.codedeploy", "CreateDeployment", map[string]any{
		"applicationName": "app", "deploymentGroupName": "dg",
		"mirrorAppSpec": appSpec,
		"mirrorRevisionFiles": map[string]any{
			"artifact.txt": string(buf),
			"start.sh":     "#!/bin/sh\necho started > \"$DESTINATION/started\"\n",
		},
		"revision": map[string]any{"revisionType": "S3", "mirrorAppSpec": appSpec},
	})
	depID := str(depOut["deploymentId"])
	got := call(cdPack, "aws.codedeploy", "GetDeployment", map[string]any{"deploymentId": depID})
	info, _ := got["deploymentInfo"].(map[string]any)
	if str(info["status"]) != "Succeeded" {
		t.Fatalf("deploy status %#v", got)
	}
	deployed, err := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	if err != nil {
		t.Fatalf("dest file: %v", err)
	}
	if string(deployed) != nonce {
		t.Fatalf("deployed %q want %q", deployed, nonce)
	}
	if _, err := os.Stat(filepath.Join(prodRoot, "started")); err != nil {
		t.Fatalf("ApplicationStart hook marker: %v", err)
	}
	site, err := delivery.Publish(map[string][]byte{"index.html": deployed})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = site.Close() })
	httpBody, err := site.Get("index.html")
	if err != nil || string(httpBody) != nonce {
		t.Fatalf("http %q err %v", httpBody, err)
	}

	// Negative: failed build → no production advance (dest untouched marker).
	before, _ := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	failSpec := "version: 0.2\nphases:\n  build:\n    commands:\n      - exit 1\n"
	call(cbPack, "aws.codebuild", "CreateProject", map[string]any{
		"name": "fail-build", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": failSpec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "S3", "location": "local/fail"},
	})
	failed := call(cbPack, "aws.codebuild", "StartBuild", map[string]any{"projectName": "fail-build"})
	fb, _ := failed["build"].(map[string]any)
	if str(fb["buildStatus"]) != "FAILED" {
		t.Fatalf("want FAILED got %#v", fb)
	}
	// Guard: do not CreateDeployment on failed build — production file unchanged.
	after, err := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	if err != nil || string(after) != string(before) {
		t.Fatalf("production advanced after failed build: before=%q after=%q err=%v", before, after, err)
	}

	// ADV-FALSE-SUCCESS: without executor, StartBuild stays IN_PROGRESS.
	cb.Disable()
	call(cbPack, "aws.codebuild", "CreateProject", map[string]any{
		"name": "no-exec-del", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": buildspec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
	})
	pending := call(cbPack, "aws.codebuild", "StartBuild", map[string]any{"projectName": "no-exec-del"})
	if str(pending["build"].(map[string]any)["buildStatus"]) == "SUCCEEDED" {
		t.Fatal("false success without executor")
	}
}

func str(v any) string { s, _ := v.(string); return s }
