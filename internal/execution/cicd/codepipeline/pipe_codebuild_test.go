package codepipeline_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cb "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	cp "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codepipeline"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// AWS-PIPE → CodeBuild: execute-on runs StartBuild; execute-off stays InProgress.
func TestAWS_PIPE_CODEBUILD(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()

	cbPack, err := bundled.New("aws.codebuild", deps)
	if err != nil {
		t.Fatal(err)
	}
	cpPack, err := bundled.New("aws.codepipeline", deps)
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

	spec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo ok > out.txt\n"
	call(cbPack, "aws.codebuild", "CreateProject", map[string]any{
		"name": "pipe-cb", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": spec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
	})
	call(cpPack, "aws.codepipeline", "CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name": "pipe-cb-demo", "roleArn": "arn:aws:iam::000000000000:role/cp",
			"artifactStore": map[string]any{"type": "S3", "location": "bucket"},
			"stages": []any{
				map[string]any{
					"name": "Source",
					"actions": []any{
						map[string]any{
							"name": "Src",
							"actionTypeId": map[string]any{
								"category": "Source", "owner": "AWS", "provider": "S3", "version": "1",
							},
						},
					},
				},
				map[string]any{
					"name": "Build",
					"actions": []any{
						map[string]any{
							"name": "Cb",
							"actionTypeId": map[string]any{
								"category": "Build", "owner": "AWS", "provider": "CodeBuild", "version": "1",
							},
							"configuration": map[string]any{"ProjectName": "pipe-cb"},
						},
					},
				},
			},
		},
	})

	cb.Disable()
	cp.Disable()
	pending := call(cpPack, "aws.codepipeline", "StartPipelineExecution", map[string]any{"name": "pipe-cb-demo"})
	pid, _ := pending["pipelineExecutionId"].(string)
	got := call(cpPack, "aws.codepipeline", "GetPipelineExecution", map[string]any{
		"pipelineName": "pipe-cb-demo", "pipelineExecutionId": pid,
	})
	pe, _ := got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "InProgress" {
		t.Fatalf("exec-off want InProgress, got %#v", pe)
	}

	cb.Enable()
	cp.Enable()
	t.Cleanup(func() { cb.Disable(); cp.Disable() })

	started := call(cpPack, "aws.codepipeline", "StartPipelineExecution", map[string]any{"name": "pipe-cb-demo"})
	execID, _ := started["pipelineExecutionId"].(string)
	got = call(cpPack, "aws.codepipeline", "GetPipelineExecution", map[string]any{
		"pipelineName": "pipe-cb-demo", "pipelineExecutionId": execID,
	})
	pe, _ = got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "Succeeded" {
		t.Fatalf("exec-on want Succeeded, got %#v", pe)
	}
}
