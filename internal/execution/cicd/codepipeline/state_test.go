package codepipeline_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestStartInProgressAndStageStates(t *testing.T) {
	deps := spitest.Deps(t)
	pack, err := bundled.New("aws.codepipeline", deps)
	if err != nil {
		t.Fatal(err)
	}
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		res, err := pack.Invoke(ctx, &spi.Request{ServiceID: "aws.codepipeline", Identity: id, Operation: op, Input: in})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return res.Output
	}

	call("CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name":    "demo",
			"roleArn": "arn:aws:iam::000000000000:role/cp",
			"artifactStore": map[string]any{
				"type":     "S3",
				"location": "bucket",
			},
			"stages": []any{
				map[string]any{
					"name": "Source",
					"actions": []any{
						map[string]any{"name": "Src", "actionTypeId": map[string]any{"category": "Source", "owner": "AWS", "provider": "S3", "version": "1"}},
					},
				},
				map[string]any{
					"name": "Build",
					"actions": []any{
						map[string]any{"name": "Cb", "actionTypeId": map[string]any{"category": "Build", "owner": "AWS", "provider": "CodeBuild", "version": "1"}},
					},
				},
			},
		},
	})

	started := call("StartPipelineExecution", map[string]any{"name": "demo"})
	execID, _ := started["pipelineExecutionId"].(string)
	if execID == "" {
		t.Fatalf("missing execution id: %#v", started)
	}
	got := call("GetPipelineExecution", map[string]any{"pipelineName": "demo", "pipelineExecutionId": execID})
	pe, _ := got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "InProgress" {
		t.Fatalf("want InProgress, got %q", status)
	}

	state := call("GetPipelineState", map[string]any{"name": "demo"})
	stages, _ := state["stageStates"].([]any)
	if len(stages) != 2 {
		t.Fatalf("want 2 stageStates, got %#v", state["stageStates"])
	}

	// Missing pipeline must not invent an execution.
	_, err = pack.Invoke(ctx, &spi.Request{
		ServiceID: "aws.codepipeline", Identity: id, Operation: "StartPipelineExecution",
		Input: map[string]any{"name": "nosuch"},
	})
	if err == nil {
		t.Fatal("expected PipelineNotFoundException")
	}
}
