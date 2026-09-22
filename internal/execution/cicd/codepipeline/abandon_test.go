package codepipeline_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestADV_ABANDON(t *testing.T) {
	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	pack, err := bundled.New("aws.codepipeline", deps)
	if err != nil {
		t.Fatal(err)
	}
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
			"name": "abandon-demo", "roleArn": "arn:aws:iam::000000000000:role/cp",
			"artifactStore": map[string]any{"type": "S3", "location": "b"},
			"stages": []any{map[string]any{"name": "Source", "actions": []any{
				map[string]any{"name": "Src", "actionTypeId": map[string]any{"category": "Source", "owner": "AWS", "provider": "S3", "version": "1"}},
			}}},
		},
	})
	// stop (no abandon)
	s1 := call("StartPipelineExecution", map[string]any{"name": "abandon-demo"})
	id1, _ := s1["pipelineExecutionId"].(string)
	call("StopPipelineExecution", map[string]any{"pipelineName": "abandon-demo", "pipelineExecutionId": id1})
	got := call("GetPipelineExecution", map[string]any{"pipelineName": "abandon-demo", "pipelineExecutionId": id1})
	pe, _ := got["pipelineExecution"].(map[string]any)
	if pe["status"] != "Stopped" {
		t.Fatalf("stop want Stopped %#v", pe)
	}
	// abandon
	s2 := call("StartPipelineExecution", map[string]any{"name": "abandon-demo"})
	id2, _ := s2["pipelineExecutionId"].(string)
	call("StopPipelineExecution", map[string]any{"pipelineName": "abandon-demo", "pipelineExecutionId": id2, "abandon": true})
	got = call("GetPipelineExecution", map[string]any{"pipelineName": "abandon-demo", "pipelineExecutionId": id2})
	pe, _ = got["pipelineExecution"].(map[string]any)
	if pe["status"] != "Abandoned" {
		t.Fatalf("abandon want Abandoned %#v", pe)
	}
}
