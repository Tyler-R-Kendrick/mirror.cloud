package awsdelivery_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	cb "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	cd "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codedeploy"
	cp "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codepipeline"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// AWS-PIPE-APPROVAL-DEPLOY: Source → Build → Approval → Deploy composite.
// Execute-on stops Pending on Manual approval; PutApprovalResult(Approved)
// continues CodeDeploy; Rejected leaves destination unchanged.
func TestAWS_PIPE_APPROVAL_DEPLOY(t *testing.T) {
	cb.Enable()
	cd.Enable()
	cp.Enable()
	t.Cleanup(func() { cb.Disable(); cd.Disable(); cp.Disable() })

	deps := spitest.Deps(t)
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	ctx := context.Background()
	nonce := "pipe-approve-" + t.Name()

	cbPack, err := bundled.New("aws.codebuild", deps)
	if err != nil {
		t.Fatal(err)
	}
	cdPack, err := bundled.New("aws.codedeploy", deps)
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

	prodRoot := t.TempDir()
	cd.SetDestinationRoot(prodRoot)

	buildspec := "version: 0.2\nphases:\n  build:\n    commands:\n      - echo -n '" + nonce + "' > artifact.txt\n"
	call(cbPack, "aws.codebuild", "CreateProject", map[string]any{
		"name": "pipe-appr-build", "serviceRole": "arn:aws:iam::000000000000:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": buildspec},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
	})
	call(cdPack, "aws.codedeploy", "CreateApplication", map[string]any{"applicationName": "pipe-app"})
	call(cdPack, "aws.codedeploy", "CreateDeploymentGroup", map[string]any{
		"applicationName": "pipe-app", "deploymentGroupName": "pipe-dg",
		"serviceRoleArn": "arn:aws:iam::000000000000:role/cd",
	})

	appSpec := "version: 0.0\nos: linux\nfiles:\n  - source: artifact.txt\n    destination: /\nhooks:\n  ApplicationStart:\n    - location: start.sh\n"
	call(cpPack, "aws.codepipeline", "CreatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name": "pipe-appr", "roleArn": "arn:aws:iam::000000000000:role/cp",
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
							"configuration": map[string]any{"ProjectName": "pipe-appr-build"},
						},
					},
				},
				map[string]any{
					"name": "Approve",
					"actions": []any{
						map[string]any{
							"name": "Manual",
							"actionTypeId": map[string]any{
								"category": "Approval", "owner": "AWS", "provider": "Manual", "version": "1",
							},
						},
					},
				},
				map[string]any{
					"name": "Deploy",
					"actions": []any{
						map[string]any{
							"name": "Cd",
							"actionTypeId": map[string]any{
								"category": "Deploy", "owner": "AWS", "provider": "CodeDeploy", "version": "1",
							},
							"configuration": map[string]any{
								"ApplicationName":     "pipe-app",
								"DeploymentGroupName": "pipe-dg",
								"mirrorAppSpec":       appSpec,
								"mirrorRevisionFiles": map[string]any{
									"artifact.txt": nonce,
									"start.sh":     "#!/bin/sh\necho started > \"$DESTINATION/started\"\n",
								},
							},
						},
					},
				},
			},
		},
	})

	started := call(cpPack, "aws.codepipeline", "StartPipelineExecution", map[string]any{"name": "pipe-appr"})
	execID, _ := started["pipelineExecutionId"].(string)
	got := call(cpPack, "aws.codepipeline", "GetPipelineExecution", map[string]any{
		"pipelineName": "pipe-appr", "pipelineExecutionId": execID,
	})
	pe, _ := got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "InProgress" {
		t.Fatalf("want Pending-approval InProgress, got %#v", pe)
	}
	state := call(cpPack, "aws.codepipeline", "GetPipelineState", map[string]any{"name": "pipe-appr"})
	if !approvalPending(state) {
		t.Fatalf("want Manual Pending, got %#v", state["stageStates"])
	}
	if _, err := os.Stat(filepath.Join(prodRoot, "artifact.txt")); !os.IsNotExist(err) {
		t.Fatalf("dest advanced before approval: %v", err)
	}

	call(cpPack, "aws.codepipeline", "PutApprovalResult", map[string]any{
		"pipelineName": "pipe-appr", "stageName": "Approve", "actionName": "Manual",
		"token":  "unused",
		"result": map[string]any{"status": "Approved"},
	})
	got = call(cpPack, "aws.codepipeline", "GetPipelineExecution", map[string]any{
		"pipelineName": "pipe-appr", "pipelineExecutionId": execID,
	})
	pe, _ = got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "Succeeded" {
		t.Fatalf("after approve want Succeeded, got %#v", pe)
	}
	deployed, err := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	if err != nil || string(deployed) != nonce {
		t.Fatalf("deployed %q err %v want %q", deployed, err, nonce)
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

	// Reject variant: new execution, Rejected leaves dest unchanged.
	before, _ := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	rejNonce := nonce + "-reject"
	call(cpPack, "aws.codepipeline", "UpdatePipeline", map[string]any{
		"pipeline": map[string]any{
			"name": "pipe-appr", "roleArn": "arn:aws:iam::000000000000:role/cp",
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
							"configuration": map[string]any{"ProjectName": "pipe-appr-build"},
						},
					},
				},
				map[string]any{
					"name": "Approve",
					"actions": []any{
						map[string]any{
							"name": "Manual",
							"actionTypeId": map[string]any{
								"category": "Approval", "owner": "AWS", "provider": "Manual", "version": "1",
							},
						},
					},
				},
				map[string]any{
					"name": "Deploy",
					"actions": []any{
						map[string]any{
							"name": "Cd",
							"actionTypeId": map[string]any{
								"category": "Deploy", "owner": "AWS", "provider": "CodeDeploy", "version": "1",
							},
							"configuration": map[string]any{
								"ApplicationName":     "pipe-app",
								"DeploymentGroupName": "pipe-dg",
								"mirrorAppSpec":       appSpec,
								"mirrorRevisionFiles": map[string]any{
									"artifact.txt": rejNonce,
									"start.sh":     "#!/bin/sh\necho started > \"$DESTINATION/started\"\n",
								},
							},
						},
					},
				},
			},
		},
	})
	rej := call(cpPack, "aws.codepipeline", "StartPipelineExecution", map[string]any{"name": "pipe-appr"})
	rejID, _ := rej["pipelineExecutionId"].(string)
	call(cpPack, "aws.codepipeline", "PutApprovalResult", map[string]any{
		"pipelineName": "pipe-appr", "stageName": "Approve", "actionName": "Manual",
		"token":  "unused",
		"result": map[string]any{"status": "Rejected"},
	})
	got = call(cpPack, "aws.codepipeline", "GetPipelineExecution", map[string]any{
		"pipelineName": "pipe-appr", "pipelineExecutionId": rejID,
	})
	pe, _ = got["pipelineExecution"].(map[string]any)
	if status, _ := pe["status"].(string); status != "Failed" {
		t.Fatalf("reject want Failed, got %#v", pe)
	}
	after, err := os.ReadFile(filepath.Join(prodRoot, "artifact.txt"))
	if err != nil || string(after) != string(before) {
		t.Fatalf("dest changed on reject: before=%q after=%q err=%v", before, after, err)
	}
}

func approvalPending(state map[string]any) bool {
	for _, raw := range asAnySlice(state["stageStates"]) {
		st, _ := raw.(map[string]any)
		if st == nil || str(st["stageName"]) != "Approve" {
			continue
		}
		for _, rawAct := range asAnySlice(st["actionStates"]) {
			as, _ := rawAct.(map[string]any)
			if as == nil || str(as["actionName"]) != "Manual" {
				continue
			}
			latest, _ := as["latestExecution"].(map[string]any)
			return latest != nil && str(latest["status"]) == "Pending"
		}
	}
	return false
}

func asAnySlice(v any) []any {
	s, _ := v.([]any)
	return s
}
