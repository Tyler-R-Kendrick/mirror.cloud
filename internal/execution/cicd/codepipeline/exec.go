// Package codepipeline advances StartPipelineExecution when execute is enabled.
//
// Boundaries (ponytail): Source/S3 stub-succeeds. Build/CodeBuild invokes
// aws.codebuild StartBuild. Approval/Manual stays Pending and blocks later
// stages until PutApprovalResult (Approved) / Approve continues them.
// Deploy/CodeDeploy invokes aws.codedeploy CreateDeployment (fixture may supply
// mirrorAppSpec / mirrorRevisionFiles on action configuration). Other providers
// stay InProgress — no Lambda/ECS/S3-deploy matrix.
package codepipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codebuild"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/codedeploy"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	engine.RegisterAfterInvoke("aws.codepipeline", "StartPipelineExecution", afterStart)
	engine.RegisterAfterInvoke("aws.codepipeline", "PutApprovalResult", afterApproval)
}

var (
	mu     sync.Mutex
	forced bool
)

// Enable forces pipeline→downstream dispatch regardless of MIRROR_CICD_EXECUTE.
func Enable() { mu.Lock(); forced = true; mu.Unlock() }

// Disable clears the force flag (env still honored).
func Disable() { mu.Lock(); forced = false; mu.Unlock() }

func enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return forced || os.Getenv("MIRROR_CICD_EXECUTE") == "1"
}

func afterStart(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	if !enabled() || deps.Store == nil || resp == nil || resp.Output == nil {
		return nil
	}
	name, _ := req.Input["name"].(string)
	execID, _ := resp.Output["pipelineExecutionId"].(string)
	if name == "" || execID == "" {
		return nil
	}
	// New execution must not inherit a prior run's stageStates snap.
	scope := deps.Store.Scope(req.Identity.Account, req.Identity.Region)
	raw, ok, err := scope.Collection("cp").Get(ctx, name)
	if err != nil || !ok {
		return err
	}
	var pipe map[string]any
	if err := json.Unmarshal(raw, &pipe); err != nil {
		return err
	}
	delete(pipe, "stageStates")
	body, err := json.Marshal(pipe)
	if err != nil {
		return err
	}
	if err := scope.Collection("cp").Put(ctx, name, body); err != nil {
		return err
	}
	return Finalize(ctx, deps, req.Identity, name, execID)
}

func afterApproval(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	if !enabled() || deps.Store == nil || req == nil {
		return nil
	}
	name := str(req.Input["pipelineName"])
	stage := str(req.Input["stageName"])
	action := str(req.Input["actionName"])
	if name == "" || stage == "" || action == "" {
		return nil
	}
	status := "Approved"
	if result, _ := req.Input["result"].(map[string]any); result != nil {
		if s := str(result["status"]); s != "" {
			status = s
		}
	}
	execID := str(req.Input["pipelineExecutionId"])
	if execID == "" {
		scope := deps.Store.Scope(req.Identity.Account, req.Identity.Region)
		raw, ok, err := scope.Collection("cp").Get(ctx, name)
		if err != nil || !ok {
			return err
		}
		var pipe map[string]any
		if err := json.Unmarshal(raw, &pipe); err != nil {
			return err
		}
		execID = str(pipe["latestExecutionId"])
	}
	if execID == "" {
		return nil
	}
	return resolveApproval(ctx, deps, req.Identity, name, execID, stage, action, status)
}

// Approve marks a Pending Manual approval Succeeded and continues downstream stages.
func Approve(ctx context.Context, deps spi.Deps, id spi.Identity, pipelineName, execID, stage, action string) error {
	return resolveApproval(ctx, deps, id, pipelineName, execID, stage, action, "Approved")
}

func resolveApproval(ctx context.Context, deps spi.Deps, id spi.Identity, pipelineName, execID, stage, action, result string) error {
	scope := deps.Store.Scope(id.Account, id.Region)
	raw, ok, err := scope.Collection("cp").Get(ctx, pipelineName)
	if err != nil || !ok {
		return err
	}
	var pipe map[string]any
	if err := json.Unmarshal(raw, &pipe); err != nil {
		return err
	}
	approved := result == "Approved"
	found := false
	for _, rawStage := range asSlice(pipe["stageStates"]) {
		st, _ := rawStage.(map[string]any)
		if st == nil || str(st["stageName"]) != stage {
			continue
		}
		for _, rawAct := range asSlice(st["actionStates"]) {
			as, _ := rawAct.(map[string]any)
			if as == nil || str(as["actionName"]) != action {
				continue
			}
			latest, _ := as["latestExecution"].(map[string]any)
			if latest == nil {
				latest = map[string]any{}
				as["latestExecution"] = latest
			}
			if str(latest["status"]) != "Pending" {
				return nil
			}
			if approved {
				latest["status"] = "Succeeded"
			} else {
				latest["status"] = "Failed"
			}
			found = true
		}
	}
	if !found {
		return nil
	}
	body, err := json.Marshal(pipe)
	if err != nil {
		return err
	}
	if err := scope.Collection("cp").Put(ctx, pipelineName, body); err != nil {
		return err
	}
	if !approved {
		return patchExec(ctx, scope, execID, "Failed")
	}
	return Finalize(ctx, deps, id, pipelineName, execID)
}

// Finalize walks declared stages, dispatches CodeBuild/CodeDeploy, and writes
// terminal execution + stageStates onto the Store records (same collections B-IR uses).
// Succeeded actions from a prior snap are not re-run; Pending approval stops the walk.
func Finalize(ctx context.Context, deps spi.Deps, id spi.Identity, pipelineName, execID string) error {
	scope := deps.Store.Scope(id.Account, id.Region)
	raw, ok, err := scope.Collection("cp").Get(ctx, pipelineName)
	if err != nil || !ok {
		return err
	}
	var pipe map[string]any
	if err := json.Unmarshal(raw, &pipe); err != nil {
		return err
	}

	prior := indexActionStatus(pipe["stageStates"])
	cb := bundled.Handler("aws.codebuild", deps)
	cd := bundled.Handler("aws.codedeploy", deps)
	stageStates := make([]any, 0)
	overall := "Succeeded"
	stop := false

	for _, rawStage := range asSlice(pipe["stages"]) {
		stage, _ := rawStage.(map[string]any)
		if stage == nil {
			continue
		}
		stageName, _ := stage["name"].(string)
		actionStates := make([]any, 0)
		for _, rawAct := range asSlice(stage["actions"]) {
			act, _ := rawAct.(map[string]any)
			if act == nil {
				continue
			}
			actName, _ := act["name"].(string)
			key := stageName + "/" + actName
			var status, extID string
			if stop {
				status = "InProgress"
			} else if st, ok := prior[key]; ok && (st.status == "Succeeded" || st.status == "Failed") {
				status, extID = st.status, st.extID
				if status == "Failed" {
					overall = "Failed"
					stop = true
				}
			} else {
				status, extID = runAction(ctx, deps, cb, cd, id, act)
				switch status {
				case "Failed":
					overall = "Failed"
					stop = true
				case "Pending", "InProgress":
					if overall == "Succeeded" {
						overall = "InProgress"
					}
					stop = true
				}
			}
			latest := map[string]any{
				"status":            status,
				"actionExecutionId": actName,
			}
			if extID != "" {
				latest["externalExecutionId"] = extID
			}
			actionStates = append(actionStates, map[string]any{
				"actionName":      actName,
				"latestExecution": latest,
			})
		}
		stageStates = append(stageStates, map[string]any{
			"stageName":              stageName,
			"inboundTransitionState": map[string]any{"enabled": true},
			"actionStates":           actionStates,
		})
	}

	pipe["stageStates"] = stageStates
	body, err := json.Marshal(pipe)
	if err != nil {
		return err
	}
	if err := scope.Collection("cp").Put(ctx, pipelineName, body); err != nil {
		return err
	}
	return patchExec(ctx, scope, execID, overall)
}

func patchExec(ctx context.Context, scope spi.Scope, execID, overall string) error {
	exRaw, ok, err := scope.Collection("cpex").Get(ctx, execID)
	if err != nil || !ok {
		return err
	}
	var ex map[string]any
	if err := json.Unmarshal(exRaw, &ex); err != nil {
		return err
	}
	if st, _ := ex["status"].(string); st != "InProgress" {
		return nil
	}
	ex["status"] = overall
	exBody, err := json.Marshal(ex)
	if err != nil {
		return err
	}
	return scope.Collection("cpex").Put(ctx, execID, exBody)
}

type priorAct struct{ status, extID string }

func indexActionStatus(v any) map[string]priorAct {
	out := map[string]priorAct{}
	for _, rawStage := range asSlice(v) {
		st, _ := rawStage.(map[string]any)
		if st == nil {
			continue
		}
		stageName := str(st["stageName"])
		for _, rawAct := range asSlice(st["actionStates"]) {
			as, _ := rawAct.(map[string]any)
			if as == nil {
				continue
			}
			latest, _ := as["latestExecution"].(map[string]any)
			if latest == nil {
				continue
			}
			out[stageName+"/"+str(as["actionName"])] = priorAct{
				status: str(latest["status"]),
				extID:  str(latest["externalExecutionId"]),
			}
		}
	}
	return out
}

func runAction(ctx context.Context, deps spi.Deps, cb, cd spi.BehaviorPack, id spi.Identity, act map[string]any) (status, externalID string) {
	tid, _ := act["actionTypeId"].(map[string]any)
	if tid == nil {
		return "InProgress", ""
	}
	cat, _ := tid["category"].(string)
	prov, _ := tid["provider"].(string)
	switch {
	case cat == "Source":
		return "Succeeded", ""
	case cat == "Approval" || prov == "Manual":
		return "Pending", ""
	case cat == "Build" && prov == "CodeBuild":
		cfg, _ := act["configuration"].(map[string]any)
		project := ""
		if cfg != nil {
			project = str(cfg["ProjectName"])
		}
		if project == "" {
			return "Failed", ""
		}
		resp, err := cb.Invoke(ctx, &spi.Request{
			ServiceID: "aws.codebuild",
			Identity:  id,
			Operation: "StartBuild",
			Input:     map[string]any{"projectName": project},
		})
		if err != nil {
			return "Failed", ""
		}
		build, _ := resp.Output["build"].(map[string]any)
		if build == nil {
			return "Failed", ""
		}
		bid := str(build["id"])
		switch str(build["buildStatus"]) {
		case "SUCCEEDED":
			return "Succeeded", bid
		case "FAILED", "FAULT", "STOPPED", "TIMED_OUT":
			return "Failed", bid
		default:
			// ADV-EXEC-OFF on codebuild: build still IN_PROGRESS — do not claim pipeline success.
			return "InProgress", bid
		}
	case cat == "Deploy" && prov == "CodeDeploy":
		cfg, _ := act["configuration"].(map[string]any)
		if cfg == nil {
			return "Failed", ""
		}
		app, dg := str(cfg["ApplicationName"]), str(cfg["DeploymentGroupName"])
		if app == "" || dg == "" {
			return "Failed", ""
		}
		input := map[string]any{
			"applicationName":     app,
			"deploymentGroupName": dg,
		}
		if spec := str(cfg["mirrorAppSpec"]); spec != "" {
			input["mirrorAppSpec"] = spec
			input["revision"] = map[string]any{"revisionType": "S3", "mirrorAppSpec": spec}
		}
		if files, ok := cfg["mirrorRevisionFiles"].(map[string]any); ok {
			input["mirrorRevisionFiles"] = files
		}
		resp, err := cd.Invoke(ctx, &spi.Request{
			ServiceID: "aws.codedeploy",
			Identity:  id,
			Operation: "CreateDeployment",
			Input:     input,
		})
		if err != nil {
			return "Failed", ""
		}
		depID := str(resp.Output["deploymentId"])
		// AfterInvoke on CreateDeployment may already have terminalized; re-read.
		scope := deps.Store.Scope(id.Account, id.Region)
		if raw, ok, err := scope.Collection("cddep").Get(ctx, depID); err == nil && ok {
			var dep map[string]any
			if json.Unmarshal(raw, &dep) == nil {
				switch str(dep["status"]) {
				case "Succeeded":
					return "Succeeded", depID
				case "Failed", "Stopped":
					return "Failed", depID
				}
			}
		}
		return "InProgress", depID
	default:
		return "InProgress", ""
	}
}

func asSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	default:
		return nil
	}
}

func str(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}
