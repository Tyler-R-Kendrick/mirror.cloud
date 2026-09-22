// Package amplify finalizes aws.amplify StartJob when execute is enabled.
package amplify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/buildspec"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	engine.RegisterAfterInvoke("aws.amplify", "StartJob", afterStartJob)
}

var (
	mu     sync.Mutex
	forced bool
)

// Enable forces local job execution.
func Enable() { mu.Lock(); forced = true; mu.Unlock() }

// Disable clears force flag.
func Disable() { mu.Lock(); forced = false; mu.Unlock() }

func enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return forced || os.Getenv("MIRROR_CICD_EXECUTE") == "1"
}

func afterStartJob(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	if !enabled() || deps.Store == nil || resp == nil || resp.Output == nil {
		return nil
	}
	sum, _ := resp.Output["jobSummary"].(map[string]any)
	if sum == nil {
		return nil
	}
	appID, _ := req.Input["appId"].(string)
	jobID, _ := sum["jobId"].(string)
	if jobID == "" {
		jobID, _ = sum["jobID"].(string)
	}
	if appID == "" || jobID == "" {
		return nil
	}
	if err := Finalize(ctx, deps, req.Identity, appID, jobID); err != nil {
		return err
	}
	raw, ok, err := deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("ampjob:"+appID).Get(ctx, jobID)
	if err != nil || !ok {
		return err
	}
	var updated map[string]any
	if err := json.Unmarshal(raw, &updated); err != nil {
		return err
	}
	resp.Output["jobSummary"] = updated
	return nil
}

// Finalize advances a RUNNING job to SUCCEED/FAILED with optional artifact bytes.
func Finalize(ctx context.Context, deps spi.Deps, id spi.Identity, appID, jobID string) error {
	scope := deps.Store.Scope(id.Account, id.Region)
	col := scope.Collection("ampjob:" + appID)
	raw, ok, err := col.Get(ctx, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("amplify: job %s/%s absent", appID, jobID)
	}
	var job map[string]any
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	if st, _ := job["status"].(string); st != "RUNNING" {
		return nil
	}

	dir, err := os.MkdirTemp("", "amplify-")
	if err != nil {
		return patchJob(ctx, col, jobID, job, "FAILED", err.Error(), nil)
	}
	defer os.RemoveAll(dir)

	status := "SUCCEED"
	reason := ""
	var log bytes.Buffer
	specYAML, _ := job["buildSpec"].(string)
	if branch, _ := job["branchName"].(string); branch != "" {
		_ = branch
	}
	if strings.Contains(specYAML, "version: 0.2") || strings.Contains(specYAML, "version:0.2") {
		spec, err := buildspec.Parse([]byte(specYAML))
		if err != nil {
			return patchJob(ctx, col, jobID, job, "FAILED", err.Error(), nil)
		}
		res, err := buildspec.Run(ctx, spec, buildspec.Config{Dir: dir, Output: &log})
		if err != nil {
			return patchJob(ctx, col, jobID, job, "FAILED", err.Error(), log.Bytes())
		}
		if res.ExitCode != 0 {
			status, reason = "FAILED", fmt.Sprintf("exit %d", res.ExitCode)
		}
	} else if strings.TrimSpace(specYAML) != "" {
		for _, line := range strings.Split(specYAML, "\n") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "- "))
			if line == "" || strings.HasPrefix(line, "#") || strings.HasSuffix(line, ":") {
				continue
			}
			code, err := process.Run(ctx, process.Config{Path: "/bin/sh", Args: []string{"-c", line}, Dir: dir}, &log)
			if err != nil || code != 0 {
				status, reason = "FAILED", fmt.Sprintf("%q exit %d", line, code)
				break
			}
		}
	} else {
		_ = os.WriteFile(filepath.Join(dir, "index.html"), []byte("ok-"+jobID), 0o644)
	}

	meta := map[string]any{}
	if status == "SUCCEED" {
		branch, _ := job["branchName"].(string)
		if b, err := os.ReadFile(filepath.Join(dir, "index.html")); err == nil {
			key := "amplify/" + appID + "/" + branch + "/" + jobID + "/index.html"
			cur := "amplify/" + appID + "/" + branch + "/current"
			if deps.Blobs != nil {
				if _, err := deps.Blobs.Put(ctx, key, bytes.NewReader(b)); err == nil {
					meta["mirrorBlobKey"] = key
					_, _ = deps.Blobs.Put(ctx, cur, bytes.NewReader(b))
					meta["mirrorBranchCurrent"] = cur
				}
			}
			meta["body"] = string(b)
			meta["branchName"] = branch
		}
	}
	return patchJob(ctx, col, jobID, job, status, reason, log.Bytes(), meta)
}

func patchJob(ctx context.Context, col spi.Collection, jobID string, job map[string]any, status, reason string, log []byte, meta ...map[string]any) error {
	job["status"] = status
	if reason != "" {
		job["statusReason"] = reason
	}
	if len(log) > 0 {
		ex := string(log)
		if len(ex) > 4096 {
			ex = ex[:4096]
		}
		job["mirrorLogExcerpt"] = ex
	}
	if len(meta) > 0 && meta[0] != nil {
		job["artifacts"] = meta[0]
	}
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return col.Put(ctx, jobID, body)
}
