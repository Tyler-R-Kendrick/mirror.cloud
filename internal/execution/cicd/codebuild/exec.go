// Package codebuild executes admitted CodeBuild builds via buildspec 0.2.
//
// Register with a blank import. StartBuild AfterHook runs only when
// MIRROR_CICD_EXECUTE=1 (or Enable() was called). Otherwise builds stay
// IN_PROGRESS — never silent SUCCEEDED.
package codebuild

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/buildspec"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	engine.RegisterAfterInvoke("aws.codebuild", "StartBuild", afterStartBuild)
}

var (
	mu       sync.Mutex
	forced   bool
	workRoot string // optional override for tests
)

// Enable forces process execution regardless of MIRROR_CICD_EXECUTE.
func Enable() { mu.Lock(); forced = true; mu.Unlock() }

// Disable clears the force flag (env still honored).
func Disable() { mu.Lock(); forced = false; mu.Unlock() }

// SetWorkRoot overrides the temp parent for build workspaces (tests).
func SetWorkRoot(dir string) { mu.Lock(); workRoot = dir; mu.Unlock() }

func enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	if forced {
		return true
	}
	return os.Getenv("MIRROR_CICD_EXECUTE") == "1"
}

func afterStartBuild(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	if !enabled() {
		return nil
	}
	if deps.Store == nil || resp == nil || resp.Output == nil {
		return nil
	}
	build, _ := resp.Output["build"].(map[string]any)
	if build == nil {
		return nil
	}
	id, _ := build["id"].(string)
	if id == "" {
		return nil
	}
	if err := Finalize(ctx, deps, req.Identity, id); err != nil {
		return err
	}
	// Refresh the wire response from the terminal store record.
	raw, ok, err := deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("cbbuild").Get(ctx, id)
	if err != nil || !ok {
		return err
	}
	var updated map[string]any
	if err := json.Unmarshal(raw, &updated); err != nil {
		return err
	}
	resp.Output["build"] = updated
	return nil
}

// Finalize runs the buildspec for an IN_PROGRESS build and writes the terminal record.
func Finalize(ctx context.Context, deps spi.Deps, id spi.Identity, buildID string) error {
	scope := deps.Store.Scope(id.Account, id.Region)
	raw, ok, err := scope.Collection("cbbuild").Get(ctx, buildID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("codebuild: build %q absent", buildID)
	}
	var build map[string]any
	if err := json.Unmarshal(raw, &build); err != nil {
		return err
	}
	if status, _ := build["buildStatus"].(string); status != "IN_PROGRESS" {
		return nil
	}

	now := "1970-01-01T00:00:00Z"
	if deps.Clock != nil {
		now = deps.Clock.Now().UTC().Format(time.RFC3339Nano)
	}

	specYAML, err := resolveBuildspec(build)
	if err != nil {
		return patchTerminal(ctx, scope, buildID, build, "FAILED", err.Error(), now, nil)
	}
	if specYAML == "" {
		return patchTerminal(ctx, scope, buildID, build, "FAILED", "missing buildspec", now, nil)
	}
	spec, err := buildspec.Parse([]byte(specYAML))
	if err != nil {
		return patchTerminal(ctx, scope, buildID, build, "FAILED", err.Error(), now, nil)
	}

	mu.Lock()
	root := workRoot
	mu.Unlock()
	if root == "" {
		root = os.TempDir()
	}
	dir, err := os.MkdirTemp(root, "cb-"+sanitize(buildID)+"-")
	if err != nil {
		return patchTerminal(ctx, scope, buildID, build, "FAILED", err.Error(), now, nil)
	}
	defer os.RemoveAll(dir)

	var logBuf bytes.Buffer
	result, err := buildspec.Run(ctx, spec, buildspec.Config{
		Dir:    dir,
		Env:    envFrom(build),
		Output: &logBuf,
	})
	if err != nil {
		return patchTerminal(ctx, scope, buildID, build, "FAILED", err.Error(), now, logBuf.Bytes())
	}
	status := "SUCCEEDED"
	msg := ""
	if result.ExitCode != 0 {
		status = "FAILED"
		msg = fmt.Sprintf("exit %d", result.ExitCode)
	}

	arts, artErr := publishArtifacts(ctx, deps, id, build, dir)
	if artErr != nil && status == "SUCCEEDED" {
		status = "FAILED"
		msg = artErr.Error()
	}
	return patchTerminal(ctx, scope, buildID, build, status, msg, now, logBuf.Bytes(), arts)
}

func resolveBuildspec(build map[string]any) (string, error) {
	if s, _ := build["buildspecOverride"].(string); strings.TrimSpace(s) != "" {
		return s, nil
	}
	src, _ := build["source"].(map[string]any)
	if src == nil {
		return "", nil
	}
	if s, _ := src["buildspec"].(string); strings.TrimSpace(s) != "" {
		return s, nil
	}
	return "", nil
}

func envFrom(build map[string]any) map[string]string {
	out := map[string]string{
		"CODEBUILD_BUILD_ID": fmt.Sprint(build["id"]),
	}
	env, _ := build["environment"].(map[string]any)
	if env == nil {
		return out
	}
	for _, raw := range asSlice(env["environmentVariables"]) {
		m, _ := raw.(map[string]any)
		if m == nil {
			continue
		}
		name, _ := m["name"].(string)
		val, _ := m["value"].(string)
		if name != "" {
			out[name] = val
		}
	}
	return out
}

func asSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	default:
		return nil
	}
}

func patchTerminal(ctx context.Context, scope spi.Scope, buildID string, build map[string]any, status, msg, now string, log []byte, arts ...map[string]any) error {
	if now == "" {
		now = "1970-01-01T00:00:00Z"
	}
	build["buildStatus"] = status
	build["currentPhase"] = "COMPLETED"
	build["endTime"] = now
	if msg != "" {
		build["buildStatusReason"] = msg
	}
	phases, _ := build["phases"].([]any)
	// Close BUILD phase.
	closed := make([]any, 0, len(phases)+1)
	for _, p := range phases {
		m, _ := p.(map[string]any)
		if m == nil {
			continue
		}
		if fmt.Sprint(m["phaseType"]) == "BUILD" && fmt.Sprint(m["phaseStatus"]) == "IN_PROGRESS" {
			m = cloneMap(m)
			m["phaseStatus"] = status
			m["endTime"] = now
			if msg != "" {
				m["contexts"] = []any{map[string]any{"message": msg}}
			}
		}
		closed = append(closed, m)
	}
	closed = append(closed, map[string]any{
		"phaseType": "COMPLETED", "phaseStatus": status, "startTime": now, "endTime": now,
	})
	build["phases"] = closed
	if len(log) > 0 {
		build["logs"] = map[string]any{
			"groupName":  "codebuild",
			"streamName": buildID,
			"deepLink":   "local://" + buildID,
		}
		// Keep a bounded log excerpt on the record for tests (not a secret sink).
		excerpt := string(log)
		if len(excerpt) > 4096 {
			excerpt = excerpt[:4096]
		}
		build["mirrorLogExcerpt"] = excerpt
	}
	if len(arts) > 0 && arts[0] != nil {
		build["artifacts"] = arts[0]
	}
	body, err := json.Marshal(build)
	if err != nil {
		return err
	}
	return scope.Collection("cbbuild").Put(ctx, buildID, body)
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func sanitize(id string) string {
	return strings.Map(func(r rune) rune {
		if r == ':' || r == '/' {
			return '-'
		}
		return r
	}, id)
}

// publishArtifacts copies declared files into BlobStore and returns artifact metadata.
// S3 upload is best-effort when artifacts.type is S3 and aws.s3 is reachable.
func publishArtifacts(ctx context.Context, deps spi.Deps, id spi.Identity, build map[string]any, dir string) (map[string]any, error) {
	arts, _ := build["artifacts"].(map[string]any)
	if arts == nil {
		return nil, nil
	}
	typ, _ := arts["type"].(string)
	if typ == "" || typ == "NO_ARTIFACTS" {
		return map[string]any{"location": ""}, nil
	}
	// Collect files: default out/** and top-level non-hidden files written by the build.
	var paths []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || strings.HasPrefix(rel, ".") {
			return nil
		}
		paths = append(paths, rel)
		return nil
	})
	if len(paths) == 0 {
		return nil, fmt.Errorf("no artifact files produced")
	}
	// Concatenate into one blob for primary artifact (bounded profile).
	var buf bytes.Buffer
	for _, rel := range paths {
		b, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	key := "codebuild/artifacts/" + fmt.Sprint(build["id"])
	sum := sha256.Sum256(buf.Bytes())
	digest := hex.EncodeToString(sum[:])
	if deps.Blobs != nil {
		if _, err := deps.Blobs.Put(ctx, key, bytes.NewReader(buf.Bytes())); err != nil {
			return nil, err
		}
	}
	loc, _ := arts["location"].(string)
	if loc == "" {
		loc = key
	}
	meta := map[string]any{
		"location":      loc,
		"sha256sum":     digest,
		"size":          buf.Len(),
		"mirrorBlobKey": key,
	}
	_ = id
	return meta, nil
}
