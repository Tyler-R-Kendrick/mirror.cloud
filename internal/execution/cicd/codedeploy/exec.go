package codedeploy

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/engine"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/process"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"gopkg.in/yaml.v3"
)

func init() { engine.RegisterAfterInvoke("aws.codedeploy", "CreateDeployment", afterCreate) }

var (
	mu       sync.Mutex
	forced   bool
	destRoot string
)

func Enable()                     { mu.Lock(); forced = true; mu.Unlock() }
func Disable()                    { mu.Lock(); forced = false; mu.Unlock() }
func SetDestinationRoot(d string) { mu.Lock(); destRoot = d; mu.Unlock() }

func enabled() bool {
	mu.Lock()
	defer mu.Unlock()
	return forced || os.Getenv("MIRROR_CICD_EXECUTE") == "1"
}

func afterCreate(ctx context.Context, deps spi.Deps, req *spi.Request, resp *spi.Response) error {
	if !enabled() || deps.Store == nil || resp == nil || resp.Output == nil {
		return nil
	}
	depID, _ := resp.Output["deploymentId"].(string)
	if depID == "" {
		return nil
	}
	return Finalize(ctx, deps, req.Identity, depID, req.Input)
}

type appSpec struct {
	Files []struct {
		Source      string `yaml:"source"`
		Destination string `yaml:"destination"`
	} `yaml:"files"`
	Hooks map[string][]struct {
		Location string `yaml:"location"`
	} `yaml:"hooks"`
}

func Finalize(ctx context.Context, deps spi.Deps, id spi.Identity, depID string, input map[string]any) error {
	scope := deps.Store.Scope(id.Account, id.Region)
	col := scope.Collection("cddep")
	raw, ok, err := col.Get(ctx, depID)
	if err != nil || !ok {
		return err
	}
	var dep map[string]any
	if err := json.Unmarshal(raw, &dep); err != nil {
		return err
	}
	if st, _ := dep["status"].(string); st != "InProgress" {
		return nil
	}
	mu.Lock()
	root := destRoot
	mu.Unlock()
	if root == "" {
		root, err = os.MkdirTemp("", "codedeploy-target-")
		if err != nil {
			return patch(ctx, col, depID, dep, "Failed", err.Error())
		}
		SetDestinationRoot(root)
	}
	specBytes := extractAppSpec(input)
	work, err := os.MkdirTemp("", "codedeploy-rev-")
	if err != nil {
		return patch(ctx, col, depID, dep, "Failed", err.Error())
	}
	defer os.RemoveAll(work)
	if len(specBytes) == 0 {
		_ = os.WriteFile(filepath.Join(root, "index.html"), []byte("deployed-"+depID), 0o644)
		dep["mirrorDestination"] = root
		return patch(ctx, col, depID, dep, "Succeeded", "")
	}
	_ = os.WriteFile(filepath.Join(work, "appspec.yml"), specBytes, 0o644)
	if files, ok := input["mirrorRevisionFiles"].(map[string]any); ok {
		for name, v := range files {
			path, err := safeRel(work, name)
			if err != nil {
				return patch(ctx, col, depID, dep, "Failed", err.Error())
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return patch(ctx, col, depID, dep, "Failed", err.Error())
			}
			if err := os.WriteFile(path, []byte(fmt.Sprint(v)), 0o755); err != nil {
				return patch(ctx, col, depID, dep, "Failed", err.Error())
			}
		}
	}
	var spec appSpec
	if err := yaml.Unmarshal(specBytes, &spec); err != nil {
		return patch(ctx, col, depID, dep, "Failed", err.Error())
	}
	for _, f := range spec.Files {
		src, err := safeRel(work, f.Source)
		if err != nil {
			return patch(ctx, col, depID, dep, "Failed", err.Error())
		}
		dst := root
		if f.Destination != "" && f.Destination != "/" {
			dst, err = safeRel(root, strings.TrimPrefix(f.Destination, "/"))
			if err != nil {
				return patch(ctx, col, depID, dep, "Failed", err.Error())
			}
		}
		if err := copyOne(src, dst); err != nil {
			return patch(ctx, col, depID, dep, "Failed", err.Error())
		}
	}
	for _, hookName := range []string{"BeforeInstall", "AfterInstall", "ApplicationStart", "ValidateService"} {
		for _, h := range spec.Hooks[hookName] {
			loc, err := safeRel(work, h.Location)
			if err != nil {
				return patch(ctx, col, depID, dep, "Failed", err.Error())
			}
			code, err := process.Run(ctx, process.Config{Path: "/bin/sh", Args: []string{loc}, Dir: work, Env: append(process.ScrubEnv(os.Environ()), "DESTINATION="+root)}, nil)
			if err != nil || code != 0 {
				return patch(ctx, col, depID, dep, "Failed", fmt.Sprintf("hook %s exit %d", hookName, code))
			}
		}
	}
	dep["mirrorDestination"] = root
	return patch(ctx, col, depID, dep, "Succeeded", "")
}

func extractAppSpec(input map[string]any) []byte {
	if s, ok := input["mirrorAppSpec"].(string); ok {
		return []byte(s)
	}
	if rev, ok := input["revision"].(map[string]any); ok {
		if s, ok := rev["mirrorAppSpec"].(string); ok {
			return []byte(s)
		}
	}
	return nil
}

func patch(ctx context.Context, col spi.Collection, id string, dep map[string]any, status, reason string) error {
	dep["status"] = status
	if reason != "" {
		dep["errorInformation"] = map[string]any{"message": reason}
	}
	body, err := json.Marshal(dep)
	if err != nil {
		return err
	}
	return col.Put(ctx, id, body)
}

func copyOne(src, dstDir string) error {
	fi, err := os.Stat(src)
	if err != nil {
		return err
	}
	_ = os.MkdirAll(dstDir, 0o755)
	if fi.IsDir() {
		return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			rel, _ := filepath.Rel(src, path)
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			target := filepath.Join(dstDir, rel)
			_ = os.MkdirAll(filepath.Dir(target), 0o755)
			return os.WriteFile(target, b, 0o644)
		})
	}
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dstDir, filepath.Base(src)), b, 0o644)
}

func safeRel(root, name string) (string, error) {
	if name == "" || filepath.IsAbs(name) || strings.Contains(name, "..") {
		return "", fmt.Errorf("codedeploy: bad path %q", name)
	}
	rel := strings.TrimPrefix(filepath.Clean("/"+name), "/")
	path := filepath.Join(root, rel)
	rootClean := filepath.Clean(root)
	if path != rootClean && !strings.HasPrefix(path, rootClean+string(os.PathSeparator)) {
		return "", fmt.Errorf("codedeploy: path escapes root")
	}
	return path, nil
}
