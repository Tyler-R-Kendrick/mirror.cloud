// Package lambda stores functions and invokes local python/node handlers.
package lambda

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.lambda", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Lambda-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.lambda" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateFunction", "GetFunction", "ListFunctions", "DeleteFunction", "Invoke",
		"UpdateFunctionCode", "UpdateFunctionConfiguration", "GetFunctionConfiguration",
		"PublishVersion", "ListVersionsByFunction",
		"CreateAlias", "GetAlias", "UpdateAlias", "DeleteAlias", "ListAliases",
		"AddPermission", "RemovePermission", "GetPolicy",
		"TagResource", "UntagResource", "ListTags",
		"PutFunctionConcurrency", "GetFunctionConcurrency", "DeleteFunctionConcurrency",
		"CreateEventSourceMapping", "GetEventSourceMapping", "ListEventSourceMappings",
		"UpdateEventSourceMapping", "DeleteEventSourceMapping",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("lambda")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil {
		req.Operation = route(req)
		if req.Input == nil {
			req.Input = map[string]any{}
		}
		if name := functionName(req); name != "" && str(req.Input["FunctionName"]) == "" {
			req.Input["FunctionName"] = name
		}
		parts := strings.Split(req.HTTP.URL.Path, "/")
		for i, p := range parts {
			if p == "aliases" && i+1 < len(parts) && first(req.Input, "Name") == "" {
				req.Input["Name"] = parts[i+1]
			}
			if p == "event-source-mappings" && i+1 < len(parts) && first(req.Input, "UUID") == "" {
				req.Input["UUID"] = parts[i+1]
			}
			if p == "tags" && i+1 < len(parts) && first(req.Input, "Resource", "ResourceArn") == "" {
				req.Input["Resource"] = strings.TrimPrefix(req.HTTP.URL.Path, "/2015-03-31/tags/")
			}
		}
	}
	switch req.Operation {
	case "CreateFunction":
		name := str(req.Input["FunctionName"])
		rec := map[string]any{
			"FunctionName": name,
			"Runtime":      str(req.Input["Runtime"]),
			"Handler":      str(req.Input["Handler"]),
			"Code":         req.Input["Code"],
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, name, b)
		arn := "arn:aws:lambda:" + req.Identity.Region + ":" + req.Identity.Account + ":function:" + name
		return &spi.Response{Output: map[string]any{"FunctionName": name, "FunctionArn": arn, "Runtime": rec["Runtime"], "Handler": rec["Handler"]}}, nil
	case "GetFunction":
		name := str(req.Input["FunctionName"])
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		delete(rec, "Code")
		return &spi.Response{Output: map[string]any{"Configuration": rec}}, nil
	case "ListFunctions":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var fns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			delete(rec, "Code")
			fns = append(fns, rec)
		}
		return &spi.Response{Output: map[string]any{"Functions": fns}}, nil
	case "DeleteFunction":
		_ = p.col(req).Delete(ctx, str(req.Input["FunctionName"]))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "Invoke":
		return p.invoke(ctx, req)
	case "UpdateFunctionCode":
		name := str(req.Input["FunctionName"])
		rec, err := p.loadFn(ctx, req, name)
		if err != nil {
			return nil, err
		}
		if c, ok := req.Input["Code"]; ok {
			rec["Code"] = c
		} else if z, ok := req.Input["ZipFile"]; ok {
			rec["Code"] = map[string]any{"ZipFile": z}
		}
		p.saveFn(ctx, req, name, rec)
		return fnConfig(req, rec), nil
	case "UpdateFunctionConfiguration":
		name := str(req.Input["FunctionName"])
		rec, err := p.loadFn(ctx, req, name)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"Runtime", "Handler", "Timeout", "MemorySize", "Description", "Role", "Environment"} {
			if v, ok := req.Input[k]; ok {
				rec[k] = v
			}
		}
		p.saveFn(ctx, req, name, rec)
		return fnConfig(req, rec), nil
	case "GetFunctionConfiguration":
		name := str(req.Input["FunctionName"])
		rec, err := p.loadFn(ctx, req, name)
		if err != nil {
			return nil, err
		}
		delete(rec, "Code")
		return fnConfig(req, rec), nil
	case "PublishVersion":
		name := str(req.Input["FunctionName"])
		rec, err := p.loadFn(ctx, req, name)
		if err != nil {
			return nil, err
		}
		n := 1
		if v, ok := rec["VersionN"].(float64); ok {
			n = int(v) + 1
		}
		rec["VersionN"] = n
		p.saveFn(ctx, req, name, rec)
		ver := map[string]any{"FunctionName": name, "Version": strconv.Itoa(n), "FunctionArn": fnARN(req, name) + ":" + strconv.Itoa(n)}
		b, _ := json.Marshal(ver)
		_ = p.col(req).Put(ctx, "ver:"+name+":"+strconv.Itoa(n), b)
		return &spi.Response{Output: ver}, nil
	case "ListVersionsByFunction":
		name := str(req.Input["FunctionName"])
		kvs, _, _ := p.col(req).List(ctx, "ver:"+name+":", "", 0)
		var vers []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			vers = append(vers, rec)
		}
		return &spi.Response{Output: map[string]any{"Versions": vers}}, nil
	case "CreateAlias":
		name := str(req.Input["FunctionName"])
		alias := first(req.Input, "Name")
		rec := map[string]any{"AliasArn": fnARN(req, name) + ":" + alias, "Name": alias, "FunctionVersion": first(req.Input, "FunctionVersion"), "Description": first(req.Input, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "alias:"+name+":"+alias, b)
		return &spi.Response{Output: rec}, nil
	case "GetAlias":
		name, alias := str(req.Input["FunctionName"]), first(req.Input, "Name")
		b, ok, _ := p.col(req).Get(ctx, "alias:"+name+":"+alias)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "UpdateAlias":
		name, alias := str(req.Input["FunctionName"]), first(req.Input, "Name")
		b, ok, _ := p.col(req).Get(ctx, "alias:"+name+":"+alias)
		rec := map[string]any{"Name": alias, "FunctionName": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if v := first(req.Input, "FunctionVersion"); v != "" {
			rec["FunctionVersion"] = v
		}
		if v := first(req.Input, "Description"); v != "" {
			rec["Description"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "alias:"+name+":"+alias, nb)
		return &spi.Response{Output: rec}, nil
	case "DeleteAlias":
		_ = p.col(req).Delete(ctx, "alias:"+str(req.Input["FunctionName"])+":"+first(req.Input, "Name"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "ListAliases":
		name := str(req.Input["FunctionName"])
		kvs, _, _ := p.col(req).List(ctx, "alias:"+name+":", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Aliases": items}}, nil
	case "AddPermission":
		name := str(req.Input["FunctionName"])
		sid := first(req.Input, "StatementId")
		stmt := map[string]any{"Sid": sid, "Effect": "Allow", "Action": first(req.Input, "Action"), "Principal": first(req.Input, "Principal")}
		b, _ := json.Marshal(stmt)
		_ = p.col(req).Put(ctx, "perm:"+name+":"+sid, b)
		raw, _ := json.Marshal(map[string]any{"Statement": stmt})
		return &spi.Response{Output: map[string]any{"Statement": string(raw)}}, nil
	case "RemovePermission":
		_ = p.col(req).Delete(ctx, "perm:"+str(req.Input["FunctionName"])+":"+first(req.Input, "StatementId"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "GetPolicy":
		name := str(req.Input["FunctionName"])
		kvs, _, _ := p.col(req).List(ctx, "perm:"+name+":", "", 0)
		var stmts []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			stmts = append(stmts, rec)
		}
		raw, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": stmts})
		return &spi.Response{Output: map[string]any{"Policy": string(raw)}}, nil
	case "TagResource":
		arn := first(req.Input, "Resource", "ResourceArn")
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req).Put(ctx, "tags:"+arn, b)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req).Delete(ctx, "tags:"+first(req.Input, "Resource", "ResourceArn"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "ListTags":
		arn := first(req.Input, "Resource", "ResourceArn")
		b, ok, _ := p.col(req).Get(ctx, "tags:"+arn)
		tags := any(map[string]any{})
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "PutFunctionConcurrency":
		name := str(req.Input["FunctionName"])
		n := req.Input["ReservedConcurrentExecutions"]
		b, _ := json.Marshal(map[string]any{"ReservedConcurrentExecutions": n})
		_ = p.col(req).Put(ctx, "conc:"+name, b)
		return &spi.Response{Output: map[string]any{"ReservedConcurrentExecutions": n}}, nil
	case "GetFunctionConcurrency":
		name := str(req.Input["FunctionName"])
		b, ok, _ := p.col(req).Get(ctx, "conc:"+name)
		if !ok {
			return &spi.Response{Output: map[string]any{"ReservedConcurrentExecutions": 0}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteFunctionConcurrency":
		_ = p.col(req).Delete(ctx, "conc:"+str(req.Input["FunctionName"]))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "CreateEventSourceMapping":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{
			"UUID": id, "FunctionName": first(req.Input, "FunctionName"),
			"EventSourceArn": first(req.Input, "EventSourceArn"), "State": "Enabled",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "esm:"+id, b)
		return &spi.Response{Output: rec}, nil
	case "GetEventSourceMapping":
		id := first(req.Input, "UUID")
		b, ok, _ := p.col(req).Get(ctx, "esm:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListEventSourceMappings":
		kvs, _, _ := p.col(req).List(ctx, "esm:", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"EventSourceMappings": items}}, nil
	case "UpdateEventSourceMapping":
		id := first(req.Input, "UUID")
		b, ok, _ := p.col(req).Get(ctx, "esm:"+id)
		rec := map[string]any{"UUID": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		for k, v := range req.Input {
			if k != "UUID" {
				rec[k] = v
			}
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, "esm:"+id, nb)
		return &spi.Response{Output: rec}, nil
	case "DeleteEventSourceMapping":
		_ = p.col(req).Delete(ctx, "esm:"+first(req.Input, "UUID"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["FunctionName"])
	b, ok, _ := p.col(req).Get(ctx, name)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	invocationType := str(req.Input["InvocationType"])
	if invocationType == "DryRun" {
		return &spi.Response{Status: http.StatusNoContent, Output: map[string]any{"StatusCode": http.StatusNoContent}}, nil
	}
	var payload []byte
	if req.Body != nil {
		var err error
		payload, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	} else {
		ev := map[string]any{}
		for k, v := range req.Input {
			if k == "FunctionName" || k == "InvocationType" || k == "LogType" || k == "Qualifier" {
				continue
			}
			ev[k] = v
		}
		payload, _ = json.Marshal(ev)
		if len(ev) == 0 {
			payload = []byte("{}")
		}
	}
	out, err := runHandler(str(rec["Runtime"]), str(rec["Handler"]), rec["Code"], payload)
	if invocationType == "Event" {
		return &spi.Response{Status: http.StatusAccepted, Output: map[string]any{"StatusCode": http.StatusAccepted}}, nil
	}
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"StatusCode": 200, "Payload": json.RawMessage(out)}}, nil
}

func runHandler(runtime, handler string, code any, payload []byte) ([]byte, error) {
	dir, err := os.MkdirTemp("", "mirror-lambda-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if err := writeCode(dir, code); err != nil {
		return nil, err
	}
	rt := strings.ToLower(runtime)
	switch {
	case strings.Contains(rt, "python"):
		bin, err := exec.LookPath("python3")
		if err != nil {
			return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
		}
		mod, fn := splitHandler(handler)
		script := "import json,sys,importlib; m=importlib.import_module('" + mod + "'); ev=json.load(sys.stdin); print(json.dumps(getattr(m,'" + fn + "')(ev,None)))"
		cmd := exec.Command(bin, "-c", script)
		cmd.Dir = dir
		cmd.Stdin = bytes.NewReader(payload)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, &spi.Fault{Code: "Unhandled", Message: string(out) + err.Error(), HTTPStatus: 200, Fault: "server"}
		}
		return bytes.TrimSpace(out), nil
	case strings.Contains(rt, "node"):
		bin, err := exec.LookPath("node")
		if err != nil {
			return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
		}
		mod, fn := splitHandler(handler)
		script := "const m=require('./" + mod + "'); let d=''; process.stdin.on('data',c=>d+=c); process.stdin.on('end',()=>{Promise.resolve(m." + fn + "(JSON.parse(d||'{}'))).then(r=>process.stdout.write(JSON.stringify(r)))})"
		cmd := exec.Command(bin, "-e", script)
		cmd.Dir = dir
		cmd.Stdin = bytes.NewReader(payload)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, &spi.Fault{Code: "Unhandled", Message: string(out), HTTPStatus: 200, Fault: "server"}
		}
		return bytes.TrimSpace(out), nil
	default:
		return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
	}
}

func writeCode(dir string, code any) error {
	raw := codeBytes(code)
	if zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw))); err == nil {
		for _, f := range zr.File {
			if f.FileInfo().IsDir() {
				continue
			}
			rc, err := f.Open()
			if err != nil {
				return err
			}
			b, _ := io.ReadAll(rc)
			_ = rc.Close()
			dst := filepath.Join(dir, f.Name)
			_ = os.MkdirAll(filepath.Dir(dst), 0o755)
			if err := os.WriteFile(dst, b, 0o644); err != nil {
				return err
			}
		}
		return nil
	}
	return os.WriteFile(filepath.Join(dir, "lambda_function.py"), raw, 0o644)
}

func codeBytes(code any) []byte {
	m, _ := code.(map[string]any)
	v := m["ZipFile"]
	switch t := v.(type) {
	case string:
		if b, err := base64.StdEncoding.DecodeString(t); err == nil {
			return b
		}
		return []byte(t)
	case []byte:
		return t
	}
	return nil
}

func splitHandler(h string) (mod, fn string) {
	mod, fn = "lambda_function", "lambda_handler"
	if i := strings.LastIndex(h, "."); i >= 0 {
		return h[:i], h[i+1:]
	}
	if h != "" {
		fn = h
	}
	return mod, fn
}

func route(req *spi.Request) string {
	if req.HTTP == nil {
		return req.Operation
	}
	if a := req.HTTP.URL.Query().Get("Action"); a != "" {
		return a
	}
	path, m := req.HTTP.URL.Path, req.HTTP.Method
	switch {
	case strings.Contains(path, "/invocations"):
		return "Invoke"
	case strings.Contains(path, "/event-source-mappings"):
		hasID := strings.Count(strings.Trim(path, "/"), "/") >= 2
		switch m {
		case http.MethodPost:
			return "CreateEventSourceMapping"
		case http.MethodPut:
			return "UpdateEventSourceMapping"
		case http.MethodDelete:
			return "DeleteEventSourceMapping"
		case http.MethodGet:
			if hasID && !strings.HasSuffix(path, "/event-source-mappings") {
				return "GetEventSourceMapping"
			}
			return "ListEventSourceMappings"
		}
	case strings.Contains(path, "/tags"):
		switch m {
		case http.MethodPost:
			return "TagResource"
		case http.MethodDelete:
			return "UntagResource"
		default:
			return "ListTags"
		}
	case strings.Contains(path, "/code") && m == http.MethodPut:
		return "UpdateFunctionCode"
	case strings.Contains(path, "/configuration") && m == http.MethodPut:
		return "UpdateFunctionConfiguration"
	case strings.Contains(path, "/configuration") && m == http.MethodGet:
		return "GetFunctionConfiguration"
	case strings.Contains(path, "/versions") && m == http.MethodPost:
		return "PublishVersion"
	case strings.Contains(path, "/versions"):
		return "ListVersionsByFunction"
	case strings.Contains(path, "/aliases"):
		named := strings.Count(path, "/") > strings.Count("/2015-03-31/functions/x/aliases", "/")-1 && !strings.HasSuffix(path, "/aliases")
		switch m {
		case http.MethodPost:
			return "CreateAlias"
		case http.MethodPut:
			return "UpdateAlias"
		case http.MethodDelete:
			return "DeleteAlias"
		case http.MethodGet:
			if named {
				return "GetAlias"
			}
			return "ListAliases"
		}
	case strings.Contains(path, "/policy"):
		switch m {
		case http.MethodPost:
			return "AddPermission"
		case http.MethodDelete:
			return "RemovePermission"
		default:
			return "GetPolicy"
		}
	case strings.Contains(path, "/concurrency"):
		switch m {
		case http.MethodPut:
			return "PutFunctionConcurrency"
		case http.MethodDelete:
			return "DeleteFunctionConcurrency"
		default:
			return "GetFunctionConcurrency"
		}
	case m == http.MethodGet && strings.Contains(path, "/functions/") && !strings.HasSuffix(path, "/functions"):
		return "GetFunction"
	case m == http.MethodGet:
		return "ListFunctions"
	case m == http.MethodDelete:
		return "DeleteFunction"
	default:
		return "CreateFunction"
	}
	return "CreateFunction"
}

func (p *Pack) loadFn(ctx context.Context, req *spi.Request, name string) (map[string]any, error) {
	b, ok, _ := p.col(req).Get(ctx, name)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, nil
}

func (p *Pack) saveFn(ctx context.Context, req *spi.Request, name string, rec map[string]any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req).Put(ctx, name, b)
}

func fnARN(req *spi.Request, name string) string {
	return "arn:aws:lambda:" + req.Identity.Region + ":" + req.Identity.Account + ":function:" + name
}

func fnConfig(req *spi.Request, rec map[string]any) *spi.Response {
	out := map[string]any{}
	for k, v := range rec {
		if k != "Code" {
			out[k] = v
		}
	}
	out["FunctionArn"] = fnARN(req, str(rec["FunctionName"]))
	return &spi.Response{Output: out}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func functionName(req *spi.Request) string {
	if req.HTTP == nil {
		return ""
	}
	parts := strings.Split(req.HTTP.URL.Path, "/")
	for i, p := range parts {
		if p == "functions" && i+1 < len(parts) && parts[i+1] != "" {
			return parts[i+1]
		}
	}
	return ""
}

func str(v any) string { s, _ := v.(string); return s }
