package lambda

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"AddLayerVersionPermission", "CheckpointDurableExecution", "CreateCapacityProvider",
		"CreateCodeSigningConfig", "CreateFunctionUrlConfig", "DeleteCapacityProvider",
		"DeleteCodeSigningConfig", "DeleteFunctionCodeSigningConfig", "DeleteFunctionEventInvokeConfig",
		"DeleteFunctionUrlConfig", "DeleteLayerVersion", "DeleteProvisionedConcurrencyConfig",
		"DeleteResourcePolicy", "GetAccountSettings", "GetCapacityProvider", "GetCodeSigningConfig",
		"GetDurableExecution", "GetDurableExecutionHistory", "GetDurableExecutionState",
		"GetFunctionCodeSigningConfig", "GetFunctionEventInvokeConfig", "GetFunctionRecursionConfig",
		"GetFunctionScalingConfig", "GetFunctionUrlConfig", "GetLayerVersion", "GetLayerVersionByArn",
		"GetLayerVersionPolicy", "GetProvisionedConcurrencyConfig", "GetResourcePolicy",
		"GetRuntimeManagementConfig", "InvokeAsync", "InvokeWithResponseStream",
		"ListCapacityProviders", "ListCodeSigningConfigs", "ListDurableExecutionsByFunction",
		"ListFunctionEventInvokeConfigs", "ListFunctionUrlConfigs", "ListFunctionVersionsByCapacityProvider",
		"ListFunctionsByCodeSigningConfig", "ListLayerVersions", "ListLayers",
		"ListProvisionedConcurrencyConfigs", "PublishLayerVersion", "PutFunctionCodeSigningConfig",
		"PutFunctionEventInvokeConfig", "PutFunctionRecursionConfig", "PutFunctionScalingConfig",
		"PutProvisionedConcurrencyConfig", "PutResourcePolicy", "PutRuntimeManagementConfig",
		"RemoveLayerVersionPermission", "SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat", "SendDurableExecutionCallbackSuccess",
		"StopDurableExecution", "UpdateCapacityProvider", "UpdateCodeSigningConfig",
		"UpdateFunctionEventInvokeConfig", "UpdateFunctionUrlConfig",
	}
}

func (p *Pack) extraCol(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	fn := first(req.Input, "FunctionName")
	switch op {
	case "CreateFunctionUrlConfig", "UpdateFunctionUrlConfig":
		url := "http://127.0.0.1/lambda-url/" + fn
		rec := map[string]any{"FunctionName": fn, "FunctionUrl": url, "AuthType": first(req.Input, "AuthType")}
		if rec["AuthType"] == "" {
			rec["AuthType"] = "NONE"
		}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["FunctionUrl"] = url
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lamurl").Put(ctx, fn, b)
		return &spi.Response{Output: rec}, nil
	case "GetFunctionUrlConfig":
		return p.getExtra(ctx, req, "lamurl", fn)
	case "ListFunctionUrlConfigs":
		return p.listExtra(ctx, req, "lamurl", "FunctionUrlConfigs")
	case "DeleteFunctionUrlConfig":
		_ = p.extraCol(req, "lamurl").Delete(ctx, fn)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PublishLayerVersion":
		name := first(req.Input, "LayerName")
		n := p.next(ctx, req, "lamlayerrev:"+name)
		arn := "arn:aws:lambda:" + req.Identity.Region + ":" + req.Identity.Account + ":layer:" + name + ":" + strconv.Itoa(n)
		rec := map[string]any{"LayerName": name, "Version": n, "LayerVersionArn": arn, "Description": first(req.Input, "Description")}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lamlayer").Put(ctx, name+":"+strconv.Itoa(n), b)
		_ = p.extraCol(req, "lamlayer").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "GetLayerVersion", "GetLayerVersionByArn":
		key := first(req.Input, "LayerName") + ":" + first(req.Input, "VersionNumber")
		if op == "GetLayerVersionByArn" {
			arn := first(req.Input, "Arn", "LayerVersionArn")
			if i := strings.Index(arn, ":layer:"); i >= 0 {
				key = arn[i+7:]
			}
		}
		if first(req.Input, "VersionNumber") == "" && first(req.Input, "LayerName") != "" && op != "GetLayerVersionByArn" {
			key = first(req.Input, "LayerName")
		}
		return p.getExtra(ctx, req, "lamlayer", key)
	case "ListLayerVersions":
		name := first(req.Input, "LayerName")
		kvs, _, _ := p.extraCol(req, "lamlayer").List(ctx, name+":", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"LayerVersions": out}}, nil
	case "ListLayers":
		kvs, _, _ := p.extraCol(req, "lamlayer").List(ctx, "", "", 0)
		seen := map[string]bool{}
		var out []any
		for _, kv := range kvs {
			if strings.Contains(kv.Key, ":") {
				continue
			}
			if seen[kv.Key] {
				continue
			}
			seen[kv.Key] = true
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Layers": out}}, nil
	case "DeleteLayerVersion":
		_ = p.extraCol(req, "lamlayer").Delete(ctx, first(req.Input, "LayerName")+":"+first(req.Input, "VersionNumber"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "AddLayerVersionPermission":
		key := first(req.Input, "LayerName") + ":" + first(req.Input, "VersionNumber") + ":" + first(req.Input, "StatementId")
		stmt := map[string]any{"Sid": first(req.Input, "StatementId"), "Action": first(req.Input, "Action"), "Principal": first(req.Input, "Principal")}
		b, _ := json.Marshal(stmt)
		_ = p.extraCol(req, "lamlayerpol").Put(ctx, key, b)
		raw, _ := json.Marshal(stmt)
		return &spi.Response{Output: map[string]any{"Statement": string(raw), "RevisionId": p.deps.Rand.Hex(4)}}, nil
	case "RemoveLayerVersionPermission":
		_ = p.extraCol(req, "lamlayerpol").Delete(ctx, first(req.Input, "LayerName")+":"+first(req.Input, "VersionNumber")+":"+first(req.Input, "StatementId"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "GetLayerVersionPolicy":
		kvs, _, _ := p.extraCol(req, "lamlayerpol").List(ctx, first(req.Input, "LayerName")+":"+first(req.Input, "VersionNumber")+":", "", 0)
		var stmts []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			stmts = append(stmts, rec)
		}
		raw, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": stmts})
		return &spi.Response{Output: map[string]any{"Policy": string(raw)}}, nil
	case "CreateCodeSigningConfig", "UpdateCodeSigningConfig":
		id := first(req.Input, "CodeSigningConfigArn", "CodeSigningConfigId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		arn := "arn:aws:lambda:" + req.Identity.Region + ":" + req.Identity.Account + ":code-signing-config:" + id
		rec := map[string]any{"CodeSigningConfigArn": arn, "CodeSigningConfigId": id}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lamcsc").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CodeSigningConfig": rec}}, nil
	case "GetCodeSigningConfig":
		id := lastColon(first(req.Input, "CodeSigningConfigArn", "CodeSigningConfigId"))
		return p.getWrap(ctx, req, "lamcsc", id, "CodeSigningConfig")
	case "ListCodeSigningConfigs":
		return p.listExtra(ctx, req, "lamcsc", "CodeSigningConfigs")
	case "DeleteCodeSigningConfig":
		_ = p.extraCol(req, "lamcsc").Delete(ctx, lastColon(first(req.Input, "CodeSigningConfigArn")))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutFunctionCodeSigningConfig":
		_ = p.extraCol(req, "lamfcsc").Put(ctx, fn, []byte(first(req.Input, "CodeSigningConfigArn")))
		return &spi.Response{Output: map[string]any{"CodeSigningConfigArn": first(req.Input, "CodeSigningConfigArn"), "FunctionName": fn}}, nil
	case "GetFunctionCodeSigningConfig":
		b, ok, _ := p.extraCol(req, "lamfcsc").Get(ctx, fn)
		arn := ""
		if ok {
			arn = string(b)
		}
		return &spi.Response{Output: map[string]any{"CodeSigningConfigArn": arn, "FunctionName": fn}}, nil
	case "DeleteFunctionCodeSigningConfig":
		_ = p.extraCol(req, "lamfcsc").Delete(ctx, fn)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "ListFunctionsByCodeSigningConfig":
		arn := first(req.Input, "CodeSigningConfigArn")
		kvs, _, _ := p.extraCol(req, "lamfcsc").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			if string(kv.Value) == arn {
				names = append(names, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{"FunctionArns": names}}, nil
	case "CreateCapacityProvider", "UpdateCapacityProvider":
		name := first(req.Input, "CapacityProviderName", "Name")
		rec := map[string]any{"CapacityProviderName": name, "State": "Active"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lamcp").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"CapacityProvider": rec}}, nil
	case "GetCapacityProvider":
		return p.getWrap(ctx, req, "lamcp", first(req.Input, "CapacityProviderName", "Name"), "CapacityProvider")
	case "ListCapacityProviders":
		return p.listExtra(ctx, req, "lamcp", "CapacityProviders")
	case "DeleteCapacityProvider":
		_ = p.extraCol(req, "lamcp").Delete(ctx, first(req.Input, "CapacityProviderName", "Name"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "ListFunctionVersionsByCapacityProvider":
		return &spi.Response{Output: map[string]any{"FunctionVersions": []any{}}}, nil
	case "PutFunctionEventInvokeConfig", "UpdateFunctionEventInvokeConfig":
		rec := map[string]any{"FunctionName": fn, "MaximumRetryAttempts": req.Input["MaximumRetryAttempts"], "MaximumEventAgeInSeconds": req.Input["MaximumEventAgeInSeconds"]}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lameic").Put(ctx, fn, b)
		return &spi.Response{Output: rec}, nil
	case "GetFunctionEventInvokeConfig":
		return p.getExtra(ctx, req, "lameic", fn)
	case "ListFunctionEventInvokeConfigs":
		return p.listExtra(ctx, req, "lameic", "FunctionEventInvokeConfigs")
	case "DeleteFunctionEventInvokeConfig":
		_ = p.extraCol(req, "lameic").Delete(ctx, fn)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutProvisionedConcurrencyConfig":
		rec := map[string]any{"RequestedProvisionedConcurrentExecutions": req.Input["ProvisionedConcurrentExecutions"], "Status": "READY", "FunctionName": fn}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lampc").Put(ctx, fn+":"+first(req.Input, "Qualifier"), b)
		return &spi.Response{Output: rec}, nil
	case "GetProvisionedConcurrencyConfig":
		return p.getExtra(ctx, req, "lampc", fn+":"+first(req.Input, "Qualifier"))
	case "ListProvisionedConcurrencyConfigs":
		return p.listExtra(ctx, req, "lampc", "ProvisionedConcurrencyConfigs")
	case "DeleteProvisionedConcurrencyConfig":
		_ = p.extraCol(req, "lampc").Delete(ctx, fn+":"+first(req.Input, "Qualifier"))
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "PutFunctionRecursionConfig":
		_ = p.extraCol(req, "lamrec").Put(ctx, fn, []byte(first(req.Input, "RecursiveLoop")))
		return &spi.Response{Output: map[string]any{"RecursiveLoop": first(req.Input, "RecursiveLoop")}}, nil
	case "GetFunctionRecursionConfig":
		b, ok, _ := p.extraCol(req, "lamrec").Get(ctx, fn)
		v := "Allow"
		if ok {
			v = string(b)
		}
		return &spi.Response{Output: map[string]any{"RecursiveLoop": v}}, nil
	case "PutFunctionScalingConfig":
		b, _ := json.Marshal(req.Input)
		_ = p.extraCol(req, "lamscale").Put(ctx, fn, b)
		return &spi.Response{Output: req.Input}, nil
	case "GetFunctionScalingConfig":
		return p.getExtra(ctx, req, "lamscale", fn)
	case "PutRuntimeManagementConfig":
		b, _ := json.Marshal(req.Input)
		_ = p.extraCol(req, "lamrt").Put(ctx, fn, b)
		return &spi.Response{Output: req.Input}, nil
	case "GetRuntimeManagementConfig":
		return p.getExtra(ctx, req, "lamrt", fn)
	case "PutResourcePolicy":
		_ = p.extraCol(req, "lamrpol").Put(ctx, fn, []byte(first(req.Input, "Policy", "ResourcePolicy")))
		return &spi.Response{Output: map[string]any{"RevisionId": p.deps.Rand.Hex(4)}}, nil
	case "GetResourcePolicy":
		b, ok, _ := p.extraCol(req, "lamrpol").Get(ctx, fn)
		pol := "{}"
		if ok {
			pol = string(b)
		}
		return &spi.Response{Output: map[string]any{"Policy": pol}}, nil
	case "DeleteResourcePolicy":
		_ = p.extraCol(req, "lamrpol").Delete(ctx, fn)
		return &spi.Response{Status: 204, Output: map[string]any{}}, nil
	case "GetAccountSettings":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		n := 0
		for _, kv := range kvs {
			if !strings.Contains(kv.Key, ":") {
				n++
			}
		}
		return &spi.Response{Output: map[string]any{
			"AccountLimit": map[string]any{"ConcurrentExecutions": 1000, "TotalCodeSize": 80530636800},
			"AccountUsage": map[string]any{"FunctionCount": n, "TotalCodeSize": 0},
		}}, nil
	case "InvokeAsync":
		resp, err := p.invoke(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Status: 202, Output: map[string]any{"Status": 202, "Payload": resp.Output["Payload"]}}, nil
	case "InvokeWithResponseStream":
		// ponytail: not a real HTTP/2 event stream; returns the same Invoke payload.
		resp, err := p.invoke(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"StatusCode": 200, "Payload": resp.Output["Payload"]}}, nil
	case "CheckpointDurableExecution", "SendDurableExecutionCallbackFailure",
		"SendDurableExecutionCallbackHeartbeat", "SendDurableExecutionCallbackSuccess",
		"StopDurableExecution":
		// ponytail: no durable-execution engine; records only.
		id := first(req.Input, "DurableExecutionArn", "Name")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"DurableExecutionArn": id, "Status": op, "FunctionName": fn}
		b, _ := json.Marshal(rec)
		_ = p.extraCol(req, "lamdur").Put(ctx, id, b)
		return &spi.Response{Output: rec}, nil
	case "GetDurableExecution", "GetDurableExecutionHistory", "GetDurableExecutionState":
		id := first(req.Input, "DurableExecutionArn", "Name")
		b, ok, _ := p.extraCol(req, "lamdur").Get(ctx, id)
		rec := map[string]any{"DurableExecutionArn": id, "Status": "RUNNING"}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if op == "GetDurableExecutionHistory" {
			return &spi.Response{Output: map[string]any{"Events": []any{rec}}}, nil
		}
		return &spi.Response{Output: rec}, nil
	case "ListDurableExecutionsByFunction":
		return p.listExtra(ctx, req, "lamdur", "DurableExecutions")
	default:
		return nil, spi.NotImplemented("aws.lambda", op, "emulate")
	}
}

func (p *Pack) next(ctx context.Context, req *spi.Request, key string) int {
	b, ok, _ := p.extraCol(req, "lamseq").Get(ctx, key)
	n := 1
	if ok {
		n, _ = strconv.Atoi(string(b))
		n++
	}
	_ = p.extraCol(req, "lamseq").Put(ctx, key, []byte(strconv.Itoa(n)))
	return n
}

func (p *Pack) getExtra(ctx context.Context, req *spi.Request, col, id string) (*spi.Response, error) {
	b, ok, _ := p.extraCol(req, col).Get(ctx, id)
	if !ok {
		return &spi.Response{Output: map[string]any{}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) getWrap(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.extraCol(req, col).Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) listExtra(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.extraCol(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		if json.Unmarshal(kv.Value, &rec) != nil {
			out = append(out, map[string]any{"Id": kv.Key})
			continue
		}
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func lastColon(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[i+1:]
	}
	return s
}
