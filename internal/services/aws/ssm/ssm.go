// Package ssm is Parameter Store. SecureString is reversible local encoding, not encryption.
package ssm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ssm", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements SSM Parameter Store.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ssm" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"PutParameter", "GetParameter", "GetParameters", "GetParametersByPath",
		"DeleteParameter", "DeleteParameters", "DescribeParameters", "LabelParameterVersion",
		"UnlabelParameterVersion", "GetParameterHistory",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
		"CreateDocument", "GetDocument", "DeleteDocument", "UpdateDocument", "DescribeDocument",
		"ListDocuments", "ListDocumentVersions", "UpdateDocumentDefaultVersion",
		"CreateAssociation", "DescribeAssociation", "UpdateAssociation", "DeleteAssociation",
		"ListAssociations",
		"SendCommand", "ListCommands", "ListCommandInvocations", "GetCommandInvocation", "CancelCommand",
		"CreatePatchBaseline", "GetPatchBaseline", "UpdatePatchBaseline", "DeletePatchBaseline",
		"DescribePatchBaselines", "RegisterDefaultPatchBaseline", "GetDefaultPatchBaseline",
		"CreateMaintenanceWindow", "GetMaintenanceWindow", "UpdateMaintenanceWindow", "DeleteMaintenanceWindow",
		"DescribeMaintenanceWindows",
		"RegisterTargetWithMaintenanceWindow", "DeregisterTargetFromMaintenanceWindow", "DescribeMaintenanceWindowTargets",
		"RegisterTaskWithMaintenanceWindow", "DeregisterTaskFromMaintenanceWindow", "DescribeMaintenanceWindowTasks",
		"StartAutomationExecution", "GetAutomationExecution", "StopAutomationExecution", "DescribeAutomationExecutions",
		"CreateOpsItem", "GetOpsItem", "UpdateOpsItem", "DeleteOpsItem", "DescribeOpsItems",
		"CreateResourceDataSync", "ListResourceDataSync", "DeleteResourceDataSync",
		"GetServiceSetting", "UpdateServiceSetting", "ResetServiceSetting",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("ssm")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := str(req.Input["Name"])
	switch req.Operation {
	case "PutParameter":
		val := str(req.Input["Value"])
		typ := str(req.Input["Type"])
		stored := val
		if typ == "SecureString" {
			stored = base64.StdEncoding.EncodeToString([]byte(val))
		}
		ver := 1
		if old, ok, _ := p.col(req).Get(ctx, name); ok {
			var m map[string]any
			_ = json.Unmarshal(old, &m)
			ver = asInt(m["Version"]) + 1
			p.appendHist(ctx, req, name, m)
		}
		rec := map[string]any{"Name": name, "Value": stored, "Type": typ, "Version": ver}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, name, b)
		p.appendHist(ctx, req, name, rec)
		return &spi.Response{Output: map[string]any{"Version": ver}}, nil
	case "GetParameter":
		b, ok, _ := p.col(req).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ParameterNotFound", HTTPStatus: 400, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: map[string]any{"Parameter": decodeParam(m)}}, nil
	case "GetParameters":
		var names []any
		if v, ok := req.Input["Names"].([]any); ok {
			names = v
		}
		var ps []any
		for _, n := range names {
			b, ok, _ := p.col(req).Get(ctx, str(n))
			if !ok {
				continue
			}
			var m map[string]any
			_ = json.Unmarshal(b, &m)
			ps = append(ps, decodeParam(m))
		}
		return &spi.Response{Output: map[string]any{"Parameters": ps}}, nil
	case "GetParametersByPath", "DescribeParameters":
		prefix := str(req.Input["Path"])
		if prefix == "" {
			prefix = str(req.Input["Name"])
		}
		kvs, _, _ := p.col(req).List(ctx, prefix, "", 0)
		recursive := req.Input["Recursive"] == true || str(req.Input["Recursive"]) == "true" || req.Operation == "DescribeParameters"
		var ps []any
		for _, kv := range kvs {
			if strings.Contains(kv.Key, ":") {
				continue
			}
			if !recursive && !directChild(prefix, kv.Key) {
				continue
			}
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if m["Name"] == nil {
				continue
			}
			ps = append(ps, decodeParam(m))
		}
		start := asInt(req.Input["NextToken"])
		if start < 0 {
			start = 0
		}
		limit := asInt(req.Input["MaxResults"])
		out := map[string]any{"Parameters": ps}
		if limit > 0 && start < len(ps) {
			end := start + limit
			if end > len(ps) {
				end = len(ps)
			}
			page := ps[start:end]
			out["Parameters"] = page
			if end < len(ps) {
				out["NextToken"] = strconv.Itoa(end)
			}
		}
		return &spi.Response{Output: out}, nil
	case "DeleteParameter":
		_ = p.col(req).Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteParameters":
		if v, ok := req.Input["Names"].([]any); ok {
			for _, n := range v {
				_ = p.col(req).Delete(ctx, str(n))
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "LabelParameterVersion":
		labels, _ := req.Input["Labels"].([]any)
		b, _ := json.Marshal(labels)
		_ = p.col(req).Put(ctx, name+":labels", b)
		return &spi.Response{Output: map[string]any{"ParameterName": name, "ParameterVersion": 1}}, nil
	case "UnlabelParameterVersion":
		_ = p.col(req).Delete(ctx, name+":labels")
		return &spi.Response{Output: map[string]any{"ParameterName": name}}, nil
	case "GetParameterHistory":
		b, ok, _ := p.col(req).Get(ctx, name+":hist")
		var hist []any
		if ok {
			_ = json.Unmarshal(b, &hist)
		}
		if len(hist) == 0 {
			if cur, ok, _ := p.col(req).Get(ctx, name); ok {
				var m map[string]any
				_ = json.Unmarshal(cur, &m)
				hist = append(hist, decodeParam(m))
			}
		} else {
			for i, h := range hist {
				if m, ok := h.(map[string]any); ok {
					hist[i] = decodeParam(m)
				}
			}
		}
		return &spi.Response{Output: map[string]any{"Parameters": hist}}, nil
	case "AddTagsToResource":
		res := str(req.Input["ResourceId"])
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req).Put(ctx, "tags:"+res, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemoveTagsFromResource":
		_ = p.col(req).Delete(ctx, "tags:"+str(req.Input["ResourceId"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req).Get(ctx, "tags:"+str(req.Input["ResourceId"]))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"TagList": tags}}, nil
	case "CreateDocument":
		n := first(req.Input, "Name")
		rec := map[string]any{"Name": n, "DocumentVersion": "1", "Status": "Active", "Content": req.Input["Content"], "DocumentType": first(req.Input, "DocumentType")}
		p.put(ctx, req, "doc:"+n, rec)
		p.put(ctx, req, "docver:"+n+":1", rec)
		return &spi.Response{Output: map[string]any{"DocumentDescription": rec}}, nil
	case "GetDocument":
		n := first(req.Input, "Name")
		rec, ok := p.get(ctx, req, "doc:"+n)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidDocument", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: rec}, nil
	case "DescribeDocument":
		n := first(req.Input, "Name")
		rec, ok := p.get(ctx, req, "doc:"+n)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidDocument", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"Document": rec}}, nil
	case "UpdateDocument":
		n := first(req.Input, "Name")
		rec, ok := p.get(ctx, req, "doc:"+n)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidDocument", HTTPStatus: 400, Fault: "client"}
		}
		ver := asInt(rec["DocumentVersion"]) + 1
		rec["DocumentVersion"] = strconv.Itoa(ver)
		rec["Content"] = req.Input["Content"]
		p.put(ctx, req, "doc:"+n, rec)
		p.put(ctx, req, "docver:"+n+":"+strconv.Itoa(ver), rec)
		return &spi.Response{Output: map[string]any{"DocumentDescription": rec}}, nil
	case "DeleteDocument":
		_ = p.col(req).Delete(ctx, "doc:"+first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListDocuments":
		return p.listPref(ctx, req, "doc:", "DocumentIdentifiers")
	case "ListDocumentVersions":
		n := first(req.Input, "Name")
		return p.listPref(ctx, req, "docver:"+n+":", "DocumentVersions")
	case "UpdateDocumentDefaultVersion":
		n, ver := first(req.Input, "Name"), first(req.Input, "DocumentVersion")
		rec, ok := p.get(ctx, req, "doc:"+n)
		if !ok {
			return nil, &spi.Fault{Code: "InvalidDocument", HTTPStatus: 400, Fault: "client"}
		}
		rec["DefaultVersion"] = ver
		p.put(ctx, req, "doc:"+n, rec)
		return &spi.Response{Output: map[string]any{"Description": rec}}, nil
	case "CreateAssociation":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"AssociationId": id, "Name": first(req.Input, "Name"), "AssociationName": first(req.Input, "AssociationName"), "Status": map[string]any{"Name": "Success"}}
		p.put(ctx, req, "assoc:"+id, rec)
		return &spi.Response{Output: map[string]any{"AssociationDescription": rec}}, nil
	case "DescribeAssociation":
		id := first(req.Input, "AssociationId")
		rec, ok := p.get(ctx, req, "assoc:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "AssociationDoesNotExist", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"AssociationDescription": rec}}, nil
	case "UpdateAssociation":
		id := first(req.Input, "AssociationId")
		rec, ok := p.get(ctx, req, "assoc:"+id)
		if !ok {
			rec = map[string]any{"AssociationId": id}
		}
		if n := first(req.Input, "Name"); n != "" {
			rec["Name"] = n
		}
		p.put(ctx, req, "assoc:"+id, rec)
		return &spi.Response{Output: map[string]any{"AssociationDescription": rec}}, nil
	case "DeleteAssociation":
		_ = p.col(req).Delete(ctx, "assoc:"+first(req.Input, "AssociationId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAssociations":
		return p.listPref(ctx, req, "assoc:", "Associations")
	case "SendCommand":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"CommandId": id, "DocumentName": first(req.Input, "DocumentName"), "Status": "Success", "InstanceIds": req.Input["InstanceIds"]}
		p.put(ctx, req, "cmd:"+id, rec)
		return &spi.Response{Output: map[string]any{"Command": rec}}, nil
	case "ListCommands":
		return p.listPref(ctx, req, "cmd:", "Commands")
	case "ListCommandInvocations":
		id := first(req.Input, "CommandId")
		rec, _ := p.get(ctx, req, "cmd:"+id)
		inv := map[string]any{"CommandId": id, "Status": "Success", "InstanceId": "i-0"}
		if rec != nil {
			inv["DocumentName"] = rec["DocumentName"]
		}
		return &spi.Response{Output: map[string]any{"CommandInvocations": []any{inv}}}, nil
	case "GetCommandInvocation":
		id := first(req.Input, "CommandId")
		rec, _ := p.get(ctx, req, "cmd:"+id)
		out := map[string]any{"CommandId": id, "Status": "Success", "InstanceId": first(req.Input, "InstanceId"), "StandardOutputContent": "", "StandardErrorContent": ""}
		if rec != nil {
			out["DocumentName"] = rec["DocumentName"]
		}
		return &spi.Response{Output: out}, nil
	case "CancelCommand":
		id := first(req.Input, "CommandId")
		if rec, ok := p.get(ctx, req, "cmd:"+id); ok {
			rec["Status"] = "Cancelled"
			p.put(ctx, req, "cmd:"+id, rec)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePatchBaseline":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"BaselineId": id, "Name": first(req.Input, "Name"), "OperatingSystem": first(req.Input, "OperatingSystem")}
		p.put(ctx, req, "patch:"+id, rec)
		return &spi.Response{Output: map[string]any{"BaselineId": id}}, nil
	case "GetPatchBaseline":
		id := first(req.Input, "BaselineId")
		rec, ok := p.get(ctx, req, "patch:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "DoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: rec}, nil
	case "UpdatePatchBaseline":
		id := first(req.Input, "BaselineId")
		rec, ok := p.get(ctx, req, "patch:"+id)
		if !ok {
			rec = map[string]any{"BaselineId": id}
		}
		if n := first(req.Input, "Name"); n != "" {
			rec["Name"] = n
		}
		p.put(ctx, req, "patch:"+id, rec)
		return &spi.Response{Output: rec}, nil
	case "DeletePatchBaseline":
		_ = p.col(req).Delete(ctx, "patch:"+first(req.Input, "BaselineId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribePatchBaselines":
		return p.listPref(ctx, req, "patch:", "BaselineIdentities")
	case "RegisterDefaultPatchBaseline":
		id := first(req.Input, "BaselineId")
		p.put(ctx, req, "patchdefault", map[string]any{"BaselineId": id})
		return &spi.Response{Output: map[string]any{"BaselineId": id}}, nil
	case "GetDefaultPatchBaseline":
		rec, ok := p.get(ctx, req, "patchdefault")
		if !ok {
			return &spi.Response{Output: map[string]any{"BaselineId": "arn:aws:ssm:us-east-1:000000000000:patchbaseline/default"}}, nil
		}
		return &spi.Response{Output: rec}, nil
	case "CreateMaintenanceWindow":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"WindowId": id, "Name": first(req.Input, "Name"), "Schedule": first(req.Input, "Schedule"), "Duration": req.Input["Duration"], "Cutoff": req.Input["Cutoff"]}
		p.put(ctx, req, "mw:"+id, rec)
		return &spi.Response{Output: map[string]any{"WindowId": id}}, nil
	case "GetMaintenanceWindow":
		id := first(req.Input, "WindowId")
		rec, ok := p.get(ctx, req, "mw:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "DoesNotExistException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: rec}, nil
	case "UpdateMaintenanceWindow":
		id := first(req.Input, "WindowId")
		rec, ok := p.get(ctx, req, "mw:"+id)
		if !ok {
			rec = map[string]any{"WindowId": id}
		}
		if n := first(req.Input, "Name"); n != "" {
			rec["Name"] = n
		}
		p.put(ctx, req, "mw:"+id, rec)
		return &spi.Response{Output: rec}, nil
	case "DeleteMaintenanceWindow":
		_ = p.col(req).Delete(ctx, "mw:"+first(req.Input, "WindowId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeMaintenanceWindows":
		return p.listPref(ctx, req, "mw:", "WindowIdentities")
	case "RegisterTargetWithMaintenanceWindow":
		wid, tid := first(req.Input, "WindowId"), p.deps.Rand.Hex(8)
		rec := map[string]any{"WindowTargetId": tid, "WindowId": wid, "ResourceType": first(req.Input, "ResourceType")}
		p.put(ctx, req, "mwt:"+wid+":"+tid, rec)
		return &spi.Response{Output: map[string]any{"WindowTargetId": tid}}, nil
	case "DeregisterTargetFromMaintenanceWindow":
		_ = p.col(req).Delete(ctx, "mwt:"+first(req.Input, "WindowId")+":"+first(req.Input, "WindowTargetId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeMaintenanceWindowTargets":
		return p.listPref(ctx, req, "mwt:"+first(req.Input, "WindowId")+":", "Targets")
	case "RegisterTaskWithMaintenanceWindow":
		wid, tid := first(req.Input, "WindowId"), p.deps.Rand.Hex(8)
		rec := map[string]any{"WindowTaskId": tid, "WindowId": wid, "TaskArn": first(req.Input, "TaskArn")}
		p.put(ctx, req, "mwk:"+wid+":"+tid, rec)
		return &spi.Response{Output: map[string]any{"WindowTaskId": tid}}, nil
	case "DeregisterTaskFromMaintenanceWindow":
		_ = p.col(req).Delete(ctx, "mwk:"+first(req.Input, "WindowId")+":"+first(req.Input, "WindowTaskId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeMaintenanceWindowTasks":
		return p.listPref(ctx, req, "mwk:"+first(req.Input, "WindowId")+":", "Tasks")
	case "StartAutomationExecution":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"AutomationExecutionId": id, "DocumentName": first(req.Input, "DocumentName"), "AutomationExecutionStatus": "Success"}
		p.put(ctx, req, "auto:"+id, rec)
		return &spi.Response{Output: map[string]any{"AutomationExecutionId": id}}, nil
	case "GetAutomationExecution":
		id := first(req.Input, "AutomationExecutionId")
		rec, ok := p.get(ctx, req, "auto:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "AutomationExecutionNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"AutomationExecution": rec}}, nil
	case "StopAutomationExecution":
		id := first(req.Input, "AutomationExecutionId")
		if rec, ok := p.get(ctx, req, "auto:"+id); ok {
			rec["AutomationExecutionStatus"] = "Cancelled"
			p.put(ctx, req, "auto:"+id, rec)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeAutomationExecutions":
		return p.listPref(ctx, req, "auto:", "AutomationExecutionMetadataList")
	case "CreateOpsItem":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"OpsItemId": id, "Title": first(req.Input, "Title"), "Source": first(req.Input, "Source"), "Status": "Open"}
		p.put(ctx, req, "ops:"+id, rec)
		return &spi.Response{Output: map[string]any{"OpsItemId": id}}, nil
	case "GetOpsItem":
		id := first(req.Input, "OpsItemId")
		rec, ok := p.get(ctx, req, "ops:"+id)
		if !ok {
			return nil, &spi.Fault{Code: "OpsItemNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{"OpsItem": rec}}, nil
	case "UpdateOpsItem":
		id := first(req.Input, "OpsItemId")
		rec, ok := p.get(ctx, req, "ops:"+id)
		if !ok {
			rec = map[string]any{"OpsItemId": id}
		}
		if s := first(req.Input, "Status"); s != "" {
			rec["Status"] = s
		}
		if t := first(req.Input, "Title"); t != "" {
			rec["Title"] = t
		}
		p.put(ctx, req, "ops:"+id, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteOpsItem":
		_ = p.col(req).Delete(ctx, "ops:"+first(req.Input, "OpsItemId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeOpsItems":
		return p.listPref(ctx, req, "ops:", "OpsItemSummaries")
	case "CreateResourceDataSync":
		n := first(req.Input, "SyncName")
		rec := map[string]any{"SyncName": n, "SyncType": first(req.Input, "SyncType"), "Status": "Successful"}
		p.put(ctx, req, "rdsync:"+n, rec)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListResourceDataSync":
		return p.listPref(ctx, req, "rdsync:", "ResourceDataSyncItems")
	case "DeleteResourceDataSync":
		_ = p.col(req).Delete(ctx, "rdsync:"+first(req.Input, "SyncName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "UpdateServiceSetting":
		id := first(req.Input, "SettingId")
		p.put(ctx, req, "svcset:"+id, map[string]any{"SettingId": id, "SettingValue": first(req.Input, "SettingValue")})
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetServiceSetting":
		id := first(req.Input, "SettingId")
		rec, ok := p.get(ctx, req, "svcset:"+id)
		if !ok {
			rec = map[string]any{"SettingId": id, "SettingValue": ""}
		}
		return &spi.Response{Output: map[string]any{"ServiceSetting": rec}}, nil
	case "ResetServiceSetting":
		_ = p.col(req).Delete(ctx, "svcset:"+first(req.Input, "SettingId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) put(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req).Put(ctx, key, b)
}

func (p *Pack) get(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req).Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) listPref(ctx context.Context, req *spi.Request, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req).List(ctx, pfx, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func str(v any) string { s, _ := v.(string); return s }

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func decodeParam(m map[string]any) map[string]any {
	if str(m["Type"]) != "SecureString" {
		return m
	}
	raw, err := base64.StdEncoding.DecodeString(str(m["Value"]))
	if err != nil {
		return m
	}
	out := map[string]any{}
	for k, v := range m {
		out[k] = v
	}
	out["Value"] = string(raw)
	return out
}

func (p *Pack) appendHist(ctx context.Context, req *spi.Request, name string, rec map[string]any) {
	var hist []any
	if b, ok, _ := p.col(req).Get(ctx, name+":hist"); ok {
		_ = json.Unmarshal(b, &hist)
	}
	hist = append(hist, rec)
	raw, _ := json.Marshal(hist)
	_ = p.col(req).Put(ctx, name+":hist", raw)
}

func directChild(path, name string) bool {
	if path == "" {
		rest := strings.TrimPrefix(name, "/")
		return rest != "" && !strings.Contains(rest, "/")
	}
	if name == path || !strings.HasPrefix(name, path) {
		return false
	}
	rest := strings.TrimPrefix(name[len(path):], "/")
	return rest != "" && !strings.Contains(rest, "/")
}
