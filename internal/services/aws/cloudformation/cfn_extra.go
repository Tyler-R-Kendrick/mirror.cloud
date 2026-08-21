package cloudformation

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"ActivateOrganizationsAccess", "ActivateType", "BatchDescribeTypeConfigurations",
		"CancelUpdateStack", "ContinueUpdateRollback", "CreateGeneratedTemplate", "CreateStackInstances",
		"CreateStackRefactor", "CreateStackSet", "DeactivateOrganizationsAccess", "DeactivateType",
		"DeleteGeneratedTemplate", "DeleteStackInstances", "DeleteStackSet", "DeregisterType",
		"DescribeAccountLimits", "DescribeChangeSetHooks", "DescribeEvents", "DescribeGeneratedTemplate",
		"DescribeOrganizationsAccess", "DescribePublisher", "DescribeResourceScan",
		"DescribeStackDriftDetectionStatus", "DescribeStackInstance", "DescribeStackRefactor",
		"DescribeStackResourceDrifts", "DescribeStackResources", "DescribeStackSet", "DescribeStackSetOperation",
		"DescribeType", "DescribeTypeRegistration", "DetectStackDrift", "DetectStackResourceDrift",
		"DetectStackSetDrift", "EstimateTemplateCost", "ExecuteStackRefactor", "GetGeneratedTemplate",
		"GetHookResult", "GetStackPolicy", "ImportStacksToStackSet", "ListGeneratedTemplates", "ListHookResults",
		"ListImports", "ListResourceScanRelatedResources", "ListResourceScanResources", "ListResourceScans",
		"ListStackInstanceResourceDrifts", "ListStackInstances", "ListStackRefactorActions", "ListStackRefactors",
		"ListStackSetAutoDeploymentTargets", "ListStackSetOperationResults", "ListStackSetOperations",
		"ListStackSets", "ListTypeRegistrations", "ListTypeVersions", "ListTypes", "PublishType",
		"RecordHandlerProgress", "RegisterPublisher", "RegisterType", "RollbackStack", "SetStackPolicy",
		"SetTypeConfiguration", "SetTypeDefaultVersion", "StartResourceScan", "StopStackSetOperation",
		"TestType", "UpdateGeneratedTemplate", "UpdateStackInstances", "UpdateStackSet",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
	case "CreateStackSet", "UpdateStackSet":
		name := first(req.Input, "StackSetName")
		rec := map[string]any{"StackSetName": name, "Status": "ACTIVE", "TemplateBody": first(req.Input, "TemplateBody")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfnss").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"StackSetId": name}}, nil
	case "DescribeStackSet":
		return p.getWrap(ctx, req, "cfnss", first(req.Input, "StackSetName"), "StackSet")
	case "ListStackSets":
		return p.listCol(ctx, req, "cfnss", "Summaries")
	case "DeleteStackSet":
		_ = p.col(req, "cfnss").Delete(ctx, first(req.Input, "StackSetName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateStackInstances", "UpdateStackInstances", "DeleteStackInstances", "ImportStacksToStackSet":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"OperationId": id, "Status": "SUCCEEDED", "Action": op, "StackSetName": first(req.Input, "StackSetName")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfnssop").Put(ctx, id, b)
		_ = p.col(req, "cfnssi").Put(ctx, first(req.Input, "StackSetName")+"/"+id, b)
		return &spi.Response{Output: map[string]any{"OperationId": id}}, nil
	case "DescribeStackInstance":
		return p.getWrap(ctx, req, "cfnssi", first(req.Input, "StackSetName")+"/"+first(req.Input, "StackInstanceAccount", "OperationId"), "StackInstance")
	case "ListStackInstances":
		return p.listCol(ctx, req, "cfnssi", "Summaries")
	case "DescribeStackSetOperation":
		return p.getWrap(ctx, req, "cfnssop", first(req.Input, "OperationId"), "StackSetOperation")
	case "ListStackSetOperations", "ListStackSetOperationResults", "ListStackSetAutoDeploymentTargets":
		return p.listCol(ctx, req, "cfnssop", "Summaries")
	case "StopStackSetOperation":
		id := first(req.Input, "OperationId")
		b, ok, _ := p.col(req, "cfnssop").Get(ctx, id)
		rec := map[string]any{"OperationId": id, "Status": "STOPPED"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			rec["Status"] = "STOPPED"
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "cfnssop").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetStackPolicy":
		_ = p.col(req, "cfnpol").Put(ctx, first(req.Input, "StackName"), []byte(first(req.Input, "StackPolicyBody")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetStackPolicy":
		b, ok, _ := p.col(req, "cfnpol").Get(ctx, first(req.Input, "StackName"))
		body := "{}"
		if ok {
			body = string(b)
		}
		return &spi.Response{Output: map[string]any{"StackPolicyBody": body}}, nil
	case "DetectStackDrift", "DetectStackSetDrift", "DetectStackResourceDrift":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"StackDriftDetectionId": id, "DetectionStatus": "DETECTION_COMPLETE", "StackDriftStatus": "IN_SYNC"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfndrift").Put(ctx, id, b)
		out := map[string]any{"StackDriftDetectionId": id}
		if op == "DetectStackResourceDrift" {
			out = map[string]any{"StackResourceDrift": map[string]any{"StackResourceDriftStatus": "IN_SYNC"}}
		}
		return &spi.Response{Output: out}, nil
	case "DescribeStackDriftDetectionStatus":
		id := first(req.Input, "StackDriftDetectionId")
		b, ok, _ := p.col(req, "cfndrift").Get(ctx, id)
		rec := map[string]any{"StackDriftDetectionId": id, "DetectionStatus": "DETECTION_COMPLETE", "StackDriftStatus": "IN_SYNC"}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: rec}, nil
	case "DescribeStackResourceDrifts", "ListStackInstanceResourceDrifts":
		return &spi.Response{Output: map[string]any{"StackResourceDrifts": []any{}}}, nil
	case "CancelUpdateStack", "ContinueUpdateRollback", "RollbackStack":
		name := first(req.Input, "StackName")
		st, err := p.load(ctx, req, name)
		if err != nil {
			st = stack{Name: name, Status: "ROLLBACK_COMPLETE"}
		}
		st.Status = "ROLLBACK_COMPLETE"
		if op == "CancelUpdateStack" {
			st.Status = "UPDATE_ROLLBACK_COMPLETE"
		}
		raw, _ := json.Marshal(st)
		_ = p.col(req, "cfn").Put(ctx, name, raw)
		return &spi.Response{Output: map[string]any{"StackId": st.ID}}, nil
	case "DescribeStackResources":
		st, err := p.load(ctx, req, first(req.Input, "StackName"))
		if err != nil {
			return &spi.Response{Output: map[string]any{"StackResources": []any{}}}, nil
		}
		var rs []any
		for _, r := range st.Resources {
			rs = append(rs, map[string]any{"LogicalResourceId": r.Logical, "PhysicalResourceId": r.Physical, "ResourceType": r.Type, "ResourceStatus": "CREATE_COMPLETE"})
		}
		return &spi.Response{Output: map[string]any{"StackResources": rs}}, nil
	case "RegisterType", "PublishType", "ActivateType", "DeactivateType", "SetTypeConfiguration", "SetTypeDefaultVersion", "TestType":
		name := first(req.Input, "TypeName", "Type")
		if name == "" {
			name = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"TypeName": name, "TypeArn": "arn:aws:cloudformation:" + req.Identity.Region + "::type/" + name, "Status": "COMPLETE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfntype").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TypeArn": rec["TypeArn"], "ProgressToken": p.deps.Rand.Hex(8)}}, nil
	case "DescribeType", "DescribeTypeRegistration":
		return p.getWrap(ctx, req, "cfntype", first(req.Input, "TypeName", "Type"), "Type")
	case "ListTypes", "ListTypeVersions", "ListTypeRegistrations", "BatchDescribeTypeConfigurations":
		return p.listCol(ctx, req, "cfntype", "TypeSummaries")
	case "DeregisterType":
		_ = p.col(req, "cfntype").Delete(ctx, first(req.Input, "TypeName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateGeneratedTemplate", "UpdateGeneratedTemplate":
		name := first(req.Input, "GeneratedTemplateName", "TemplateName")
		if name == "" {
			name = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"GeneratedTemplateName": name, "Status": "COMPLETE", "TemplateBody": "{}"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfngen").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"GeneratedTemplateId": name}}, nil
	case "DescribeGeneratedTemplate", "GetGeneratedTemplate":
		return p.getWrap(ctx, req, "cfngen", first(req.Input, "GeneratedTemplateName", "GeneratedTemplateId"), "GeneratedTemplate")
	case "ListGeneratedTemplates":
		return p.listCol(ctx, req, "cfngen", "Summaries")
	case "DeleteGeneratedTemplate":
		_ = p.col(req, "cfngen").Delete(ctx, first(req.Input, "GeneratedTemplateName", "GeneratedTemplateId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartResourceScan":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"ResourceScanId": id, "Status": "COMPLETE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfnscan").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ResourceScanId": id}}, nil
	case "DescribeResourceScan":
		return p.getWrap(ctx, req, "cfnscan", first(req.Input, "ResourceScanId"), "ResourceScan")
	case "ListResourceScans", "ListResourceScanResources", "ListResourceScanRelatedResources":
		return p.listCol(ctx, req, "cfnscan", "ResourceScans")
	case "CreateStackRefactor", "ExecuteStackRefactor":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"StackRefactorId": id, "Status": "COMPLETE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfnref").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"StackRefactorId": id}}, nil
	case "DescribeStackRefactor":
		return p.getWrap(ctx, req, "cfnref", first(req.Input, "StackRefactorId"), "StackRefactor")
	case "ListStackRefactors", "ListStackRefactorActions":
		return p.listCol(ctx, req, "cfnref", "StackRefactorSummaries")
	case "ActivateOrganizationsAccess", "DeactivateOrganizationsAccess":
		st := "ENABLED"
		if strings.HasPrefix(op, "Deactivate") {
			st = "DISABLED"
		}
		_ = p.col(req, "cfnorg").Put(ctx, "access", []byte(st))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeOrganizationsAccess":
		b, ok, _ := p.col(req, "cfnorg").Get(ctx, "access")
		st := "DISABLED"
		if ok {
			st = string(b)
		}
		return &spi.Response{Output: map[string]any{"Status": st}}, nil
	case "RegisterPublisher":
		id := p.deps.Rand.Hex(8)
		_ = p.col(req, "cfnpub").Put(ctx, "id", []byte(id))
		return &spi.Response{Output: map[string]any{"PublisherId": id}}, nil
	case "DescribePublisher":
		b, ok, _ := p.col(req, "cfnpub").Get(ctx, "id")
		id := ""
		if ok {
			id = string(b)
		}
		return &spi.Response{Output: map[string]any{"PublisherId": id, "PublisherStatus": "VERIFIED"}}, nil
	case "DescribeAccountLimits":
		return &spi.Response{Output: map[string]any{"AccountLimits": []any{map[string]any{"Name": "StackLimit", "Value": 200}}}}, nil
	case "DescribeChangeSetHooks", "ListHookResults":
		return &spi.Response{Output: map[string]any{"Hooks": []any{}}}, nil
	case "GetHookResult":
		return &spi.Response{Output: map[string]any{"HookStatus": "HOOK_COMPLETE_SUCCEEDED"}}, nil
	case "DescribeEvents":
		return &spi.Response{Output: map[string]any{"OperationEvents": []any{}}}, nil
	case "EstimateTemplateCost":
		return &spi.Response{Output: map[string]any{"Url": "http://127.0.0.1/cfn-cost"}}, nil
	case "ListImports":
		return &spi.Response{Output: map[string]any{"Imports": []any{}}}, nil
	case "RecordHandlerProgress":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudformation", op, "emulate")
	}
}

func (p *Pack) getWrap(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) listCol(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}
