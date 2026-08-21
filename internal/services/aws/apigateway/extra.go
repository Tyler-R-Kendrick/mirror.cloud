package apigateway

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// extraOps remaining API Gateway ops served as control-plane KV.
// leftoverOps are remaining Smithy operations served as control-plane KV.
// ponytail: no real custom domains, VPC links, or SDK generation; upgrade is per-op AWS shapes.
func ExtraOps() []string { return extraOps() }

func extraOps() []string {
	return []string{
		"CreateBasePathMapping",
		"CreateDocumentationPart",
		"CreateDocumentationVersion",
		"CreateDomainName",
		"CreateDomainNameAccessAssociation",
		"CreateModel",
		"CreateRequestValidator",
		"CreateUsagePlanKey",
		"CreateVpcLink",
		"DeleteBasePathMapping",
		"DeleteClientCertificate",
		"DeleteDocumentationPart",
		"DeleteDocumentationVersion",
		"DeleteDomainName",
		"DeleteDomainNameAccessAssociation",
		"DeleteGatewayResponse",
		"DeleteModel",
		"DeleteRequestValidator",
		"DeleteUsagePlanKey",
		"DeleteVpcLink",
		"FlushStageAuthorizersCache",
		"FlushStageCache",
		"GenerateClientCertificate",
		"GetAccount",
		"GetBasePathMapping",
		"GetBasePathMappings",
		"GetClientCertificate",
		"GetClientCertificates",
		"GetDocumentationPart",
		"GetDocumentationParts",
		"GetDocumentationVersion",
		"GetDocumentationVersions",
		"GetDomainName",
		"GetDomainNameAccessAssociations",
		"GetDomainNames",
		"GetExport",
		"GetGatewayResponse",
		"GetGatewayResponses",
		"GetModel",
		"GetModelTemplate",
		"GetModels",
		"GetRequestValidator",
		"GetRequestValidators",
		"GetSdk",
		"GetSdkType",
		"GetSdkTypes",
		"GetStages",
		"GetTags",
		"GetUsage",
		"GetUsagePlanKey",
		"GetUsagePlanKeys",
		"GetVpcLink",
		"GetVpcLinks",
		"ImportApiKeys",
		"ImportDocumentationParts",
		"ImportRestApi",
		"PutGatewayResponse",
		"PutRestApi",
		"RejectDomainNameAccessAssociation",
		"TagResource",
		"TestInvokeAuthorizer",
		"TestInvokeMethod",
		"UntagResource",
		"UpdateAccount",
		"UpdateApiKey",
		"UpdateAuthorizer",
		"UpdateBasePathMapping",
		"UpdateClientCertificate",
		"UpdateDeployment",
		"UpdateDocumentationPart",
		"UpdateDocumentationVersion",
		"UpdateDomainName",
		"UpdateGatewayResponse",
		"UpdateIntegration",
		"UpdateIntegrationResponse",
		"UpdateMethod",
		"UpdateMethodResponse",
		"UpdateModel",
		"UpdateRequestValidator",
		"UpdateResource",
		"UpdateUsage",
		"UpdateUsagePlan",
		"UpdateVpcLink",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	kind, idKey, listKey, wrap := extraShape(op)
	id := first(req.Input, idKey, "id", "name", "Name", "domainName", "restApiId")
	switch {
	case isExtraWrite(op):
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		if idKey != "" {
			if _, ok := rec[idKey]; !ok {
				rec[idKey] = id
			}
		}
		if op == "GenerateClientCertificate" {
			rec["clientCertificateId"] = id
			rec["pemEncodedCertificate"] = "-----BEGIN CERTIFICATE-----\nM\n-----END CERTIFICATE-----"
		}
		if op == "TestInvokeMethod" || op == "TestInvokeAuthorizer" {
			rec["status"] = 200
			rec["body"] = "{}"
		}
		p.lput(ctx, req, kind+":"+id, rec)
		out := rec
		if wrap != "" {
			out = map[string]any{wrap: rec, idKey: id}
		}
		return &spi.Response{Status: 201, Output: out}, nil
	case strings.HasPrefix(op, "Delete") || strings.HasPrefix(op, "Untag") || strings.HasPrefix(op, "Reject") ||
		strings.HasPrefix(op, "Flush"):
		if id != "" {
			_ = p.col(req, "apigw-l").Delete(ctx, kind+":"+id)
		}
		return &spi.Response{Status: 202, Output: map[string]any{}}, nil
	case strings.HasPrefix(op, "Get") && strings.HasSuffix(op, "s") || strings.HasPrefix(op, "Get") && strings.Contains(op, "List"):
		return p.llist(ctx, req, kind+":", listKey)
	default:
		if rec, ok := p.lget(ctx, req, kind+":"+id); ok {
			return &spi.Response{Output: rec}, nil
		}
		if strings.HasPrefix(op, "Get") && (strings.HasSuffix(op, "s") || strings.HasSuffix(op, "ings") || strings.HasSuffix(op, "tions") || strings.HasSuffix(op, "Keys") || strings.HasSuffix(op, "Links") || strings.HasSuffix(op, "Names") || strings.HasSuffix(op, "Types") || strings.HasSuffix(op, "Parts") || strings.HasSuffix(op, "Versions") || strings.HasSuffix(op, "Associations") || strings.HasSuffix(op, "Mappings") || strings.HasSuffix(op, "Certificates") || strings.HasSuffix(op, "Responses") || strings.HasSuffix(op, "Models") || strings.HasSuffix(op, "Validators") || strings.HasSuffix(op, "Tags")) {
			return p.llist(ctx, req, kind+":", listKey)
		}
		out := map[string]any{}
		if op == "GetExport" || op == "GetSdk" {
			out["body"] = ""
		}
		if op == "GetAccount" {
			out["cloudwatchRoleArn"] = ""
		}
		return &spi.Response{Output: out}, nil
	}
}

func (p *Pack) lput(ctx context.Context, req *spi.Request, key string, rec any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, "apigw-l").Put(ctx, key, b)
}

func (p *Pack) lget(ctx context.Context, req *spi.Request, key string) (map[string]any, bool) {
	b, ok, _ := p.col(req, "apigw-l").Get(ctx, key)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func (p *Pack) llist(ctx context.Context, req *spi.Request, pfx, outKey string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "apigw-l").List(ctx, pfx, "", 0)
	items := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	if outKey == "" {
		outKey = "item"
	}
	return &spi.Response{Output: map[string]any{outKey: items}}, nil
}

func isExtraWrite(op string) bool {
	for _, p := range []string{"Create", "Put", "Update", "Tag", "Import", "Generate", "Test"} {
		if strings.HasPrefix(op, p) && !strings.HasPrefix(op, "Untag") {
			return true
		}
	}
	return false
}

func extraShape(op string) (kind, idKey, listKey, wrap string) {
	switch {
	case strings.Contains(op, "DomainName"):
		return "ldn", "domainName", "items", ""
	case strings.Contains(op, "VpcLink"):
		return "lvpc", "id", "items", ""
	case strings.Contains(op, "Model"):
		return "lmod", "name", "items", ""
	case strings.Contains(op, "Documentation"):
		return "ldoc", "id", "items", ""
	case strings.Contains(op, "RequestValidator"):
		return "lrv", "id", "items", ""
	case strings.Contains(op, "UsagePlanKey"):
		return "lupk", "id", "items", ""
	case strings.Contains(op, "GatewayResponse"):
		return "lgr", "responseType", "items", ""
	case strings.Contains(op, "ClientCertificate"):
		return "lcc", "clientCertificateId", "items", ""
	case strings.Contains(op, "BasePath"):
		return "lbp", "basePath", "items", ""
	default:
		return "lmisc", "id", "item", ""
	}
}
