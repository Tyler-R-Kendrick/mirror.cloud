// Package cloudformation is CloudFormation's template engine: the operations
// behavior/aws/cloudformation lists as native.
package cloudformation

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// natives are the operations behavior/aws/cloudformation lists as native:
// the template engine. Parsing a template, resolving its intrinsics and
// provisioning resources through their owning services is Go; the stack,
// change set, stack set and type bookkeeping is the bundle's.
var natives = []string{"CreateStack", "UpdateStack", "DeleteStack", "ExecuteChangeSet", "ValidateTemplate", "GetTemplateSummary", "ListExports"}

func init() {
	for _, op := range natives {
		bundled.RegisterNative("aws.cloudformation", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return New(deps).Invoke(ctx, req)
		})
	}
}

// Pack is the template engine behind the native operations.
type Pack struct{ deps spi.Deps }

// New constructs the template engine.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

// Invoke serves one native operation.
func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "StackName")
	switch req.Operation {
	case "ValidateTemplate":
		body, err := p.templateBody(ctx, req)
		if err != nil {
			return nil, err
		}
		tpl, err := parseTemplate(body)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Description": str(tpl["Description"]), "Parameters": paramDecls(tpl)}}, nil
	case "CreateStack", "UpdateStack":
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack")
	case "DeleteStack":
		return p.drop(ctx, req, name)
	case "GetTemplateSummary":
		body, bodyErr := p.templateBody(ctx, req)
		tpl, err := parseTemplate(body)
		if bodyErr != nil {
			err = bodyErr
		}
		if err != nil {
			if name != "" {
				st, lerr := p.load(ctx, req, name)
				if lerr != nil {
					return nil, err
				}
				tpl, err = parseTemplate(st.Template)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		var types []any
		if res, ok := tpl["Resources"].(map[string]any); ok {
			for _, v := range res {
				m, _ := v.(map[string]any)
				if t := str(m["Type"]); t != "" {
					types = append(types, t)
				}
			}
		}
		return &spi.Response{Output: map[string]any{"Parameters": paramDecls(tpl), "ResourceTypes": types}}, nil
	case "ListExports":
		kvs, _, _ := p.col(req, "cfn").List(ctx, "", "", 0)
		var ex []any
		for _, kv := range kvs {
			var st stack
			_ = json.Unmarshal(kv.Value, &st)
			for _, o := range st.Outputs {
				m, _ := o.(map[string]any)
				ex = append(ex, map[string]any{"Name": m["OutputKey"], "Value": m["OutputValue"], "ExportingStackId": st.ID})
			}
		}
		return &spi.Response{Output: map[string]any{"Exports": ex}}, nil
	case "ExecuteChangeSet":
		csn := first(req.Input, "ChangeSetName")
		b, ok, _ := p.col(req, "cfn-cs").Get(ctx, name+":"+csn)
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		req.Input["TemplateBody"] = rec["TemplateBody"]
		_, err := p.upsert(ctx, req, name, true)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudformation", req.Operation, "emulate")
	}
}

type stack struct {
	Name, ID, Status, Template string
	Resources                  []res
	Outputs                    []any
	Refs                       map[string]string
	Protect                    bool
}

type res struct {
	Logical, Physical, Type string
}

func (p *Pack) upsert(ctx context.Context, req *spi.Request, name string, update bool) (*spi.Response, error) {
	if name == "" {
		return nil, &spi.Fault{Code: "ValidationError", Message: "StackName required", HTTPStatus: 400, Fault: "client"}
	}
	body, err := p.templateBody(ctx, req)
	if err != nil {
		return nil, err
	}
	tpl, err := parseTemplate(body)
	if err != nil {
		return nil, err
	}
	params := formParams(req.Input)
	mergeParamDefaults(tpl, params)
	id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/" + p.deps.Rand.Hex(8)
	if update {
		if old, err := p.load(ctx, req, name); err == nil {
			id = old.ID
		}
	}
	st := stack{Name: name, ID: id, Status: "CREATE_COMPLETE", Template: body, Refs: map[string]string{}}
	for k, v := range params {
		st.Refs[k] = v
	}
	if update {
		st.Status = "UPDATE_COMPLETE"
	}
	resources, _ := tpl["Resources"].(map[string]any)
	keys := make([]string, 0, len(resources))
	for k := range resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, logical := range keys {
		rm, _ := resources[logical].(map[string]any)
		typ := str(rm["Type"])
		props, _ := p.resolve(rm["Properties"], st.Refs, req, name).(map[string]any)
		if props == nil {
			props = map[string]any{}
		}
		phys, err := p.provision(ctx, req, typ, logical, name, props)
		if err != nil {
			return nil, err
		}
		st.Refs[logical] = phys
		st.Resources = append(st.Resources, res{Logical: logical, Physical: phys, Type: typ})
	}
	if outs, ok := tpl["Outputs"].(map[string]any); ok {
		okeys := make([]string, 0, len(outs))
		for k := range outs {
			okeys = append(okeys, k)
		}
		sort.Strings(okeys)
		for _, k := range okeys {
			om, _ := outs[k].(map[string]any)
			val := p.resolve(om["Value"], st.Refs, req, name)
			st.Outputs = append(st.Outputs, map[string]any{"OutputKey": k, "OutputValue": val})
		}
	}
	raw, _ := json.Marshal(st)
	_ = p.col(req, "cfn").Put(ctx, name, raw)
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

func (p *Pack) templateBody(ctx context.Context, req *spi.Request) (string, error) {
	if body := str(req.Input["TemplateBody"]); body != "" {
		return body, nil
	}
	location := str(req.Input["TemplateURL"])
	parsed, err := url.Parse(location)
	if err != nil || location == "" {
		return "", &spi.Fault{Code: "ValidationError", Message: "TemplateBody or TemplateURL required", HTTPStatus: 400, Fault: "client"}
	}
	bucket, key := "", strings.TrimPrefix(parsed.Path, "/")
	if parsed.Scheme == "s3" {
		bucket = parsed.Host
	} else if i := strings.Index(strings.ToLower(parsed.Host), ".s3"); i > 0 {
		bucket = parsed.Host[:i]
	} else if slash := strings.IndexByte(key, '/'); slash > 0 {
		bucket, key = key[:slash], key[slash+1:]
	}
	if bucket == "" || key == "" {
		return "", &spi.Fault{Code: "ValidationError", Message: "TemplateURL must identify an S3 object", HTTPStatus: 400, Fault: "client"}
	}
	reader, _, err := p.deps.Blobs.Get(ctx, req.Identity.Account+"/"+req.Identity.Region+"/"+bucket+"/"+key)
	if err != nil {
		return "", &spi.Fault{Code: "ValidationError", Message: "TemplateURL object not found", HTTPStatus: 400, Fault: "client"}
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (p *Pack) drop(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	if err != nil {
		return nil, err
	}
	if st.Protect {
		return nil, &spi.Fault{Code: "ValidationError", Message: "stack has termination protection", HTTPStatus: 400, Fault: "client"}
	}
	for i := len(st.Resources) - 1; i >= 0; i-- {
		p.deprovision(ctx, req, st.Resources[i])
	}
	_ = p.col(req, "cfn").Delete(ctx, name)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) load(ctx context.Context, req *spi.Request, name string) (stack, error) {
	var st stack
	b, ok, _ := p.col(req, "cfn").Get(ctx, name)
	if !ok {
		return st, &spi.Fault{Code: "ValidationError", Message: "Stack [" + name + "] does not exist", HTTPStatus: 400, Fault: "client"}
	}
	_ = json.Unmarshal(b, &st)
	return st, nil
}

// provision creates a resource through the service that owns it, and answers
// the resource's physical id. Going through the owner's API keeps its store
// layout, validation and cascades that service's business.
func (p *Pack) provision(ctx context.Context, req *spi.Request, typ, logical, stack string, props map[string]any) (string, error) {
	name := func(member, fallback string) string {
		if n := str(props[member]); n != "" {
			return n
		}
		return fallback
	}
	switch typ {
	case "AWS::S3::Bucket":
		n := name("BucketName", strings.ToLower(stack+"-"+logical))
		if _, err := p.call(ctx, req, "aws.s3", "CreateBucket", map[string]any{"Bucket": n}); err != nil {
			return "", err
		}
		return n, p.configureBucket(ctx, req, n, props)
	case "AWS::SQS::Queue":
		out, err := p.call(ctx, req, "aws.sqs", "CreateQueue", map[string]any{"QueueName": name("QueueName", logical)})
		return str(out["QueueUrl"]), err
	case "AWS::SNS::Topic":
		out, err := p.call(ctx, req, "aws.sns", "CreateTopic", map[string]any{"Name": name("TopicName", logical)})
		return str(out["TopicArn"]), err
	case "AWS::DynamoDB::Table":
		in := maps.Clone(props)
		in["TableName"] = name("TableName", logical)
		_, err := p.call(ctx, req, "aws.dynamodb", "CreateTable", in)
		return str(in["TableName"]), err
	case "AWS::IAM::Role":
		out, err := p.call(ctx, req, "aws.iam", "CreateRole", map[string]any{"RoleName": name("RoleName", logical), "AssumeRolePolicyDocument": jsonText(props["AssumeRolePolicyDocument"])})
		return str(asMap(out["Role"])["Arn"]), err
	case "AWS::SSM::Parameter":
		n := name("Name", "/"+stack+"/"+logical)
		_, err := p.call(ctx, req, "aws.ssm", "PutParameter", map[string]any{"Name": n, "Value": str(props["Value"]), "Type": name("Type", "String")})
		return n, err
	case "AWS::SecretsManager::Secret":
		out, err := p.call(ctx, req, "aws.secretsmanager", "CreateSecret", map[string]any{"Name": name("Name", logical), "SecretString": str(props["SecretString"])})
		return str(out["ARN"]), err
	case "AWS::KMS::Key":
		out, err := p.call(ctx, req, "aws.kms", "CreateKey", map[string]any{"Description": str(props["Description"])})
		return str(asMap(out["KeyMetadata"])["KeyId"]), err
	case "AWS::Logs::LogGroup":
		n := name("LogGroupName", "/aws/"+stack+"/"+logical)
		_, err := p.call(ctx, req, "aws.logs", "CreateLogGroup", map[string]any{"logGroupName": n})
		return n, err
	case "AWS::Events::Rule":
		in := map[string]any{"Name": name("Name", logical)}
		for _, member := range []string{"ScheduleExpression", "State", "Description", "EventBusName"} {
			if v, ok := props[member]; ok {
				in[member] = v
			}
		}
		if pattern, ok := props["EventPattern"]; ok {
			in["EventPattern"] = jsonText(pattern)
		}
		out, err := p.call(ctx, req, "aws.events", "PutRule", in)
		if targets, ok := props["Targets"]; ok && err == nil {
			_, err = p.call(ctx, req, "aws.events", "PutTargets", map[string]any{"Rule": in["Name"], "Targets": targets})
		}
		return str(out["RuleArn"]), err
	case "AWS::Lambda::Function":
		in := maps.Clone(props)
		in["FunctionName"] = name("FunctionName", logical)
		out, err := p.call(ctx, req, "aws.lambda", "CreateFunction", in)
		return str(out["FunctionArn"]), err
	case "AWS::Kinesis::Stream":
		n := name("Name", logical)
		_, err := p.call(ctx, req, "aws.kinesis", "CreateStream", map[string]any{"StreamName": n})
		return n, err
	case "AWS::Kinesis::ResourcePolicy":
		arn := name("ResourceArn", str(props["ResourceARN"]))
		policy := props["ResourcePolicy"]
		if policy == nil {
			policy = props["Policy"]
		}
		_, err := p.call(ctx, req, "aws.kinesis", "PutResourcePolicy", map[string]any{"ResourceARN": arn, "Policy": jsonText(policy)})
		return arn, err
	case "AWS::ApiGateway::RestApi":
		out, err := p.call(ctx, req, "aws.apigateway", "CreateRestApi", map[string]any{"name": name("Name", logical)})
		return str(out["id"]), err
	default:
		return "", &spi.Fault{Code: "ValidationError", Message: "unsupported resource type " + typ, HTTPStatus: 400, Fault: "client"}
	}
}

// deprovision deletes a resource through its owner. Failures are ignored, as
// a stack delete proceeds past a resource that is already gone.
func (p *Pack) deprovision(ctx context.Context, req *spi.Request, r res) {
	id := r.Physical
	del := map[string]struct {
		svc, op string
		in      map[string]any
	}{
		"AWS::S3::Bucket":              {"aws.s3", "DeleteBucket", map[string]any{"Bucket": id}},
		"AWS::SQS::Queue":              {"aws.sqs", "DeleteQueue", map[string]any{"QueueUrl": id}},
		"AWS::SNS::Topic":              {"aws.sns", "DeleteTopic", map[string]any{"TopicArn": id}},
		"AWS::DynamoDB::Table":         {"aws.dynamodb", "DeleteTable", map[string]any{"TableName": id}},
		"AWS::IAM::Role":               {"aws.iam", "DeleteRole", map[string]any{"RoleName": lastSlash(id)}},
		"AWS::SSM::Parameter":          {"aws.ssm", "DeleteParameter", map[string]any{"Name": id}},
		"AWS::SecretsManager::Secret":  {"aws.secretsmanager", "DeleteSecret", map[string]any{"SecretId": id, "ForceDeleteWithoutRecovery": true}},
		"AWS::KMS::Key":                {"aws.kms", "ScheduleKeyDeletion", map[string]any{"KeyId": id, "PendingWindowInDays": 7}},
		"AWS::Logs::LogGroup":          {"aws.logs", "DeleteLogGroup", map[string]any{"logGroupName": id}},
		"AWS::Events::Rule":            {"aws.events", "DeleteRule", map[string]any{"Name": lastSlash(id), "Force": true}},
		"AWS::Lambda::Function":        {"aws.lambda", "DeleteFunction", map[string]any{"FunctionName": lastColon(id)}},
		"AWS::Kinesis::Stream":         {"aws.kinesis", "DeleteStream", map[string]any{"StreamName": id}},
		"AWS::Kinesis::ResourcePolicy": {"aws.kinesis", "DeleteResourcePolicy", map[string]any{"ResourceARN": id}},
		"AWS::ApiGateway::RestApi":     {"aws.apigateway", "DeleteRestApi", map[string]any{"restApiId": id}},
	}[r.Type]
	if del.svc != "" {
		_, _ = p.call(ctx, req, del.svc, del.op, del.in)
	}
}

// call invokes another service the way the edge would: through whatever the
// registry serves it with, pack or bundle. A pack that runs a worker is
// closed again, since this construction is per call.
func (p *Pack) call(ctx context.Context, req *spi.Request, id, op string, in map[string]any) (map[string]any, error) {
	for _, f := range registry.Factories() {
		if f.ServiceID != id {
			continue
		}
		pack, err := f.New(p.deps)
		if err != nil {
			return nil, err
		}
		if closer, ok := pack.(interface{ Close() error }); ok {
			defer closer.Close()
		}
		resp, err := pack.Invoke(ctx, &spi.Request{Identity: req.Identity, AdvertiseURL: advertise(req), Operation: op, Input: in})
		if err != nil {
			return nil, err
		}
		return resp.Output, nil
	}
	return nil, spi.NotImplemented(id, op, "emulate")
}

// configureBucket applies the bucket properties that map one to one onto an
// S3 operation, and refuses the rest by name rather than storing a shape S3
// does not read.
func (p *Pack) configureBucket(ctx context.Context, req *spi.Request, bucket string, props map[string]any) error {
	// The S3 pack reads Status and TagSet lifted to the top of the input, as
	// the restXml decoder hands them over (its liftedMembers debt).
	for property, apply := range map[string]func(v any) (string, map[string]any){
		"VersioningConfiguration": func(v any) (string, map[string]any) {
			return "PutBucketVersioning", map[string]any{"VersioningConfiguration": v, "Status": asMap(v)["Status"]}
		},
		"Tags": func(v any) (string, map[string]any) {
			return "PutBucketTagging", map[string]any{"Tagging": map[string]any{"TagSet": v}, "TagSet": v}
		},
		"PublicAccessBlockConfiguration": func(v any) (string, map[string]any) {
			return "PutPublicAccessBlock", map[string]any{"PublicAccessBlockConfiguration": v}
		},
		"OwnershipControls": func(v any) (string, map[string]any) {
			return "PutBucketOwnershipControls", map[string]any{"OwnershipControls": v}
		},
	} {
		if v, ok := props[property]; ok {
			op, in := apply(v)
			in["Bucket"] = bucket
			if _, err := p.call(ctx, req, "aws.s3", op, in); err != nil {
				return err
			}
		}
	}
	// ponytail: the configurations with no one-to-one S3 operation are stored
	// in CloudFormation's own shape, which S3 does not read and Cloud Control
	// reads back. Mapping each onto its S3 operation (CorsRules to CORSRules,
	// and so on) is the upgrade, in both directions.
	for property, kind := range map[string]string{
		"CorsConfiguration": "cors", "BucketEncryption": "encryption", "LifecycleConfiguration": "lifecycle",
		"ReplicationConfiguration": "replication", "NotificationConfiguration": "notification", "WebsiteConfiguration": "website",
	} {
		if value, ok := props[property]; ok {
			raw, _ := json.Marshal(map[string]any{property: value})
			_ = p.col(req, "bktcfg").Put(ctx, bucket+"/"+kind, raw)
		}
	}
	return nil
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// jsonText answers a policy-like property as the JSON text an API takes; a
// template may give it as an object or as a string already.
func jsonText(v any) string {
	if text, ok := v.(string); ok {
		return text
	}
	raw, _ := json.Marshal(v)
	return string(raw)
}

func (p *Pack) resolve(v any, refs map[string]string, req *spi.Request, stack string) any {
	switch t := v.(type) {
	case map[string]any:
		if r, ok := t["Ref"]; ok {
			return p.ref(str(r), refs, req, stack)
		}
		if g, ok := t["Fn::GetAtt"]; ok {
			return p.getAtt(g, refs, req)
		}
		if j, ok := t["Fn::Join"]; ok {
			return p.fnJoin(j, refs, req, stack)
		}
		if s, ok := t["Fn::Sub"]; ok {
			return p.fnSub(s, refs, req, stack)
		}
		out := map[string]any{}
		for k, val := range t {
			out[k] = p.resolve(val, refs, req, stack)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = p.resolve(val, refs, req, stack)
		}
		return out
	default:
		return v
	}
}

func (p *Pack) ref(name string, refs map[string]string, req *spi.Request, stack string) string {
	switch name {
	case "AWS::AccountId":
		return req.Identity.Account
	case "AWS::Region":
		return req.Identity.Region
	case "AWS::StackName":
		return stack
	case "AWS::Partition":
		return "aws"
	case "AWS::URLSuffix":
		return "amazonaws.com"
	}
	if v, ok := refs[name]; ok {
		return v
	}
	return name
}

func (p *Pack) getAtt(g any, refs map[string]string, req *spi.Request) string {
	var logical, attr string
	switch t := g.(type) {
	case string:
		parts := strings.SplitN(t, ".", 2)
		logical = parts[0]
		if len(parts) > 1 {
			attr = parts[1]
		}
	case []any:
		if len(t) > 0 {
			logical = str(t[0])
		}
		if len(t) > 1 {
			attr = str(t[1])
		}
	}
	phys := refs[logical]
	switch attr {
	case "Arn":
		if strings.Contains(phys, "arn:aws:") {
			return phys
		}
		if strings.Contains(phys, "/") && strings.Contains(phys, "://") {
			// queue URL -> ARN
			name := lastSlash(phys)
			return "arn:aws:sqs:" + req.Identity.Region + ":" + req.Identity.Account + ":" + name
		}
		return "arn:aws:s3:::" + phys
	case "QueueName":
		return lastSlash(phys)
	default:
		return phys
	}
}

func (p *Pack) fnJoin(j any, refs map[string]string, req *spi.Request, stack string) string {
	arr, _ := j.([]any)
	if len(arr) < 2 {
		return ""
	}
	sep := str(arr[0])
	parts, _ := arr[1].([]any)
	ss := make([]string, 0, len(parts))
	for _, x := range parts {
		ss = append(ss, str(p.resolve(x, refs, req, stack)))
	}
	return strings.Join(ss, sep)
}

func (p *Pack) fnSub(s any, refs map[string]string, req *spi.Request, stack string) string {
	tmpl := str(s)
	if arr, ok := s.([]any); ok && len(arr) > 0 {
		tmpl = str(arr[0])
	}
	out := tmpl
	for k, v := range refs {
		out = strings.ReplaceAll(out, "${"+k+"}", v)
	}
	out = strings.ReplaceAll(out, "${AWS::AccountId}", req.Identity.Account)
	out = strings.ReplaceAll(out, "${AWS::Region}", req.Identity.Region)
	out = strings.ReplaceAll(out, "${AWS::StackName}", stack)
	return out
}

func parseTemplate(body string) (map[string]any, error) {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil, &spi.Fault{Code: "ValidationError", Message: "empty TemplateBody", HTTPStatus: 400, Fault: "client"}
	}
	if body[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			return nil, &spi.Fault{Code: "ValidationError", Message: err.Error(), HTTPStatus: 400, Fault: "client"}
		}
		return m, nil
	}
	m, err := parseYAML(body)
	if err != nil {
		return nil, &spi.Fault{Code: "ValidationError", Message: "TemplateBody YAML: " + err.Error(), HTTPStatus: 400, Fault: "client"}
	}
	return m, nil
}

func formParams(in map[string]any) map[string]string {
	keys, vals := map[string]string{}, map[string]string{}
	for k, v := range in {
		if !strings.HasPrefix(k, "Parameters.member.") {
			continue
		}
		if strings.HasSuffix(k, ".ParameterKey") {
			keys[strings.TrimSuffix(k, ".ParameterKey")] = str(v)
		}
		if strings.HasSuffix(k, ".ParameterValue") {
			vals[strings.TrimSuffix(k, ".ParameterValue")] = str(v)
		}
	}
	out := map[string]string{}
	for pfx, k := range keys {
		out[k] = vals[pfx]
	}
	return out
}

func mergeParamDefaults(tpl map[string]any, params map[string]string) {
	pm, _ := tpl["Parameters"].(map[string]any)
	for k, v := range pm {
		if _, ok := params[k]; ok {
			continue
		}
		m, _ := v.(map[string]any)
		if d, ok := m["Default"]; ok {
			params[k] = str(d)
		}
	}
	for k, v := range params {
		if tpl["_params"] == nil {
			tpl["_params"] = map[string]any{}
		}
		tpl["_params"].(map[string]any)[k] = v
	}
}

func paramDecls(tpl map[string]any) []any {
	pm, _ := tpl["Parameters"].(map[string]any)
	var out []any
	keys := make([]string, 0, len(pm))
	for k := range pm {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, map[string]any{"ParameterKey": k})
	}
	return out
}

func advertise(req *spi.Request) string {
	if req.HTTP != nil && req.HTTP.Host != "" {
		return "http://" + req.HTTP.Host
	}
	return "http://127.0.0.1:4566"
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func lastSlash(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func lastColon(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[i+1:]
	}
	return s
}
