// Package cloudformation emulates CreateStack for a fixed resource-type set.
package cloudformation

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.cloudformation", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements CloudFormation-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.cloudformation" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateStack", "UpdateStack", "DeleteStack", "DescribeStacks", "ListStacks",
		"GetTemplate", "ListStackResources", "DescribeStackEvents", "ValidateTemplate",
		"DescribeStackResource", "GetTemplateSummary", "ListExports",
		"CreateChangeSet", "DescribeChangeSet", "ExecuteChangeSet", "DeleteChangeSet", "ListChangeSets",
		"UpdateTerminationProtection", "SignalResource"}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

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
	case "DescribeStacks":
		return p.describe(ctx, req, name)
	case "ListStacks":
		kvs, _, _ := p.col(req, "cfn").List(ctx, "", "", 0)
		var sums []any
		for _, kv := range kvs {
			var st stack
			_ = json.Unmarshal(kv.Value, &st)
			sums = append(sums, map[string]any{"StackName": st.Name, "StackId": st.ID, "StackStatus": st.Status})
		}
		return &spi.Response{Output: map[string]any{"StackSummaries": sums}}, nil
	case "GetTemplate":
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"TemplateBody": st.Template, "StagesAvailable": []any{"Original"}}}, nil
	case "ListStackResources":
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		var rs []any
		for _, r := range st.Resources {
			rs = append(rs, map[string]any{"LogicalResourceId": r.Logical, "PhysicalResourceId": r.Physical, "ResourceType": r.Type, "ResourceStatus": "CREATE_COMPLETE"})
		}
		return &spi.Response{Output: map[string]any{"StackResourceSummaries": rs}}, nil
	case "DescribeStackEvents":
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		var ev []any
		for _, r := range st.Resources {
			ev = append(ev, map[string]any{"LogicalResourceId": r.Logical, "PhysicalResourceId": r.Physical, "ResourceType": r.Type, "ResourceStatus": "CREATE_COMPLETE", "StackName": st.Name, "StackId": st.ID})
		}
		ev = append(ev, map[string]any{"LogicalResourceId": st.Name, "ResourceType": "AWS::CloudFormation::Stack", "ResourceStatus": st.Status, "StackName": st.Name, "StackId": st.ID})
		return &spi.Response{Output: map[string]any{"StackEvents": ev}}, nil
	case "DescribeStackResource":
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		want := first(req.Input, "LogicalResourceId")
		for _, r := range st.Resources {
			if r.Logical == want {
				return &spi.Response{Output: map[string]any{"StackResourceDetail": map[string]any{
					"LogicalResourceId": r.Logical, "PhysicalResourceId": r.Physical, "ResourceType": r.Type,
					"ResourceStatus": "CREATE_COMPLETE", "StackName": st.Name, "StackId": st.ID,
				}}}, nil
			}
		}
		return nil, &spi.Fault{Code: "ValidationError", Message: "resource not found", HTTPStatus: 400, Fault: "client"}
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
	case "CreateChangeSet":
		csn := first(req.Input, "ChangeSetName")
		body, err := p.templateBody(ctx, req)
		if err != nil {
			return nil, err
		}
		id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":changeSet/" + csn + "/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"ChangeSetName": csn, "ChangeSetId": id, "StackName": name, "Status": "CREATE_COMPLETE", "TemplateBody": body}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "cfn-cs").Put(ctx, name+":"+csn, b)
		return &spi.Response{Output: map[string]any{"Id": id, "StackId": "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name}}, nil
	case "DescribeChangeSet":
		csn := first(req.Input, "ChangeSetName")
		b, ok, _ := p.col(req, "cfn-cs").Get(ctx, name+":"+csn)
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListChangeSets":
		kvs, _, _ := p.col(req, "cfn-cs").List(ctx, name+":", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"Summaries": items}}, nil
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
	case "DeleteChangeSet":
		_ = p.col(req, "cfn-cs").Delete(ctx, name+":"+first(req.Input, "ChangeSetName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "UpdateTerminationProtection":
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		st.Protect = truthy(req.Input["EnableTerminationProtection"])
		raw, _ := json.Marshal(st)
		_ = p.col(req, "cfn").Put(ctx, name, raw)
		return &spi.Response{Output: map[string]any{"StackId": st.ID}}, nil
	case "SignalResource":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
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

func (p *Pack) describe(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	var stacks []any
	if name != "" {
		st, err := p.load(ctx, req, name)
		if err != nil {
			return nil, err
		}
		stacks = append(stacks, wireStack(st))
	} else {
		kvs, _, _ := p.col(req, "cfn").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var st stack
			_ = json.Unmarshal(kv.Value, &st)
			stacks = append(stacks, wireStack(st))
		}
	}
	return &spi.Response{Output: map[string]any{"Stacks": stacks}}, nil
}

func wireStack(st stack) map[string]any {
	return map[string]any{"StackName": st.Name, "StackId": st.ID, "StackStatus": st.Status, "Outputs": st.Outputs}
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

func (p *Pack) provision(ctx context.Context, req *spi.Request, typ, logical, stack string, props map[string]any) (string, error) {
	acct, region := req.Identity.Account, req.Identity.Region
	switch typ {
	case "AWS::S3::Bucket":
		name := str(props["BucketName"])
		if name == "" {
			name = strings.ToLower(stack + "-" + logical)
		}
		meta, _ := json.Marshal(map[string]any{"name": name, "region": region})
		_ = p.col(req, "buckets").Put(ctx, name, meta)
		p.configureBucket(ctx, req, name, props)
		return name, nil
	case "AWS::SQS::Queue":
		q := str(props["QueueName"])
		if q == "" {
			q = logical
		}
		url := fmt.Sprintf("%s/%s/%s", advertise(req), acct, q)
		meta, _ := json.Marshal(map[string]any{"url": url, "name": q, "attrs": map[string]any{}, "seq": 0})
		_ = p.col(req, "queues").Put(ctx, q, meta)
		return url, nil
	case "AWS::SNS::Topic":
		n := str(props["TopicName"])
		if n == "" {
			n = logical
		}
		arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, acct, n)
		b, _ := json.Marshal(map[string]any{"arn": arn, "name": n})
		_ = p.col(req, "topics").Put(ctx, n, b)
		return arn, nil
	case "AWS::DynamoDB::Table":
		n := str(props["TableName"])
		if n == "" {
			n = logical
		}
		props["TableName"] = n
		b, _ := json.Marshal(props)
		_ = p.col(req, "tables").Put(ctx, n, b)
		return n, nil
	case "AWS::IAM::Role":
		n := str(props["RoleName"])
		if n == "" {
			n = logical
		}
		rec := map[string]any{"RoleName": n, "Arn": "arn:aws:iam::" + acct + ":role/" + n, "AssumeRolePolicyDocument": props["AssumeRolePolicyDocument"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "iam").Put(ctx, "role:"+n, b)
		return str(rec["Arn"]), nil
	case "AWS::SSM::Parameter":
		n := str(props["Name"])
		if n == "" {
			n = "/" + stack + "/" + logical
		}
		rec := map[string]any{"Name": n, "Value": str(props["Value"]), "Type": str(props["Type"]), "Version": 1}
		if rec["Type"] == "" {
			rec["Type"] = "String"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ssm").Put(ctx, n, b)
		return n, nil
	case "AWS::SecretsManager::Secret":
		n := str(props["Name"])
		if n == "" {
			n = logical
		}
		arn := "arn:aws:secretsmanager:" + region + ":" + acct + ":secret:" + n
		rec := map[string]any{"Name": n, "ARN": arn, "SecretString": str(props["SecretString"])}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "secrets").Put(ctx, n, b)
		return arn, nil
	case "AWS::KMS::Key":
		id := p.deps.Rand.Hex(8)
		mat := p.deps.Rand.Bytes(32)
		arn := "arn:aws:kms:" + region + ":" + acct + ":key/" + id
		rec := map[string]any{"KeyId": id, "Arn": arn, "KeyMaterial": base64.StdEncoding.EncodeToString(mat), "KeyState": "Enabled"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "kms").Put(ctx, id, b)
		return id, nil
	case "AWS::Logs::LogGroup":
		n := str(props["LogGroupName"])
		if n == "" {
			n = "/aws/" + stack + "/" + logical
		}
		_ = p.col(req, "groups").Put(ctx, n, []byte(n))
		return n, nil
	case "AWS::Events::Rule":
		n := str(props["Name"])
		if n == "" {
			n = logical
		}
		b, _ := json.Marshal(props)
		_ = p.col(req, "rules").Put(ctx, n, b)
		return "arn:aws:events:" + region + ":" + acct + ":rule/" + n, nil
	case "AWS::Lambda::Function":
		n := str(props["FunctionName"])
		if n == "" {
			n = logical
		}
		rec := map[string]any{"FunctionName": n, "Runtime": props["Runtime"], "Handler": props["Handler"], "Code": props["Code"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "lambda").Put(ctx, n, b)
		return "arn:aws:lambda:" + region + ":" + acct + ":function:" + n, nil
	case "AWS::Kinesis::Stream":
		n := str(props["Name"])
		if n == "" {
			n = logical
		}
		rec := map[string]any{"StreamName": n, "Status": "ACTIVE", "Seq": 0}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "kinesis").Put(ctx, n, b)
		return n, nil
	case "AWS::Kinesis::ResourcePolicy":
		arn := str(props["ResourceArn"])
		if arn == "" {
			arn = str(props["ResourceARN"])
		}
		policy := props["ResourcePolicy"]
		if policy == nil {
			policy = props["Policy"]
		}
		if text, ok := policy.(string); ok {
			_ = p.col(req, "kpol").Put(ctx, arn, []byte(text))
		} else {
			body, _ := json.Marshal(policy)
			_ = p.col(req, "kpol").Put(ctx, arn, body)
		}
		return arn, nil
	case "AWS::ApiGateway::RestApi":
		n := str(props["Name"])
		if n == "" {
			n = logical
		}
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "name": n, "rootResourceId": "root"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "apigw").Put(ctx, id, b)
		rb, _ := json.Marshal(map[string]any{"id": "root", "path": "/", "pathPart": ""})
		_ = p.col(req, "apigw-res").Put(ctx, id+"/root", rb)
		return id, nil
	default:
		return "", &spi.Fault{Code: "ValidationError", Message: "unsupported resource type " + typ, HTTPStatus: 400, Fault: "client"}
	}
}

func (p *Pack) deprovision(ctx context.Context, req *spi.Request, r res) {
	switch r.Type {
	case "AWS::S3::Bucket":
		_ = p.col(req, "buckets").Delete(ctx, r.Physical)
		_ = p.col(req, "versioning").Delete(ctx, r.Physical)
		_ = p.col(req, "tags").Delete(ctx, r.Physical)
		for _, kind := range []string{"cors", "encryption", "lifecycle", "replication", "notification", "ownershipcontrols", "publicaccessblock", "website"} {
			_ = p.col(req, "bktcfg").Delete(ctx, r.Physical+"/"+kind)
		}
	case "AWS::SQS::Queue":
		_ = p.col(req, "queues").Delete(ctx, lastSlash(r.Physical))
	case "AWS::SNS::Topic":
		_ = p.col(req, "topics").Delete(ctx, lastColon(r.Physical))
	case "AWS::DynamoDB::Table":
		_ = p.col(req, "tables").Delete(ctx, r.Physical)
	case "AWS::IAM::Role":
		_ = p.col(req, "iam").Delete(ctx, "role:"+lastSlash(r.Physical))
	case "AWS::SSM::Parameter":
		_ = p.col(req, "ssm").Delete(ctx, r.Physical)
	case "AWS::SecretsManager::Secret":
		_ = p.col(req, "secrets").Delete(ctx, lastColon(r.Physical))
	case "AWS::KMS::Key":
		_ = p.col(req, "kms").Delete(ctx, r.Physical)
	case "AWS::Logs::LogGroup":
		_ = p.col(req, "groups").Delete(ctx, r.Physical)
	case "AWS::Events::Rule":
		_ = p.col(req, "rules").Delete(ctx, lastSlash(r.Physical))
	case "AWS::Lambda::Function":
		_ = p.col(req, "lambda").Delete(ctx, lastColon(r.Physical))
	case "AWS::Kinesis::Stream":
		_ = p.col(req, "kinesis").Delete(ctx, r.Physical)
	case "AWS::Kinesis::ResourcePolicy":
		_ = p.col(req, "kpol").Delete(ctx, r.Physical)
	case "AWS::ApiGateway::RestApi":
		_ = p.col(req, "apigw").Delete(ctx, r.Physical)
	}
}

func (p *Pack) configureBucket(ctx context.Context, req *spi.Request, bucket string, props map[string]any) {
	if version, ok := props["VersioningConfiguration"].(map[string]any); ok {
		if status := str(version["Status"]); status != "" {
			_ = p.col(req, "versioning").Put(ctx, bucket, []byte(status))
		}
	}
	if tags, ok := props["Tags"]; ok {
		raw, _ := json.Marshal(tags)
		_ = p.col(req, "tags").Put(ctx, bucket, raw)
	}
	configs := map[string]string{
		"CorsConfiguration":              "cors",
		"BucketEncryption":               "encryption",
		"LifecycleConfiguration":         "lifecycle",
		"ReplicationConfiguration":       "replication",
		"NotificationConfiguration":      "notification",
		"OwnershipControls":              "ownershipcontrols",
		"PublicAccessBlockConfiguration": "publicaccessblock",
		"WebsiteConfiguration":           "website",
	}
	for property, kind := range configs {
		if value, ok := props[property]; ok {
			raw, _ := json.Marshal(map[string]any{property: value})
			_ = p.col(req, "bktcfg").Put(ctx, bucket+"/"+kind, raw)
		}
	}
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

func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		return t == "true" || t == "True"
	}
	return false
}
