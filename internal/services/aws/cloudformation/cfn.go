// Package cloudformation is CloudFormation's template engine: the operations
// behavior/aws/cloudformation lists as native.
package cloudformation

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/prim"
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
	name := str(req.Input["StackName"])
	switch req.Operation {
	case "ValidateTemplate":
		_, tpl, err := p.template(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Description": str(tpl["Description"]), "Parameters": paramDecls(tpl)}}, nil
	case "CreateStack", "UpdateStack":
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack")
	case "DeleteStack":
		return p.drop(ctx, req, name)
	case "GetTemplateSummary":
		_, tpl, err := p.template(ctx, req)
		if st, loadErr := p.load(ctx, req, name); err != nil && name != "" && loadErr == nil {
			tpl, err = parseTemplate(st.Template)
		}
		if err != nil {
			return nil, err
		}
		var types []any
		for _, v := range asMap(tpl["Resources"]) {
			if t := str(asMap(v)["Type"]); t != "" {
				types = append(types, t)
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
				ex = append(ex, map[string]any{"Name": asMap(o)["OutputKey"], "Value": asMap(o)["OutputValue"], "ExportingStackId": st.ID})
			}
		}
		return &spi.Response{Output: map[string]any{"Exports": ex}}, nil
	case "ExecuteChangeSet":
		csn := str(req.Input["ChangeSetName"])
		b, ok, _ := p.col(req, "cfn-cs").Get(ctx, name+":"+csn)
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		req.Input["TemplateBody"] = rec["TemplateBody"]
		if _, err := p.upsert(ctx, req, name, true); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.cloudformation", req.Operation, "emulate")
	}
}

type stack struct {
	Name, ID, Status, Template string
	Reason                     string `json:",omitempty"` // StackStatusReason
	Resources                  []res
	Outputs                    []any
	Refs                       map[string]string
	Protect                    bool
}

// res is a resource a stack made, the name it was made under and, once a
// delete of it failed, DELETE_FAILED with the error as its reason.
type res struct {
	Logical, Physical, Type string
	Name, Status, Reason    string `json:",omitempty"`
}

func (p *Pack) upsert(ctx context.Context, req *spi.Request, name string, update bool) (*spi.Response, error) {
	if name == "" {
		return nil, invalid("StackName required")
	}
	body, tpl, err := p.template(ctx, req)
	if err != nil {
		return nil, err
	}
	params := formParams(req.Input)
	mergeParamDefaults(tpl, params)
	id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/" + p.deps.Rand.Hex(8)
	var old stack // what an update finds already provisioned
	if found, err := p.load(ctx, req, name); err == nil && !update {
		return nil, fault("AlreadyExistsException", "Stack ["+name+"] already exists")
	} else if err == nil && (strings.HasPrefix(found.Status, "ROLLBACK_") || found.Status == "DELETE_FAILED") {
		return nil, invalid("Stack:" + found.ID + " is in " + found.Status + " state and can not be updated.")
	} else if err == nil {
		old, id = found, found.ID
	}
	st := stack{Name: name, ID: id, Status: "CREATE_COMPLETE", Template: body, Refs: params}
	if update {
		st.Status = "UPDATE_COMPLETE"
	}
	fail := func(err error) (*spi.Response, error) {
		if st.Reason = err.Error(); !update { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, st, "ROLLBACK_COMPLETE", "ROLLBACK_FAILED")
		}
		return nil, err
	}
	resources, _ := tpl["Resources"].(map[string]any)
	order := slices.Sorted(maps.Keys(resources))
	for _, logical := range order { // a name taken fails the stack before anything is made
		rm := asMap(resources[logical])
		if _, err := p.claim(ctx, req, st, old, logical, str(rm["Type"]), asMap(p.resolve(rm["Properties"], st.Refs, req, name))); err != nil {
			return fail(err)
		}
	}
	for _, logical := range order {
		rm, _ := resources[logical].(map[string]any)
		typ := str(rm["Type"])
		props := map[string]any{}
		maps.Copy(props, asMap(p.resolve(rm["Properties"], st.Refs, req, name)))
		phys, n, err := "", "", error(nil)
		if n, err = p.claim(ctx, req, st, old, logical, typ, props); err == nil { // again: a name may come from a resource made before it
			phys, err = p.provision(ctx, req, typ, logical, n, props, old.Refs[logical], id)
		}
		if phys != "" { // recorded once it exists, so a rollback finds a half-made one too
			st.Refs[logical] = phys
			st.Resources = append(st.Resources, res{Logical: logical, Physical: phys, Type: typ, Name: n})
		}
		if err != nil {
			return fail(err)
		}
	}
	outs := asMap(tpl["Outputs"])
	for _, k := range slices.Sorted(maps.Keys(outs)) {
		st.Outputs = append(st.Outputs, map[string]any{"OutputKey": k, "OutputValue": p.resolve(asMap(outs[k])["Value"], st.Refs, req, name)})
	}
	raw, _ := json.Marshal(st)
	_ = p.col(req, "cfn").Put(ctx, name, raw)
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// template reads the request's template body and parses it.
func (p *Pack) template(ctx context.Context, req *spi.Request) (string, map[string]any, error) {
	body, err := p.templateBody(ctx, req)
	if err != nil {
		return "", nil, err
	}
	tpl, err := parseTemplate(body)
	return body, tpl, err
}

func (p *Pack) templateBody(ctx context.Context, req *spi.Request) (string, error) {
	if body := str(req.Input["TemplateBody"]); body != "" {
		return body, nil
	}
	location := str(req.Input["TemplateURL"])
	parsed, err := url.Parse(location)
	if err != nil || location == "" {
		return "", invalid("TemplateBody or TemplateURL required")
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
		return "", invalid("TemplateURL must identify an S3 object")
	}
	reader, _, err := p.deps.Blobs.Get(ctx, req.Identity.Account+"/"+req.Identity.Region+"/"+bucket+"/"+key)
	if err != nil {
		return "", invalid("TemplateURL object not found")
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	return string(body), err
}

func (p *Pack) drop(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	if err != nil {
		return nil, err
	}
	if st.Protect {
		return nil, invalid("stack has termination protection")
	}
	if err := p.unwind(ctx, req, st, "DELETE_COMPLETE", "DELETE_FAILED"); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

// unwind deletes a stack's resources, newest first, as a rollback or a
// DeleteStack does, and records the outcome. With nothing left the stack is
// done, keeping st's reason (a DELETE_COMPLETE stack is removed); else failed, the
// resources left listed as DELETE_FAILED for the next DeleteStack to try
// again and the first error, which unwind answers, its StackStatusReason.
func (p *Pack) unwind(ctx context.Context, req *spi.Request, st stack, done, failed string) error {
	left := []res{} // never null: every read of the stack lists its resources
	var first error
	for _, r := range slices.Backward(st.Resources) {
		if err := p.deprovision(ctx, req, r); err != nil {
			r.Status, r.Reason, first = "DELETE_FAILED", err.Error(), cmp.Or(first, err)
			left = append([]res{r}, left...)
		}
	}
	st.Resources, st.Status = left, done
	if first != nil {
		st.Status, st.Reason = failed, first.Error()
	}
	if st.Status == "DELETE_COMPLETE" {
		return p.col(req, "cfn").Delete(ctx, st.Name)
	}
	raw, _ := json.Marshal(st)
	return cmp.Or(first, p.col(req, "cfn").Put(ctx, st.Name, raw))
}

func (p *Pack) load(ctx context.Context, req *spi.Request, name string) (stack, error) {
	var st stack
	b, ok, _ := p.col(req, "cfn").Get(ctx, name)
	if !ok {
		return st, invalid("Stack [" + name + "] does not exist")
	}
	_ = json.Unmarshal(b, &st)
	return st, nil
}

// kinds are the types provision names: the property that names one, the longest name its service takes
// (API Gateway sets none for a REST API, nor holds a name to one; 1024 only bounds it), a read that finds
// one by name, and the service's error for a name taken (SNS's CreateTopic "is idempotent": CloudFormation's).
var kinds = map[string]struct {
	member            string
	max               int
	svc, op, in, code string
}{
	"AWS::S3::Bucket":             {"BucketName", 63, "aws.s3", "HeadBucket", "Bucket", "BucketAlreadyOwnedByYou"},
	"AWS::SQS::Queue":             {"QueueName", 80, "aws.sqs", "GetQueueUrl", "QueueName", "QueueAlreadyExists"},
	"AWS::SNS::Topic":             {"TopicName", 256, "aws.sns", "GetTopicAttributes", "TopicArn", "AlreadyExistsException"},
	"AWS::DynamoDB::Table":        {"TableName", 255, "aws.dynamodb", "DescribeTable", "TableName", "ResourceInUseException"},
	"AWS::IAM::Role":              {"RoleName", 64, "aws.iam", "GetRole", "RoleName", "EntityAlreadyExists"},
	"AWS::SSM::Parameter":         {"Name", 2048, "aws.ssm", "GetParameter", "Name", "ParameterAlreadyExists"},
	"AWS::SecretsManager::Secret": {"Name", 512, "aws.secretsmanager", "DescribeSecret", "SecretId", "ResourceExistsException"},
	"AWS::Logs::LogGroup":         {"LogGroupName", 512, "aws.logs", "DescribeLogGroups", "logGroupNamePrefix", "ResourceAlreadyExistsException"},
	"AWS::Events::Rule":           {"Name", 64, "aws.events", "DescribeRule", "Name", "ResourceAlreadyExistsException"},
	"AWS::Lambda::Function":       {"FunctionName", 64, "aws.lambda", "GetFunction", "FunctionName", "ResourceConflictException"},
	"AWS::Kinesis::Stream":        {"Name", 128, "aws.kinesis", "DescribeStreamSummary", "StreamName", "ResourceInUseException"},
	"AWS::ApiGateway::RestApi":    {"Name", 1024, "", "", "", ""},
}

// claim answers the name a resource is made under: the template's, or else physicalName's. A given name
// that a resource the stack did not make under it holds is the service's error for a name taken: several
// services answer such a create with what they hold, which a rollback or DeleteStack would then delete.
func (p *Pack) claim(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (string, error) {
	k, n := kinds[typ], str(props[kinds[typ].member])
	if n == "" {
		return physicalName(st.Name, logical, st.ID, typ, props), nil
	}
	id := n
	if k.in == "TopicArn" {
		id = "arn:aws:sns:" + req.Identity.Region + ":" + req.Identity.Account + ":" + n
	}
	out, err := p.call(ctx, req, k.svc, k.op, given(map[string]any{k.in: id}, props, "EventBusName"))
	groups, list := out["logGroups"].([]any) // DescribeLogGroups answers a list by prefix, never a fault
	if err != nil || list && !slices.ContainsFunc(groups, func(g any) bool { return str(asMap(g)["logGroupName"]) == n }) ||
		slices.ContainsFunc(old.Resources, func(r res) bool { // a record from before res.Name: by the physical id, which ends in the name
			return r.Logical == logical && (r.Name == n || r.Name == "" && (r.Physical == n || strings.HasSuffix(r.Physical, "/"+n) || strings.HasSuffix(r.Physical, ":"+n)))
		}) {
		return n, nil
	}
	return "", fault(k.code, "Resource of type '"+typ+"' with identifier '"+n+"' already exists.")
}

var unnameable = regexp.MustCompile(`[^A-Za-z0-9-]+`)

func fifo(p map[string]any) bool { return p["FifoQueue"] == true || str(p["FifoQueue"]) == "true" }

// physicalName is the name of a resource the template leaves unnamed, in the modelled form
// <stack>-<logical id>-<suffix>. The suffix is twelve hex digits of a hash of the stack id and logical
// id, so no two stacks, and no two resources of one stack, share a name, and an update finds the name its
// create made. Only letters, digits and hyphens are kept; a bucket's name is lower case and a FIFO
// queue's ends in .fifo; the stack name and logical id, never the suffix, are cut to the longest name.
func physicalName(stack, logical, stackID, typ string, props map[string]any) string {
	k, named := kinds[typ]
	if !named {
		return ""
	}
	sum := sha256.Sum256([]byte(stackID + "/" + logical))
	end := "-" + hex.EncodeToString(sum[:6])
	if typ == "AWS::SQS::Queue" && fifo(props) {
		end += ".fifo"
	}
	s, l, room := unnameable.ReplaceAllString(stack, ""), unnameable.ReplaceAllString(logical, ""), k.max-len(end)-1
	l = l[:min(len(l), max(room/2, room-len(s)))]
	n := s[:min(len(s), room-len(l))] + "-" + l + end
	if typ == "AWS::S3::Bucket" {
		return strings.ToLower(n)
	}
	return n
}

// provision creates a resource through the service that owns it, and answers
// the resource's physical id. Going through the owner's API keeps its store
// layout, validation and cascades that service's business.
func (p *Pack) provision(ctx context.Context, req *spi.Request, typ, logical, n string, props map[string]any, existing, stackID string) (string, error) {
	switch typ {
	case "AWS::S3::Bucket":
		if _, err := p.call(ctx, req, "aws.s3", "CreateBucket", map[string]any{"Bucket": n}); err != nil {
			return "", err
		}
		return n, p.configureBucket(ctx, req, n, props)
	case "AWS::SQS::Queue":
		in := map[string]any{"QueueName": n}
		if fifo(props) {
			in["Attributes"] = map[string]any{"FifoQueue": "true"}
		}
		out, err := p.call(ctx, req, "aws.sqs", "CreateQueue", in)
		return str(out["QueueUrl"]), err
	case "AWS::SNS::Topic":
		out, err := p.call(ctx, req, "aws.sns", "CreateTopic", map[string]any{"Name": n})
		return str(out["TopicArn"]), err
	case "AWS::DynamoDB::Table":
		in := maps.Clone(props)
		in["TableName"] = n
		_, err := p.call(ctx, req, "aws.dynamodb", "CreateTable", in)
		return made(n, err)
	case "AWS::IAM::Role":
		out, err := p.call(ctx, req, "aws.iam", "CreateRole", map[string]any{"RoleName": n, "AssumeRolePolicyDocument": jsonText(props["AssumeRolePolicyDocument"])})
		return str(asMap(out["Role"])["Arn"]), err
	case "AWS::SSM::Parameter":
		_, err := p.call(ctx, req, "aws.ssm", "PutParameter", map[string]any{"Name": n, "Value": str(props["Value"]), "Type": cmp.Or(str(props["Type"]), "String")})
		return made(n, err)
	case "AWS::SecretsManager::Secret":
		// existing is what an update finds; a second CreateSecret of its name is ResourceExistsException.
		op, in := "CreateSecret", map[string]any{"Name": n}
		if strings.HasSuffix(existing, ":secret:"+n) {
			op, in = "UpdateSecret", map[string]any{"SecretId": existing}
		}
		if _, ok := props["GenerateSecretString"]; ok {
			value, err := generated(props, stackID+"/"+logical)
			if err != nil {
				return "", err
			}
			in["SecretString"] = value
		}
		out, err := p.call(ctx, req, "aws.secretsmanager", op, given(in, props, "SecretString", "Description"))
		return str(out["ARN"]), err
	case "AWS::KMS::Key":
		out, err := p.call(ctx, req, "aws.kms", "CreateKey", map[string]any{"Description": str(props["Description"])})
		return str(asMap(out["KeyMetadata"])["KeyId"]), err
	case "AWS::Logs::LogGroup":
		_, err := p.call(ctx, req, "aws.logs", "CreateLogGroup", map[string]any{"logGroupName": n})
		return made(n, err)
	case "AWS::Events::Rule":
		in := given(map[string]any{"Name": n}, props, "ScheduleExpression", "State", "Description", "EventBusName")
		if pattern, ok := props["EventPattern"]; ok {
			in["EventPattern"] = jsonText(pattern)
		}
		out, err := p.call(ctx, req, "aws.events", "PutRule", in)
		if targets, ok := props["Targets"]; ok && err == nil {
			_, err = p.call(ctx, req, "aws.events", "PutTargets", given(map[string]any{"Rule": in["Name"], "Targets": targets}, props, "EventBusName"))
		}
		return str(out["RuleArn"]), err
	case "AWS::Lambda::Function":
		in := maps.Clone(props)
		in["FunctionName"] = n
		out, err := p.call(ctx, req, "aws.lambda", "CreateFunction", in)
		return str(out["FunctionArn"]), err
	case "AWS::Kinesis::Stream":
		_, err := p.call(ctx, req, "aws.kinesis", "CreateStream", map[string]any{"StreamName": n})
		return made(n, err)
	case "AWS::Kinesis::ResourcePolicy":
		arn := cmp.Or(str(props["ResourceArn"]), str(props["ResourceARN"]))
		policy := props["ResourcePolicy"]
		if policy == nil {
			policy = props["Policy"]
		}
		_, err := p.call(ctx, req, "aws.kinesis", "PutResourcePolicy", map[string]any{"ResourceARN": arn, "Policy": jsonText(policy)})
		return made(arn, err)
	case "AWS::ApiGateway::RestApi":
		out, err := p.call(ctx, req, "aws.apigateway", "CreateRestApi", map[string]any{"name": n})
		return str(out["id"]), err
	default:
		return "", invalid("unsupported resource type " + typ)
	}
}

// ruleBus is the event bus in a rule's ARN, rule/<bus>/<name>: none for the default bus.
func ruleBus(arn string) string {
	_, path, _ := strings.Cut(arn, ":rule/")
	return path[:max(strings.LastIndex(path, "/"), 0)]
}

// made answers the id of what a create made: none when it failed.
func made(id string, err error) (string, error) {
	if err != nil {
		id = ""
	}
	return id, err
}

// deprovision deletes a resource through its owner. One already gone (the
// owner's not-found error) counts as deleted, as a stack delete proceeds past it.
func (p *Pack) deprovision(ctx context.Context, req *spi.Request, r res) error {
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
		"AWS::Events::Rule":            {"aws.events", "DeleteRule", map[string]any{"Name": lastSlash(id), "Force": true, "EventBusName": ruleBus(id)}},
		"AWS::Lambda::Function":        {"aws.lambda", "DeleteFunction", map[string]any{"FunctionName": lastColon(id)}},
		"AWS::Kinesis::Stream":         {"aws.kinesis", "DeleteStream", map[string]any{"StreamName": id}},
		"AWS::Kinesis::ResourcePolicy": {"aws.kinesis", "DeleteResourcePolicy", map[string]any{"ResourceARN": id}},
		"AWS::ApiGateway::RestApi":     {"aws.apigateway", "DeleteRestApi", map[string]any{"restApiId": id}},
	}[r.Type]
	if del.svc == "" {
		return nil
	}
	_, err := p.call(ctx, req, del.svc, del.op, del.in)
	var f *spi.Fault
	if errors.As(err, &f) && (strings.Contains(f.Code, "NotFound") || strings.Contains(f.Code, "NoSuch") || strings.Contains(f.Code, "NonExistent")) {
		return nil
	}
	return err
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

// given sets on in each of members the template's props carry, and answers in.
func given(in, props map[string]any, members ...string) map[string]any {
	for _, member := range members {
		if v, ok := props[member]; ok {
			in[member] = v
		}
	}
	return in
}

// generated is the value GenerateSecretString asks for: a password made as GetRandomPassword makes one,
// seeded by the stack and logical id, put under GenerateStringKey in SecretStringTemplate when there is
// one. Each way the property can be wrong has its own ValidationError.
func generated(props map[string]any, seed string) (string, error) {
	spec, isMap := props["GenerateSecretString"].(map[string]any)
	key, tpl := str(spec["GenerateStringKey"]), str(spec["SecretStringTemplate"])
	_, both := props["SecretString"]
	var doc any
	switch {
	case !isMap:
		return "", invalid("GenerateSecretString must be an object, not " + jsonText(props["GenerateSecretString"]))
	case both:
		return "", invalid("SecretString and GenerateSecretString can't both be given")
	case tpl == "" && key != "":
		return "", invalid("GenerateStringKey needs a SecretStringTemplate to put the password in")
	case tpl != "" && key == "":
		return "", invalid("SecretStringTemplate needs a GenerateStringKey to name the password")
	case tpl != "" && json.Unmarshal([]byte(tpl), &doc) != nil:
		return "", invalid("SecretStringTemplate must be JSON: " + tpl)
	case tpl != "" && asMap(doc) == nil:
		return "", invalid("SecretStringTemplate must be a JSON object, not " + tpl)
	}
	params := map[string]any{}
	for member, v := range spec {
		// A template's number or boolean may be its text ("16", "true"), as a Ref to a parameter always is.
		want, text := generateTypes[member], str(v)
		if n, err := strconv.ParseFloat(text, 64); want == "number" && err == nil {
			v = n
		} else if want == "boolean" && (text == "true" || text == "false") {
			v = text == "true"
		}
		if want == "" {
			return "", invalid("GenerateSecretString has no property " + member)
		}
		if got := map[string]string{"float64": "number", "int": "number", "bool": "boolean", "string": "string"}[fmt.Sprintf("%T", v)]; got != want {
			return "", invalid("GenerateSecretString." + member + " must be a " + want + ", not " + jsonText(v))
		}
		params[member] = v
	}
	value, err := prim.RandomPassword(params, seed)
	if err != nil {
		return "", invalid("GenerateSecretString makes no password: " + err.Error())
	} else if key == "" {
		return value, nil
	}
	asMap(doc)[key] = value
	raw, err := json.Marshal(doc)
	return string(raw), err
}

// generateTypes are GenerateSecretString's properties and their types.
var generateTypes = map[string]string{"PasswordLength": "number", "ExcludeCharacters": "string", "GenerateStringKey": "string",
	"SecretStringTemplate": "string", "ExcludeLowercase": "boolean", "ExcludeUppercase": "boolean", "ExcludeNumbers": "boolean",
	"ExcludePunctuation": "boolean", "IncludeSpace": "boolean", "RequireEachIncludedType": "boolean"}

func invalid(message string) *spi.Fault { return fault("ValidationError", message) }

func fault(code, message string) *spi.Fault {
	return &spi.Fault{Code: code, Message: message, HTTPStatus: 400, Fault: "client"}
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

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
	pseudo := map[string]string{"AWS::AccountId": req.Identity.Account, "AWS::Region": req.Identity.Region,
		"AWS::StackName": stack, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com"}
	if v, ok := pseudo[name]; ok {
		return v
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
		logical, attr, _ = strings.Cut(t, ".")
	case []any:
		t = append(slices.Clip(t), nil, nil)
		logical, attr = str(t[0]), str(t[1])
	}
	phys := refs[logical]
	switch attr {
	case "Arn":
		if strings.Contains(phys, "arn:aws:") {
			return phys
		}
		if strings.Contains(phys, "/") && strings.Contains(phys, "://") { // a queue URL
			return "arn:aws:sqs:" + req.Identity.Region + ":" + req.Identity.Account + ":" + lastSlash(phys)
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
	ss := make([]string, len(parts))
	for i, x := range parts {
		ss[i] = str(p.resolve(x, refs, req, stack))
	}
	return strings.Join(ss, sep)
}

func (p *Pack) fnSub(s any, refs map[string]string, req *spi.Request, stack string) string {
	out := str(s)
	if arr, ok := s.([]any); ok && len(arr) > 0 {
		out = str(arr[0])
	}
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
		return nil, invalid("empty TemplateBody")
	}
	var m map[string]any
	if body[0] == '{' {
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			return nil, invalid(err.Error())
		}
	} else if yml, err := parseYAML(body); err != nil {
		return nil, invalid("TemplateBody YAML: " + err.Error())
	} else {
		m = yml
	}
	if len(asMap(m["Resources"])) == 0 { // "The Resources section is required in every CloudFormation template"
		return nil, invalid("Template format error: At least one Resources member must be defined.")
	}
	return m, nil
}

func formParams(in map[string]any) map[string]string {
	keys, vals := map[string]string{}, map[string]string{}
	for k, v := range in {
		if pfx, ok := strings.CutSuffix(k, ".ParameterKey"); ok && strings.HasPrefix(k, "Parameters.member.") {
			keys[pfx] = str(v)
		} else if pfx, ok := strings.CutSuffix(k, ".ParameterValue"); ok && strings.HasPrefix(k, "Parameters.member.") {
			vals[pfx] = str(v)
		}
	}
	out := map[string]string{}
	for pfx, k := range keys {
		out[k] = vals[pfx]
	}
	return out
}

func mergeParamDefaults(tpl map[string]any, params map[string]string) {
	for k, v := range asMap(tpl["Parameters"]) {
		if _, given := params[k]; !given {
			if d, ok := asMap(v)["Default"]; ok {
				params[k] = jsonText(d) // a parameter "becomes a string": Default 16 is "16"
			}
		}
	}
}

func paramDecls(tpl map[string]any) []any {
	var out []any
	for _, k := range slices.Sorted(maps.Keys(asMap(tpl["Parameters"]))) {
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

func str(v any) string { s, _ := v.(string); return s }

func lastSlash(s string) string { return s[strings.LastIndex(s, "/")+1:] }

func lastColon(s string) string { return s[strings.LastIndex(s, ":")+1:] }
