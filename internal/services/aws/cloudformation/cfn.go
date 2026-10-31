// Package cloudformation is CloudFormation's template engine: the operations behavior/aws/cloudformation lists as native.
package cloudformation

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// natives are the operations behavior/aws/cloudformation lists as native: the template engine. Parsing a
// template, resolving its intrinsics and provisioning resources through their owning services is Go; the
// stack, change set, stack set and type bookkeeping is the bundle's.
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
	if _, given := req.Input["StackName"]; given && (req.Operation == "ExecuteChangeSet" || req.Operation == "GetTemplateSummary") {
		if err := checkStackName(name, true); err != nil { // the model's StackNameOrId
			return nil, err
		}
	}
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
		b, ok, _ := p.col(req, "cfn-cs").Get(ctx, named(name)+":"+csn)
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		req.Input["TemplateBody"] = rec["TemplateBody"]
		if _, err := p.upsert(ctx, req, name, rec["ChangeSetType"] != "CREATE"); err != nil { // an UPDATE change set's stack must exist
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
	if err := checkStackName(name, update); err != nil {
		return nil, err
	}
	body, tpl, err := p.template(ctx, req)
	if err != nil {
		return nil, err
	}
	params := formParams(req.Input)
	mergeParamDefaults(tpl, params)
	id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/" + p.deps.Rand.Hex(8)
	var old stack // what an update finds; a create reserves the name, CREATE_IN_PROGRESS, in the same transaction
	given, name := name, named(name)
	if err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, found, _ := tx.Get(name)
		if _ = json.Unmarshal(raw, &old); found && !update {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists")
		} else if update && (!found || given != name && old.ID != given) { // an update of a stack that does not exist makes none
			return invalid("Stack [" + given + "] does not exist")
		} else if found && (strings.HasPrefix(old.Status, "ROLLBACK_") || strings.HasSuffix(old.Status, "_FAILED")) {
			return invalid("Stack:" + old.ID + " is in " + old.Status + " state and can not be updated.")
		} else if id = cmp.Or(old.ID, id); found {
			return nil
		}
		raw, _ = json.Marshal(stack{Name: name, ID: id, Status: "CREATE_IN_PROGRESS"})
		return tx.Put(name, raw)
	}); err != nil {
		return nil, err
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: params}
	fail := func(err error) (*spi.Response, error) {
		if st.Reason, old.Reason = err.Error(), err.Error(); !update || old.ID == "" { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, st, nil, st.Resources, nil, "ROLLBACK_COMPLETE", "ROLLBACK_FAILED")
		} else {
			_ = p.rollback(ctx, req, old, st.Resources)
		}
		return nil, err
	}
	resources, planned := asMap(tpl["Resources"]), maps.Clone(st.Refs)
	order, _ := p.order(req, tpl)
	for _, logical := range order { // a name taken fails the stack before anything is made: a Ref answers the id planned
		rm := asMap(resources[logical])
		typ, props := str(rm["Type"]), asMap(p.resolve(rm["Properties"], planned, req, name))
		prev, how, n, err := p.next(ctx, req, st, old, logical, typ, props)
		if planned[logical] = planID(req, typ, n, props); how == "" || how == "update" {
			planned[logical] = prev.Physical
		}
		if err != nil {
			return fail(err)
		}
	}
	for _, logical := range order {
		rm := asMap(resources[logical]) // props is never nil: provision adds to it
		typ, props := str(rm["Type"]), maps.Collect(maps.All(asMap(p.resolve(rm["Properties"], st.Refs, req, name))))
		prev, how, n, err := p.next(ctx, req, st, old, logical, typ, props) // again: a name may come from a resource made before it
		phys := map[string]string{"": prev.Physical}[how]                   // one an update keeps as it is
		if how != "" && err == nil {
			phys, err = p.provision(ctx, req, typ, logical, n, props, map[string]string{"update": prev.Physical}[how], id)
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
	// The update is done, so its cleanup deletes the old resources the stack no longer holds, replaced
	// ("creating a new one and deleting the old one") or dropped; one it cannot delete stays listed.
	stale := slices.DeleteFunc(slices.Clone(old.Resources), func(r res) bool {
		return slices.ContainsFunc(st.Resources, func(n res) bool { return n.Physical == r.Physical })
	})
	_ = p.unwind(ctx, req, st, st.Resources, stale, nil, st.Status, st.Status)
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// template reads the request's template body and parses it.
func (p *Pack) template(ctx context.Context, req *spi.Request) (string, map[string]any, error) {
	body, err := p.templateBody(ctx, req)
	if err != nil {
		return "", nil, err
	}
	tpl, err := parseTemplate(body)
	if err == nil {
		_, err = p.order(req, tpl)
	}
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
	if nameErr := checkStackName(name, true); nameErr != nil || err != nil {
		return nil, cmp.Or(nameErr, err)
	} else if st.Protect {
		err = invalid("stack has termination protection")
	} else {
		err = p.unwind(ctx, req, st, nil, st.Resources, nil, "DELETE_COMPLETE", "DELETE_FAILED")
	}
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

// unwind deletes the doomed resources, newest first, for a rollback or DeleteStack, and records the outcome on
// st, which keeps kept: done, keeping st's reason (a DELETE_COMPLETE stack is removed), with nothing left and
// no earlier error (first); else failed, what it could not delete listed as DELETE_FAILED for the next
// DeleteStack to try again, and the first error, which it answers, as StackStatusReason.
func (p *Pack) unwind(ctx context.Context, req *spi.Request, st stack, kept, doomed []res, first error, done, failed string) error {
	st.Resources = append([]res{}, kept...) // never null: every read of the stack lists its resources
	for _, r := range slices.Backward(doomed) {
		if err := p.deprovision(ctx, req, r); err != nil {
			r.Status, r.Reason, first = "DELETE_FAILED", err.Error(), cmp.Or(first, err)
			st.Resources = slices.Insert(st.Resources, len(kept), r)
		}
	}
	st.Status = done
	if first != nil {
		st.Status, st.Reason = failed, first.Error()
	}
	if st.Status == "DELETE_COMPLETE" {
		return p.col(req, "cfn").Delete(ctx, st.Name)
	}
	raw, _ := json.Marshal(st)
	return cmp.Or(first, p.col(req, "cfn").Put(ctx, st.Name, raw))
}

// checkStackName is the framework's ValidationError for a StackName outside CreateStack's rule,
// "alphanumeric characters (case sensitive) and hyphens. It must start with an alphabetical character
// and can't be longer than 128 characters"; or, byID, where "The name or unique stack ID" is taken, the
// model's StackNameOrId: a length of 1 or more and its pattern, each alternative matched whole.
func checkStackName(name string, byID bool) error {
	pattern, constraint := `[a-zA-Z][-a-zA-Z0-9]*`, ""
	if byID {
		pattern = `^([a-zA-Z][-a-zA-Z0-9]*)|(arn:\b(aws|aws-us-gov|aws-cn)\b:[-a-zA-Z0-9:/._+]*)$`
	}
	if ok, _ := regexp.MatchString("^(?:"+strings.Trim(pattern, "^$")+")$", name); name == "" {
		constraint = "have length greater than or equal to 1"
	} else if !ok {
		constraint = "satisfy regular expression pattern: " + pattern
	} else if len(name) > 128 && !byID {
		constraint = "have length less than or equal to 128"
	} else {
		return nil
	}
	return invalid("1 validation error detected: Value '" + name + "' at 'stackName' failed to satisfy constraint: Member must " + constraint)
}

// load reads the stack a StackName names: by its name, or by its stack id while the stack has that id.
func (p *Pack) load(ctx context.Context, req *spi.Request, name string) (stack, error) {
	var st stack
	b, ok, _ := p.col(req, "cfn").Get(ctx, named(name))
	if _ = json.Unmarshal(b, &st); !ok || named(name) != name && st.ID != name {
		return stack{}, invalid("Stack [" + name + "] does not exist")
	}
	return st, nil
}

// named is the name of the stack a StackName names: "The name or unique stack ID", a stack id being
// arn:...:stack/<name>/<id>. Stacks are keyed by name, never by an id.
func named(s string) string {
	if _, path, isID := strings.Cut(s, ":stack/"); isID {
		s, _, _ = strings.Cut(path, "/")
	}
	return s
}

// kind is what the engine knows of a resource type: the property that names one and the longest name its
// service takes (API Gateway sets none for a REST API, nor holds a name to one; 1024 only bounds it), the
// read that finds one by name and the service's error for a name taken (SNS's CreateTopic "is idempotent":
// CloudFormation's), the id one is made with (planID), the properties whose change "Update requires:
// Replacement" (the type's Template Reference page), and the calls (do) that create one, where in their
// answer its physical id is, and that update one in place and delete one.
type kind struct {
	member                                             string
	max                                                int
	find, taken, ref, replace, create, id, update, del string
}

var kinds = map[string]kind{
	"AWS::S3::Bucket":              {"BucketName", 63, "aws.s3 HeadBucket Bucket={n}", "BucketAlreadyOwnedByYou", "{n}", "BucketName BucketNamePrefix BucketNamespace", "aws.s3 CreateBucket Bucket={n}", "Bucket", "", "aws.s3 DeleteBucket Bucket={id}"},
	"AWS::SQS::Queue":              {"QueueName", 80, "aws.sqs GetQueueUrl QueueName={n}", "QueueAlreadyExists", "", "QueueName FifoQueue", "aws.sqs CreateQueue QueueName={n} Attributes", "QueueUrl", "", "aws.sqs DeleteQueue QueueUrl={id}"},
	"AWS::SNS::Topic":              {"TopicName", 256, "aws.sns GetTopicAttributes TopicArn=arn:aws:sns:{r}:{a}:{n}", "AlreadyExistsException", "arn:aws:sns:{r}:{a}:{n}", "TopicName FifoTopic", "aws.sns CreateTopic Name={n}", "TopicArn", "", "aws.sns DeleteTopic TopicArn={id}"},
	"AWS::DynamoDB::Table":         {"TableName", 255, "aws.dynamodb DescribeTable TableName={n}", "ResourceInUseException", "{n}", "TableName ImportSourceSpecification", "aws.dynamodb CreateTable * TableName={n}", "TableName", "aws.dynamodb UpdateTable TableName={id} BillingMode ProvisionedThroughput StreamSpecification TableClass DeletionProtectionEnabled", "aws.dynamodb DeleteTable TableName={id}"},
	"AWS::IAM::Role":               {"RoleName", 64, "aws.iam GetRole RoleName={n}", "EntityAlreadyExists", "arn:aws:iam::{a}:role/{n}", "RoleName Path", "aws.iam CreateRole RoleName={n} AssumeRolePolicyDocument=%AssumeRolePolicyDocument", "Role.Arn", "aws.iam UpdateAssumeRolePolicy RoleName={id/} PolicyDocument=%AssumeRolePolicyDocument", "aws.iam DeleteRole RoleName={id/}"},
	"AWS::SSM::Parameter":          {"Name", 2048, "aws.ssm GetParameter Name={n}", "ParameterAlreadyExists", "{n}", "Name", "aws.ssm PutParameter Name={n} Value= Value=$Value Type=String Type=$Type", "Name", "aws.ssm PutParameter Name={id} Value= Value=$Value Type=String Type=$Type Overwrite=true", "aws.ssm DeleteParameter Name={id}"},
	"AWS::SecretsManager::Secret":  {"Name", 512, "aws.secretsmanager DescribeSecret SecretId={n}", "ResourceExistsException", "arn:aws:secretsmanager:{r}:{a}:secret:{n}", "Name", "aws.secretsmanager CreateSecret Name={n} SecretString=$SecretString Description=$Description", "ARN", "aws.secretsmanager UpdateSecret SecretId={id} SecretString=$SecretString Description=$Description", "aws.secretsmanager DeleteSecret SecretId={id} ForceDeleteWithoutRecovery=true"},
	"AWS::KMS::Key":                {"", 0, "", "", "", "KeySpec KeyUsage MultiRegion", "aws.kms CreateKey Description= Description=$Description", "KeyMetadata.KeyId", "aws.kms UpdateKeyDescription KeyId={id} Description= Description=$Description", "aws.kms ScheduleKeyDeletion KeyId={id} PendingWindowInDays=7"},
	"AWS::Logs::LogGroup":          {"LogGroupName", 512, "aws.logs DescribeLogGroups logGroupNamePrefix={n}", "ResourceAlreadyExistsException", "{n}", "LogGroupName", "aws.logs CreateLogGroup logGroupName={n}", "logGroupName", "", "aws.logs DeleteLogGroup logGroupName={id}"},
	"AWS::Events::Rule":            {"Name", 64, "aws.events DescribeRule Name={n} EventBusName=$EventBusName", "ResourceAlreadyExistsException", "arn:aws:events:{r}:{a}:rule/{n}", "Name EventBusName", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "RuleArn", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "aws.events DeleteRule Name={id/} EventBusName={bus}"},
	"AWS::Lambda::Function":        {"FunctionName", 64, "aws.lambda GetFunction FunctionName={n}", "ResourceConflictException", "arn:aws:lambda:{r}:{a}:function:{n}", "FunctionName PackageType DurableConfig TenancyConfig", "aws.lambda CreateFunction * FunctionName={n}", "FunctionArn", "aws.lambda UpdateFunctionConfiguration * FunctionName={id:}", "aws.lambda DeleteFunction FunctionName={id:}"},
	"AWS::Kinesis::Stream":         {"Name", 128, "aws.kinesis DescribeStreamSummary StreamName={n}", "ResourceInUseException", "{n}", "Name", "aws.kinesis CreateStream StreamName={n} ShardCount", "StreamName", "aws.kinesis UpdateShardCount StreamName={id} TargetShardCount=1 TargetShardCount=@ShardCount ScalingType=UNIFORM_SCALING", "aws.kinesis DeleteStream StreamName={id}"},
	"AWS::Kinesis::ResourcePolicy": {"", 0, "", "", "", "ResourceArn", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "ResourceARN", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "aws.kinesis DeleteResourcePolicy ResourceARN={id}"},
	"AWS::ApiGateway::RestApi":     {"Name", 1024, "", "", "", "", "aws.apigateway CreateRestApi name={n}", "id", "aws.apigateway UpdateRestApi restApiId={id} name={n}", "aws.apigateway DeleteRestApi restApiId={id}"},
}

// do makes a call a kind spells "service Operation member=value ...", and answers the answer over the
// request. A value is {n} the resource's name, {id} its physical id ({id/} and {id:}, its last segment),
// {bus} a rule's event bus, {r} and {a} the region and account; $Prop is the template's property Prop as
// text, %Prop as JSON text, @Prop as given, each set only when given; else the value itself, true, false
// and digits typed. A member alone is @member, and * is every property. An empty spec calls nothing.
func (p *Pack) do(ctx context.Context, req *spi.Request, spec, n, id string, props map[string]any) (map[string]any, error) {
	f, in := strings.Fields(spec), map[string]any{}
	if len(f) < 2 {
		return in, nil
	}
	sub := strings.NewReplacer("{n}", n, "{id}", id, "{id/}", lastSlash(id), "{id:}", lastColon(id), "{bus}", ruleBus(id), "{r}", req.Identity.Region, "{a}", req.Identity.Account)
	for _, arg := range f[2:] {
		k, v, set := strings.Cut(arg, "=")
		if !set {
			v = "@" + k
		}
		if pv, given := props[v[min(1, len(v)):]]; arg == "*" {
			maps.Copy(in, props)
		} else if sigil := strings.IndexByte("@$%", (v + " ")[0]); sigil >= 0 {
			if given {
				in[k] = []any{pv, str(pv), jsonText(pv)}[sigil]
			}
		} else if b, isBool := coerce(v, "Boolean"); isBool {
			in[k] = b
		} else if i, isInt := coerce(v, "Integer"); isInt {
			in[k] = i
		} else {
			in[k] = sub.Replace(v)
		}
	}
	out, err := p.call(ctx, req, f[0], f[1], in)
	maps.Copy(in, out)
	return in, err
}

// unknown stands for an id known only once its resource is made, and in order for one not yet ordered.
const unknown = "\uffff"

// planID is the id a resource named n is made with, which a Ref to it answers: n in the form its
// service gives it back. A queue's URL (its endpoint's business), a KMS key's id and a REST API's are
// known only once made, as is an id built on one: unknown. A Kinesis resource policy's is its ARN.
func planID(req *spi.Request, typ, n string, props map[string]any) string {
	if bus := lastSlash(str(props["EventBusName"])); typ == "AWS::Events::Rule" && bus != "" && bus != "default" {
		n = bus + "/" + n
	}
	if f := kinds[typ].ref; f != "" && !strings.Contains(n, unknown) {
		return strings.NewReplacer("{r}", req.Identity.Region, "{a}", req.Identity.Account, "{n}", n).Replace(f)
	}
	return cmp.Or(str(props["ResourceArn"]), str(props["ResourceARN"]), unknown)
}

// order is the order a template's resources are made in: each after those its properties name, by Ref,
// Fn::GetAtt or Fn::Sub ("Resource B is created before resource A", Template Reference, DependsOn
// attribute), else by logical id. A cycle orders none, and is CloudFormation's error.
func (p *Pack) order(req *spi.Request, tpl map[string]any) ([]string, error) {
	resources, marks, order := asMap(tpl["Resources"]), map[string]string{}, []string(nil)
	todo := slices.Sorted(maps.Keys(resources))
	for _, logical := range todo {
		marks[logical] = unknown
	}
	for len(todo) > 0 {
		i := slices.IndexFunc(todo, func(l string) bool {
			return !strings.Contains(jsonText(p.resolve(asMap(resources[l])["Properties"], marks, req, "")), unknown)
		})
		if i < 0 {
			return nil, invalid("Circular dependency between resources: [" + strings.Join(todo, ", ") + "]")
		}
		marks[todo[i]], order, todo = "", append(order, todo[i]), slices.Delete(slices.Clone(todo), i, i+1)
	}
	return order, nil
}

// change is what an update does to the resource of logical the stack holds: "" keeps it as it is,
// "update" updates it in place, and "replace" makes a new one, when a property whose change "requires
// replacement" (the type's Template Reference page) changed. A resource the stack does not hold is a "create".
func (p *Pack) change(req *spi.Request, old stack, logical, typ string, props map[string]any) (res, string) {
	i := slices.IndexFunc(old.Resources, func(r res) bool { return r.Logical == logical && r.Type == typ && r.Status == "" })
	if i < 0 {
		return res{}, "create"
	}
	tpl, _ := parseTemplate(old.Template)
	before, how := asMap(p.resolve(asMap(asMap(tpl["Resources"])[logical])["Properties"], old.Refs, req, old.Name)), ""
	for _, k := range slices.Concat(slices.Collect(maps.Keys(before)), slices.Collect(maps.Keys(props))) {
		if jsonText(before[k]) == jsonText(props[k]) {
			continue
		} else if how = "update"; slices.Contains(strings.Fields(kinds[typ].replace), k) {
			return old.Resources[i], "replace"
		}
	}
	return old.Resources[i], how
}

// next is what an update does to one resource (change) and the name it is kept or made under: its own,
// claim's, or for a replacement one of its own, made from the old id. A replacement may not keep a name
// the template gives: "CloudFormation cannot update a stack when a custom-named resource requires replacing".
func (p *Pack) next(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (res, string, string, error) {
	prev, how := p.change(req, old, logical, typ, props)
	if how == "" || how == "update" && prev.Name != "" {
		return prev, how, prev.Name, nil
	} else if how == "replace" {
		st.ID += "/" + prev.Physical
	}
	n, err := p.claim(ctx, req, st, old, logical, typ, props)
	if err == nil && how == "replace" && n == prev.Name {
		err = invalid("CloudFormation cannot update a stack when a custom-named resource requires replacing. Rename " + n + " and update the stack again.")
	}
	return prev, how, n, err
}

// claim answers the name a resource is made under: the template's, or else physicalName's. A given name
// that a resource the stack did not make under it holds is the service's error for a name taken: several
// services answer such a create with what they hold, which a rollback or DeleteStack would then delete.
func (p *Pack) claim(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (string, error) {
	k, n := kinds[typ], str(props[kinds[typ].member])
	if n == "" || strings.Contains(n, unknown) || k.find == "" { // one built on an id known only once made is checked as it is made
		return cmp.Or(n, physicalName(st.Name, logical, st.ID, typ, props)), nil
	}
	out, err := p.do(ctx, req, k.find, n, "", props)
	groups, list := out["logGroups"].([]any) // DescribeLogGroups answers a list by prefix, never a fault
	if err != nil || list && !slices.ContainsFunc(groups, func(g any) bool { return str(asMap(g)["logGroupName"]) == n }) ||
		slices.ContainsFunc(old.Resources, func(r res) bool { // a record from before res.Name: by the physical id, which ends in the name
			return r.Logical == logical && (r.Name == n || r.Name == "" && (r.Physical == n || strings.HasSuffix(r.Physical, "/"+n) || strings.HasSuffix(r.Physical, ":"+n)))
		}) {
		return n, nil
	}
	return "", fault(k.taken, "Resource of type '"+typ+"' with identifier '"+n+"' already exists.")
}

var unnameable = regexp.MustCompile(`[^A-Za-z0-9-]+`)

func fifo(p map[string]any) bool { b, _ := coerce(p["FifoQueue"], "Boolean"); return b == true }

// physicalName is the name of a resource the template leaves unnamed, in the modelled form
// <stack>-<logical id>-<suffix>. The suffix is twelve hex digits of a hash of the stack id and logical
// id, so no two stacks, and no two resources of one stack, share a name, and an update finds the name its
// create made. Only letters, digits and hyphens are kept; a bucket's name is lower case and a FIFO
// queue's ends in .fifo; the stack name and logical id, never the suffix, are cut to the longest name.
func physicalName(stack, logical, stackID, typ string, props map[string]any) string {
	k := kinds[typ]
	if k.member == "" {
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

// provision creates a resource through the service that owns it, and answers the resource's physical id.
// Going through the owner's API keeps its store layout, validation and cascades that service's business.
// existing is what an update finds: a resource its kind updates in place is updated under it.
func (p *Pack) provision(ctx context.Context, req *spi.Request, typ, logical, n string, props map[string]any, existing, stackID string) (string, error) {
	k, known := kinds[typ]
	spec, phys := k.create, ""
	if !known {
		return "", invalid("unsupported resource type " + typ)
	} else if existing != "" {
		spec = k.update
	}
	if _, gen := props["GenerateSecretString"]; gen && typ == "AWS::SecretsManager::Secret" {
		value, err := generated(props, stackID+"/"+logical)
		if err != nil {
			return "", err
		}
		props["SecretString"] = value
	} else if fifo(props) && typ == "AWS::SQS::Queue" {
		props["Attributes"] = map[string]any{"FifoQueue": "true"}
	}
	out, err := p.do(ctx, req, spec, n, existing, props)
	for _, member := range strings.Split(k.id, ".") { // where a create's answer holds the id
		out, phys = asMap(out[member]), str(out[member])
	}
	if phys = cmp.Or(existing, phys); err == nil && typ == "AWS::Events::Rule" {
		targets, set := props["Targets"]
		keep, _ := targets.([]any)
		if err = p.dropTargets(ctx, req, phys, keep); set && err == nil { // the rule's targets are the template's, no others
			// "each entry in FailedEntries provides the ID of the failed target and the error code":
			// a target not added fails the rule, as its error does.
			put, putErr := p.call(ctx, req, "aws.events", "PutTargets", given(map[string]any{"Rule": n, "Targets": targets}, props, "EventBusName"))
			if failed, _ := put["FailedEntries"].([]any); putErr == nil && len(failed) > 0 {
				putErr = fault(cmp.Or(str(asMap(failed[0])["ErrorCode"]), "ValidationError"), "PutTargets failed to add targets: "+jsonText(failed))
			}
			err = putErr
		}
	} else if err == nil && typ == "AWS::S3::Bucket" {
		err = p.configureBucket(ctx, req, n, props)
	} else if err != nil && existing == "" { // none once a create failed
		phys = ""
	}
	return phys, err
}

// rollback undoes a failed update as UPDATE_ROLLBACK does. What the update changed in place is
// provisioned again from the previous template; what it made, half-made ones included, is deleted; the
// stack keeps the previous template and resources, UPDATE_ROLLBACK_COMPLETE, or UPDATE_ROLLBACK_FAILED with
// what it could not delete still listed for DeleteStack. Only a finished update deletes what it replaced.
func (p *Pack) rollback(ctx context.Context, req *spi.Request, old stack, made []res) error {
	tpl, _ := parseTemplate(old.Template)
	var doomed []res
	var first error
	for _, r := range made {
		if old.Refs[r.Logical] != r.Physical {
			doomed = append(doomed, r)
		} else if props := asMap(p.resolve(asMap(asMap(tpl["Resources"])[r.Logical])["Properties"], old.Refs, req, old.Name)); props != nil {
			_, err := p.provision(ctx, req, r.Type, r.Logical, r.Name, props, r.Physical, old.ID)
			first = cmp.Or(first, err)
		}
	}
	return p.unwind(ctx, req, old, old.Resources, doomed, first, "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED")
}

// ruleBus is the event bus in a rule's ARN, rule/<bus>/<name>: none for the default bus.
func ruleBus(arn string) string {
	_, path, _ := strings.Cut(arn, ":rule/")
	return path[:max(strings.LastIndex(path, "/"), 0)]
}

// deprovision deletes a resource through its owner. One already gone (the
// owner's not-found error) counts as deleted, as a stack delete proceeds past it.
func (p *Pack) deprovision(ctx context.Context, req *spi.Request, r res) (err error) {
	if r.Type == "AWS::Events::Rule" {
		err = p.dropTargets(ctx, req, r.Physical, nil)
	}
	if err == nil {
		_, err = p.do(ctx, req, kinds[r.Type].del, "", r.Physical, nil)
	}
	var f *spi.Fault
	if errors.As(err, &f) && (strings.Contains(f.Code, "NotFound") || strings.Contains(f.Code, "NoSuch") || strings.Contains(f.Code, "NonExistent")) {
		return nil
	}
	return err
}

// dropTargets removes the rule's targets but those keep names: all of them before the rule is deleted
// ("Before you can delete the rule, you must remove all targets, using RemoveTargets", EventBridge).
func (p *Pack) dropTargets(ctx context.Context, req *spi.Request, rule string, keep []any) error {
	listed, err := p.do(ctx, req, "aws.events ListTargetsByRule Rule={id/} EventBusName={bus}", "", rule, nil)
	targets, _ := listed["Targets"].([]any)
	var ids []any
	for _, t := range targets {
		if !slices.ContainsFunc(keep, func(k any) bool { return str(asMap(k)["Id"]) == str(asMap(t)["Id"]) }) {
			ids = append(ids, asMap(t)["Id"])
		}
	}
	if len(ids) > 0 && err == nil {
		_, err = p.do(ctx, req, "aws.events RemoveTargets Rule={id/} EventBusName={bus} Ids", "", rule, map[string]any{"Ids": ids})
	}
	return err
}

// call invokes another service the way the edge would: through whatever the registry serves it with, pack
// or bundle. A pack that runs a worker is closed again, since this construction is per call.
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

// configureBucket applies the bucket properties that map one to one onto an S3 operation, and stores the
// rest as they are given.
func (p *Pack) configureBucket(ctx context.Context, req *spi.Request, bucket string, props map[string]any) error {
	for property, op := range map[string]string{"VersioningConfiguration": "PutBucketVersioning", "Tags": "PutBucketTagging",
		"PublicAccessBlockConfiguration": "PutPublicAccessBlock", "OwnershipControls": "PutBucketOwnershipControls"} {
		if v, ok := props[property]; ok {
			in := map[string]any{"Bucket": bucket, property: v}
			if property == "VersioningConfiguration" { // S3 reads Status and TagSet lifted to the top, as restXml hands them over (its liftedMembers debt)
				in["Status"] = asMap(v)["Status"]
			} else if property == "Tags" {
				in = map[string]any{"Bucket": bucket, "Tagging": map[string]any{"TagSet": v}, "TagSet": v}
			}
			if _, err := p.call(ctx, req, "aws.s3", op, in); err != nil {
				return err
			}
		}
	}
	// ponytail: the configurations with no one-to-one S3 operation are stored in CloudFormation's own
	// shape, which S3 does not read and Cloud Control reads back. Mapping each onto its S3 operation
	// (CorsRules to CORSRules, and so on) is the upgrade, in both directions.
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

// given sets on in each of members, all Strings, the template's props carry, and answers in.
func given(in, props map[string]any, members ...string) map[string]any {
	for _, member := range members {
		if v, ok := props[member]; ok {
			in[member], _ = coerce(v, "String")
		}
	}
	return in
}

// coerce reads a template's value as a property of type want (the template reference's Boolean,
// Integer or String), itself or its text as a Ref to a parameter gives it ("the parameter value
// becomes a string", User Guide, Parameters), and answers whether it is one. A Boolean is true or
// false in any case, as YAML 1.1, the templates' YAML, spells it true, True or TRUE; an Integer is
// decimal digits, as a Number parameter's text is ("016" is 16; "16.0", "1e1", "+16" and " 16" are
// none); a String is a scalar's text (123 is "123"). Anything else is answered as it was given.
func coerce(v any, want string) (any, bool) {
	switch v.(type) {
	case string, bool, float64, int, int64, uint64:
	default:
		return v, false
	}
	if text, yes := jsonText(v), strings.EqualFold(jsonText(v), "true"); want == "Boolean" {
		return yes, yes || strings.EqualFold(text, "false")
	} else if n, err := strconv.Atoi(text); want == "Integer" {
		return n, err == nil && strings.TrimLeft(text, "0123456789") == ""
	} else {
		return text, true
	}
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
		typed, ok := coerce(v, generateTypes[member])
		if generateTypes[member] == "" {
			return "", invalid("GenerateSecretString has no property " + member)
		} else if !ok {
			return "", invalid("GenerateSecretString." + member + " must be " + generateTypes[member] + ", not " + jsonText(v))
		}
		params[member] = typed
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

// generateTypes are GenerateSecretString's properties and their types, as its template reference gives them.
var generateTypes = map[string]string{"PasswordLength": "Integer", "ExcludeCharacters": "String", "GenerateStringKey": "String",
	"SecretStringTemplate": "String", "ExcludeLowercase": "Boolean", "ExcludeUppercase": "Boolean", "ExcludeNumbers": "Boolean",
	"ExcludePunctuation": "Boolean", "IncludeSpace": "Boolean", "RequireEachIncludedType": "Boolean"}

func invalid(message string) *spi.Fault { return fault("ValidationError", message) }

func fault(code, message string) *spi.Fault {
	return &spi.Fault{Code: code, Message: message, HTTPStatus: 400, Fault: "client"}
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

// jsonText is a policy-like property as the JSON text an API takes: an object's, or a string as given.
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
	logical, attr, _ := strings.Cut(str(g), ".") // "A.B", or [A, B]
	if t, ok := g.([]any); ok {
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
	arr, _ := s.([]any) // "text", or ["text", {variables}]
	out := str(append(slices.Clip(arr), s)[0])
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
	out := map[string]string{}
	for k, v := range in {
		if pfx, ok := strings.CutSuffix(k, ".ParameterKey"); ok && strings.HasPrefix(k, "Parameters.member.") {
			out[str(v)] = str(in[pfx+".ParameterValue"])
		}
	}
	return out
}

func mergeParamDefaults(tpl map[string]any, params map[string]string) {
	for k, v := range asMap(tpl["Parameters"]) {
		if _, given := params[k]; !given && asMap(v)["Default"] != nil {
			params[k] = jsonText(asMap(v)["Default"]) // a parameter "becomes a string": Default 16 is "16"
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

// str is a String property's value: a scalar's text (coerce), nothing for anything else.
func str(v any) string { s, _ := coerce(v, "String"); t, _ := s.(string); return t }

func lastSlash(s string) string { return s[strings.LastIndex(s, "/")+1:] }

func lastColon(s string) string { return s[strings.LastIndex(s, ":")+1:] }
