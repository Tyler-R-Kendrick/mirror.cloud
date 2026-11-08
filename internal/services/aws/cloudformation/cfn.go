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
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/prim"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
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
type Pack struct {
	deps  spi.Deps
	walks int // resources whose properties order walked, which a test counts
}

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
		_, tpl, _, err := p.template(ctx, req)
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Description": str(tpl["Description"]), "Parameters": paramDecls(tpl)}}, nil
	case "CreateStack", "UpdateStack":
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack")
	case "DeleteStack":
		return p.drop(ctx, req, name)
	case "GetTemplateSummary":
		_, tpl, _, err := p.template(ctx, req)
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
	body, tpl, order, err := p.template(ctx, req)
	if err != nil {
		return nil, err
	}
	params := parameters(req.Input, tpl)
	id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/" + p.deps.Rand.Hex(8)
	var old stack // what an update finds; a create reserves the name, CREATE_IN_PROGRESS, in the same transaction
	if err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, found, _ := tx.Get(name)
		if _ = json.Unmarshal(raw, &old); found && !update {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists")
		} else if found && (strings.HasPrefix(old.Status, "ROLLBACK_") || strings.HasSuffix(old.Status, "_IN_PROGRESS") || old.Status == "DELETE_FAILED") {
			return invalid("Stack:" + old.ID + " is in " + old.Status + " state and can not be updated.")
		} else if id = cmp.Or(old.ID, id); found || update { // an update reserves nothing: it writes only over what it read
			return nil
		}
		raw, _ = json.Marshal(stack{Name: name, ID: id, Status: "CREATE_IN_PROGRESS", Template: body, Resources: []res{}, Outputs: []any{}})
		return tx.Put(name, raw)
	}); err != nil {
		return nil, err
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: params,
		Resources: []res{}, Outputs: []any{}} // never null: every read of a stack lists both
	fail := func(err error) (*spi.Response, error) {
		if st.Reason = err.Error(); !update { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, st, "CREATE_IN_PROGRESS", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED")
		}
		return nil, err
	}
	resources, planned := asMap(tpl["Resources"]), maps.Clone(st.Refs)
	for _, logical := range order { // a name taken fails the stack before anything is made: a Ref answers the id planned
		rm := asMap(resources[logical])
		typ, props := str(rm["Type"]), asMap(p.resolve(rm["Properties"], planned, req, name))
		n, err := p.claim(ctx, req, st, old, logical, typ, props)
		if planned[logical] = planID(req, typ, n, props); err != nil {
			return fail(err)
		}
		attribute(planned, req, typ, logical, planned[logical])
	}
	for _, logical := range order {
		rm := asMap(resources[logical]) // props is never nil: provision adds to it
		typ, props := str(rm["Type"]), maps.Collect(maps.All(asMap(p.resolve(rm["Properties"], st.Refs, req, name))))
		phys, n, err := "", "", error(nil)
		if n, err = p.claim(ctx, req, st, old, logical, typ, props); err == nil { // again: a name may come from a resource made before it
			phys, err = p.provision(ctx, req, typ, logical, n, props, old.Refs[logical], id)
		}
		if phys != "" { // recorded once it exists, so a rollback finds a half-made one too
			st.Refs[logical] = phys
			attribute(st.Refs, req, typ, logical, phys)
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
	if !update { // a DeleteStack begun during the create ends here: what it made goes, and then the stack
		if _, wrote, err := p.settle(ctx, req, name, from(st, "CREATE_IN_PROGRESS")); err == nil && !wrote {
			_ = p.unwind(ctx, req, st, "DELETE_IN_PROGRESS", "DELETE_COMPLETE", "DELETE_FAILED")
		}
	} else if cur, wrote, err := p.settle(ctx, req, name, as(old, st)); err == nil && !wrote {
		// Another change came first, in progress while this update ran: what this update made that the stack
		// does not hold goes, all of it when the stack is being deleted (its delete may have run first).
		rival := map[bool]string{true: "DELETE_IN_PROGRESS", false: "UPDATE_IN_PROGRESS"}[cur.ID != id || strings.HasPrefix(cur.Status, "DELETE_")]
		if cur.ID == id && rival == "DELETE_IN_PROGRESS" {
			cur.Resources = nil
		}
		st.Resources = slices.DeleteFunc(st.Resources, func(r res) bool {
			return slices.ContainsFunc(cur.Resources, func(c res) bool { return c.Physical == r.Physical })
		})
		_ = p.unwind(ctx, req, st, "", "", "")
		return nil, invalid("Stack:" + id + " is in " + rival + " state and can not be updated.")
	}
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// settle rewrites the stack record in one store transaction: take, given the record (empty when there is
// none), answers the record to write instead, and whether to write it; a DELETE_COMPLETE one is removed.
// settle answers the record it found and whether it wrote.
func (p *Pack) settle(ctx context.Context, req *spi.Request, name string, take func(stack) (stack, bool)) (stack, bool, error) {
	var cur stack
	wrote := false
	err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, _, _ := tx.Get(name)
		_ = json.Unmarshal(raw, &cur)
		next, yes := take(cur)
		if wrote = yes; !yes {
			return nil
		} else if next.Status == "DELETE_COMPLETE" {
			return tx.Delete(name)
		}
		raw, _ = json.Marshal(next)
		return tx.Put(name, raw)
	})
	return cur, wrote, err
}

// from writes st while the record is still st's stack in status was, keeping the record's termination
// protection, which UpdateTerminationProtection sets whenever it runs.
func from(st stack, was string) func(stack) (stack, bool) {
	return func(cur stack) (stack, bool) {
		st.Protect = cur.Protect
		return st, cur.ID == st.ID && cur.Status == was
	}
}

// as writes st while the record is still old, termination protection aside: st keeps the record's.
func as(old, st stack) func(stack) (stack, bool) {
	return func(cur stack) (stack, bool) {
		st.Protect, cur.Protect = cur.Protect, old.Protect
		return st, reflect.DeepEqual(cur, old)
	}
}

// template reads the request's template body, parses it and orders its resources.
func (p *Pack) template(ctx context.Context, req *spi.Request) (string, map[string]any, []string, error) {
	body, err := p.templateBody(ctx, req)
	if err != nil {
		return "", nil, nil, err
	}
	tpl, err := parseTemplate(body)
	var order []string
	if err == nil {
		order, err = p.order(tpl, parameters(req.Input, tpl))
	}
	return body, tpl, order, err
}

// templateBody is the request's template: TemplateBody, "a maximum length of 51,200 bytes", or the S3
// object TemplateURL names, "max size: 1 MB" (API reference, CreateStack), which the quotas page, writing
// 204,800 bytes "204.8 KB", counts as 1,000,000 bytes.
func (p *Pack) templateBody(ctx context.Context, req *spi.Request) (string, error) {
	if body := str(req.Input["TemplateBody"]); len(body) > 51_200 {
		return "", invalid("1 validation error detected: Value at 'templateBody' failed to satisfy constraint: Member must have length less than or equal to 51200")
	} else if body != "" {
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
	body, err := io.ReadAll(io.LimitReader(reader, 1_000_001))
	if len(body) > 1_000_000 {
		return "", invalid("The template at TemplateURL is larger than a template's maximum size in S3, 1 MB")
	}
	return string(body), err
}

// drop starts a stack's deletion ("Once the call completes successfully, stack deletion starts", API
// reference, DeleteStack): the record becomes DELETE_IN_PROGRESS, written only over the record read, and
// its resources are deleted. One being created is left to its create, which deletes what it made and then
// the stack; one already being deleted is left to that delete.
func (p *Pack) drop(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	if err == nil && st.Protect {
		err = invalid("stack has termination protection")
	} else if was, del := st.Status, st; err == nil && was != "DELETE_IN_PROGRESS" {
		del.Status = "DELETE_IN_PROGRESS"
		_, started, settleErr := p.settle(ctx, req, name, func(cur stack) (stack, bool) { return del, reflect.DeepEqual(cur, st) })
		if err = settleErr; err == nil && !started { // it changed since it was read
			return p.drop(ctx, req, name)
		} else if err == nil && was != "CREATE_IN_PROGRESS" {
			err = p.unwind(ctx, req, del, "DELETE_IN_PROGRESS", "DELETE_COMPLETE", "DELETE_FAILED")
		}
	}
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

// unwind deletes a stack's resources, newest first, as a rollback or a DeleteStack does, and records the
// outcome while the record is still st's stack in status was ("" records nothing). With nothing left the
// stack is done, keeping st's reason (a DELETE_COMPLETE stack is removed); else failed, the resources left
// listed as DELETE_FAILED for the next DeleteStack to try again and the first error, which unwind answers,
// its StackStatusReason. A create's rollback that finds a DeleteStack begun ends that delete instead.
func (p *Pack) unwind(ctx context.Context, req *spi.Request, st stack, was, done, failed string) error {
	left, first := []res{}, error(nil) // left is never null: every read of the stack lists its resources
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
	_, wrote, err := p.settle(ctx, req, st.Name, from(st, was))
	if !wrote && err == nil && was == "CREATE_IN_PROGRESS" {
		st.Status = map[bool]string{true: "DELETE_COMPLETE", false: "DELETE_FAILED"}[first == nil]
		_, _, err = p.settle(ctx, req, st.Name, from(st, "DELETE_IN_PROGRESS"))
	}
	return cmp.Or(first, err)
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

// kinds are the types provision names: the property that names one, the longest name its service takes (API
// Gateway sets none for a REST API, nor holds a name to one; 1024 only bounds it), a read that finds one by
// name, the service's error for a name taken (SNS's CreateTopic "is idempotent": CloudFormation's), its id.
var kinds = map[string]struct {
	member                 string
	max                    int
	svc, op, in, code, ref string
}{
	"AWS::S3::Bucket":             {"BucketName", 63, "aws.s3", "HeadBucket", "Bucket", "BucketAlreadyOwnedByYou", "{n}"},
	"AWS::SQS::Queue":             {"QueueName", 80, "aws.sqs", "GetQueueUrl", "QueueName", "QueueAlreadyExists", ""},
	"AWS::SNS::Topic":             {"TopicName", 256, "aws.sns", "GetTopicAttributes", "TopicArn", "AlreadyExistsException", "arn:aws:sns:{r}:{a}:{n}"},
	"AWS::DynamoDB::Table":        {"TableName", 255, "aws.dynamodb", "DescribeTable", "TableName", "ResourceInUseException", "{n}"},
	"AWS::IAM::Role":              {"RoleName", 64, "aws.iam", "GetRole", "RoleName", "EntityAlreadyExists", "{n}"},
	"AWS::SSM::Parameter":         {"Name", 2048, "aws.ssm", "GetParameter", "Name", "ParameterAlreadyExists", "{n}"},
	"AWS::SecretsManager::Secret": {"Name", 512, "aws.secretsmanager", "DescribeSecret", "SecretId", "ResourceExistsException", "arn:aws:secretsmanager:{r}:{a}:secret:{n}"},
	"AWS::Logs::LogGroup":         {"LogGroupName", 512, "aws.logs", "DescribeLogGroups", "logGroupNamePrefix", "ResourceAlreadyExistsException", "{n}"},
	"AWS::Events::Rule":           {"Name", 64, "aws.events", "DescribeRule", "Name", "ResourceAlreadyExistsException", "{n}"},
	"AWS::Lambda::Function":       {"FunctionName", 64, "aws.lambda", "GetFunction", "FunctionName", "ResourceConflictException", "{n}"},
	"AWS::Kinesis::Stream":        {"Name", 128, "aws.kinesis", "DescribeStreamSummary", "StreamName", "ResourceInUseException", "{n}"},
	"AWS::ApiGateway::RestApi":    {"Name", 1024, "", "", "", "", ""},
}

// attributes are each type's Fn::GetAtt attributes, from the id its Ref answers, as its Template Reference
// page lists them: {id/} is the id's last path segment, {id:} its last colon-separated one, {rule} a rule's
// name and {bus/} the bus before it, and {param} a parameter's name without a leading slash.
var attributes = map[string]string{
	"AWS::S3::Bucket":             "Arn=arn:aws:s3:::{id} DomainName={id}.s3.amazonaws.com",
	"AWS::SQS::Queue":             "Arn=arn:aws:sqs:{r}:{a}:{id/} QueueName={id/} QueueUrl={id}",
	"AWS::SNS::Topic":             "TopicArn={id} TopicName={id:}",
	"AWS::DynamoDB::Table":        "Arn=arn:aws:dynamodb:{r}:{a}:table/{id}",
	"AWS::IAM::Role":              "Arn=arn:aws:iam::{a}:role/{id}",
	"AWS::SSM::Parameter":         "Arn=arn:aws:ssm:{r}:{a}:parameter/{param}",
	"AWS::SecretsManager::Secret": "Id={id}",
	"AWS::KMS::Key":               "Arn=arn:aws:kms:{r}:{a}:key/{id} KeyId={id}",
	"AWS::Logs::LogGroup":         "Arn=arn:aws:logs:{r}:{a}:log-group:{id}:*",
	"AWS::Events::Rule":           "Arn=arn:aws:events:{r}:{a}:rule/{bus/}{rule} RuleName={rule}",
	"AWS::Lambda::Function":       "Arn=arn:aws:lambda:{r}:{a}:function:{id}",
	"AWS::Kinesis::Stream":        "Arn=arn:aws:kinesis:{r}:{a}:stream/{id}",
	"AWS::ApiGateway::RestApi":    "RestApiId={id}",
}

// attribute sets refs' <logical>.<attribute> for each attribute of typ, from id, what its Ref answers.
func attribute(refs map[string]string, req *spi.Request, typ, logical, id string) {
	bus, rule, onBus := strings.Cut(id, "|")
	if bus += "/"; !onBus {
		bus, rule = "", id
	}
	sub := strings.NewReplacer("{r}", req.Identity.Region, "{a}", req.Identity.Account, "{id}", id, "{id/}", spi.LastSlash(id),
		"{id:}", lastColon(id), "{rule}", rule, "{bus/}", bus, "{param}", strings.TrimPrefix(id, "/"))
	for _, a := range strings.Fields(attributes[typ]) {
		k, f, _ := strings.Cut(a, "=")
		refs[logical+"."+k] = sub.Replace(f)
	}
}

// unknown stands for an id known only once its resource is made, and in order for one not yet ordered.
const unknown = "\uffff"

// planID is the id a resource named n is made with, which a Ref to it answers: n in the form its
// service gives it back. A queue's URL (its endpoint's business), a KMS key's id and a REST API's are
// known only once made, as is an id built on one: unknown. A Kinesis resource policy's is its ARN.
func planID(req *spi.Request, typ, n string, props map[string]any) string {
	if bus := spi.LastSlash(str(props["EventBusName"])); typ == "AWS::Events::Rule" && bus != "" && bus != "default" {
		n = bus + "|" + n // a rule's "event rule ID": <bus>|<name> on any bus but the default (a reading)
	}
	if f := kinds[typ].ref; f != "" && !strings.Contains(n, unknown) {
		return strings.NewReplacer("{r}", req.Identity.Region, "{a}", req.Identity.Account, "{n}", n).Replace(f)
	}
	return cmp.Or(str(props["ResourceArn"]), str(props["ResourceARN"]), unknown)
}

// order is the order a template's resources are made in: each after those its properties name, by Ref,
// Fn::GetAtt or Fn::Sub ("Resource B is created before resource A", Template Reference, DependsOn
// attribute), else by logical id. One walk of each resource's properties finds the names, and Kahn's
// algorithm orders them, the earliest logical id first of those ready. A cycle orders none, and is
// CloudFormation's error. A Fn::If whose condition params settle names only what its branch taken does.
func (p *Pack) order(tpl map[string]any, params map[string]string) ([]string, error) {
	resources, waiting, users := asMap(tpl["Resources"]), map[string]int{}, map[string][]string{}
	var ready, order []string
	refs := conditions(tpl, params)
	for logical, r := range resources {
		p.walks++
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, refs)
		for n := range named {
			if _, ok := resources[n]; ok {
				waiting[logical], users[n] = waiting[logical]+1, append(users[n], logical)
			}
		}
		if waiting[logical] == 0 {
			ready = append(ready, logical)
		}
	}
	for slices.Sort(ready); len(ready) > 0; ready = ready[1:] {
		order = append(order, ready[0])
		for _, u := range users[ready[0]] {
			if waiting[u]--; waiting[u] == 0 {
				i, _ := slices.BinarySearch(ready[1:], u)
				ready = slices.Insert(ready, i+1, u)
			}
		}
	}
	if len(order) < len(resources) { // the cycle is those that lead back to themselves, not those after it
		cycle := slices.DeleteFunc(slices.Sorted(maps.Keys(resources)), func(l string) bool {
			seen := map[string]bool{}
			for next := slices.Clone(users[l]); len(next) > 0 && !seen[l]; {
				u := next[len(next)-1]
				if next = next[:len(next)-1]; !seen[u] {
					seen[u], next = true, append(next, users[u]...)
				}
			}
			return !seen[l]
		})
		return nil, invalid("Circular dependency between resources: [" + strings.Join(cycle, ", ") + "]")
	}
	return order, nil
}

// names adds to named every name v refers to: a Ref's, a Fn::GetAtt's resource, and each Name of a
// ${Name} or ${Name.Attribute} in a Fn::Sub string, but for a variable its map defines, whose value is
// walked instead, and a ${!Literal}, which names nothing.
func names(v any, named map[string]bool, refs map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			switch k {
			case "Ref":
				named[str(x)] = true
			case "Fn::GetAtt":
				logical, _, _ := strings.Cut(str(x), ".")
				named[cmp.Or(str(append(spi.Slice(x), nil)[0]), logical)] = true
			case "Fn::Sub":
				parts := append(spi.Slice(x), x, nil) // "text", or ["text", {variables}]
				for _, m := range subVariable.FindAllStringSubmatch(str(parts[0]), -1) {
					if n, _, _ := strings.Cut(m[1], "."); asMap(parts[1])[m[1]] == nil && !strings.HasPrefix(n, "!") {
						named[n] = true
					}
				}
				names(parts[1], named, refs)
			case "Fn::If": // [condition, then, else]
				branches := append(spi.Slice(x), nil, nil, nil)
				if yes, settled := refs["Condition::"+str(branches[0])]; settled {
					x = branches[map[bool]int{true: 1, false: 2}[yes == "true"]]
				}
				names(x, named, refs)
			default:
				names(x, named, refs)
			}
		}
	case []any:
		for _, x := range t {
			names(x, named, refs)
		}
	}
}

// truth is a condition's value when the parameters settle it, and whether they do: Fn::Equals of two
// literals or Refs to parameters, and Fn::Not, Fn::And, Fn::Or and Condition over such conditions (Template
// Reference, condition functions). Anything else, a pseudo parameter included, settles nothing, nor does a
// condition that names itself. memo holds each named condition once evaluated, so each is evaluated once.
func truth(c any, conds map[string]any, params map[string]string, memo map[string][2]bool) (bool, bool) {
	m, text := asMap(c), [2]string{}
	if name, ok := m["Condition"]; ok {
		if r, seen := memo[str(name)]; seen { // or being evaluated: a condition that names itself
			return r[0], r[1]
		}
		memo[str(name)] = [2]bool{}
		yes, settled := truth(conds[str(name)], conds, params, memo)
		memo[str(name)] = [2]bool{yes, settled}
		return yes, settled
	} else if args := spi.Slice(m["Fn::Equals"]); len(args) == 2 {
		for i, x := range args {
			v, ok := coerce(x, "String")
			if r, isRef := asMap(x)["Ref"]; isRef {
				v, ok = params[str(r)]
			}
			if text[i] = str(v); !ok {
				return false, false
			}
		}
		return text[0] == text[1], true
	}
	for op, a := range m {
		args, trues := spi.Slice(a), 0
		for _, x := range args {
			yes, ok := truth(x, conds, params, memo)
			if trues += map[bool]int{true: 1}[yes]; !ok {
				return false, false
			}
		}
		if rule, ok := map[string][2]bool{"Fn::Not": {trues == 0, len(args) == 1}, "Fn::And": {trues == len(args), len(args) > 0},
			"Fn::Or": {trues > 0, len(args) > 0}}[op]; ok {
			return rule[0], rule[1]
		}
	}
	return false, false
}

// conditions are params, with each template condition they settle as "Condition::<name>", "true" or
// "false": every condition evaluated once (truth).
func conditions(tpl map[string]any, params map[string]string) map[string]string {
	refs, memo := maps.Clone(params), map[string][2]bool{}
	for name := range asMap(tpl["Conditions"]) {
		if yes, settled := truth(map[string]any{"Condition": name}, asMap(tpl["Conditions"]), params, memo); settled {
			refs["Condition::"+name] = strconv.FormatBool(yes)
		}
	}
	return refs
}

// subVariable is a ${...} in a Fn::Sub string.
var subVariable = regexp.MustCompile(`\$\{([^}]*)\}`)

// claim answers the name a resource is made under: the template's, or else physicalName's. A given name
// that a resource the stack did not make under it holds is the service's error for a name taken: several
// services answer such a create with what they hold, which a rollback or DeleteStack would then delete.
func (p *Pack) claim(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (string, error) {
	k, n := kinds[typ], str(props[kinds[typ].member])
	if v, set := props["FifoQueue"]; typ == "AWS::SQS::Queue" && set && !isBoolean(v) { // FifoQueue is a Boolean (Template Reference)
		return "", invalid("Properties validation failed for resource " + logical + ": FifoQueue must be a Boolean, not " + spi.JSONText(v))
	}
	if n == "" || strings.Contains(n, unknown) { // one built on an id known only once made is checked as it is made
		return cmp.Or(n, physicalName(st.Name, logical, st.ID, typ, props)), nil
	}
	id := n
	if k.in == "TopicArn" { // GetTopicAttributes finds a topic by its ARN
		id = planID(req, typ, n, props)
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

func fifo(p map[string]any) bool { b, ok := coerce(p["FifoQueue"], "Boolean"); return ok && b == true }

func isBoolean(v any) bool { _, ok := coerce(v, "Boolean"); return ok }

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

// provision creates a resource through the service that owns it, and answers the resource's physical id.
// Going through the owner's API keeps its store layout, validation and cascades that service's business.
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
		_, err := p.call(ctx, req, "aws.iam", "CreateRole", map[string]any{"RoleName": n, "AssumeRolePolicyDocument": spi.JSONText(props["AssumeRolePolicyDocument"])})
		return made(n, err)
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
			in["EventPattern"] = spi.JSONText(pattern)
		}
		out, err := p.call(ctx, req, "aws.events", "PutRule", in)
		if targets, ok := props["Targets"]; ok && err == nil {
			_, err = p.call(ctx, req, "aws.events", "PutTargets", given(map[string]any{"Rule": in["Name"], "Targets": targets}, props, "EventBusName"))
		}
		if out["RuleArn"] == nil { // PutRule made nothing
			return "", err
		}
		return planID(req, typ, n, props), err
	case "AWS::Lambda::Function":
		in := maps.Clone(props)
		in["FunctionName"] = n
		_, err := p.call(ctx, req, "aws.lambda", "CreateFunction", in)
		return made(n, err)
	case "AWS::Kinesis::Stream":
		_, err := p.call(ctx, req, "aws.kinesis", "CreateStream", map[string]any{"StreamName": n})
		return made(n, err)
	case "AWS::Kinesis::ResourcePolicy":
		arn := cmp.Or(str(props["ResourceArn"]), str(props["ResourceARN"]))
		policy := props["ResourcePolicy"]
		if policy == nil {
			policy = props["Policy"]
		}
		_, err := p.call(ctx, req, "aws.kinesis", "PutResourcePolicy", map[string]any{"ResourceARN": arn, "Policy": spi.JSONText(policy)})
		return made(arn, err)
	case "AWS::ApiGateway::RestApi":
		out, err := p.call(ctx, req, "aws.apigateway", "CreateRestApi", map[string]any{"name": n})
		return str(out["id"]), err
	default:
		return "", invalid("unsupported resource type " + typ)
	}
}

// ruleBus is the event bus in a rule's id, <bus>|<name>, or its ARN, rule/<bus>/<name> (a stack recorded
// before Ref answered the id): none for the default bus.
func ruleBus(id string) string {
	if bus, _, ok := strings.Cut(id, "|"); ok {
		return bus
	}
	_, path, _ := strings.Cut(id, ":rule/")
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
		"AWS::IAM::Role":               {"aws.iam", "DeleteRole", map[string]any{"RoleName": spi.LastSlash(id)}},
		"AWS::SSM::Parameter":          {"aws.ssm", "DeleteParameter", map[string]any{"Name": id}},
		"AWS::SecretsManager::Secret":  {"aws.secretsmanager", "DeleteSecret", map[string]any{"SecretId": id, "ForceDeleteWithoutRecovery": true}},
		"AWS::KMS::Key":                {"aws.kms", "ScheduleKeyDeletion", map[string]any{"KeyId": id, "PendingWindowInDays": 7}},
		"AWS::Logs::LogGroup":          {"aws.logs", "DeleteLogGroup", map[string]any{"logGroupName": id}},
		"AWS::Events::Rule":            {"aws.events", "DeleteRule", map[string]any{"Name": spi.LastSlash(id[strings.Index(id, "|")+1:]), "Force": true, "EventBusName": ruleBus(id)}},
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
		resp, err := pack.Invoke(ctx, &spi.Request{Identity: req.Identity, AdvertiseURL: advertise(req), Operation: op, Input: input(id, op, in)})
		if err != nil {
			return nil, err
		}
		return resp.Output, nil
	}
	return nil, spi.NotImplemented(id, op, "emulate")
}

// input is in with each member read as the type the operation's model declares for it (typed).
func input(id, op string, in map[string]any) map[string]any {
	svc := specboot.Bundle().ServiceByID(id)
	if o := svc.OperationByName(op); o != nil {
		in, _ = typed(svc, o.Input, in).(map[string]any)
	}
	return in
}

// typed is v read as the shape it is given for declares (coerce), through structures, lists and maps:
// a template's 256 or "256" is 256 for an integer member, true and 123 are "true" and "123" for a string,
// "true" is true for a boolean. A value that is not its type is left as it was given, for the service to
// refuse.
func typed(svc *model.Service, shapeID string, v any) any {
	sh := svc.Shapes[shapeID]
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = typed(svc, map[bool]string{true: sh.Member, false: sh.Members[k].Shape}[sh.Kind == model.KindMap], x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = typed(svc, sh.Member, x)
		}
		return out
	}
	want := map[model.ShapeKind]string{model.KindString: "String", model.KindEnum: "String", model.KindInteger: "Integer",
		model.KindLong: "Integer", model.KindBoolean: "Boolean"}[sh.Kind]
	if c, ok := coerce(v, want); ok && want != "" {
		return c
	}
	return v
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
			} else if property == "Tags" { // typed before it is lifted, as call types only the model's members
				tagging := asMap(input("aws.s3", op, map[string]any{"Tagging": map[string]any{"TagSet": v}})["Tagging"])
				in = map[string]any{"Bucket": bucket, "Tagging": tagging, "TagSet": tagging["TagSet"]}
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
	if text, yes := spi.JSONText(v), strings.EqualFold(spi.JSONText(v), "true"); want == "Boolean" {
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
		return "", invalid("GenerateSecretString must be an object, not " + spi.JSONText(props["GenerateSecretString"]))
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
			return "", invalid("GenerateSecretString." + member + " must be " + generateTypes[member] + ", not " + spi.JSONText(v))
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

func (p *Pack) resolve(v any, refs map[string]string, req *spi.Request, stack string) any {
	switch t := v.(type) {
	case map[string]any:
		if r, ok := t["Ref"]; ok {
			v, _ := p.ref(str(r), refs, req, stack)
			return v
		}
		if g, ok := t["Fn::GetAtt"]; ok {
			return getAtt(g, refs)
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

// ref is what a Ref to name answers, a pseudo parameter's value, a parameter's or a resource's, and
// whether name is any of them; the name itself when it is none.
func (p *Pack) ref(name string, refs map[string]string, req *spi.Request, stack string) (string, bool) {
	pseudo := map[string]string{"AWS::AccountId": req.Identity.Account, "AWS::Region": req.Identity.Region,
		"AWS::StackName": stack, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com"}
	if v, ok := pseudo[name]; ok {
		return v, true
	}
	v, ok := refs[name]
	return cmp.Or(v, name), ok
}

// getAtt is a Fn::GetAtt's value: the attribute the resource's type has (attribute), or else its Ref's.
func getAtt(g any, refs map[string]string) string {
	logical, attr, _ := strings.Cut(str(g), ".") // "A.B", or [A, B]
	if t, ok := g.([]any); ok {
		t = append(slices.Clip(t), nil, nil)
		logical, attr = str(t[0]), str(t[1])
	}
	return cmp.Or(refs[logical+"."+attr], refs[logical])
}

func (p *Pack) fnJoin(j any, refs map[string]string, req *spi.Request, stack string) string {
	arr, parts := append(spi.Slice(j), nil, nil), []string(nil) // [separator, [values]]
	for _, x := range spi.Slice(arr[1]) {
		parts = append(parts, str(p.resolve(x, refs, req, stack)))
	}
	return strings.Join(parts, str(arr[0]))
}

// fnSub is the Fn::Sub string with each ${...} substituted: ${!Literal} is the literal ${Literal}, a
// variable its map defines is the variable's value, ${Name.Attribute} is Fn::GetAtt's and ${Name} a Ref's
// (Template Reference, Fn::Sub). A name that is none of them is left as written.
func (p *Pack) fnSub(s any, refs map[string]string, req *spi.Request, stack string) string {
	parts := append(spi.Slice(s), s, nil) // "text", or ["text", {variables}]
	return subVariable.ReplaceAllStringFunc(str(parts[0]), func(m string) string {
		name := m[2 : len(m)-1]
		if literal, ok := strings.CutPrefix(name, "!"); ok {
			return "${" + literal + "}"
		} else if v, ok := asMap(parts[1])[name]; ok {
			return str(p.resolve(v, refs, req, stack))
		} else if logical, attr, ok := strings.Cut(name, "."); ok {
			return getAtt([]any{logical, attr}, refs)
		} else if v, ok := p.ref(name, refs, req, stack); ok {
			return v
		}
		return m
	})
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

// parameters are the request's parameter values, and each Default of a parameter it does not give.
func parameters(in, tpl map[string]any) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		if pfx, ok := strings.CutSuffix(k, ".ParameterKey"); ok && strings.HasPrefix(k, "Parameters.member.") {
			out[str(v)] = str(in[pfx+".ParameterValue"])
		}
	}
	for k, v := range asMap(tpl["Parameters"]) {
		if _, given := out[k]; !given && asMap(v)["Default"] != nil {
			out[k] = spi.JSONText(asMap(v)["Default"]) // a parameter "becomes a string": Default 16 is "16"
		}
	}
	return out
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

func lastColon(s string) string { return s[strings.LastIndex(s, ":")+1:] }
