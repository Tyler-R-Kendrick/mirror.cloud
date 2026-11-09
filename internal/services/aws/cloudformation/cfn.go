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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/prim"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/specboot"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// natives are the operations behavior/aws/cloudformation lists as native: the template engine. Parsing a
// template, resolving its intrinsics and provisioning resources through their owning services is Go; the
// stack, change set, stack set and type bookkeeping is the bundle's.
var natives = []string{"CreateStack", "UpdateStack", "DeleteStack", "ExecuteChangeSet", "ValidateTemplate", "GetTemplateSummary", "ListExports", "ContinueUpdateRollback", "CreateChangeSet", "UpdateTerminationProtection"}

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
	if _, given := req.Input["StackName"]; given && (req.Operation == "ExecuteChangeSet" || req.Operation == "GetTemplateSummary" || req.Operation == "ContinueUpdateRollback" || req.Operation == "CreateChangeSet" || req.Operation == "UpdateTerminationProtection") {
		if err := checkStackName(name, true); err != nil { // the model's StackNameOrId
			return nil, err
		}
	}
	switch req.Operation {
	case "ValidateTemplate":
		_, tpl, _, _, err := p.template(ctx, req, "", "")
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Description": str(tpl["Description"]), "Parameters": paramDecls(tpl)}}, nil
	case "CreateStack", "UpdateStack":
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack")
	case "DeleteStack":
		return p.drop(ctx, req, name)
	case "ContinueUpdateRollback":
		return p.resume(ctx, req, name)
	case "UpdateTerminationProtection": // in one transaction, as every write of the record is (settle)
		v := req.Input["EnableTerminationProtection"] // required, a Boolean (the model), in any case
		protect, isBoolean := coerce(v, "Boolean")
		if v == nil { // the bundle's missing_input_error, which the operation answered before it was native
			return nil, invalid("EnableTerminationProtection")
		} else if !isBoolean {
			return nil, invalid("EnableTerminationProtection must be a Boolean, not " + spi.JSONText(v))
		}
		var st stack
		err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) (err error) {
			raw, ok, _ := tx.Get(named(name))
			if st, err = found(name, raw, ok); err == nil && st.Status == "DELETE_COMPLETE" { // read by its id, a deleted stack changes no more
				err = invalid("Stack [" + name + "] does not exist")
			} else if err == nil {
				st.Protect = protect == true
				err = tx.Put(named(name), []byte(spi.JSONText(st)))
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"StackId": st.ID}}, nil
	case "GetTemplateSummary":
		_, tpl, _, _, err := p.template(ctx, req, "", "")
		if st, loadErr := p.load(ctx, req, name); err != nil && name != "" && loadErr == nil && cmp.Or(str(req.Input["TemplateBody"]), str(req.Input["TemplateURL"])) == "" {
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
	case "CreateChangeSet": // the template is read now: "if your attempt to create a change set failed, CloudFormation shows the error message" (StatusReason)
		csn, body := str(req.Input["ChangeSetName"]), str(req.Input["TemplateBody"])
		if body == "" {
			return nil, invalid("TemplateBody required")
		} else if st, _ := p.load(ctx, req, name); !updatable(st) { // as UpdateStack refuses one
			return nil, invalid("Stack:" + st.ID + " is in " + st.Status + " state and can not be updated.")
		} else if _, err := p.templateBody(ctx, req); err != nil { // the member's size, as every operation's
			return nil, err
		}
		rec := map[string]any{"ChangeSetName": csn, "ChangeSetId": "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":changeSet/" + csn + "/" + p.deps.Rand.Hex(8),
			"StackName": name, "ChangeSetType": cmp.Or(str(req.Input["ChangeSetType"]), "UPDATE"), "Status": "CREATE_COMPLETE", "TemplateBody": body}
		if _, _, _, _, err := p.template(ctx, req, named(name), ""); err != nil {
			rec["Status"], rec["StatusReason"] = "FAILED", err.Error()
		}
		raw, _ := json.Marshal(rec)
		if err := p.col(req, "cfn-cs").Put(ctx, named(name)+":"+csn, raw); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Id": rec["ChangeSetId"], "StackId": "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name}}, nil
	case "ExecuteChangeSet":
		csn := str(req.Input["ChangeSetName"])
		b, ok, _ := p.col(req, "cfn-cs").Get(ctx, named(name)+":"+csn)
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		if _ = json.Unmarshal(b, &rec); rec["Status"] == "FAILED" { // "The specified change set can't be used to update the stack"
			return nil, fault("InvalidChangeSetStatus", "ChangeSet ["+str(rec["ChangeSetId"])+"] cannot be executed in its current status of [FAILED]")
		}
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
	Events                     []res `json:",omitempty"` // what left the stack and how: DELETE_SKIPPED
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
	given, name := name, named(name)
	id := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/" + p.deps.Rand.Hex(8)
	if cur, err := p.load(ctx, req, given); err == nil && update { // the id an update's AWS::StackId answers
		id = cur.ID
	}
	body, tpl, order, refs, err := p.template(ctx, req, name, id)
	for _, c := range slices.Sorted(maps.Keys(asMap(tpl["Conditions"]))) { // a stack is made only with every condition settled
		if _, settled := refs["Condition::"+c]; err == nil && !settled {
			err = invalid("Template error: the condition " + c + " cannot be evaluated") // a function the engine does not evaluate
		}
	}
	if err != nil {
		return nil, err
	}
	// "CloudFormation creates the resource only when the condition evaluates to true", and "any resources that
	// depend on that resource aren't created either" (Template Reference, Condition attribute): those skip.
	resources, outs := asMap(tpl["Resources"]), asMap(tpl["Outputs"])
	off := func(v any) bool { c := str(asMap(v)["Condition"]); return c != "" && refs["Condition::"+c] != "true" }
	skip := map[string]bool{}
	for _, l := range order {
		skip[l] = off(resources[l]) || slices.ContainsFunc(append(referred(asMap(resources[l])["Properties"], refs), dependsOn(resources[l])...), func(n string) bool { return skip[n] })
	}
	maps.DeleteFunc(outs, func(_ string, o any) bool { return off(o) }) // "CloudFormation creates the output only when the associated condition evaluates to true"
	for _, o := range outs {
		if gone := slices.DeleteFunc(referred(asMap(o)["Value"], refs), func(n string) bool { return !skip[n] }); len(gone) > 0 {
			return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(gone, ", ") + "] in the Outputs block of the template")
		}
	}
	order = slices.DeleteFunc(order, func(l string) bool { return skip[l] })
	var old stack // what an update finds; a create reserves the name, CREATE_IN_PROGRESS, in the same transaction
	reserved := stack{Name: name, ID: id, Status: "CREATE_IN_PROGRESS", Template: body, Resources: []res{}, Outputs: []any{}}
	if err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, found, _ := tx.Get(name)
		if _ = json.Unmarshal(raw, &old); old.Status == "DELETE_COMPLETE" { // a deleted stack's name is free
			found, old = false, stack{}
		}
		if found && !update {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists")
		} else if update && (!found || given != name && old.ID != given) { // an update of a stack that does not exist makes none
			return invalid("Stack [" + given + "] does not exist")
		} else if found && !updatable(old) {
			return invalid("Stack:" + old.ID + " is in " + old.Status + " state and can not be updated.")
		} else if id = cmp.Or(old.ID, id); found { // an update reserves nothing: it writes only over what it read
			return nil
		}
		raw, _ = json.Marshal(reserved)
		return tx.Put(name, raw)
	}); err != nil {
		return nil, err
	}
	read := old // the record as the update read it, which its writes go over (a rollback changes old's resources)
	read.Resources = slices.Clone(old.Resources)
	if prev, err := parseTemplate(old.Template); err == nil && update { // settled again: a record before round 9 holds no conditions
		old.Refs = conditioned(prev, old.Refs, req, name, old.ID)
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: refs,
		Resources: []res{}, Outputs: []any{}} // never null: every read of a stack lists both
	changed := map[string]bool{} // what the update changed in place, which a rollback changes back
	fail := func(err error) (*spi.Response, error) {
		if st.Reason, old.Reason = err.Error(), err.Error(); !update || old.ID == "" { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, reserved, st, nil, st.Resources, nil, "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", retains(tpl, "DeletionPolicy", "Retain"))
		} else {
			_ = p.rollback(ctx, req, read, old, st.Resources, changed, retains(tpl, "DeletionPolicy", "Retain"))
		}
		return nil, err
	}
	planned := maps.Clone(st.Refs)
	for _, logical := range order { // a name taken fails the stack before anything is made: a Ref answers the id planned
		rm := asMap(resources[logical])
		resolved, err := p.value(rm["Properties"], planned, req, name)
		typ, props := str(rm["Type"]), asMap(resolved)
		prev, how, n, nextErr := p.next(ctx, req, st, old, logical, typ, props)
		if planned[logical], err = planID(req, typ, n, props), cmp.Or(err, nextErr); how == "" || how == "update" {
			planned[logical] = prev.Physical
		}
		if attribute(planned, req, typ, logical, planned[logical]); err != nil {
			return fail(err)
		}
	}
	for _, o := range outs {
		if _, err := p.value(asMap(o)["Value"], planned, req, name); err != nil {
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
		changed[logical] = how == "update"
		if phys != "" { // recorded once it exists, so a rollback finds a half-made one too
			st.Refs[logical] = phys
			attribute(st.Refs, req, typ, logical, phys)
			st.Resources = append(st.Resources, res{Logical: logical, Physical: phys, Type: typ, Name: n})
		}
		if err != nil {
			return fail(err)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(outs)) {
		st.Outputs = append(st.Outputs, map[string]any{"OutputKey": k, "OutputValue": p.resolve(asMap(outs[k])["Value"], st.Refs, req, name)})
	}
	if cur, wrote, err := p.settle(ctx, req, map[bool]stack{false: reserved, true: read}[update], st); err == nil && !wrote && !update {
		// A DeleteStack begun during the create ends here: what it made goes, and then the stack.
		if cur.ID != id || cur.Status != "DELETE_IN_PROGRESS" {
			cur = stack{} // the stack is another's now: nothing is recorded
		}
		_ = p.unwind(ctx, req, cur, st, nil, st.Resources, nil, "DELETE_COMPLETE", "DELETE_FAILED", retains(tpl, "DeletionPolicy", "Retain"))
		return &spi.Response{Output: map[string]any{"StackId": id}}, nil
	} else if err == nil && !wrote {
		// Another change came first, in progress while this update ran: what this update made that the stack
		// does not hold goes, all of it when the stack is being deleted (its delete may have run first).
		rival := map[bool]string{true: "DELETE_IN_PROGRESS", false: "UPDATE_IN_PROGRESS"}[cur.ID != id || strings.HasPrefix(cur.Status, "DELETE_")]
		if cur.ID == id && rival == "DELETE_IN_PROGRESS" {
			cur.Resources = nil
		}
		made := slices.DeleteFunc(st.Resources, func(r res) bool {
			return slices.ContainsFunc(cur.Resources, func(c res) bool { return c.Physical == r.Physical })
		})
		_ = p.unwind(ctx, req, stack{}, st, nil, made, nil, "", "", func(res) bool { return false })
		return nil, invalid("Stack:" + id + " is in " + rival + " state and can not be updated.")
	}
	// The update is done, so its cleanup deletes the old resources the stack no longer holds, replaced
	// ("creating a new one and deleting the old one") or dropped; one it cannot delete stays listed. A
	// replaced one the template's UpdateReplacePolicy retains, a dropped one the old DeletionPolicy.
	stale := slices.DeleteFunc(slices.Clone(old.Resources), func(r res) bool {
		return slices.ContainsFunc(st.Resources, func(n res) bool { return n.Physical == r.Physical })
	})
	oldTpl, _ := parseTemplate(old.Template)
	_ = p.unwind(ctx, req, st, st, st.Resources, stale, nil, st.Status, st.Status, func(r res) bool {
		if _, replaced := resources[r.Logical]; replaced {
			return retains(tpl, "UpdateReplacePolicy", "Retain")(r)
		}
		return retains(oldTpl, "DeletionPolicy", "Retain", "RetainExceptOnCreate")(r)
	})
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// template reads the request's template body, parses it and orders its resources, and answers what a Ref
// reads besides a resource: the parameters, the pseudo parameters known for the stack named stack, of id
// stack (none of a stack, to read a template alone), and the conditions they settle.
func (p *Pack) template(ctx context.Context, req *spi.Request, stack, id string) (string, map[string]any, []string, map[string]string, error) {
	body, err := p.templateBody(ctx, req)
	if err != nil {
		return "", nil, nil, nil, err
	}
	tpl, err := parseTemplate(body)
	var order []string
	var refs map[string]string
	if err == nil {
		refs = conditioned(tpl, parameters(req.Input, tpl), req, stack, id)
		order, err = p.order(tpl, refs)
	}
	return body, tpl, order, refs, err
}

// conditioned is refs, what a Ref reads, with the conditions of tpl that they and the pseudo parameters of
// the stack named stack, of id id, settle; of the pseudo parameters it keeps the stack id: ref answers the rest.
func conditioned(tpl map[string]any, refs map[string]string, req *spi.Request, stack, id string) map[string]string {
	known, pseudos := maps.Clone(refs), pseudo(req, stack, id)
	maps.Copy(known, pseudos)
	known = conditions(tpl, known)
	maps.DeleteFunc(known, func(k, _ string) bool { return pseudos[k] != "" && k != "AWS::StackId" })
	return known
}

// pseudo are the pseudo parameters (User Guide, pseudo parameters) of the stack named stack, of id id, each
// as a Ref answers it: the stack's two only when it has them.
func pseudo(req *spi.Request, stack, id string) map[string]string {
	known := map[string]string{"AWS::AccountId": req.Identity.Account, "AWS::Region": req.Identity.Region, "AWS::StackName": stack,
		"AWS::StackId": id, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com", "AWS::NoValue": noValue}
	maps.DeleteFunc(known, func(_, v string) bool { return v == "" })
	return known
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

func (p *Pack) drop(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	if nameErr := checkStackName(name, true); nameErr != nil || err != nil {
		return nil, cmp.Or(nameErr, err)
	} else if st.Protect {
		err = invalid("stack has termination protection")
	} else if was, del := st.Status, st; was != "DELETE_IN_PROGRESS" { // one being deleted is left to that delete
		// "Once the call completes successfully, stack deletion starts" (API reference): the record is
		// DELETE_IN_PROGRESS, written only over the record read. One being created is left to its create,
		// which deletes what it made, and then the stack.
		del.Status = "DELETE_IN_PROGRESS"
		tpl, _ := parseTemplate(st.Template)
		if _, started, settleErr := p.settle(ctx, req, st, del); settleErr != nil {
			return nil, settleErr
		} else if !started { // it changed since it was read
			return p.drop(ctx, req, name)
		} else if was != "CREATE_IN_PROGRESS" {
			err = p.unwind(ctx, req, del, del, nil, st.Resources, nil, "DELETE_COMPLETE", "DELETE_FAILED", retains(tpl, "DeletionPolicy", "Retain", "RetainExceptOnCreate"))
		}
	}
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

// unwind deletes the doomed resources, newest first, for a rollback, DeleteStack or an update's cleanup, and
// records the outcome on st, which keeps kept: done, keeping st's reason, with nothing left and no earlier
// error (first); else failed, what it could not delete listed as DELETE_FAILED for the next DeleteStack to try
// again (a cleanup, done and failed alike, abandons it: an event, no longer the stack's), and the first error,
// which it answers, as StackStatusReason. One its policy keeps (skip), or another stack holds, is not deleted:
// it leaves the stack, DELETE_SKIPPED. A DELETE_COMPLETE stack answers its stack id alone, and exports nothing.
func (p *Pack) unwind(ctx context.Context, req *spi.Request, seen, st stack, kept, doomed []res, first error, done, failed string, skip func(res) bool) error {
	st.Resources = append([]res{}, kept...) // never null: every read of the stack lists its resources
	for _, r := range slices.Backward(doomed) {
		if other := p.holder(ctx, req, st.Name, r); other != "" || skip(r) { // "Retain ... keeps the resource"
			r.Status, r.Reason = "DELETE_SKIPPED", map[bool]string{true: "held by stack " + other}[other != ""]
			st.Events = append(st.Events, r)
		} else if err := p.deprovision(ctx, req, r); err != nil {
			r.Status, r.Reason, first = "DELETE_FAILED", err.Error(), cmp.Or(first, err)
			if done == failed { // a cleanup abandons what it cannot delete: an event, no longer the stack's
				st.Events = append(st.Events, r)
			} else {
				st.Resources = slices.Insert(st.Resources, len(kept), r)
			}
		}
	}
	st.Status = done
	if first != nil {
		st.Status, st.Reason = failed, first.Error()
	}
	if st.Status == "DELETE_COMPLETE" {
		st.Outputs = []any{} // never null, as every read of a stack lists its lists
	}
	if seen.ID == "" { // the stack is another's: nothing is recorded
		return first
	}
	_, wrote, err := p.settle(ctx, req, seen, st)
	if seen.Status == "CREATE_IN_PROGRESS" && !wrote && err == nil { // a DeleteStack began during the create: it ends here
		seen.Status, st.Status = "DELETE_IN_PROGRESS", map[bool]string{true: "DELETE_COMPLETE", false: "DELETE_FAILED"}[first == nil]
		_, _, err = p.settle(ctx, req, seen, st)
	}
	return cmp.Or(first, err)
}

// settle writes st over the stack record, in one store transaction, while the record is still seen
// (termination protection aside, which st takes from the record: UpdateTerminationProtection sets it
// whenever it runs), and answers the record it found and whether it wrote.
func (p *Pack) settle(ctx context.Context, req *spi.Request, seen, st stack) (stack, bool, error) {
	var cur stack
	wrote := false
	err := p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, _, _ := tx.Get(st.Name)
		_ = json.Unmarshal(raw, &cur)
		if st.Protect, seen.Protect = cur.Protect, cur.Protect; spi.JSONText(cur) != spi.JSONText(seen) {
			return nil
		}
		wrote = true
		return tx.Put(st.Name, []byte(spi.JSONText(st)))
	})
	return cur, wrote, err
}

// updatable is whether UpdateStack takes the stack st: not ROLLBACK_*, which only DeleteStack leaves, nor
// one that failed (*_FAILED) or is changing (*_IN_PROGRESS).
func updatable(st stack) bool {
	return !strings.HasPrefix(st.Status, "ROLLBACK_") && !strings.HasSuffix(st.Status, "_FAILED") && !strings.HasSuffix(st.Status, "_IN_PROGRESS")
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

// holder is another stack whose record holds r: no stack's delete touches what another holds.
func (p *Pack) holder(ctx context.Context, req *spi.Request, name string, r res) string {
	kvs, _, _ := p.col(req, "cfn").List(ctx, "", "", 0)
	for _, kv := range kvs {
		var other stack
		if _ = json.Unmarshal(kv.Value, &other); other.Name != name && slices.ContainsFunc(other.Resources, func(o res) bool { return o.Type == r.Type && o.Physical == r.Physical }) {
			return other.Name
		}
	}
	return ""
}

// retains answers whether tpl's policy attr keeps a resource rather than deleting it: it is one of keep.
func retains(tpl map[string]any, attr string, keep ...string) func(res) bool {
	return func(r res) bool { return slices.Contains(keep, str(asMap(asMap(tpl["Resources"])[r.Logical])[attr])) }
}

// load reads the stack a StackName names: by its name, or by its stack id while the stack has that id.
func (p *Pack) load(ctx context.Context, req *spi.Request, name string) (stack, error) {
	b, ok, _ := p.col(req, "cfn").Get(ctx, named(name))
	return found(name, b, ok)
}

// found is the stack the record b holds (ok, there is one) while it is the stack name names: by its name
// while it is not deleted, or by its stack id while the stack has that id.
func found(name string, b []byte, ok bool) (stack, error) {
	var st stack
	if _ = json.Unmarshal(b, &st); !ok || named(name) != name && st.ID != name || named(name) == name && st.Status == "DELETE_COMPLETE" {
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
// CloudFormation's), the id one is made with, which a Ref answers (planID), the properties whose change
// "Update requires: Replacement" (the type's Template Reference page), the calls (do) that create one, where
// in their answer its physical id is, and that update one in place and delete one, and its Fn::GetAtt
// attributes (attribute).
type kind struct {
	member                                                    string
	max                                                       int
	find, taken, ref, replace, create, id, update, del, attrs string
}

var kinds = map[string]kind{
	"AWS::S3::Bucket":              {"BucketName", 63, "aws.s3 HeadBucket Bucket={n}", "BucketAlreadyOwnedByYou", "{n}", "BucketName BucketNamePrefix BucketNamespace", "aws.s3 CreateBucket Bucket={n}", "Bucket", "", "aws.s3 DeleteBucket Bucket={id}", "Arn=arn:aws:s3:::{id} DomainName={id}.s3.amazonaws.com"},
	"AWS::SQS::Queue":              {"QueueName", 80, "aws.sqs GetQueueUrl QueueName={n}", "QueueAlreadyExists", "", "QueueName FifoQueue", "aws.sqs CreateQueue QueueName={n} Attributes", "QueueUrl", "", "aws.sqs DeleteQueue QueueUrl={id}", "Arn=arn:aws:sqs:{r}:{a}:{id/} QueueName={id/} QueueUrl={id}"},
	"AWS::SNS::Topic":              {"TopicName", 256, "aws.sns GetTopicAttributes TopicArn=arn:aws:sns:{r}:{a}:{n}", "AlreadyExistsException", "arn:aws:sns:{r}:{a}:{n}", "TopicName FifoTopic", "aws.sns CreateTopic Name={n}", "TopicArn", "", "aws.sns DeleteTopic TopicArn={id}", "TopicArn={id} TopicName={id:}"},
	"AWS::DynamoDB::Table":         {"TableName", 255, "aws.dynamodb DescribeTable TableName={n}", "ResourceInUseException", "{n}", "TableName ImportSourceSpecification", "aws.dynamodb CreateTable * TableName={n}", "TableName", "aws.dynamodb UpdateTable TableName={id} BillingMode ProvisionedThroughput StreamSpecification TableClass DeletionProtectionEnabled", "aws.dynamodb DeleteTable TableName={id}", "Arn=arn:aws:dynamodb:{r}:{a}:table/{id}"},
	"AWS::IAM::Role":               {"RoleName", 64, "aws.iam GetRole RoleName={n}", "EntityAlreadyExists", "{n}", "RoleName Path", "aws.iam CreateRole RoleName={n} AssumeRolePolicyDocument=%AssumeRolePolicyDocument", "Role.RoleName", "aws.iam UpdateAssumeRolePolicy RoleName={id/} PolicyDocument=%AssumeRolePolicyDocument", "aws.iam DeleteRole RoleName={id/}", "Arn=arn:aws:iam::{a}:role/{id}"},
	"AWS::SSM::Parameter":          {"Name", 2048, "aws.ssm GetParameter Name={n}", "ParameterAlreadyExists", "{n}", "Name", "aws.ssm PutParameter Name={n} Value= Value=$Value Type=String Type=$Type", "Name", "aws.ssm PutParameter Name={id} Value= Value=$Value Type=String Type=$Type Overwrite=true", "aws.ssm DeleteParameter Name={id}", "Arn=arn:aws:ssm:{r}:{a}:parameter/{param}"},
	"AWS::SecretsManager::Secret":  {"Name", 512, "aws.secretsmanager DescribeSecret SecretId={n}", "ResourceExistsException", "arn:aws:secretsmanager:{r}:{a}:secret:{n}", "Name", "aws.secretsmanager CreateSecret Name={n} SecretString=$SecretString Description=$Description", "ARN", "aws.secretsmanager UpdateSecret SecretId={id} SecretString=$SecretString Description= Description=$Description", "aws.secretsmanager DeleteSecret SecretId={id} ForceDeleteWithoutRecovery=true", "Id={id}"},
	"AWS::KMS::Key":                {"", 0, "", "", "", "KeySpec KeyUsage MultiRegion", "aws.kms CreateKey Description= Description=$Description", "KeyMetadata.KeyId", "aws.kms UpdateKeyDescription KeyId={id} Description= Description=$Description", "aws.kms ScheduleKeyDeletion KeyId={id} PendingWindowInDays=7", "Arn=arn:aws:kms:{r}:{a}:key/{id} KeyId={id}"},
	"AWS::Logs::LogGroup":          {"LogGroupName", 512, "aws.logs DescribeLogGroups logGroupNamePrefix={n}", "ResourceAlreadyExistsException", "{n}", "LogGroupName", "aws.logs CreateLogGroup logGroupName={n}", "logGroupName", "", "aws.logs DeleteLogGroup logGroupName={id}", "Arn=arn:aws:logs:{r}:{a}:log-group:{id}:*"},
	"AWS::Events::Rule":            {"Name", 64, "aws.events DescribeRule Name={n} EventBusName=$EventBusName", "ResourceAlreadyExistsException", "{n}", "Name EventBusName", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "RuleArn", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "aws.events DeleteRule Name={rule} EventBusName={bus}", "Arn=arn:aws:events:{r}:{a}:rule/{bus/}{rule} RuleName={rule}"},
	"AWS::Lambda::Function":        {"FunctionName", 64, "aws.lambda GetFunction FunctionName={n}", "ResourceConflictException", "{n}", "FunctionName PackageType DurableConfig TenancyConfig", "aws.lambda CreateFunction * FunctionName={n}", "FunctionName", "aws.lambda UpdateFunctionConfiguration * FunctionName={id:}", "aws.lambda DeleteFunction FunctionName={id:}", "Arn=arn:aws:lambda:{r}:{a}:function:{id}"},
	"AWS::Kinesis::Stream":         {"Name", 128, "aws.kinesis DescribeStreamSummary StreamName={n}", "ResourceInUseException", "{n}", "Name", "aws.kinesis CreateStream StreamName={n} ShardCount", "StreamName", "aws.kinesis UpdateShardCount StreamName={id} TargetShardCount=1 TargetShardCount=@ShardCount ScalingType=UNIFORM_SCALING", "aws.kinesis DeleteStream StreamName={id}", "Arn=arn:aws:kinesis:{r}:{a}:stream/{id}"},
	"AWS::Kinesis::ResourcePolicy": {"", 0, "", "", "", "ResourceArn", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "ResourceARN", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "aws.kinesis DeleteResourcePolicy ResourceARN={id}", ""},
	"AWS::ApiGateway::RestApi":     {"Name", 1024, "", "", "", "", "aws.apigateway CreateRestApi name={n}", "id", "aws.apigateway UpdateRestApi restApiId={id} name={n}", "aws.apigateway DeleteRestApi restApiId={id}", "RestApiId={id}"},
}

// do makes a call a kind spells "service Operation member=value ...", and answers the answer over the
// request. A value is {n} the resource's name, {id} its physical id ({id/} and {id:}, its last segment),
// {rule} and {bus} a rule's name and event bus, {r} and {a} the region and account; $Prop is the template's
// property Prop as text, %Prop as JSON text, @Prop as given, each set only when given; else the value
// itself, which call types by the member's model. A member alone is @member, and * is every property. An
// empty spec calls nothing.
func (p *Pack) do(ctx context.Context, req *spi.Request, spec, n, id string, props map[string]any) (map[string]any, error) {
	f, in := strings.Fields(spec), map[string]any{}
	if len(f) < 2 {
		return in, nil
	}
	sub := strings.NewReplacer("{n}", n, "{id}", id, "{id/}", spi.LastSlash(id), "{id:}", lastColon(id), "{rule}", spi.LastSlash(id[strings.Index(id, "|")+1:]),
		"{bus}", ruleBus(id), "{r}", req.Identity.Region, "{a}", req.Identity.Account)
	for _, arg := range f[2:] {
		k, v, set := strings.Cut(arg, "=")
		if !set {
			v = "@" + k
		}
		if pv, given := props[v[min(1, len(v)):]]; arg == "*" {
			maps.Copy(in, props)
		} else if sigil := strings.IndexByte("@$%", (v + " ")[0]); sigil >= 0 {
			if given {
				in[k] = []any{pv, str(pv), spi.JSONText(pv)}[sigil]
			}
		} else {
			in[k] = sub.Replace(v)
		}
	}
	out, err := p.call(ctx, req, f[0], f[1], in)
	maps.Copy(in, out)
	return in, err
}

// attribute sets refs' <logical>.<attribute> for each of typ's attributes, from id, what its Ref answers: {id/}
// is the id's last path segment, {id:} its last colon-separated one, {rule} a rule's name and {bus/} the bus
// before it, and {param} a parameter's name without a leading slash.
func attribute(refs map[string]string, req *spi.Request, typ, logical, id string) {
	bus, rule, onBus := strings.Cut(id, "|")
	if bus += "/"; !onBus {
		bus, rule = "", id
	}
	sub := strings.NewReplacer("{r}", req.Identity.Region, "{a}", req.Identity.Account, "{id}", id, "{id/}", spi.LastSlash(id),
		"{id:}", lastColon(id), "{rule}", rule, "{bus/}", bus, "{param}", strings.TrimPrefix(id, "/"))
	for _, a := range strings.Fields(kinds[typ].attrs) {
		k, f, _ := strings.Cut(a, "=")
		refs[logical+"."+k] = sub.Replace(f)
	}
}

// unknown stands for an id known only once its resource is made, and in order for one not yet ordered.
const unknown = "\uffff"

// noValue is what a Ref to AWS::NoValue answers, and resolve drops.
const noValue = "\ufffe"

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
// attribute), and those its DependsOn names, else by logical id. One walk of each resource's properties finds the names, and Kahn's
// algorithm orders them, the earliest logical id first of those ready. A cycle orders none, and is
// CloudFormation's error. A Fn::If whose condition refs settles names only what its branch taken does.
func (p *Pack) order(tpl map[string]any, refs map[string]string) ([]string, error) {
	resources, waiting, users := asMap(tpl["Resources"]), map[string]int{}, map[string][]string{}
	var ready, order []string
	for logical, r := range resources {
		p.walks++
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, refs)
		for _, d := range dependsOn(r) {
			named[d] = true
		}
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
		cycle := cycles(resources, users, &p.walks)
		return nil, invalid("Circular dependency between resources: [" + strings.Join(cycle, ", ") + "]")
	}
	return order, nil
}

// cycles are the nodes (resources, or conditions) in a cycle of users' references, sorted: each strongly
// connected set of more than one, and each that names itself, found in one pass (Tarjan's algorithm), each
// visit counted in walks.
func cycles(nodes map[string]any, users map[string][]string, walks *int) []string {
	index, low, on := map[string]int{}, map[string]int{}, map[string]bool{}
	var stack, cycle []string
	var visit func(string)
	visit = func(v string) {
		*walks++
		n := len(index) + 1
		index[v], low[v], on[v], stack = n, n, true, append(stack, v)
		for _, w := range users[v] {
			if index[w] == 0 {
				visit(w)
				low[v] = min(low[v], low[w])
			} else if on[w] {
				low[v] = min(low[v], index[w])
			}
		}
		if low[v] == index[v] { // v roots a strongly connected set: what the stack holds from v up
			i := len(stack) - 1
			for stack[i] != v {
				i--
			}
			for _, w := range stack[i:] {
				on[w] = false
			}
			if len(stack[i:]) > 1 || slices.Contains(users[v], v) {
				cycle = append(cycle, stack[i:]...)
			}
			stack = stack[:i]
		}
	}
	for _, l := range slices.Sorted(maps.Keys(nodes)) {
		if index[l] == 0 {
			visit(l)
		}
	}
	slices.Sort(cycle)
	return cycle
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
				logical = cmp.Or(str(append(spi.Slice(x), nil)[0]), logical)
				named[logical], named["."+logical] = true, true // ".": a resource's alone
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
// Reference, condition functions). Anything else, a pseudo parameter included, settles nothing (a cycle of
// conditions parseTemplate refuses). memo holds each named condition once evaluated, so each is evaluated once.
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

// subVariable is a ${...} in a Fn::Sub string, and subName the name one may hold.
var subVariable, subName = regexp.MustCompile(`\$\{([^}]*)\}`), regexp.MustCompile(`^[A-Za-z0-9_.:]+$`)

// change is what an update does to the resource of logical the stack holds: "" keeps it as it is,
// "update" updates it in place, and "replace" makes a new one, when a property whose change "requires
// replacement" (the type's Template Reference page) changed. A resource the stack does not hold is a "create";
// it holds every one it lists, whatever its status (a skipped one is UPDATE_COMPLETE), but one DELETE_FAILED.
func (p *Pack) change(req *spi.Request, old stack, logical, typ string, props map[string]any) (res, string) {
	i := slices.IndexFunc(old.Resources, func(r res) bool { return r.Logical == logical && r.Type == typ && r.Status != "DELETE_FAILED" })
	if i < 0 {
		return res{}, "create"
	}
	tpl, _ := parseTemplate(old.Template)
	before, how := asMap(p.resolve(asMap(asMap(tpl["Resources"])[logical])["Properties"], old.Refs, req, old.Name)), ""
	for _, k := range slices.Concat(slices.Collect(maps.Keys(before)), slices.Collect(maps.Keys(props))) {
		if spi.JSONText(before[k]) == spi.JSONText(props[k]) {
			continue
		} else if how = "update"; slices.Contains(strings.Fields(kinds[typ].replace), k) {
			return old.Resources[i], "replace"
		}
	}
	return old.Resources[i], how
}

// next is what an update does to one resource (change) and the name it is kept or made under: its own,
// claim's, or for a replacement one of its own, made from the old id. A replacement may not keep a name
// the template gives: "CloudFormation cannot update a stack when a custom-named resource requires replacing";
// one the template does not name, a KMS key's or a resource policy's none, is replaced as any other.
func (p *Pack) next(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (res, string, string, error) {
	prev, how := p.change(req, old, logical, typ, props)
	if how == "" || how == "update" && prev.Name != "" {
		return prev, how, prev.Name, nil
	} else if how == "replace" {
		st.ID += "/" + prev.Physical
	}
	n, err := p.claim(ctx, req, st, old, logical, typ, props)
	if err == nil && how == "replace" && n == prev.Name && str(props[kinds[typ].member]) != "" {
		err = invalid("CloudFormation cannot update a stack when a custom-named resource requires replacing. Rename " + n + " and update the stack again.")
	}
	return prev, how, n, err
}

// claim answers the name a resource is made under: the template's, or else physicalName's. A given name
// that a resource the stack did not make under it holds is the service's error for a name taken: several
// services answer such a create with what they hold, which a rollback or DeleteStack would then delete.
func (p *Pack) claim(ctx context.Context, req *spi.Request, st, old stack, logical, typ string, props map[string]any) (string, error) {
	k, n := kinds[typ], str(props[kinds[typ].member])
	if v, set := props["FifoQueue"]; typ == "AWS::SQS::Queue" && set && !isBoolean(v) { // FifoQueue is a Boolean (Template Reference)
		return "", invalid("Properties validation failed for resource " + logical + ": FifoQueue must be a Boolean, not " + spi.JSONText(v))
	}
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

func fifo(p map[string]any) bool { b, ok := coerce(p["FifoQueue"], "Boolean"); return ok && b == true }

func isBoolean(v any) bool { _, ok := coerce(v, "Boolean"); return ok }

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
	if phys != "" && existing == "" && typ == "AWS::Events::Rule" { // made: its id, not the ARN PutRule answers
		phys = planID(req, typ, n, props)
	}
	if phys = cmp.Or(existing, phys); err == nil && typ == "AWS::Events::Rule" {
		targets, set := props["Targets"]
		keep, _ := targets.([]any)
		if err = p.dropTargets(ctx, req, phys, keep); set && err == nil { // the rule's targets are the template's, no others
			// "each entry in FailedEntries provides the ID of the failed target and the error code":
			// a target not added fails the rule, as its error does.
			put, putErr := p.do(ctx, req, "aws.events PutTargets Rule={n} Targets EventBusName=$EventBusName", n, "", props)
			if failed, _ := put["FailedEntries"].([]any); putErr == nil && len(failed) > 0 {
				putErr = fault(cmp.Or(str(asMap(failed[0])["ErrorCode"]), "ValidationError"), "PutTargets failed to add targets: "+spi.JSONText(failed))
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

// rollback undoes a failed update as UPDATE_ROLLBACK does. What the update changed in place is updated
// back, through the same update, to exactly the previous template's properties; what it made, half-made
// ones included, is deleted; what it kept is not called. The stack keeps the previous template and
// resources, UPDATE_ROLLBACK_COMPLETE, or UPDATE_ROLLBACK_FAILED with what it could not delete still listed
// for DeleteStack. Only a finished update deletes what it replaced.
func (p *Pack) rollback(ctx context.Context, req *spi.Request, seen, old stack, made []res, changed map[string]bool, skip func(res) bool) error {
	tpl, _ := parseTemplate(old.Template)
	var doomed []res
	var first error
	for _, r := range made {
		if old.Refs[r.Logical] != r.Physical {
			doomed = append(doomed, r)
		} else if props := maps.Collect(maps.All(asMap(p.resolve(asMap(asMap(tpl["Resources"])[r.Logical])["Properties"], old.Refs, req, old.Name)))); changed[r.Logical] {
			if _, err := p.provision(ctx, req, r.Type, r.Logical, r.Name, props, r.Physical, old.ID); err != nil { // UPDATE_FAILED, for ContinueUpdateRollback
				i := slices.IndexFunc(old.Resources, func(o res) bool { return o.Logical == r.Logical })
				old.Resources[i].Status, old.Resources[i].Reason, first = "UPDATE_FAILED", err.Error(), cmp.Or(first, err)
			}
		}
	}
	return p.unwind(ctx, req, seen, old, old.Resources, doomed, first, "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED", skip)
}

// resume is ContinueUpdateRollback, which "Continues rolling back a stack from UPDATE_ROLLBACK_FAILED to
// UPDATE_ROLLBACK_COMPLETE state": it deletes again what the rollback could not delete (DELETE_FAILED) and
// updates back what it could not restore (UPDATE_FAILED), but the ResourcesToSkip, which "CloudFormation
// sets ... to UPDATE_COMPLETE" and which may be only "resources that are in the UPDATE_FAILED state because
// a rollback failed". A retry that fails leaves the stack UPDATE_ROLLBACK_FAILED, with why.
func (p *Pack) resume(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	skip, _ := req.Input["ResourcesToSkip"].([]any)
	for k, v := range req.Input { // the query protocol's ResourcesToSkip.member.N
		if strings.HasPrefix(k, "ResourcesToSkip.member.") {
			skip = append(skip, v)
		}
	}
	if err == nil && st.Status != "UPDATE_ROLLBACK_FAILED" {
		err = invalid("ContinueUpdateRollback cannot be called from current stack status")
	}
	for _, l := range skip {
		if err == nil && !slices.ContainsFunc(st.Resources, func(r res) bool { return r.Logical == str(l) && r.Status == "UPDATE_FAILED" }) {
			err = invalid("ResourcesToSkip may name only resources UPDATE_FAILED because a rollback failed, not " + str(l))
		}
	}
	if err != nil {
		return nil, err
	}
	tpl, _ := parseTemplate(st.Template)
	kept, doomed, first := []res{}, []res{}, error(nil)
	for _, r := range st.Resources {
		props := maps.Collect(maps.All(asMap(p.resolve(asMap(asMap(tpl["Resources"])[r.Logical])["Properties"], st.Refs, req, st.Name))))
		if r.Status == "DELETE_FAILED" {
			doomed = append(doomed, r)
			continue
		} else if r.Status == "UPDATE_FAILED" && !slices.Contains(skip, any(r.Logical)) {
			_, err = p.provision(ctx, req, r.Type, r.Logical, r.Name, props, r.Physical, st.ID)
		}
		if r.Status == "UPDATE_FAILED" && err != nil {
			r.Reason, first, err = err.Error(), cmp.Or(first, err), nil
		} else if r.Status == "UPDATE_FAILED" {
			r.Status, r.Reason = "UPDATE_COMPLETE", ""
		}
		kept = append(kept, r)
	}
	seen := st
	st.Reason = ""
	if err := p.unwind(ctx, req, seen, st, kept, doomed, first, "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED", func(res) bool { return false }); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
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
	listed, err := p.do(ctx, req, "aws.events ListTargetsByRule Rule={rule} EventBusName={bus}", "", rule, nil)
	targets, _ := listed["Targets"].([]any)
	var ids []any
	for _, t := range targets {
		if !slices.ContainsFunc(keep, func(k any) bool { return str(asMap(k)["Id"]) == str(asMap(t)["Id"]) }) {
			ids = append(ids, asMap(t)["Id"])
		}
	}
	if len(ids) > 0 && err == nil {
		_, err = p.do(ctx, req, "aws.events RemoveTargets Rule={rule} EventBusName={bus} Ids", "", rule, map[string]any{"Ids": ids})
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

// coerce reads a template's value as a property of type want (the template reference's Boolean,
// Integer or String), itself or its text as a Ref to a parameter gives it ("the parameter value
// becomes a string", User Guide, Parameters), and answers whether it is one. A Boolean is true or
// false in any case, as YAML 1.1, the templates' YAML, spells it true, True or TRUE; an Integer is
// decimal digits after an optional minus, as a Number parameter's text is ("016" is 16, "-5" is -5; "16.0", "1e1", "+16" and " 16" are
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
		return n, err == nil && strings.TrimLeft(strings.TrimPrefix(text, "-"), "0123456789") == ""
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
		branches := append(spi.Slice(t["Fn::If"]), nil, nil, nil) // the branch its condition takes (conditions)
		if yes, settled := refs["Condition::"+str(branches[0])]; settled && t["Fn::If"] != nil {
			return p.resolve(branches[map[bool]int{true: 1, false: 2}[yes == "true"]], refs, req, stack)
		}
		out := map[string]any{}
		for k, val := range t {
			if r := p.resolve(val, refs, req, stack); r != noValue { // AWS::NoValue "Removes the corresponding resource property"
				out[k] = r
			}
		}
		return out
	case []any:
		out := []any{}
		for _, val := range t {
			if r := p.resolve(val, refs, req, stack); r != noValue {
				out = append(out, r)
			}
		}
		return out
	default:
		return v
	}
}

// ref is what a Ref to name answers, a pseudo parameter's value, a parameter's or a resource's, and
// whether name is any of them; the name itself when it is none.
func (p *Pack) ref(name string, refs map[string]string, req *spi.Request, stack string) (string, bool) {
	if v, ok := pseudo(req, stack, refs["AWS::StackId"])[name]; ok {
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

// value is v resolved, which AWS::NoValue may leave only as a property it removes or a list element: it
// "Removes the corresponding resource property" (User Guide, pseudo parameters), so it is never a value,
// and as part of one, a Fn::Join's separator, a Fn::Sub's string or variable, an output, it is refused.
func (p *Pack) value(v any, refs map[string]string, req *spi.Request, stack string) (any, error) {
	if r := p.resolve(v, refs, req, stack); !strings.Contains(spi.JSONText(r), noValue) {
		return r, nil
	}
	return nil, invalid("Template error: AWS::NoValue removes a property or a list element; it cannot be part of a value")
}

func (p *Pack) fnJoin(j any, refs map[string]string, req *spi.Request, stack string) string {
	arr, parts := append(spi.Slice(j), nil, nil), []string(nil)        // [separator, [values]]
	for _, x := range spi.Slice(p.resolve(arr[1], refs, req, stack)) { // a list, from which AWS::NoValue removes its element
		parts = append(parts, str(x))
	}
	return strings.Join(parts, str(p.resolve(arr[0], refs, req, stack)))
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
	} else if n := len(asMap(m["Resources"])); n > 500 { // "The maximum number of resources that you can declare ... 500 resources" (quotas)
		return nil, invalid("Template format error: Number of resources, " + strconv.Itoa(n) + ", is greater than maximum allowed, 500")
	}
	// DeletionPolicy's options and UpdateReplacePolicy's (Template Reference): DeletionPolicy's Snapshot is
	// for the types it lists, none of which the engine makes; UpdateReplacePolicy's "reverts to ... Delete".
	for logical, r := range asMap(m["Resources"]) {
		for attr, options := range map[string]string{"DeletionPolicy": "Delete Retain RetainExceptOnCreate", "UpdateReplacePolicy": "Delete Retain Snapshot"} {
			if v, set := asMap(r)[attr]; set && !slices.Contains(strings.Fields(options), str(v)) {
				return nil, invalid("Template format error: " + logical + "'s " + attr + " must be one of [" + strings.ReplaceAll(options, " ", ", ") + "], not " + spi.JSONText(v))
			}
		}
	}
	conds, users := asMap(m["Conditions"]), map[string][]string{} // conditions "based on ... Other conditions" (User Guide)
	for name, c := range conds {
		each(c, func(x map[string]any) {
			if n, ok := x["Condition"]; ok {
				users[str(n)] = append(users[str(n)], name)
			}
		})
	}
	var named, missing []string // "you can't reference resource logical IDs or their attributes in conditions" (User Guide)
	each(conds, func(x map[string]any) {
		target, _, _ := strings.Cut(cmp.Or(str(x["Ref"]), str(append(spi.Slice(x["Fn::GetAtt"]), x["Fn::GetAtt"])[0])), ".")
		if _, resource := asMap(m["Resources"])[target]; resource {
			named = append(named, target)
		}
		if c, ok := x["Condition"].(string); ok && conds[c] == nil {
			missing = append(missing, c)
		}
	})
	spaced := false
	each([]any{m["Resources"], m["Outputs"]}, func(x map[string]any) {
		if c := str(append(spi.Slice(x["Fn::If"]), nil)[0]); x["Fn::If"] != nil && conds[c] == nil {
			missing = append(missing, c)
		}
		for _, v := range subVariable.FindAllStringSubmatch(str(append(spi.Slice(x["Fn::Sub"]), x["Fn::Sub"])[0]), -1) {
			spaced = spaced || !strings.HasPrefix(v[1], "!") && !subName.MatchString(v[1])
		}
	})
	if spaced { // the service's wording: the Template Reference says nothing of a space in ${} (a reading)
		return nil, invalid("Template error: variable names in Fn::Sub syntax must contain only alphanumeric characters, underscores, periods, and colons")
	}
	for _, v := range slices.Concat(slices.Collect(maps.Values(asMap(m["Resources"]))), slices.Collect(maps.Values(asMap(m["Outputs"])))) {
		if c, ok := asMap(v)["Condition"].(string); ok && conds[c] == nil { // a resource's or output's Condition attribute
			missing = append(missing, c)
		}
	}
	if len(named) > 0 {
		return nil, invalid("Template format error: Unresolved dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(named))), ", ") + "]. Cannot reference resources in the Conditions block of the template")
	} else if len(missing) > 0 {
		return nil, invalid("Template error: unresolved condition dependency " + strings.Join(slices.Compact(slices.Sorted(slices.Values(missing))), ", "))
	}
	if cycle := cycles(conds, users, new(int)); len(cycle) > 0 { // none of them can be evaluated
		return nil, invalid("Template format error: Circular dependency between conditions: [" + strings.Join(cycle, ", ") + "]")
	}
	// A Ref and a Fn::Sub variable, which "can be template parameter names, resource logical IDs, resource
	// attributes", name a resource, parameter or pseudo parameter (AWS::...); a Fn::GetAtt and a DependsOn,
	// a resource.
	undeclared := map[string]bool{}
	for _, r := range asMap(m["Resources"]) {
		named := map[string]bool{}
		names(r, named, nil)
		for _, d := range dependsOn(r) {
			named["."+d] = true
		}
		for n := range named {
			if l, resource := strings.CutPrefix(n, "."); asMap(m["Resources"])[l] == nil && (resource || asMap(m["Parameters"])[n] == nil && !strings.HasPrefix(n, "AWS::")) {
				undeclared[l] = true
			}
		}
	}
	if len(undeclared) > 0 {
		return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(slices.Sorted(maps.Keys(undeclared)), ", ") + "] in the Resources block of the template")
	}
	return m, nil
}

// dependsOn is the resources a resource's DependsOn names: it "can take a single string or list of
// strings" (Template Reference, DependsOn attribute).
func dependsOn(r any) []string {
	names := []string{str(asMap(r)["DependsOn"])}
	for _, d := range spi.Slice(asMap(r)["DependsOn"]) {
		names = append(names, str(d))
	}
	return slices.DeleteFunc(names, func(n string) bool { return n == "" })
}

// referred is the names v refers to (names), sorted.
func referred(v any, refs map[string]string) []string {
	named := map[string]bool{}
	names(v, named, refs)
	return slices.Sorted(maps.Keys(named))
}

// each calls fn with every map v holds, v itself included.
func each(v any, fn func(map[string]any)) {
	if m, ok := v.(map[string]any); ok {
		fn(m)
		for _, x := range m {
			each(x, fn)
		}
	}
	for _, x := range spi.Slice(v) {
		each(x, fn)
	}
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
