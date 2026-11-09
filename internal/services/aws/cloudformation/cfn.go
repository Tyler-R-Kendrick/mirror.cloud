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
var natives = []string{"CreateStack", "UpdateStack", "DeleteStack", "CreateChangeSet", "ExecuteChangeSet", "DeleteChangeSet", "ValidateTemplate", "GetTemplateSummary", "ListExports", "UpdateTerminationProtection"}

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
	walks int            // resources whose properties order walked, which a test counts
	tpl   map[string]any // the template an operation reads: its Mappings, which findInMap reads, and its parameters' types
	body  string         // a change set's template, as it was read when the change set was made (templateBody)
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
		_, tpl, _, _, err := p.template(ctx, req, "", "")
		if err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{"Description": str(tpl["Description"]), "Parameters": paramDecls(tpl)}}, nil
	case "CreateStack", "UpdateStack":
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack", "", changeSet{})
	case "DeleteStack":
		return p.drop(ctx, req, name)
	case "UpdateTerminationProtection": // in one transaction, as every write of the record is (settle)
		v := req.Input["EnableTerminationProtection"] // required, a Boolean (the model), in any case
		protect, isBoolean := coerce(v, "Boolean")
		if v == nil { // the bundle's missing_input_error, which the operation answered before it was native
			return nil, invalid("EnableTerminationProtection")
		} else if !isBoolean {
			return nil, invalid("EnableTerminationProtection must be a Boolean, not " + spi.JSONText(v))
		}
		st, found, err := p.settle(ctx, req, name, func(cur stack) (stack, bool) { cur.Protect = protect == true; return cur, cur.ID != "" })
		if err == nil && !found {
			err = invalid("Stack [" + name + "] does not exist")
		}
		return &spi.Response{Output: map[string]any{"StackId": st.ID}}, err
	case "GetTemplateSummary":
		_, tpl, _, _, err := p.template(ctx, req, "", "")
		if st, loadErr := p.load(ctx, req, name); err != nil && name != "" && loadErr == nil && cmp.Or(str(req.Input["TemplateBody"]), str(req.Input["TemplateURL"])) == "" {
			tpl, err = stored(st.Template), nil
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
			st := spi.Decode[stack](kv.Value)
			for _, o := range st.Outputs {
				ex = append(ex, map[string]any{"Name": asMap(o)["OutputKey"], "Value": asMap(o)["OutputValue"], "ExportingStackId": st.ID})
			}
		}
		return &spi.Response{Output: map[string]any{"Exports": ex}}, nil
	case "CreateChangeSet":
		return p.createChangeSet(ctx, req, name)
	case "ExecuteChangeSet":
		key := name + ":" + str(req.Input["ChangeSetName"])
		raw, ok, _ := p.col(req, "cfn-cs").Get(ctx, key)
		rec := spi.Decode[map[string]any](raw) // what CreateChangeSet took that CreateStack and UpdateStack take
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		}
		for _, k := range changeSetMembers {
			if rec[k] != nil {
				req.Input[k] = rec[k]
			}
		}
		p.body = str(rec["TemplateBody"])                                                                                                  // read again by no TemplateBody limit: a TemplateURL's is 1 MB
		_, err := p.upsert(ctx, req, name, rec["ChangeSetType"] != "CREATE", str(rec["StackId"]), changeSet{key, str(rec["ChangeSetId"])}) // claims this copy as it starts
		p.unsettled(ctx, req, key, "AVAILABLE", err)                                                                                       // a refusal leaves it as it was, and says why
		return &spi.Response{Output: map[string]any{}}, err
	case "DeleteChangeSet": // not one being executed (the wording is a reading)
		key := name + ":" + str(req.Input["ChangeSetName"])
		err := p.col(req, "cfn-cs").Txn(ctx, func(tx spi.Tx) error {
			if raw, _, _ := tx.Get(key); status(spi.Decode[map[string]any](raw)) == "EXECUTE_IN_PROGRESS" {
				return fault("InvalidChangeSetStatus", "ChangeSet ["+key+"] cannot be deleted in its current execution status of [EXECUTE_IN_PROGRESS]")
			}
			return tx.Delete(key)
		})
		return &spi.Response{Output: map[string]any{}}, err
	default:
		return nil, spi.NotImplemented("aws.cloudformation", req.Operation, "emulate")
	}
}

// changeSetMembers are what CreateChangeSet takes that CreateStack and UpdateStack take, besides the template
// (TemplateBody, which it keeps as it read it): a change set keeps
// those given, DescribeChangeSet answers them, and ExecuteChangeSet makes or updates the stack with them.
var changeSetMembers = []string{"Parameters", "NotificationARNs", "Tags", "Capabilities"}

// createChangeSet records the change set, and for a new stack the stack itself, in one transaction over what
// it finds: "You can't use the UPDATE type to create a change set for a new stack or the CREATE type to create a
// change set for an existing stack", and "If you create a change set for a new stack, CloudFormation creates a
// stack with a unique stack ID, but no template or resources. The stack will be in the REVIEW_IN_PROGRESS state
// until you execute the change set" (API reference, CreateChangeSet). A UsePreviousValue is read now: its
// Parameters are "the input parameters and their values used to create the change set" (DescribeChangeSet).
func (p *Pack) createChangeSet(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	csn, create := str(req.Input["ChangeSetName"]), req.Input["ChangeSetType"] == "CREATE"
	body, err := p.templateBody(ctx, req)
	if _, paramErr := parameters(req.Input, stored(body)); err == nil {
		err = paramErr
	}
	if err == nil && len(spi.Slice(req.Input["NotificationARNs"])) > 5 { // as CreateStack's
		err = invalid("1 validation error detected: Value at 'notificationARNs' failed to satisfy constraint: Member must have length less than or equal to 5")
	}
	if err != nil {
		return nil, err
	}
	prefix := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account
	rec := map[string]any{"ChangeSetName": csn, "ChangeSetId": prefix + ":changeSet/" + csn + "/" + p.deps.Rand.Hex(8), "StackName": name, "TemplateBody": body, // a TemplateURL's too
		"ChangeSetType": cmp.Or(str(req.Input["ChangeSetType"]), "UPDATE"), "Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE",
		"StackId": prefix + ":stack/" + name + "/" + p.deps.Rand.Hex(8)}
	for _, k := range changeSetMembers {
		if v, given := req.Input[k]; given { // one not given is not kept, Tags and NotificationARNs too: its execution keeps the stack's (previous)
			rec[k] = v
		}
	}
	err = p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		raw, found, _ := stx.Collection("cfn").Get(name)
		cur := spi.Decode[stack](raw)
		review := create && cur.Status == "REVIEW_IN_PROGRESS"
		if _, taken, _ := stx.Collection("cfn-cs").Get(name + ":" + csn); taken { // the wordings are readings
			return fault("AlreadyExistsException", "ChangeSet ["+csn+"] already exists")
		} else if !found && !create {
			return invalid("Stack [" + name + "] does not exist")
		} else if found && create && !review {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists and cannot be created again with the changeSet ["+csn+"].")
		} else if found && !review && (strings.HasSuffix(cur.Status, "_IN_PROGRESS") || strings.HasPrefix(cur.Status, "ROLLBACK_") || cur.Status == "DELETE_FAILED") {
			return invalid("Stack:" + cur.ID + " is in " + cur.Status + " state and can not be updated.")
		} else if found {
			previous(req.Input, cur)
			rec["StackId"] = cur.ID
		} else if err := stx.Collection("cfn").Put(name, []byte(spi.JSONText(stack{Name: name, ID: str(rec["StackId"]), Status: "REVIEW_IN_PROGRESS",
			Resources: []res{}, Outputs: []any{}, NotificationARNs: []any{}, Tags: []any{}}))); err != nil {
			return err
		}
		return stx.Collection("cfn-cs").Put(name+":"+csn, []byte(spi.JSONText(rec)))
	})
	return &spi.Response{Output: map[string]any{"Id": rec["ChangeSetId"], "StackId": rec["StackId"]}}, err
}

// status is a change set's ExecutionStatus; AVAILABLE for a record stored before it had one.
func status(rec map[string]any) string { return cmp.Or(str(rec["ExecutionStatus"]), "AVAILABLE") }

// unsettled records why an execution of the change set under key did not finish, while the change set is
// still in status was: one a refusal left AVAILABLE stays so, one claimed is EXECUTE_FAILED, each with the
// error as its StatusReason.
func (p *Pack) unsettled(ctx context.Context, req *spi.Request, key, was string, cause error) {
	_ = p.col(req, "cfn-cs").Txn(ctx, func(tx spi.Tx) error {
		raw, ok, _ := tx.Get(key)
		if rec := spi.Decode[map[string]any](raw); ok && cause != nil && status(rec) == was {
			if rec["StatusReason"] = cause.Error(); was != "AVAILABLE" {
				rec["ExecutionStatus"] = "EXECUTE_FAILED"
			}
			return tx.Put(key, []byte(spi.JSONText(rec)))
		}
		return nil
	})
}

// retire is what a stack's write that ends an operation does to its change sets, in the same transaction:
// the change set under key being executed (its claim, and take's check of the stack, hold it) is
// EXECUTE_COMPLETE, and "CloudFormation deletes all other change sets associated with the stack"
// (ExecuteChangeSet), every one for a key no change set has; an update of no change set (key "") leaves
// each available one OBSOLETE, "because the stack was already updated" (DescribeChangeSet).
func retire(name, key string) func(spi.Tx) error {
	return func(tx spi.Tx) error {
		kvs, _, _ := tx.List(name+":", "", 0)
		for _, kv := range kvs {
			rec, err := spi.Decode[map[string]any](kv.Value), error(nil)
			if kv.Key == key || key == "" && status(rec) == "AVAILABLE" {
				if rec["ExecutionStatus"] = map[bool]string{true: "EXECUTE_COMPLETE", false: "OBSOLETE"}[kv.Key == key]; kv.Key == key {
					delete(rec, "StatusReason") // a refusal's, before it was executed
				}
				err = tx.Put(kv.Key, []byte(spi.JSONText(rec)))
			} else if key != "" {
				err = tx.Delete(kv.Key)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
}

type stack struct {
	Name, ID, Status, Template string
	Reason                     string `json:",omitempty"` // StackStatusReason
	Resources                  []res
	Outputs                    []any
	Refs                       map[string]string
	NotificationARNs, Tags     []any
	Protect                    bool
}

// res is a resource a stack made, the name it was made under and, once a
// delete of it failed, DELETE_FAILED with the error as its reason.
type res struct {
	Logical, Physical, Type string
	Name, Status, Reason    string `json:",omitempty"`
}

// upsert makes the stack named name, or updates it, a create under id when one is given (a CREATE change
// set's, which it answered), as the execution of the change set under key when one is given: claimed in the
// transaction that starts it, retired in the one that ends it.
func (p *Pack) upsert(ctx context.Context, req *spi.Request, name string, update bool, id string, cs changeSet) (*spi.Response, error) {
	if name == "" {
		return nil, invalid("StackName required")
	} else if len(spi.Slice(req.Input["NotificationARNs"])) > 5 { // "Maximum number of 5 items" (API reference, CreateStack and UpdateStack)
		return nil, invalid("1 validation error detected: Value at 'notificationARNs' failed to satisfy constraint: Member must have length less than or equal to 5")
	}
	id = cmp.Or(id, "arn:aws:cloudformation:"+req.Identity.Region+":"+req.Identity.Account+":stack/"+name+"/"+p.deps.Rand.Hex(8))
	if cur, err := p.load(ctx, req, name); err == nil && update { // the id an update's AWS::StackId answers
		id = cur.ID
		previous(req.Input, cur)
	}
	arns, tags := append([]any{}, spi.Slice(req.Input["NotificationARNs"])...), append([]any{}, spi.Slice(req.Input["Tags"])...)
	body, tpl, order, refs, err := p.template(ctx, req, name, id)
	missing := func(section, prefix string) []string { // what a section declares that refs holds no value of
		return slices.DeleteFunc(slices.Sorted(maps.Keys(asMap(tpl[section]))), func(k string) bool { _, ok := refs[prefix+k]; return ok })
	}
	if unvalued := missing("Parameters", ""); len(unvalued) > 0 && err == nil { // for parameters without default values (User Guide)
		err = invalid("Parameters: [" + strings.Join(unvalued, ", ") + "] must have values")
	} else if unsettled := missing("Conditions", "Condition::"); len(unsettled) > 0 && err == nil { // a stack is made only with every condition settled
		err = invalid("Template error: the condition " + unsettled[0] + " cannot be evaluated") // a function the engine does not evaluate
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
		skip[l] = off(resources[l]) || slices.ContainsFunc(referred(asMap(resources[l])["Properties"], refs), func(n string) bool { return skip[n] })
	}
	maps.DeleteFunc(outs, func(_ string, o any) bool { return off(o) }) // "CloudFormation creates the output only when the associated condition evaluates to true"
	for _, o := range outs {
		if gone := slices.DeleteFunc(referred(asMap(o)["Value"], refs), func(n string) bool { return !skip[n] }); len(gone) > 0 {
			return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(gone, ", ") + "] in the Outputs block of the template")
		}
	}
	order = slices.DeleteFunc(order, func(l string) bool { return skip[l] })
	var old stack // what an update finds; a create reserves the name, CREATE_IN_PROGRESS, in the same transaction
	if err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		tx := stx.Collection("cfn")
		raw, found, _ := tx.Get(name)
		if err := claim(stx.Collection("cfn-cs"), cs); err != nil { // a refusal below puts it back: nothing is written
			return err
		} else if _ = json.Unmarshal(raw, &old); update && !found { // "Stack [x] does not exist": an update makes no stack
			return invalid("Stack [" + name + "] does not exist")
		} else if found && !update && old.Status == "REVIEW_IN_PROGRESS" && old.ID == id {
			found, old = false, stack{} // a CREATE change set's stack, which its execution makes
		}
		if found && !update {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists")
		} else if found && (strings.HasPrefix(old.Status, "ROLLBACK_") || strings.HasSuffix(old.Status, "_IN_PROGRESS") || old.Status == "DELETE_FAILED") {
			return invalid("Stack:" + old.ID + " is in " + old.Status + " state and can not be updated.")
		} else if id = cmp.Or(old.ID, id); found || update { // an update reserves nothing: it writes only over what it read
			return nil
		}
		return tx.Put(name, []byte(spi.JSONText(stack{Name: name, ID: id, Status: "CREATE_IN_PROGRESS", Template: body, Resources: []res{}, Outputs: []any{}, NotificationARNs: arns, Tags: tags})))
	}); err != nil {
		return nil, err
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: refs,
		Resources: []res{}, Outputs: []any{}, NotificationARNs: arns, Tags: tags} // never null: every read of a stack lists them
	fail := func(err error) (*spi.Response, error) {
		if st.Reason = err.Error(); !update { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, st, "CREATE_IN_PROGRESS", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED")
		}
		p.unsettled(ctx, req, cs.key, "EXECUTE_IN_PROGRESS", err)
		return nil, err
	}
	planned := maps.Clone(st.Refs)
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
		phys, n, err := "", "", selected(props)
		if err == nil {
			n, err = p.claim(ctx, req, st, old, logical, typ, props) // again: a name may come from a resource made before it
		}
		if err == nil {
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
	for _, k := range slices.Sorted(maps.Keys(outs)) {
		st.Outputs = append(st.Outputs, map[string]any{"OutputKey": k, "OutputValue": text(p.resolve(asMap(outs[k])["Value"], st.Refs, req, name))})
	}
	if err := selected(st.Outputs); err != nil {
		return fail(err)
	}
	if !update { // a DeleteStack begun during the create ends here: what it made goes, and then the stack
		if _, wrote, err := p.settle(ctx, req, name, from(st, "CREATE_IN_PROGRESS"), retire(name, cs.key)); err == nil && !wrote {
			_ = p.unwind(ctx, req, st, "DELETE_IN_PROGRESS", "DELETE_COMPLETE", "DELETE_FAILED")
		}
	} else if cur, wrote, err := p.settle(ctx, req, name, as(old, st), retire(name, cs.key)); err == nil && !wrote {
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
		return fail(invalid("Stack:" + id + " is in " + rival + " state and can not be updated."))
	}
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// settle rewrites the stack record in one store transaction: take, given the record (empty when there is
// none), answers the record to write instead, and whether to write it; a DELETE_COMPLETE one is removed,
// and with it the stack's change sets. sets, when given, writes the change sets in the same transaction.
// settle answers the record it found and whether it wrote.
func (p *Pack) settle(ctx context.Context, req *spi.Request, name string, take func(stack) (stack, bool), sets ...func(spi.Tx) error) (stack, bool, error) {
	var cur stack
	wrote := false
	err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		tx, cs := stx.Collection("cfn"), stx.Collection("cfn-cs")
		raw, _, _ := tx.Get(name)
		cur = spi.Decode[stack](raw)
		next, yes := take(cur)
		if wrote = yes; !yes {
			return nil
		} else if next.Status == "DELETE_COMPLETE" { // its change sets go with it: retire, keyed by no change set
			if err := retire(name, name)(cs); err != nil {
				return err
			}
			return tx.Delete(name)
		}
		for _, write := range sets {
			if err := write(cs); err != nil {
				return err
			}
		}
		return tx.Put(name, []byte(spi.JSONText(next)))
	})
	return cur, wrote, err
}

// changeSet is the change set an execution executes: its key, and the id of the copy it read.
type changeSet struct{ key, id string }

// claim moves the change set cs (none for a zero cs), still the copy read, from AVAILABLE to
// EXECUTE_IN_PROGRESS: a change set
// executes once, and one executed, failed, obsolete or being executed is InvalidChangeSetStatus, "The
// specified change set can't be used to update the stack" (the wording is a reading).
func claim(tx spi.Tx, cs changeSet) error {
	raw, found, _ := tx.Get(cs.key)
	if rec := spi.Decode[map[string]any](raw); cs.key == "" {
		return nil
	} else if !found || str(rec["ChangeSetId"]) != cs.id { // deleted, or made again, since it was read
		return &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
	} else if s := status(rec); s != "AVAILABLE" {
		return fault("InvalidChangeSetStatus", "ChangeSet ["+str(rec["ChangeSetId"])+"] cannot be executed in its current execution status of ["+s+"]")
	} else {
		rec["ExecutionStatus"] = "EXECUTE_IN_PROGRESS"
		return tx.Put(cs.key, []byte(spi.JSONText(rec)))
	}
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
		return st, cur.ID != "" && reflect.DeepEqual(cur, old) // never over no record: only a create makes one
	}
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
	var known map[string]string
	if err == nil {
		known, err = parameters(req.Input, tpl)
	}
	if err == nil {
		maps.Copy(known, pseudo(req, stack, id))
		p.tpl, refs = tpl, conditions(tpl, known)
		delete(refs, "AWS::NoValue") // ref answers it: no record holds the sentinel
		order, err = p.order(tpl, refs)
	}
	if err == nil {
		err = cmp.Or(p.sound(tpl, refs, req, stack), capable(tpl, req.Input, stack))
	}
	return body, tpl, order, refs, err
}

// pseudo are the pseudo parameters (User Guide, pseudo parameters) of the stack named stack, of id id, each
// as a Ref answers it: the stack's two only when it has them.
func pseudo(req *spi.Request, stack, id string) map[string]string {
	known := map[string]string{"AWS::AccountId": req.Identity.Account, "AWS::Region": req.Identity.Region, "AWS::StackName": stack,
		"AWS::StackId": id, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com", "AWS::NoValue": noValue,
		"AWS::NotificationARNs": spi.JSONText(append([]any{}, spi.Slice(req.Input["NotificationARNs"])...))} // a list, as JSON: [] for none
	maps.DeleteFunc(known, func(_, v string) bool { return v == "" })
	return known
}

// templateBody is the request's template: TemplateBody, "a maximum length of 51,200 bytes", or the S3
// object TemplateURL names, "max size: 1 MB" (API reference, CreateStack), which the quotas page, writing
// 204,800 bytes "204.8 KB", counts as 1,000,000 bytes.
func (p *Pack) templateBody(ctx context.Context, req *spi.Request) (string, error) {
	if p.body != "" { // checked against its own source's limit when its change set was made
		return p.body, nil
	} else if body := str(req.Input["TemplateBody"]); len(body) > 51_200 {
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
	return &spi.Response{Output: map[string]any{}}, err
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
	if b, ok, _ := p.col(req, "cfn").Get(ctx, name); ok {
		return spi.Decode[stack](b), nil
	}
	return stack{}, invalid("Stack [" + name + "] does not exist")
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

// unknown stands for an id known only once its resource is made, and in order for one not yet ordered: what
// a Ref to or a Fn::GetAtt of a resource not made yet answers, and any value holding it is. resolve has three
// outcomes, a value, unselected (a Fn::Select of no element of a list it knows) or unknown.
const unknown = "\uffff"

// noValue is what a Ref to AWS::NoValue answers, and resolve drops; unselected is what a Fn::Select of no
// element answers, which sound refuses.
const noValue, unselected = "\ufffe", "\ufdd0Fn::Select"

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
// CloudFormation's error. A Fn::If whose condition refs settles names only what its branch taken does.
func (p *Pack) order(tpl map[string]any, refs map[string]string) ([]string, error) {
	resources, waiting, users := asMap(tpl["Resources"]), map[string]int{}, map[string][]string{}
	var ready, order, undeclared []string
	for logical, r := range resources {
		p.walks++
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, refs)
		for n := range named { // a Fn::Sub variable "can be template parameter names, resource logical IDs, resource attributes"
			if v, sub := strings.CutPrefix(n, "${"); sub && resources[v] == nil && asMap(tpl["Parameters"])[v] == nil && !strings.HasPrefix(v, "AWS::") {
				undeclared = append(undeclared, v)
			} else if _, ok := resources[n]; ok {
				waiting[logical], users[n] = waiting[logical]+1, append(users[n], logical)
			}
		}
		if waiting[logical] == 0 {
			ready = append(ready, logical)
		}
	}
	if len(undeclared) > 0 {
		return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(undeclared))), ", ") + "] in the Resources block of the template")
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
				named[cmp.Or(str(append(spi.Slice(x), nil)[0]), logical)] = true
			case "Fn::Sub":
				parts := append(spi.Slice(x), x, nil) // "text", or ["text", {variables}]
				for _, m := range subVariable.FindAllStringSubmatch(str(parts[0]), -1) {
					if n, _, _ := strings.Cut(m[1], "."); asMap(parts[1])[m[1]] == nil && !strings.HasPrefix(n, "!") {
						named[n], named["${"+n] = true, true // a variable, which order checks is declared
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
func truth(c any, conds, mappings map[string]any, params map[string]string, memo map[string][2]bool) (bool, bool) {
	m, text := asMap(c), [2]string{}
	if name, ok := m["Condition"]; ok {
		if r, seen := memo[str(name)]; seen { // or being evaluated: a condition that names itself
			return r[0], r[1]
		}
		memo[str(name)] = [2]bool{}
		yes, settled := truth(conds[str(name)], conds, mappings, params, memo)
		memo[str(name)] = [2]bool{yes, settled}
		return yes, settled
	} else if args := spi.Slice(m["Fn::Equals"]); len(args) == 2 {
		for i, x := range args {
			v, ok := coerce(x, "String")
			if r, isRef := asMap(x)["Ref"]; isRef {
				v, ok = params[str(r)]
			} else if f, isMapped := asMap(x)["Fn::FindInMap"]; isMapped {
				v, _, ok = findInMap(f, params, mappings)
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
			yes, ok := truth(x, conds, mappings, params, memo)
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
		if yes, settled := truth(map[string]any{"Condition": name}, asMap(tpl["Conditions"]), asMap(tpl["Mappings"]), params, memo); settled {
			refs["Condition::"+name] = strconv.FormatBool(yes)
		}
	}
	return refs
}

// subVariable is a ${...} in a Fn::Sub string, and subName the name one may hold.
var subVariable, subName = regexp.MustCompile(`\$\{([^}]*)\}`), regexp.MustCompile(`^[A-Za-z0-9_.:]+$`)

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

func fifo(p map[string]any) bool { return isTrue(p["FifoQueue"]) }

// isTrue is whether v is the Boolean true, in any case (coerce).
func isTrue(v any) bool { b, _ := coerce(v, "Boolean"); return b == true }

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
			if p.unmade(str(r), refs) {
				return unknown
			} else if str(r) == "AWS::NotificationARNs" {
				arns := spi.Decode[[]any]([]byte(v))
				return arns
			} else if typ := str(asMap(asMap(p.tpl["Parameters"])[str(r)])["Type"]); typ == "CommaDelimitedList" || strings.HasPrefix(typ, "List<") {
				items := []any{} // "a Ref would result in ["test","dev","prod"]": "each member string is space trimmed"
				for _, item := range strings.Split(v, ",") {
					items = append(items, strings.TrimSpace(item))
				}
				return items
			}
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
		if args, ok := t["Fn::Select"]; ok { // [index, listOfObjects]: "a single object from a list of objects by index"
			args := append(spi.Slice(args), nil, nil)
			index, list := p.resolve(args[0], refs, req, stack), p.resolve(args[1], refs, req, stack)
			if i, isInt := coerce(index, "Integer"); strings.Contains(spi.JSONText([]any{index, list}), unknown) {
				return unknown // of what is not made yet: checked as it is made, when it is known
			} else if isInt && i.(int) >= 0 && i.(int) < len(spi.Slice(list)) {
				return spi.Slice(list)[i.(int)]
			}
			return unselected // "doesn't check ... if the index is out of bounds": the stack's error (selected)
		}
		if args, ok := t["Fn::Split"]; ok { // [delimiter, source string]: "A list of string values"
			args, parts := append(spi.Slice(args), nil, nil), []any{}
			for _, part := range strings.Split(str(p.resolve(args[1], refs, req, stack)), str(args[0])) {
				parts = append(parts, part)
			}
			return parts
		}
		if region, ok := t["Fn::GetAZs"]; ok { // "" is the stack's Region; three zones, a reading
			r := cmp.Or(str(p.resolve(region, refs, req, stack)), req.Identity.Region)
			return []any{r + "a", r + "b", r + "c"}
		}
		if f, ok := t["Fn::FindInMap"]; ok {
			if v, _, found := findInMap(f, refs, asMap(p.tpl["Mappings"])); found {
				return v
			}
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
	if v, ok := refs[name]; ok { // an empty parameter is a value: "" is not its name
		return v, true
	}
	return name, false
}

// getAtt is a Fn::GetAtt's value: the attribute the resource's type has (attribute), or else its Ref's.
func getAtt(g any, refs map[string]string) string {
	logical, attr, _ := strings.Cut(str(g), ".") // "A.B", or [A, B]
	if t, ok := g.([]any); ok {
		t = append(slices.Clip(t), nil, nil)
		logical, attr = str(t[0]), str(t[1])
	}
	if _, made := refs[logical]; !made { // a resource not made yet
		return unknown
	}
	return cmp.Or(refs[logical+"."+attr], refs[logical])
}

// text is a value as a String holds it, an output's OutputValue (the model's String) or a ${Name} in a
// Fn::Sub: a list's members comma-joined, as a CommaDelimitedList is written, anything else its text.
func text(v any) string {
	if list, isList := v.([]any); isList {
		parts := []string{}
		for _, x := range list {
			parts = append(parts, str(x))
		}
		return strings.Join(parts, ",")
	}
	return spi.JSONText(v)
}

// unmade is whether name is a resource of the template read whose id refs does not hold yet: what reads it
// is unknown until it is made (unknown).
func (p *Pack) unmade(name string, refs map[string]string) bool {
	_, resource := asMap(p.tpl["Resources"])[name]
	_, made := refs[name]
	return resource && !made
}

// sound is the error for a template whose values cannot be made, found before anything is reserved or
// made: AWS::NoValue "Removes the corresponding resource property" (User Guide, pseudo parameters), or a list
// element, so it is never a value, and as part of one, a Fn::Join's separator, a Fn::Sub's string or
// variable, an output's value, it is refused.
func (p *Pack) sound(tpl map[string]any, refs map[string]string, req *spi.Request, stack string) error {
	var values []any
	for _, r := range asMap(tpl["Resources"]) { // all its properties, when AWS::NoValue is the whole attribute (a reading)
		if c := str(asMap(r)["Condition"]); c != "" && refs["Condition::"+c] == "false" {
			continue // "CloudFormation creates the resource only when the condition evaluates to true"
		} else if v := p.resolve(asMap(r)["Properties"], refs, req, stack); v != noValue {
			values = append(values, v)
		}
	}
	for _, o := range asMap(tpl["Outputs"]) {
		values = append(values, p.resolve(asMap(o)["Value"], refs, req, stack))
	}
	if err := selected(values); err != nil && stack != "" {
		return err
	} else if strings.Contains(spi.JSONText(values), noValue) {
		return invalid("Template error: AWS::NoValue removes a property or a list element; it cannot be part of a value")
	}
	var err error // without a DefaultValue, "Fn::FindInMap raises an error when a key is not found"
	each([]any{tpl["Resources"], tpl["Outputs"], tpl["Conditions"]}, func(x map[string]any) {
		f, mapped := x["Fn::FindInMap"]
		args, literal := append(spi.Slice(f), nil), 0 // literal counts the keys given as literals
		for _, a := range args {
			_, ok := coerce(a, "String")
			literal += map[bool]int{true: 1}[ok]
		}
		_, key, found := findInMap(f, refs, asMap(tpl["Mappings"]))
		switch _, ok := coerce(args[0], "String"); {
		case !mapped || err != nil || found:
		case len(args) != 4: // "Fn::FindInMap: [ MapName, TopLevelKey, SecondLevelKey ]" (Template Reference); the wordings are readings
			err = invalid("Template error: every Fn::FindInMap object requires three parameters, the map name, map key and the attribute for return value")
		case ok && asMap(tpl["Mappings"])[str(args[0])] == nil:
			err = invalid("Template error: Mapping named '" + str(args[0]) + "' is not present in the 'Mappings' section of template.")
		case stack == "" && literal < 3: // a template read alone checks only what literals name
		case key != "":
			err = invalid("Template error: Unable to get mapping for " + key)
		case stack != "": // a stack is made with every key settled
			err = invalid("Template error: the Fn::FindInMap " + spi.JSONText(f) + " cannot be evaluated")
		}
	})
	return err
}

// selected is the error for a value holding a Fn::Select of no element of its list (the wording is a reading):
// sound finds one in a list it knows whole, and a stack's resource or output, resolved, in any list.
func selected(v any) error {
	if strings.Contains(spi.JSONText(v), unselected) {
		return invalid("Template error: Fn::Select cannot select nonexistent value")
	}
	return nil
}

// findInMap is the value a Fn::FindInMap names, "the value corresponding to keys in a two-level map that's
// declared in the Mappings section", the template's mappings (Template Reference, Fn::FindInMap):
// its three keys literals, or a Ref or another Fn::FindInMap, the functions the page allows there, that refs
// settles. key is the keys, "map::top::second", "" while one is unsettled; found, whether the map holds it.
func findInMap(args any, refs map[string]string, mappings map[string]any) (v any, key string, found bool) {
	var keys []string
	for _, a := range spi.Slice(args) {
		k, ok := coerce(a, "String")
		if r, isRef := asMap(a)["Ref"]; isRef {
			k, ok = refs[str(r)]
		} else if f, isMapped := asMap(a)["Fn::FindInMap"]; isMapped {
			k, _, ok = findInMap(f, refs, mappings)
		} else if asMap(a) != nil {
			ok = false
		}
		if keys = append(keys, str(k)); !ok {
			return nil, "", false
		}
	}
	if len(keys) != 3 { // a DefaultValue needs the AWS::LanguageExtensions transform, which the engine does not run
		return nil, "", false
	}
	v, found = asMap(asMap(mappings[keys[0]])[keys[1]])[keys[2]]
	return v, strings.Join(keys, "::"), found
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
		} else if _, ok := p.ref(name, refs, req, stack); ok || p.unmade(name, refs) { // a list as its text, as Fn::Join's
			return text(p.resolve(map[string]any{"Ref": name}, refs, req, stack))
		}
		return m
	})
}

func parseTemplate(body string) (map[string]any, error) {
	m, err := decode(body)
	if err != nil {
		return nil, err
	} else if len(asMap(m["Resources"])) == 0 { // "The Resources section is required in every CloudFormation template"
		return nil, invalid("Template format error: At least one Resources member must be defined.")
	} else if n := len(asMap(m["Resources"])); n > 500 { // "The maximum number of resources that you can declare ... 500 resources" (quotas)
		return nil, invalid("Template format error: Number of resources, " + strconv.Itoa(n) + ", is greater than maximum allowed, 500")
	}
	if err := mapped(m["Mappings"]); err != nil {
		return nil, err
	}
	conds, users := asMap(m["Conditions"]), map[string][]string{} // conditions "based on ... Other conditions" (User Guide)
	for name, c := range conds {
		each(c, func(x map[string]any) {
			if n, ok := x["Condition"]; ok {
				users[str(n)] = append(users[str(n)], name)
			}
		})
	}
	var named, undeclared []string // "you can't reference resource logical IDs or their attributes in conditions" (User Guide)
	each(conds, func(x map[string]any) {
		target, _, _ := strings.Cut(cmp.Or(str(x["Ref"]), str(append(spi.Slice(x["Fn::GetAtt"]), x["Fn::GetAtt"])[0])), ".")
		if _, resource := asMap(m["Resources"])[target]; resource {
			named = append(named, target)
		}
		if c, ok := x["Condition"].(string); ok && conds[c] == nil {
			undeclared = append(undeclared, c)
		}
	})
	var refused *spi.Fault // the first of these refusals, each a reading (the Template Reference gives no wording)
	each([]any{m["Resources"], m["Outputs"]}, func(x map[string]any) {
		if c := str(append(spi.Slice(x["Fn::If"]), nil)[0]); x["Fn::If"] != nil && conds[c] == nil {
			undeclared = append(undeclared, c)
		}
		for _, v := range subVariable.FindAllStringSubmatch(str(append(spi.Slice(x["Fn::Sub"]), x["Fn::Sub"])[0]), -1) {
			if !strings.HasPrefix(v[1], "!") && !subName.MatchString(v[1]) { // the Template Reference says nothing of a space in ${}
				refused = cmp.Or(refused, invalid("Template error: variable names in Fn::Sub syntax must contain only alphanumeric characters, underscores, periods, and colons"))
			}
		}
		if x["Fn::Split"] != nil && str(append(spi.Slice(x["Fn::Split"]), nil)[0]) == "" { // "For the Fn::Split delimiter, you can't use any functions. You must specify a string value"
			refused = cmp.Or(refused, invalid("Template error: Fn::Split's delimiter must be a string of one or more characters"))
		}
	})
	if refused != nil {
		return nil, refused
	}
	for _, v := range slices.Concat(slices.Collect(maps.Values(asMap(m["Resources"]))), slices.Collect(maps.Values(asMap(m["Outputs"])))) {
		if c, ok := asMap(v)["Condition"].(string); ok && conds[c] == nil { // a resource's or output's Condition attribute
			undeclared = append(undeclared, c)
		}
	}
	if len(named) > 0 {
		return nil, invalid("Template format error: Unresolved dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(named))), ", ") + "]. Cannot reference resources in the Conditions block of the template")
	} else if len(undeclared) > 0 {
		return nil, invalid("Template error: unresolved condition dependency " + strings.Join(slices.Compact(slices.Sorted(slices.Values(undeclared))), ", "))
	}
	if cycle := cycles(conds, users, new(int)); len(cycle) > 0 { // none of them can be evaluated
		return nil, invalid("Template format error: Circular dependency between conditions: [" + strings.Join(cycle, ", ") + "]")
	}
	return m, nil
}

// mapped is the error for a Mappings section that is not one: "Within the mapping, each map is a key followed
// by another mapping", and its values "can be of type String or List" (User Guide, Mappings syntax). Each is
// refused in the service's words, "Every Mappings member <name> must be a map" as a user's report quotes it
// and the rest readings, the first in sorted order.
func mapped(section any) error {
	mappings, isMap := section.(map[string]any)
	if section != nil && !isMap {
		return invalid("Template format error: Every Mappings member must be a map")
	}
	for _, name := range slices.Sorted(maps.Keys(mappings)) {
		if mapping, isMap := mappings[name].(map[string]any); !isMap {
			return invalid("Template format error: Every Mappings member " + name + " must be a map")
		} else if len(mapping) == 0 {
			return invalid("Template format error: Mappings member " + name + " must contain at least one key")
		}
		for _, key := range slices.Sorted(maps.Keys(asMap(mappings[name]))) {
			attrs, isMap := asMap(mappings[name])[key].(map[string]any)
			if !isMap {
				return invalid("Template format error: Every Mappings member " + key + " must be a map")
			}
			for _, v := range attrs {
				if _, scalar := coerce(v, "String"); !scalar && reflect.TypeOf(v) != reflect.TypeOf([]any{}) {
					return invalid("Template format error: Every Mappings attribute must be a String or a List.")
				}
			}
		}
	}
	return nil
}

// referred is the names v refers to (names), sorted.
func referred(v any, refs map[string]string) []string {
	named := map[string]bool{}
	names(v, named, refs)
	return slices.Sorted(maps.Keys(named))
}

// decode is a template body as JSON, or YAML, before any check of what it holds.
func decode(body string) (m map[string]any, err error) {
	if body = strings.TrimSpace(body); body == "" {
		err = invalid("empty TemplateBody")
	} else if body[0] == '{' {
		if jsonErr := json.Unmarshal([]byte(body), &m); jsonErr != nil {
			err = invalid(jsonErr.Error())
		}
	} else if m, err = parseYAML(body); err != nil {
		err = invalid("TemplateBody YAML: " + err.Error())
	}
	return m, err
}

// stored is a stack's stored template as it holds it: the stack was made from it, so it is read, not
// checked again by checks added since.
func stored(body string) map[string]any {
	m, _ := decode(body)
	return m
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

// parameters are the request's parameter values, the Parameters list the query protocol's decoder builds
// (awsquery's unflatten), and each Default of a parameter it does not give; or the error for a parameter
// the template does not declare, or one given both a value and UsePreviousValue: "If you specify true, do
// not specify a parameter value" (API reference, Parameter). The wordings are readings.
func parameters(in, tpl map[string]any) (map[string]string, error) {
	out, undeclared := map[string]string{}, []string{}
	for _, p := range spi.Slice(in["Parameters"]) {
		k, v, valued := str(asMap(p)["ParameterKey"]), asMap(p)["ParameterValue"], asMap(p)["ParameterValue"] != nil
		if str(v) != "" && isTrue(asMap(p)["UsePreviousValue"]) { // "non empty", as the service words it
			return nil, invalid("Invalid input for parameter key " + k + ". Cannot specify usePreviousValue as true and non empty value for a parameter")
		} else if asMap(tpl["Parameters"])[k] == nil {
			undeclared = append(undeclared, k)
		} else if valued {
			out[k] = str(v)
		}
	}
	for k, v := range asMap(tpl["Parameters"]) {
		if _, given := out[k]; !given && asMap(v)["Default"] != nil {
			out[k] = spi.JSONText(asMap(v)["Default"]) // a parameter "becomes a string": Default 16 is "16"
		}
	}
	if len(undeclared) > 0 {
		return nil, invalid("Parameters: [" + strings.Join(undeclared, ", ") + "] do not exist in the template")
	}
	return out, nil
}

// previous puts in an update's request what it takes from the stack cur: its NotificationARNs when the
// request names none, as UpdateStack keeps them ("Specify an empty list to remove all notification topics",
// API reference), and the value of each parameter whose UsePreviousValue is true, "the existing parameter
// value that the stack is using for a given parameter key" (API reference, Parameter).
func previous(in map[string]any, cur stack) {
	for k, kept := range map[string][]any{"NotificationARNs": cur.NotificationARNs, "Tags": cur.Tags} {
		if _, named := in[k]; !named { // "If you don't specify this parameter, CloudFormation doesn't modify the stack's tags"
			in[k] = kept
		}
	}
	for _, p := range spi.Slice(in["Parameters"]) {
		valued := str(asMap(p)["ParameterValue"]) != "" // one given both is refused (parameters)
		if v, ok := cur.Refs[str(asMap(p)["ParameterKey"])]; ok && !valued && isTrue(asMap(p)["UsePreviousValue"]) {
			asMap(p)["ParameterValue"] = v
			delete(asMap(p), "UsePreviousValue") // taken
		}
	}
}

// capable is the error for a template of IAM resources a stack's request does not acknowledge: "If you have IAM
// resources, you can specify either capability. If you have IAM resources with custom names, you must specify
// CAPABILITY_NAMED_IAM. If you don't specify either of these capabilities, CloudFormation returns an
// InsufficientCapabilities error" (API reference, CreateStack). The wording is a reading.
func capable(tpl, in map[string]any, stack string) error {
	need, given := "", spi.Slice(in["Capabilities"])
	for _, r := range asMap(tpl["Resources"]) {
		if typ := str(asMap(r)["Type"]); strings.HasPrefix(typ, "AWS::IAM::") && asMap(asMap(r)["Properties"])[kinds[typ].member] != nil {
			need = "CAPABILITY_NAMED_IAM"
		} else if strings.HasPrefix(typ, "AWS::IAM::") {
			need = cmp.Or(need, "CAPABILITY_IAM")
		}
	}
	if need == "" || stack == "" || slices.Contains(given, any("CAPABILITY_NAMED_IAM")) || slices.Contains(given, any(need)) {
		return nil
	}
	return fault("InsufficientCapabilitiesException", "Requires capabilities : ["+need+"]")
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
