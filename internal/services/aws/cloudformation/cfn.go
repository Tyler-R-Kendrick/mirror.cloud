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
	"sync"
	"sync/atomic"

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
var natives = []string{"CreateStack", "UpdateStack", "DeleteStack", "CreateChangeSet", "ExecuteChangeSet", "DeleteChangeSet", "ValidateTemplate", "GetTemplateSummary", "ListExports", "ContinueUpdateRollback", "UpdateTerminationProtection"}

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
	walks int             // resources whose properties order walked, which a test counts
	tpl   map[string]any  // the template an operation reads: its Mappings, which findInMap reads, and its parameters' types
	skip  map[string]bool // the resources of the template read that the stack does not make (skipped)
	body  string          // a change set's template, as it was read when the change set was made (templateBody)
}

// New constructs the template engine.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

// Invoke serves one native operation.
func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (out *spi.Response, err error) {
	name := str(req.Input["StackName"])
	defer func() { // no panic of the engine's own escapes an operation: the service's InternalFailure
		if recover() != nil {
			out, err = nil, internalFailure()
		}
	}()
	if slices.Contains(natives[:5], req.Operation) || req.Operation == "ContinueUpdateRollback" { // each takes over a stack its last operation left in progress
		_ = recovered(func() error { p.orphan(ctx, req, name); return nil })
	}
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
		return p.upsert(ctx, req, name, req.Operation == "UpdateStack", "", changeSet{})
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
		key := named(name) + ":" + str(req.Input["ChangeSetName"])
		raw, ok, _ := p.col(req, "cfn-cs").Get(ctx, key)
		rec := spi.Decode[map[string]any](raw) // what CreateChangeSet took that CreateStack and UpdateStack take
		if !ok {
			return nil, &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
		} else if err := executable(rec); err != nil && rec["Status"] == "FAILED" { // refused before its template is read again; any other, as it is claimed
			return nil, err
		}
		for _, k := range changeSetMembers {
			if rec[k] != nil {
				req.Input[k] = rec[k]
			}
		}
		p.body = str(rec["TemplateBody"])                                                                                                  // read again by no TemplateBody limit: a TemplateURL's is 1 MB
		_, err := p.upsert(ctx, req, name, rec["ChangeSetType"] != "CREATE", str(rec["StackId"]), changeSet{key, str(rec["ChangeSetId"])}) // claims this copy as it starts
		p.unsettled(ctx, req, changeSet{key, str(rec["ChangeSetId"])}, "AVAILABLE", err)                                                   // a refusal leaves it as it was, and says why
		return &spi.Response{Output: map[string]any{}}, err
	case "DeleteChangeSet": // not one being executed (the wording is a reading)
		key, err := named(name)+":"+str(req.Input["ChangeSetName"]), error(nil)
		if _, given := req.Input["StackName"]; given { // "The name or unique stack ID" (StackNameOrId)
			err = checkStackName(name, true)
		}
		if err == nil {
			err = p.col(req, "cfn-cs").Txn(ctx, func(tx spi.Tx) error {
				if raw, _, _ := tx.Get(key); status(spi.Decode[map[string]any](raw)) == "EXECUTE_IN_PROGRESS" {
					return fault("InvalidChangeSetStatus", "ChangeSet ["+key+"] cannot be deleted in its current execution status of [EXECUTE_IN_PROGRESS]")
				}
				return tx.Delete(key)
			})
		}
		return &spi.Response{Output: map[string]any{}}, err
	default:
		return nil, spi.NotImplemented("aws.cloudformation", req.Operation, "emulate")
	}
}

// changeSetMembers are what CreateChangeSet takes that CreateStack and UpdateStack take, besides the template:
// a change set keeps those given, DescribeChangeSet answers them, and ExecuteChangeSet makes or updates the
// stack with them.
var changeSetMembers = []string{"Parameters", "NotificationARNs", "Tags", "Capabilities"}

// createChangeSet records the change set, and for a new stack the stack itself, in one transaction over what
// it finds: "You can't use the UPDATE type to create a change set for a new stack or the CREATE type to create a
// change set for an existing stack", and "If you create a change set for a new stack, CloudFormation creates a
// stack with a unique stack ID, but no template or resources. The stack will be in the REVIEW_IN_PROGRESS state
// until you execute the change set" (API reference, CreateChangeSet). The request's own errors are refused; the
// template's are the change set's, FAILED: "if your attempt to create a change set failed, CloudFormation shows
// the error message" (StatusReason). A UsePreviousValue is read now: its Parameters are "the input parameters
// and their values used to create the change set" (DescribeChangeSet).
func (p *Pack) createChangeSet(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	csn, create := str(req.Input["ChangeSetName"]), req.Input["ChangeSetType"] == "CREATE"
	body, err := p.templateBody(ctx, req)                          // TemplateBody, within the member's size, or the TemplateURL's object
	p.body, err = body, cmp.Or(checkStackName(name, !create), err) // read once: template reads it again from p.body
	tpl, parseErr := parseTemplate(body)                           // one that does not parse names no parameters to check
	if err != nil {
		return nil, err
	} else if _, err := parameters(req.Input, tpl); err != nil && parseErr == nil {
		return nil, err
	} else if err := capable(tpl, req.Input, name); err != nil && parseErr == nil {
		return nil, err
	}
	prefix := "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account
	rec := map[string]any{"ChangeSetName": csn, "ChangeSetId": prefix + ":changeSet/" + csn + "/" + p.deps.Rand.Hex(8), "StackName": name, "TemplateBody": body, // a TemplateURL's too
		"ChangeSetType": cmp.Or(str(req.Input["ChangeSetType"]), "UPDATE"), "Status": "CREATE_COMPLETE", "ExecutionStatus": "AVAILABLE",
		"StackId": prefix + ":stack/" + named(name) + "/" + p.deps.Rand.Hex(8)} // a CREATE change set's stack is made under the id minted here
	for _, k := range changeSetMembers {
		if v, given := req.Input[k]; given { // one not given is not kept, Tags and NotificationARNs too: its execution keeps the stack's (previous)
			rec[k] = v
		}
	}
	err = p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		raw, ok, _ := stx.Collection("cfn").Get(named(name))
		st, loadErr := found(name, raw, ok)
		review := create && st.Status == "REVIEW_IN_PROGRESS"                            // a CREATE change set's stack, which takes more of them
		if _, taken, _ := stx.Collection("cfn-cs").Get(named(name) + ":" + csn); taken { // the wordings are readings
			return fault("AlreadyExistsException", "ChangeSet ["+csn+"] already exists")
		} else if loadErr != nil && !create {
			return loadErr
		} else if st.ID != "" && create && !review {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists and cannot be created again with the changeSet ["+csn+"].")
		} else if st.ID != "" && !updatable(st) && !review { // as UpdateStack refuses one
			return invalid("Stack:" + st.ID + " is in " + st.Status + " state and can not be updated.")
		} else if st.ID != "" {
			previous(req.Input, st)
			rec["StackId"] = st.ID
		} else if err := stx.Collection("cfn").Put(named(name), []byte(spi.JSONText(stack{Name: named(name), ID: str(rec["StackId"]), Status: "REVIEW_IN_PROGRESS",
			Resources: []res{}, Outputs: []any{}, NotificationARNs: []any{}, Tags: []any{}}))); err != nil {
			return err
		}
		if _, _, _, _, err := p.template(ctx, req, named(name), str(rec["StackId"])); err != nil { // every check its execution makes, the store untouched
			rec["Status"], rec["StatusReason"], rec["ExecutionStatus"] = "FAILED", err.Error(), "UNAVAILABLE"
		}
		return stx.Collection("cfn-cs").Put(named(name)+":"+csn, []byte(spi.JSONText(rec)))
	})
	return &spi.Response{Output: map[string]any{"Id": rec["ChangeSetId"], "StackId": rec["StackId"]}}, err
}

// status is a change set's ExecutionStatus; AVAILABLE for a record stored before it had one.
func status(rec map[string]any) string { return cmp.Or(str(rec["ExecutionStatus"]), "AVAILABLE") }

// unsettled records why an execution of the change set under key did not finish, while the change set is
// still in status was: one a refusal left AVAILABLE stays so, one claimed is EXECUTE_FAILED, each with the
// error as its StatusReason.
func (p *Pack) unsettled(ctx context.Context, req *spi.Request, cs changeSet, was string, cause error) {
	_ = p.col(req, "cfn-cs").Txn(ctx, func(tx spi.Tx) error {
		raw, _, _ := tx.Get(cs.key)
		if rec := spi.Decode[map[string]any](raw); cs.mine(rec) && cause != nil && status(rec) == was { // not one made again under its name
			if rec["StatusReason"] = cause.Error(); was != "AVAILABLE" {
				rec["ExecutionStatus"] = "EXECUTE_FAILED"
			}
			return tx.Put(cs.key, []byte(spi.JSONText(rec)))
		}
		return nil
	})
}

// retire is what a stack's write that ends an operation does to its change sets, in the same transaction:
// the change set under key being executed (its claim, and take's check of the stack, hold it) is
// EXECUTE_COMPLETE, and "CloudFormation deletes all other change sets associated with the stack"
// (ExecuteChangeSet), every one for a key no change set has; an update of no change set (key "") leaves
// each available one OBSOLETE, "because the stack was already updated" (DescribeChangeSet).
func retire(name string, cs changeSet) func(spi.Tx) error {
	return func(tx spi.Tx) error {
		kvs, _, _ := tx.List(name+":", "", 0)
		for _, kv := range kvs {
			rec, err := spi.Decode[map[string]any](kv.Value), error(nil)
			if own := kv.Key == cs.key; own && !cs.mine(rec) {
				continue // made again under its name: defence in depth, as an execution holds its stack and no path reaches it
			} else if own || cs.key == "" && status(rec) == "AVAILABLE" {
				if rec["ExecutionStatus"] = map[bool]string{true: "EXECUTE_COMPLETE", false: "OBSOLETE"}[own]; own {
					delete(rec, "StatusReason") // a refusal's, before it was executed
				}
				err = tx.Put(kv.Key, []byte(spi.JSONText(rec)))
			} else if cs.key != "" {
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
	Events                     []res `json:",omitempty"` // what left the stack and how: DELETE_SKIPPED
	Outputs                    []any
	Refs                       map[string]string
	NotificationARNs, Tags     []any
	Protect                    bool
	Op                         string `json:",omitempty"` // while *_IN_PROGRESS: "/" and its operation's id (ops)
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
func (p *Pack) upsert(ctx context.Context, req *spi.Request, name string, update bool, id string, cs changeSet) (out *spi.Response, err error) {
	if err := checkStackName(name, update); err != nil {
		return nil, err
	}
	if arns := spi.Slice(req.Input["NotificationARNs"]); len(arns) > 5 { // a member's constraint, before whether the stack exists
		return nil, invalid("1 validation error detected: Value at 'notificationARNs' failed to satisfy constraint: Member must have length less than or equal to 5")
	}
	given, name := name, named(name)
	id = cmp.Or(id, "arn:aws:cloudformation:"+req.Identity.Region+":"+req.Identity.Account+":stack/"+name+"/"+p.deps.Rand.Hex(8))
	input, loaded := spi.Decode[map[string]any]([]byte(spi.JSONText(req.Input))), stack{}
	rereads, _ := ctx.Value(staleKey{}).(int)
	if cur, err := p.load(ctx, req, given); err == nil && update { // the id an update's AWS::StackId answers
		id, loaded = cur.ID, cur
		previous(req.Input, cur)
	} else if update { // "Stack [x] does not exist" before any template error; read again where it is written
		return nil, err
	}
	arns, tags := append([]any{}, spi.Slice(req.Input["NotificationARNs"])...), append([]any{}, spi.Slice(req.Input["Tags"])...)
	body, tpl, order, refs, err := p.template(ctx, req, name, id)
	if err != nil {
		return nil, err
	}
	resources, outs := asMap(tpl["Resources"]), asMap(tpl["Outputs"])
	// old is what an update finds, and held the record it reserves it with, UPDATE_IN_PROGRESS; a create
	// reserves the name, CREATE_IN_PROGRESS, in the same transaction.
	var old, held stack
	var leave *stack // what a failing operation leaves, until it has settled it: the next operation's to settle (orphan)
	op, release := p.hold()
	reserved := stack{Name: name, ID: id, Status: "CREATE_IN_PROGRESS", Template: body, Resources: []res{}, Outputs: []any{}, NotificationARNs: arns, Tags: tags, Op: op}
	if err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		tx := stx.Collection("cfn")
		raw, found, _ := tx.Get(name)
		if err := claim(stx.Collection("cfn-cs"), cs); err != nil { // a refusal below puts it back: nothing is written
			return err
		} else if _ = json.Unmarshal(raw, &old); old.Status == "DELETE_COMPLETE" || found && !update && old.Status == "REVIEW_IN_PROGRESS" && old.ID == id {
			found, old = false, stack{} // a deleted stack's name is free; a CREATE change set's stack is made by its execution
		} else if update && found && !reflect.DeepEqual(old, loaded) {
			return errStale // changed since its previous values were read: read again
		}
		if found && !update {
			return fault("AlreadyExistsException", "Stack ["+name+"] already exists")
		} else if update && (!found || given != name && old.ID != given) { // an update of a stack that does not exist makes none
			return invalid("Stack [" + given + "] does not exist")
		} else if found && !updatable(old) {
			return invalid("Stack:" + old.ID + " is in " + old.Status + " state and can not be updated.")
		} else if id, held = cmp.Or(old.ID, id), old; update { // "is in UPDATE_IN_PROGRESS state" to another change
			held.Status, held.Op = "UPDATE_IN_PROGRESS", op
			return tx.Put(name, []byte(spi.JSONText(held)))
		}
		return tx.Put(name, []byte(spi.JSONText(reserved)))
	}); errors.Is(err, errStale) && rereads < 3 {
		req.Input = input
		return p.upsert(context.WithValue(ctx, staleKey{}, rereads+1), req, given, update, "", cs)
	} else if errors.Is(err, errStale) { // still changing: a caller it will not serve now, in the service's words
		return nil, fault("Throttling", "Rate exceeded")
	} else if err != nil {
		return nil, err
	}
	read := held // the record as the update reserved it, which its writes go over (a rollback changes old's resources)
	read.Resources = slices.Clone(old.Resources)
	if update { // settled again: a record before round 9 holds no conditions
		old.Refs = conditioned(stored(old.Template), old.Refs, req, name, old.ID)
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: refs,
		Resources: []res{}, Outputs: []any{}, NotificationARNs: arns, Tags: tags} // never null: every read of a stack lists them
	changed := map[string]bool{} // what the update changed in place, which a rollback changes back
	defer func() {
		if leave == &old { // an update: what it made, which its rollback did not settle, is listed for ContinueUpdateRollback
			for _, r := range st.Resources {
				if !slices.ContainsFunc(old.Resources, func(o res) bool { return o.Physical == r.Physical }) {
					r.Status, r.Reason = "DELETE_FAILED", old.Reason
					old.Resources = append(old.Resources, r)
				}
			}
		}
		st.Status, old.Status = "ROLLBACK_FAILED", "UPDATE_ROLLBACK_FAILED" // where a rollback that did not settle leaves it
		release(leave)
	}()
	fail := func(cause error) (*spi.Response, error) {
		st.Reason, old.Reason = cause.Error(), cause.Error()
		if leave = &st; !update || old.ID == "" { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, reserved, st, nil, st.Resources, nil, "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", retains(tpl, "DeletionPolicy", "Retain"))
		} else {
			leave = &old
			_ = p.rollback(ctx, req, read, old, st.Resources, changed, retains(tpl, "DeletionPolicy", "Retain"))
		}
		if raw, _, _ := p.col(req, "cfn").Get(ctx, name); spi.Decode[stack](raw).Op != op { // settled: nothing is left
			leave = nil
		}
		p.unsettled(ctx, req, cs, "EXECUTE_IN_PROGRESS", cause)
		return nil, cause
	}
	defer func() { // a panic of the engine's own, after the reservation, fails the operation as any failure does;
		// one in that rollback leaves what remains, with why, for the next operation to settle
		if recover() != nil {
			out, err = nil, internalFailure()
			if st.Reason, old.Reason = err.Error(), err.Error(); leave == nil { // not one in the rollback itself
				_ = recovered(func() error { _, _ = fail(err); return nil })
			}
		}
	}()
	planned := maps.Clone(st.Refs)
	for _, logical := range order { // a name taken fails the stack before anything is made: a Ref answers the id planned
		rm := asMap(resources[logical])
		typ, props := str(rm["Type"]), asMap(p.resolve(rm["Properties"], planned, req, name))
		prev, how, n, err := p.next(ctx, req, st, old, logical, typ, props)
		if err != nil {
			return fail(err)
		} else if id := cmp.Or(map[bool]string{true: prev.Physical}[how == "" || how == "update"], planID(req, typ, n, props)); id != "" { // else its Ref is unknown (unmade)
			planned[logical] = id
			attribute(planned, req, typ, logical, id)
		}
	}
	for _, logical := range order {
		rm := asMap(resources[logical]) // props is never nil: provision adds to it
		typ, props := str(rm["Type"]), maps.Collect(maps.All(asMap(p.resolve(rm["Properties"], st.Refs, req, name))))
		prev, how, n, err := p.next(ctx, req, st, old, logical, typ, props) // again: a name may come from a resource made before it
		phys := map[string]string{"": prev.Physical}[how]                   // one an update keeps as it is
		if err = cmp.Or(err, encode(props)); how != "" && err == nil {
			phys, err = p.provision(ctx, req, typ, logical, n, props, map[string]string{"update": prev.Physical}[how], id)
		}
		changed[logical] = how == "update"
		if phys != "" { // recorded once it exists, before anything is set on it, so a rollback finds a half-made one too
			st.Refs[logical] = phys
			attribute(st.Refs, req, typ, logical, phys)
			st.Resources = append(st.Resources, res{Logical: logical, Physical: phys, Type: typ, Name: n})
		}
		if how != "" && how != "update" && err == nil && typ == "AWS::S3::Bucket" {
			err = p.configureBucket(ctx, req, phys, props)
		}
		if err != nil {
			return fail(err)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(outs)) {
		v := p.resolve(asMap(outs[k])["Value"], st.Refs, req, name)
		if err := encode(v); err != nil {
			return fail(err)
		}
		st.Outputs = append(st.Outputs, map[string]any{"OutputKey": k, "OutputValue": text(v)})
	}
	if cur, wrote, err := p.settle(ctx, req, map[bool]stack{false: reserved, true: read}[update], st, retire(name, cs)); err != nil {
		return fail(err) // the final write failed: an error, never success
	} else if !wrote && !update {
		// A DeleteStack begun during the create ends here: what it made goes, and then the stack.
		if cur.ID != id || cur.Status != "DELETE_IN_PROGRESS" {
			cur = stack{} // the stack is another's now: nothing is recorded
		}
		_ = p.unwind(ctx, req, cur, st, nil, st.Resources, nil, "DELETE_COMPLETE", "DELETE_FAILED", retains(tpl, "DeletionPolicy", "Retain"))
		return &spi.Response{Output: map[string]any{"StackId": id}}, nil
	} else if !wrote {
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
		err := invalid("Stack:" + id + " is in " + rival + " state and can not be updated.")
		p.unsettled(ctx, req, cs, "EXECUTE_IN_PROGRESS", err)
		return nil, err
	}
	// The update is done, so its cleanup deletes the old resources the stack no longer holds, replaced
	// ("creating a new one and deleting the old one") or dropped; one it cannot delete stays listed. A
	// replaced one (the stack still holds its logical id) the template's UpdateReplacePolicy retains; a
	// dropped one, or one its condition now leaves out, the old DeletionPolicy.
	stale := slices.DeleteFunc(slices.Clone(old.Resources), func(r res) bool {
		return slices.ContainsFunc(st.Resources, func(n res) bool { return n.Physical == r.Physical })
	})
	oldTpl := stored(old.Template)
	_ = p.unwind(ctx, req, st, st, st.Resources, stale, nil, st.Status, st.Status, func(r res) bool {
		if replaced := slices.ContainsFunc(st.Resources, func(n res) bool { return n.Logical == r.Logical }); replaced { // still the stack's
			return retains(tpl, "UpdateReplacePolicy", "Retain")(r)
		}
		return retains(oldTpl, "DeletionPolicy", "Retain", "RetainExceptOnCreate")(r)
	})
	return &spi.Response{Output: map[string]any{"StackId": id}}, nil
}

// errStale is an update's reservation finding the record changed since the update read it, which it reads
// again, as many times as staleKey counts in its context.
var errStale = errors.New("stale")

type staleKey struct{}

// changeSet is the change set an execution executes: its key, and the id of the copy it read.
type changeSet struct{ key, id string }

// mine is whether rec, a change set's record, is the copy cs read: none of a deleted one, or of one made again.
func (cs changeSet) mine(rec map[string]any) bool {
	return rec != nil && str(rec["ChangeSetId"]) == cs.id
}

// claim moves the change set cs (none for a zero cs), still the copy read, from AVAILABLE to
// EXECUTE_IN_PROGRESS: a change set
// executes once, and one executed, failed, obsolete or being executed is InvalidChangeSetStatus, "The
// specified change set can't be used to update the stack" (the wording is a reading).
func claim(tx spi.Tx, cs changeSet) error {
	raw, found, _ := tx.Get(cs.key)
	if rec := spi.Decode[map[string]any](raw); cs.key == "" {
		return nil
	} else if !found || !cs.mine(rec) { // deleted, or made again, since it was read
		return &spi.Fault{Code: "ChangeSetNotFound", HTTPStatus: 404, Fault: "client"}
	} else if err := executable(rec); err != nil {
		return err
	} else {
		rec["ExecutionStatus"] = "EXECUTE_IN_PROGRESS"
		return tx.Put(cs.key, []byte(spi.JSONText(rec)))
	}
}

// executable is the error for a change set not AVAILABLE, which claim refuses.
func executable(rec map[string]any) error {
	if s := status(rec); s != "AVAILABLE" {
		return fault("InvalidChangeSetStatus", "ChangeSet ["+str(rec["ChangeSetId"])+"] cannot be executed in its current execution status of ["+s+"]")
	}
	return nil
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
		p.tpl, refs = tpl, conditioned(tpl, known, req, stack, id)
		order, err = p.order(tpl, refs)
		p.skipped(order, refs)
	}
	if err == nil {
		err = cmp.Or(p.sound(tpl, refs, req, stack), capable(tpl, req.Input, stack))
	}
	if err == nil && stack != "" { // a stack, or a change set to one, is made from it
		order, err = made(tpl, order, refs)
	}
	return body, tpl, order, refs, err
}

// made is the resources of order a stack makes from tpl, and the error for a template no stack can be made
// from: a parameter or condition without a value, or an output naming a resource the stack does not make.
// The outputs tpl answers are left in it.
func made(tpl map[string]any, order []string, refs map[string]string) ([]string, error) {
	missing := func(section, prefix string) []string { // what a section declares that refs holds no value of
		return slices.DeleteFunc(slices.Sorted(maps.Keys(asMap(tpl[section]))), func(k string) bool { _, ok := refs[prefix+k]; return ok })
	}
	if unvalued := missing("Parameters", ""); len(unvalued) > 0 { // for parameters without default values (User Guide)
		return nil, invalid("Parameters: [" + strings.Join(unvalued, ", ") + "] must have values")
	} else if unsettled := missing("Conditions", "Condition::"); len(unsettled) > 0 { // a stack is made only with every condition settled
		return nil, invalid("Template error: the condition " + unsettled[0] + " cannot be evaluated") // a function the engine does not evaluate
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
	return slices.DeleteFunc(order, func(l string) bool { return skip[l] }), nil
}

// conditioned is refs, what a Ref reads, with the pseudo parameters of the stack named stack, of id id, but
// AWS::NoValue, tpl's Mappings (findInMap) and the conditions of tpl that they settle.
func conditioned(tpl map[string]any, refs map[string]string, req *spi.Request, stack, id string) map[string]string {
	known := maps.Clone(refs)
	maps.Copy(known, pseudo(req, stack, id))
	known = conditions(tpl, known)
	delete(known, "AWS::NoValue") // ref answers it: no record holds the sentinel
	return known
}

// pseudo are the pseudo parameters (User Guide, pseudo parameters) of the stack named stack, of id id, each
// as a Ref answers it: the stack's two only when it has them.
func pseudo(req *spi.Request, stack, id string) map[string]string {
	known := map[string]string{"AWS::AccountId": req.Identity.Account, "AWS::Region": req.Identity.Region, "AWS::StackName": stack,
		"AWS::StackId": id, "AWS::Partition": "aws", "AWS::URLSuffix": "amazonaws.com",
		"AWS::NotificationARNs": spi.JSONText(append([]any{}, spi.Slice(req.Input["NotificationARNs"])...))} // a list, as JSON: [] for none
	maps.DeleteFunc(known, func(_, v string) bool { return v == "" })
	return known
}

// templateBody is the request's template: TemplateBody, "a maximum length of 51,200 bytes", or the S3
// object TemplateURL names, "max size: 1 MB" (API reference, CreateStack), which the quotas page, writing
// 204,800 bytes "204.8 KB", counts as 1,000,000 bytes.
func (p *Pack) templateBody(ctx context.Context, req *spi.Request) (string, error) {
	if len(spi.Slice(req.Input["NotificationARNs"])) > 5 { // its other member: "Maximum number of 5 items" (CreateStack, UpdateStack, CreateChangeSet)
		return "", invalid("1 validation error detected: Value at 'notificationARNs' failed to satisfy constraint: Member must have length less than or equal to 5")
	} else if p.body != "" { // checked against its own source's limit when its change set was made
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

func (p *Pack) drop(ctx context.Context, req *spi.Request, name string) (*spi.Response, error) {
	st, err := p.load(ctx, req, name)
	if nameErr := checkStackName(name, true); nameErr != nil || err != nil {
		return nil, cmp.Or(nameErr, err)
	} else if st.Protect {
		err = invalid("stack has termination protection")
	} else if strings.HasPrefix(st.Status, "UPDATE_") && strings.HasSuffix(st.Status, "_IN_PROGRESS") { // the wording is the service's
		err = invalid("Stack [" + name + "] cannot be deleted while in status " + st.Status)
	} else if was, del := st.Status, st; was != "DELETE_IN_PROGRESS" { // one being deleted is left to that delete
		// "Once the call completes successfully, stack deletion starts" (API reference): the record is
		// DELETE_IN_PROGRESS, written only over the record read. One being created is left to its create,
		// which deletes what it made, and then the stack.
		op, release := p.hold() // the create's, when it ends a create: that create ends this delete
		defer release(nil)
		del.Status, del.Op = "DELETE_IN_PROGRESS", cmp.Or(st.Op, op)
		tpl := stored(st.Template)
		if _, started, settleErr := p.settle(ctx, req, st, del); settleErr != nil {
			return nil, settleErr
		} else if !started { // it changed since it was read
			return p.drop(ctx, req, name)
		} else if was != "CREATE_IN_PROGRESS" {
			err = p.unwind(ctx, req, del, del, nil, st.Resources, nil, "DELETE_COMPLETE", "DELETE_FAILED", retains(tpl, "DeletionPolicy", "Retain", "RetainExceptOnCreate"))
		}
	}
	return &spi.Response{Output: map[string]any{}}, err
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
	st.Status, st.Op = done, "" // settled: no operation holds it
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
// whenever it runs), and with it sets, which write the change sets; a DELETE_COMPLETE stack's go (retire,
// keyed by no change set). settle answers the record it found and whether it wrote.
func (p *Pack) settle(ctx context.Context, req *spi.Request, seen, st stack, sets ...func(spi.Tx) error) (stack, bool, error) {
	var cur stack
	wrote := false
	err := p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(stx spi.ScopeTx) error {
		tx, cs := stx.Collection("cfn"), stx.Collection("cfn-cs")
		raw, _, _ := tx.Get(st.Name)
		if cur = spi.Decode[stack](raw); st.Status == "DELETE_COMPLETE" { // its change sets go with it
			sets = append(sets, retire(st.Name, changeSet{key: st.Name}))
		}
		if st.Protect, seen.Protect = cur.Protect, cur.Protect; spi.JSONText(cur) != spi.JSONText(seen) {
			return nil
		}
		for _, write := range sets {
			if err := write(cs); err != nil {
				return err
			}
		}
		wrote = true
		return tx.Put(st.Name, []byte(spi.JSONText(st)))
	})
	return cur, wrote, err
}

// ops are the operations running in this process, by the id each writes on the record it holds (Op). An
// *_IN_PROGRESS record whose operation is not among them was left by one that ended without settling it,
// or by a process that ended: no operation will settle it, so the next one on the stack does (orphan).
var ops sync.Map
var opSeq atomic.Int64 // numbers them

// hold registers a running operation, and answers its id, after a "/", and its release, which the
// operation defers: given the record a failed operation could not settle, the release keeps it for the
// operation that takes the stack over.
func (p *Pack) hold() (string, func(*stack)) {
	id := p.deps.Rand.Derive("cloudformation-op").Hex(8) + "-" + strconv.FormatInt(opSeq.Add(1), 10)
	ops.Store(id, nil)
	return "/" + id, func(left *stack) {
		if ops.Delete(id); left != nil {
			ops.Store(id, *left)
		}
	}
}

// orphan settles a record left *_IN_PROGRESS by an operation that is not running as that operation's
// failure: a create ROLLBACK_FAILED, a delete DELETE_FAILED, an update UPDATE_ROLLBACK_FAILED, each with
// why. The operation that meets it goes on from that state.
func (p *Pack) orphan(ctx context.Context, req *spi.Request, name string) {
	var cur stack // read as settle reads it: in a transaction
	_ = p.col(req, "cfn").Txn(ctx, func(tx spi.Tx) error {
		raw, _, err := tx.Get(named(name))
		cur = spi.Decode[stack](raw)
		return err
	})
	id := strings.TrimPrefix(cur.Op, "/")
	v, known := ops.Load(id)
	next, kept := v.(stack) // what its operation, ended in this process, could not write
	if !strings.HasSuffix(cur.Status, "_IN_PROGRESS") || cur.Status == "REVIEW_IN_PROGRESS" || known && !kept {
		return
	} else if kept {
		ops.Delete(id)
	} else {
		next = cur
		next.Status = cmp.Or(map[string]string{"CREATE_IN_PROGRESS": "ROLLBACK_FAILED"}[cur.Status], "UPDATE_ROLLBACK_FAILED")
		next.Reason = "The operation on the stack ended before it settled the stack." // the wording is a reading
	}
	if next.Op = ""; cur.Status == "DELETE_IN_PROGRESS" {
		next.Status = "DELETE_FAILED"
	}
	_, _, _ = p.settle(ctx, req, cur, next)
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

// mark is what resolve answers that is no value a template holds: its own type, so no string a template
// gives, whatever characters it has, is one (holds). resolve has three outcomes, a value, unselected (a
// Fn::Select of no element of a list it knows) or unknown: what a Ref to or a Fn::GetAtt of a resource not
// made yet answers, and a string built on one. noValue is what a Ref to AWS::NoValue answers, which resolve
// drops from a map or a list; inValue a string built on it, which sound refuses, as it does unselected.
type mark string

const unknown, unselected, noValue, inValue mark = "unknown", "Fn::Select", "AWS::NoValue", "AWS::NoValue in a value"

// holds is whether v is the mark m or holds it.
func holds(v any, m mark) bool {
	switch t := v.(type) {
	case map[string]any:
		return slices.ContainsFunc(slices.Collect(maps.Values(t)), func(x any) bool { return holds(x, m) })
	case []any:
		return slices.ContainsFunc(t, func(x any) bool { return holds(x, m) })
	}
	return v == m
}

// combine is the mark a value built on vs is, and whether it is one: an error (AWS::NoValue in it, inValue,
// or unselected) before unknown, and unknown before a value. Every intrinsic that builds a value from
// others answers this when it is one.
func combine(vs ...any) (any, bool) {
	for _, m := range []mark{inValue, noValue, unselected, unknown} {
		if holds(vs, m) {
			return map[bool]mark{true: inValue, false: m}[m == noValue], true
		}
	}
	return nil, false
}

// refused is the error for a value holding an error mark: a Fn::Select of no element (the wording is a
// reading), or AWS::NoValue as part of a value. sound refuses them before anything is made, encode as it is.
func refused(v any) error {
	if holds(v, inValue) || holds(v, noValue) {
		return invalid("Template error: AWS::NoValue removes a property or a list element; it cannot be part of a value")
	} else if holds(v, unselected) {
		return invalid("Template error: Fn::Select cannot select nonexistent value")
	}
	return nil
}

// encode is the error for a resolved value going to a provider, into the stack record or into an answer
// that holds any mark: an error mark (refused), or an id not yet known (the wording is a reading).
func encode(v any) error {
	if err := refused(v); err != nil || !holds(v, unknown) {
		return err
	}
	return invalid("Template error: a value is not known when it is used")
}

// planID is the id a resource named n is made with, which a Ref to it answers: n in the form its
// service gives it back. A queue's URL (its endpoint's business), a KMS key's id and a REST API's are
// known only once made, as is an id built on one: unknown. A Kinesis resource policy's is its ARN.
func planID(req *spi.Request, typ, n string, props map[string]any) string {
	if bus := spi.LastSlash(str(props["EventBusName"])); typ == "AWS::Events::Rule" && bus != "" && bus != "default" {
		n = bus + "|" + n // a rule's "event rule ID": <bus>|<name> on any bus but the default (a reading)
	}
	if f := kinds[typ].ref; f != "" && n != "" { // no name: one built on an id known only once made (claim)
		return strings.NewReplacer("{r}", req.Identity.Region, "{a}", req.Identity.Account, "{n}", n).Replace(f)
	}
	return cmp.Or(str(props["ResourceArn"]), str(props["ResourceARN"])) // none: known only once made
}

// order is the order a template's resources are made in: each after those its properties name, by Ref,
// Fn::GetAtt or Fn::Sub ("Resource B is created before resource A", Template Reference, DependsOn
// attribute), and those its DependsOn names, else by logical id. One walk of each resource's properties finds the names, and Kahn's
// algorithm orders them, the earliest logical id first of those ready. A cycle orders none, and is
// CloudFormation's error. A Fn::If whose condition refs settles names only what its branch taken does.
func (p *Pack) order(tpl map[string]any, refs map[string]string) ([]string, error) {
	resources, waiting, users := asMap(tpl["Resources"]), map[string]int{}, map[string][]string{}
	var ready, order, undeclared []string
	// A Ref, a Fn::GetAtt, a Fn::Sub variable or a DependsOn names a parameter, a pseudo parameter or a
	// resource the template declares ("can be template parameter names, resource logical IDs, resource
	// attributes"; a Fn::GetAtt's and a DependsOn's, a resource), or it is "Unresolved": refused before
	// anything is made, in any block and either branch of a Fn::If, as the service refuses it.
	unresolved := func(n string) string {
		v, _ := strings.CutPrefix(n, "${")
		if l, attr := strings.CutPrefix(v, "."); l != "" && resources[l] == nil && (attr || asMap(tpl["Parameters"])[l] == nil && !strings.HasPrefix(l, "AWS::")) {
			return v
		}
		return ""
	}
	for logical, r := range resources {
		p.walks++
		named := map[string]bool{}
		names(asMap(r)["Properties"], named, refs) // the order prunes what a settled Fn::If does not take
		deps := asMap(r)["DependsOn"]              // "a string or a list of strings" (Template Reference, DependsOn attribute), as parseTemplate checks
		if d, ok := deps.(string); ok {
			deps = []any{d}
		}
		for _, d := range spi.Slice(deps) { // "Resource B is created before resource A"; a resource the stack makes, nothing else,
			// unless this one is left out too; a template read alone settles no condition here, as ValidateTemplate evaluates none
			if d := str(d); resources[d] != nil && (off(r, refs) || !off(resources[d], refs) || refs["AWS::StackName"] == "") {
				named[d] = true
			} else {
				undeclared = append(undeclared, d)
			}
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
	block := "Resources"
	for _, b := range []string{"Resources", "Outputs", "Conditions"} {
		if all := map[string]bool{}; len(undeclared) == 0 {
			names(tpl[b], all, nil) // every branch of a Fn::If
			for n := range all {
				if v := unresolved(n); v != "" {
					block, undeclared = b, append(undeclared, v)
				}
			}
		}
	}
	if i := slices.IndexFunc(undeclared, func(u string) bool { return strings.HasPrefix(u, ".") && asMap(tpl["Parameters"])[u[1:]] != nil }); i >= 0 {
		return nil, invalid("Template error: instance of Fn::GetAtt references undefined resource " + undeclared[i][1:])
	}
	for i, u := range undeclared {
		undeclared[i] = strings.TrimPrefix(u, ".")
	}
	if len(undeclared) > 0 {
		return nil, invalid("Template format error: Unresolved resource dependencies [" + strings.Join(slices.Compact(slices.Sorted(slices.Values(undeclared))), ", ") + "] in the " + block + " block of the template")
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

// change is what an update does to the resource of logical the stack holds: "" keeps it as it is,
// "update" updates it in place, and "replace" makes a new one, when a property whose change "requires
// replacement" (the type's Template Reference page) changed. A resource the stack does not hold is a "create";
// it holds every one it lists, whatever its status (a skipped one is UPDATE_COMPLETE), but one DELETE_FAILED.
func (p *Pack) change(req *spi.Request, old stack, logical, typ string, props map[string]any) (res, string) {
	i := slices.IndexFunc(old.Resources, func(r res) bool { return r.Logical == logical && r.Type == typ && r.Status != "DELETE_FAILED" })
	if i < 0 {
		return res{}, "create"
	}
	tpl := stored(old.Template)
	before, how := p.props(tpl, logical, old.Refs, req, old.Name), ""
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
	if arn := planID(req, typ, "", props); typ == "AWS::Kinesis::ResourcePolicy" && p.holder(ctx, req, st.Name, res{Type: typ, Physical: arn}) != "" { // another stack's
		return "", fault("AlreadyExistsException", "Resource of type '"+typ+"' with identifier '"+arn+"' already exists.") // the wording is a reading
	} else if v, set := props["FifoQueue"]; typ == "AWS::SQS::Queue" && set && !isBoolean(v) { // FifoQueue is a Boolean (Template Reference)
		return "", invalid("Properties validation failed for resource " + logical + ": FifoQueue must be a Boolean, not " + spi.JSONText(v))
	}
	if holds(props[k.member], unknown) { // one built on an id known only once made is checked as it is made:
		return "", nil // its id is not planned (planID), and a Ref to it is unknown
	} else if n == "" || k.find == "" {
		return cmp.Or(n, physicalName(st.Name, logical, st.ID, typ, props)), nil
	}
	out, err := p.do(ctx, req, k.find, n, "", props)
	if err != nil && !gone(err) { // only the owner's not-found says the name is free
		return "", err
	}
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
	} else if err == nil && typ == "AWS::S3::Bucket" && existing != "" { // one it makes, upsert configures once it is recorded
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
	tpl := stored(old.Template)
	var doomed []res
	var first error
	for _, r := range made {
		if old.Refs[r.Logical] != r.Physical {
			doomed = append(doomed, r)
		} else if props := maps.Collect(maps.All(p.props(tpl, r.Logical, old.Refs, req, old.Name))); changed[r.Logical] {
			if _, err := p.provision(ctx, req, r.Type, r.Logical, r.Name, props, r.Physical, old.ID); err != nil { // UPDATE_FAILED, for ContinueUpdateRollback
				i := slices.IndexFunc(old.Resources, func(o res) bool { return o.Logical == r.Logical })
				old.Resources[i].Status, old.Resources[i].Reason = "UPDATE_FAILED", err.Error() // the stack's reason in the service's words
				first = cmp.Or(first, error(invalid("The following resource(s) failed to update: ["+r.Logical+"].")))
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
	skip := spi.Slice(req.Input["ResourcesToSkip"]) // the list the query decoder builds, to its first gap
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
	tpl := stored(st.Template)
	kept, doomed, first := []res{}, []res{}, error(nil)
	for _, r := range st.Resources {
		props := maps.Collect(maps.All(p.props(tpl, r.Logical, st.Refs, req, st.Name)))
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
	if gone(err) {
		return nil
	}
	return err
}

// gone is whether err is its service's not-found error.
func gone(err error) bool {
	var f *spi.Fault
	return errors.As(err, &f) && (strings.Contains(f.Code, "NotFound") || strings.Contains(f.Code, "NoSuch") || strings.Contains(f.Code, "NonExistent"))
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
		var resp *spi.Response // a service that panics fails the call, as InternalFailure: a rollback goes on past it
		if err := recovered(func() error {
			resp, err = pack.Invoke(ctx, &spi.Request{Identity: req.Identity, AdvertiseURL: advertise(req), Operation: op, Input: input(id, op, in)})
			return err
		}); err != nil {
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
			_ = spi.PutJSON(ctx, p.col(req, "bktcfg"), bucket+"/"+kind, map[string]any{property: value})
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

// recovered runs f and answers its error, or InternalFailure when f panics.
func recovered(f func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = internalFailure()
		}
	}()
	return f()
}

// internalFailure is the service's answer to a failure of its own, which says nothing of what failed (Common
// Errors, API reference).
func internalFailure() *spi.Fault {
	return &spi.Fault{Code: "InternalFailure", Message: "The request processing has failed because of an unknown error, exception or failure.", HTTPStatus: 500, Fault: "server"}
}

func fault(code, message string) *spi.Fault {
	return &spi.Fault{Code: code, Message: message, HTTPStatus: 400, Fault: "client"}
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

func (p *Pack) resolve(v any, refs map[string]string, req *spi.Request, stack string) any {
	switch t := v.(type) {
	case map[string]any:
		if r, ok := t["Ref"]; ok {
			v, _ := p.ref(str(r), refs, req, stack)
			if str(r) == "AWS::NoValue" {
				return noValue
			} else if p.unmade(str(r), refs) {
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
			if m, isMark := combine(index, list); isMark && (m != unknown || index == unknown || list == unknown) {
				return m // an error in either, or a list or index not known yet: checked as it is made
			} else if i, isInt := coerce(index, "Integer"); isInt && i.(int) >= 0 && i.(int) < len(spi.Slice(list)) {
				return spi.Slice(list)[i.(int)]
			}
			return unselected // "doesn't check ... if the index is out of bounds": the stack's error (selected)
		}
		if args, ok := t["Fn::Split"]; ok { // [delimiter, source string]: "A list of string values"
			args, parts := append(spi.Slice(args), nil, nil), []any{}
			if m, isMark := combine(p.resolve(args[1], refs, req, stack)); isMark {
				return m
			}
			for _, part := range strings.Split(str(p.resolve(args[1], refs, req, stack)), str(args[0])) {
				parts = append(parts, part)
			}
			return parts
		}
		if region, ok := t["Fn::GetAZs"]; ok { // "" is the stack's Region; three zones, a reading
			v := p.resolve(region, refs, req, stack)
			if m, isMark := combine(v); isMark {
				return m
			}
			r := cmp.Or(str(v), req.Identity.Region)
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

// props are the resolved Properties of tpl's resource logical, a stack's stored template, which its
// Fn::FindInMaps and Refs read (its Mappings and its parameters' types), not the template the operation was given.
func (p *Pack) props(tpl map[string]any, logical string, refs map[string]string, req *spi.Request, stack string) map[string]any {
	defer func(given map[string]any) { p.tpl = given }(p.tpl)
	p.tpl = tpl
	return asMap(p.resolve(asMap(asMap(tpl["Resources"])[logical])["Properties"], refs, req, stack))
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
func getAtt(g any, refs map[string]string) any {
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

// skipped notes in p.skip each resource the stack does not make: "CloudFormation creates the resource only
// when the condition evaluates to true", and "any resources that depend on that resource aren't created
// either" (Template Reference, Condition attribute).
func (p *Pack) skipped(order []string, refs map[string]string) {
	resources := asMap(p.tpl["Resources"])
	p.skip = map[string]bool{}
	for _, l := range order {
		p.skip[l] = off(resources[l], refs) || slices.ContainsFunc(referred(asMap(resources[l])["Properties"], refs), func(n string) bool { return p.skip[n] })
	}
}

// off is whether the condition of a resource or an output v leaves it out: one settled false (a stack's are
// all settled).
func off(v any, refs map[string]string) bool {
	c := str(asMap(v)["Condition"])
	return c != "" && refs["Condition::"+c] == "false" // one not settled, a template read alone's, is checked: both its Fn::If branches
}

// sound is the error for a template whose values cannot be made, found before anything is reserved or
// made: AWS::NoValue "Removes the corresponding resource property" (User Guide, pseudo parameters), or a list
// element, so it is never a value, and as part of one, a Fn::Join's separator, a Fn::Sub's string or
// variable, an output's value, it is refused.
func (p *Pack) sound(tpl map[string]any, refs map[string]string, req *spi.Request, stack string) error {
	var values []any
	for l, r := range asMap(tpl["Resources"]) { // all its properties, when AWS::NoValue is the whole attribute (a reading)
		if v := p.resolve(asMap(r)["Properties"], refs, req, stack); v != noValue && !p.skip[l] { // what the stack makes (skipped)
			values = append(values, v)
		}
	}
	for _, o := range asMap(tpl["Outputs"]) {
		if !off(o, refs) {
			values = append(values, p.resolve(asMap(o)["Value"], refs, req, stack))
		}
	}
	if err := refused(values); err != nil && (stack != "" || !holds(values, unselected) || holds(values, noValue) || holds(values, inValue)) {
		return err // a template read alone takes a Fn::Select of no element (round 12)
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

func (p *Pack) fnJoin(j any, refs map[string]string, req *spi.Request, stack string) any {
	arr, parts := append(spi.Slice(j), nil, nil), []string(nil) // [separator, [values]]
	sep, list := p.resolve(arr[0], refs, req, stack), p.resolve(arr[1], refs, req, stack)
	if m, isMark := combine(sep, list); isMark { // a list, from which AWS::NoValue removes its element
		return m
	}
	for _, x := range spi.Slice(list) {
		parts = append(parts, str(x))
	}
	return strings.Join(parts, str(sep))
}

// fnSub is the Fn::Sub string with each ${...} substituted: ${!Literal} is the literal ${Literal}, a
// variable its map defines is the variable's value, ${Name.Attribute} is Fn::GetAtt's and ${Name} a Ref's
// (Template Reference, Fn::Sub). A name that is none of them is left as written.
func (p *Pack) fnSub(s any, refs map[string]string, req *spi.Request, stack string) any {
	parts, held := append(spi.Slice(s), s, nil), []any{} // "text", or ["text", {variables}]; held, what each ${} is
	out := subVariable.ReplaceAllStringFunc(str(parts[0]), func(m string) string {
		name, v := m[2:len(m)-1], any(nil)
		if literal, ok := strings.CutPrefix(name, "!"); ok {
			return "${" + literal + "}"
		} else if x, ok := asMap(parts[1])[name]; ok {
			v = p.resolve(x, refs, req, stack)
		} else if logical, attr, ok := strings.Cut(name, "."); ok {
			v = getAtt([]any{logical, attr}, refs)
		} else if _, ok := p.ref(name, refs, req, stack); ok || p.unmade(name, refs) || name == "AWS::NoValue" { // a list as its text, as Fn::Join's
			v = p.resolve(map[string]any{"Ref": name}, refs, req, stack)
		} else {
			return m
		}
		held = append(held, v)
		return text(v)
	})
	if m, isMark := combine(held...); isMark { // a string built on a mark is one
		return m
	}
	return out
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
	// DeletionPolicy's options and UpdateReplacePolicy's (Template Reference): DeletionPolicy's Snapshot is
	// for the types it lists, none of which the engine makes; UpdateReplacePolicy's "reverts to ... Delete".
	for logical, r := range asMap(m["Resources"]) {
		for attr, options := range map[string]string{"DeletionPolicy": "Delete Retain RetainExceptOnCreate", "UpdateReplacePolicy": "Delete Retain Snapshot"} {
			if v, set := asMap(r)[attr]; set && !slices.Contains(strings.Fields(options), str(v)) {
				return nil, invalid("Template format error: " + logical + "'s " + attr + " must be one of [" + strings.ReplaceAll(options, " ", ", ") + "], not " + spi.JSONText(v))
			}
		}
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
	var refused *spi.Fault // the first of these refusals, each a reading (the Template Reference gives no wording)
	each([]any{m["Resources"], m["Outputs"]}, func(x map[string]any) {
		if c := str(append(spi.Slice(x["Fn::If"]), nil)[0]); x["Fn::If"] != nil && conds[c] == nil {
			missing = append(missing, c)
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
		names(asMap(r)["Properties"], named, nil)
		dep := asMap(r)["DependsOn"] // "a string or a list of strings" (Template Reference, DependsOn attribute)
		if l, ok := dep.([]any); dep != nil && reflect.TypeOf(dep).Kind() != reflect.String && (!ok || slices.ContainsFunc(l, func(d any) bool { _, s := d.(string); return !s })) {
			return nil, invalid("Template format error: DependsOn must be a string or a list of strings") // the wording is a reading
		}
		for _, d := range slices.DeleteFunc(dependsOn(r), func(d string) bool { return asMap(m["Resources"])[d] != nil }) {
			undeclared[d] = true // a DependsOn names a resource, nothing else
		}
		for n := range named {
			if l, resource := strings.CutPrefix(n, "."); resource && asMap(m["Resources"])[l] == nil && asMap(m["Parameters"])[l] != nil {
				return nil, invalid("Template error: instance of Fn::GetAtt references undefined resource " + l)
			} else if asMap(m["Resources"])[l] == nil && (resource || asMap(m["Parameters"])[n] == nil && !strings.HasPrefix(n, "AWS::")) {
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
		if _, listed := in[k]; !listed { // "If you don't specify this parameter, CloudFormation doesn't modify the stack's tags"
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
