// Package cloudformation is CloudFormation's template engine: the operations behavior/aws/cloudformation lists as native.
package cloudformation

import (
	"cmp"
	"context"
	"crypto/rand"
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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/cfntemplate"
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
	deps               spi.Deps
	cfntemplate.Reader        // reads the template an operation is given, for the request Invoke serves
	body               string // a change set's template, as it was read when the change set was made (templateBody)
}

// The template library's readers of a value, under the names the engine calls them by.
var (
	str, text, coerce, isTrue     = cfntemplate.Str, cfntemplate.Text, cfntemplate.Coerce, cfntemplate.IsTrue
	encode, stored, parseTemplate = cfntemplate.Encode, cfntemplate.Stored, cfntemplate.Parse
	parameters                    = cfntemplate.Parameters
)

// New constructs the template engine.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

// Invoke serves one native operation.
func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (out *spi.Response, err error) {
	name := str(req.Input["StackName"])
	p.Account, p.Region = req.Identity.Account, req.Identity.Region
	p.NotificationARNs = func() []any { return spi.Slice(req.Input["NotificationARNs"]) }
	defer func() { // no panic of the engine's own escapes an operation: the service's InternalFailure
		if recover() != nil {
			out, err = nil, internalFailure()
		}
	}()
	if slices.Contains(natives[:5], req.Operation) || req.Operation == "ContinueUpdateRollback" { // each takes over a stack its last operation left in progress
		p.orphan(ctx, req, name)
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
		_, tpl, _, _, err := p.template(ctx, req, named(name), str(rec["StackId"])) // every check its execution makes, the store untouched
		if err = cmp.Or(err, retyped(st, asMap(tpl["Resources"]))); err != nil {
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
	Name, Status, Reason    string    `json:",omitempty"`
	Gave                    *[]string `json:",omitempty"` // while UPDATE_FAILED: the names of the properties the failed update gave another value than the template's; none is [], and absent is a record from before they were kept
	Targets                 *[]string `json:",omitempty"` // and the ids of the targets it gave a rule
	Took                    *[]string `json:",omitempty"` // and the names of the properties its template gives that the failed update did not
}

// upsert makes the stack named name, or updates it, in two phases that share no recover. apply is the
// operation up to the write that ends it, and fails or rolls back as one; what it answers to run after is
// a finished update's cleanup, which runs once that write is made and can undo nothing of it.
func (p *Pack) upsert(ctx context.Context, req *spi.Request, name string, update bool, id string, cs changeSet) (*spi.Response, error) {
	out, after, err := p.apply(ctx, req, name, update, id, cs)
	if after != nil {
		after()
	}
	return out, err
}

// apply makes the stack named name, or updates it, a create under id when one is given (a CREATE change
// set's, which it answered), as the execution of the change set under key when one is given: claimed in the
// transaction that starts it, retired in the one that ends it.
func (p *Pack) apply(ctx context.Context, req *spi.Request, name string, update bool, id string, cs changeSet) (out *spi.Response, after func(), err error) {
	if err := checkStackName(name, update); err != nil {
		return nil, nil, err
	}
	if arns := spi.Slice(req.Input["NotificationARNs"]); len(arns) > 5 { // a member's constraint, before whether the stack exists
		return nil, nil, invalid("1 validation error detected: Value at 'notificationARNs' failed to satisfy constraint: Member must have length less than or equal to 5")
	}
	given, name := name, named(name)
	id = cmp.Or(id, stackARN(req, name)+p.deps.Rand.Hex(8))
	input, loaded := spi.Decode[map[string]any]([]byte(spi.JSONText(req.Input))), stack{}
	rereads, _ := ctx.Value(staleKey{}).(int)
	if cur, err := p.load(ctx, req, given); err == nil && update { // the id an update's AWS::StackId answers
		id, loaded = cur.ID, cur
		previous(req.Input, cur)
	} else if update { // "Stack [x] does not exist" before any template error; read again where it is written
		return nil, nil, err
	}
	arns, tags := append([]any{}, spi.Slice(req.Input["NotificationARNs"])...), append([]any{}, spi.Slice(req.Input["Tags"])...)
	body, tpl, order, refs, err := p.template(ctx, req, name, id)
	if err != nil {
		return nil, nil, err
	}
	resources, outs := asMap(tpl["Resources"]), asMap(tpl["Outputs"])
	// old is what an update finds, and held the record it reserves it with, UPDATE_IN_PROGRESS; a create
	// reserves the name, CREATE_IN_PROGRESS, in the same transaction.
	var old, held stack
	var leave *stack // what a failing operation leaves, until it has settled it: the next operation's to settle (orphan)
	op, release := p.hold()
	defer func() { release(leave) }() // registered at once: a refused reservation releases too
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
		} else if err := retyped(old, resources); update && err != nil {
			return err
		} else if id, held = cmp.Or(old.ID, id), old; update { // "is in UPDATE_IN_PROGRESS state" to another change
			held.Status, held.Op = "UPDATE_IN_PROGRESS", op
			return tx.Put(name, []byte(spi.JSONText(held)))
		}
		return tx.Put(name, []byte(spi.JSONText(reserved)))
	}); errors.Is(err, errStale) && rereads < 3 {
		req.Input = input
		return p.apply(context.WithValue(ctx, staleKey{}, rereads+1), req, given, update, "", cs)
	} else if errors.Is(err, errStale) { // still changing: a caller it will not serve now, in the service's words
		return nil, nil, fault("Throttling", "Rate exceeded")
	} else if err != nil {
		return nil, nil, err
	}
	read := held // the record as the update reserved it, which its writes go over (a rollback changes old's resources)
	read.Resources = slices.Clone(old.Resources)
	if update { // settled again: a record before round 9 holds no conditions
		old.Refs = p.Conditioned(stored(old.Template), old.Refs, name, old.ID)
	}
	st := stack{Name: name, ID: id, Status: map[bool]string{false: "CREATE_COMPLETE", true: "UPDATE_COMPLETE"}[update], Template: body, Refs: refs,
		Resources: []res{}, Outputs: []any{}, NotificationARNs: arns, Tags: tags} // never null: every read of a stack lists them
	applied := map[string]map[string]any{} // the properties the update gave what it changed in place, until a rollback has changed each back
	defer func() {
		if leave == &old { // an update whose rollback did not settle: what it had not restored and what it made are ContinueUpdateRollback's
			for i, o := range old.Resources {
				if failed := applied[o.Logical]; failed != nil { // changed in place, and not put back
					old.Resources[i].Status, old.Resources[i].Reason = "UPDATE_FAILED", old.Reason
					old.Resources[i].Gave, old.Resources[i].Targets, old.Resources[i].Took = gave(failed, p.Props(stored(old.Template), o.Logical, old.Refs, old.Name))
				}
			}
			for _, r := range st.Resources {
				if has(old.Resources, r) {
				} else if retains(tpl, "DeletionPolicy", "Retain")(r) { // kept, as its rollback keeps it: it leaves the stack
					r.Status = "DELETE_SKIPPED"
					old.Events = append(old.Events, r)
				} else {
					r.Status, r.Reason = "DELETE_FAILED", old.Reason
					old.Resources = append(old.Resources, r)
				}
			}
		}
		st.Status, old.Status = "ROLLBACK_FAILED", "UPDATE_ROLLBACK_FAILED" // where a rollback that did not settle leaves it
	}()
	fail := func(cause error) (*spi.Response, func(), error) {
		st.Reason, old.Reason = cause.Error(), cause.Error()
		if leave = &st; !update || old.ID == "" { // a failed create rolls back what it made (OnFailure ROLLBACK, the default) and keeps the stack, and why
			_ = p.unwind(ctx, req, reserved, st, nil, st.Resources, nil, "ROLLBACK_COMPLETE", "ROLLBACK_FAILED", retains(tpl, "DeletionPolicy", "Retain"))
		} else {
			leave = &old
			_ = p.rollback(ctx, req, read, old, st.Resources, applied, retains(tpl, "DeletionPolicy", "Retain"))
		}
		if raw, _, err := p.col(req, "cfn").Get(ctx, name); err == nil && spi.Decode[stack](raw).Op != op { // settled: nothing is left
			leave = nil
		}
		p.unsettled(ctx, req, cs, "EXECUTE_IN_PROGRESS", cause)
		return nil, nil, cause
	}
	defer func() { // a panic of the engine's own, after the reservation, fails the operation as any failure does;
		// one in that rollback leaves what remains, with why, for the next operation to settle
		if recover() != nil {
			out, after, err = nil, nil, internalFailure()
			if st.Reason, old.Reason = err.Error(), err.Error(); leave == nil { // not one in the rollback itself
				_, _, _ = fail(err) // a panic of its own is Invoke's to answer, and what remains the next operation's
			}
		}
	}()
	planned := maps.Clone(st.Refs)
	for _, logical := range order { // a name taken fails the stack before anything is made: a Ref answers the id planned
		rm := asMap(resources[logical])
		typ, props := str(rm["Type"]), asMap(p.Resolve(rm["Properties"], planned, name))
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
		typ, props := str(rm["Type"]), maps.Collect(maps.All(asMap(p.Resolve(rm["Properties"], st.Refs, name))))
		prev, how, n, err := p.next(ctx, req, st, old, logical, typ, props)  // again: a name may come from a resource made before it
		phys := map[string]string{"": prev.Physical}[how]                    // one an update keeps as it is
		if err = cmp.Or(err, encode(props)); how == "update" && err == nil { // recorded before it is called: a half-made change is put back too
			applied[logical] = maps.Clone(props)
			phys, err = p.provision(ctx, req, typ, logical, n, props, prior{props: p.Props(stored(old.Template), logical, old.Refs, old.Name)}, prev.Physical, id)
		} else if how != "" && err == nil {
			phys, err = p.provision(ctx, req, typ, logical, n, props, prior{}, "", id)
		}
		if phys != "" { // recorded once it exists, before anything is set on it, so a rollback finds a half-made one too
			st.Refs[logical] = phys
			attribute(st.Refs, req, typ, logical, phys)
			st.Resources = append(st.Resources, res{Logical: logical, Physical: phys, Type: typ, Name: n})
		}
		if how != "" && how != "update" && err == nil && typ == "AWS::S3::Bucket" {
			err = p.configureBucket(ctx, req, phys, props, prior{})
		}
		if err != nil {
			return fail(err)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(outs)) {
		v := p.Resolve(asMap(outs[k])["Value"], st.Refs, name)
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
		return &spi.Response{Output: map[string]any{"StackId": id}}, nil, nil
	} else if !wrote {
		// Another change came first, in progress while this update ran: what this update made that the stack
		// does not hold goes, all of it when the stack is being deleted (its delete may have run first).
		rival := map[bool]string{true: "DELETE_IN_PROGRESS", false: "UPDATE_IN_PROGRESS"}[cur.ID != id || strings.HasPrefix(cur.Status, "DELETE_")]
		if cur.ID == id && rival == "DELETE_IN_PROGRESS" {
			cur.Resources = nil
		}
		made := slices.DeleteFunc(st.Resources, func(r res) bool { return has(cur.Resources, r) })
		_ = p.unwind(ctx, req, stack{}, st, nil, made, nil, "", "", func(res) bool { return false })
		err := invalid("Stack:" + id + " is in " + rival + " state and can not be updated.")
		p.unsettled(ctx, req, cs, "EXECUTE_IN_PROGRESS", err)
		return nil, nil, err
	}
	if !update {
		return &spi.Response{Output: map[string]any{"StackId": id}}, nil, nil
	}
	done, was := st, old // as they are now: apply's own ending changes them
	return &spi.Response{Output: map[string]any{"StackId": id}}, func() { p.cleanup(ctx, req, done, was, tpl) }, nil
}

// cleanup is what a finished update does once its record is written: it deletes the old resources the
// stack no longer holds, replaced ("creating a new one and deleting the old one") or dropped. A replaced one
// (the stack still holds its logical id) the template's UpdateReplacePolicy retains; a dropped one, or one
// its condition now leaves out, the old DeletionPolicy. The stack "has been updated and is usable, but
// CloudFormation is still deleting the old resources" (User Guide, stack status codes), so nothing here
// rolls the update back: one it cannot delete is abandoned, and a panic of the engine's own abandons what
// is not yet deleted the same way, each a DELETE_FAILED event of a stack that stays UPDATE_COMPLETE.
func (p *Pack) cleanup(ctx context.Context, req *spi.Request, st, old stack, tpl map[string]any) {
	seen, stale := st, []res(nil)
	defer func() { // installed before anything is read
		if recover() == nil {
			return
		}
		for _, r := range stale { // those finish had not come to, or was at
			r.Status, r.Reason = "DELETE_FAILED", "The cleanup ended before it deleted the resource." // the wordings are readings
			st.Events, st.Reason = append(st.Events, r), "The cleanup ended before it deleted every old resource."
		}
		_ = recovered(func() error { _, _, err := p.settle(ctx, req, seen, st); return err })
	}()
	stale = slices.DeleteFunc(slices.Clone(old.Resources), func(r res) bool { return has(st.Resources, r) })
	oldTpl := stored(old.Template)
	_ = p.finish(ctx, req, seen, &st, slices.Clone(st.Resources), &stale, nil, st.Status, st.Status, func(r res) bool {
		if slices.ContainsFunc(st.Resources, func(n res) bool { return n.Logical == r.Logical }) { // still the stack's
			return retains(tpl, "UpdateReplacePolicy", "Retain")(r)
		}
		return retains(oldTpl, "DeletionPolicy", "Retain", "RetainExceptOnCreate")(r)
	})
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
	tpl, order, refs, err := p.Read(body, req.Input, stack, id, func(tpl map[string]any) error { return capable(tpl, req.Input, stack) })
	return body, tpl, order, refs, err
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
	} else if st.Status == "DELETE_COMPLETE" { // read by its id: deleted already, which nothing changes
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
		del.Status, del.Op = "DELETE_IN_PROGRESS", map[bool]string{true: st.Op, false: op}[was == "CREATE_IN_PROGRESS"]
		del.Reason = "" // an earlier failure's: a delete that ends cleanly has none
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
	return p.finish(ctx, req, seen, &st, kept, &doomed, first, done, failed, skip)
}

// finish is unwind over the caller's own stack and list: *doomed loses each resource as it is dealt with, so
// a caller that recovers a panic finds in it what was not, and in st what was recorded so far.
func (p *Pack) finish(ctx context.Context, req *spi.Request, seen stack, st *stack, kept []res, doomed *[]res, first error, done, failed string, skip func(res) bool) error {
	st.Resources = append([]res{}, kept...) // never null: every read of the stack lists its resources
	for ; len(*doomed) > 0; *doomed = (*doomed)[:len(*doomed)-1] {
		r := (*doomed)[len(*doomed)-1]
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
	_, wrote, err := p.settle(ctx, req, seen, *st)
	if seen.Status == "CREATE_IN_PROGRESS" && !wrote && err == nil { // a DeleteStack began during the create: it ends here
		seen.Status, st.Status = "DELETE_IN_PROGRESS", map[bool]string{true: "DELETE_COMPLETE", false: "DELETE_FAILED"}[first == nil]
		_, _, err = p.settle(ctx, req, seen, *st)
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
	if !strings.HasSuffix(st.Status, "_IN_PROGRESS") { // a settled record is no operation's
		st.Op = ""
	}
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
var opSeq, opKept atomic.Int64 // numbers them, and counts what they keep
var opNonce = rand.Text()      // this process's: an id in a record another process left, or a restored one, is never one of ours

// kept is a record an operation kept for the one that takes its stack over, and the store the stack is in.
type kept struct {
	stack
	store spi.Store
}

// stackARN is the stack id of the caller's stack named name, up to the part that tells one such stack from
// the next: its account, its Region and its name.
func stackARN(req *spi.Request, name string) string {
	return "arn:aws:cloudformation:" + req.Identity.Region + ":" + req.Identity.Account + ":stack/" + name + "/"
}

// sameStore is whether a and b are one store: false for a kind of store that cannot be compared.
func sameStore(a, b spi.Store) (same bool) {
	defer func() { _ = recover() }()
	return a == b
}

// hold registers a running operation, and answers its id, after a "/", and its release, which the
// operation defers: given the record a failed operation could not settle, the release keeps it for the
// operation that takes the stack over, while fewer than 1024 are kept: with that many waiting, no more is.
func (p *Pack) hold() (string, func(*stack)) {
	id := opNonce + "-" + strconv.FormatInt(opSeq.Add(1), 10)
	ops.Store(id, nil)
	return "/" + id, func(left *stack) {
		if ops.Delete(id); left != nil && opKept.Load() < 1024 { // counted as it is kept, and as it is written
			opKept.Add(1)
			ops.Store(id, kept{*left, p.deps.Store})
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
	ops.Range(func(k, v any) bool { // a record kept for this stack by an operation it is no longer held by is no one's now
		if left, isKept := v.(kept); isKept && strings.HasPrefix(left.ID, stackARN(req, named(name))) && k != id && sameStore(left.store, p.deps.Store) {
			unkeep(k)
		}
		return true
	})
	v, known := ops.Load(id)
	left, mine := v.(kept) // what its operation, ended in this process, could not write
	next, kept := left.stack, mine && left.ID == cur.ID
	if !strings.HasSuffix(cur.Status, "_IN_PROGRESS") || cur.Status == "REVIEW_IN_PROGRESS" || known && !mine { // not one in progress, or one running
		return
	} else if !kept {
		next = cur
		next.Status = cmp.Or(map[string]string{"CREATE_IN_PROGRESS": "ROLLBACK_FAILED"}[cur.Status], "UPDATE_ROLLBACK_FAILED")
		next.Reason = "The operation on the stack ended before it settled the stack." // the wording is a reading
	}
	if cur.Status == "DELETE_IN_PROGRESS" {
		next.Status = "DELETE_FAILED"
	}
	if _, wrote, err := p.settle(ctx, req, cur, next); wrote && err == nil && kept { // kept until it is written
		unkeep(id)
	}
}

// unkeep releases the record kept under id, counted once however many operations come to release it.
func unkeep(id any) {
	if _, was := ops.LoadAndDelete(id); was {
		opKept.Add(-1)
	}
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
		if _ = json.Unmarshal(kv.Value, &other); other.Name != name && has(other.Resources, r) {
			return other.Name
		}
	}
	return ""
}

// has is whether rs holds the resource r: one of its type under its physical id, since two types may share
// a name (a bucket and a parameter).
func has(rs []res, r res) bool {
	return slices.ContainsFunc(rs, func(o res) bool { return o.Type == r.Type && o.Physical == r.Physical })
}

// retyped is CloudFormation's refusal of an update whose template, of resources, gives a logical id another
// type than the stack old holds it under (the wording is a reading); one held only for a delete
// (DELETE_FAILED) is no longer the stack's to keep a type for.
func retyped(old stack, resources map[string]any) error {
	var changed []string
	for _, r := range old.Resources {
		if t := str(asMap(resources[r.Logical])["Type"]); t != "" && t != r.Type && r.Status != "DELETE_FAILED" {
			changed = append(changed, r.Logical)
		}
	}
	if slices.Sort(changed); len(changed) > 0 {
		return invalid("Update of resource type is not permitted. The new template modifies resource type of the following resources: [" + strings.Join(changed, ", ") + "]")
	}
	return nil
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
	"AWS::S3::Bucket":              {"BucketName", 63, "aws.s3 HeadBucket Bucket={n}", "BucketAlreadyOwnedByYou", "{n}", "BucketName BucketNamePrefix BucketNamespace ObjectLockEnabled", "aws.s3 CreateBucket Bucket={n}", "Bucket", "", "aws.s3 DeleteBucket Bucket={id}", "Arn=arn:aws:s3:::{id} DomainName={id}.s3.amazonaws.com"},
	"AWS::SQS::Queue":              {"QueueName", 80, "aws.sqs GetQueueUrl QueueName={n}", "QueueAlreadyExists", "", "QueueName FifoQueue", "aws.sqs CreateQueue QueueName={n} Attributes", "QueueUrl", "", "aws.sqs DeleteQueue QueueUrl={id}", "Arn=arn:aws:sqs:{r}:{a}:{id/} QueueName={id/} QueueUrl={id}"},
	"AWS::SNS::Topic":              {"TopicName", 256, "aws.sns GetTopicAttributes TopicArn=arn:aws:sns:{r}:{a}:{n}", "AlreadyExistsException", "arn:aws:sns:{r}:{a}:{n}", "TopicName FifoTopic", "aws.sns CreateTopic Name={n}", "TopicArn", "", "aws.sns DeleteTopic TopicArn={id}", "TopicArn={id} TopicName={id:}"},
	"AWS::DynamoDB::Table":         {"TableName", 255, "aws.dynamodb DescribeTable TableName={n}", "ResourceInUseException", "{n}", "TableName ImportSourceSpecification KeySchema LocalSecondaryIndexes", "aws.dynamodb CreateTable * TableName={n}", "TableName", "aws.dynamodb UpdateTable TableName={id} BillingMode ProvisionedThroughput StreamSpecification TableClass", "aws.dynamodb DeleteTable TableName={id}", "Arn=arn:aws:dynamodb:{r}:{a}:table/{id}"},
	"AWS::IAM::Role":               {"RoleName", 64, "aws.iam GetRole RoleName={n}", "EntityAlreadyExists", "{n}", "RoleName Path", "aws.iam CreateRole RoleName={n} AssumeRolePolicyDocument=%AssumeRolePolicyDocument", "Role.RoleName", "aws.iam UpdateAssumeRolePolicy RoleName={id/} PolicyDocument=%AssumeRolePolicyDocument", "aws.iam DeleteRole RoleName={id/}", "Arn=arn:aws:iam::{a}:role/{id}"},
	"AWS::SSM::Parameter":          {"Name", 2048, "aws.ssm GetParameter Name={n}", "ParameterAlreadyExists", "{n}", "Name", "aws.ssm PutParameter Name={n} Value= Value=$Value Type=String Type=$Type", "Name", "aws.ssm PutParameter Name={id} Value= Value=$Value Type=String Type=$Type Overwrite=true", "aws.ssm DeleteParameter Name={id}", "Arn=arn:aws:ssm:{r}:{a}:parameter/{param}"},
	"AWS::SecretsManager::Secret":  {"Name", 512, "aws.secretsmanager DescribeSecret SecretId={n}", "ResourceExistsException", "arn:aws:secretsmanager:{r}:{a}:secret:{n}", "Name", "aws.secretsmanager CreateSecret Name={n} SecretString=$SecretString Description=$Description", "ARN", "aws.secretsmanager UpdateSecret SecretId={id} SecretString=$SecretString Description=$Description", "aws.secretsmanager DeleteSecret SecretId={id} ForceDeleteWithoutRecovery=true", "Id={id}"},
	"AWS::KMS::Key":                {"", 0, "", "", "", "KeySpec KeyUsage MultiRegion", "aws.kms CreateKey Description= Description=$Description", "KeyMetadata.KeyId", "aws.kms UpdateKeyDescription KeyId={id} Description=$Description need:Description", "aws.kms ScheduleKeyDeletion KeyId={id} PendingWindowInDays=7", "Arn=arn:aws:kms:{r}:{a}:key/{id} KeyId={id}"},
	"AWS::Logs::LogGroup":          {"LogGroupName", 512, "aws.logs DescribeLogGroups logGroupNamePrefix={n}", "ResourceAlreadyExistsException", "{n}", "LogGroupName", "aws.logs CreateLogGroup logGroupName={n}", "logGroupName", "", "aws.logs DeleteLogGroup logGroupName={id}", "Arn=arn:aws:logs:{r}:{a}:log-group:{id}:*"},
	"AWS::Events::Rule":            {"Name", 64, "aws.events DescribeRule Name={n} EventBusName=$EventBusName", "ResourceAlreadyExistsException", "{n}", "Name EventBusName", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "RuleArn", "aws.events PutRule Name={n} ScheduleExpression=$ScheduleExpression State=$State Description=$Description EventBusName=$EventBusName EventPattern=%EventPattern", "aws.events DeleteRule Name={rule} EventBusName={bus}", "Arn=arn:aws:events:{r}:{a}:rule/{bus/}{rule} RuleName={rule}"},
	"AWS::Lambda::Function":        {"FunctionName", 64, "aws.lambda GetFunction FunctionName={n}", "ResourceConflictException", "{n}", "FunctionName PackageType DurableConfig TenancyConfig", "aws.lambda CreateFunction * FunctionName={n}", "FunctionName", "aws.lambda UpdateFunctionConfiguration * FunctionName={id:}", "aws.lambda DeleteFunction FunctionName={id:}", "Arn=arn:aws:lambda:{r}:{a}:function:{id}"},
	"AWS::Kinesis::Stream":         {"Name", 128, "aws.kinesis DescribeStreamSummary StreamName={n}", "ResourceInUseException", "{n}", "Name", "aws.kinesis CreateStream StreamName={n} ShardCount", "StreamName", "aws.kinesis UpdateShardCount StreamName={id} TargetShardCount=@ShardCount ScalingType=UNIFORM_SCALING need:TargetShardCount", "aws.kinesis DeleteStream StreamName={id}", "Arn=arn:aws:kinesis:{r}:{a}:stream/{id}"},
	"AWS::Kinesis::ResourcePolicy": {"", 0, "", "", "", "ResourceArn", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "ResourceARN", "aws.kinesis PutResourcePolicy ResourceARN=$ResourceARN ResourceARN=$ResourceArn Policy=%Policy Policy=%ResourcePolicy", "aws.kinesis DeleteResourcePolicy ResourceARN={id}", ""},
	"AWS::ApiGateway::RestApi":     {"Name", 1024, "", "", "", "", "aws.apigateway CreateRestApi name={n}", "id", "aws.apigateway UpdateRestApi restApiId={id} name={n}", "aws.apigateway DeleteRestApi restApiId={id}", "RestApiId={id}"},
}

// prior is what the template a resource is updated from gave it: its properties, none for a resource the
// template declared without any (it is known, to have been given nothing), or unknown. A continued rollback
// knows them by name alone, from what its resource recorded (res.Gave, res.Targets): changed holds the
// properties the failed update gave another value than the template's, and props only its rule targets,
// by id. It passes unknown only for a record written before the names were kept; only then is every
// property the template lacks read as given before, and every value as changed.
type prior struct {
	props         map[string]any
	changed, took map[string]bool // took: what the template gives that the failed update did not, a change that gave nothing
	unknown       bool
}

// gave is whether the template the resource is updated from gave it property.
func (w prior) gave(property string) bool {
	return w.unknown || w.props[property] != nil || w.changed[property]
}

// differs is whether the template the resource is updated from gave property another value than now.
func (w prior) differs(property string, now any) bool {
	if w.changed != nil {
		return w.changed[property] || w.took[property]
	}
	return w.unknown || !same(w.props[property], now)
}

// same is whether a and b are one property value: both absent, or both given and alike member for member, a
// number or a Boolean being its text (a Number parameter's Ref is a String). Each is compared inside a
// list, where a string is quoted: it is its own value, never the list, the object or the nothing it reads as.
func same(a, b any) bool {
	return spi.JSONText([]any{plain(a)}) == spi.JSONText([]any{plain(b)})
}

func plain(v any) any {
	switch v := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, member := range v {
			out[k] = plain(member)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, member := range v {
			out[i] = plain(member)
		}
		return out
	case nil:
		return nil
	}
	return str(v)
}

// gave is what is recorded on a resource left UPDATE_FAILED of what the failed update gave it, which its
// template (now) does not: the names of those properties, never a value, [] for none, and the ids of the
// targets it gave a rule; and, apart from what it gave, the names of the properties the template gives that
// it did not (took), whose values a restore carries back.
func gave(failed, now map[string]any) (names, targets, took *[]string) {
	for _, property := range slices.Sorted(maps.Keys(now)) {
		if failed[property] == nil {
			took = cmp.Or(took, &[]string{})
			*took = append(*took, property)
		}
	}
	changed := []string{}
	for _, property := range slices.Sorted(maps.Keys(failed)) {
		if !same(failed[property], now[property]) {
			changed = append(changed, property)
		}
	}
	if list, listed := failed["Targets"].([]any); listed {
		ids := []string{}
		for _, t := range list {
			ids = append(ids, str(asMap(t)["Id"]))
		}
		targets = &ids
	}
	return &changed, targets, took
}

// recorded is the prior a continued rollback has of the resource r: what gave recorded.
func recorded(r res) prior {
	if r.Gave == nil {
		return prior{unknown: true}
	}
	was := prior{changed: map[string]bool{}, took: map[string]bool{}, props: map[string]any{}}
	for _, property := range *r.Gave {
		was.changed[property] = true
	}
	for _, property := range *cmp.Or(r.Took, &[]string{}) {
		was.took[property] = true
	}
	if r.Targets != nil {
		ids := []any{}
		for _, id := range *r.Targets {
			ids = append(ids, map[string]any{"Id": id})
		}
		was.props["Targets"] = ids
	}
	return was
}

// under is the name the resource r of the stack st, with the properties props, was made under: the one
// recorded, or, for a record from before names were, the template's or the generated one, as claim finds it.
func under(st stack, r res, props map[string]any) string {
	return cmp.Or(r.Name, str(props[kinds[r.Type].member]), physicalName(st.Name, r.Logical, st.ID, r.Type, props))
}

// clears are what a type's update call sends for a property the template it is updated from gave and the
// template gives no longer, so that an update, and a rollback's restore, leave exactly the template's
// properties and nothing that was set outside it is touched: each service's update keeps what a call does
// not name, so the value is the one its API reference gives a resource made without the property (Lambda's
// Timeout "The default is 3 seconds", MemorySize "The default value is 128 MB"; DynamoDB's StreamEnabled
// false, BillingMode PROVISIONED, TableClass STANDARD; an empty description; a stream's one shard). An
// entry is the member to send, named as its property, or property>member where the names differ. A
// bucket's are bucketOps, a rule's targets provision's, and a secret's value is sent only when the
// template's differs (provision). Four updates send what the template gives whatever was there, because
// the template must give it and the call is a whole replacement: a parameter's value and type
// (PutParameter), a role's trust policy, a stream's resource policy, and a rule (PutRule: "If you omit arguments in
// PutRule, the old values for those arguments are not kept"), so a rule's description or state set outside
// the template does not outlive an update of the rule, in the service as here. A table's
// DeletionProtectionEnabled is not sent at all: the table service here keeps it false.
var clears = map[string]string{
	"AWS::DynamoDB::Table":        `StreamSpecification=^{"StreamEnabled":false} BillingMode=PROVISIONED TableClass=STANDARD`,
	"AWS::Lambda::Function":       `Description= Timeout=3 MemorySize=128 Environment=^{"Variables":{}} Layers=^[] KMSKeyArn= DeadLetterConfig=^{"TargetArn":""} TracingConfig=^{"Mode":"PassThrough"} EphemeralStorage=^{"Size":512}`,
	"AWS::SecretsManager::Secret": `Description=`,
	"AWS::KMS::Key":               `Description=`,
	"AWS::Kinesis::Stream":        `ShardCount>TargetShardCount=1`,
}

// do makes a call a kind spells "service Operation member=value ...", and answers the answer over the
// request. A value is {n} the resource's name, {id} its physical id ({id/} and {id:}, its last segment),
// {rule} and {bus} a rule's name and event bus, {r} and {a} the region and account; $Prop is the template's
// property Prop as text, %Prop as JSON text, @Prop as given, each set only when given; ^ and JSON, that JSON; else the value
// itself, which call types by the member's model. A member alone is @member, and * is every property. An
// empty spec calls nothing, and neither does one with need:member whose member nothing set.
func (p *Pack) do(ctx context.Context, req *spi.Request, spec, n, id string, props map[string]any) (map[string]any, error) {
	f, in := strings.Fields(spec), map[string]any{}
	if len(f) < 2 {
		return in, nil
	}
	sub := strings.NewReplacer("{n}", n, "{id}", id, "{id/}", spi.LastSlash(id), "{id:}", lastColon(id), "{rule}", spi.LastSlash(id[strings.Index(id, "|")+1:]),
		"{bus}", ruleBus(id), "{r}", req.Identity.Region, "{a}", req.Identity.Account)
	var needs []string
	for _, arg := range f[2:] {
		k, v, set := strings.Cut(arg, "=")
		if !set {
			v = "@" + k
		}
		if pv, given := props[v[min(1, len(v)):]]; strings.HasPrefix(arg, "need:") {
			needs = append(needs, arg[len("need:"):])
		} else if arg == "*" {
			maps.Copy(in, props)
		} else if strings.HasPrefix(v, "^") {
			in[k] = spi.Decode[any]([]byte(v[1:]))
		} else if sigil := strings.IndexByte("@$%", (v + " ")[0]); sigil >= 0 {
			if given {
				in[k] = []any{pv, str(pv), spi.JSONText(pv)}[sigil]
			}
		} else {
			in[k] = sub.Replace(v)
		}
	}
	if slices.ContainsFunc(needs, func(member string) bool { _, set := in[member]; return !set }) {
		return in, nil
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
	before, how := p.Props(tpl, logical, old.Refs, old.Name), ""
	for _, k := range slices.Concat(slices.Collect(maps.Keys(before)), slices.Collect(maps.Keys(props))) {
		if same(before[k], props[k]) {
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
	if cfntemplate.Unknown(props[k.member]) { // one built on an id known only once made is checked as it is made:
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

func fifo(p map[string]any) bool { return isTrue(p["FifoQueue"]) }

// physicalName is the name of a resource the template leaves unnamed (cfntemplate.PhysicalName), within the
// longest name its service takes: none for a type no template names.
func physicalName(stack, logical, stackID, typ string, props map[string]any) string {
	k := kinds[typ]
	if k.member == "" {
		return ""
	}
	return cfntemplate.PhysicalName(stack, logical, stackID, typ, k.max, props)
}

func isBoolean(v any) bool { _, ok := coerce(v, "Boolean"); return ok }

// provision creates a resource through the service that owns it, and answers the resource's physical id.
// Going through the owner's API keeps its store layout, validation and cascades that service's business.
// existing is what an update finds: a resource its kind updates in place is updated under it, and was what
// the template it is updated from gave it, of which each property the template no longer gives is cleared
// (clears).
func (p *Pack) provision(ctx context.Context, req *spi.Request, typ, logical, n string, props map[string]any, was prior, existing, stackID string) (string, error) {
	k, known := kinds[typ]
	spec, phys := k.create, ""
	if !known {
		return "", invalid("unsupported resource type " + typ)
	} else if _, list := props["Targets"].([]any); typ == "AWS::Events::Rule" && props["Targets"] != nil && !list { // "Array of Target", before the rule is touched
		return "", invalid("Properties validation failed for resource " + logical + ": Targets must be a list, not " + spi.JSONText(props["Targets"]))
	} else if existing != "" {
		spec = k.update
		for _, c := range strings.Fields(clears[typ]) {
			property, send, renamed := strings.Cut(c, ">")
			if !renamed {
				property, send = c[:strings.Index(c, "=")], c
			}
			if props[property] == nil && was.gave(property) {
				spec += " " + send
			}
		}
	}
	if typ == "AWS::SecretsManager::Secret" && existing != "" && !was.differs("SecretString", props["SecretString"]) && !was.differs("GenerateSecretString", props["GenerateSecretString"]) {
		props = maps.Clone(props) // the value is the template's as it was: not sent, so one put there since stays
		delete(props, "SecretString")
		delete(props, "GenerateSecretString")
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
		if err = p.dropTargets(ctx, req, phys, keep, was); set && err == nil { // what the template gave before and gives no longer goes
			// "each entry in FailedEntries provides the ID of the failed target and the error code":
			// a target not added fails the rule, as its error does.
			put, putErr := p.do(ctx, req, "aws.events PutTargets Rule={n} Targets EventBusName=$EventBusName", n, "", props)
			if failed, _ := put["FailedEntries"].([]any); putErr == nil && len(failed) > 0 {
				putErr = fault(cmp.Or(str(asMap(failed[0])["ErrorCode"]), "ValidationError"), "PutTargets failed to add targets: "+spi.JSONText(failed))
			}
			err = putErr
		}
	} else if err == nil && typ == "AWS::S3::Bucket" && existing != "" { // one it makes, upsert configures once it is recorded
		err = p.configureBucket(ctx, req, n, props, was)
	} else if err != nil && existing == "" { // none once a create failed
		phys = ""
	}
	return phys, err
}

// rollback undoes a failed update as UPDATE_ROLLBACK does. What the update changed in place is updated
// back, through the same update, to exactly the previous template's properties (applied holds what the
// update gave each, so what it added is cleared); what it made, half-made ones included, is deleted; what
// it kept is not called. The stack keeps the previous template and resources, UPDATE_ROLLBACK_COMPLETE, or
// UPDATE_ROLLBACK_FAILED with what it could not restore UPDATE_FAILED and what it could not delete still
// listed, for ContinueUpdateRollback and DeleteStack. Only a finished update deletes what it replaced.
func (p *Pack) rollback(ctx context.Context, req *spi.Request, seen, old stack, made []res, applied map[string]map[string]any, skip func(res) bool) error {
	tpl := stored(old.Template)
	var doomed []res
	var first error
	for _, r := range made {
		if old.Refs[r.Logical] != r.Physical {
			doomed = append(doomed, r)
		}
	}
	for i, r := range old.Resources { // each the update changed in place, a change it did not finish included
		was := applied[r.Logical]
		if was == nil {
			continue
		}
		props := maps.Collect(maps.All(p.Props(tpl, r.Logical, old.Refs, old.Name)))
		if _, err := p.provision(ctx, req, r.Type, r.Logical, under(old, r, props), props, prior{props: was}, r.Physical, old.ID); err != nil { // UPDATE_FAILED, for ContinueUpdateRollback
			old.Resources[i].Status, old.Resources[i].Reason = "UPDATE_FAILED", err.Error() // the stack's reason in the service's words
			old.Resources[i].Gave, old.Resources[i].Targets, old.Resources[i].Took = gave(was, props)
			first = cmp.Or(first, error(invalid("The following resource(s) failed to update: ["+r.Logical+"].")))
		}
		delete(applied, r.Logical) // put back, or marked here: no longer a takeover's to mark
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
	// It reserves the stack as every operation does: UPDATE_ROLLBACK_IN_PROGRESS under its own id, written
	// only over the record read, so no other change runs beside it. One that does not settle leaves the
	// stack as it found it, UPDATE_ROLLBACK_FAILED, for the next operation to settle (orphan).
	op, release := p.hold()
	seen, left := st, &stack{}
	*left, seen.Status, seen.Op = st, "UPDATE_ROLLBACK_IN_PROGRESS", op
	defer func() { release(left) }()
	if _, started, err := p.settle(ctx, req, st, seen); err != nil || !started {
		if left = nil; err == nil { // it changed since it was read
			return p.resume(ctx, req, name)
		}
		return nil, err
	}
	tpl := stored(st.Template)
	kept, doomed, first := []res{}, []res{}, error(nil)
	for _, r := range st.Resources {
		props := maps.Collect(maps.All(p.Props(tpl, r.Logical, st.Refs, st.Name)))
		if r.Status == "DELETE_FAILED" {
			doomed = append(doomed, r)
			continue
		} else if r.Status == "UPDATE_FAILED" && !slices.Contains(skip, any(r.Logical)) {
			_, err = p.provision(ctx, req, r.Type, r.Logical, under(st, r, props), props, recorded(r), r.Physical, st.ID)
		}
		if r.Status == "UPDATE_FAILED" && err != nil {
			r.Reason, first, err = err.Error(), cmp.Or(first, err), nil
		} else if r.Status == "UPDATE_FAILED" {
			r.Status, r.Reason, r.Gave, r.Targets, r.Took = "UPDATE_COMPLETE", "", nil, nil, nil
		}
		kept = append(kept, r)
	}
	st.Reason = "" // what the failed update made is deleted whatever its policy: Retain left the stack when it rolled back
	err = p.unwind(ctx, req, seen, st, kept, doomed, first, "UPDATE_ROLLBACK_COMPLETE", "UPDATE_ROLLBACK_FAILED", func(res) bool { return false })
	if raw, _, readErr := p.col(req, "cfn").Get(ctx, st.Name); readErr == nil && spi.Decode[stack](raw).Op != op { // settled: nothing is left
		left = nil
	}
	if err != nil {
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
		err = p.dropTargets(ctx, req, r.Physical, nil, prior{unknown: true})
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

// dropTargets removes the rule's targets that the template it is updated from gave (was) but those keep
// names, so one put on the rule outside its template stays; with what it gave unknown, or known by name
// alone (a continued rollback), every target but those keep names: all of them before the rule is deleted
// ("Before you can delete the rule, you must remove all targets, using RemoveTargets", EventBridge).
func (p *Pack) dropTargets(ctx context.Context, req *spi.Request, rule string, keep []any, was prior) error {
	gave, listed2 := was.props["Targets"].([]any)                        // the targets themselves, when the previous properties are known in full
	if !was.gave("Targets") || was.props["Targets"] != nil && !listed2 { // none given, or a value refused before it gave any
		return nil
	}
	listed, err := p.do(ctx, req, "aws.events ListTargetsByRule Rule={rule} EventBusName={bus}", "", rule, nil)
	targets, _ := listed["Targets"].([]any)
	named := func(in []any, t any) bool {
		return slices.ContainsFunc(in, func(k any) bool { return str(asMap(k)["Id"]) == str(asMap(t)["Id"]) })
	}
	var ids []any
	for _, t := range targets {
		if !named(keep, t) && (!listed2 || named(gave, t)) {
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

// bucketOps are the bucket properties that map one to one onto an S3 operation: the one that sets each,
// and what clears it, an operation or, as JSON, the value S3 gives a new bucket ("all four Block Public
// Access settings enabled and ACLs disabled", S3 User Guide), which the first sets again. Versioning has
// none: "Once you version-enable a bucket, it can never return to an unversioned state. You can, however,
// suspend versioning on that bucket" (S3 User Guide), so an update that drops it suspends it.
var bucketOps = map[string][2]string{"VersioningConfiguration": {"PutBucketVersioning", ""}, "Tags": {"PutBucketTagging", "DeleteBucketTagging"},
	"PublicAccessBlockConfiguration": {"PutPublicAccessBlock", `{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":true,"RestrictPublicBuckets":true}`},
	"OwnershipControls":              {"PutBucketOwnershipControls", `{"Rules":[{"ObjectOwnership":"BucketOwnerEnforced"}]}`}}

// configureBucket applies the bucket properties that map one to one onto an S3 operation, and stores the
// rest as they are given, and clears each that the template it is updated from gave (was) and the template
// gives no longer.
func (p *Pack) configureBucket(ctx context.Context, req *spi.Request, bucket string, props map[string]any, was prior) error {
	dropped := func(property string) bool {
		_, given := props[property]
		return !given && was.gave(property)
	}
	for property, ops := range bucketOps {
		v, ok := props[property]
		op, in := ops[0], map[string]any{"Bucket": bucket, property: v}
		if property == "VersioningConfiguration" { // S3 reads Status and TagSet lifted to the top, as restXml hands them over (its liftedMembers debt)
			in["Status"] = asMap(v)["Status"]
		} else if property == "Tags" { // typed before it is lifted, as call types only the model's members
			tagging := asMap(input("aws.s3", op, map[string]any{"Tagging": map[string]any{"TagSet": v}})["Tagging"])
			in = map[string]any{"Bucket": bucket, "Tagging": tagging, "TagSet": tagging["TagSet"]}
		}
		if dropped(property) && strings.HasPrefix(ops[1], "{") {
			in[property] = spi.JSONObject([]byte(ops[1]))
		} else if dropped(property) && ops[1] != "" {
			op, in = ops[1], map[string]any{"Bucket": bucket}
		} else if dropped(property) { // versioning: suspended, where it is on
			if cur, _ := p.call(ctx, req, "aws.s3", "GetBucketVersioning", map[string]any{"Bucket": bucket}); cur["Status"] != "Enabled" {
				continue
			}
			in["Status"], in[property] = "Suspended", map[string]any{"Status": "Suspended"}
		} else if !ok {
			continue
		}
		if _, err := p.call(ctx, req, "aws.s3", op, in); err != nil {
			return err
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
		} else if dropped(property) {
			_ = p.col(req, "bktcfg").Delete(ctx, bucket+"/"+kind)
		}
	}
	return nil
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

func lastColon(s string) string { return s[strings.LastIndex(s, ":")+1:] }
