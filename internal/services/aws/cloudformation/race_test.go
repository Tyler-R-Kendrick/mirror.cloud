package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// gate is a store that holds the first write match picks, until the test lets
// it go, so a test can act while an operation is in flight, at a point it
// chose. gated's picks a bucket's CORS configuration, which configureBucket
// stores itself (S3 holds a lock across its own "bktcfg" writes).
type gate struct {
	spi.Store
	match            func(collection, key string, val []byte) bool
	held             atomic.Bool
	reached, release chan struct{}
}

type gateScope struct {
	spi.Scope
	g *gate
}

type gateCollection struct {
	spi.Collection
	g    *gate
	name string
}

func (g *gate) Scope(account, region string) spi.Scope {
	return gateScope{g.Store.Scope(account, region), g}
}

func (s gateScope) Collection(name string) spi.Collection {
	return gateCollection{s.Scope.Collection(name), s.g, name}
}

func (c gateCollection) Put(ctx context.Context, key string, val []byte) error {
	if c.g.match(c.name, key, val) && c.g.held.CompareAndSwap(false, true) { // only the first: every later write goes through
		close(c.g.reached)
		<-c.g.release
	}
	return c.Collection.Put(ctx, key, val)
}

// gated is stacks whose store holds the first CORS configuration written.
func gated(t *testing.T) (*stacks, *gate) {
	h := newStacks(t)
	return h, h.gateOn(func(collection, key string, _ []byte) bool {
		return collection == "bktcfg" && strings.HasSuffix(key, "/cors")
	})
}

// gateOn puts a gate holding the first write match picks over h's store.
func (h *stacks) gateOn(match func(collection, key string, val []byte) bool) *gate {
	g := &gate{Store: h.deps.Store, match: match, reached: make(chan struct{}), release: make(chan struct{})}
	h.deps.Store = g
	h.pack = New(h.deps)
	h.cfn = bundled.Handler("aws.cloudformation", h.deps)
	return g
}

// within waits for c, failing the test only on a hang.
func within[T any](t *testing.T, c <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-c:
		return v
	case <-time.After(30 * time.Second):
		t.Fatalf("%s did not happen within 30s", what)
		panic("unreachable")
	}
}

// paused is a template of n SSM parameters P000... and, between P150 and
// P151, a bucket whose configuration the gate holds.
func paused(n int) map[string]map[string]any {
	resources := map[string]map[string]any{"P150b": {"Type": "AWS::S3::Bucket", "Properties": map[string]any{
		"CorsConfiguration": map[string]any{"CorsRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}}}}}
	for i := range n {
		resources[fmt.Sprintf("P%03d", i)] = map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Type": "String", "Value": "v"}}
	}
	return resources
}

// record answers the stack's record as the store holds it.
func (h *stacks) record(name string) map[string]any {
	h.t.Helper()
	m, ok := h.stored(name)
	if !ok {
		h.t.Fatalf("no record for %s", name)
	}
	return m
}

// stored answers the stack's record as the store holds it, and whether it holds one.
func (h *stacks) stored(name string) (map[string]any, bool) {
	raw, ok, err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Get(h.ctx, name)
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m, ok && err == nil
}

// The record a create reserves its name with, and every record written
// after it, lists its resources and outputs, empty or not: "Resources":[],
// "Outputs":[], never null, so a read of a stack mid-create, or of one with
// no outputs, iterates them. Before, the reservation held "Resources":null
// and "Outputs":null, and so did a stack made with no outputs.
func TestCloudFormationARecordHoldsNoNullList(t *testing.T) {
	h, g := gated(t)
	created := make(chan error, 1)
	go func() { created <- h.create("lists", paused(2)) }()
	within(t, g.reached, "the create reaching the bucket")
	for _, member := range []string{"Resources", "Outputs"} {
		if v, ok := h.record("lists")[member].([]any); !ok || len(v) != 0 {
			t.Errorf("mid-create the record's %s is %#v, want []", member, h.record("lists")[member])
		}
	}
	for _, op := range []string{"DescribeStackResources", "ListStackResources", "DescribeStacks"} {
		if _, err := h.invoke(op, map[string]any{"StackName": "lists"}); err != nil {
			t.Errorf("%s mid-create answered %v", op, err)
		}
	}
	close(g.release)
	if err := within(t, created, "the create"); err != nil {
		t.Fatal(err)
	}
	if v, ok := h.record("lists")["Outputs"].([]any); !ok || len(v) != 0 {
		t.Errorf("a stack made with no outputs records Outputs %#v, want []", h.record("lists")["Outputs"])
	}
	if !strings.Contains(fmt.Sprint(h.record("lists")["Resources"]), "P150b") {
		t.Errorf("the stack records %v, want its resources", h.record("lists")["Resources"])
	}
}

// params answers the SSM parameters whose names start with prefix.
func (h *stacks) params(prefix string) []string {
	h.t.Helper()
	var names []string
	for in := map[string]any{"MaxResults": 50}; ; {
		out, err := h.service("aws.ssm", "DescribeParameters", in)
		if err != nil {
			h.t.Fatalf("DescribeParameters: %v", err)
		}
		for _, p := range out["Parameters"].([]any) {
			if name := str(p.(map[string]any)["Name"]); strings.HasPrefix(name, prefix) {
				names = append(names, name)
			}
		}
		if in["NextToken"] = out["NextToken"]; str(in["NextToken"]) == "" {
			return names
		}
	}
}

// "Once the call completes successfully, stack deletion starts" (API
// reference, DeleteStack), and a stack being created is no exception: a
// DeleteStack of a CREATE_IN_PROGRESS stack answers at once and leaves it
// DELETE_IN_PROGRESS ("Ongoing removal of one or more stacks", User Guide,
// stack status codes), its name still taken (a second CreateStack is
// AlreadyExistsException) and no update or change set accepted ("...is in
// DELETE_IN_PROGRESS state and can not be updated."). The create, finding at
// its last write that its reservation is no longer CREATE_IN_PROGRESS,
// deletes everything it made and then the stack: no parameter is left, and
// the name is free for a new stack. A second DeleteStack while the first runs
// is answered and changes nothing. Before (the round-8 repro), DeleteStack
// unwound the empty reservation and removed it, a second CreateStack made a
// second set of 300 parameters, the first create's last write brought its
// stack back CREATE_COMPLETE over the second's record, and a final delete
// left 300 parameters behind.
func TestCloudFormationADeleteDuringACreateEndsIt(t *testing.T) {
	h, g := gated(t)
	created := make(chan error, 1)
	go func() { created <- h.create("race", paused(300)) }()
	within(t, g.reached, "the create reaching its bucket")
	if status, _, _ := h.status("race"); status != "CREATE_IN_PROGRESS" {
		t.Fatalf("mid-create race is %s, want CREATE_IN_PROGRESS", status)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "race"}); err != nil {
		t.Fatalf("DeleteStack mid-create answered %v", err)
	}
	if status, _, _ := h.status("race"); status != "DELETE_IN_PROGRESS" {
		t.Errorf("after DeleteStack, race is %s, want DELETE_IN_PROGRESS", status)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "race"}); err != nil {
		t.Errorf("a second DeleteStack answered %v", err)
	}
	if err := h.create("race", paused(300)); code(err) != "AlreadyExistsException" {
		t.Errorf("CreateStack of a name being deleted answered %v, want AlreadyExistsException", err)
	}
	tpl, _ := json.Marshal(map[string]any{"Resources": paused(1)})
	for _, op := range []string{"UpdateStack", "CreateChangeSet"} {
		_, err := h.invoke(op, map[string]any{"StackName": "race", "ChangeSetName": "cs", "TemplateBody": string(tpl)})
		if code(err) != "ValidationError" || !strings.Contains(err.Error(), "is in DELETE_IN_PROGRESS state and can not be updated.") {
			t.Errorf("%s of a stack being deleted answered %v, want ValidationError: DELETE_IN_PROGRESS", op, err)
		}
	}
	close(g.release)
	if err := within(t, created, "the create"); err != nil {
		t.Errorf("the deleted create answered %v, want its StackId: it was started", err)
	}
	if _, _, err := h.status("race"); code(err) != "ValidationError" { // DescribeStacks of a stack that does not exist
		t.Errorf("after the create, DescribeStacks of race answered %v, want ValidationError: it does not exist", err)
	}
	if left := h.params("race-"); len(left) != 0 {
		t.Errorf("%d parameters were left, the first %s", len(left), left[0])
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": strings.ToLower("race-P150b-")}); err == nil {
		t.Error("a bucket was left")
	}
	h.mustCreate("race", paused(3))
	if status, _, _ := h.status("race"); status != "CREATE_COMPLETE" {
		t.Errorf("a new race is %s, want CREATE_COMPLETE", status)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "race"}); err != nil {
		t.Fatal(err)
	}
	if left := h.params("race-"); len(left) != 0 {
		t.Errorf("deleting the new stack left %v", left)
	}
}

// A stack in any _IN_PROGRESS state takes no update: UpdateStack and the
// change sets made for it are ValidationError, "Stack:<id> is in
// CREATE_IN_PROGRESS state and can not be updated.", the words CloudFormation
// answers a ROLLBACK_COMPLETE stack with, and the change set being executed
// is InvalidChangeSetStatus, "the change set status might be
// CREATE_IN_PROGRESS, or the stack status might be UPDATE_IN_PROGRESS" (API
// reference, ExecuteChangeSet). The create, here a CREATE change set's, goes
// on and makes exactly its own template, which GetTemplate and
// GetTemplateSummary read mid-create. Before, UpdateStack of a stack being
// created went ahead beside the create, and the reservation held no template.
func TestCloudFormationNothingUpdatesAStackBeingCreated(t *testing.T) {
	h, g := gated(t)
	other, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Other": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "other", "Type": "String", "Value": "v"}}}})
	busy, _ := json.Marshal(map[string]any{"Resources": paused(2)})
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "busy", "ChangeSetName": "early", "ChangeSetType": "CREATE", "TemplateBody": string(busy)}); err != nil {
		t.Fatal(err)
	}
	created := make(chan error, 1)
	go func() {
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "busy", "ChangeSetName": "early"})
		created <- err
	}()
	within(t, g.reached, "the create reaching its bucket")
	for _, op := range []string{"GetTemplate", "GetTemplateSummary"} { // the reservation holds the template
		if out, err := h.invoke(op, map[string]any{"StackName": "busy"}); err != nil || !strings.Contains(fmt.Sprint(out), "AWS::S3::Bucket") {
			t.Errorf("%s mid-create answered %v, %v, want the create's template", op, out, err)
		}
	}
	for _, c := range []struct{ op, changeSet string }{{"UpdateStack", ""}, {"CreateChangeSet", "late"}} {
		_, err := h.invoke(c.op, map[string]any{"StackName": "busy", "ChangeSetName": c.changeSet, "TemplateBody": string(other)})
		if code(err) != "ValidationError" || !strings.Contains(err.Error(), "is in CREATE_IN_PROGRESS state and can not be updated.") {
			t.Errorf("%s of a stack being created answered %v, want ValidationError: CREATE_IN_PROGRESS", c.op, err)
		}
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "busy", "ChangeSetName": "early"}); code(err) != "InvalidChangeSetStatus" {
		t.Errorf("ExecuteChangeSet of the change set being executed answered %v, want InvalidChangeSetStatus", err)
	}
	close(g.release)
	if err := within(t, created, "the create"); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := h.status("busy"); status != "CREATE_COMPLETE" {
		t.Errorf("busy is %s, want CREATE_COMPLETE", status)
	}
	if got := len(h.resources("busy")); got != 3 {
		t.Errorf("busy lists %d resources, want its template's 3", got)
	}
	if _, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "other"}); err == nil {
		t.Error("the refused update made its parameter")
	}
}

// An update writes its record only over the record it read (termination
// protection aside). One that finds the stack changed under it, by another
// update or a DeleteStack, deletes what it made that the stack now does not
// hold and is ValidationError, so nothing it made outlives it. Before, the
// later of two updates wrote its record over the earlier's, whose resources
// then belonged to no stack, and an update during a DeleteStack brought the
// deleted stack back.
func TestCloudFormationAnUpdateThatLosesUndoesItself(t *testing.T) {
	for _, rival := range []string{"UpdateStack", "DeleteStack"} {
		t.Run(rival, func(t *testing.T) {
			h, g := gated(t)
			named := func(n string) map[string]any { // a queue, which an update makes again as it was
				return map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": n}}
			}
			h.mustCreate("u", map[string]map[string]any{"A": named("u-a")})
			first := paused(0)
			first["A"], first["N1"] = named("u-a"), named("u-n1")
			body, _ := json.Marshal(map[string]any{"Resources": first})
			updated := make(chan error, 1)
			go func() {
				_, err := h.invoke("UpdateStack", map[string]any{"StackName": "u", "TemplateBody": string(body)})
				updated <- err
			}()
			within(t, g.reached, "the update reaching its bucket")
			second, _ := json.Marshal(map[string]any{"Resources": map[string]any{"A": named("u-a"), "N2": named("u-n2")}})
			if _, err := h.invoke(rival, map[string]any{"StackName": "u", "TemplateBody": string(second)}); err != nil {
				t.Fatalf("%s during the update answered %v", rival, err)
			}
			close(g.release)
			if err := within(t, updated, "the update"); code(err) != "ValidationError" {
				t.Errorf("the update that lost answered %v, want ValidationError", err)
			}
			want := map[string][]string{"UpdateStack": {"u-a", "u-n2"}, "DeleteStack": nil}[rival]
			out, err := h.service("aws.sqs", "ListQueues", map[string]any{"QueueNamePrefix": "u-"})
			var left []string
			for _, url := range spi.Slice(out["QueueUrls"]) {
				left = append(left, spi.LastSlash(str(url)))
			}
			if slices.Sort(left); err != nil || strings.Join(left, ",") != strings.Join(want, ",") {
				t.Errorf("after %s won, the queues are %v (%v), want %v", rival, left, err, want)
			}
			if _, err := h.service("aws.s3", "ListBuckets", map[string]any{}); err != nil {
				t.Fatal(err)
			} else if out, _ := h.service("aws.s3", "ListBuckets", map[string]any{}); strings.Contains(fmt.Sprint(out), "u-P150b") {
				t.Errorf("the losing update's bucket was left: %v", out)
			}
		})
	}
}

// Termination protection is the stack's until UpdateTerminationProtection
// changes it ("you can't delete the stack while termination protection is
// enabled", User Guide, Protect a stack from being deleted): an UpdateStack,
// and an ExecuteChangeSet, keep it, so DeleteStack is still refused and the
// stack still stands. Before, UpdateStack wrote a record without it, and a
// DeleteStack after the update deleted the stack.
func TestCloudFormationAnUpdateKeepsTerminationProtection(t *testing.T) {
	h := newStacks(t)
	queue := func(n string) map[string]map[string]any {
		return map[string]map[string]any{"Q": {"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": n}}}
	}
	h.mustCreate("kept", queue("kept-a"))
	if _, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "kept", "EnableTerminationProtection": true}); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]any{"Resources": queue("kept-b")})
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "kept", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "kept", "ChangeSetName": "cs", "TemplateBody": string(body)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "kept", "ChangeSetName": "cs"}); err != nil {
		t.Fatal(err)
	}
	if protect, _ := h.record("kept")["Protect"].(bool); !protect {
		t.Errorf("after the updates the record is %v, want Protect", h.record("kept"))
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "kept"}); code(err) != "ValidationError" {
		t.Errorf("DeleteStack of the protected stack answered %v, want ValidationError", err)
	}
	if status, _, err := h.status("kept"); err != nil || status != "UPDATE_COMPLETE" {
		t.Errorf("kept is %s (%v), want UPDATE_COMPLETE", status, err)
	}
}

// RollbackStack accepts a stack in CREATE_FAILED or UPDATE_FAILED and
// CancelUpdateStack one in UPDATE_IN_PROGRESS (API reference), and here no
// stack rests in any of them: a failed create rolls back at once and an
// update reserves no UPDATE_IN_PROGRESS record. Neither can carry out what it
// says, so a record in one of those states (planted, as no operation makes
// one) is refused, ValidationError, and left as it was, never answered with a
// success that changes nothing. Before, RollbackStack of CREATE_FAILED and
// UPDATE_FAILED and CancelUpdateStack of UPDATE_IN_PROGRESS answered success
// and did nothing.
func TestCloudFormationTheRollbackFamilyNeverSucceedsSilently(t *testing.T) {
	h := newStacks(t)
	cfn := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn")
	for _, status := range []string{"CREATE_FAILED", "UPDATE_FAILED", "UPDATE_IN_PROGRESS"} {
		raw, _ := json.Marshal(stack{Name: "planted", ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/planted/1", Status: status, Resources: []res{}, Outputs: []any{}})
		if err := cfn.Put(h.ctx, "planted", raw); err != nil {
			t.Fatal(err)
		}
		for _, op := range []string{"RollbackStack", "CancelUpdateStack"} {
			if _, err := h.invoke(op, map[string]any{"StackName": "planted"}); code(err) != "ValidationError" {
				t.Errorf("%s of a %s stack answered %v, want ValidationError", op, status, err)
			}
			if got, _, _ := h.status("planted"); got != status {
				t.Errorf("%s left the %s stack %s", op, status, got)
			}
		}
	}
}

// UpdateTerminationProtection's EnableTerminationProtection is required and a
// Boolean (the model: UpdateTerminationProtectionInput's member, smithy.api#
// required, targets the Boolean EnableTerminationProtection): a request
// without it is ValidationError naming it, as the bundle answered before the
// operation was native, and one that is no Boolean, "maybe", is
// ValidationError too. Neither changes the protection. Before, the native
// operation read a missing member as false, so {"StackName": "tp"} turned the
// protection off, and "maybe" did the same.
func TestCloudFormationTerminationProtectionNeedsItsBoolean(t *testing.T) {
	h := newStacks(t)
	h.mustCreate("tp", map[string]map[string]any{"Q": nameable()["Queue"]})
	if _, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "tp", "EnableTerminationProtection": true}); err != nil {
		t.Fatal(err)
	}
	for _, in := range []map[string]any{{"StackName": "tp"}, {"StackName": "tp", "EnableTerminationProtection": "maybe"}, {"StackName": "tp", "EnableTerminationProtection": nil}} {
		_, err := h.invoke("UpdateTerminationProtection", in)
		if missing := in["EnableTerminationProtection"] == nil; code(err) != "ValidationError" || missing && err.Error() != "ValidationError: EnableTerminationProtection" || !strings.Contains(err.Error(), "EnableTerminationProtection") {
			t.Errorf("UpdateTerminationProtection %v answered %v, want ValidationError naming EnableTerminationProtection", in, err)
		}
		if h.record("tp")["Protect"] != true {
			t.Errorf("after UpdateTerminationProtection %v the stack is unprotected", in)
		}
	}
}

// UpdateTerminationProtection writes the stack record in the same store
// transaction as the engine's writes, so a create that settles while it runs
// keeps its CREATE_COMPLETE and the protection both: here the protection is
// asked for mid-create, its write held (when it is the engine's Get then Put)
// while the create finishes, then let go. The stack ends CREATE_COMPLETE and
// protected, so DeleteStack is refused; EnableTerminationProtection's
// Boolean reads TRUE as coerce does. ContinueUpdateRollback writes nothing.
// Before, the held Put wrote the CREATE_IN_PROGRESS record it had read back
// over the finished stack, which then stayed CREATE_IN_PROGRESS.
func TestCloudFormationTerminationProtectionWritesWithTheEngine(t *testing.T) {
	h, create := gated(t)
	protect := h.gateOn(func(collection, key string, val []byte) bool {
		return collection == "cfn" && strings.Contains(string(val), `"Protect":true`)
	})
	created, protected := make(chan error, 1), make(chan error, 1)
	go func() { created <- h.create("tp", paused(1)) }()
	within(t, create.reached, "the create reaching its bucket")
	go func() {
		_, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "tp", "EnableTerminationProtection": true})
		protected <- err
	}()
	select {
	case <-protect.reached: // the bundle's write, held
	case err := <-protected:
		protected <- err
	case <-time.After(30 * time.Second):
		t.Fatal("UpdateTerminationProtection did not answer or write within 30s")
	}
	close(create.release)
	if err := within(t, created, "the create"); err != nil {
		t.Fatal(err)
	}
	close(protect.release)
	if err := within(t, protected, "UpdateTerminationProtection"); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := h.status("tp"); status != "CREATE_COMPLETE" {
		t.Errorf("tp is %s, want CREATE_COMPLETE", status)
	}
	if protect, _ := h.record("tp")["Protect"].(bool); !protect {
		t.Errorf("tp is not protected: %v", h.record("tp"))
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "tp"}); code(err) != "ValidationError" {
		t.Errorf("DeleteStack of the protected stack answered %v", err)
	}
	for _, text := range []string{"False", "TRUE"} { // a Boolean in any case
		if _, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "tp", "EnableTerminationProtection": text}); err != nil {
			t.Fatal(err)
		}
	}
	if protect, _ := h.record("tp")["Protect"].(bool); !protect {
		t.Error("EnableTerminationProtection TRUE left tp unprotected")
	}
}

// A change set is refused for the stacks UpdateStack and ExecuteChangeSet
// refuse: one ROLLBACK_* or DELETE_FAILED, as well as one in progress, each
// "Stack:<id> is in <status> state and can not be updated.". Before,
// CreateChangeSet refused only a stack in progress.
func TestCloudFormationNoChangeSetForAStackThatTakesNoUpdate(t *testing.T) {
	h := newStacks(t)
	if err := h.create("rolled", map[string]map[string]any{"Q": nameable()["Queue"], "ZZZ": {"Type": "AWS::Nope::Thing"}}); err == nil {
		t.Fatal("a template with an unsupported resource type was created")
	}
	tpl, _ := json.Marshal(map[string]any{"Resources": map[string]any{"Q": nameable()["Queue"]}})
	for _, op := range []string{"UpdateStack", "CreateChangeSet"} {
		_, err := h.invoke(op, map[string]any{"StackName": "rolled", "ChangeSetName": "cs", "TemplateBody": string(tpl)})
		if code(err) != "ValidationError" || !strings.Contains(err.Error(), "is in ROLLBACK_COMPLETE state and can not be updated.") {
			t.Errorf("%s of a ROLLBACK_COMPLETE stack answered %v, want ValidationError: ROLLBACK_COMPLETE", op, err)
		}
	}
}

// GetTemplateSummary reads the stored template only when the request gives
// none: a TemplateBody over 51,200 bytes beside a StackName is the member's
// constraint error, as on every operation. Before, the stored template was
// summarised instead.
func TestCloudFormationSummarisesNoStoredTemplateForAnOversizeBody(t *testing.T) {
	h := newStacks(t)
	h.mustCreate("sum", map[string]map[string]any{"Q": nameable()["Queue"]})
	_, err := h.invoke("GetTemplateSummary", map[string]any{"StackName": "sum", "TemplateBody": padded(51_201, false)})
	if code(err) != "ValidationError" || !strings.Contains(err.Error(), "51200") {
		t.Errorf("GetTemplateSummary of an oversize body answered %v, want the 51200 constraint", err)
	}
	if _, err := h.invoke("GetTemplateSummary", map[string]any{"StackName": "sum"}); err != nil {
		t.Errorf("GetTemplateSummary of the stored template answered %v", err)
	}
}
