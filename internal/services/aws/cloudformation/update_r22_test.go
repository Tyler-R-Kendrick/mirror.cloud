package cloudformation

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

var rv22Topic = map[string]any{"Type": "AWS::SNS::Topic"}

func rv22Bad(name string, dependsOn any) map[string]any {
	return map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": dependsOn, "Properties": map[string]any{"QueueName": name, "FifoQueue": true}}
}

func (c ftxcol) Delete(k string) error {
	if c.pick("txdelete", c.name, k) {
		panic("rv22: the store panics")
	}
	return c.Tx.Delete(k)
}

func (c fcol) Txn(ctx context.Context, fn func(spi.Tx) error) error {
	c.at("coltxn", "")
	return c.Collection.Txn(ctx, fn)
}

// A record is *_IN_PROGRESS only while an operation of this process holds it: one left so by an operation
// that ended, here a stack's record written as another process left it, is taken over by the next
// operation on the stack, which settles it as that operation's failure and goes on from there. A create
// is ROLLBACK_FAILED, which UpdateStack refuses and DeleteStack deletes; an update is the status it found,
// which takes an update; a delete is DELETE_FAILED. REVIEW_IN_PROGRESS is no operation's. (Round 22: the
// restart.)
func TestCloudFormationTakesOverAStackItsOperationLeft(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	tpl := rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic}})
	for name, c := range map[string]struct {
		status, op string // as the process that ended left the record
		settled    string // as the next operation settles it
	}{
		"create":     {"CREATE_IN_PROGRESS", "/gone-1", "ROLLBACK_FAILED"},
		"create-old": {"CREATE_IN_PROGRESS", "", "ROLLBACK_FAILED"}, // a record from before operations had ids
		"update":     {"UPDATE_IN_PROGRESS", "CREATE_COMPLETE/gone-2", "CREATE_COMPLETE"},
		"update-old": {"UPDATE_IN_PROGRESS", "", "UPDATE_ROLLBACK_FAILED"},
		"delete":     {"DELETE_IN_PROGRESS", "/gone-3", "DELETE_FAILED"},
	} {
		rec := stack{Name: name, ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/" + name + "/1", Status: c.status, Template: tpl, Resources: []res{}, Outputs: []any{}, Op: c.op}
		if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, name, []byte(spi.JSONText(rec))); err != nil {
			t.Fatal(err)
		}
		if st, _, _ := h.status(name); st != c.status {
			t.Errorf("%s: a read shows %s; want %s, as the record is", name, st, c.status)
		}
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": name, "TemplateBody": tpl}); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Errorf("%s: CreateStack of its name answered %v; want already exists", name, err)
		}
		if st, reason, _ := h.status(name); st != c.settled || !strings.Contains(reason, "ended before it settled") || h.record(name)["Op"] != nil {
			t.Errorf("%s: the stack is %s (%q), held by %v; want %s, with why, held by none", name, st, reason, h.record(name)["Op"], c.settled)
		}
		refused := strings.HasSuffix(c.settled, "_FAILED") // as UpdateStack takes each state: UPDATE_ROLLBACK_FAILED takes a delete, no update
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": name, "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, "N": rv22Topic}})})
		if refused != (err != nil && strings.Contains(err.Error(), "is in "+c.settled+" state")) || !refused && err != nil {
			t.Errorf("%s: UpdateStack answered %v; want it refused: %v", name, err, refused)
		}
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": name}); err != nil {
			t.Errorf("%s: DeleteStack answered %v", name, err)
		}
		if _, held := h.stored(name); held {
			t.Errorf("%s: still stored after DeleteStack", name)
		}
	}
	review := stack{Name: "rv", ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/rv/1", Status: "REVIEW_IN_PROGRESS", Resources: []res{}, Outputs: []any{}}
	_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "rv", []byte(spi.JSONText(review)))
	_, _ = h.invoke("UpdateStack", map[string]any{"StackName": "rv", "TemplateBody": tpl})
	if st, _, _ := h.status("rv"); st != "REVIEW_IN_PROGRESS" {
		t.Errorf("a stack in review is %s after an UpdateStack; want it left REVIEW_IN_PROGRESS", st)
	}
}

// A stack a running operation holds is not taken over: while a create, an update or a delete is in
// flight, another operation on the stack is refused, or answered, as before, the record stays the
// operation's, and the operation completes. (Round 22: no stealing.)
func TestCloudFormationARunningOperationKeepsItsStack(t *testing.T) {
	t.Parallel()
	gated := map[string]any{"K": rv22Topic, "M": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "K", "Properties": map[string]any{"TopicName": "rv22held"}}}
	for name, c := range map[string]struct {
		op, during string // the operation in flight, and the status it holds the stack in
		update     string // what an UpdateStack during it answers
		after      string // the status the operation ends in ("": deleted)
	}{
		"create": {"CreateStack", "CREATE_IN_PROGRESS", "is in CREATE_IN_PROGRESS state", "CREATE_COMPLETE"},
		"update": {"UpdateStack", "UPDATE_IN_PROGRESS", "is in UPDATE_IN_PROGRESS state", "UPDATE_COMPLETE"},
		"delete": {"DeleteStack", "DELETE_IN_PROGRESS", "is in DELETE_IN_PROGRESS state", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if c.op != "CreateStack" {
				first := map[string]any{"K": rv22Topic}
				if c.op == "DeleteStack" {
					first = gated
				}
				if err := rvMake(t, h, "ro", map[string]any{"Resources": first}); err != nil {
					t.Fatal(err)
				}
			}
			reached, release := make(chan struct{}), make(chan struct{}) // the operation is held at its topic's write, or delete
			var first sync.Once
			if c.op == "DeleteStack" {
				off := faultStore(h, func(op, col, k string) bool {
					if op == "delete" && strings.Contains(k, "rv22held") {
						first.Do(func() { close(reached); <-release })
					}
					return false
				})
				defer off()
			} else {
				g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv22held") })
				reached, release = g.reached, g.release
			}
			in := map[string]any{"StackName": "ro"}
			if c.op != "DeleteStack" {
				in["TemplateBody"] = rv19Body(map[string]any{"Resources": gated})
			}
			done := make(chan error, 1)
			go func() { _, err := h.invoke(c.op, in); done <- err }()
			within(t, reached, "the operation reaching its topic")
			held := h.record("ro")["Op"]
			_, uerr := h.invoke("UpdateStack", map[string]any{"StackName": "ro", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic}})})
			_, cerr := h.invoke("CreateStack", map[string]any{"StackName": "ro", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic}})})
			if uerr == nil || !strings.Contains(uerr.Error(), c.update) || cerr == nil || !strings.Contains(cerr.Error(), "already exists") {
				t.Errorf("during the %s, UpdateStack answered %v and CreateStack %v; want %q and already exists", name, uerr, cerr, c.update)
			}
			if rec := h.record("ro"); rec["Status"] != c.during || rec["Op"] != held || held == nil {
				t.Errorf("during the %s the record is %v, held by %v; want %s, still held by %v", name, rec["Status"], rec["Op"], c.during, held)
			}
			close(release)
			if err := within(t, done, "the operation"); err != nil {
				t.Fatalf("the %s answered %v; want it completed", name, err)
			}
			if rec, _ := h.stored("ro"); str(rec["Status"]) != c.after || rec["Op"] != nil {
				t.Errorf("after the %s the record is %v, held by %v; want %q, held by none", name, rec["Status"], rec["Op"], c.after)
			}
		})
	}
}

// A stack whose operation could not write its record, for however long, is settled by the next operation
// once the store takes writes again, and DeleteStack then deletes it: the record's write panics n times
// from the operation's second write on. A failed create with a DeleteStack begun, whose rollback panics
// once, ends the same way, and what it made is deleted. (Round 22's B1 and B2.)
func TestCloudFormationSettlesAStackWhoseWritesFailed(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		op string
		n  int32
	}{{"CreateStack", 2}, {"CreateStack", 6}, {"UpdateStack", 4}, {"DeleteStack", 4}, {"DeleteStack", 1000}} {
		h := newStacks(t)
		if c.op != "CreateStack" {
			if err := rvMake(t, h, "ps", map[string]any{"Resources": map[string]any{"K": rv22Topic}}); err != nil {
				t.Fatal(err)
			}
		}
		var seen, fired atomic.Int32 // the reservation's write goes through; the n after it panic
		off := faultStore(h, func(o, col, k string) bool {
			return (o == "txput" || o == "txdelete") && col == "cfn" && k == "ps" && seen.Add(1) >= 2 && fired.Add(1) <= c.n
		})
		in := map[string]any{"StackName": "ps"}
		if c.op != "DeleteStack" {
			in["TemplateBody"] = rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, "N": rv22Topic}})
		}
		if err := invoked(t, h, c.op, in); !isInternalFailure(err) {
			t.Errorf("%s (%d failed writes) answered %v; want InternalFailure", c.op, c.n, err)
		}
		off()
		if err := invoked(t, h, "DeleteStack", map[string]any{"StackName": "ps"}); err != nil {
			t.Errorf("%s (%d failed writes): DeleteStack, the store healed, answered %v", c.op, c.n, err)
		}
		if rec, held := h.stored("ps"); held {
			t.Errorf("%s (%d failed writes): the stack is still stored, %v, after DeleteStack", c.op, c.n, rec["Status"])
		}
	}
	h := newStacks(t)
	g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv22m") })
	var armed atomic.Bool
	off := faultStore(h, once(func(op, col, k string) bool { return armed.Load() && op == "list" && col == "cfn" }))
	done := make(chan error, 1)
	go func() {
		done <- invoked(t, h, "CreateStack", map[string]any{"StackName": "s", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
			"M": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv22m"}}, "Z": rv22Bad("rv22-nofifo", "M")}})})
	}()
	within(t, g.reached, "the create making M")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	close(g.release)
	if err := within(t, done, "the create"); !isInternalFailure(err) {
		t.Errorf("the create whose rollback panicked answered %v; want InternalFailure", err)
	}
	off()
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Errorf("DeleteStack after the create answered %v", err)
	}
	if rec, held := h.stored("s"); held {
		t.Errorf("the stack is still stored, %v, after DeleteStack", rec["Status"])
	}
	if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": "arn:aws:sns:us-east-1:000000000000:rv22m"}); err == nil {
		t.Errorf("topic rv22m, which the create made, survives the stack")
	}
}

// A failed update whose cleanup panics keeps what it had yet to delete: the next operation settles the
// stack as the update found it, with why and with the topic the update made left on it (DELETE_FAILED,
// with a reason), and DeleteStack deletes the topic. The error the update answered is the stack's reason.
// (Round 22's S4 and N6.)
func TestCloudFormationAFailedUpdateWhoseCleanupPanicsKeepsWhatRemains(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "uo", map[string]any{"Resources": map[string]any{"K": rv22Topic}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, once(func(op, col, k string) bool { return op == "list" && col == "cfn" }))
	err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "uo", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"K": rv22Topic, "N": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv22orphan"}}, "Z": rv22Bad("rv22-nofifo2", "N")}})})
	off()
	if !isInternalFailure(err) {
		t.Fatalf("the update answered %v; want InternalFailure", err)
	}
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "uo", "TemplateBody": topicTpl()}); err == nil { // any operation settles it
		t.Fatal("CreateStack of the stack's name was taken")
	}
	rec := h.record("uo")
	if left := spi.JSONText(rec["Left"]); rec["Status"] != "CREATE_COMPLETE" || rec["Reason"] != err.Error() || !strings.Contains(left, "rv22orphan") || !strings.Contains(left, `"Status":"DELETE_FAILED"`) || !strings.Contains(left, `"Reason":"`) {
		t.Errorf("the stack is %v (%v), left %s; want CREATE_COMPLETE, the update's error, and topic rv22orphan left DELETE_FAILED with why", rec["Status"], rec["Reason"], left)
	}
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "uo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": "arn:aws:sns:us-east-1:000000000000:rv22orphan"}); err == nil {
		t.Errorf("topic rv22orphan, made by the failed update, survives the stack")
	}
}

// A stack takes back what its own failed update left: the update again, the bucket emptied, succeeds and
// holds the bucket, with nothing left; while the bucket could not be deleted and the update is not
// retried, it stays left. (Round 22's S5.)
func TestCloudFormationAStackTakesBackWhatItsUpdateLeft(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bucket := func(n string) map[string]any {
		return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": n}}
	}
	if err := rvMake(t, h, "tb", map[string]any{"Resources": map[string]any{"A": bucket("rv22a")}}); err != nil {
		t.Fatal(err)
	}
	g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv22mm") })
	update := func(bad bool) map[string]any {
		r := map[string]any{"A": bucket("rv22a"), "B": bucket("rv22b"), "M": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "B", "Properties": map[string]any{"TopicName": "rv22mm"}}}
		if bad {
			r["Z"] = rv22Bad("rv22-nofifo3", "M")
		}
		return map[string]any{"StackName": "tb", "TemplateBody": rv19Body(map[string]any{"Resources": r})}
	}
	done := make(chan error, 1)
	go func() { _, err := h.invoke("UpdateStack", update(true)); done <- err }()
	within(t, g.reached, "the update reaching M")
	h.fill("rv22b", true)
	close(g.release)
	if err := within(t, done, "the update"); err == nil || !strings.Contains(spi.JSONText(h.record("tb")["Left"]), "rv22b") {
		t.Fatalf("the update answered %v and left %v; want it failed, leaving bucket rv22b", err, h.record("tb")["Left"])
	}
	if rec := h.record("tb"); rec["Status"] != "CREATE_COMPLETE" || rec["Op"] != nil { // at once, by no later operation
		t.Errorf("the failed update left the stack %v, held by %v; want CREATE_COMPLETE, held by none", rec["Status"], rec["Op"])
	}
	h.fill("rv22b", false)
	if _, err := h.invoke("UpdateStack", update(false)); err != nil {
		t.Fatalf("the update again, of the bucket the stack left, answered %v", err)
	}
	rec := h.record("tb")
	if !strings.Contains(spi.JSONText(rec["Resources"]), `"Logical":"B","Name":"rv22b","Physical":"rv22b"`) || rec["Left"] != nil {
		t.Errorf("the stack holds %v and has left %v; want it to hold B (rv22b) with nothing left", rec["Resources"], rec["Left"])
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv22b"}); err != nil {
		t.Errorf("bucket rv22b, which the stack holds again, is gone: %v", err)
	}
	// a parameter left is taken back as one the stack holds is: put over, where a create of its name is refused
	if _, err := h.service("aws.ssm", "PutParameter", map[string]any{"Name": "rv22p", "Type": "String", "Value": "old"}); err != nil {
		t.Fatal(err)
	}
	left := stack{Name: "tp", ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/tp/1", Status: "CREATE_COMPLETE", Template: topicTpl(), Resources: []res{}, Outputs: []any{},
		Left: []res{{Logical: "P", Physical: "rv22p", Type: "AWS::SSM::Parameter", Name: "rv22p", Status: "DELETE_FAILED"}}}
	_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "tp", []byte(spi.JSONText(left)))
	if _, err := h.invoke("UpdateStack", map[string]any{"StackName": "tp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"P": map[string]any{"Type": "AWS::SSM::Parameter", "Properties": map[string]any{"Name": "rv22p", "Type": "String", "Value": "new"}}}})}); err != nil {
		t.Errorf("the update that declares the parameter the stack left answered %v", err)
	}
	if out, err := h.service("aws.ssm", "GetParameter", map[string]any{"Name": "rv22p"}); err != nil || asMap(out["Parameter"])["Value"] != "new" {
		t.Errorf("the parameter the stack took back is %v (%v); want its value new", out["Parameter"], err)
	}
}

// A panic in a failing operation's change-set bookkeeping, after an engine panic or a plain failure,
// does not escape the operation. (Round 22's N6.)
func TestCloudFormationNoPanicEscapesAFailingOperation(t *testing.T) {
	t.Parallel()
	for mode, resources := range map[string]map[string]any{
		"engine":  {"K": rv22Topic, "B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv22cors", "CorsConfiguration": map[string]any{"CorsRules": []any{}}}}},
		"failure": {"K": rv22Topic, "Z": rv22Bad("rv22-nofifo9", "K")},
	} {
		h := newStacks(t)
		if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "dbl", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateBody": rv19Body(map[string]any{"Resources": resources})}); err != nil {
			t.Fatal(err)
		}
		bookkeeping := once(func(op, col, k string) bool { return op == "coltxn" && col == "cfn-cs" })
		off := faultStore(h, func(op, col, k string) bool {
			return op == "put" && strings.HasSuffix(k, "rv22cors/cors") || bookkeeping(op, col, k)
		})
		err := invoked(t, h, "ExecuteChangeSet", map[string]any{"StackName": "dbl", "ChangeSetName": "c"})
		off()
		if err == nil {
			t.Errorf("%s: the execution was taken", mode)
		}
		if _, derr := h.invoke("DeleteStack", map[string]any{"StackName": "dbl"}); derr != nil {
			t.Errorf("%s: DeleteStack answered %v", mode, derr)
		}
	}
}

// A stack's resources are deleted newest first; a DeleteStack that deletes them but not what an update
// left fails, DELETE_FAILED with the error it answers as the stack's reason. (Round 22's N7.)
func TestCloudFormationDeletesNewestFirstAndFailsOnWhatWasLeft(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	named := func(n string, dependsOn any) map[string]any {
		return map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": dependsOn, "Properties": map[string]any{"TopicName": n}}
	}
	if err := rvMake(t, h, "nf", map[string]any{"Resources": map[string]any{"A": named("rv22first", nil), "B": named("rv22second", "A"), "C": named("rv22third", "B")}}); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	off := faultStore(h, func(op, col, k string) bool {
		if i := strings.Index(k, "rv22"); op == "delete" && col == "topics" && i >= 0 {
			mu.Lock()
			order = append(order, k[i:])
			mu.Unlock()
		}
		return false
	})
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "nf"}); err != nil {
		t.Fatal(err)
	}
	off()
	if !slices.Equal(order, []string{"rv22third", "rv22second", "rv22first"}) {
		t.Errorf("the topics were deleted in the order %v; want newest first", order)
	}
	full := stack{Name: "lf", ID: "arn:aws:cloudformation:us-east-1:000000000000:stack/lf/1", Status: "CREATE_COMPLETE", Template: topicTpl(), Resources: []res{}, Outputs: []any{},
		Left: []res{{Logical: "B", Physical: "rv22left", Type: "AWS::S3::Bucket", Name: "rv22left", Status: "DELETE_FAILED"}}}
	if _, err := h.service("aws.s3", "CreateBucket", map[string]any{"Bucket": "rv22left"}); err != nil {
		t.Fatal(err)
	}
	h.fill("rv22left", true)
	_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "lf", []byte(spi.JSONText(full)))
	_, err := h.invoke("DeleteStack", map[string]any{"StackName": "lf"})
	if f := (*spi.Fault)(nil); !errors.As(err, &f) || f.Code != "BucketNotEmpty" {
		t.Errorf("DeleteStack of a stack whose leftover bucket is full answered %v; want BucketNotEmpty", err)
	}
	if st, reason, _ := h.status("lf"); st != "DELETE_FAILED" || err == nil || reason != err.Error() {
		t.Errorf("the stack is %s (%q); want DELETE_FAILED, with the error DeleteStack answered", st, reason)
	}
}
