package cloudformation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func rv23Bucket(n string) map[string]any {
	return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": n}}
}

func rv23Topic(n string, dependsOn any) map[string]any {
	return map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": dependsOn, "Properties": map[string]any{"TopicName": n}}
}

// settledHoldNoOperation fails the test for any stack record of h's store that is settled, in no
// *_IN_PROGRESS status, and still carries an operation's id: newStacks checks it as every test ends.
func settledHoldNoOperation(t *testing.T, h *stacks) {
	defer func() { _ = recover() }() // a store a test left failing is read by no one
	kvs, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").List(context.Background(), "", "", 0)
	for _, kv := range kvs {
		if rec := spi.Decode[stack](kv.Value); rec.Op != "" && !strings.HasSuffix(rec.Status, "_IN_PROGRESS") {
			t.Errorf("stack %s ends %s still held by operation %s; want a settled record to carry none", kv.Key, rec.Status, rec.Op)
		}
	}
}

// A DeleteStack that failed leaves the stack DELETE_FAILED and no operation's, so the DeleteStack that
// tries again holds it under its own id: while it runs, a CreateStack, an UpdateStack, a CreateChangeSet
// or another DeleteStack does not take the stack over, and the delete completes. (Round 23's B1.)
func TestCloudFormationARetriedDeleteHoldsItsStack(t *testing.T) {
	t.Parallel()
	for _, during := range []string{"UpdateStack", "CreateStack", "CreateChangeSet", "DeleteStack"} {
		t.Run(during, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "sd", map[string]any{"Resources": map[string]any{"B": rv23Bucket("rv23sd"), "M": rv23Topic("rv23sdm", "B")}}); err != nil {
				t.Fatal(err)
			}
			h.fill("rv23sd", true)
			if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "sd"}); err == nil {
				t.Fatal("DeleteStack of a full bucket did not fail")
			}
			if rec := h.record("sd"); rec["Status"] != "DELETE_FAILED" || rec["Op"] != nil {
				t.Fatalf("the failed delete left the stack %v, held by %v; want DELETE_FAILED, held by none", rec["Status"], rec["Op"])
			}
			legacy := spi.Decode[stack]([]byte(spi.JSONText(h.record("sd")))) // and one a version before this left held is not reused
			legacy.Op = "/gone-9"
			_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "sd", []byte(spi.JSONText(legacy)))
			h.fill("rv23sd", false)
			reached, release := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			off := faultStore(h, func(op, col, k string) bool { // holds the retry at its bucket's delete
				if op == "delete" && strings.Contains(k, "rv23sd") {
					if first.CompareAndSwap(false, true) { // the first only: a second delete of it passes, never waits
						close(reached)
						<-release
					}
				}
				return false
			})
			defer off()
			done := make(chan error, 1)
			go func() { _, err := h.invoke("DeleteStack", map[string]any{"StackName": "sd"}); done <- err }()
			within(t, reached, "the retried delete reaching its bucket")
			held := h.record("sd")["Op"]
			_, err := h.invoke(during, map[string]any{"StackName": "sd", "ChangeSetName": "c", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic}})})
			if during != "DeleteStack" && err == nil {
				t.Errorf("%s during the retried delete was taken", during)
			}
			if rec := h.record("sd"); rec["Status"] != "DELETE_IN_PROGRESS" || rec["Op"] != held || held == nil || held == "/gone-9" {
				t.Errorf("during the retried delete, after %s (%v), the record is %v (%v), held by %v; want DELETE_IN_PROGRESS, still held by %v", during, err, rec["Status"], rec["Reason"], rec["Op"], held)
			}
			close(release)
			if err := within(t, done, "the retried delete"); err != nil {
				t.Errorf("the retried delete answered %v", err)
			}
			if rec, _ := h.stored("sd"); rec != nil && rec["Status"] != "DELETE_COMPLETE" { // this branch keeps a deleted stack's record
				t.Errorf("the stack is %v after its delete; want DELETE_COMPLETE", rec["Status"])
			}
		})
	}
}

// What a failed operation could not write is kept until the operation that takes the stack over has
// written it: a takeover whose own write fails loses nothing, and the next one deletes the topic the
// failed update made. The takeover writes the stack's termination protection as the record has it.
// (Round 23's S-1 and a nit.)
func TestCloudFormationWhatAnOperationLeftIsKeptUntilItIsWritten(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "uo", map[string]any{"Resources": map[string]any{"K": rv22Topic}}); err != nil {
		t.Fatal(err)
	}
	off := faultStore(h, once(func(op, col, k string) bool { return op == "list" && col == "cfn" }))
	if err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "uo", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"K": rv22Topic, "N": rv23Topic("rv23orphan", nil), "Z": rv22Bad("rv23-nofifo2", "N")}})}); !isInternalFailure(err) {
		t.Fatalf("the update answered %v; want InternalFailure", err)
	}
	off()
	if _, err := h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "uo", "EnableTerminationProtection": true}); err != nil {
		t.Fatal(err)
	}
	off = faultStore(h, func(op, col, k string) bool { return op == "txput" && col == "cfn" && k == "uo" })
	if err := invoked(t, h, "DeleteStack", map[string]any{"StackName": "uo"}); !isInternalFailure(err) {
		t.Errorf("DeleteStack, its takeover's write panicking, answered %v; want InternalFailure", err)
	}
	off()
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "uo"}); err == nil || !strings.Contains(err.Error(), "termination protection") {
		t.Errorf("DeleteStack of the stack taken over answered %v; want its termination protection, set while it was in progress, kept", err)
	}
	if rec := h.record("uo"); !strings.Contains(spi.JSONText(rec["Resources"]), "rv23orphan") { // listed with its resources on this branch
		t.Errorf("the stack taken over holds %v; want the topic its failed update made listed", rec["Resources"])
	}
	_, _ = h.invoke("UpdateTerminationProtection", map[string]any{"StackName": "uo", "EnableTerminationProtection": false})
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "uo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": "arn:aws:sns:us-east-1:000000000000:rv23orphan"}); err == nil {
		t.Errorf("topic rv23orphan, made by the failed update, survives the stack")
	}
}

// errStore answers an error, where faultStore panics, for the writes in a transaction and the reads pick
// picks.
type errStore struct {
	spi.Store
	pick func(op, collection, key string) bool
}
type errScope struct {
	spi.Scope
	pick func(op, collection, key string) bool
}
type errCol struct {
	spi.Collection
	name string
	pick func(op, collection, key string) bool
}
type errScopeTx struct {
	spi.ScopeTx
	pick func(op, collection, key string) bool
}
type errTx struct {
	spi.Tx
	name string
	pick func(op, collection, key string) bool
}

var errDown = errors.New("rv23: the store is unavailable")

func (s *errStore) Scope(a, r string) spi.Scope { return errScope{s.Store.Scope(a, r), s.pick} }
func (s errScope) Collection(n string) spi.Collection {
	return errCol{s.Scope.Collection(n), n, s.pick}
}
func (s errScope) Txn(ctx context.Context, fn func(spi.ScopeTx) error) error {
	return s.Scope.Txn(ctx, func(stx spi.ScopeTx) error { return fn(errScopeTx{stx, s.pick}) })
}
func (t errScopeTx) Collection(n string) spi.Tx { return errTx{t.ScopeTx.Collection(n), n, t.pick} }
func (c errTx) Put(k string, v []byte) error {
	if c.pick("txput", c.name, k) {
		return errDown
	}
	return c.Tx.Put(k, v)
}
func (c errCol) Get(ctx context.Context, k string) ([]byte, bool, error) {
	if c.pick("get", c.name, k) {
		return nil, false, errDown
	}
	return c.Collection.Get(ctx, k)
}

// A failed update whose record the store will not write, answering errors, and will or will not read, is
// not settled: what it could not delete is kept, and written by the takeover once the store answers, so
// the bucket is left on the stack and DeleteStack, the bucket emptied, deletes it. (Round 23's S-2.)
func TestCloudFormationAFailedUpdateTheStoreRefusesIsNotSettled(t *testing.T) {
	t.Parallel()
	for _, reads := range []bool{false, true} {
		h := newStacks(t)
		if err := rvMake(t, h, "se", map[string]any{"Resources": map[string]any{"A": rv23Bucket("rv23sea")}}); err != nil {
			t.Fatal(err)
		}
		var down atomic.Bool
		h.deps.Store = &errStore{Store: h.deps.Store, pick: func(op, col, k string) bool {
			return down.Load() && col == "cfn" && k == "se" && (op == "txput" || reads && op == "get")
		}}
		g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv23sem") })
		done := make(chan error, 1)
		go func() {
			_, err := h.invoke("UpdateStack", map[string]any{"StackName": "se", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
				"A": rv23Bucket("rv23sea"), "B": rv23Bucket("rv23seb"), "M": rv23Topic("rv23sem", "B"), "Z": rv22Bad("rv23-nofifo3", "M")}})})
			done <- err
		}()
		within(t, g.reached, "the update reaching M")
		h.fill("rv23seb", true)
		down.Store(true)
		close(g.release)
		if err := within(t, done, "the update"); err == nil {
			t.Fatal("the update did not fail")
		}
		_, _ = h.invoke("CreateStack", map[string]any{"StackName": "se", "TemplateBody": topicTpl()}) // a takeover the store refuses too
		down.Store(false)
		if _, err := h.invoke("CreateStack", map[string]any{"StackName": "se", "TemplateBody": topicTpl()}); err == nil { // any operation settles it
			t.Fatal("CreateStack of the stack's name was taken")
		}
		if rec := h.record("se"); rec["Status"] != "UPDATE_ROLLBACK_FAILED" || !strings.Contains(spi.JSONText(rec["Resources"]), "rv23seb") { // as a rollback that failed leaves it here
			t.Errorf("reads failing %v: the stack is %v and holds %v; want UPDATE_ROLLBACK_FAILED, with bucket rv23seb listed", reads, rec["Status"], rec["Resources"])
		}
		h.fill("rv23seb", false)
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "se"}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv23seb"}); err == nil {
			t.Errorf("reads failing %v: bucket rv23seb, made by the failed update, survives the stack", reads)
		}
	}
}

// An operation's id is this process's, not the seed's: one process's ids do not repeat a seed's value,
// and a record under an id this process holds something for, but of another stack (a restored
// snapshot's), is settled as its own stack, never as what was kept for the other. (Round 23's S-3.)
func TestCloudFormationAnOperationIdIsItsProcesss(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	id, release := h.pack.hold()
	release(nil)
	if seeded := h.deps.Rand.Derive("cloudformation-op").Hex(8); strings.Contains(id, seeded) {
		t.Errorf("operation id %s holds the seed's value %s; want a value of this process alone", id, seeded)
	}
	for _, n := range []string{"pa", "pb"} {
		if err := rvMake(t, h, n, map[string]any{"Resources": map[string]any{"K": rv23Topic("rv23-"+n, nil)}}); err != nil {
			t.Fatal(err)
		}
	}
	var writes atomic.Int32 // pb's update cannot settle: its record is kept under its id
	off := faultStore(h, func(o, col, k string) bool { return o == "txput" && col == "cfn" && k == "pb" && writes.Add(1) >= 2 })
	_ = invoked(t, h, "UpdateStack", map[string]any{"StackName": "pb", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv23Topic("rv23-pb", nil), "N": rv22Topic}})})
	off()
	rec := spi.Decode[stack]([]byte(spi.JSONText(h.record("pa")))) // pa, as a snapshot would hold it under pb's id
	rec.Status, rec.Op = "UPDATE_IN_PROGRESS", str(h.record("pb")["Op"])
	if rec.Op == "" {
		t.Fatal("pb's update left no operation id")
	}
	_ = h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "pa", []byte(spi.JSONText(rec)))
	_, _ = h.invoke("CreateChangeSet", map[string]any{"StackName": "pa", "ChangeSetName": "c", "TemplateBody": topicTpl()})
	if pa := h.record("pa"); pa["Name"] != "pa" || !strings.Contains(spi.JSONText(pa["Resources"]), "rv23-pa") || pa["Status"] != "UPDATE_ROLLBACK_FAILED" {
		t.Errorf("stack pa's record is now %v's, %v, holding %v; want it left pa's, and settled UPDATE_ROLLBACK_FAILED", pa["Name"], pa["Status"], pa["Resources"])
	}
	// pb's own next operation takes what was kept for it: the test leaves nothing in the process's registry,
	// which a later run in the same process would count.
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "pb"}); err != nil || h.record("pb")["Status"] != "DELETE_COMPLETE" {
		t.Errorf("deleting pb, whose update never settled, answered %v and left it %v", err, h.record("pb")["Status"])
	}
}

// Every operation that changes a stack takes over one left in progress, CreateChangeSet and
// ExecuteChangeSet among them; and an update's own record, as it reserved the stack, is settled as the
// status the update found: the record a running update wrote, put back after it ends as a process that
// ended would leave it. (Round 23's S-4.)
func TestCloudFormationChangeSetsTakeOverAndAnUpdateIsSettledAsItFoundTheStack(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "fs", map[string]any{"Resources": map[string]any{"K": rv22Topic}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "fs", "ChangeSetName": "c", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, "N": rv22Topic}})}); err != nil {
		t.Fatal(err)
	}
	g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv23fs") })
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "fs", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, "M": rv23Topic("rv23fs", "K")}})})
		done <- err
	}()
	within(t, g.reached, "the update reaching M")
	raw, _, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Get(h.ctx, "fs") // as the update holds it
	close(g.release)
	if err := within(t, done, "the update"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"CreateChangeSet", "ExecuteChangeSet"} {
		if err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Put(h.ctx, "fs", raw); err != nil {
			t.Fatal(err)
		}
		_, err := h.invoke(op, map[string]any{"StackName": "fs", "ChangeSetName": map[string]string{"CreateChangeSet": "d", "ExecuteChangeSet": "c"}[op], "TemplateBody": topicTpl()})
		if rec := h.record("fs"); err == nil || rec["Status"] != "UPDATE_ROLLBACK_FAILED" || rec["Op"] != nil { // settled first, as this branch settles an update: neither then takes it
			t.Errorf("%s of the stack an update left in progress answered %v, the stack %v, held by %v; want it taken over, UPDATE_ROLLBACK_FAILED", op, err, rec["Status"], rec["Op"])
		}
	}
}

// Operations of every kind on three stacks, from eight goroutines, over a store whose stack writes or
// lists panic one time in seven: no panic escapes, and once the store heals every stack can be deleted.
func TestCloudFormationOperationsRaceOverAFailingStore(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"none", "txput", "list", "both"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			var n atomic.Int64
			off := faultStore(h, func(o, col, k string) bool {
				return mode != "none" && (o == "txput" && mode != "list" || o == "list" && mode != "txput") && col == "cfn" && n.Add(1)%7 == 0
			})
			var wg sync.WaitGroup
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 40 {
						op := []string{"CreateStack", "UpdateStack", "DeleteStack", "CreateChangeSet", "UpdateStack"}[(g*3+i)%5]
						tpl := rv19Body(map[string]any{"Resources": map[string]any{"K": rv22Topic, fmt.Sprintf("N%d", i%2): rv22Topic}})
						func() {
							defer func() {
								if r := recover(); r != nil {
									t.Errorf("%s: a panic escaped: %v", op, r)
								}
							}()
							_, _ = bundled.Handler("aws.cloudformation", h.deps).Invoke(h.ctx, &spi.Request{Identity: h.id, Operation: op,
								Input: map[string]any{"StackName": fmt.Sprintf("st%d", (g+i)%3), "ChangeSetName": "c", "TemplateBody": tpl}})
						}()
					}
				}()
			}
			wg.Wait()
			off()
			for i := range 3 {
				name := fmt.Sprintf("st%d", i)
				_, err := h.invoke("DeleteStack", map[string]any{"StackName": name})
				if rec, _ := h.stored(name); rec != nil && rec["Status"] != "DELETE_COMPLETE" { // one whose delete failed is deleted by the next
					_, err = h.invoke("DeleteStack", map[string]any{"StackName": name})
				}
				if rec, _ := h.stored(name); rec != nil && rec["Status"] != "DELETE_COMPLETE" {
					t.Errorf("%s cannot be deleted, the store healed: %v leaves it %v (%v)", name, err, rec["Status"], rec["Reason"])
				}
			}
		})
	}
}
