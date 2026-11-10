package cloudformation

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// faultStore panics on the reads, writes, deletes and lists of h's store that pick picks, given the
// operation ("get", "put", "delete", "list", and "txput" for a put in a transaction), the collection and the
// key (a list's prefix): a store, or a dependency, failing unexpectedly. Turn it off with the func it answers.
func faultStore(h *stacks, pick func(op, collection, key string) bool) (off func()) {
	var stop atomic.Bool
	h.deps.Store = &fstore{Store: h.deps.Store, pick: func(op, c, k string) bool { return !stop.Load() && pick(op, c, k) }}
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	return func() { stop.Store(true) }
}

type fstore struct {
	spi.Store
	pick func(op, collection, key string) bool
}
type fscope struct {
	spi.Scope
	pick func(op, collection, key string) bool
}
type fcol struct {
	spi.Collection
	name string
	pick func(op, collection, key string) bool
}

type ftx struct {
	spi.ScopeTx
	pick func(op, collection, key string) bool
}
type ftxcol struct {
	spi.Tx
	name string
	pick func(op, collection, key string) bool
}

func (s fscope) Txn(ctx context.Context, fn func(spi.ScopeTx) error) error {
	return s.Scope.Txn(ctx, func(stx spi.ScopeTx) error { return fn(ftx{stx, s.pick}) })
}
func (t ftx) Collection(n string) spi.Tx { return ftxcol{t.ScopeTx.Collection(n), n, t.pick} }
func (c ftxcol) Put(k string, v []byte) error {
	if c.pick("txput", c.name, k) {
		panic("rv21: the store panics")
	}
	return c.Tx.Put(k, v)
}

func (s *fstore) Scope(a, r string) spi.Scope       { return fscope{s.Store.Scope(a, r), s.pick} }
func (s fscope) Collection(n string) spi.Collection { return fcol{s.Scope.Collection(n), n, s.pick} }
func (c fcol) at(op, key string) {
	if c.pick(op, c.name, key) {
		panic("rv21: the store panics")
	}
}
func (c fcol) Get(ctx context.Context, k string) ([]byte, bool, error) {
	c.at("get", k)
	return c.Collection.Get(ctx, k)
}
func (c fcol) Put(ctx context.Context, k string, v []byte) error {
	c.at("put", k)
	return c.Collection.Put(ctx, k, v)
}
func (c fcol) Delete(ctx context.Context, k string) error {
	c.at("delete", k)
	return c.Collection.Delete(ctx, k)
}
func (c fcol) List(ctx context.Context, prefix, after string, limit int) ([]spi.KV, bool, error) {
	c.at("list", prefix)
	return c.Collection.List(ctx, prefix, after, limit)
}

// once picks the first operation pick picks, and no other.
func once(pick func(op, collection, key string) bool) func(op, collection, key string) bool {
	var done atomic.Bool
	return func(op, c, k string) bool { return pick(op, c, k) && !done.Swap(true) }
}

// invoked is h.invoke, with a panic that escapes it as an error the test reports.
func invoked(t *testing.T, h *stacks, op string, in map[string]any) (err error) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s: a panic escaped: %v", op, r)
		}
	}()
	_, err = h.invoke(op, in)
	return err
}

// A panic of the engine's own, not a service's, fails the operation as InternalFailure, and the stack is
// never left in progress for good: a create's or an update's store write of a bucket configuration
// (configureBucket writes it itself), which the ordinary cleanup follows, deleting the bucket made before
// it; a store list in a failed create's or update's rollback, or in a DeleteStack's, after which the next
// operation on the stack settles it (as the refusal of an UpdateStack then names it) and DeleteStack
// deletes it; and an update's final write. (Round 21's BLOCKER 1; round 22's takeover, S3 and N7.)
func TestCloudFormationAnEnginePanicEndsTheOperation(t *testing.T) {
	t.Parallel()
	cors := func(op, c, k string) bool { return op == "put" && strings.HasSuffix(k, "rv21cors/cors") }
	holders := func(op, c, k string) bool { return op == "list" && c == "cfn" }
	bad := map[string]any{"Type": "AWS::SQS::Queue", "Properties": map[string]any{"QueueName": "rv21-nofifo", "FifoQueue": true}}
	bucket := map[string]any{"Type": "AWS::S3::Bucket", "DependsOn": "K", "Properties": map[string]any{"BucketName": "rv21cors", "CorsConfiguration": map[string]any{"CorsRules": []any{}}}}
	for name, c := range map[string]struct {
		op    string // CreateStack, UpdateStack or DeleteStack, of a stack holding a topic K
		pick  func(op, collection, key string) bool
		tpl   map[string]any
		now   string // the stack's status as the operation leaves it
		later string // and as the next operation settles it, when an UpdateStack's refusal names it
	}{
		"create":          {"CreateStack", cors, map[string]any{"B": bucket}, "ROLLBACK_COMPLETE", "ROLLBACK_COMPLETE"},
		"update":          {"UpdateStack", cors, map[string]any{"B": bucket}, "UPDATE_ROLLBACK_COMPLETE", ""},
		"create-rollback": {"CreateStack", once(holders), map[string]any{"Z": bad}, "CREATE_IN_PROGRESS", "ROLLBACK_FAILED"},
		"update-rollback": {"UpdateStack", once(holders), map[string]any{"N": map[string]any{"Type": "AWS::SNS::Topic"}, "Z": map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "N", "Properties": bad["Properties"]}}, "UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"},
		"delete":          {"DeleteStack", once(holders), nil, "DELETE_IN_PROGRESS", "DELETE_FAILED"},
		// the engine panics, and then its rollback does too
		"create-twice": {"CreateStack", func() func(op, c, k string) bool {
			var fired atomic.Bool // the rollback's first list of the stacks, once the write has panicked
			first := once(func(op, c, k string) bool { return fired.Load() && holders(op, c, k) })
			return func(op, c, k string) bool { return cors(op, c, k) && (fired.Swap(true) || true) || first(op, c, k) }
		}(), map[string]any{"B": bucket}, "CREATE_IN_PROGRESS", "ROLLBACK_FAILED"},
		// the update's final write panics once: the update fails, and puts the stack back
		"settle": {"UpdateStack", func() func(op, c, k string) bool {
			var n atomic.Int32
			return func(op, c, k string) bool { return op == "txput" && c == "cfn" && k == "ep" && n.Add(1) == 2 }
		}(), map[string]any{"N": map[string]any{"Type": "AWS::SNS::Topic"}}, "UPDATE_ROLLBACK_COMPLETE", ""},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			topic := map[string]any{"K": map[string]any{"Type": "AWS::SNS::Topic"}}
			if c.op != "CreateStack" {
				if err := rvMake(t, h, "ep", map[string]any{"Resources": topic}); err != nil {
					t.Fatal(err)
				}
			}
			off := faultStore(h, c.pick)
			in := map[string]any{"StackName": "ep"}
			if c.tpl != nil {
				resources := map[string]any{"K": topic["K"]}
				for k, v := range c.tpl {
					resources[k] = v
				}
				in["TemplateBody"] = rv19Body(map[string]any{"Resources": resources})
			}
			err := invoked(t, h, c.op, in)
			off()
			if !isInternalFailure(err) {
				t.Errorf("%s answered %v; want InternalFailure, saying nothing of the panic", c.op, err)
			}
			if st, reason, _ := h.status("ep"); st != c.now || strings.Contains(reason, "panic") {
				t.Errorf("the stack is %s (%q) after the panic; want %s, not saying what panicked", st, reason, c.now)
			}
			if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv21cors"}); err == nil && !strings.HasSuffix(c.now, "_IN_PROGRESS") {
				t.Errorf("bucket rv21cors, made before the panic, survives the operation's cleanup")
			}
			err = invoked(t, h, "UpdateStack", map[string]any{"StackName": "ep", "TemplateBody": rv19Body(map[string]any{"Resources": topic})})
			if st, reason, _ := h.status("ep"); c.later != "" && (err == nil || !strings.Contains(err.Error(), "is in "+c.later+" state") || st != c.later || !strings.Contains(reason, "InternalFailure") && !strings.Contains(reason, "ended before it settled")) {
				t.Errorf("the next operation answered %v and the stack is %s (%q); want it settled %s, with why", err, st, reason, c.later)
			} else if strings.HasSuffix(st, "_IN_PROGRESS") {
				t.Errorf("the stack is still %s after the next operation", st)
			}
			if err := invoked(t, h, "DeleteStack", map[string]any{"StackName": "ep"}); err != nil {
				t.Errorf("DeleteStack after the panic answered %v", err)
			}
			if rec, _ := h.stored("ep"); rec != nil && rec["Status"] != "DELETE_COMPLETE" { // this branch keeps a deleted stack's record
				t.Errorf("stack ep is %v after DeleteStack; want DELETE_COMPLETE", rec["Status"])
			}
			if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv21cors"}); err == nil {
				t.Errorf("bucket rv21cors survives the stack")
			}
		})
	}
}

// What a failed update left counts as the stack's: another stack cannot take it (here a Kinesis resource
// policy, whose id is its stream's ARN). On this branch the stack is UPDATE_ROLLBACK_FAILED and lists it
// with its resources, which holder reads. (Round 21's SHOULD 2.)
func TestCloudFormationWhatAFailedUpdateLeftIsTheStacks(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	arn := "arn:aws:kinesis:us-east-1:000000000000:stream/rv21str"
	policy := map[string]any{"Type": "AWS::Kinesis::ResourcePolicy", "Properties": map[string]any{"ResourceArn": arn,
		"ResourcePolicy": map[string]any{"Version": "2012-10-17", "Statement": []any{}}}}
	topic := map[string]any{"Type": "AWS::SNS::Topic"}
	if err := rvMake(t, h, "x", map[string]any{"Resources": map[string]any{"K": topic}}); err != nil {
		t.Fatal(err)
	}
	var puts atomic.Int32 // the policy's put goes through; its delete, in the rollback, panics
	off := faultStore(h, func(op, c, k string) bool { return strings.Contains(k, "rv21str") && op != "get" && puts.Add(1) > 1 })
	if err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "x", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"K": topic, "P": policy, "Z": map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "P", "Properties": map[string]any{"QueueName": "rv21-nofifo2", "FifoQueue": true}}}})}); err == nil {
		t.Fatal("the update did not fail")
	}
	off()
	if err := rvMake(t, h, "y", map[string]any{"Resources": map[string]any{"P": policy}}); err == nil || !strings.Contains(err.Error(), "AlreadyExistsException") {
		t.Errorf("stack y's policy on the stream stack x left answered %v; want AlreadyExistsException", err)
	}
}

// Only the owner's not-found says a name is free: a find that fails otherwise (here InternalFailure, a
// panic in S3) fails the create, which makes nothing. (Round 21's NIT 5.)
func TestCloudFormationAFindThatFailsTakesNoName(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	off := faultStore(h, once(func(op, c, k string) bool { return op == "get" && strings.Contains(k, "rv21find") }))
	err := invoked(t, h, "CreateStack", map[string]any{"StackName": "fd", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
		"B": map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": "rv21find"}}}})})
	off()
	if !isInternalFailure(err) {
		t.Errorf("the create answered %v; want InternalFailure, the find's", err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv21find"}); err == nil {
		t.Errorf("the create made bucket rv21find with its find failed")
	}
}

// An update rereads a stack that changed under it three times, and no more: three stale reads and then a
// fresh one is served; a fourth stale read is Throttling. (Round 21's NIT 6.)
func TestCloudFormationAnUpdateRereadsAStaleStackThreeTimes(t *testing.T) {
	t.Parallel()
	for stale, want := range map[int32]string{3: "", 4: "Throttling"} {
		h := newStacks(t)
		if err := rvMake(t, h, "st", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
			t.Fatal(err)
		}
		s := &staleStore{Store: h.deps.Store}
		s.n.Store(stale)
		h.deps.Store = s
		h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "st", "TemplateBody": topicTpl("U")})
		if want == "" && err != nil || want != "" && (err == nil || !strings.Contains(err.Error(), want)) {
			t.Errorf("after %d stale reads the update answered %v; want %q (none: served)", stale, err, want)
		}
	}
}
