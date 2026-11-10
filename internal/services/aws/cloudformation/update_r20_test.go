package cloudformation

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// panicky puts a store over h's that panics on a Put or Delete of a key match picks: a dependency failing
// unexpectedly. Turn it off with off.
func panicky(h *stacks, match func(key string) bool) (off func()) {
	var stop atomic.Bool
	h.deps.Store = &panicStore{Store: h.deps.Store, match: func(key string) bool { return !stop.Load() && match(key) }}
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	return func() { stop.Store(true) }
}

func (c panicCol) Delete(ctx context.Context, k string) error {
	if c.match(k) {
		panic("rv20: a dependency panics")
	}
	return c.Collection.Delete(ctx, k)
}

// isInternalFailure is whether err is the service's InternalFailure (500), which says nothing of the panic
// behind it: "The request processing has failed because of an unknown error, exception or failure"
// (Common Errors, API reference).
func isInternalFailure(err error) bool {
	f := (*spi.Fault)(nil)
	return errors.As(err, &f) && f.Code == "InternalFailure" && f.HTTPStatus == 500 && !strings.Contains(f.Message, "panic")
}

// A panic in a dependency during a failed operation's own put-back or rollback is a failure like any
// other: the update that cannot put its secret back still ends its reservation, saying which resource it
// could not put back; the create whose rollback cannot delete its topic ends ROLLBACK_FAILED. Neither says
// what panicked, on the wire or in StackStatusReason; each stack can be deleted after. (Round 20's B2.)
func TestCloudFormationAPanicInARollbackEndsTheOperation(t *testing.T) {
	t.Parallel()
	t.Run("update", func(t *testing.T) {
		t.Parallel()
		h := newStacks(t)
		secret := func(v string) map[string]any {
			return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": map[string]any{"Name": "rv20sec", "SecretString": v}}
		}
		if err := rvMake(t, h, "dp", map[string]any{"Resources": map[string]any{"A": secret("old")}}); err != nil {
			t.Fatal(err)
		}
		var boomed atomic.Bool // rv20boom panics, and after it every write of rv20sec: the put-back
		off := panicky(h, func(key string) bool {
			return strings.Contains(key, "rv20boom") && !boomed.Swap(true) || boomed.Load() && strings.Contains(key, "rv20sec")
		})
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "dp", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
			"A": secret("new"), "B": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "A", "Properties": map[string]any{"TopicName": "rv20boom"}}}})})
		off()
		if !isInternalFailure(err) {
			t.Errorf("the update answered %v; want InternalFailure (500), saying nothing of the panic", err)
		}
		if st, reason, _ := h.status("dp"); strings.HasSuffix(st, "_IN_PROGRESS") || !strings.Contains(reason, "[A]") || strings.Contains(reason, "panic") {
			t.Errorf("the stack is %s (%q); want its reservation ended, naming A, which was not put back, and not the panic", st, reason)
		}
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "dp"}); err != nil {
			t.Errorf("DeleteStack answered %v", err)
		}
	})
	t.Run("create", func(t *testing.T) {
		t.Parallel()
		h := newStacks(t)
		off := panicky(h, func(key string) bool { return strings.Contains(key, "rv20dt") })
		_, err := h.invoke("CreateStack", map[string]any{"StackName": "dc", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
			"T": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv20dt"}}}})})
		if !isInternalFailure(err) {
			t.Errorf("the create answered %v; want InternalFailure (500), saying nothing of the panic", err)
		}
		if st, reason, _ := h.status("dc"); st != "ROLLBACK_COMPLETE" && st != "ROLLBACK_FAILED" || strings.Contains(reason, "panic") {
			t.Errorf("the stack is %s (%q); want it rolled back, not saying what panicked", st, reason)
		}
		off()
		if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "dc"}); err != nil {
			t.Errorf("DeleteStack answered %v", err)
		}
		if st, _ := h.stored("dc"); st != nil && st["Status"] != "DELETE_COMPLETE" { // this branch keeps a deleted stack's record
			t.Errorf("stack dc is %v after DeleteStack; want DELETE_COMPLETE", st["Status"])
		}
	})
}

// A stack's rollback deletes only what no other stack's record holds. Stack x reads a topic's name as
// free; stack y then makes the topic, which SNS's CreateTopic "is idempotent" also answers to x; x then
// fails, and its rollback, of an update and of a create alike, leaves y's topic. (Round 20's S5 and S1.)
func TestCloudFormationARollbackLeavesWhatAnotherStackMade(t *testing.T) {
	t.Parallel()
	for _, update := range []bool{true, false} {
		t.Run(map[bool]string{true: "update", false: "create"}[update], func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if update {
				if err := rvMake(t, h, "x", map[string]any{"Resources": map[string]any{"K": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
					t.Fatal(err)
				}
			}
			g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv20shared") })
			done := make(chan error, 1)
			go func() {
				_, err := h.invoke(map[bool]string{true: "UpdateStack", false: "CreateStack"}[update], map[string]any{"StackName": "x", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
					"K": map[string]any{"Type": "AWS::SNS::Topic"},
					"T": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv20shared"}},
					"Z": map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "T", "Properties": map[string]any{"QueueName": "rv20-nofifo", "FifoQueue": true}}}})})
				done <- err
			}()
			within(t, g.reached, "x making the topic")
			if err := rvMake(t, h, "y", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic", "Properties": map[string]any{"TopicName": "rv20shared"}}}}); err != nil {
				t.Fatal(err)
			}
			close(g.release)
			if err := within(t, done, "x"); err == nil {
				t.Fatal("x did not fail")
			}
			arn := "arn:aws:sns:us-east-1:000000000000:rv20shared"
			if _, err := h.service("aws.sns", "GetTopicAttributes", map[string]any{"TopicArn": arn}); err != nil {
				t.Errorf("stack y's topic is gone after x's rollback: %v", err)
			}
		})
	}
}

// What a failed update could not delete stays the stack's to delete; on this branch the stack is
// UPDATE_ROLLBACK_FAILED and ContinueUpdateRollback deletes it. (While it waits, a logical id the update
// replaced is listed twice, the kept resource and the one left: a limitation here.)
// (Round 20's S2.)
func TestCloudFormationAnUpdateDeletesWhatAFailedOneLeft(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	bucket := func(n string) map[string]any {
		return map[string]any{"Type": "AWS::S3::Bucket", "Properties": map[string]any{"BucketName": n}}
	}
	if err := rvMake(t, h, "dl", map[string]any{"Resources": map[string]any{"A": bucket("rv20a")}}); err != nil {
		t.Fatal(err)
	}
	g := h.gateOn(func(_, key string, _ []byte) bool { return strings.Contains(key, "rv20m") })
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "dl", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
			"A": bucket("rv20b"), // a new name: a replacement
			"M": map[string]any{"Type": "AWS::SNS::Topic", "DependsOn": "A", "Properties": map[string]any{"TopicName": "rv20m"}},
			"Z": map[string]any{"Type": "AWS::SQS::Queue", "DependsOn": "M", "Properties": map[string]any{"QueueName": "rv20-nofifo2", "FifoQueue": true}}}})})
		done <- err
	}()
	within(t, g.reached, "the update reaching M")
	h.fill("rv20b", true) // S3 refuses to delete the replacement
	close(g.release)
	if err := within(t, done, "the update"); err == nil {
		t.Fatal("the update did not fail")
	}
	h.fill("rv20b", false)
	if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "dl"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv20b"}); err == nil {
		t.Errorf("bucket rv20b, which the failed update left, survives the next update")
	}
	if _, err := h.service("aws.s3", "HeadBucket", map[string]any{"Bucket": "rv20a"}); err != nil {
		t.Errorf("bucket rv20a, the stack's, is gone: %v", err)
	}
}

// staleStore answers the first n reads of a stack's record outside a transaction with a record that has
// changed since (a reason added): to the update that read it, the record changes under it every time.
type staleStore struct {
	spi.Store
	n atomic.Int32
}
type staleScope struct {
	spi.Scope
	s *staleStore
}
type staleCol struct {
	spi.Collection
	s *staleStore
}

func (s *staleStore) Scope(a, r string) spi.Scope       { return staleScope{s.Store.Scope(a, r), s} }
func (s staleScope) Collection(n string) spi.Collection { return staleCol{s.Scope.Collection(n), s.s} }
func (c staleCol) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, ok, err := c.Collection.Get(ctx, key)
	if m, isStack := spi.Decode[map[string]any](b)["Template"]; ok && isStack && m != nil && c.s.n.Add(-1) >= 0 {
		rec := spi.Decode[map[string]any](b)
		rec["Reason"] = "changed"
		return []byte(spi.JSONText(rec)), ok, err
	}
	return b, ok, err
}

// An update that finds its stack changed every time it reads it again gives up after a few reads, with
// the service's answer to a caller it will not serve now: Throttling, "Rate exceeded". (Round 20's NIT 3.)
func TestCloudFormationAnUpdateRereadsAStaleStackAFewTimes(t *testing.T) {
	t.Parallel()
	h := newStacks(t)
	if err := rvMake(t, h, "st", map[string]any{"Resources": map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}}); err != nil {
		t.Fatal(err)
	}
	s := &staleStore{Store: h.deps.Store}
	s.n.Store(10)
	h.deps.Store = s
	h.pack, h.cfn = New(h.deps), bundled.Handler("aws.cloudformation", h.deps)
	_, err := h.invoke("UpdateStack", map[string]any{"StackName": "st", "TemplateBody": topicTpl("U")})
	if f := (*spi.Fault)(nil); !errors.As(err, &f) || f.Code != "Throttling" || f.Message != "Rate exceeded" {
		t.Errorf("the update answered %v; want Throttling, Rate exceeded", err)
	}
	if st, _, _ := h.status("st"); st != "CREATE_COMPLETE" {
		t.Errorf("the stack is %s; want CREATE_COMPLETE, untouched", st)
	}
}
