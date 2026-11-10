package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// rget holds the first Get matching, after it has read, until released.
type rget struct {
	spi.Store
	armed            atomic.Bool
	held             atomic.Bool
	match            func(col, key string) bool
	reached, release chan struct{}
}
type rgetScope struct {
	spi.Scope
	g *rget
}
type rgetCol struct {
	spi.Collection
	g    *rget
	name string
}

func (g *rget) Scope(a, r string) spi.Scope            { return rgetScope{g.Store.Scope(a, r), g} }
func (s rgetScope) Collection(n string) spi.Collection { return rgetCol{s.Scope.Collection(n), s.g, n} }
func (c rgetCol) Get(ctx context.Context, key string) ([]byte, bool, error) {
	b, ok, err := c.Collection.Get(ctx, key)
	if c.g.armed.Load() && c.g.match(c.name, key) && c.g.held.CompareAndSwap(false, true) {
		close(c.g.reached)
		<-c.g.release
	}
	return b, ok, err
}

func (h *stacks) getGate(match func(col, key string) bool) *rget {
	g := &rget{Store: h.deps.Store, match: match, reached: make(chan struct{}), release: make(chan struct{})}
	h.deps.Store = g
	h.pack = New(h.deps)
	h.cfn = bundled.Handler("aws.cloudformation", h.deps)
	return g
}

func topicTpl(extra ...string) string {
	r := map[string]any{"T": map[string]any{"Type": "AWS::SNS::Topic"}}
	for _, e := range extra {
		r[e] = map[string]any{"Type": "AWS::SNS::Topic"}
	}
	b, _ := json.Marshal(map[string]any{"Resources": r})
	return string(b)
}

// UpdateStack loads the stack, a DeleteStack completes, and the update's
// reservation finds no record: does the update write the stack back?
func TestRV14UpdateVsDeleteResurrects(t *testing.T) {
	h := newStacks(t)
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
	if _, err := h.invoke("CreateStack", map[string]any{"StackName": "s", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		_, err := h.invoke("UpdateStack", map[string]any{"StackName": "s", "TemplateBody": topicTpl("U")})
		done <- err
	}()
	within(t, g.reached, "update's load")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "s"}); err != nil {
		t.Fatal(err)
	}
	if rec, ok := h.stored("s"); ok && rec["Status"] != "DELETE_COMPLETE" { // this branch keeps a deleted stack's record
		t.Fatal("stack still stored after DeleteStack")
	}
	close(g.release)
	err := within(t, done, "update")
	rec, ok := h.stored("s")
	t.Logf("update answered %v; record after: %v %v", err, ok, rec["Status"])
	if ok && rec["Status"] != "DELETE_COMPLETE" {
		t.Errorf("BUG: the deleted stack was written back by the racing UpdateStack: %v", rec)
	}
	if err == nil || !strings.Contains(err.Error(), "Stack [s] does not exist") {
		t.Errorf("BUG: the update of a stack deleted under it answered %v, want that it does not exist", err)
	}
}

// ExecuteChangeSet reads the change set, a DeleteChangeSet deletes it, and the
// claim then finds no record.
func TestRV14ExecuteVsDeleteChangeSetGhost(t *testing.T) {
	h := newStacks(t)
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
	changeSets(t, h, map[string]string{"a": topicTpl("U")})
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("PANIC: %v", r)
			}
		}()
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "execute's load")
	if _, err := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := within(t, done, "execute")
	raw, ok, _ := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn-cs").Get(h.ctx, "s:a")
	t.Logf("execute answered %v; record s:a after: %v %s", err, ok, raw)
	if code(err) != "ChangeSetNotFound" {
		t.Errorf("BUG: the execution of a change set deleted under it answered %v, want ChangeSetNotFound", err)
	}
	if err == nil {
		t.Errorf("BUG: a change set DeleteChangeSet deleted was executed anyway")
	}
	if ok {
		t.Errorf("BUG: the deleted change set's key holds a ghost record: %s", raw)
	}
	out, lerr := h.invoke("ListChangeSets", map[string]any{"StackName": "s"})
	t.Logf("ListChangeSets: %v %v", out, lerr)
}

// As above, but a change set of the same name is made again before the
// claim: which template is executed?
func TestRV14ExecuteVsRecreatedChangeSet(t *testing.T) {
	h := newStacks(t)
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "s" })
	changeSets(t, h, map[string]string{"a": topicTpl("OLD")})
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("PANIC: %v", r)
			}
		}()
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"})
		done <- err
	}()
	within(t, g.reached, "execute's load")
	if _, err := h.invoke("DeleteChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "s", "ChangeSetName": "a", "TemplateBody": topicTpl("NEW")}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := within(t, done, "execute")
	out, _ := h.invoke("DescribeStackResources", map[string]any{"StackName": "s"})
	got := spi.JSONText(out)
	d, _ := h.describe("a")
	t.Logf("execute answered %v; resources %s; new a %v", err, got, d["ExecutionStatus"])
	if strings.Contains(got, "OLD") {
		t.Errorf("BUG: the new change set a was marked %v but the deleted one's template (OLD) was executed", d["ExecutionStatus"])
	}
}

// ExecuteChangeSet of a CREATE change set; DeleteStack of the stack in review
// completes between the read of the change set and the claim.
func TestRV14ExecuteCreateVsDeleteStack(t *testing.T) {
	h := newStacks(t)
	g := h.getGate(func(col, key string) bool { return col == "cfn" && key == "r" })
	if _, err := h.invoke("CreateChangeSet", map[string]any{"StackName": "r", "ChangeSetName": "c", "ChangeSetType": "CREATE", "TemplateBody": topicTpl()}); err != nil {
		t.Fatal(err)
	}
	g.armed.Store(true)
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("PANIC: %v", r)
			}
		}()
		_, err := h.invoke("ExecuteChangeSet", map[string]any{"StackName": "r", "ChangeSetName": "c"})
		done <- err
	}()
	within(t, g.reached, "execute's load")
	if _, err := h.invoke("DeleteStack", map[string]any{"StackName": "r"}); err != nil {
		t.Fatal(err)
	}
	close(g.release)
	err := within(t, done, "execute")
	rec, ok := h.stored("r")
	t.Logf("execute answered %v; stack %v %v", err, ok, rec["Status"])
	if code(err) != "ChangeSetNotFound" {
		t.Errorf("BUG: the execution of a change set DeleteStack removed answered %v, want ChangeSetNotFound", err)
	}
	if err == nil || ok && rec["Status"] != "DELETE_COMPLETE" {
		t.Errorf("BUG: a CREATE change set whose stack (and itself) DeleteStack removed was executed: stack %v", rec["Status"])
	}
}
