package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// gate is a store that holds the first write of a bucket's configuration
// (configureBucket's "bktcfg" Put) until the test lets it go, so a test can
// act while a stack operation is in flight, at a point it chose.
type gate struct {
	spi.Store
	once             sync.Once
	reached, release chan struct{}
}

type gateScope struct {
	spi.Scope
	g *gate
}

type gateCollection struct {
	spi.Collection
	g *gate
}

func (g *gate) Scope(account, region string) spi.Scope {
	return gateScope{g.Store.Scope(account, region), g}
}

func (s gateScope) Collection(name string) spi.Collection {
	if name != "bktcfg" {
		return s.Scope.Collection(name)
	}
	return gateCollection{s.Scope.Collection(name), s.g}
}

func (c gateCollection) Put(ctx context.Context, key string, val []byte) error {
	c.g.once.Do(func() { close(c.g.reached); <-c.g.release })
	return c.Collection.Put(ctx, key, val)
}

// gated is stacks whose store holds the first bucket configuration written.
func gated(t *testing.T) (*stacks, *gate) {
	h := newStacks(t)
	g := &gate{Store: h.deps.Store, reached: make(chan struct{}), release: make(chan struct{})}
	h.deps.Store = g
	h.pack = New(h.deps)
	h.cfn = bundled.Handler("aws.cloudformation", h.deps)
	return h, g
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
	raw, ok, err := h.deps.Store.Scope(h.id.Account, h.id.Region).Collection("cfn").Get(h.ctx, name)
	if err != nil || !ok {
		h.t.Fatalf("no record for %s (%v)", name, err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
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
