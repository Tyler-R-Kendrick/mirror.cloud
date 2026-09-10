package states

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func FuzzJSONPath(f *testing.F) {
	for _, path := range []string{"$", "$.items.length()", "$.items.size()", "$..items.length()", "$.items.length($.items[0])", "$.items[0].keys()", "$.items.first()", "$.items.last()", "$.items.index(-1)", "$.items.concat(\",\", $.items[0].price)", "$.items.append($.items.first())", "$.items[*].price.sum()", "$.items.sum(1, $.items[*].price)", "$.items[*].price.stddev()", "$.items.length(1)", "$[?(@.price < 10)]", "$[?(@.name =~ /a # comment\n b/x)]", "$[?(@.name =~ /ä/iuU)]", `$[?(@.name =~ /\d/U)]`, `$['\u0069tems']`, `$.items[?(@.price == '\u0035')]`} {
		f.Add(path)
	}
	data := map[string]any{"items": []any{map[string]any{"price": 5.0}}}
	f.Fuzz(func(t *testing.T, path string) {
		if len(path) > 1024 {
			t.Skip()
		}
		if _, found := jsonPathLookup(data, path); found && path != "" && path[0] == '$' && !validJSONPath(path, false) {
			t.Fatalf("lookup accepted invalid path %q", path)
		}
	})
}

func FuzzPendingExecutionCommitOrder(f *testing.F) {
	for _, seed := range []struct {
		hasExecution, corrupt  bool
		status, executionToken string
	}{
		{false, false, "", ""},
		{true, false, "RUNNING", "pending"},
		{true, false, "RUNNING", "other"},
		{true, false, "SUCCEEDED", "pending"},
		{true, true, "", ""},
	} {
		f.Add(seed.hasExecution, seed.corrupt, seed.status, seed.executionToken)
	}
	f.Fuzz(func(t *testing.T, hasExecution, corrupt bool, status, executionToken string) {
		if len(status)+len(executionToken) > 1024 {
			t.Skip()
		}
		p := New(spitest.Deps(t))
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		req := &spi.Request{Identity: spi.Identity{Account: "1", Region: "us-east-1"}}
		const token = "pending"
		const executionARN = "arn:aws:states:us-east-1:1:execution:machine:run"
		if err := p.col(req, "pending").Put(ctx, token, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if hasExecution {
			body := []byte(`{`)
			if !corrupt {
				body, _ = json.Marshal(map[string]any{"status": status, "pendingToken": executionToken})
			}
			if err := p.col(req, "ex").Put(ctx, executionARN, body); err != nil {
				t.Fatal(err)
			}
		}
		_, ready := p.pendingExecution(ctx, req, token, executionARN)
		wantReady := hasExecution && !corrupt && status == "RUNNING" && executionToken == token
		if ready != wantReady {
			t.Fatalf("ready=%v, want %v", ready, wantReady)
		}
		_, pending, err := p.col(req, "pending").Get(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		wantPending := !hasExecution || !corrupt && status == "RUNNING"
		if pending != wantPending {
			t.Fatalf("pending=%v, want %v", pending, wantPending)
		}
	})
}
