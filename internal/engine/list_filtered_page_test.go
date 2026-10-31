package engine_test

import (
	"context"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

// A filtered list fetches a page from the store and then filters it, so a
// page can keep nothing while the store has more. Its NextToken has to move
// past what it read: answering the last item it kept -- none -- sent the
// caller back to the start, and a client following NextToken never finished.
// Secrets Manager's ListSecrets is the case: a secret scheduled for deletion
// sits between two listed ones.
func TestAFilteredPageThatKeepsNothingStillAdvances(t *testing.T) {
	p, err := bundled.New("aws.secretsmanager", spitest.Deps(t))
	if err != nil {
		t.Fatal(err)
	}
	call := func(op string, in map[string]any) map[string]any {
		t.Helper()
		resp, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.secretsmanager", Operation: op, Input: in,
			Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}})
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		return resp.Output
	}
	for _, name := range []string{"a", "b", "c"} {
		call("CreateSecret", map[string]any{"Name": name, "SecretString": "v"})
	}
	call("DeleteSecret", map[string]any{"SecretId": "b"})

	var names []any
	in := map[string]any{"MaxResults": 1}
	for page := 0; ; page++ {
		if page > 5 {
			t.Fatalf("still paging after %d pages, having listed %v", page, names)
		}
		out := call("ListSecrets", in)
		list, _ := out["SecretList"].([]any)
		for _, e := range list {
			names = append(names, e.(map[string]any)["Name"])
		}
		token, _ := out["NextToken"].(string)
		if token == "" {
			break
		}
		in = map[string]any{"MaxResults": 1, "NextToken": token}
	}
	if len(names) != 2 || names[0] != "a" || names[1] != "c" {
		t.Fatalf("listed %v, want a and c", names)
	}
}
