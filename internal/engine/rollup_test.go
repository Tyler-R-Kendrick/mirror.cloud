package engine_test

import (
	"testing"
)

// `rollup` builds the map-shaped answer to a list-shaped store -- Secrets
// Manager's VersionIdsToStages is a map keyed by a member of each element,
// which no CEL comprehension can produce. These hold what it has to do: one
// entry per element keyed by the named member, and nothing for an empty list.

func TestRollupKeysTheAnswerByAnElementMember(t *testing.T) {
	p := served(t, "aws.secretsmanager")
	created := invoke(t, p, "CreateSecret", map[string]any{"Name": "n", "SecretString": "v1"})
	v1, _ := created["VersionId"].(string)
	put := invoke(t, p, "PutSecretValue", map[string]any{"SecretId": "n", "SecretString": "v2"})
	v2, _ := put["VersionId"].(string)

	described := invoke(t, p, "DescribeSecret", map[string]any{"SecretId": "n"})
	stages, ok := described["VersionIdsToStages"].(map[string]any)
	if !ok {
		t.Fatalf("VersionIdsToStages is %#v, not a map", described["VersionIdsToStages"])
	}
	if len(stages) != 2 {
		t.Fatalf("%d entries for 2 versions: %v", len(stages), stages)
	}
	if got := asStrings(stages[v1]); len(got) != 1 || got[0] != "AWSPREVIOUS" {
		t.Errorf("v1 stages %v, want [AWSPREVIOUS]", stages[v1])
	}
	if got := asStrings(stages[v2]); len(got) != 1 || got[0] != "AWSCURRENT" {
		t.Errorf("v2 stages %v, want [AWSCURRENT]", stages[v2])
	}
}

func TestRollupOfAnEmptyListIsEmpty(t *testing.T) {
	p := served(t, "aws.secretsmanager")
	// A record carries no Versions until a write creates one, and nothing can
	// create one without a version -- so the empty case is an unknown name:
	// the listing of an absent secret's ids is empty, not an error.
	out := invoke(t, p, "ListSecretVersionIds", map[string]any{"SecretId": "absent"})
	if v, _ := out["Versions"].([]any); len(v) != 0 {
		t.Errorf("Versions for an unknown secret: %v", out["Versions"])
	}
}

func asStrings(v any) []string {
	list, _ := v.([]any)
	out := make([]string, 0, len(list))
	for _, e := range list {
		s, _ := e.(string)
		out = append(out, s)
	}
	return out
}
