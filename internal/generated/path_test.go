package generated

import "testing"

func TestServicePath(t *testing.T) {
	for _, test := range []struct {
		id, provider, pkg string
	}{
		{"aws.s3", "aws", "s3"},
		{"gcp.storage", "gcp", "storage"},
		{"aws.resourcegroupstaggingapi", "aws", "resourcegroupstaggingapi"},
		{"aws.lex-models", "aws", "lexmodels"},
		{"123-service", "unknown", "123service"},
		{"aws.---", "aws", "service"},
	} {
		provider, pkg := ServicePath(test.id)
		if provider != test.provider || pkg != test.pkg {
			t.Errorf("ServicePath(%q) = %q, %q; want %q, %q", test.id, provider, pkg, test.provider, test.pkg)
		}
	}
}
