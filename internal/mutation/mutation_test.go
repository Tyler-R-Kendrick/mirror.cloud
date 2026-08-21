package mutation

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestMutantsAreKilled is a tiny in-tree mutation suite. Each mutant
// rewrites one production token via `go test -overlay` and must make
// the targeted tests fail. A surviving mutant means the tests are blind.
func TestMutantsAreKilled(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	type mutant struct {
		name string
		file string
		old  string
		new  string
		pkg  string
		run  string
	}
	mutants := []mutant{
		{
			name: "identity-expiry-after-to-before",
			file: filepath.Join("internal", "identity", "identity.go"),
			old:  "now.UTC().After(t.Add(time.Duration(secs) * time.Second))",
			new:  "now.UTC().Before(t.Add(time.Duration(secs) * time.Second))",
			pkg:  "./internal/identity",
			run:  "TestParseAndExpiry",
		},
		{
			name: "identity-region-segment",
			file: filepath.Join("internal", "identity", "identity.go"),
			old:  "region = parts[2]",
			new:  "region = parts[1]",
			pkg:  "./internal/identity",
			run:  "TestParseAndExpiry",
		},
		{
			name: "expr-and-to-or",
			file: filepath.Join("internal", "services", "aws", "dynamodb", "expr", "expr.go"),
			old:  `case "AND":`,
			new:  `case "AND_MUTATED":`,
			pkg:  "./internal/services/aws/dynamodb/expr",
			run:  "TestANDNotOR|TestEvalBool",
		},
		{
			name: "expr-eq-to-neq",
			file: filepath.Join("internal", "services", "aws", "dynamodb", "expr", "expr.go"),
			old:  "return cmp == 0, nil",
			new:  "return cmp != 0, nil",
			pkg:  "./internal/services/aws/dynamodb/expr",
			run:  "TestEquals",
		},
		{
			name: "fault-501-to-500",
			file: filepath.Join("internal", "spi", "spi.go"),
			old:  "HTTPStatus: 501,",
			new:  "HTTPStatus: 500,",
			pkg:  "./internal/conformance",
			run:  "TestFaultErrorString|TestCodecRoundTripAndFaultEnvelope",
		},
		{
			name: "edge-unknown-service-501-to-500",
			file: filepath.Join("internal", "edge", "edge.go"),
			old:  `http.Error(w, "MirrorNotImplemented: unknown service", http.StatusNotImplemented)`,
			new:  `http.Error(w, "MirrorNotImplemented: unknown service", http.StatusInternalServerError)`,
			pkg:  "./internal/edge",
			run:  "TestS3PutGetAndForeignService501",
		},
		{
			name: "edge-target-prefix-boundary-to-substring",
			file: filepath.Join("internal", "edge", "edge.go"),
			old:  `low == prefix || strings.HasPrefix(low, prefix+".") || strings.HasPrefix(low, prefix+"_")`,
			new:  `strings.Contains(low, prefix)`,
			pkg:  "./internal/edge",
			run:  "TestS3PutGetAndForeignService501",
		},
		{
			name: "restxml-tagging-route",
			file: filepath.Join("internal", "proto", "aws", "restxml", "restxml.go"),
			old:  `case hasQuery(r, "tagging"):`,
			new:  `case hasQuery(r, "tagging-mutated"):`,
			pkg:  "./internal/proto/aws/restxml",
			run:  "TestRouteNameQueryOps",
		},
		{
			name: "s3-copy-replace-to-copy",
			file: filepath.Join("internal", "services", "aws", "s3", "s3.go"),
			old:  `strings.EqualFold(directive, "REPLACE")`,
			new:  `strings.EqualFold(directive, "COPY")`,
			pkg:  "./internal/services/aws/s3",
			run:  "TestCopyObjectTaggingDirective",
		},
		{
			name: "s3-enable-disabled-replication-rule",
			file: filepath.Join("internal", "services", "aws", "s3", "replication.go"),
			old:  `strings.EqualFold(str(rule["Status"]), "Disabled")`,
			new:  `strings.EqualFold(str(rule["Status"]), "Enabled")`,
			pkg:  "./internal/services/aws/s3",
			run:  "TestReplicationFiltersStatusMetadataAndDeleteMarker",
		},
		{
			name: "s3-replication-prefix-to-suffix",
			file: filepath.Join("internal", "services", "aws", "s3", "replication.go"),
			old:  "strings.HasPrefix(key, prefix)",
			new:  "strings.HasSuffix(key, prefix)",
			pkg:  "./internal/services/aws/s3",
			run:  "TestReplicationFiltersStatusMetadataAndDeleteMarker",
		},
		{
			name: "iam-deny-precedence",
			file: filepath.Join("internal", "services", "aws", "iam", "authorizer.go"),
			old:  `effectMatch(docs, "Deny", action, resource, values)`,
			new:  `effectMatch(docs, "Allow", action, resource, values)`,
			pkg:  "./internal/services/aws/iam",
			run:  "TestAuthorizerExplicitDeny|TestAuthorizerAllowThenDeny",
		},
		{
			name: "iam-inactive-key-to-active",
			file: filepath.Join("internal", "services", "aws", "iam", "authorizer.go"),
			old:  `!strings.EqualFold(str(rec["Status"]), "Inactive")`,
			new:  `!strings.EqualFold(str(rec["Status"]), "Active")`,
			pkg:  "./internal/services/aws/iam",
			run:  "TestAuthorizerUserAndGroupPolicies",
		},
		{
			name: "sqs-dedup-window-zero",
			file: filepath.Join("internal", "services", "aws", "sqs", "sqs.go"),
			old:  "now.Add(5 * time.Minute).UnixNano()",
			new:  "now.Add(0 * time.Minute).UnixNano()",
			pkg:  "./internal/services/aws/sqs",
			run:  "TestFIFODedupDLQLongPoll",
		},
		{
			name: "sqs-max-messages-one",
			file: filepath.Join("internal", "services", "aws", "sqs", "sqs.go"),
			old:  "if max > 10 {\n\t\tmax = 10\n\t}",
			new:  "if max > 10 {\n\t\tmax = 1\n\t}",
			pkg:  "./internal/services/aws/sqs",
			run:  "TestFIFODedupDLQLongPoll",
		},
		{
			name: "cloudcontrol-write-wrong-collection",
			file: filepath.Join("internal", "services", "aws", "cloudcontrol", "cloudcontrol.go"),
			old:  `p.col(req, "ccres").Put(ctx, id, b)`,
			new:  `p.col(req, "ccres-mutated").Put(ctx, id, b)`,
			pkg:  "./internal/services/aws/cloudcontrol",
			run:  "TestCreatedResourceRoundTrip",
		},
		{
			name: "catalog-drop-cloudcontrol",
			file: filepath.Join("internal", "catalog", "catalog.go"),
			old:  `svc("aws.cloudcontrol", "cloudcontrolapi"`,
			new:  `svc("aws.cloudcontrol-mutated", "cloudcontrolapi"`,
			pkg:  "./internal/catalog",
			run:  "TestBundleCharacterization",
		},
		{
			name: "events-deliver-disabled-rule",
			file: filepath.Join("internal", "services", "aws", "events", "events.go"),
			old:  `str(rule["State"]) == "DISABLED"`,
			new:  `str(rule["State"]) == "ENABLED"`,
			pkg:  "./internal/services/aws/events",
			run:  "TestPutEventsDeliversOnlyMatchingRules",
		},
		{
			name: "events-prefix-to-suffix",
			file: filepath.Join("internal", "services", "aws", "events", "events_extra.go"),
			old:  "strings.HasPrefix(got, prefix)",
			new:  "strings.HasSuffix(got, prefix)",
			pkg:  "./internal/services/aws/events",
			run:  "TestMatchEventPatternOperators",
		},
		{
			name: "lambda-async-to-sync",
			file: filepath.Join("internal", "services", "aws", "lambda", "lambda.go"),
			old:  `invocationType == "Event"`,
			new:  `invocationType == "RequestResponse"`,
			pkg:  "./internal/services/aws/lambda",
			run:  "TestInvokeEventAndDryRunStatus",
		},
		{
			name: "sqs-disable-queue-existence-guard",
			file: filepath.Join("internal", "services", "aws", "sqs", "sqs.go"),
			old:  `queueScoped(req.Operation) && !p.queueExists(ctx, req, queueName(req))`,
			new:  `false && !p.queueExists(ctx, req, queueName(req))`,
			pkg:  "./internal/services/aws/sqs",
			run:  "TestQueueScopedOperationsRejectMissingQueue",
		},
	}

	for _, m := range mutants {
		t.Run(m.name, func(t *testing.T) {
			src := filepath.Join(root, m.file)
			body, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), m.old) {
				t.Fatalf("needle not found in %s: %q", m.file, m.old)
			}
			mutated := strings.Replace(string(body), m.old, m.new, 1)
			dir := t.TempDir()
			dst := filepath.Join(dir, filepath.Base(m.file))
			if err := os.WriteFile(dst, []byte(mutated), 0o644); err != nil {
				t.Fatal(err)
			}
			overlay := filepath.Join(dir, "overlay.json")
			enc, _ := json.Marshal(map[string]any{"Replace": map[string]string{src: dst}})
			if err := os.WriteFile(overlay, enc, 0o644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", m.pkg, "-count=1", "-run", m.run, "-overlay", overlay)
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("mutant survived (tests still passed)\n%s", out)
			}
		})
	}
}
