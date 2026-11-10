package cloudformation

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The tests of the fifth review of the engine (of 22553ebc), one for each of its findings; the reviewer's
// own walks are review_r32{a,b,c}_test.go.

// A rollback and a continued one agree on a value the failed update took away: the template gives the
// secret's SecretString, the failed update gave none, and both put the template's value back, over one
// rotated outside the template since. Before, a continued rollback knew only what the failed update gave,
// read the value as unchanged, and left the rotated one, where the rollback itself restored the template's.
func TestCloudFormationARollbackAndAContinuedOneAgreeOnAValueTakenAway(t *testing.T) {
	t.Parallel()
	secret := func(p map[string]any) map[string]any {
		return map[string]any{"Type": "AWS::SecretsManager::Secret", "Properties": p}
	}
	for name, failRestore := range map[string]bool{"a rollback": false, "a continued rollback": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newStacks(t)
			if err := rvMake(t, h, "ta", map[string]any{"Resources": map[string]any{"S": secret(map[string]any{"SecretString": "A", "Description": "one"})}}); err != nil {
				t.Fatal(err)
			}
			id := rv31IDs(h, "ta")["S"]
			if _, err := h.service("aws.secretsmanager", "PutSecretValue", map[string]any{"SecretId": id, "SecretString": "ROT"}); err != nil {
				t.Fatal(err)
			}
			var writes atomic.Int32 // of the secret: the update's lands, and the restore's fails when it is to
			off := faultStore(h, func(op, c, k string) bool {
				return failRestore && (op == "put" || op == "txput") && strings.Contains(c, "secret") && writes.Add(1) == 2
			})
			err := invoked(t, h, "UpdateStack", map[string]any{"StackName": "ta", "TemplateBody": rv19Body(map[string]any{"Resources": map[string]any{
				"S": secret(map[string]any{"Description": "two"}), "Z": rv31Dep(rv31Bad, "S")}})})
			off()
			if st, _, _ := h.status("ta"); err == nil || (st == "UPDATE_ROLLBACK_FAILED") != failRestore {
				t.Fatalf("the failed update answered %v and left %s (%d secret writes)", err, st, writes.Load())
			}
			if failRestore {
				if rec := spi.JSONText(h.record("ta")["Resources"]); !strings.Contains(rec, `"Gave":["Description"]`) || !strings.Contains(rec, `"Took":["SecretString"]`) {
					t.Errorf("the record is %s; want the description given and the value taken away, by name", rec)
				}
				if _, err := h.invoke("ContinueUpdateRollback", map[string]any{"StackName": "ta"}); err != nil {
					t.Fatal(err)
				}
			}
			value := rv31Get(h, "aws.secretsmanager", "GetSecretValue", map[string]any{"SecretId": id})["SecretString"]
			if st, _, _ := h.status("ta"); st != "UPDATE_ROLLBACK_COMPLETE" || value != "A" || strings.Contains(spi.JSONText(h.record("ta")["Resources"]), "Took") {
				t.Errorf("the stack is %s, the secret's value %v and its record %s; want UPDATE_ROLLBACK_COMPLETE, the template's value A, and nothing recorded any more", st, value, spi.JSONText(h.record("ta")["Resources"]))
			}
		})
	}
}
