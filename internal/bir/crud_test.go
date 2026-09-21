package bir_test

import (
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
)

// The shorthand is a claim that three keys mean exactly the ten lines they
// replace. This is the check that fails if the expansion drifts from the long
// form by so much as a binding name, and it is the only check the shorthand
// needs: past the loader there is nothing new to test.

const transcribeHead = `schema: bir/1
service: aws.transcribe
provenance: authored
errors:
  NotFound: { code: NotFoundException, http: 400, fault: client }
resources:
  vocabulary:
    collection: trvocab
    id: { input_members: [VocabularyName] }
    record:
      VocabularyName: id
      LanguageCode: "'LanguageCode' in input ? string(input.LanguageCode) : ''"
      VocabularyState: "'READY'"
  job:
    collection: trjob
    id: { input_members: [TranscriptionJobName] }
    record:
      TranscriptionJobName: id
      TranscriptionJobStatus: "'COMPLETED'"
operations:
`

func loadOps(t *testing.T, ops string) map[string]bir.Operation {
	t.Helper()
	svc, err := generated.Model("aws.transcribe")
	if err != nil {
		t.Fatal(err)
	}
	fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(transcribeHead + ops)}}
	s, err := bir.Load(fsys, "svc", svc)
	if err != nil {
		t.Fatal(err)
	}
	return s.Operations
}

func TestShorthandExpandsToExactlyTheLongForm(t *testing.T) {
	short := loadOps(t, `
  GetVocabulary:         { get: { resource: vocabulary, error: NotFound } }
  CreateVocabulary:      { put: { resource: vocabulary } }
  GetTranscriptionJob:   { get: { resource: job, error: NotFound, wrap: TranscriptionJob } }
  StartTranscriptionJob: { put: { resource: job, wrap: TranscriptionJob } }
  DeleteVocabulary:      { delete: { resource: vocabulary, missing: ignore } }
`)
	long := loadOps(t, `
  DeleteVocabulary:
    effects: [ { delete: { resource: vocabulary, missing: ignore } } ]
  GetTranscriptionJob:
    reads: { rec: { resource: job } }
    require: [ { cond: rec_found, error: NotFound } ]
    output: { TranscriptionJob: rec }
  StartTranscriptionJob:
    effects: [ { put: { resource: job } } ]
    output: { TranscriptionJob: rec }
  GetVocabulary:
    reads: { rec: { resource: vocabulary } }
    require: [ { cond: rec_found, error: NotFound } ]
    output:
      VocabularyName: rec.VocabularyName
      LanguageCode: rec.LanguageCode
      VocabularyState: rec.VocabularyState
  CreateVocabulary:
    effects: [ { put: { resource: vocabulary } } ]
    output:
      VocabularyName: rec.VocabularyName
      LanguageCode: rec.LanguageCode
      VocabularyState: rec.VocabularyState
`)
	for _, name := range []string{"GetVocabulary", "CreateVocabulary", "GetTranscriptionJob", "StartTranscriptionJob", "DeleteVocabulary"} {
		if !reflect.DeepEqual(short[name], long[name]) {
			t.Errorf("%s: shorthand expanded to\n%#v\nwant the long form\n%#v", name, short[name], long[name])
		}
	}
}

// Each refusal is a way the shorthand could quietly mean something other than
// the long form it stands for.
func TestShorthandRefusals(t *testing.T) {
	svc, err := generated.Model("aws.transcribe")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, ops, want string }{
		{"beside long-form keys", `
  GetVocabulary:
    get: { resource: vocabulary, error: NotFound }
    output: { VocabularyName: rec.VocabularyName }
`, "cannot sit beside"},
		{"get without an error", `
  GetVocabulary: { get: { resource: vocabulary } }
`, "names no error"},
		{"error on a write", `
  CreateVocabulary: { put: { resource: vocabulary, error: NotFound } }
`, "meaningless here"},
		{"two verbs", `
  CreateVocabulary:
    create: { resource: vocabulary }
    put: { resource: vocabulary }
`, "use one"},
		{"missing off a delete", `
  GetVocabulary: { get: { resource: vocabulary, error: NotFound, missing: ignore } }
`, "means nothing here"},
		{"wrap on a delete", `
  DeleteVocabulary: { delete: { resource: vocabulary, wrap: Vocabulary } }
`, "nothing to wrap"},
		{"unknown resource", `
  GetVocabulary: { get: { resource: nosuch, error: NotFound } }
`, `unknown resource "nosuch"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsys := fstest.MapFS{"svc/service.yaml": &fstest.MapFile{Data: []byte(transcribeHead + tc.ops)}}
			_, err := bir.Load(fsys, "svc", svc)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want an error containing %q, got %v", tc.want, err)
			}
		})
	}
}
