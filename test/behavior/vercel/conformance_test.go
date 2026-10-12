package vercel

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// The conformance run answers one question for every operation vercel.api
// serves: when a client calls it through the running emulator, is the body that
// comes back a body the vendored OpenAPI document says that operation returns?
//
// That is a narrower claim than "matches Vercel". The document is the vendor's
// own description of its API and nothing here has been compared with
// api.vercel.com; what a pass establishes is that a client generated from the
// document (the @vercel/sdk validates every response against it) can read what
// the emulator sends. Each area file drives its operations through call(), which
// validates the body against the schema the document declares for that
// operation and status, and records what was driven. conformance_coverage_test
// then holds the served surface to what was driven: an operation added to the
// model without a driver fails there, which is how "every operation implemented
// here" stays true.
//
// Operations the vendored document does not describe (the ones api-extra.json
// and clients.json supply) are validated against the schema those supplements
// declare, which is authored, and are reported as such in docs/VERCEL.md.

// confOp is one served operation as the model states it.
type confOp struct {
	name   string
	method string
	uri    string
	alts   []string
	doc    *oasDoc        // the document that describes it, nil when none does
	spec   map[string]any // its operation object
}

// provenance names where an operation's schema comes from.
func (o *confOp) provenance() string {
	switch {
	case o.doc == nil:
		return "undocumented"
	case o.doc.name == "api.json":
		return "spec-validated"
	default:
		return "authored"
	}
}

type conf struct {
	*vx
	main    *oasDoc
	extra   *oasDoc
	ops     map[string]*confOp
	statuse map[string]map[int]bool // operation -> statuses driven and validated
	// undeclared are the error statuses the emulator answered that the document
	// does not list for the operation, as "Operation 404".
	undeclared map[string]bool
	spelled    map[string]bool // "operation address" -> driven
	areas      int
}

type confArea struct {
	name string
	run  func(c *conf, t *testing.T)
}

// confAreas is filled by each area file's init, so adding an area is adding a
// file and never editing a shared list.
var confAreas []confArea

func registerArea(name string, run func(c *conf, t *testing.T)) {
	confAreas = append(confAreas, confArea{name, run})
}

func newConf(t *testing.T, opts ...func(*bootOptions)) *conf {
	t.Helper()
	// The control routes are on, because they are served operations and the run
	// holds every served operation to the same question.
	faults := withConfig(func(c *config.Config) { c.VercelFaults = true })
	v := newVXWith(t, "vercel-conformance", []string{"vercel.api", "aws.s3"}, append([]func(*bootOptions){faults}, opts...)...)
	c := &conf{
		vx:         v,
		main:       loadOAS(t, "api.json"),
		extra:      loadOAS(t, "api-extra.json"),
		ops:        map[string]*confOp{},
		statuse:    map[string]map[int]bool{},
		undeclared: map[string]bool{},
		spelled:    map[string]bool{},
	}
	svc := v.rt.Deps.Model.ServiceByID("vercel.api")
	if svc == nil {
		t.Fatal("the booted model has no vercel.api")
	}
	for _, mo := range svc.Operations {
		op := &confOp{name: mo.Name, method: mo.HTTP.Method, uri: mo.HTTP.URI, alts: mo.HTTP.AltURIs}
		for _, d := range []*oasDoc{c.main, c.extra} {
			if spec, ok := d.operation(mo.HTTP.Method, mo.HTTP.URI); ok {
				op.doc, op.spec = d, spec
				break
			}
		}
		c.ops[mo.Name] = op
	}
	return c
}

// journalLen counts the requests vercel.api has served so far.
func (c *conf) journalLen() int {
	return len(c.rt.Deps.Journal.Query(spi.Filter{ServiceID: "vercel.api"}))
}

// call sends one request and checks it: the status must be want, and the body
// must satisfy the document's schema for that operation and status. It returns
// the reply for the next step to read.
func (c *conf) call(t *testing.T, method, path string, body any, want int, headers ...string) reply {
	t.Helper()
	before := c.journalLen()
	r := c.do(method, path, body, headers...)
	var served string
	if c.journalLen() > before {
		served = c.rt.Deps.Journal.Query(spi.Filter{ServiceID: "vercel.api", Limit: 1})[0].Operation
	}
	label := fmt.Sprintf("%s %s", method, path)
	if r.status != want {
		t.Errorf("%s = %d, want %d: %s", label, r.status, want, clip(r.raw))
	}
	op := c.ops[served]
	if op == nil {
		t.Errorf("%s was not served by any vercel.api operation (status %d)", label, r.status)
		return r
	}
	c.note(op, path, r.status)
	var inst any
	hasJSON := len(r.raw) > 0 && json.Unmarshal(r.raw, &inst) == nil
	if r.status >= 400 {
		c.checkEnvelope(t, label, op, r, inst, hasJSON)
	}
	if op.doc != nil {
		schema, declared := op.doc.responseSchema(op.spec, r.status)
		_, noBody := confNoSuccessBody[op.name]
		switch {
		case !declared && noBody && r.status < 300:
			// The document gives this operation no success response at all, so
			// there is nothing to hold the answer to; the reason is on the list.
		case !declared && r.status >= 400:
			// The document leaves out errors the real API answers: getProject
			// declares no 404. An error status it does not list is not a shape
			// to hold the answer to, and the envelope check above still applies;
			// the count is reported so the gap is visible.
			c.undeclared[op.name+" "+fmt.Sprint(r.status)] = true
		case !declared:
			t.Errorf("%s: %s answered %d, a status %s does not declare", label, op.name, r.status, op.doc.name)
		case schema != nil && hasJSON:
			if !strings.Contains(r.header.Get("Content-Type"), "json") {
				t.Errorf("%s: body is JSON but Content-Type is %q", label, r.header.Get("Content-Type"))
			}
			if errs := op.doc.validate(schema, inst); len(errs) > 0 {
				t.Errorf("%s: %s %d body does not satisfy %s:\n    %s\n  body: %s",
					label, op.name, r.status, op.doc.name, strings.Join(clipList(errs, 12), "\n    "), clip(r.raw))
			}
		case schema != nil && !hasJSON && r.status != 204 && r.status != 304 && len(r.raw) > 0:
			t.Errorf("%s: the document declares a JSON body for %d and the answer is not JSON: %s", label, r.status, clip(r.raw))
		case schema != nil && len(r.raw) == 0 && r.status != 204 && r.status != 304:
			t.Errorf("%s: the document declares a JSON body for %d and the answer is empty", label, r.status)
		}
	}
	// An alternate address the vendored document also declares, under an
	// operation of its own, holds the answer to that operation's schema too:
	// POST /v1/teams is mirror's CreateTeam and the document's createTeam, and a
	// client that follows the document at that address reads the document's
	// shape from it.
	if c.main != nil && r.status < 400 && hasJSON {
		bare, _, _ := strings.Cut(path, "?")
		for _, alt := range op.alts {
			if !templateMatches(alt, bare) {
				continue
			}
			other, ok := c.main.operation(op.method, alt)
			if !ok || (op.doc == c.main && alt == op.uri) {
				continue
			}
			if schema, declared := c.main.responseSchema(other, r.status); declared && schema != nil {
				if errs := c.main.validate(schema, inst); len(errs) > 0 {
					t.Errorf("%s: the answer at %s does not satisfy the document's operation there:\n    %s\n  body: %s",
						label, alt, strings.Join(clipList(errs, 12), "\n    "), clip(r.raw))
				}
			}
		}
	}
	return r
}

// note records that an operation was driven, at which status and which address.
func (c *conf) note(op *confOp, path string, status int) {
	if c.statuse[op.name] == nil {
		c.statuse[op.name] = map[int]bool{}
	}
	c.statuse[op.name][status] = true
	bare, _, _ := strings.Cut(path, "?")
	for _, tmpl := range append([]string{op.uri}, op.alts...) {
		if templateMatches(tmpl, bare) {
			c.spelled[op.name+" "+tmpl] = true
		}
	}
}

// checkEnvelope holds an error answer to Vercel's envelope, {"error":{"code",
// "message"}}. The document declares most error statuses without a body, so this
// is the one check that applies to all of them.
func (c *conf) checkEnvelope(t *testing.T, label string, op *confOp, r reply, inst any, hasJSON bool) {
	t.Helper()
	if op.name == "OauthToken" {
		return // RFC 6749's flat {error, error_description}, by design
	}
	obj, _ := inst.(map[string]any)
	e, _ := obj["error"].(map[string]any)
	if !hasJSON || e == nil || str(e, "code") == "" || str(e, "message") == "" {
		t.Errorf("%s: %d is not Vercel's error envelope {error:{code,message}}: %s", label, r.status, clip(r.raw))
	}
}

// templateMatches reports whether path is an instance of a URI template, a
// {label} standing for one non-empty segment and {label+} for the rest.
func templateMatches(tmpl, path string) bool {
	ts, ps := strings.Split(strings.Trim(tmpl, "/"), "/"), strings.Split(strings.Trim(path, "/"), "/")
	for i, seg := range ts {
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "+}") {
			return i < len(ps)
		}
		if i >= len(ps) {
			return false
		}
		if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
			if ps[i] == "" {
				return false
			}
			continue
		}
		if seg != ps[i] {
			return false
		}
	}
	return len(ts) == len(ps)
}

func clip(b []byte) string {
	s := string(b)
	if len(s) > 400 {
		return s[:397] + "..."
	}
	return s
}

func clipList(l []string, n int) []string {
	if len(l) <= n {
		return l
	}
	return append(append([]string(nil), l[:n]...), fmt.Sprintf("... and %d more", len(l)-n))
}

// TestVercelResponsesConformToTheDocument drives every area and then checks
// that the served surface is exactly what was driven.
func TestVercelResponsesConformToTheDocument(t *testing.T) {
	c := newConf(t)
	for _, a := range confAreas {
		a := a
		if t.Run(a.name, func(t *testing.T) { a.run(c, t) }) {
			c.areas++
		}
	}
	if c.areas < len(confAreas) {
		t.Skipf("%d of %d areas ran; coverage is only meaningful for a whole run", c.areas, len(confAreas))
	}
	t.Run("coverage", c.coverage)
	var names []string
	for k := range c.undeclared {
		names = append(names, k)
	}
	sort.Strings(names)
	t.Logf("%d error answers the document does not list for their operation: %s", len(names), strings.Join(names, ", "))
}

// confNoSuccessBody lists the operations the vendored document declares only
// errors for. A client generated from it has no success shape to read, so the
// run checks the status and nothing about the body.
var confNoSuccessBody = map[string]string{
	"GetDeploymentFileContents": "the document declares 400, 401, 403, 404 and 410 and no 200",
}

// pending is the reason an operation is on the ledger while the area that will
// drive it has not landed. The ledger only shrinks: an operation driven after
// all fails the run until its entry is deleted, and what remains when every area
// is in is the operations that cannot be driven, each with its own reason.
const pending = "its area's conformance driver has not landed"

// confUncovered names the served operations the run does not drive, and why.
var confUncovered = map[string]string{
	"AddProjectDomain":                        pending,
	"ApproveRollingReleaseStage":              pending,
	"ArtifactExists":                          pending,
	"ArtifactQuery":                           pending,
	"AssignAlias":                             pending,
	"CompleteRollingRelease":                  pending,
	"CreateApiKey":                            pending,
	"CreateCheck":                             pending,
	"CreateCustomEnvironment":                 pending,
	"CreateDeploymentCheckRun":                pending,
	"CreateProjectCheck":                      pending,
	"CreateProjectEnv":                        pending,
	"DeleteAlias":                             pending,
	"DeleteAllArtifacts":                      pending,
	"DeleteApiKey":                            pending,
	"DeleteProjectCheck":                      pending,
	"DeleteRollingReleaseConfig":              pending,
	"DownloadArtifact":                        pending,
	"EditProjectEnv":                          pending,
	"FilterProjectEnvs":                       pending,
	"GetAlias":                                pending,
	"GetAllChecks":                            pending,
	"GetCheck":                                pending,
	"GetCustomEnvironment":                    pending,
	"GetDeploymentCheckRun":                   pending,
	"GetProjectCheck":                         pending,
	"GetProjectDomain":                        pending,
	"GetProjectDomains":                       pending,
	"GetProjectEnv":                           pending,
	"GetProjectsByIdOrNameCustomEnvironments": pending,
	"GetRegistration":                         pending,
	"GetRollingRelease":                       pending,
	"GetRollingReleaseBillingStatus":          pending,
	"GetRollingReleaseConfig":                 pending,
	"GetRuntimeLogs":                          pending,
	"ListAliases":                             pending,
	"ListApiKeys":                             pending,
	"ListCheckRuns":                           pending,
	"ListDeploymentCheckRuns":                 pending,
	"ListProjectChecks":                       pending,
	"ListPromoteAliases":                      pending,
	"OauthAuthorize":                          pending,
	"OauthAuthorizeCallback":                  pending,
	"OauthToken":                              pending,
	"OauthUserinfo":                           pending,
	"PatchUrlProtectionBypass":                pending,
	"RecordEvents":                            pending,
	"RemoveCustomEnvironment":                 pending,
	"RemoveProjectDomain":                     pending,
	"RemoveProjectEnv":                        pending,
	"RequestPromote":                          pending,
	"RequestRollback":                         pending,
	"RerequestCheck":                          pending,
	"StartRollingRelease":                     pending,
	"Status":                                  pending,
	"UpdateCheck":                             pending,
	"UpdateCustomEnvironment":                 pending,
	"UpdateDeploymentCheckRun":                pending,
	"UpdateProjectCheck":                      pending,
	"UpdateProjectDomain":                     pending,
	"UpdateProjectProtectionBypass":           pending,
	"UpdateProjectsByProjectIdRollbackByDeploymentIdUpdateDescription": pending,
	"UpdateRollingReleaseConfig":                                       pending,
	"UploadArtifact":                                                   pending,
	"VerifyProjectDomain":                                              pending,
}

func (c *conf) coverage(t *testing.T) {
	var names []string
	for n := range c.ops {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		op := c.ops[n]
		reason, excused := confUncovered[n]
		driven := len(c.statuse[n]) > 0
		switch {
		case excused && driven:
			t.Errorf("%s is listed as not driven (%s) and was driven: delete the entry", n, reason)
		case !excused && !driven:
			t.Errorf("%s (%s %s) is served and no conformance step drives it", n, op.method, op.uri)
		}
		if excused {
			continue
		}
		for _, tmpl := range append([]string{op.uri}, op.alts...) {
			if !c.spelled[n+" "+tmpl] {
				t.Errorf("%s answers at %s and no conformance step calls that spelling", n, tmpl)
			}
		}
	}
	for n := range confUncovered {
		if c.ops[n] == nil {
			t.Errorf("confUncovered names %s, which is not a served operation", n)
		}
	}
}
