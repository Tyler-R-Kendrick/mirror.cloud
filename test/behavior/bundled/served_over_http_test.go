// Package bundled_test asks one question of every behavior bundle: given a
// booted server, when a client calls the service the way an SDK would, then
// the engine answers it.
//
// It exists because the extraction has been quietly losing that coverage. Each
// hand-written pack carried a TestBootedServer<Service> test -- a real request
// through the HTTP edge, the protocol codec, the signature path and the
// runtime's boot -- and deleting the pack deleted it. The equivalence gate
// that replaced the pack does not stand in for it: Replay builds the bundle
// directly and calls Invoke, so it proves the bundle matches the pack's
// semantics and nothing at all about whether the service can be reached.
//
// Eight such tests went with the last four batches of extractions. Nothing
// replaced them, and nothing would have said so: a bundle that no protocol
// codec can serialize, or that the edge cannot route to, passes every gate the
// project had.
//
// So this one is generic rather than per-service. A hand-written test per
// bundle would have to be written for each of the ninety-seven, and again for
// each future extraction, which is how the coverage was lost in the first
// place.
package bundled_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	behaviors "github.com/tyler-r-kendrick/mirror.cloud/behavior"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/check"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/generated"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/allservices"
)

// reachable picks the operation to call: one the bundle serves whose input
// requires nothing, so the call can be made from the model alone. That is
// almost always a list or a describe-all, which is the operation a client
// reaches for first and the one most likely to be exercised in anger.
func reachable(ir *bir.Service, svc *model.Service) (model.Operation, bool) {
	var best model.Operation
	found := false
	for _, op := range svc.Operations {
		if _, served := ir.Operations[op.Name]; !served {
			continue
		}
		if op.Input != "" {
			shape, ok := svc.Shapes[op.Input]
			if !ok {
				continue
			}
			required := false
			for _, m := range shape.Members {
				if m.Required {
					required = true
					break
				}
			}
			if required {
				continue
			}
		}
		// Deterministic: the alphabetically first qualifying operation, so a
		// failure names the same operation on every run.
		if !found || op.Name < best.Name {
			best, found = op, true
		}
	}
	return best, found
}

// shared counts how many services claim each endpoint prefix, so the ones that
// collide can be addressed by name instead.
var shared = map[string]int{}

func request(base string, svc *model.Service, op model.Operation) (*http.Request, error) {
	uri := op.HTTP.URI
	if uri == "" {
		uri = "/"
	}
	// A REST URI may carry path parameters. An operation that requires none
	// still may have optional ones; leaving a brace in the path would ask for
	// a route nothing serves, so those operations are not candidates.
	if strings.ContainsAny(uri, "{}") {
		return nil, fmt.Errorf("templated URI %q", uri)
	}
	method := op.HTTP.Method
	if method == "" {
		method = http.MethodPost
	}

	var body io.Reader
	req, err := http.NewRequest(method, base+uri, nil)
	if err != nil {
		return nil, err
	}
	switch svc.Protocol {
	case model.ProtoAWSJSON10, model.ProtoAWSJSON11:
		version := "1.0"
		if svc.Protocol == model.ProtoAWSJSON11 {
			version = "1.1"
		}
		body = strings.NewReader("{}")
		req, err = http.NewRequest(method, base+uri, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-amz-json-"+version)
		target := op.Target
		if target == "" {
			target = svc.TargetPrefix + "." + op.Name
		}
		req.Header.Set("X-Amz-Target", target)
	case model.ProtoAWSQuery, model.ProtoEC2Query:
		action := op.QueryAction
		if action == "" {
			action = op.Name
		}
		form := url.Values{"Action": {action}, "Version": {svc.QueryVersion}}
		req, err = http.NewRequest(method, base+uri, strings.NewReader(form.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	case model.ProtoRESTJSON1:
		if method == http.MethodPost || method == http.MethodPut {
			req, err = http.NewRequest(method, base+uri, strings.NewReader("{}"))
			if err != nil {
				return nil, err
			}
		}
		req.Header.Set("Content-Type", "application/json")
	case model.ProtoRESTXML:
		req.Header.Set("Content-Type", "application/xml")
	default:
		return nil, fmt.Errorf("unhandled protocol %q", svc.Protocol)
	}
	// The Host is how the edge routes a REST-protocol request: there is no
	// X-Amz-Target and no Action to demux on, so a request to "/" with a bare
	// localhost Host falls through to S3's path-style routing and the answer
	// is a 501 about an S3 operation. An SDK sends the regional endpoint, so
	// this does too.
	// DocumentDB and Neptune declare `rds` as their endpoint prefix -- they
	// are forks of the RDS API and their specifications say so -- and a host
	// built from that prefix is genuinely ambiguous. Real clients reach them
	// at docdb.<region>.amazonaws.com, which the specification does not
	// record, so the service's own name is the closest thing available.
	label := svc.EndpointPrefix
	if shared[label] > 1 {
		label = strings.TrimPrefix(svc.ID, "aws.")
	}
	req.Host = label + ".us-east-1.amazonaws.com"
	// The edge only needs a credential it can scope; signature verification is
	// off by default, as it is in every other booted-server test here.
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/"+
			svc.EndpointPrefix+"/aws4_request, SignedHeaders=host, Signature=00")
	return req, nil
}

// TestEveryBundleAnswersOverHTTP is the gate. For every bundle, boot a server
// with only that service enabled and make one real request through the edge.
//
// What it proves is deliberately shallow and deliberately universal: the
// runtime boots the bundle, the edge routes the request to it, the protocol
// codec serializes what the engine answered, and the response is a modeled one
// rather than a 5xx. Anything deeper is the equivalence recording's job.
func TestEveryBundleAnswersOverHTTP(t *testing.T) {
	ids := behaviors.ServiceIDs()
	if len(ids) == 0 {
		t.Fatal("no bundles")
	}
	// A bundle the runtime serves in a protocol the specification disagrees
	// with -- a different protocol, or a different X-Amz-Target prefix --
	// cannot be called the way an SDK would -- the request this test
	// builds from the model is the request that does not reach it, which is
	// the defect rather than a fault of the test. Those are counted by the
	// ratchet (`protocol_mismatches`, currently 48) and excluded here, so this
	// gate is a real one today and widens by itself as they are fixed.
	for _, id := range ids {
		if m, err := generated.Model(id); err == nil {
			shared[m.EndpointPrefix]++
		}
	}
	served, spec := check.ServedAndSpecRouting()
	_, mismatched := check.MeasureRouting(served, spec)
	skip := map[string]bool{}
	for _, id := range mismatched {
		skip[id] = true
	}
	skipped := 0
	for _, id := range ids {
		if skip[id] {
			skipped++
			continue
		}
		t.Run(id, func(t *testing.T) {
			svc, err := generated.Model(id)
			if err != nil {
				t.Fatalf("model: %v", err)
			}
			ir, err := behaviors.Load(id, svc)
			if err != nil {
				t.Fatalf("bundle: %v", err)
			}
			op, ok := reachable(ir, svc)
			if !ok {
				// A service whose every served operation requires something is
				// not a hole in this gate; it is a service this shape of test
				// cannot reach without inventing values the model does not
				// supply. Reported so the number stays visible.
				skipped++
				t.Skip("no served operation with an input that requires nothing")
			}

			cfg := config.Default()
			cfg.Services = []string{id}
			cfg.Seed = "bundled-http"
			rt, err := rtpkg.Boot(cfg)
			if err != nil {
				t.Fatalf("boot %s: %v", id, err)
			}
			ts := httptest.NewServer(rt.Handler())
			defer ts.Close()

			req, err := request(ts.URL, svc, op)
			if err != nil {
				skipped++
				t.Skipf("%s: %v", op.Name, err)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("%s: %v", op.Name, err)
			}
			raw, _ := io.ReadAll(res.Body)
			res.Body.Close()

			if res.StatusCode >= 500 {
				t.Fatalf("%s %s: %d %s\n\tThe service is bundled and boots, but a "+
					"request the model says is well-formed does not reach a modeled "+
					"answer. This is the coverage each pack's TestBootedServer test "+
					"used to carry.", id, op.Name, res.StatusCode, raw)
			}
			if got := res.Header.Get("x-mirror-fidelity"); got == "" {
				t.Errorf("%s %s: no fidelity header; the edge did not treat this as "+
					"a served response", id, op.Name)
			}
		})
	}
	t.Logf("%d bundles, %d skipped (protocol mismatch, or no operation "+
		"reachable from the model alone)", len(ids), skipped)
}
