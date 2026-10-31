// Package lambda runs the functions behavior/aws/lambda stores: Invoke and its
// variants, which the bundle declares native, execute a local python3 or node
// handler, and the worker delivers SQS messages to mapped functions.
package lambda

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// PublishSNS publishes onto an SNS topic. SNS registers this from init to
// avoid an import cycle when Event invokes fail into a function DLQ.
var PublishSNS func(ctx context.Context, deps spi.Deps, id spi.Identity, topicARN, message string)

func init() {
	for op, invoke := range map[string]func(*runner, context.Context, *spi.Request) (*spi.Response, error){
		"Invoke": (*runner).invoke, "InvokeAsync": (*runner).invokeAsync, "InvokeWithResponseStream": (*runner).invokeStream,
	} {
		bundled.RegisterNative("aws.lambda", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return invoke(&runner{deps: deps}, ctx, req)
		})
	}
	bundled.RegisterWorker("aws.lambda", Start)
}

// runner runs the functions behavior/aws/lambda stores, from the collection
// and in the layout the bundle writes them: keyed by name, with Runtime,
// Handler, Code, Environment and DeadLetterConfig.
type runner struct{ deps spi.Deps }

func (p *runner) col(req *spi.Request, name string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(name)
}

// Start runs the SQS event-source worker against deps and answers how to
// stop it.
func Start(d spi.Deps) func() error {
	if d.Bus == nil {
		return func() error { return nil }
	}
	cancel := d.Bus.Subscribe("sqs", (&runner{deps: d}).consumeSQS)
	return func() error { cancel(); return nil }
}

func (p *runner) emitDLQ(ctx context.Context, req *spi.Request, rec map[string]any, payload []byte, invokeErr error) {
	target := spi.Str(asMap(rec["DeadLetterConfig"])["TargetArn"])
	if target == "" || PublishSNS == nil || !strings.Contains(target, ":sns:") {
		return
	}
	envelope, _ := json.Marshal(map[string]any{
		"requestContext":  map[string]any{"condition": "RetriesExhausted", "functionArn": "arn:aws:lambda:" + req.Identity.Region + ":" + req.Identity.Account + ":function:" + spi.Str(rec["FunctionName"])},
		"requestPayload":  json.RawMessage(payload),
		"responseContext": map[string]any{"functionError": "Unhandled"},
		"responsePayload": invokeErr.Error(),
	})
	PublishSNS(ctx, p.deps, req.Identity, target, string(envelope))
}

func asMap(v any) map[string]any {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return map[string]any{}
}

func (p *runner) invokeAsync(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	resp, err := p.invoke(ctx, req)
	if err != nil {
		return nil, err
	}
	return &spi.Response{Status: 202, Output: map[string]any{"Status": 202, "Payload": resp.Output["Payload"]}}, nil
}

// ponytail: not a real HTTP/2 event stream; returns the same Invoke payload.
func (p *runner) invokeStream(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	resp, err := p.invoke(ctx, req)
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"StatusCode": 200, "Payload": resp.Output["Payload"]}}, nil
}

func (p *runner) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := spi.Str(req.Input["FunctionName"])
	b, ok, _ := p.col(req, "lambda").Get(ctx, name)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	invocationType := spi.Str(req.Input["InvocationType"])
	if invocationType == "DryRun" {
		return &spi.Response{Status: http.StatusNoContent, Output: map[string]any{"StatusCode": http.StatusNoContent}}, nil
	}
	var payload []byte
	switch raw := spi.Str(req.Input["Payload"]); {
	case req.Body != nil:
		var err error
		payload, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
	case raw != "":
		// The model binds `Payload` to the request payload, and the REST/JSON
		// codec now hands it over as the body rather than parsing the event
		// and scattering its keys across the input. Reading it here is what
		// makes an invocation over HTTP see the event a client actually sent.
		payload = []byte(raw)
	default:
		// The legacy shape, for a caller that builds the request by hand and
		// names the event's members as input. It cannot be removed: the
		// event-source mapping below invokes this pack directly, and so do
		// several tests.
		ev := map[string]any{}
		for k, v := range req.Input {
			if k == "FunctionName" || k == "InvocationType" || k == "LogType" || k == "Qualifier" {
				continue
			}
			ev[k] = v
		}
		payload, _ = json.Marshal(ev)
		if len(ev) == 0 {
			payload = []byte("{}")
		}
	}
	out, err := runHandler(name, spi.Str(rec["Runtime"]), spi.Str(rec["Handler"]), rec["Code"], rec["Environment"], payload)
	if invocationType == "Event" {
		if err != nil {
			p.emitDLQ(ctx, req, rec, payload, err)
		}
		return &spi.Response{Status: http.StatusAccepted, Output: map[string]any{"StatusCode": http.StatusAccepted}}, nil
	}
	if err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{"StatusCode": 200, "Payload": json.RawMessage(out)}}, nil
}

// consumeSQS delivers messages for mappings created through the Lambda API.
// The SQS bus event is only a wake-up; ReceiveMessage remains the source of
// truth so visibility, retry, and redrive semantics stay in the SQS pack.
func (p *runner) consumeSQS(ctx context.Context, payload []byte) {
	var event map[string]any
	if json.Unmarshal(payload, &event) != nil {
		return
	}
	queue := spi.Str(event["queue"])
	if queue == "" {
		return
	}
	identity := spi.Identity{Account: spi.Str(event["account"]), Region: spi.Str(event["region"])}
	if identity.Account == "" || identity.Region == "" {
		return
	}
	sourceARN := spi.Str(event["queueArn"])
	if sourceARN == "" {
		sourceARN = "arn:aws:sqs:" + identity.Region + ":" + identity.Account + ":" + queue
	}
	req := &spi.Request{Identity: identity}
	kvs, _, _ := p.col(req, "lambdaesm").List(ctx, "", "", 0)
	for _, kv := range kvs {
		var mapping map[string]any
		if json.Unmarshal(kv.Value, &mapping) != nil || spi.Str(mapping["EventSourceArn"]) != sourceARN {
			continue
		}
		if enabled, ok := mapping["Enabled"].(bool); ok && !enabled {
			continue
		}
		if spi.Str(mapping["State"]) == "Disabled" {
			continue
		}
		p.processSQSMapping(ctx, identity, queue, sourceARN, spi.Str(mapping["FunctionName"]))
	}
}

func (p *runner) processSQSMapping(ctx context.Context, identity spi.Identity, queue, sourceARN, function string) {
	if function == "" {
		return
	}
	if i := strings.Index(function, ":function:"); i >= 0 {
		function = function[i+len(":function:"):]
		if i := strings.IndexByte(function, ':'); i >= 0 {
			function = function[:i]
		}
	}
	queuePack := bundled.Handler("aws.sqs", p.deps)
	// A failed invocation is retried immediately with zero visibility. This
	// keeps local event delivery deterministic and lets SQS redrive after the
	// configured receive count without a second scheduler.
	for attempt := 0; attempt < 10; attempt++ {
		received, err := queuePack.Invoke(ctx, &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{
			"QueueName": queue, "MaxNumberOfMessages": 10, "VisibilityTimeout": 0,
			"AttributeNames": []any{"All"}, "MessageAttributeNames": []any{"All"},
		}})
		if err != nil {
			return
		}
		messages := spi.Slice(received.Output["Messages"])
		if len(messages) == 0 {
			return
		}
		records := make([]any, 0, len(messages))
		for _, raw := range messages {
			message, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			records = append(records, map[string]any{
				"messageId": message["MessageId"], "receiptHandle": message["ReceiptHandle"],
				"body": message["Body"], "attributes": message["Attributes"],
				"messageAttributes": message["MessageAttributes"], "md5OfBody": message["MD5OfBody"],
				"eventSource": "aws:sqs", "eventSourceARN": sourceARN, "awsRegion": identity.Region,
			})
		}
		body, _ := json.Marshal(map[string]any{"Records": records})
		response, invokeErr := p.invoke(ctx, &spi.Request{Identity: identity, Operation: "Invoke", Input: map[string]any{"FunctionName": function}, Body: io.NopCloser(bytes.NewReader(body))})
		if invokeErr != nil {
			continue
		}
		failed := failedSQSRecords(response)
		for _, raw := range messages {
			message, ok := raw.(map[string]any)
			if !ok || failed[spi.Str(message["MessageId"])] {
				continue
			}
			_, _ = queuePack.Invoke(ctx, &spi.Request{Identity: identity, Operation: "DeleteMessage", Input: map[string]any{
				"QueueName": queue, "ReceiptHandle": message["ReceiptHandle"],
			}})
		}
		if len(failed) == 0 {
			return
		}
	}
}

func failedSQSRecords(response *spi.Response) map[string]bool {
	failed := map[string]bool{}
	if response == nil {
		return failed
	}
	payload, ok := response.Output["Payload"].(json.RawMessage)
	if !ok {
		return failed
	}
	var output map[string]any
	if json.Unmarshal(payload, &output) != nil {
		return failed
	}
	items, _ := output["batchItemFailures"].([]any)
	for _, item := range items {
		if entry, ok := item.(map[string]any); ok {
			if id := spi.Str(entry["itemIdentifier"]); id != "" {
				failed[id] = true
			}
		}
	}
	return failed
}

func runHandler(name, runtime, handler string, code, environment any, payload []byte) ([]byte, error) {
	dir, err := os.MkdirTemp("", "mirror-lambda-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if err := writeCode(dir, code); err != nil {
		return nil, err
	}
	rt := strings.ToLower(runtime)
	env := append(os.Environ(), "AWS_LAMBDA_FUNCTION_NAME="+name)
	if configuration, ok := environment.(map[string]any); ok {
		if variables, ok := configuration["Variables"].(map[string]any); ok {
			for key, value := range variables {
				env = append(env, key+"="+spi.Str(value))
			}
		}
	}
	switch {
	case strings.Contains(rt, "python"):
		bin, err := exec.LookPath("python3")
		if err != nil {
			return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
		}
		mod, fn := splitHandler(handler)
		script := "import json,sys,importlib; m=importlib.import_module('" + mod + "'); ev=json.load(sys.stdin); print(json.dumps(getattr(m,'" + fn + "')(ev,None)))"
		cmd := exec.Command(bin, "-c", script)
		cmd.Dir = dir
		cmd.Env = env
		cmd.Stdin = bytes.NewReader(payload)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, &spi.Fault{Code: "Unhandled", Message: string(out) + err.Error(), HTTPStatus: 200, Fault: "server"}
		}
		return bytes.TrimSpace(out), nil
	case strings.Contains(rt, "node"):
		bin, err := exec.LookPath("node")
		if err != nil {
			return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
		}
		mod, fn := splitHandler(handler)
		script := "const m=require('./" + mod + "'); let d=''; process.stdin.on('data',c=>d+=c); process.stdin.on('end',()=>{Promise.resolve(m." + fn + "(JSON.parse(d||'{}'))).then(r=>process.stdout.write(JSON.stringify(r)))})"
		cmd := exec.Command(bin, "-e", script)
		cmd.Dir = dir
		cmd.Env = env
		cmd.Stdin = bytes.NewReader(payload)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return nil, &spi.Fault{Code: "Unhandled", Message: string(out), HTTPStatus: 200, Fault: "server"}
		}
		return bytes.TrimSpace(out), nil
	default:
		return nil, spi.NotImplemented("aws.lambda", "Invoke/"+runtime, "emulate")
	}
}

func writeCode(dir string, code any) error {
	raw := codeBytes(code)
	if zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw))); err == nil {
		for _, f := range zr.File {
			if f.FileInfo().IsDir() {
				continue
			}
			rc, err := f.Open()
			if err != nil {
				return err
			}
			b, _ := io.ReadAll(rc)
			_ = rc.Close()
			dst := filepath.Join(dir, f.Name)
			_ = os.MkdirAll(filepath.Dir(dst), 0o755)
			if err := os.WriteFile(dst, b, 0o644); err != nil {
				return err
			}
		}
		return nil
	}
	return os.WriteFile(filepath.Join(dir, "lambda_function.py"), raw, 0o644)
}

func codeBytes(code any) []byte {
	m, _ := code.(map[string]any)
	v := m["ZipFile"]
	switch t := v.(type) {
	case string:
		if b, err := base64.StdEncoding.DecodeString(t); err == nil {
			return b
		}
		return []byte(t)
	case []byte:
		return t
	}
	return nil
}

func splitHandler(h string) (mod, fn string) {
	mod, fn = "lambda_function", "lambda_handler"
	if i := strings.LastIndex(h, "."); i >= 0 {
		return h[:i], h[i+1:]
	}
	if h != "" {
		fn = h
	}
	return mod, fn
}
