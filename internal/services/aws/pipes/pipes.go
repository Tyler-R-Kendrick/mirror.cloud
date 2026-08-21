// Package pipes emulates EventBridge Pipes control plane and SQS source delivery.
package pipes

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.pipes", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EventBridge Pipes.
type Pack struct {
	deps      spi.Deps
	wake      chan struct{}
	stop      chan struct{}
	done      chan struct{}
	cancel    func()
	closeOnce sync.Once
}

// New constructs the pack and resumes running pipes.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if d.Store == nil || d.Clock == nil || d.Bus == nil {
		close(p.done)
		return p
	}
	p.cancel = d.Bus.Subscribe("sqs", func(context.Context, []byte) { p.notify() })
	go p.loop()
	return p
}

func (p *Pack) ServiceID() string { return "aws.pipes" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreatePipe", "DescribePipe", "ListPipes", "UpdatePipe", "DeletePipe",
		"StartPipe", "StopPipe",
		"TagResource", "UntagResource", "ListTagsForResource",
	}
}

// Close stops source polling.
func (p *Pack) Close() error {
	p.closeOnce.Do(func() {
		if p.cancel != nil {
			p.cancel()
		}
		close(p.stop)
	})
	<-p.done
	return nil
}

func (p *Pack) col(req *spi.Request, name string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(name)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	switch req.Operation {
	case "CreatePipe":
		if name == "" || first(req.Input, "Source") == "" || first(req.Input, "Target") == "" || first(req.Input, "RoleArn") == "" {
			return nil, validation("Name, Source, Target, and RoleArn are required.")
		}
		if _, ok, err := p.col(req, "pipe").Get(ctx, name); err != nil {
			return nil, err
		} else if ok {
			return nil, conflict("Pipe already exists.")
		}
		arn := "arn:aws:pipes:" + req.Identity.Region + ":" + req.Identity.Account + ":pipe/" + name
		rec := clone(req.Input)
		state := first(req.Input, "DesiredState")
		if state == "" {
			state = "RUNNING"
		}
		if state != "RUNNING" && state != "STOPPED" {
			return nil, validation("DesiredState must be RUNNING or STOPPED.")
		}
		rec["Name"], rec["Arn"], rec["CurrentState"], rec["DesiredState"] = name, arn, state, state
		if err := putRecord(ctx, p.col(req, "pipe"), name, rec); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": arn, "CurrentState": state}}, nil
	case "DescribePipe":
		rec, ok, err := getRecord(ctx, p.col(req, "pipe"), name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound()
		}
		return &spi.Response{Output: rec}, nil
	case "ListPipes":
		kvs, _, err := p.col(req, "pipe").List(ctx, "", "", 0)
		if err != nil {
			return nil, err
		}
		items := make([]any, 0, len(kvs))
		for _, kv := range kvs {
			var rec map[string]any
			if json.Unmarshal(kv.Value, &rec) == nil {
				items = append(items, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"Pipes": items}}, nil
	case "UpdatePipe":
		rec, ok, err := getRecord(ctx, p.col(req, "pipe"), name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound()
		}
		for key, value := range req.Input {
			if key != "Name" {
				rec[key] = value
			}
		}
		if state := first(req.Input, "DesiredState"); state != "" {
			if state != "RUNNING" && state != "STOPPED" {
				return nil, validation("DesiredState must be RUNNING or STOPPED.")
			}
			rec["CurrentState"] = state
		}
		if err := putRecord(ctx, p.col(req, "pipe"), name, rec); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": rec["Arn"], "CurrentState": rec["CurrentState"]}}, nil
	case "DeletePipe":
		if _, ok, err := p.col(req, "pipe").Get(ctx, name); err != nil {
			return nil, err
		} else if !ok {
			return nil, notFound()
		}
		if err := p.col(req, "pipe").Delete(ctx, name); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartPipe", "StopPipe":
		rec, ok, err := getRecord(ctx, p.col(req, "pipe"), name)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, notFound()
		}
		state := "RUNNING"
		if req.Operation == "StopPipe" {
			state = "STOPPED"
		}
		rec["CurrentState"], rec["DesiredState"] = state, state
		if err := putRecord(ctx, p.col(req, "pipe"), name, rec); err != nil {
			return nil, err
		}
		p.notify()
		return &spi.Response{Output: map[string]any{"Name": name, "Arn": rec["Arn"], "CurrentState": state}}, nil
	case "TagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		tags := map[string]any{}
		if b, ok, err := p.col(req, "pipetag").Get(ctx, arn); err != nil {
			return nil, err
		} else if ok {
			_ = json.Unmarshal(b, &tags)
		}
		incoming, _ := req.Input["tags"].(map[string]any)
		if incoming == nil {
			incoming, _ = req.Input["Tags"].(map[string]any)
		}
		for key, value := range incoming {
			tags[key] = value
		}
		b, _ := json.Marshal(tags)
		if err := p.col(req, "pipetag").Put(ctx, arn, b); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := first(req.Input, "resourceArn", "ResourceArn")
		b, ok, err := p.col(req, "pipetag").Get(ctx, arn)
		if err != nil {
			return nil, err
		}
		tags := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		keys := req.Input["tagKeys"]
		if keys == nil {
			keys = req.Input["TagKeys"]
		}
		switch keys := keys.(type) {
		case []any:
			for _, key := range keys {
				delete(tags, stringValue(key))
			}
		case []string:
			for _, key := range keys {
				delete(tags, key)
			}
		}
		b, _ = json.Marshal(tags)
		if err := p.col(req, "pipetag").Put(ctx, arn, b); err != nil {
			return nil, err
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, err := p.col(req, "pipetag").Get(ctx, first(req.Input, "resourceArn", "ResourceArn"))
		if err != nil {
			return nil, err
		}
		var tags any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	default:
		return nil, spi.NotImplemented("aws.pipes", req.Operation, "emulate")
	}
}

func (p *Pack) loop() {
	defer close(p.done)
	for {
		if p.drain(context.Background()) {
			continue
		}
		// ponytail: one-second fallback poll covers delayed/failed SQS visibility; add per-queue deadlines if throughput demands it.
		select {
		case <-p.wake:
		case <-p.deps.Clock.After(time.Second):
		case <-p.stop:
			return
		}
	}
}

func (p *Pack) drain(ctx context.Context) bool {
	more := false
	scopes, err := p.deps.Store.Scopes(ctx)
	if err != nil {
		return false
	}
	for _, identity := range scopes {
		kvs, _, err := p.deps.Store.Scope(identity.Account, identity.Region).Collection("pipe").List(ctx, "", "", 0)
		if err != nil {
			continue
		}
		for _, kv := range kvs {
			var pipe map[string]any
			if json.Unmarshal(kv.Value, &pipe) != nil || stringValue(pipe["CurrentState"]) != "RUNNING" {
				continue
			}
			source, target := stringValue(pipe["Source"]), stringValue(pipe["Target"])
			if !strings.Contains(source, ":sqs:") || target == "" {
				continue
			}
			queue := source[strings.LastIndex(source, ":")+1:]
			batchSize := sourceBatchSize(pipe)
			request := &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{
				"QueueName": queue, "MaxNumberOfMessages": batchSize, "MessageAttributeNames": []any{"All"},
			}}
			response, err := sqs.New(p.deps).Invoke(ctx, request)
			if err != nil {
				continue
			}
			messages, _ := response.Output["Messages"].([]any)
			if len(messages) == batchSize {
				more = true
			}
			p.processBatch(ctx, identity, pipe, source, queue, messages)
		}
	}
	return more
}

func (p *Pack) processBatch(ctx context.Context, identity spi.Identity, pipe map[string]any, source, queue string, messages []any) {
	var records []any
	var matched []map[string]any
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		record := sqsRecord(message, source, identity.Region)
		if !matchesFilters(pipe["FilterCriteria"], record) {
			p.deleteMessage(ctx, identity, queue, stringValue(message["ReceiptHandle"]))
			continue
		}
		records = append(records, record)
		matched = append(matched, message)
	}
	if len(records) == 0 {
		return
	}
	target := stringValue(pipe["Target"])
	config := map[string]any{"Arn": target}
	if parameters, ok := pipe["TargetParameters"].(map[string]any); ok {
		if sqsParameters, ok := parameters["SqsQueueParameters"].(map[string]any); ok {
			config["SqsParameters"] = sqsParameters
		}
	}
	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = p.inputPayload(pipe, "EnrichmentParameters", record)
	}
	deliveryEvents, enriched, ok := p.enrich(ctx, identity, pipe, payloads, records)
	if !ok {
		return
	}
	if len(deliveryEvents) == 0 {
		for _, message := range matched {
			p.deleteMessage(ctx, identity, queue, stringValue(message["ReceiptHandle"]))
		}
		return
	}
	payloads = make([][]byte, len(deliveryEvents))
	for i, event := range deliveryEvents {
		payloads[i] = p.inputPayload(pipe, "TargetParameters", event)
	}
	if strings.Contains(target, ":lambda:") {
		failed, ok := p.invokeLambda(ctx, identity, target, batchPayload(payloads), matched)
		if !ok {
			return
		}
		for _, message := range matched {
			if !failed[stringValue(message["MessageId"])] {
				p.deleteMessage(ctx, identity, queue, stringValue(message["ReceiptHandle"]))
			}
		}
		return
	}
	allDelivered := true
	for i, payload := range payloads {
		if events.DeliverTarget(ctx, p.deps, identity, target, config, payload) == nil {
			if !enriched {
				p.deleteMessage(ctx, identity, queue, stringValue(matched[i]["ReceiptHandle"]))
			}
		} else {
			allDelivered = false
		}
	}
	if enriched && allDelivered {
		for _, message := range matched {
			p.deleteMessage(ctx, identity, queue, stringValue(message["ReceiptHandle"]))
		}
	}
}

func (p *Pack) enrich(ctx context.Context, identity spi.Identity, pipe map[string]any, inputs [][]byte, fallback []any) ([]any, bool, bool) {
	arn := stringValue(pipe["Enrichment"])
	if arn == "" {
		return fallback, false, true
	}
	if !strings.Contains(arn, ":lambda:") {
		return nil, true, false
	}
	raw, err := p.invokeLambdaPayload(ctx, identity, arn, batchPayload(inputs))
	if err != nil {
		return nil, true, false
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || bytes.Equal(trimmed, []byte(`""`)) || bytes.Equal(trimmed, []byte("{}")) || bytes.Equal(trimmed, []byte("[]")) {
		return nil, true, true
	}
	var output any
	if json.Unmarshal(trimmed, &output) != nil {
		return []any{[]byte(trimmed)}, true, true
	}
	if batch, ok := output.([]any); ok {
		return batch, true, true
	}
	return []any{output}, true, true
}

func (p *Pack) inputPayload(pipe map[string]any, parameterName string, event any) []byte {
	parameters, _ := pipe[parameterName].(map[string]any)
	template := stringValue(parameters["InputTemplate"])
	if template == "" {
		if payload, ok := event.([]byte); ok {
			return payload
		}
		payload, _ := json.Marshal(event)
		return payload
	}
	eventJSON := event
	if record, ok := event.(map[string]any); ok {
		eventJSON = clone(record)
		if body := stringValue(record["body"]); json.Valid([]byte(body)) {
			var decoded any
			_ = json.Unmarshal([]byte(body), &decoded)
			eventJSON.(map[string]any)["body"] = decoded
		}
	}
	for {
		start := strings.IndexByte(template, '<')
		if start < 0 {
			break
		}
		end := strings.IndexByte(template[start:], '>')
		if end < 0 {
			break
		}
		end += start
		name := template[start+1 : end]
		var value any
		switch name {
		case "aws.pipes.pipe-arn":
			value = pipe["Arn"]
		case "aws.pipes.pipe-name":
			value = pipe["Name"]
		case "aws.pipes.source-arn":
			value = pipe["Source"]
		case "aws.pipes.enrichment-arn":
			value = pipe["Enrichment"]
		case "aws.pipes.target-arn":
			value = pipe["Target"]
		case "aws.pipes.event.ingestion-time":
			value = p.deps.Clock.Now().UTC().Format(time.RFC3339Nano)
		case "aws.pipes.event":
			value = event
		case "aws.pipes.event.json":
			value = eventJSON
		default:
			value = events.EventPath(eventJSON, name)
		}
		raw, _ := json.Marshal(value)
		if insideJSONString(template, start) {
			if len(raw) >= 2 && raw[0] == '"' {
				raw = raw[1 : len(raw)-1]
			} else {
				raw, _ = json.Marshal(string(raw))
				raw = raw[1 : len(raw)-1]
			}
		}
		template = template[:start] + string(raw) + template[end+1:]
	}
	return []byte(template)
}

func (p *Pack) invokeLambda(ctx context.Context, identity spi.Identity, arn string, payload []byte, messages []map[string]any) (map[string]bool, bool) {
	raw, err := p.invokeLambdaPayload(ctx, identity, arn, payload)
	if err != nil {
		return nil, false
	}
	var output map[string]any
	if json.Unmarshal(raw, &output) != nil || output["batchItemFailures"] == nil {
		return map[string]bool{}, true
	}
	items, ok := output["batchItemFailures"].([]any)
	if !ok {
		return nil, false
	}
	known := map[string]bool{}
	for _, message := range messages {
		known[stringValue(message["MessageId"])] = true
	}
	failed := map[string]bool{}
	for _, item := range items {
		entry, _ := item.(map[string]any)
		id := stringValue(entry["itemIdentifier"])
		if id == "" || !known[id] {
			return nil, false
		}
		failed[id] = true
	}
	return failed, true
}

func (p *Pack) invokeLambdaPayload(ctx context.Context, identity spi.Identity, arn string, payload []byte) (json.RawMessage, error) {
	_, name, ok := strings.Cut(arn, ":function:")
	if !ok {
		return nil, validation("Invalid Lambda ARN.")
	}
	if index := strings.IndexByte(name, ':'); index >= 0 {
		name = name[:index]
	}
	response, err := lambda.New(p.deps).Invoke(ctx, &spi.Request{
		Identity: identity, Operation: "Invoke", Input: map[string]any{"FunctionName": name}, Body: io.NopCloser(bytes.NewReader(payload)),
	})
	if err != nil {
		return nil, err
	}
	raw, _ := response.Output["Payload"].(json.RawMessage)
	return raw, nil
}

func batchPayload(payloads [][]byte) []byte {
	batch := make([]json.RawMessage, len(payloads))
	for i, payload := range payloads {
		if json.Valid(payload) {
			batch[i] = payload
		} else {
			batch[i], _ = json.Marshal(string(payload))
		}
	}
	payload, _ := json.Marshal(batch)
	return payload
}

func insideJSONString(value string, end int) bool {
	quoted, escaped := false, false
	for _, char := range value[:end] {
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' {
			escaped = true
		} else if char == '"' {
			quoted = !quoted
		}
	}
	return quoted
}

func (p *Pack) deleteMessage(ctx context.Context, identity spi.Identity, queue, handle string) {
	_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "DeleteMessage", Input: map[string]any{"QueueName": queue, "ReceiptHandle": handle}})
}

func sqsRecord(message map[string]any, source, region string) map[string]any {
	body := stringValue(message["Body"])
	sum := md5.Sum([]byte(body))
	attributes, _ := message["Attributes"].(map[string]any)
	messageAttributes, _ := message["MessageAttributes"].(map[string]any)
	return map[string]any{
		"messageId": stringValue(message["MessageId"]), "receiptHandle": stringValue(message["ReceiptHandle"]),
		"body": body, "attributes": attributes, "messageAttributes": messageAttributes,
		"md5OfBody": hex.EncodeToString(sum[:]), "eventSource": "aws:sqs", "eventSourceARN": source, "awsRegion": region,
	}
}

func matchesFilters(raw any, event map[string]any) bool {
	criteria, _ := raw.(map[string]any)
	filters, _ := criteria["Filters"].([]any)
	if len(filters) == 0 {
		return true
	}
	filterEvent := clone(event)
	if body := stringValue(event["body"]); json.Valid([]byte(body)) {
		var decoded any
		_ = json.Unmarshal([]byte(body), &decoded)
		filterEvent["body"] = decoded
	}
	for _, rawFilter := range filters {
		filter, _ := rawFilter.(map[string]any)
		if events.MatchEventPattern(first(filter, "Pattern"), filterEvent) {
			return true
		}
	}
	return false
}

func sourceBatchSize(pipe map[string]any) int {
	parameters, _ := pipe["SourceParameters"].(map[string]any)
	sqsParameters, _ := parameters["SqsQueueParameters"].(map[string]any)
	size := intValue(sqsParameters["BatchSize"])
	if size < 1 {
		return 10
	}
	if size > 10 {
		return 10
	}
	return size
}

func (p *Pack) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func getRecord(ctx context.Context, collection spi.Collection, key string) (map[string]any, bool, error) {
	b, ok, err := collection.Get(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	var rec map[string]any
	if err := json.Unmarshal(b, &rec); err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func putRecord(ctx context.Context, collection spi.Collection, key string, rec map[string]any) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return collection.Put(ctx, key, b)
}

func clone(input map[string]any) map[string]any {
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func first(input map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := input[key].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func stringValue(value any) string { valueString, _ := value.(string); return valueString }

func intValue(value any) int {
	switch value := value.(type) {
	case int:
		return value
	case float64:
		return int(value)
	}
	return 0
}

func validation(message string) *spi.Fault {
	return &spi.Fault{Code: "ValidationException", Message: message, HTTPStatus: 400, Fault: "client"}
}

func conflict(message string) *spi.Fault {
	return &spi.Fault{Code: "ConflictException", Message: message, HTTPStatus: 409, Fault: "client"}
}

func notFound() *spi.Fault {
	return &spi.Fault{Code: "NotFoundException", Message: "Pipe does not exist.", HTTPStatus: 400, Fault: "client"}
}

var _ interface{ Close() error } = (*Pack)(nil)
