// Package pipes emulates EventBridge Pipes control plane and SQS source delivery.
package pipes

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
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
	if strings.Contains(target, ":lambda:") {
		payload, _ := json.Marshal(records)
		if events.DeliverTarget(ctx, p.deps, identity, target, config, payload) == nil {
			for _, message := range matched {
				p.deleteMessage(ctx, identity, queue, stringValue(message["ReceiptHandle"]))
			}
		}
		return
	}
	for i, record := range records {
		payload, _ := json.Marshal(record)
		if events.DeliverTarget(ctx, p.deps, identity, target, config, payload) == nil {
			p.deleteMessage(ctx, identity, queue, stringValue(matched[i]["ReceiptHandle"]))
		}
	}
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
