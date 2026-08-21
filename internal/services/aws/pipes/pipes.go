// Package pipes emulates EventBridge Pipes control plane and source delivery.
package pipes

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigateway"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/events"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/kinesis"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/states"
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
	cancels   []func()
	closeOnce sync.Once
}

// New constructs the pack and resumes running pipes.
func New(d spi.Deps) *Pack {
	p := &Pack{deps: d, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if d.Store == nil || d.Clock == nil || d.Bus == nil {
		close(p.done)
		return p
	}
	for _, topic := range []string{"sqs", "kinesis", "dynamodb-stream"} {
		p.cancels = append(p.cancels, d.Bus.Subscribe(topic, func(context.Context, []byte) { p.notify() }))
	}
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
		for _, cancel := range p.cancels {
			cancel()
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
		if err := validateSource(req.Input); err != nil {
			return nil, err
		}
		if err := validateTarget(req.Input); err != nil {
			return nil, err
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
		if source, exists := req.Input["Source"]; exists && stringValue(source) != stringValue(rec["Source"]) {
			return nil, validation("Source cannot be updated.")
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
		if err := validateSource(rec); err != nil {
			return nil, err
		}
		if err := validateTarget(rec); err != nil {
			return nil, err
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
		_ = p.col(req, "pipecheckpoint").Delete(ctx, name)
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
			if target == "" {
				continue
			}
			switch {
			case strings.Contains(source, ":sqs:"):
				more = p.drainSQS(ctx, identity, pipe, source) || more
			case strings.Contains(source, ":kinesis:"):
				more = p.drainKinesis(ctx, identity, pipe, source) || more
			case strings.Contains(source, ":dynamodb:") && strings.Contains(source, "/stream/"):
				more = p.drainDynamoDB(ctx, identity, pipe, source) || more
			}
		}
	}
	return more
}

func (p *Pack) drainSQS(ctx context.Context, identity spi.Identity, pipe map[string]any, source string) bool {
	queue := source[strings.LastIndex(source, ":")+1:]
	batchSize := sourceBatchSize(pipe)
	response, err := sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "ReceiveMessage", Input: map[string]any{
		"QueueName": queue, "MaxNumberOfMessages": batchSize, "MessageAttributeNames": []any{"All"},
	}})
	if err != nil {
		return false
	}
	messages, _ := response.Output["Messages"].([]any)
	p.processBatch(ctx, identity, pipe, source, queue, messages)
	return len(messages) == batchSize
}

func (p *Pack) drainKinesis(ctx context.Context, identity spi.Identity, pipe map[string]any, source string) bool {
	stream := source[strings.LastIndex(source, "/")+1:]
	return p.drainStream(ctx, identity, pipe, kinesis.New(p.deps), map[string]any{
		"StreamName": stream, "ShardId": "shardId-000000000000",
	}, "KinesisStreamParameters", "StartingSequenceNumber", func(record map[string]any) string {
		return stringValue(record["SequenceNumber"])
	}, func(record map[string]any) map[string]any {
		return kinesisRecord(record, source, identity.Region, stringValue(pipe["RoleArn"]))
	})
}

func (p *Pack) drainDynamoDB(ctx context.Context, identity spi.Identity, pipe map[string]any, source string) bool {
	return p.drainStream(ctx, identity, pipe, dynamodb.New(p.deps), map[string]any{
		"StreamArn": source, "ShardId": "shardId-000000000000",
	}, "DynamoDBStreamParameters", "SequenceNumber", func(record map[string]any) string {
		return stringValue(record["dynamodb"].(map[string]any)["SequenceNumber"])
	}, func(record map[string]any) map[string]any {
		return dynamodbRecord(record, source)
	})
}

func (p *Pack) drainStream(ctx context.Context, identity spi.Identity, pipe map[string]any, client spi.BehaviorPack, iteratorInput map[string]any, parameterName, sequenceField string, sequence func(map[string]any) string, eventFor func(map[string]any) map[string]any) bool {
	request := &spi.Request{Identity: identity, Input: iteratorInput}
	checkpoint := p.deps.Store.Scope(identity.Account, identity.Region).Collection("pipecheckpoint")
	iterator, ok, err := checkpoint.Get(ctx, stringValue(pipe["Name"]))
	if err != nil {
		return false
	}
	streamParameters := sourceParameters(pipe, parameterName)
	if !ok {
		request.Operation = "GetShardIterator"
		request.Input["ShardIteratorType"] = stringValue(streamParameters["StartingPosition"])
		request.Input["Timestamp"] = streamParameters["StartingPositionTimestamp"]
		response, invokeErr := client.Invoke(ctx, request)
		if invokeErr != nil {
			return false
		}
		iterator = []byte(stringValue(response.Output["ShardIterator"]))
		if checkpoint.Put(ctx, stringValue(pipe["Name"]), iterator) != nil {
			return false
		}
	}
	request.Operation = "GetRecords"
	request.Input = map[string]any{"ShardIterator": string(iterator), "Limit": streamBatchSize(pipe, parameterName)}
	response, err := client.Invoke(ctx, request)
	if err != nil {
		return false
	}
	records, _ := response.Output["Records"].([]any)
	if len(records) == 0 {
		_ = checkpoint.Put(ctx, stringValue(pipe["Name"]), []byte(stringValue(response.Output["NextShardIterator"])))
		return false
	}
	events, ids := make([]any, len(records)), make([]string, len(records))
	for i, raw := range records {
		record, _ := raw.(map[string]any)
		event := eventFor(record)
		events[i], ids[i] = event, stringValue(event["eventID"])
	}
	succeeded := p.processRecords(ctx, identity, pipe, events, ids)
	for i, id := range ids {
		if succeeded[id] {
			continue
		}
		request.Operation = "GetShardIterator"
		request.Input = clone(iteratorInput)
		request.Input["ShardIteratorType"], request.Input[sequenceField] = "AT_SEQUENCE_NUMBER", sequence(records[i].(map[string]any))
		if retry, invokeErr := client.Invoke(ctx, request); invokeErr == nil {
			_ = checkpoint.Put(ctx, stringValue(pipe["Name"]), []byte(stringValue(retry.Output["ShardIterator"])))
		}
		return false
	}
	if checkpoint.Put(ctx, stringValue(pipe["Name"]), []byte(stringValue(response.Output["NextShardIterator"]))) != nil {
		return false
	}
	return len(records) == streamBatchSize(pipe, parameterName)
}

func (p *Pack) processBatch(ctx context.Context, identity spi.Identity, pipe map[string]any, source, queue string, messages []any) map[string]bool {
	records, ids := make([]any, 0, len(messages)), make([]string, 0, len(messages))
	byID := map[string]map[string]any{}
	for _, raw := range messages {
		message, _ := raw.(map[string]any)
		id := stringValue(message["MessageId"])
		records, ids, byID[id] = append(records, sqsRecord(message, source, identity.Region)), append(ids, id), message
	}
	succeeded := p.processRecords(ctx, identity, pipe, records, ids)
	for id := range succeeded {
		p.deleteMessage(ctx, identity, queue, stringValue(byID[id]["ReceiptHandle"]))
	}
	return succeeded
}

func (p *Pack) processRecords(ctx context.Context, identity spi.Identity, pipe map[string]any, records []any, ids []string) map[string]bool {
	succeeded := map[string]bool{}
	matchedRecords, matchedIDs := make([]any, 0, len(records)), make([]string, 0, len(ids))
	for i, record := range records {
		if matchesFilters(pipe["FilterCriteria"], record.(map[string]any)) {
			matchedRecords, matchedIDs = append(matchedRecords, record), append(matchedIDs, ids[i])
		} else {
			succeeded[ids[i]] = true
		}
	}
	if len(matchedRecords) == 0 {
		return succeeded
	}
	target := stringValue(pipe["Target"])
	config := map[string]any{"Arn": target}
	if parameters, ok := pipe["TargetParameters"].(map[string]any); ok {
		if sqsParameters, ok := parameters["SqsQueueParameters"].(map[string]any); ok {
			config["SqsParameters"] = sqsParameters
		}
		if stateMachineParameters, ok := parameters["StateMachineParameters"].(map[string]any); ok {
			config["StateMachineParameters"] = stateMachineParameters
		}
	}
	if strings.Contains(target, ":states:") {
		parameters, _ := config["StateMachineParameters"].(map[string]any)
		if stringValue(parameters["InvocationType"]) == "" {
			config["StateMachineParameters"] = map[string]any{"InvocationType": "REQUEST_RESPONSE"}
		}
	}
	payloads := make([][]byte, len(matchedRecords))
	for i, record := range matchedRecords {
		payloads[i] = p.inputPayload(pipe, "EnrichmentParameters", record)
	}
	deliveryEvents, enriched, ok := p.enrich(ctx, identity, pipe, payloads, matchedRecords)
	if !ok {
		return succeeded
	}
	if len(deliveryEvents) == 0 {
		for _, id := range matchedIDs {
			succeeded[id] = true
		}
		return succeeded
	}
	payloads = make([][]byte, len(deliveryEvents))
	for i, event := range deliveryEvents {
		payloads[i] = p.inputPayload(pipe, "TargetParameters", event)
	}
	if strings.Contains(target, ":lambda:") {
		failed, ok := p.invokeLambda(ctx, identity, target, batchPayload(payloads), matchedIDs)
		if !ok {
			return succeeded
		}
		for _, id := range matchedIDs {
			if !failed[id] {
				succeeded[id] = true
			}
		}
		return succeeded
	}
	allDelivered := true
	for i, payload := range payloads {
		if parameters, ok := pipe["TargetParameters"].(map[string]any); ok {
			config["HttpParameters"] = resolvedHTTPParameters(parameters["HttpParameters"], decodedEvent(deliveryEvents[i]))
		}
		if events.DeliverTarget(ctx, p.deps, identity, target, config, payload) == nil {
			if !enriched {
				succeeded[matchedIDs[i]] = true
			}
		} else {
			allDelivered = false
		}
	}
	if enriched && allDelivered {
		for _, id := range matchedIDs {
			succeeded[id] = true
		}
	}
	return succeeded
}

func (p *Pack) enrich(ctx context.Context, identity spi.Identity, pipe map[string]any, inputs [][]byte, fallback []any) ([]any, bool, bool) {
	arn := stringValue(pipe["Enrichment"])
	if arn == "" {
		return fallback, false, true
	}
	payload := batchPayload(inputs)
	var raw []byte
	switch {
	case strings.Contains(arn, ":lambda:"):
		response, err := p.invokeLambdaPayload(ctx, identity, arn, payload)
		if err != nil {
			return nil, true, false
		}
		raw = response
	case strings.Contains(arn, ":states:"):
		response, err := states.New(p.deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "StartSyncExecution", Input: map[string]any{"stateMachineArn": arn, "input": string(payload)}})
		if err != nil || stringValue(response.Output["status"]) != "SUCCEEDED" {
			return nil, true, false
		}
		raw = []byte(stringValue(response.Output["output"]))
	case strings.Contains(arn, ":execute-api:"):
		parameterEvent := any(fallback)
		if len(fallback) == 1 {
			parameterEvent = decodedEvent(fallback[0])
		}
		response, err := p.invokeAPIGateway(ctx, identity, pipe, arn, payload, parameterEvent)
		if err != nil {
			return nil, true, false
		}
		raw = response
	case strings.Contains(arn, ":api-destination/"):
		parameterEvent := any(fallback)
		if len(fallback) == 1 {
			parameterEvent = decodedEvent(fallback[0])
		}
		parameters, _ := pipe["EnrichmentParameters"].(map[string]any)
		response, err := events.InvokeAPIDestination(ctx, p.deps, identity, arn, resolvedHTTPParameters(parameters["HttpParameters"], parameterEvent), payload)
		if err != nil {
			return nil, true, false
		}
		raw = response
	default:
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

func (p *Pack) invokeAPIGateway(ctx context.Context, identity spi.Identity, pipe map[string]any, arn string, payload []byte, event any) ([]byte, error) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 {
		return nil, validation("Invalid API Gateway enrichment ARN.")
	}
	endpoint := strings.SplitN(parts[5], "/", 4)
	if len(endpoint) < 3 || endpoint[0] == "" || endpoint[1] == "" || endpoint[2] == "" {
		return nil, validation("Invalid API Gateway enrichment ARN.")
	}
	path := "/"
	if len(endpoint) == 4 && endpoint[3] != "" {
		path += endpoint[3]
	}
	parameters, _ := pipe["EnrichmentParameters"].(map[string]any)
	httpParameters := resolvedHTTPParameters(parameters["HttpParameters"], event)
	if values, ok := httpParameters["PathParameterValues"].([]any); ok {
		for _, value := range values {
			path = strings.Replace(path, "*", url.PathEscape(httpParameter(value, event)), 1)
		}
	}
	request, err := http.NewRequest(endpoint[2], "http://local/restapis/"+endpoint[0]+"/"+endpoint[1]+"/_user_request_"+path, nil)
	if err != nil {
		return nil, err
	}
	if headers, ok := httpParameters["HeaderParameters"].(map[string]any); ok {
		for key, value := range headers {
			request.Header.Set(key, httpParameter(value, event))
		}
	}
	query := request.URL.Query()
	if values, ok := httpParameters["QueryStringParameters"].(map[string]any); ok {
		for key, value := range values {
			query.Set(key, httpParameter(value, event))
		}
	}
	request.URL.RawQuery = query.Encode()
	response, err := apigateway.New(p.deps).Invoke(ctx, &spi.Request{Identity: identity, Operation: "ExecuteApi", HTTP: request, Body: io.NopCloser(bytes.NewReader(payload))})
	if err != nil {
		return nil, err
	}
	status := response.Status
	if status == 0 {
		status = http.StatusOK
	}
	if status < 200 || status >= 300 {
		return nil, validation("API Gateway enrichment failed.")
	}
	if response.Stream == nil {
		return json.Marshal(response.Output)
	}
	defer response.Stream.Close()
	return io.ReadAll(response.Stream)
}

func resolvedHTTPParameters(raw, event any) map[string]any {
	parameters, _ := raw.(map[string]any)
	resolved := map[string]any{}
	if values, ok := parameters["PathParameterValues"].([]any); ok {
		paths := make([]any, len(values))
		for i, value := range values {
			paths[i] = httpParameter(value, event)
		}
		resolved["PathParameterValues"] = paths
	}
	for _, name := range []string{"HeaderParameters", "QueryStringParameters"} {
		if values, ok := parameters[name].(map[string]any); ok {
			items := map[string]any{}
			for key, value := range values {
				items[key] = httpParameter(value, event)
			}
			resolved[name] = items
		}
	}
	return resolved
}

func httpParameter(value, event any) string {
	parameter := fmt.Sprint(value)
	if strings.HasPrefix(parameter, "$") {
		return fmt.Sprint(events.EventPath(event, parameter))
	}
	return parameter
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
	eventJSON := decodedEvent(event)
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

func decodedEvent(event any) any {
	record, ok := event.(map[string]any)
	if !ok {
		return event
	}
	decoded := clone(record)
	if body := stringValue(record["body"]); json.Valid([]byte(body)) {
		var value any
		_ = json.Unmarshal([]byte(body), &value)
		decoded["body"] = value
	}
	return decoded
}

func (p *Pack) invokeLambda(ctx context.Context, identity spi.Identity, arn string, payload []byte, ids []string) (map[string]bool, bool) {
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
	for _, id := range ids {
		known[id] = true
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

func kinesisRecord(record map[string]any, source, region, role string) map[string]any {
	sequence := stringValue(record["SequenceNumber"])
	return map[string]any{
		"kinesisSchemaVersion": "1.0", "partitionKey": record["PartitionKey"], "sequenceNumber": sequence,
		"data": record["Data"], "approximateArrivalTimestamp": record["ApproximateArrivalTimestamp"],
		"eventSource": "aws:kinesis", "eventVersion": "1.0", "eventID": "shardId-000000000000:" + sequence,
		"eventName": "aws:kinesis:record", "invokeIdentityArn": role, "awsRegion": region, "eventSourceARN": source,
	}
}

func dynamodbRecord(record map[string]any, source string) map[string]any {
	event := clone(record)
	event["eventVersion"] = "1.0"
	event["eventSourceARN"] = strings.Split(source, "/stream/")[0]
	return event
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
	sqsParameters := sourceParameters(pipe, "SqsQueueParameters")
	size := intValue(sqsParameters["BatchSize"])
	if size < 1 {
		return 10
	}
	if size > 10 {
		return 10
	}
	return size
}

func streamBatchSize(pipe map[string]any, parameterName string) int {
	size := intValue(sourceParameters(pipe, parameterName)["BatchSize"])
	if size < 1 {
		return 100
	}
	if size > 10000 {
		return 10000
	}
	return size
}

func sourceParameters(pipe map[string]any, name string) map[string]any {
	parameters, _ := pipe["SourceParameters"].(map[string]any)
	result, _ := parameters[name].(map[string]any)
	return result
}

func validateSource(pipe map[string]any) error {
	source := stringValue(pipe["Source"])
	parameterName, positions := "", map[string]bool{}
	switch {
	case strings.Contains(source, ":kinesis:"):
		parameterName, positions = "KinesisStreamParameters", map[string]bool{"TRIM_HORIZON": true, "LATEST": true, "AT_TIMESTAMP": true}
	case strings.Contains(source, ":dynamodb:") && strings.Contains(source, "/stream/"):
		parameterName, positions = "DynamoDBStreamParameters", map[string]bool{"TRIM_HORIZON": true, "LATEST": true}
	default:
		return nil
	}
	parameters := sourceParameters(pipe, parameterName)
	position := stringValue(parameters["StartingPosition"])
	if !positions[position] {
		return validation(parameterName + ".StartingPosition is invalid.")
	}
	if _, exists := parameters["BatchSize"]; exists {
		size := intValue(parameters["BatchSize"])
		if size < 1 || size > 10000 {
			return validation(parameterName + ".BatchSize must be between 1 and 10000.")
		}
	}
	return nil
}

func validateTarget(pipe map[string]any) error {
	if !strings.Contains(stringValue(pipe["Target"]), ":states:") {
		return nil
	}
	parameters, _ := pipe["TargetParameters"].(map[string]any)
	stateMachine, _ := parameters["StateMachineParameters"].(map[string]any)
	invocation := stringValue(stateMachine["InvocationType"])
	if invocation != "" && invocation != "REQUEST_RESPONSE" && invocation != "FIRE_AND_FORGET" {
		return validation("StateMachineParameters.InvocationType must be REQUEST_RESPONSE or FIRE_AND_FORGET.")
	}
	return nil
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
