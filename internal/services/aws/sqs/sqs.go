// Package sqs is the emulate-tier SQS pack.
package sqs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sqs", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements SQS.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sqs" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
		"SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags",
		"AddPermission", "RemovePermission", "ListDeadLetterSourceQueues",
		"StartMessageMoveTask", "CancelMessageMoveTask", "ListMessageMoveTasks"}
	return core
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	base := advertise(req)
	if queueScoped(req.Operation) && !p.queueExists(ctx, req, queueName(req)) {
		return nil, queueMissing()
	}
	switch req.Operation {
	case "CreateQueue":
		name := str(req.Input["QueueName"])
		url := fmt.Sprintf("%s/%s/%s", base, req.Identity.Account, name)
		attrs := asMap(req.Input["Attributes"])
		if strings.HasSuffix(name, ".fifo") {
			attrs["FifoQueue"] = "true"
		}
		meta, _ := json.Marshal(map[string]any{"url": url, "name": name, "attrs": attrs, "seq": 0})
		_ = p.col(req, "queues").Put(ctx, name, meta)
		if len(attrs) > 0 {
			ab, _ := json.Marshal(attrs)
			_ = p.col(req, "qattrs").Put(ctx, name, ab)
		}
		return &spi.Response{Output: map[string]any{"QueueUrl": url}}, nil
	case "GetQueueUrl":
		name := str(req.Input["QueueName"])
		b, ok, _ := p.col(req, "queues").Get(ctx, name)
		if !ok {
			return nil, queueMissing()
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		return &spi.Response{Output: map[string]any{"QueueUrl": m["url"]}}, nil
	case "ListQueues":
		kvs, _, _ := p.col(req, "queues").List(ctx, "", "", 0)
		var urls []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			urls = append(urls, m["url"])
		}
		return &spi.Response{Output: map[string]any{"QueueUrls": urls}}, nil
	case "DeleteQueue":
		name := queueName(req)
		_ = p.col(req, "queues").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "SendMessage":
		return p.send(ctx, req)
	case "ReceiveMessage":
		return p.receive(ctx, req)
	case "DeleteMessage":
		name := queueName(req)
		_ = p.col(req, "msgs:"+name).Delete(ctx, str(req.Input["ReceiptHandle"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetQueueAttributes":
		name := queueName(req)
		n := p.countMsgs(ctx, req, name)
		attrs := map[string]any{"ApproximateNumberOfMessages": fmt.Sprintf("%d", n), "VisibilityTimeout": "30"}
		if b, ok, _ := p.col(req, "qattrs").Get(ctx, name); ok {
			var extra map[string]any
			_ = json.Unmarshal(b, &extra)
			for k, v := range extra {
				attrs[k] = v
			}
		}
		return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
	case "SetQueueAttributes":
		name := queueName(req)
		attrs := asMap(req.Input["Attributes"])
		b, _ := json.Marshal(attrs)
		_ = p.col(req, "qattrs").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PurgeQueue":
		name := queueName(req)
		kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
		for _, kv := range kvs {
			_ = p.col(req, "msgs:"+name).Delete(ctx, kv.Key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagQueue":
		name := queueName(req)
		tags := asMap(req.Input["Tags"])
		b, _ := json.Marshal(tags)
		_ = p.col(req, "qtags").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagQueue":
		name := queueName(req)
		b, ok, _ := p.col(req, "qtags").Get(ctx, name)
		cur := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &cur)
		}
		if keys, ok := req.Input["TagKeys"].([]any); ok {
			for _, k := range keys {
				delete(cur, str(k))
			}
		}
		nb, _ := json.Marshal(cur)
		_ = p.col(req, "qtags").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListQueueTags":
		name := queueName(req)
		b, ok, _ := p.col(req, "qtags").Get(ctx, name)
		tags := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "SendMessageBatch":
		entries, _ := req.Input["Entries"].([]any)
		var ok []any
		var failed []any
		for _, e := range entries {
			m := asMap(e)
			sub := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Input: map[string]any{
				"QueueUrl": req.Input["QueueUrl"], "QueueName": queueName(req), "MessageBody": str(m["MessageBody"]),
				"MessageGroupId": m["MessageGroupId"], "MessageDeduplicationId": m["MessageDeduplicationId"],
				"MessageAttributes": m["MessageAttributes"], "DelaySeconds": m["DelaySeconds"],
			}, Operation: "SendMessage"}
			resp, err := p.Invoke(ctx, sub)
			if err != nil {
				fault, _ := err.(*spi.Fault)
				entry := map[string]any{"Id": m["Id"], "Code": "InternalError", "Message": err.Error(), "SenderFault": false}
				if fault != nil {
					entry["Code"], entry["Message"], entry["SenderFault"] = fault.Code, fault.Message, fault.Fault == "client"
				}
				failed = append(failed, entry)
				continue
			}
			ok = append(ok, map[string]any{"Id": m["Id"], "MessageId": resp.Output["MessageId"], "MD5OfMessageBody": resp.Output["MD5OfMessageBody"]})
		}
		return &spi.Response{Output: map[string]any{"Successful": ok, "Failed": failed}}, nil
	case "DeleteMessageBatch":
		name := queueName(req)
		entries, _ := req.Input["Entries"].([]any)
		var ok []any
		for _, e := range entries {
			m := asMap(e)
			_ = p.col(req, "msgs:"+name).Delete(ctx, str(m["ReceiptHandle"]))
			ok = append(ok, map[string]any{"Id": m["Id"]})
		}
		return &spi.Response{Output: map[string]any{"Successful": ok}}, nil
	case "ChangeMessageVisibility", "ChangeMessageVisibilityBatch":
		name := queueName(req)
		if req.Operation == "ChangeMessageVisibility" {
			p.setVis(ctx, req, name, str(req.Input["ReceiptHandle"]), str(req.Input["VisibilityTimeout"]))
		} else if entries, ok := req.Input["Entries"].([]any); ok {
			for _, e := range entries {
				m := asMap(e)
				p.setVis(ctx, req, name, str(m["ReceiptHandle"]), str(m["VisibilityTimeout"]))
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddPermission":
		return p.addPermission(ctx, req)
	case "RemovePermission":
		return p.removePermission(ctx, req)
	case "ListDeadLetterSourceQueues":
		return p.listDeadLetterSources(ctx, req)
	case "StartMessageMoveTask":
		return p.startMove(ctx, req)
	case "CancelMessageMoveTask":
		return p.cancelMove(ctx, req)
	case "ListMessageMoveTasks":
		return p.listMoves(ctx, req)
	default:
		return nil, spi.NotImplemented("aws.sqs", req.Operation, "emulate")
	}
}

func queueScoped(operation string) bool {
	switch operation {
	case "DeleteQueue", "GetQueueAttributes", "SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "PurgeQueue",
		"TagQueue", "UntagQueue", "ListQueueTags", "AddPermission", "RemovePermission", "ListDeadLetterSourceQueues":
		return true
	}
	return false
}

func (p *Pack) queueExists(ctx context.Context, req *spi.Request, name string) bool {
	_, ok, _ := p.col(req, "queues").Get(ctx, name)
	return ok
}

func queueMissing() *spi.Fault {
	return &spi.Fault{Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist.", HTTPStatus: 400, Fault: "client"}
}

func (p *Pack) countMsgs(ctx context.Context, req *spi.Request, name string) int {
	kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
	return len(kvs)
}

func (p *Pack) send(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	body := str(req.Input["MessageBody"])
	sum := md5.Sum([]byte(body))
	md5hex := hex.EncodeToString(sum[:])
	attrs := p.queueAttrs(ctx, req, name)
	now := p.deps.Clock.Now()
	group := str(req.Input["MessageGroupId"])
	dedup := str(req.Input["MessageDeduplicationId"])
	if strings.HasSuffix(name, ".fifo") && group == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "MessageGroupId", HTTPStatus: 400, Fault: "client"}
	}
	if dedup == "" && str(attrs["ContentBasedDeduplication"]) == "true" {
		dedup = md5hex
	}
	if strings.HasSuffix(name, ".fifo") && dedup == "" {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "MessageDeduplicationId is required when ContentBasedDeduplication is disabled.", HTTPStatus: 400, Fault: "client"}
	}
	delay := asInt(req.Input["DelaySeconds"])
	if req.Input["DelaySeconds"] == nil {
		delay = asInt(attrs["DelaySeconds"])
	}
	if delay < 0 || delay > 900 {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "DelaySeconds must be between 0 and 900.", HTTPStatus: 400, Fault: "client"}
	}
	if dedup != "" {
		if b, ok, _ := p.col(req, "dedup:"+name).Get(ctx, dedup); ok {
			var d map[string]any
			_ = json.Unmarshal(b, &d)
			until := int64(asFloat(d["until"]))
			if now.UnixNano() < until {
				return &spi.Response{Output: map[string]any{"MessageId": d["id"], "MD5OfMessageBody": d["md5"]}}, nil
			}
		}
	}
	id := p.deps.Rand.Hex(16)
	rh := p.deps.Rand.Hex(64)
	seq := p.nextSeq(ctx, req, name)
	msg := map[string]any{
		"id": id, "body": body, "handle": rh, "md5": md5hex,
		"group": group, "seq": seq,
		"visibleAt": now.Add(time.Duration(delay) * time.Second).UnixNano(), "receiveCount": 0,
		"attrs": req.Input["MessageAttributes"],
	}
	raw, _ := json.Marshal(msg)
	_ = p.col(req, "msgs:"+name).Put(ctx, rh, raw)
	if dedup != "" {
		db, _ := json.Marshal(map[string]any{"id": id, "md5": md5hex, "until": now.Add(5 * time.Minute).UnixNano()})
		_ = p.col(req, "dedup:"+name).Put(ctx, dedup, db)
	}
	return &spi.Response{Output: map[string]any{"MessageId": id, "MD5OfMessageBody": md5hex}}, nil
}

func (p *Pack) receive(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	max := asInt(req.Input["MaxNumberOfMessages"])
	if max <= 0 {
		max = 1
	}
	if max > 10 {
		max = 10
	}
	wait := time.Duration(asInt(req.Input["WaitTimeSeconds"])) * time.Second
	attrs := p.queueAttrs(ctx, req, name)
	vis := 30
	if v, ok := req.Input["VisibilityTimeout"]; ok && v != nil && v != "" {
		vis = asInt(v)
	} else if v, ok := attrs["VisibilityTimeout"]; ok {
		vis = asInt(v)
	}
	deadline := p.deps.Clock.Now().Add(wait)
	for {
		now := p.deps.Clock.Now()
		msgs := p.visible(ctx, req, name, now, max)
		if len(msgs) > 0 || wait == 0 || !now.Before(deadline) {
			out := make([]any, 0, len(msgs))
			for _, m := range msgs {
				p.afterReceive(ctx, req, name, m, vis)
				wire := map[string]any{"MessageId": m["id"], "ReceiptHandle": m["handle"], "Body": m["body"], "Attributes": map[string]any{"ApproximateReceiveCount": fmt.Sprintf("%v", m["receiveCount"])}}
				if want := req.Input["MessageAttributeNames"]; want != nil && m["attrs"] != nil {
					wire["MessageAttributes"] = filterMsgAttrs(m["attrs"], want)
				}
				out = append(out, wire)
			}
			return &spi.Response{Output: map[string]any{"Messages": out}}, nil
		}
		d := deadline.Sub(now)
		if d < 0 {
			d = 0
		}
		select {
		case <-p.deps.Clock.After(d):
		case <-ctx.Done():
			return &spi.Response{Output: map[string]any{"Messages": []any{}}}, nil
		}
	}
}

func (p *Pack) visible(ctx context.Context, req *spi.Request, name string, now time.Time, max int) []map[string]any {
	kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
	var cand []map[string]any
	inFlight := map[string]bool{}
	for _, kv := range kvs {
		var m map[string]any
		_ = json.Unmarshal(kv.Value, &m)
		if int64(asFloat(m["visibleAt"])) > now.UnixNano() {
			if g := str(m["group"]); g != "" {
				inFlight[g] = true
			}
			continue
		}
		cand = append(cand, m)
	}
	sortMsgs(cand)
	var out []map[string]any
	seenGroup := map[string]bool{}
	for _, m := range cand {
		g := str(m["group"])
		if g != "" && (inFlight[g] || seenGroup[g]) {
			continue
		}
		if g != "" {
			seenGroup[g] = true
		}
		out = append(out, m)
		if len(out) >= max {
			break
		}
	}
	return out
}

func (p *Pack) afterReceive(ctx context.Context, req *spi.Request, name string, m map[string]any, vis int) {
	n := asInt(m["receiveCount"]) + 1
	m["receiveCount"] = n
	rh := str(m["handle"])
	attrs := p.queueAttrs(ctx, req, name)
	max, dlq := redrive(attrs)
	if max > 0 && n > max && dlq != "" && dlq != name {
		_ = p.col(req, "msgs:"+name).Delete(ctx, rh)
		m["visibleAt"] = p.deps.Clock.Now().UnixNano()
		m["handle"] = p.deps.Rand.Hex(64)
		if str(m["origin"]) == "" {
			m["origin"] = name
		}
		raw, _ := json.Marshal(m)
		_ = p.col(req, "msgs:"+dlq).Put(ctx, str(m["handle"]), raw)
		return
	}
	m["visibleAt"] = p.deps.Clock.Now().Add(time.Duration(vis) * time.Second).UnixNano()
	raw, _ := json.Marshal(m)
	_ = p.col(req, "msgs:"+name).Put(ctx, rh, raw)
}

func (p *Pack) setVis(ctx context.Context, req *spi.Request, name, handle, timeout string) {
	b, ok, _ := p.col(req, "msgs:"+name).Get(ctx, handle)
	if !ok {
		return
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	sec := asInt(timeout)
	m["visibleAt"] = p.deps.Clock.Now().Add(time.Duration(sec) * time.Second).UnixNano()
	nb, _ := json.Marshal(m)
	_ = p.col(req, "msgs:"+name).Put(ctx, handle, nb)
}

func (p *Pack) queueAttrs(ctx context.Context, req *spi.Request, name string) map[string]any {
	out := map[string]any{}
	if b, ok, _ := p.col(req, "queues").Get(ctx, name); ok {
		var meta map[string]any
		_ = json.Unmarshal(b, &meta)
		for k, v := range asMap(meta["attrs"]) {
			out[k] = v
		}
	}
	if b, ok, _ := p.col(req, "qattrs").Get(ctx, name); ok {
		var extra map[string]any
		_ = json.Unmarshal(b, &extra)
		for k, v := range extra {
			out[k] = v
		}
	}
	return out
}

func (p *Pack) nextSeq(ctx context.Context, req *spi.Request, name string) int {
	b, ok, _ := p.col(req, "queues").Get(ctx, name)
	meta := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &meta)
	}
	n := asInt(meta["seq"]) + 1
	meta["seq"] = n
	nb, _ := json.Marshal(meta)
	_ = p.col(req, "queues").Put(ctx, name, nb)
	return n
}

func redrive(attrs map[string]any) (max int, dlq string) {
	raw := str(attrs["RedrivePolicy"])
	if raw == "" {
		return 0, ""
	}
	var p map[string]any
	_ = json.Unmarshal([]byte(raw), &p)
	max = asInt(p["maxReceiveCount"])
	arn := str(p["deadLetterTargetArn"])
	if i := strings.LastIndex(arn, ":"); i >= 0 {
		dlq = arn[i+1:]
	}
	return max, dlq
}

func sortMsgs(msgs []map[string]any) {
	for i := 0; i < len(msgs); i++ {
		for j := i + 1; j < len(msgs); j++ {
			if asInt(msgs[j]["seq"]) < asInt(msgs[i]["seq"]) {
				msgs[i], msgs[j] = msgs[j], msgs[i]
			}
		}
	}
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(n)
		return i
	}
	return 0
}

func asFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func advertise(req *spi.Request) string {
	if req.HTTP != nil && req.HTTP.Host != "" {
		scheme := "http"
		return scheme + "://" + req.HTTP.Host
	}
	return "http://127.0.0.1:4566"
}

func queueName(req *spi.Request) string {
	if n := str(req.Input["QueueName"]); n != "" {
		return n
	}
	u := str(req.Input["QueueUrl"])
	if i := strings.LastIndex(u, "/"); i >= 0 {
		return u[i+1:]
	}
	return u
}

func str(v any) string { s, _ := v.(string); return s }

func filterMsgAttrs(attrs, want any) any {
	m := asMap(attrs)
	names, _ := want.([]any)
	if len(names) == 0 {
		if s, ok := want.(string); ok && (s == "All" || s == ".") {
			return m
		}
		return m
	}
	for _, n := range names {
		if str(n) == "All" || str(n) == "." {
			return m
		}
	}
	out := map[string]any{}
	for _, n := range names {
		if v, ok := m[str(n)]; ok {
			out[str(n)] = v
		}
	}
	return out
}

func (p *Pack) addPermission(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	label := str(req.Input["Label"])
	if label == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "Label", HTTPStatus: 400, Fault: "client"}
	}
	accts := stringList(req.Input, "AWSAccountIds", "AWSAccountId")
	acts := stringList(req.Input, "Actions", "ActionName")
	if len(accts) == 0 || len(acts) == 0 {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "AWSAccountIds/Actions", HTTPStatus: 400, Fault: "client"}
	}
	pol := p.loadPolicy(ctx, req, name)
	stmts, _ := pol["Statement"].([]any)
	for _, s := range stmts {
		if str(asMap(s)["Sid"]) == label {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Label exists", HTTPStatus: 400, Fault: "client"}
		}
	}
	arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", req.Identity.Region, req.Identity.Account, name)
	actsOut := make([]any, 0, len(acts))
	for _, a := range acts {
		if !strings.Contains(a, ":") {
			a = "SQS:" + a
		}
		actsOut = append(actsOut, a)
	}
	acctsOut := make([]any, 0, len(accts))
	for _, a := range accts {
		acctsOut = append(acctsOut, a)
	}
	stmts = append(stmts, map[string]any{
		"Sid": label, "Effect": "Allow",
		"Principal": map[string]any{"AWS": acctsOut},
		"Action":    actsOut, "Resource": arn,
	})
	pol["Statement"] = stmts
	if err := p.savePolicy(ctx, req, name, pol); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) removePermission(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	label := str(req.Input["Label"])
	pol := p.loadPolicy(ctx, req, name)
	stmts, _ := pol["Statement"].([]any)
	kept := make([]any, 0, len(stmts))
	found := false
	for _, s := range stmts {
		if str(asMap(s)["Sid"]) == label {
			found = true
			continue
		}
		kept = append(kept, s)
	}
	if !found {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Label not found", HTTPStatus: 400, Fault: "client"}
	}
	pol["Statement"] = kept
	if err := p.savePolicy(ctx, req, name, pol); err != nil {
		return nil, err
	}
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) listDeadLetterSources(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	dlq := queueName(req)
	kvs, _, _ := p.col(req, "queues").List(ctx, "", "", 0)
	var urls []any
	for _, kv := range kvs {
		_, target := redrive(p.queueAttrs(ctx, req, kv.Key))
		if target != dlq {
			continue
		}
		var meta map[string]any
		_ = json.Unmarshal(kv.Value, &meta)
		if u := str(meta["url"]); u != "" {
			urls = append(urls, u)
		}
	}
	return &spi.Response{Output: map[string]any{"QueueUrls": urls}}, nil
}

func (p *Pack) startMove(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	src := arnQueue(str(req.Input["SourceArn"]))
	if src == "" {
		src = queueName(req)
	}
	if src == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "SourceArn", HTTPStatus: 400, Fault: "client"}
	}
	dst := arnQueue(str(req.Input["DestinationArn"]))
	kvs, _, _ := p.col(req, "msgs:"+src).List(ctx, "", "", 0)
	moved := 0
	now := p.deps.Clock.Now().UnixNano()
	for _, kv := range kvs {
		var m map[string]any
		_ = json.Unmarshal(kv.Value, &m)
		dest := dst
		if dest == "" {
			dest = str(m["origin"])
		}
		if dest == "" || dest == src {
			continue
		}
		_ = p.col(req, "msgs:"+src).Delete(ctx, kv.Key)
		m["handle"] = p.deps.Rand.Hex(64)
		m["visibleAt"] = now
		raw, _ := json.Marshal(m)
		_ = p.col(req, "msgs:"+dest).Put(ctx, str(m["handle"]), raw)
		moved++
	}
	handle := p.deps.Rand.Hex(16)
	rec := map[string]any{
		"TaskHandle": handle, "Status": "COMPLETED",
		"SourceArn": req.Input["SourceArn"], "DestinationArn": req.Input["DestinationArn"],
		"ApproximateNumberOfMessagesMoved": moved,
		"StartedTimestamp":                 p.deps.Clock.Now().Unix(),
		"source":                           src,
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "qmove").Put(ctx, handle, b)
	return &spi.Response{Output: map[string]any{"TaskHandle": handle}}, nil
}

func (p *Pack) cancelMove(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	h := str(req.Input["TaskHandle"])
	b, ok, _ := p.col(req, "qmove").Get(ctx, h)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "TaskHandle", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if str(rec["Status"]) != "RUNNING" {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "task is not in progress", HTTPStatus: 400, Fault: "client"}
	}
	rec["Status"] = "CANCELLING"
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "qmove").Put(ctx, h, nb)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) listMoves(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	src := arnQueue(str(req.Input["SourceArn"]))
	kvs, _, _ := p.col(req, "qmove").List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if src != "" && str(rec["source"]) != src {
			continue
		}
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{"Results": out}}, nil
}

func (p *Pack) loadPolicy(ctx context.Context, req *spi.Request, name string) map[string]any {
	attrs := p.queueAttrs(ctx, req, name)
	raw := str(attrs["Policy"])
	pol := map[string]any{"Version": "2012-10-17", "Statement": []any{}}
	if raw != "" {
		_ = json.Unmarshal([]byte(raw), &pol)
		if pol["Statement"] == nil {
			pol["Statement"] = []any{}
		}
	}
	return pol
}

func (p *Pack) savePolicy(ctx context.Context, req *spi.Request, name string, pol map[string]any) error {
	attrs := p.queueAttrs(ctx, req, name)
	b, _ := json.Marshal(pol)
	attrs["Policy"] = string(b)
	ab, _ := json.Marshal(attrs)
	return p.col(req, "qattrs").Put(ctx, name, ab)
}

func arnQueue(s string) string {
	if i := strings.LastIndex(s, ":"); i >= 0 && strings.HasPrefix(s, "arn:") {
		return s[i+1:]
	}
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}

func stringList(in map[string]any, names ...string) []string {
	for _, n := range names {
		if v, ok := in[n]; ok {
			switch t := v.(type) {
			case []any:
				out := make([]string, 0, len(t))
				for _, x := range t {
					if s := str(x); s != "" {
						out = append(out, s)
					}
				}
				return out
			case []string:
				return t
			case string:
				if t != "" {
					return []string{t}
				}
			}
		}
	}
	var out []string
	for k, v := range in {
		for _, n := range names {
			if strings.HasPrefix(k, n+".") {
				if s := str(v); s != "" {
					out = append(out, s)
				}
			}
		}
	}
	return out
}
