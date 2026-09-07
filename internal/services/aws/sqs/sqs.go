// Package sqs is the emulate-tier SQS pack.
package sqs

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

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
type Pack struct {
	deps   spi.Deps
	moveMu sync.Mutex
}

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
		if !validQueueName(name) {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Queue name must be 1 to 80 characters of alphanumeric characters, hyphens, underscores, and an optional .fifo suffix.", HTTPStatus: 400, Fault: "client"}
		}
		if deleted, ok, _ := p.col(req, "qdeleted").Get(ctx, name); ok {
			deletedAt, _ := strconv.ParseInt(string(deleted), 10, 64)
			if p.deps.Clock.Now().Sub(time.Unix(0, deletedAt)) < time.Minute {
				return nil, &spi.Fault{
					Code: "AWS.SimpleQueueService.QueueDeletedRecently", Message: "You must wait 60 seconds after deleting a queue before you can create another with the same name.",
					HTTPStatus: 400, Fault: "client",
				}
			}
			_ = p.col(req, "qdeleted").Delete(ctx, name)
		}
		attrs := asMap(req.Input["Attributes"])
		if fault := validateSSEAttributes(attrs); fault != nil {
			return nil, fault
		}
		if raw, present := attrs["RedrivePolicy"]; present && str(raw) != "" {
			if fault := validateRedrivePolicy(str(raw)); fault != nil {
				return nil, fault
			}
		}
		fifo, fifoSpecified := attrs["FifoQueue"]
		if strings.HasSuffix(name, ".fifo") && (!fifoSpecified || str(fifo) != "true") {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "FifoQueue must be specified as true for FIFO queues.", HTTPStatus: 400, Fault: "client"}
		}
		if !strings.HasSuffix(name, ".fifo") && fifoSpecified && str(fifo) == "true" {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Queue name must end in .fifo for FIFO queues", HTTPStatus: 400, Fault: "client"}
		}
		if !strings.HasSuffix(name, ".fifo") {
			for key, message := range map[string]string{
				"FifoQueue": "Unknown Attribute FifoQueue.", "ContentBasedDeduplication": "Unknown Attribute ContentBasedDeduplication.",
				"DeduplicationScope":  "You can specify the DeduplicationScope only when FifoQueue is set to true.",
				"FifoThroughputLimit": "You can specify the FifoThroughputLimit only when FifoQueue is set to true.",
			} {
				if _, present := attrs[key]; present {
					return nil, &spi.Fault{Code: "InvalidAttributeName", Message: message, HTTPStatus: 400, Fault: "client"}
				}
			}
		}
		if existing, ok, _ := p.col(req, "queues").Get(ctx, name); ok {
			var current map[string]any
			_ = json.Unmarshal(existing, &current)
			effective := p.queueAttrs(ctx, req, name)
			for key, value := range attrs {
				if str(effective[key]) != str(value) {
					return nil, &spi.Fault{Code: "QueueAlreadyExists", Message: "A queue already exists with the same name and a different value for attribute " + key, HTTPStatus: 400, Fault: "client"}
				}
			}
			return &spi.Response{Output: map[string]any{"QueueUrl": current["url"]}}, nil
		}
		url := fmt.Sprintf("%s/%s/%s", base, req.Identity.Account, name)
		if strings.HasSuffix(name, ".fifo") {
			attrs["FifoQueue"] = "true"
		}
		now := strconv.FormatInt(p.deps.Clock.Now().Unix(), 10)
		meta, _ := json.Marshal(map[string]any{
			"url": url, "name": name, "attrs": attrs, "dedupScope": str(attrs["DeduplicationScope"]), "seq": 0,
			"created": now, "lastModified": now,
		})
		_ = p.col(req, "queues").Put(ctx, name, meta)
		if len(attrs) > 0 {
			ab, _ := json.Marshal(attrs)
			_ = p.col(req, "qattrs").Put(ctx, name, ab)
		}
		tags := asMap(req.Input["Tags"])
		if len(tags) == 0 {
			tags = asMap(req.Input["tags"])
		}
		if len(tags) > 0 {
			tb, _ := json.Marshal(tags)
			_ = p.col(req, "qtags").Put(ctx, name, tb)
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
		prefix := str(req.Input["QueueNamePrefix"])
		after := ""
		if token := str(req.Input["NextToken"]); token != "" {
			decoded, err := base64.StdEncoding.DecodeString(token)
			if err != nil {
				return nil, &spi.Fault{Code: "InvalidAddress", Message: "The address " + token + " is not valid for this endpoint.", HTTPStatus: 400, Fault: "client"}
			}
			after = arnQueue(string(decoded))
		}
		kvs, more, _ := p.col(req, "queues").List(ctx, prefix, after, asInt(req.Input["MaxResults"]))
		var urls []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			urls = append(urls, m["url"])
		}
		out := map[string]any{}
		if len(urls) > 0 {
			out["QueueUrls"] = urls
		}
		if more {
			out["NextToken"] = base64.StdEncoding.EncodeToString([]byte(str(urls[len(urls)-1])))
		}
		return &spi.Response{Output: out}, nil
	case "DeleteQueue":
		name := queueName(req)
		_ = p.col(req, "qdeleted").Put(ctx, name, []byte(strconv.FormatInt(p.deps.Clock.Now().UnixNano(), 10)))
		_ = p.col(req, "queues").Delete(ctx, name)
		_ = p.col(req, "qattrs").Delete(ctx, name)
		_ = p.col(req, "qtags").Delete(ctx, name)
		_ = p.col(req, "qpurge").Delete(ctx, name)
		for _, collection := range []string{"msgs:" + name, "dedup:" + name, "rhandles:" + name} {
			kvs, _, _ := p.col(req, collection).List(ctx, "", "", 0)
			for _, kv := range kvs {
				_ = p.col(req, collection).Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "SendMessage":
		return p.send(ctx, req)
	case "ReceiveMessage":
		return p.receive(ctx, req)
	case "DeleteMessage":
		name := queueName(req)
		handle := str(req.Input["ReceiptHandle"])
		if !validReceiptHandle(handle) {
			return nil, receiptHandleFault(handle)
		}
		if strings.HasSuffix(name, ".fifo") && p.receiptExpired(ctx, req, name, handle) {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %s for parameter ReceiptHandle is invalid. Reason: The receipt handle has expired.", handle), HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "msgs:"+name).Delete(ctx, p.resolveHandle(ctx, req, name, handle))
		_ = p.col(req, "rhandles:"+name).Delete(ctx, handle)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetQueueAttributes":
		name := queueName(req)
		visible, notVisible, delayed := p.countMsgStates(ctx, req, name)
		attrs := map[string]any{
			"ApproximateNumberOfMessages":           fmt.Sprintf("%d", visible),
			"ApproximateNumberOfMessagesNotVisible": fmt.Sprintf("%d", notVisible),
			"ApproximateNumberOfMessagesDelayed":    fmt.Sprintf("%d", delayed),
			"QueueArn":                              fmt.Sprintf("arn:%s:sqs:%s:%s:%s", arnPartition(req.Identity.Region), req.Identity.Region, req.Identity.Account, name),
			"VisibilityTimeout":                     "30",
			"DelaySeconds":                          "0",
			"MaximumMessageSize":                    "1048576",
			"MessageRetentionPeriod":                "345600",
			"ReceiveMessageWaitTimeSeconds":         "0",
			"SqsManagedSseEnabled":                  "true",
		}
		if b, ok, _ := p.col(req, "queues").Get(ctx, name); ok {
			var meta map[string]any
			_ = json.Unmarshal(b, &meta)
			attrs["CreatedTimestamp"] = meta["created"]
			attrs["LastModifiedTimestamp"] = meta["lastModified"]
			if attrs["LastModifiedTimestamp"] == nil {
				attrs["LastModifiedTimestamp"] = meta["created"]
			}
		}
		if b, ok, _ := p.col(req, "qattrs").Get(ctx, name); ok {
			var extra map[string]any
			_ = json.Unmarshal(b, &extra)
			for k, v := range extra {
				attrs[k] = v
			}
		}
		filtered := map[string]any{}
		for _, name := range stringList(req.Input, "AttributeNames", "AttributeName") {
			if name == "All" {
				filtered = attrs
				break
			}
			if value, ok := attrs[name]; ok {
				filtered[name] = value
			}
		}
		attrs = filtered
		return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
	case "SetQueueAttributes":
		name := queueName(req)
		attrs := asMap(req.Input["Attributes"])
		if raw, present := attrs["RedrivePolicy"]; present && str(raw) != "" {
			if fault := validateRedrivePolicy(str(raw)); fault != nil {
				return nil, fault
			}
		}
		if fifo, present := attrs["FifoQueue"]; present && (!strings.HasSuffix(name, ".fifo") || str(fifo) != "true") {
			return nil, &spi.Fault{Code: "InvalidAttributeName", Message: "Unknown Attribute FifoQueue.", HTTPStatus: 400, Fault: "client"}
		}
		current := map[string]any{}
		if b, ok, _ := p.col(req, "qattrs").Get(ctx, name); ok {
			_ = json.Unmarshal(b, &current)
		}
		for key, value := range attrs {
			if (key == "RedrivePolicy" || key == "Policy") && str(value) == "" {
				delete(current, key)
				continue
			}
			if key == "KmsMasterKeyId" && str(value) == "" {
				delete(current, key)
				continue
			}
			if key == "KmsDataKeyReusePeriodSeconds" && str(value) == "300" {
				delete(current, key)
				continue
			}
			current[key] = value
		}
		if str(attrs["KmsMasterKeyId"]) != "" && str(attrs["SqsManagedSseEnabled"]) == "" {
			current["SqsManagedSseEnabled"] = "false"
		}
		if fault := validateSSEAttributes(current); fault != nil {
			return nil, fault
		}
		b, _ := json.Marshal(current)
		_ = p.col(req, "qattrs").Put(ctx, name, b)
		if metaBytes, ok, _ := p.col(req, "queues").Get(ctx, name); ok {
			var meta map[string]any
			_ = json.Unmarshal(metaBytes, &meta)
			meta["lastModified"] = strconv.FormatInt(p.deps.Clock.Now().Unix(), 10)
			if updated, marshalErr := json.Marshal(meta); marshalErr == nil {
				_ = p.col(req, "queues").Put(ctx, name, updated)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "PurgeQueue":
		name := queueName(req)
		if previous, ok, _ := p.col(req, "qpurge").Get(ctx, name); ok {
			purgedAt, _ := strconv.ParseInt(string(previous), 10, 64)
			if p.deps.Clock.Now().Sub(time.Unix(0, purgedAt)) < time.Minute {
				return nil, &spi.Fault{Code: "AWS.SimpleQueueService.PurgeQueueInProgress", Message: fmt.Sprintf("Only one PurgeQueue operation on %s is allowed every 60 seconds.", name), HTTPStatus: 403, Fault: "client"}
			}
		}
		_ = p.col(req, "qpurge").Put(ctx, name, []byte(strconv.FormatInt(p.deps.Clock.Now().UnixNano(), 10)))
		kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
		for _, kv := range kvs {
			_ = p.col(req, "msgs:"+name).Delete(ctx, kv.Key)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "TagQueue":
		name := queueName(req)
		tags := asMap(req.Input["Tags"])
		current := map[string]any{}
		if b, ok, _ := p.col(req, "qtags").Get(ctx, name); ok {
			_ = json.Unmarshal(b, &current)
		}
		for key, value := range tags {
			current[key] = value
		}
		b, _ := json.Marshal(current)
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
		if len(tags) == 0 {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "SendMessageBatch":
		entries, _ := req.Input["Entries"].([]any)
		if len(entries) == 0 {
			return nil, &spi.Fault{Code: "AWS.SimpleQueueService.EmptyBatchRequest", Message: "There should be at least one SendMessageBatchRequestEntry in the request.", HTTPStatus: 400, Fault: "client"}
		}
		if len(entries) > 10 {
			return nil, &spi.Fault{Code: "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", Message: fmt.Sprintf("Maximum number of entries per request are 10. You have sent %d.", len(entries)), HTTPStatus: 400, Fault: "client"}
		}
		name := queueName(req)
		attrs := p.queueAttrs(ctx, req, name)
		total := 0
		for _, entry := range entries {
			message := asMap(entry)
			if !validBatchEntryID(str(message["Id"])) {
				return nil, &spi.Fault{Code: "AWS.SimpleQueueService.InvalidBatchEntryId", Message: "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.", HTTPStatus: 400, Fault: "client"}
			}
			if strings.HasSuffix(name, ".fifo") && str(message["MessageGroupId"]) == "" {
				return nil, &spi.Fault{Code: "MissingParameter", Message: "MessageGroupId", HTTPStatus: 400, Fault: "client"}
			}
			if strings.HasSuffix(name, ".fifo") && str(attrs["ContentBasedDeduplication"]) != "true" && str(message["MessageGroupId"]) != "" {
				if _, provided := message["MessageDeduplicationId"]; !provided || str(message["MessageDeduplicationId"]) == "" {
					return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "The queue should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly", HTTPStatus: 400, Fault: "client"}
				}
			}
			total += messageSize(str(message["MessageBody"]), message["MessageAttributes"])
		}
		if total > 1<<20 {
			return nil, &spi.Fault{Code: "AWS.SimpleQueueService.BatchRequestTooLong", Message: fmt.Sprintf("Batch requests cannot be longer than 1048576 bytes. You have sent %d bytes.", total), HTTPStatus: 400, Fault: "client"}
		}
		var ok []any
		var failed []any
		for _, e := range entries {
			m := asMap(e)
			input := map[string]any{
				"QueueUrl": req.Input["QueueUrl"], "QueueName": queueName(req), "MessageBody": str(m["MessageBody"]),
				"MessageDeduplicationId": m["MessageDeduplicationId"],
				"MessageAttributes":      m["MessageAttributes"], "DelaySeconds": m["DelaySeconds"],
			}
			if group, present := m["MessageGroupId"]; present && group != nil {
				input["MessageGroupId"] = group
			}
			sub := &spi.Request{Identity: req.Identity, HTTP: req.HTTP, Input: input, Operation: "SendMessage"}
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
		output := map[string]any{"Successful": ok}
		if len(failed) > 0 {
			output["Failed"] = failed
		}
		return &spi.Response{Output: output}, nil
	case "DeleteMessageBatch":
		entries, _ := req.Input["Entries"].([]any)
		if len(entries) == 0 {
			return nil, &spi.Fault{Code: "AWS.SimpleQueueService.EmptyBatchRequest", Message: "There should be at least one DeleteMessageBatchRequestEntry in the request.", HTTPStatus: 400, Fault: "client"}
		}
		if len(entries) > 10 {
			return nil, &spi.Fault{Code: "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", Message: fmt.Sprintf("Maximum number of entries per request are 10. You have sent %d.", len(entries)), HTTPStatus: 400, Fault: "client"}
		}
		for _, entry := range entries {
			if !validBatchEntryID(str(asMap(entry)["Id"])) {
				return nil, &spi.Fault{Code: "AWS.SimpleQueueService.InvalidBatchEntryId", Message: "A batch entry id can only contain alphanumeric characters, hyphens and underscores. It can be at most 80 letters long.", HTTPStatus: 400, Fault: "client"}
			}
		}
		name := queueName(req)
		var ok []any
		for _, e := range entries {
			m := asMap(e)
			handle := str(m["ReceiptHandle"])
			_ = p.col(req, "msgs:"+name).Delete(ctx, p.resolveHandle(ctx, req, name, handle))
			_ = p.col(req, "rhandles:"+name).Delete(ctx, handle)
			ok = append(ok, map[string]any{"Id": m["Id"]})
		}
		return &spi.Response{Output: map[string]any{"Successful": ok}}, nil
	case "ChangeMessageVisibility", "ChangeMessageVisibilityBatch":
		name := queueName(req)
		if req.Operation == "ChangeMessageVisibility" {
			handle := str(req.Input["ReceiptHandle"])
			if !validReceiptHandle(handle) {
				return nil, receiptHandleFault(handle)
			}
			if !p.setVis(ctx, req, name, handle, str(req.Input["VisibilityTimeout"])) {
				return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %s for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.", handle), HTTPStatus: 400, Fault: "client"}
			}
		} else if entries, ok := req.Input["Entries"].([]any); ok {
			if len(entries) > 10 {
				return nil, &spi.Fault{Code: "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", Message: fmt.Sprintf("Maximum number of entries per request are 10. You have sent %d.", len(entries)), HTTPStatus: 400, Fault: "client"}
			}
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
	visible, _, _ := p.countMsgStates(ctx, req, name)
	return visible
}

func (p *Pack) countMsgStates(ctx context.Context, req *spi.Request, name string) (visible, notVisible, delayed int) {
	kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
	now := p.deps.Clock.Now().UnixNano()
	attrs := p.queueAttrs(ctx, req, name)
	for _, kv := range kvs {
		var message map[string]any
		if json.Unmarshal(kv.Value, &message) != nil || messageExpired(attrs, message, now) {
			continue
		}
		if int64(asFloat(message["visibleAt"])) <= now {
			visible++
		} else if asInt(message["receiveCount"]) > 0 {
			notVisible++
		} else {
			delayed++
		}
	}
	return visible, notVisible, delayed
}

func (p *Pack) send(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	body := str(req.Input["MessageBody"])
	if body == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "The request must contain the parameter MessageBody.", HTTPStatus: 400, Fault: "client"}
	}
	if !validMessageContents(body) {
		return nil, &spi.Fault{Code: "InvalidMessageContents", Message: "The message contains characters outside the allowed set.", HTTPStatus: 400, Fault: "client"}
	}
	if fault := validateMessageAttributes(req.Input["MessageAttributes"]); fault != nil {
		return nil, fault
	}
	sum := md5.Sum([]byte(body))
	md5hex := hex.EncodeToString(sum[:])
	md5attrs := md5MessageAttributes(req.Input["MessageAttributes"])
	systemAttrs := req.Input["MessageSystemAttributes"]
	if len(asMap(systemAttrs)) == 0 && req.HTTP != nil {
		if trace := req.HTTP.Header.Get("X-Amzn-Trace-Id"); trace != "" {
			systemAttrs = map[string]any{"AWSTraceHeader": map[string]any{"DataType": "String", "StringValue": trace}}
		}
	}
	md5system := md5MessageAttributes(systemAttrs)
	attrs := p.queueAttrs(ctx, req, name)
	maximum := 1 << 20
	if configured := asInt(attrs["MaximumMessageSize"]); configured > 0 {
		maximum = configured
	}
	if messageSize(body, req.Input["MessageAttributes"]) > maximum {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("One or more parameters are invalid. Reason: Message must be shorter than %d bytes.", maximum), HTTPStatus: 400, Fault: "client"}
	}
	now := p.deps.Clock.Now()
	group := str(req.Input["MessageGroupId"])
	dedup := str(req.Input["MessageDeduplicationId"])
	fifo := strings.HasSuffix(name, ".fifo")
	if !fifo {
		if _, provided := req.Input["MessageGroupId"]; provided && !validMessageGroupID(group) {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %s for parameter MessageGroupId is invalid. Reason: MessageGroupId can only include alphanumeric and punctuation characters. 1 to 128 in length.", group), HTTPStatus: 400, Fault: "client"}
		}
	}
	if fifo && group == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "MessageGroupId", HTTPStatus: 400, Fault: "client"}
	}
	if fifo {
		_, provided := req.Input["MessageDeduplicationId"]
		if provided && !validMessageGroupID(dedup) {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %s for parameter MessageDeduplicationId is invalid. Reason: MessageDeduplicationId can only include alphanumeric and punctuation characters. 1 to 128 in length.", dedup), HTTPStatus: 400, Fault: "client"}
		}
		if !provided && dedup == "" && str(attrs["ContentBasedDeduplication"]) != "true" {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "MessageDeduplicationId is required when ContentBasedDeduplication is disabled.", HTTPStatus: 400, Fault: "client"}
		}
	}
	if dedup == "" && str(attrs["ContentBasedDeduplication"]) == "true" {
		dedup = md5hex
	}
	explicitDelay := req.Input["DelaySeconds"] != nil
	delay := asInt(req.Input["DelaySeconds"])
	if fifo && explicitDelay && delay > 0 {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %d for parameter DelaySeconds is invalid. Reason: The request include parameter that is not valid for this queue type.", delay), HTTPStatus: 400, Fault: "client"}
	}
	if req.Input["DelaySeconds"] == nil || (fifo && delay == 0) {
		delay = asInt(attrs["DelaySeconds"])
	}
	if delay < 0 || delay > 900 {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "DelaySeconds must be between 0 and 900.", HTTPStatus: 400, Fault: "client"}
	}
	dedupKey := dedup
	if fifo && p.dedupScope(ctx, req, name, attrs) == "messageGroup" {
		dedupKey = group + "\x1f" + dedup
	}
	if dedup != "" {
		if b, ok, _ := p.col(req, "dedup:"+name).Get(ctx, dedupKey); ok {
			var d map[string]any
			_ = json.Unmarshal(b, &d)
			until := int64(asFloat(d["until"]))
			if now.UnixNano() < until {
				output := map[string]any{"MessageId": d["id"], "MD5OfMessageBody": md5hex}
				if strings.HasSuffix(name, ".fifo") {
					output["SequenceNumber"] = strconv.Itoa(asInt(d["seq"]))
				}
				if md5system != "" {
					output["MD5OfMessageSystemAttributes"] = md5system
				}
				if md5attrs != "" {
					output["MD5OfMessageAttributes"] = md5attrs
				}
				return &spi.Response{Output: output}, nil
			}
		}
	}
	id := p.deps.Rand.Hex(16)
	rh := p.deps.Rand.Hex(64)
	seq := p.nextSeq(ctx, req, name)
	trace := str(asMap(asMap(systemAttrs)["AWSTraceHeader"])["StringValue"])
	msg := map[string]any{
		"id": id, "body": body, "handle": rh, "md5": md5hex, "md5Attrs": md5attrs,
		"group": group, "seq": seq,
		"visibleAt": now.Add(time.Duration(delay) * time.Second).UnixNano(), "receiveCount": 0, "sentAt": now.UnixMilli(),
		"attrs": req.Input["MessageAttributes"], "trace": trace,
	}
	raw, _ := json.Marshal(msg)
	_ = p.col(req, "msgs:"+name).Put(ctx, rh, raw)
	if dedup != "" {
		db, _ := json.Marshal(map[string]any{"id": id, "md5": md5hex, "md5Attrs": md5attrs, "md5System": md5system, "seq": seq, "until": now.Add(5 * time.Minute).UnixNano()})
		_ = p.col(req, "dedup:"+name).Put(ctx, dedupKey, db)
	}
	if p.deps.Bus != nil {
		_ = p.deps.Bus.Publish(ctx, "sqs", raw)
	}
	output := map[string]any{"MessageId": id, "MD5OfMessageBody": md5hex}
	if fifo {
		output["SequenceNumber"] = strconv.Itoa(seq)
	}
	if md5attrs != "" {
		output["MD5OfMessageAttributes"] = md5attrs
	}
	if md5system != "" {
		output["MD5OfMessageSystemAttributes"] = md5system
	}
	return &spi.Response{Output: output}, nil
}

func validMessageContents(body string) bool {
	for _, r := range body {
		if r != '\t' && r != '\n' && r != '\r' && (r < 0x20 || r > 0xD7FF && r < 0xE000 || r > 0xFFFD && r < 0x10000) {
			return false
		}
	}
	return true
}

func messageSize(body string, attrs any) int {
	size := len([]byte(body))
	for name, raw := range asMap(attrs) {
		attribute := asMap(raw)
		size += len(name) + len(str(attribute["DataType"])) + len(str(attribute["StringValue"])) + len(str(attribute["BinaryValue"]))
		for _, value := range []any{attribute["StringListValues"], attribute["BinaryListValues"]} {
			if values, ok := value.([]any); ok {
				for _, item := range values {
					size += len(str(item))
				}
			}
		}
	}
	return size
}

func md5MessageAttributes(attrs any) string {
	values := asMap(attrs)
	if len(values) == 0 {
		return ""
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	digest := md5.New()
	writeField := func(value []byte) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(value)
	}
	for _, name := range names {
		attribute := asMap(values[name])
		dataType := str(attribute["DataType"])
		writeField([]byte(name))
		writeField([]byte(dataType))
		transport := byte(1)
		if strings.HasPrefix(dataType, "Binary") {
			transport = 2
		}
		_, _ = digest.Write([]byte{transport})
		value := str(attribute["StringValue"])
		if transport == 2 {
			switch raw := attribute["BinaryValue"].(type) {
			case []byte:
				value = string(raw)
			default:
				decoded, err := base64.StdEncoding.DecodeString(str(raw))
				if err == nil {
					value = string(decoded)
				}
			}
		}
		writeField([]byte(value))
	}
	sum := digest.Sum(nil)
	return hex.EncodeToString(sum)
}

func validateMessageAttributes(attrs any) *spi.Fault {
	for name, raw := range asMap(attrs) {
		if !validMessageAttributeName(name) {
			return invalidMessageAttributeFault()
		}
		attribute := asMap(raw)
		dataType := str(attribute["DataType"])
		if !validMessageAttributeDataType(dataType) {
			return invalidMessageAttributeFault()
		}
		stringValue, hasString := attribute["StringValue"]
		binaryValue, hasBinary := attribute["BinaryValue"]
		stringList, hasStringList := attribute["StringListValues"]
		binaryList, hasBinaryList := attribute["BinaryListValues"]
		count := 0
		for _, present := range []bool{hasString, hasBinary, hasStringList, hasBinaryList} {
			if present {
				count++
			}
		}
		if count != 1 {
			return invalidMessageAttributeFault()
		}
		if hasString {
			value := str(stringValue)
			if value == "" || !validMessageContents(value) {
				return invalidMessageAttributeFault()
			}
		}
		if hasBinary && binaryValue == nil {
			return invalidMessageAttributeFault()
		}
		if hasStringList && len(asAnySlice(stringList)) == 0 {
			return invalidMessageAttributeFault()
		}
		if hasBinaryList && len(asAnySlice(binaryList)) == 0 {
			return invalidMessageAttributeFault()
		}
	}
	return nil
}

func validMessageAttributeName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > 256 || strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return false
	}
	if !unicode.IsLetter(runes[0]) && !unicode.IsDigit(runes[0]) {
		return false
	}
	if !unicode.IsLetter(runes[len(runes)-1]) && !unicode.IsDigit(runes[len(runes)-1]) {
		return false
	}
	for _, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func validMessageAttributeDataType(dataType string) bool {
	runes := []rune(dataType)
	if len(runes) == 0 || len(runes) > 256 {
		return false
	}
	base, suffix, hasSuffix := strings.Cut(dataType, ".")
	if base != "String" && base != "Number" && base != "Binary" {
		return false
	}
	if !hasSuffix || suffix == "" {
		return !hasSuffix
	}
	for _, r := range suffix {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func invalidMessageAttributeFault() *spi.Fault {
	return &spi.Fault{Code: "InvalidParameterValue", Message: "The message attribute is invalid.", HTTPStatus: 400, Fault: "client"}
}

func asAnySlice(value any) []any {
	values, _ := value.([]any)
	return values
}

func validBatchEntryID(value string) bool {
	if len(value) == 0 || len(value) > 80 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func (p *Pack) receive(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := queueName(req)
	max := 1
	if raw, ok := req.Input["MaxNumberOfMessages"]; ok {
		max = asInt(raw)
		if max < 1 || max > 10 {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %d for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, if provided.", max), HTTPStatus: 400, Fault: "client"}
		}
	}
	waitSeconds := asInt(req.Input["WaitTimeSeconds"])
	if raw, ok := req.Input["WaitTimeSeconds"]; ok {
		waitSeconds = asInt(raw)
		if waitSeconds < 0 || waitSeconds > 20 {
			return nil, &spi.Fault{Code: "InvalidParameterValue", Message: fmt.Sprintf("Value %d for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if provided.", waitSeconds), HTTPStatus: 400, Fault: "client"}
		}
	}
	attrs := p.queueAttrs(ctx, req, name)
	if _, explicit := req.Input["WaitTimeSeconds"]; !explicit {
		waitSeconds = asInt(attrs["ReceiveMessageWaitTimeSeconds"])
	}
	wait := time.Duration(waitSeconds) * time.Second
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
			if len(msgs) == 0 {
				return &spi.Response{Output: map[string]any{}}, nil
			}
			out := make([]any, 0, len(msgs))
			wanted := map[string]bool{}
			names := stringList(req.Input, "AttributeNames", "AttributeName")
			names = append(names, stringList(req.Input, "MessageSystemAttributeNames", "MessageSystemAttributeName")...)
			for _, name := range names {
				wanted[name] = true
			}
			for _, m := range msgs {
				if p.afterReceive(ctx, req, name, m, vis) {
					continue
				}
				attributes := map[string]any{}
				if wanted["All"] || wanted["ApproximateReceiveCount"] {
					attributes["ApproximateReceiveCount"] = fmt.Sprintf("%v", m["receiveCount"])
				}
				if wanted["All"] || wanted["SentTimestamp"] {
					attributes["SentTimestamp"] = strconv.FormatInt(int64(asFloat(m["sentAt"])), 10)
				}
				if wanted["All"] || wanted["ApproximateFirstReceiveTimestamp"] {
					attributes["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(int64(asFloat(m["firstReceiveAt"])), 10)
				}
				if (wanted["All"] || wanted["AWSTraceHeader"]) && str(m["trace"]) != "" {
					attributes["AWSTraceHeader"] = m["trace"]
				}
				wire := map[string]any{"MessageId": m["id"], "ReceiptHandle": m["handle"], "Body": m["body"], "MD5OfBody": m["md5"]}
				if len(attributes) > 0 {
					wire["Attributes"] = attributes
				}
				if want := req.Input["MessageAttributeNames"]; want != nil && m["attrs"] != nil {
					wire["MessageAttributes"] = filterMsgAttrs(m["attrs"], want)
					if digest := str(m["md5Attrs"]); digest != "" {
						wire["MD5OfMessageAttributes"] = digest
					}
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
			return &spi.Response{Output: map[string]any{}}, nil
		}
	}
}

func (p *Pack) visible(ctx context.Context, req *spi.Request, name string, now time.Time, max int) []map[string]any {
	kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 0)
	attrs := p.queueAttrs(ctx, req, name)
	if strings.HasSuffix(name, ".fifo") {
		groups := map[string][]map[string]any{}
		firstSeq := map[string]int{}
		order := []string{}
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if messageExpired(attrs, m, now.UnixNano()) {
				_ = p.col(req, "msgs:"+name).Delete(ctx, kv.Key)
				continue
			}
			group := str(m["group"])
			if group == "" {
				group = kv.Key
			}
			if _, ok := groups[group]; !ok {
				order = append(order, group)
				firstSeq[group] = asInt(m["seq"])
			}
			if seq := asInt(m["seq"]); seq < firstSeq[group] {
				firstSeq[group] = seq
			}
			groups[group] = append(groups[group], m)
		}
		for _, messages := range groups {
			sortMsgs(messages)
		}
		partial := map[string]bool{}
		for group, messages := range groups {
			visible := 0
			for _, m := range messages {
				if int64(asFloat(m["visibleAt"])) > now.UnixNano() {
					break
				}
				visible++
			}
			if visible > 0 && visible < len(messages) {
				partial[group] = true
			}
		}
		for i := 0; i < len(order); i++ {
			for j := i + 1; j < len(order); j++ {
				if partial[order[j]] != partial[order[i]] {
					if !partial[order[j]] {
						order[i], order[j] = order[j], order[i]
					}
				} else if firstSeq[order[j]] < firstSeq[order[i]] {
					order[i], order[j] = order[j], order[i]
				}
			}
		}
		var out []map[string]any
		for _, group := range order {
			for _, m := range groups[group] {
				if int64(asFloat(m["visibleAt"])) > now.UnixNano() {
					break
				}
				out = append(out, m)
				if len(out) >= max {
					return out
				}
			}
		}
		return out
	}
	var cand []map[string]any
	inFlight := map[string]bool{}
	for _, kv := range kvs {
		var m map[string]any
		_ = json.Unmarshal(kv.Value, &m)
		if messageExpired(attrs, m, now.UnixNano()) {
			_ = p.col(req, "msgs:"+name).Delete(ctx, kv.Key)
			continue
		}
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

func messageExpired(attrs, message map[string]any, now int64) bool {
	retention := asInt(attrs["MessageRetentionPeriod"])
	return retention > 0 && asFloat(message["sentAt"])+float64(retention*1000) <= float64(now/1_000_000)
}

func (p *Pack) afterReceive(ctx context.Context, req *spi.Request, name string, m map[string]any, vis int) bool {
	n := asInt(m["receiveCount"]) + 1
	m["receiveCount"] = n
	if n == 1 {
		m["firstReceiveAt"] = p.deps.Clock.Now().UnixMilli()
	}
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
		return true
	}
	m["visibleAt"] = p.deps.Clock.Now().Add(time.Duration(vis) * time.Second).UnixNano()
	if n > 1 {
		newHandle := p.deps.Rand.Hex(64)
		m["handle"] = newHandle
		raw, _ := json.Marshal(m)
		_ = p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(tx spi.ScopeTx) error {
			if err := tx.Collection("rhandles:"+name).Put(rh, []byte(newHandle)); err != nil {
				return err
			}
			if err := tx.Collection("msgs:" + name).Delete(rh); err != nil {
				return err
			}
			return tx.Collection("msgs:"+name).Put(newHandle, raw)
		})
		return false
	}
	raw, _ := json.Marshal(m)
	_ = p.col(req, "msgs:"+name).Put(ctx, str(m["handle"]), raw)
	return false
}

func (p *Pack) setVis(ctx context.Context, req *spi.Request, name, handle, timeout string) bool {
	resolved := p.resolveHandle(ctx, req, name, handle)
	b, ok, _ := p.col(req, "msgs:"+name).Get(ctx, resolved)
	if !ok {
		return false
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	sec := asInt(timeout)
	m["visibleAt"] = p.deps.Clock.Now().Add(time.Duration(sec) * time.Second).UnixNano()
	nb, _ := json.Marshal(m)
	_ = p.col(req, "msgs:"+name).Put(ctx, resolved, nb)
	return true
}

func (p *Pack) resolveHandle(ctx context.Context, req *spi.Request, name, handle string) string {
	seen := map[string]bool{}
	for !seen[handle] {
		seen[handle] = true
		next, ok, _ := p.col(req, "rhandles:"+name).Get(ctx, handle)
		if !ok || string(next) == "" {
			return handle
		}
		handle = string(next)
	}
	return handle
}

func (p *Pack) receiptExpired(ctx context.Context, req *spi.Request, name, handle string) bool {
	b, ok, _ := p.col(req, "msgs:"+name).Get(ctx, p.resolveHandle(ctx, req, name, handle))
	if !ok {
		return false
	}
	var message map[string]any
	if json.Unmarshal(b, &message) != nil {
		return false
	}
	return asInt(message["receiveCount"]) > 0 && int64(asFloat(message["visibleAt"])) <= p.deps.Clock.Now().UnixNano()
}

func validReceiptHandle(handle string) bool {
	if len(handle) != 64 {
		return false
	}
	_, err := hex.DecodeString(handle)
	return err == nil
}

func receiptHandleFault(handle string) *spi.Fault {
	return &spi.Fault{Code: "ReceiptHandleIsInvalid", Message: fmt.Sprintf("The input receipt handle %q is not a valid receipt handle.", handle), HTTPStatus: 400, Fault: "client"}
}

func (p *Pack) queueAttrs(ctx context.Context, req *spi.Request, name string) map[string]any {
	out := map[string]any{
		"DelaySeconds":                  "0",
		"MaximumMessageSize":            "1048576",
		"MessageRetentionPeriod":        "345600",
		"ReceiveMessageWaitTimeSeconds": "0",
		"SqsManagedSseEnabled":          "true",
		"VisibilityTimeout":             "30",
	}
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

func (p *Pack) dedupScope(ctx context.Context, req *spi.Request, name string, attrs map[string]any) string {
	if b, ok, _ := p.col(req, "queues").Get(ctx, name); ok {
		var meta map[string]any
		_ = json.Unmarshal(b, &meta)
		if scope := str(meta["dedupScope"]); scope != "" {
			return scope
		}
	}
	return str(attrs["DeduplicationScope"])
}

func (p *Pack) nextSeq(ctx context.Context, req *spi.Request, name string) int {
	n := 0
	_ = p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Txn(ctx, func(tx spi.ScopeTx) error {
		b, ok, err := tx.Collection("queues").Get(name)
		if err != nil {
			return err
		}
		meta := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &meta)
		}
		n = asInt(meta["seq"]) + 1
		meta["seq"] = n
		nb, _ := json.Marshal(meta)
		return tx.Collection("queues").Put(name, nb)
	})
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

func validateRedrivePolicy(raw string) *spi.Fault {
	var policy map[string]any
	if json.Unmarshal([]byte(raw), &policy) != nil {
		return invalidRedrivePolicyFault()
	}
	arn := str(policy["deadLetterTargetArn"])
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sqs" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
		return invalidRedrivePolicyFault()
	}
	var count int
	switch value := policy["maxReceiveCount"].(type) {
	case string:
		var err error
		count, err = strconv.Atoi(value)
		if err != nil {
			return invalidRedrivePolicyFault()
		}
	default:
		count = asInt(value)
	}
	if count < 1 || count > 1000 {
		return invalidRedrivePolicyFault()
	}
	return nil
}

func invalidRedrivePolicyFault() *spi.Fault {
	return &spi.Fault{Code: "InvalidParameterValue", Message: "Invalid value for the parameter RedrivePolicy.", HTTPStatus: 400, Fault: "client"}
}

func validateSSEAttributes(attrs map[string]any) *spi.Fault {
	if str(attrs["KmsMasterKeyId"]) != "" && str(attrs["SqsManagedSseEnabled"]) == "true" {
		return &spi.Fault{Code: "InvalidAttributeName", Message: "You can use one type of server-side encryption (SSE) at one time. You can either enable KMS SSE or SQS SSE.", HTTPStatus: 400, Fault: "client"}
	}
	return nil
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
	switch m := v.(type) {
	case map[string]any:
		return m
	case map[string]string:
		out := make(map[string]any, len(m))
		for key, value := range m {
			out[key] = value
		}
		return out
	}
	return map[string]any{}
}

func advertise(req *spi.Request) string {
	if req.AdvertiseURL != "" {
		return strings.TrimRight(req.AdvertiseURL, "/")
	}
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

func validMessageGroupID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	const punctuation = `!"#$%&'()*+,-./:;<=>?@[\\]^_` + "`" + `{|}~`
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && !strings.ContainsRune(punctuation, r) {
			return false
		}
	}
	return true
}

func validQueueName(value string) bool {
	if strings.HasSuffix(value, ".fifo") {
		value = strings.TrimSuffix(value, ".fifo")
	}
	if value == "" || len(value) > 80 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func filterMsgAttrs(attrs, want any) any {
	m := asMap(attrs)
	var names []string
	switch values := want.(type) {
	case []any:
		for _, value := range values {
			names = append(names, str(value))
		}
	case []string:
		names = values
	case string:
		names = []string{values}
	}
	if len(names) == 0 {
		return map[string]any{}
	}
	for _, name := range names {
		if name == "All" || name == "*" || name == ".*" || name == "." {
			return m
		}
	}
	out := map[string]any{}
	for _, name := range names {
		if strings.HasSuffix(name, ".*") {
			prefix := strings.TrimSuffix(name, ".*")
			for key, value := range m {
				if strings.HasPrefix(key, prefix) {
					out[key] = value
				}
			}
			continue
		}
		if value, ok := m[name]; ok {
			out[name] = value
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
	arn := fmt.Sprintf("arn:%s:sqs:%s:%s:%s", arnPartition(req.Identity.Region), req.Identity.Region, req.Identity.Account, name)
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
	sourceArn := str(req.Input["SourceArn"])
	src := arnQueue(sourceArn)
	if src == "" {
		src = queueName(req)
		sourceArn = queueARN(req, src)
	}
	if src == "" {
		return nil, &spi.Fault{Code: "MissingParameter", Message: "SourceArn", HTTPStatus: 400, Fault: "client"}
	}
	if !p.queueExists(ctx, req, src) || !p.isDeadLetterQueue(ctx, req, sourceArn) {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Source queue must be configured as a Dead Letter Queue.", HTTPStatus: 400, Fault: "client"}
	}
	destinationArn := str(req.Input["DestinationArn"])
	dst := arnQueue(destinationArn)
	if dst != "" && !p.queueExists(ctx, req, dst) {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "The resource that you specified for the DestinationArn parameter doesn't exist.", HTTPStatus: 404, Fault: "client"}
	}
	kvs, _, _ := p.col(req, "msgs:"+src).List(ctx, "", "", 0)
	toMove := len(kvs)
	taskIDHex := p.deps.Rand.Hex(32)
	taskID := fmt.Sprintf("%s-%s-%s-%s-%s", taskIDHex[:8], taskIDHex[8:12], taskIDHex[12:16], taskIDHex[16:20], taskIDHex[20:])
	handle := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"taskId":"%s","sourceArn":"%s"}`, taskID, sourceArn)))
	maxPerSecond := asInt(req.Input["MaxNumberOfMessagesPerSecond"])
	if maxPerSecond > 0 {
		p.moveMu.Lock()
		defer p.moveMu.Unlock()
		active, _, _ := p.col(req, "qmove").List(ctx, "", "", 0)
		for _, kv := range active {
			var prior map[string]any
			if json.Unmarshal(kv.Value, &prior) == nil && str(prior["source"]) == src && str(prior["Status"]) == "RUNNING" {
				return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "There is already a task running. Only one active task is allowed for a source queue arn at a given time.", HTTPStatus: 400, Fault: "client"}
			}
		}
		rec := map[string]any{
			"TaskHandle": handle, "Status": "RUNNING", "SourceArn": sourceArn, "DestinationArn": req.Input["DestinationArn"],
			"ApproximateNumberOfMessagesMoved": 0, "ApproximateNumberOfMessagesToMove": toMove,
			"MaxNumberOfMessagesPerSecond": maxPerSecond, "StartedTimestamp": p.deps.Clock.Now().Unix(), "source": src,
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "qmove").Put(ctx, handle, b)
		request := *req
		go p.runMoveTask(&request, handle, src, dst, maxPerSecond)
		return &spi.Response{Output: map[string]any{"TaskHandle": handle}}, nil
	}
	moved := 0
	for _, kv := range kvs {
		if ok, _ := p.moveOne(ctx, req, src, dst, kv); ok {
			moved++
		}
	}
	rec := map[string]any{
		"TaskHandle": handle, "Status": "COMPLETED",
		"SourceArn": sourceArn, "DestinationArn": req.Input["DestinationArn"],
		"ApproximateNumberOfMessagesMoved": moved, "ApproximateNumberOfMessagesToMove": toMove,
		"StartedTimestamp": p.deps.Clock.Now().Unix(),
		"source":           src,
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "qmove").Put(ctx, handle, b)
	return &spi.Response{Output: map[string]any{"TaskHandle": handle}}, nil
}

func (p *Pack) moveOne(ctx context.Context, req *spi.Request, src, dst string, kv spi.KV) (bool, bool) {
	var m map[string]any
	if json.Unmarshal(kv.Value, &m) != nil {
		return false, false
	}
	dest := dst
	if dest == "" {
		dest = str(m["origin"])
	}
	if dest == "" || dest == src {
		return false, false
	}
	if !p.queueExists(ctx, req, dest) {
		return false, true
	}
	_ = p.col(req, "msgs:"+src).Delete(ctx, kv.Key)
	m["handle"] = p.deps.Rand.Hex(64)
	m["receiveCount"] = 0
	m["visibleAt"] = p.deps.Clock.Now().UnixNano()
	raw, _ := json.Marshal(m)
	_ = p.col(req, "msgs:"+dest).Put(ctx, str(m["handle"]), raw)
	return true, false
}

func (p *Pack) runMoveTask(req *spi.Request, handle, src, dst string, maxPerSecond int) {
	ctx := context.Background()
	interval := time.Second / time.Duration(maxPerSecond)
	for {
		b, ok, _ := p.col(req, "qmove").Get(ctx, handle)
		if !ok {
			return
		}
		var rec map[string]any
		if json.Unmarshal(b, &rec) != nil {
			return
		}
		if str(rec["Status"]) == "CANCELLING" {
			rec["Status"] = "CANCELLED"
			p.updateMoveRecord(ctx, req, handle, rec)
			return
		}
		if dst != "" {
			if !p.queueExists(ctx, req, dst) {
				rec["Status"] = "FAILED"
				rec["FailureReason"] = "The destination queue does not exist."
				p.updateMoveRecord(ctx, req, handle, rec)
				return
			}
		}
		kvs, _, _ := p.col(req, "msgs:"+src).List(ctx, "", "", 0)
		moved := false
		for _, kv := range kvs {
			var failed bool
			moved, failed = p.moveOne(ctx, req, src, dst, kv)
			if failed {
				rec["Status"] = "FAILED"
				rec["FailureReason"] = "The destination queue does not exist."
			}
			if moved || failed {
				break
			}
		}
		if str(rec["Status"]) == "FAILED" {
			p.updateMoveRecord(ctx, req, handle, rec)
			return
		}
		if moved {
			rec["ApproximateNumberOfMessagesMoved"] = asInt(rec["ApproximateNumberOfMessagesMoved"]) + 1
		} else {
			rec["Status"] = "COMPLETED"
		}
		if p.updateMoveRecord(ctx, req, handle, rec) != "RUNNING" {
			return
		}
		<-p.deps.Clock.After(interval)
	}
}

func (p *Pack) updateMoveRecord(ctx context.Context, req *spi.Request, handle string, rec map[string]any) string {
	_ = p.col(req, "qmove").Txn(ctx, func(tx spi.Tx) error {
		b, ok, err := tx.Get(handle)
		if err != nil || !ok {
			return err
		}
		var current map[string]any
		if json.Unmarshal(b, &current) == nil && str(current["Status"]) == "CANCELLING" && str(rec["Status"]) != "CANCELLED" {
			rec["Status"] = "CANCELLED"
		}
		nb, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			return marshalErr
		}
		return tx.Put(handle, nb)
	})
	return str(rec["Status"])
}

func (p *Pack) isDeadLetterQueue(ctx context.Context, req *spi.Request, targetArn string) bool {
	kvs, _, _ := p.col(req, "queues").List(ctx, "", "", 0)
	for _, kv := range kvs {
		var meta map[string]any
		if json.Unmarshal(kv.Value, &meta) != nil {
			continue
		}
		name := str(meta["name"])
		attrs := p.queueAttrs(ctx, req, name)
		var policy map[string]any
		if json.Unmarshal([]byte(str(attrs["RedrivePolicy"])), &policy) == nil && str(policy["deadLetterTargetArn"]) == targetArn {
			return true
		}
	}
	return false
}

func queueARN(req *spi.Request, name string) string {
	return fmt.Sprintf("arn:%s:sqs:%s:%s:%s", arnPartition(req.Identity.Region), req.Identity.Region, req.Identity.Account, name)
}

func (p *Pack) cancelMove(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	h := str(req.Input["TaskHandle"])
	decoded, err := base64.StdEncoding.DecodeString(h)
	var document map[string]any
	if err != nil || json.Unmarshal(decoded, &document) != nil || str(document["taskId"]) == "" || str(document["sourceArn"]) == "" {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "Value for parameter TaskHandle is invalid.", HTTPStatus: 400, Fault: "client"}
	}
	if !p.queueExists(ctx, req, arnQueue(str(document["sourceArn"]))) {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "The resource that you specified for the SourceArn parameter doesn't exist.", HTTPStatus: 404, Fault: "client"}
	}
	b, ok, _ := p.col(req, "qmove").Get(ctx, h)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", Message: "Task does not exist.", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if str(rec["Status"]) != "RUNNING" {
		return nil, &spi.Fault{Code: "InvalidParameterValue", Message: "task is not in progress", HTTPStatus: 400, Fault: "client"}
	}
	rec["Status"] = "CANCELLING"
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "qmove").Put(ctx, h, nb)
	return &spi.Response{Output: map[string]any{"ApproximateNumberOfMessagesMoved": rec["ApproximateNumberOfMessagesMoved"]}}, nil
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
		public := map[string]any{
			"ApproximateNumberOfMessagesMoved":  rec["ApproximateNumberOfMessagesMoved"],
			"ApproximateNumberOfMessagesToMove": rec["ApproximateNumberOfMessagesToMove"],
			"SourceArn":                         rec["SourceArn"],
			"StartedTimestamp":                  rec["StartedTimestamp"],
			"Status":                            rec["Status"],
		}
		if destination := str(rec["DestinationArn"]); destination != "" {
			public["DestinationArn"] = destination
		}
		if reason := str(rec["FailureReason"]); reason != "" {
			public["FailureReason"] = reason
		}
		if str(rec["Status"]) == "RUNNING" {
			public["TaskHandle"] = rec["TaskHandle"]
			public["MaxNumberOfMessagesPerSecond"] = rec["MaxNumberOfMessagesPerSecond"]
		}
		out = append(out, public)
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

func arnPartition(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "us-iso-b-"):
		return "aws-iso-b"
	case strings.HasPrefix(region, "us-iso-"):
		return "aws-iso"
	default:
		return "aws"
	}
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
