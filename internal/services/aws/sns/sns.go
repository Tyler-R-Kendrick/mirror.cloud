// Package sns is the emulate-tier SNS pack.
package sns

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/lambda"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sqs"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.sns", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return &Pack{deps: d}, nil
	}})
}

// Pack implements SNS.
type Pack struct {
	deps    spi.Deps
	dedupMu sync.Mutex
}

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.sns" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateTopic", "DeleteTopic", "ListTopics", "GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "ConfirmSubscription", "Unsubscribe", "ListSubscriptions",
		"ListSubscriptionsByTopic", "Publish", "PublishBatch", "TagResource", "UntagResource",
		"AddPermission", "RemovePermission", "GetSubscriptionAttributes", "SetSubscriptionAttributes",
		"ListTagsForResource", "PutDataProtectionPolicy", "GetDataProtectionPolicy",
		"CreatePlatformApplication", "GetPlatformApplicationAttributes", "SetPlatformApplicationAttributes",
		"ListPlatformApplications", "DeletePlatformApplication",
		"CreatePlatformEndpoint", "GetEndpointAttributes", "SetEndpointAttributes",
		"ListEndpointsByPlatformApplication", "DeleteEndpoint",
		"SetSMSAttributes", "GetSMSAttributes",
		"OptInPhoneNumber", "CheckIfPhoneNumberIsOptedOut", "ListPhoneNumbersOptedOut",
		"CreateSMSSandboxPhoneNumber", "VerifySMSSandboxPhoneNumber", "DeleteSMSSandboxPhoneNumber",
		"ListSMSSandboxPhoneNumbers", "GetSMSSandboxAccountStatus", "ListOriginationNumbers"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateTopic":
		name := str(req.Input["Name"])
		attrs := asMap(req.Input["Attributes"])
		tags := req.Input["Tags"]
		if tags == nil {
			tags = flattenMembers(req.Input, "Tags")
		}
		if !validTopicName(name) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Name", HTTPStatus: 400, Fault: "client"}
		}
		fifo := str(attrs["FifoTopic"]) == "true"
		if strings.HasSuffix(name, ".fifo") != fifo {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: FifoTopic", HTTPStatus: 400, Fault: "client"}
		}
		arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", req.Identity.Region, req.Identity.Account, name)
		if existing, ok, _ := p.col(req, "topics").Get(ctx, name); ok {
			var current map[string]any
			_ = json.Unmarshal(existing, &current)
			for key, value := range attrs {
				if str(asMap(current["attrs"])[key]) != str(value) {
					return nil, &spi.Fault{Code: "TopicAlreadyExists", Message: "Topic already exists with a different attribute value.", HTTPStatus: 400, Fault: "client"}
				}
			}
			if len(asSlice(tags)) > 0 {
				existingTags, found, _ := p.col(req, "tags").Get(ctx, str(current["arn"]))
				var decodedTags []any
				_ = json.Unmarshal(existingTags, &decodedTags)
				if !found || len(decodedTags) == 0 {
					return nil, &spi.Fault{Code: "TopicAlreadyExists", Message: "Topic already exists with a different attribute value.", HTTPStatus: 400, Fault: "client"}
				}
			}
			return &spi.Response{Output: map[string]any{"TopicArn": current["arn"]}}, nil
		}
		b, _ := json.Marshal(map[string]any{"arn": arn, "name": name, "attrs": attrs})
		_ = p.col(req, "topics").Put(ctx, name, b)
		if len(asSlice(tags)) > 0 {
			tb, _ := json.Marshal(tags)
			_ = p.col(req, "tags").Put(ctx, arn, tb)
		}
		return &spi.Response{Output: map[string]any{"TopicArn": arn}}, nil
	case "ListTopics":
		kvs, _, _ := p.col(req, "topics").List(ctx, "", "", 0)
		var topics []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			topics = append(topics, map[string]any{"TopicArn": m["arn"]})
		}
		page, next, fault := snsPage(req.Input, topics)
		if fault != nil {
			return nil, fault
		}
		out := map[string]any{"Topics": page}
		if next != "" {
			out["NextToken"] = next
		}
		return &spi.Response{Output: out}, nil
	case "DeleteTopic":
		arn := str(req.Input["TopicArn"])
		name := topicName(arn)
		_ = p.col(req, "topics").Delete(ctx, name)
		_ = p.col(req, "tags").Delete(ctx, arn)
		_ = p.col(req, "dpp").Delete(ctx, arn)
		kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var sub map[string]any
			if json.Unmarshal(kv.Value, &sub) == nil && str(sub["TopicArn"]) == arn {
				_ = p.col(req, "subs").Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTopicAttributes":
		arn := str(req.Input["TopicArn"])
		b, ok, _ := p.col(req, "topics").Get(ctx, topicName(arn))
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", HTTPStatus: 404, Fault: "client"}
		}
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		attrs := map[string]any{"TopicArn": m["arn"], "DisplayName": m["name"]}
		if extra, ok := m["attrs"].(map[string]any); ok {
			for k, v := range extra {
				attrs[k] = v
			}
		}
		return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
	case "SetTopicAttributes":
		arn := str(req.Input["TopicArn"])
		name := topicName(arn)
		b, ok, _ := p.col(req, "topics").Get(ctx, name)
		if !ok {
			return nil, topicNotFoundFault()
		}
		m := map[string]any{"arn": arn, "name": name}
		_ = json.Unmarshal(b, &m)
		if m["attrs"] == nil {
			m["attrs"] = map[string]any{}
		}
		attrs, _ := m["attrs"].(map[string]any)
		if attrs == nil {
			attrs = map[string]any{}
		}
		attributeName := str(req.Input["AttributeName"])
		if attributeName == "FifoTopic" {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Cannot modify FifoTopic after creation", HTTPStatus: 400, Fault: "client"}
		}
		if attributeName == "DeliveryPolicy" && str(req.Input["AttributeValue"]) == "" {
			delete(attrs, attributeName)
		} else {
			attrs[attributeName] = req.Input["AttributeValue"]
		}
		m["attrs"] = attrs
		nb, _ := json.Marshal(m)
		_ = p.col(req, "topics").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "Publish":
		if phone := str(req.Input["PhoneNumber"]); phone != "" && str(req.Input["TopicArn"]) == "" && str(req.Input["TargetArn"]) == "" {
			if !validSMSNumber(phone) {
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PhoneNumber", HTTPStatus: 400, Fault: "client"}
			}
			if fault := validatePublishMessage(req); fault != nil {
				return nil, fault
			}
			_ = p.deps.Bus.Publish(ctx, "sns:sms:"+phone, []byte(str(req.Input["Message"])))
			return &spi.Response{Output: map[string]any{"MessageId": p.deps.Rand.Hex(16)}}, nil
		}
		if target := str(req.Input["TargetArn"]); target != "" && str(req.Input["TopicArn"]) == "" && endpointResourceARN(target) {
			if fault := p.validateEndpointTarget(ctx, req, target); fault != nil {
				return nil, fault
			}
			if fault := validatePublishMessage(req); fault != nil {
				return nil, fault
			}
			if message, ok := p.platformEndpointMessage(ctx, req, target, str(req.Input["Message"]), str(req.Input["MessageStructure"])); ok {
				_ = p.deps.Bus.Publish(ctx, "sns:"+target, []byte(message))
			}
			return &spi.Response{Output: map[string]any{"MessageId": p.deps.Rand.Hex(16)}}, nil
		}
		if fault := p.validatePublishTarget(ctx, req, topicARN(req.Input)); fault != nil {
			return nil, fault
		}
		if fault := validatePublishMessage(req); fault != nil {
			return nil, fault
		}
		return p.publishOne(ctx, req, str(req.Input["Message"]), messageAttrs(req.Input))
	case "PublishBatch":
		if fault := p.validatePublishTarget(ctx, req, topicARN(req.Input)); fault != nil {
			return nil, fault
		}
		entries, _ := req.Input["Entries"].([]any)
		if len(entries) == 0 && str(req.Input["Message"]) != "" {
			if fault := validatePublishMessage(req); fault != nil {
				return nil, fault
			}
			return p.publishOne(ctx, req, str(req.Input["Message"]), asMap(req.Input["MessageAttributes"]))
		}
		if len(entries) == 0 {
			return nil, &spi.Fault{Code: "EmptyBatchRequest", Message: "There should be at least one PublishBatchRequestEntry in the request.", HTTPStatus: 400, Fault: "client"}
		}
		if len(entries) > 10 {
			return nil, &spi.Fault{Code: "TooManyEntriesInBatchRequest", Message: "Maximum number of entries per request are 10.", HTTPStatus: 400, Fault: "client"}
		}
		var ok []any
		seenIDs := map[string]bool{}
		totalBytes := 0
		for _, e := range entries {
			m := asMap(e)
			entryID := str(m["Id"])
			if !validBatchEntryID(entryID) || seenIDs[entryID] {
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "Id is invalid or duplicated.", HTTPStatus: 400, Fault: "client"}
			}
			seenIDs[entryID] = true
			attrs := asMap(m["MessageAttributes"])
			if len(attrs) == 0 {
				attrs = messageAttrs(m)
			}
			checkInput := map[string]any{"Message": m["Message"], "MessageAttributes": attrs}
			if _, present := m["Subject"]; present {
				checkInput["Subject"] = m["Subject"]
			}
			if _, present := m["MessageStructure"]; present {
				checkInput["MessageStructure"] = m["MessageStructure"]
			}
			entryCheck := &spi.Request{Input: checkInput}
			if fault := validatePublishMessage(entryCheck); fault != nil {
				return nil, fault
			}
			totalBytes += publishMessageSize(str(m["Message"]), attrs)
			if totalBytes > 262144 {
				return nil, &spi.Fault{Code: "BatchRequestTooLong", Message: "Batch requests cannot be longer than 262144 bytes.", HTTPStatus: 400, Fault: "client"}
			}
			entryInput := make(map[string]any, len(req.Input)+6)
			for key, value := range req.Input {
				entryInput[key] = value
			}
			for _, key := range []string{"MessageGroupId", "MessageDeduplicationId", "Subject", "MessageStructure"} {
				if value, present := m[key]; present {
					entryInput[key] = value
				}
			}
			entryInput["Message"] = m["Message"]
			entryInput["MessageAttributes"] = attrs
			entryReq := *req
			entryReq.Input = entryInput
			entryReq.Operation = "Publish"
			if fault := p.validatePublishTarget(ctx, &entryReq, topicARN(entryInput)); fault != nil {
				continue
			}
			resp, err := p.publishOne(ctx, &entryReq, str(m["Message"]), attrs)
			if err != nil {
				continue
			}
			ok = append(ok, map[string]any{"Id": m["Id"], "MessageId": resp.Output["MessageId"]})
		}
		return &spi.Response{Output: map[string]any{"Successful": ok}}, nil
	case "Subscribe":
		topicArn := str(req.Input["TopicArn"])
		if _, ok, _ := p.col(req, "topics").Get(ctx, topicName(topicArn)); !ok {
			return nil, topicNotFoundFault()
		}
		protocol := str(req.Input["Protocol"])
		switch protocol {
		case "http", "https", "email", "email-json", "sqs", "lambda", "application", "sms", "firehose":
		default:
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Protocol", HTTPStatus: 400, Fault: "client"}
		}
		if protocol == "sms" && !validSMSNumber(str(req.Input["Endpoint"])) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Endpoint", HTTPStatus: 400, Fault: "client"}
		}
		if protocol == "sqs" {
			if fault := p.validateSQSSubscription(ctx, req, topicArn, str(req.Input["Endpoint"])); fault != nil {
				return nil, fault
			}
		}
		sub := str(req.Input["TopicArn"]) + ":" + p.deps.Rand.UUID()
		attrs := asMap(req.Input["Attributes"])
		if raw := str(attrs["FilterPolicy"]); raw != "" {
			if fault := validateFilterPolicy(raw); fault != nil {
				return nil, fault
			}
		}
		if str(req.Input["FilterPolicy"]) != "" {
			attrs["FilterPolicy"] = req.Input["FilterPolicy"]
			if fault := validateFilterPolicy(str(req.Input["FilterPolicy"])); fault != nil {
				return nil, fault
			}
		}
		if str(req.Input["RawMessageDelivery"]) != "" {
			attrs["RawMessageDelivery"] = strings.ToLower(str(req.Input["RawMessageDelivery"]))
		}
		for name, value := range attrs {
			if fault := validateSubscriptionAttribute(name, str(value)); fault != nil {
				return nil, fault
			}
		}
		if raw, ok := attrs["RawMessageDelivery"]; ok {
			attrs["RawMessageDelivery"] = strings.ToLower(str(raw))
		}
		requestedFilter := str(attrs["FilterPolicy"])
		requestedRaw := strings.ToLower(str(attrs["RawMessageDelivery"]))
		rawProvided := attrs["RawMessageDelivery"] != nil
		requestedScope := str(attrs["FilterPolicyScope"])
		scopeProvided := attrs["FilterPolicyScope"] != nil
		if requestedScope == "" {
			requestedScope = "MessageAttributes"
		}
		kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var existing map[string]any
			if json.Unmarshal(kv.Value, &existing) != nil || str(existing["TopicArn"]) != str(req.Input["TopicArn"]) || str(existing["Protocol"]) != str(req.Input["Protocol"]) || str(existing["Endpoint"]) != str(req.Input["Endpoint"]) {
				continue
			}
			existingFilter := str(existing["FilterPolicy"])
			existingRaw := strings.ToLower(str(existing["RawMessageDelivery"]))
			existingScope := str(existing["FilterPolicyScope"])
			if existingScope == "" {
				existingScope = "MessageAttributes"
			}
			if (attrs["FilterPolicy"] == nil || existingFilter == requestedFilter) && (!rawProvided || existingRaw == requestedRaw) && (!scopeProvided || existingScope == requestedScope) {
				return &spi.Response{Output: map[string]any{"SubscriptionArn": existing["SubscriptionArn"]}}, nil
			}
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Subscription already exists with different attributes.", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{
			"SubscriptionArn":    sub,
			"TopicArn":           str(req.Input["TopicArn"]),
			"Protocol":           str(req.Input["Protocol"]),
			"Endpoint":           str(req.Input["Endpoint"]),
			"FilterPolicy":       attrs["FilterPolicy"],
			"FilterPolicyScope":  attrs["FilterPolicyScope"],
			"RawMessageDelivery": attrs["RawMessageDelivery"],
			"Confirmed":          true,
		}
		proto := str(req.Input["Protocol"])
		if proto == "http" || proto == "https" {
			tok := p.deps.Rand.Hex(16)
			rec["Confirmed"] = false
			rec["Token"] = tok
			_ = p.col(req, "pending").Put(ctx, tok, mustJSON(rec))
			p.httpPost(str(req.Input["Endpoint"]), map[string]any{
				"Type": "SubscriptionConfirmation", "Token": tok, "TopicArn": rec["TopicArn"],
				"SubscribeURL": "http://127.0.0.1/confirm?Token=" + tok,
			})
			b, _ := json.Marshal(rec)
			_ = p.col(req, "subs").Put(ctx, sub, b)
			return &spi.Response{Output: map[string]any{"SubscriptionArn": "pending confirmation"}}, nil
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "subs").Put(ctx, sub, b)
		return &spi.Response{Output: map[string]any{"SubscriptionArn": sub}}, nil
	case "Unsubscribe":
		arn := str(req.Input["SubscriptionArn"])
		if !validSubscriptionARN(arn) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: SubscriptionArn", HTTPStatus: 400, Fault: "client"}
		}
		_ = p.col(req, "subs").Delete(ctx, arn)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		want := str(req.Input["TopicArn"])
		kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
		var subs []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			if want != "" && str(m["TopicArn"]) != want {
				continue
			}
			subs = append(subs, m)
		}
		page, next, fault := snsPage(req.Input, subs)
		if fault != nil {
			return nil, fault
		}
		out := map[string]any{"Subscriptions": page}
		if next != "" {
			out["NextToken"] = next
		}
		return &spi.Response{Output: out}, nil
	case "ConfirmSubscription":
		tok := str(req.Input["Token"])
		if topicArn := str(req.Input["TopicArn"]); topicArn != "" {
			if _, ok, _ := p.col(req, "topics").Get(ctx, topicName(topicArn)); !ok {
				return nil, topicNotFoundFault()
			}
		}
		b, ok, _ := p.col(req, "pending").Get(ctx, tok)
		if !ok {
			if str(req.Input["TopicArn"]) != "" {
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Token", HTTPStatus: 400, Fault: "client"}
			}
			return &spi.Response{Output: map[string]any{"SubscriptionArn": tok}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if topicArn := str(req.Input["TopicArn"]); topicArn != "" && str(rec["TopicArn"]) != topicArn {
			return nil, topicNotFoundFault()
		}
		rec["Confirmed"] = true
		arn := str(rec["SubscriptionArn"])
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "subs").Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{"SubscriptionArn": arn}}, nil
	case "TagResource":
		arn := str(req.Input["ResourceArn"])
		if topicResourceARN(arn) {
			if _, ok, _ := p.col(req, "topics").Get(ctx, topicName(arn)); !ok {
				return nil, topicNotFoundFault()
			}
		}
		tags := req.Input["Tags"]
		if tags == nil {
			tags = flattenMembers(req.Input, "Tags")
		}
		seen := map[string]bool{}
		for _, tag := range asSlice(tags) {
			key := str(asMap(tag)["Key"])
			if key != "" && seen[key] {
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "Duplicate tag key", HTTPStatus: 400, Fault: "client"}
			}
			seen[key] = true
		}
		current := []any{}
		if b, found, _ := p.col(req, "tags").Get(ctx, arn); found {
			_ = json.Unmarshal(b, &current)
		}
		byKey := map[string]any{}
		for _, tag := range current {
			m := asMap(tag)
			if key := str(m["Key"]); key != "" {
				byKey[key] = tag
			}
		}
		for _, tag := range asSlice(tags) {
			m := asMap(tag)
			if key := str(m["Key"]); key != "" {
				byKey[key] = tag
			}
		}
		merged := make([]any, 0, len(byKey))
		for _, tag := range byKey {
			merged = append(merged, tag)
		}
		sort.Slice(merged, func(i, j int) bool { return str(asMap(merged[i])["Key"]) < str(asMap(merged[j])["Key"]) })
		b, _ := json.Marshal(merged)
		_ = p.col(req, "tags").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := str(req.Input["ResourceArn"])
		keys := req.Input["TagKeys"]
		if keys == nil {
			keys = flattenValues(req.Input, "TagKeys")
			if len(asSlice(keys)) == 0 {
				_ = p.col(req, "tags").Delete(ctx, arn)
				return &spi.Response{Output: map[string]any{}}, nil
			}
		}
		if len(asSlice(keys)) == 0 {
			return &spi.Response{Output: map[string]any{}}, nil
		}
		b, found, _ := p.col(req, "tags").Get(ctx, arn)
		if found {
			var current []any
			_ = json.Unmarshal(b, &current)
			remove := map[string]bool{}
			for _, key := range asSlice(keys) {
				remove[str(key)] = true
			}
			kept := make([]any, 0, len(current))
			for _, tag := range current {
				if !remove[str(asMap(tag)["Key"])] {
					kept = append(kept, tag)
				}
			}
			if len(kept) == 0 {
				_ = p.col(req, "tags").Delete(ctx, arn)
			} else {
				nb, _ := json.Marshal(kept)
				_ = p.col(req, "tags").Put(ctx, arn, nb)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddPermission", "RemovePermission":
		return p.topicPermission(ctx, req)
	case "GetSubscriptionAttributes", "SetSubscriptionAttributes":
		return p.subAttrs(ctx, req)
	case "ListTagsForResource":
		return p.listTags(ctx, req)
	case "PutDataProtectionPolicy", "GetDataProtectionPolicy":
		return p.dataProtection(ctx, req)
	case "CreatePlatformApplication", "GetPlatformApplicationAttributes", "SetPlatformApplicationAttributes",
		"ListPlatformApplications", "DeletePlatformApplication":
		return p.platformApp(ctx, req)
	case "CreatePlatformEndpoint", "GetEndpointAttributes", "SetEndpointAttributes",
		"ListEndpointsByPlatformApplication", "DeleteEndpoint":
		return p.platformEndpoint(ctx, req)
	case "SetSMSAttributes", "GetSMSAttributes":
		return p.smsAttrs(ctx, req)
	case "OptInPhoneNumber", "CheckIfPhoneNumberIsOptedOut", "ListPhoneNumbersOptedOut":
		return p.smsOpt(ctx, req)
	case "CreateSMSSandboxPhoneNumber", "VerifySMSSandboxPhoneNumber", "DeleteSMSSandboxPhoneNumber",
		"ListSMSSandboxPhoneNumbers", "GetSMSSandboxAccountStatus":
		return p.smsSandbox(ctx, req)
	case "ListOriginationNumbers":
		kvs, _, _ := p.col(req, "orig").List(ctx, "", "", 0)
		var nums []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			nums = append(nums, rec)
		}
		return &spi.Response{Output: map[string]any{"PhoneNumbers": nums}}, nil
	default:
		return nil, spi.NotImplemented("aws.sns", req.Operation, "emulate")
	}
}

func (p *Pack) validateSQSSubscription(ctx context.Context, req *spi.Request, topicArn, endpoint string) *spi.Fault {
	name, account, region := endpoint, req.Identity.Account, req.Identity.Region
	parts := strings.Split(endpoint, ":")
	if len(parts) == 6 && parts[0] == "arn" && parts[2] == "sqs" {
		region, account, name = parts[3], parts[4], parts[5]
	} else {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Endpoint", HTTPStatus: 400, Fault: "client"}
	}
	queue, ok, _ := p.deps.Store.Scope(account, region).Collection("queues").Get(ctx, name)
	if !ok {
		// SNS accepts a well-formed cross-account/cross-region queue ARN even
		// when the queue is not locally discoverable.
		return nil
	}
	var queueMeta map[string]any
	_ = json.Unmarshal(queue, &queueMeta)
	queueFIFO := str(asMap(queueMeta["attrs"])["FifoQueue"]) == "true" || strings.HasSuffix(name, ".fifo")
	if !strings.HasSuffix(topicName(topicArn), ".fifo") && queueFIFO {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: FIFO queue requires a FIFO topic", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func (p *Pack) publishOne(ctx context.Context, req *spi.Request, body string, msgAttrs map[string]any) (*spi.Response, error) {
	arn := topicARN(req.Input)
	fifo := false
	if b, found, _ := p.col(req, "topics").Get(ctx, topicName(arn)); found {
		var topic map[string]any
		_ = json.Unmarshal(b, &topic)
		fifo = str(asMap(topic["attrs"])["FifoTopic"]) == "true"
	}
	dedupKey := ""
	if fifo {
		dedupKey = str(req.Input["MessageDeduplicationId"])
		if dedupKey == "" {
			digest := sha256.Sum256([]byte(body))
			dedupKey = fmt.Sprintf("%x", digest[:])
		}
	}
	if dedupKey != "" {
		p.dedupMu.Lock()
		defer p.dedupMu.Unlock()
		if b, found, _ := p.col(req, "snsdedup").Get(ctx, arn+"\x1f"+dedupKey); found {
			var prior map[string]any
			until, _ := strconv.ParseInt(fmt.Sprint(prior["until"]), 10, 64)
			if json.Unmarshal(b, &prior) == nil {
				if value, ok := prior["until"].(float64); ok {
					until = int64(value)
				}
			}
			if until > 0 && p.deps.Clock.Now().UnixNano() < until {
				return &spi.Response{Output: map[string]any{"MessageId": prior["id"]}}, nil
			}
		}
	}
	mid := p.deps.Rand.Hex(16)
	_ = p.deps.Bus.Publish(ctx, "sns:"+arn, []byte(body))
	kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
	for _, kv := range kvs {
		var sub map[string]any
		_ = json.Unmarshal(kv.Value, &sub)
		if str(sub["TopicArn"]) != arn {
			continue
		}
		if conf, ok := sub["Confirmed"].(bool); ok && !conf {
			continue
		}
		filterAttrs := msgAttrs
		if str(sub["FilterPolicyScope"]) == "MessageBody" {
			filterAttrs = map[string]any{}
			if json.Unmarshal([]byte(body), &filterAttrs) != nil {
				continue
			}
		}
		if !matchFilter(sub["FilterPolicy"], filterAttrs) {
			continue
		}
		protocol := str(sub["Protocol"])
		if protocol == "application" {
			if message, ok := p.platformEndpointMessage(ctx, req, str(sub["Endpoint"]), body, str(req.Input["MessageStructure"])); ok {
				_ = p.deps.Bus.Publish(ctx, "sns:"+str(sub["Endpoint"]), []byte(message))
			}
			continue
		}
		if protocol == "sms" {
			_ = p.deps.Bus.Publish(ctx, "sns:sms:"+str(sub["Endpoint"]), []byte(structuredMessage(body, str(req.Input["MessageStructure"]), protocol)))
			continue
		}
		message := structuredMessage(body, str(req.Input["MessageStructure"]), protocol)
		payload := message
		if str(sub["RawMessageDelivery"]) != "true" {
			notification := map[string]any{"Type": "Notification", "Message": message, "TopicArn": arn, "MessageId": mid}
			if subject := str(req.Input["Subject"]); subject != "" {
				notification["Subject"] = subject
			}
			env, _ := json.Marshal(notification)
			payload = string(env)
		}
		switch str(sub["Protocol"]) {
		case "sqs":
			p.deliverSQS(ctx, req, str(sub["Endpoint"]), payload)
		case "lambda":
			_, _ = p.deliverLambda(ctx, req, sub, message, mid, msgAttrs)
		case "http", "https":
			notification := map[string]any{"Type": "Notification", "Message": message, "TopicArn": arn, "MessageId": mid}
			if subject := str(req.Input["Subject"]); subject != "" {
				notification["Subject"] = subject
			}
			p.httpPost(str(sub["Endpoint"]), notification)
		}
	}
	if dedupKey != "" {
		_ = p.col(req, "snsdedup").Put(ctx, arn+"\x1f"+dedupKey, mustJSON(map[string]any{"id": mid, "until": p.deps.Clock.Now().Add(5 * time.Minute).UnixNano()}))
	}
	return &spi.Response{Output: map[string]any{"MessageId": mid}}, nil
}

func (p *Pack) platformEndpointMessage(ctx context.Context, req *spi.Request, endpointARN, body, structure string) (string, bool) {
	b, found, _ := p.col(req, "platend").Get(ctx, endpointARN)
	if !found {
		return "", false
	}
	var endpoint map[string]any
	if json.Unmarshal(b, &endpoint) != nil || strings.EqualFold(str(endpoint["Enabled"]), "false") {
		return "", false
	}
	platform := "application"
	if app, ok, _ := p.col(req, "platapps").Get(ctx, str(endpoint["PlatformApplicationArn"])); ok {
		var record map[string]any
		if json.Unmarshal(app, &record) == nil && str(record["Platform"]) != "" {
			platform = str(record["Platform"])
		}
	}
	return structuredMessage(body, structure, platform), true
}

func (p *Pack) validatePublishTarget(ctx context.Context, req *spi.Request, arn string) *spi.Fault {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sns" || parts[3] == "" || parts[4] == "" || parts[5] == "" {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: TopicArn", HTTPStatus: 400, Fault: "client"}
	}
	if parts[3] != req.Identity.Region || parts[4] != req.Identity.Account {
		return topicNotFoundFault()
	}
	b, ok, _ := p.col(req, "topics").Get(ctx, parts[5])
	if !ok {
		return topicNotFoundFault()
	}
	var topic map[string]any
	_ = json.Unmarshal(b, &topic)
	attrs := asMap(topic["attrs"])
	fifo := str(attrs["FifoTopic"]) == "true"
	if req.Operation == "PublishBatch" {
		return nil
	}
	group, groupProvided := req.Input["MessageGroupId"]
	dedup, dedupProvided := req.Input["MessageDeduplicationId"]
	if fifo {
		if !groupProvided || str(group) == "" {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: MessageGroupId", HTTPStatus: 400, Fault: "client"}
		}
		if !validFIFOField(str(group)) {
			return &spi.Fault{Code: "InvalidParameterValue", Message: "MessageGroupId is invalid.", HTTPStatus: 400, Fault: "client"}
		}
		if dedupProvided && !validFIFOField(str(dedup)) {
			return &spi.Fault{Code: "InvalidParameterValue", Message: "MessageDeduplicationId is invalid.", HTTPStatus: 400, Fault: "client"}
		}
		if !dedupProvided && str(attrs["ContentBasedDeduplication"]) != "true" {
			return &spi.Fault{Code: "InvalidParameterValue", Message: "MessageDeduplicationId is required when ContentBasedDeduplication is disabled.", HTTPStatus: 400, Fault: "client"}
		}
	} else if dedupProvided {
		return &spi.Fault{Code: "InvalidParameter", Message: "MessageDeduplicationId is only supported for FIFO topics.", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func endpointResourceARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 6 && parts[0] == "arn" && parts[2] == "sns" && strings.Contains(parts[5], "/endpoint/")
}

func (p *Pack) validateEndpointTarget(ctx context.Context, req *spi.Request, arn string) *spi.Fault {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[3] != req.Identity.Region || parts[4] != req.Identity.Account {
		return &spi.Fault{Code: "NotFound", Message: "Endpoint does not exist", HTTPStatus: 404, Fault: "client"}
	}
	b, ok, _ := p.col(req, "platend").Get(ctx, arn)
	if !ok {
		return &spi.Fault{Code: "NotFound", Message: "Endpoint does not exist", HTTPStatus: 404, Fault: "client"}
	}
	var endpoint map[string]any
	if json.Unmarshal(b, &endpoint) != nil {
		return &spi.Fault{Code: "NotFound", Message: "Endpoint does not exist", HTTPStatus: 404, Fault: "client"}
	}
	if strings.EqualFold(str(endpoint["Enabled"]), "false") {
		return &spi.Fault{Code: "EndpointDisabled", Message: "Endpoint is disabled", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func validFIFOField(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func validBatchEntryID(value string) bool {
	if len(value) < 1 || len(value) > 80 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func publishMessageSize(body string, attrs map[string]any) int {
	size := len([]byte(body))
	for name, raw := range attrs {
		attribute := asMap(raw)
		size += len(name) + len(str(attribute["DataType"])) + len(str(attribute["StringValue"])) + len(str(attribute["BinaryValue"]))
	}
	return size
}

func validatePublishMessage(req *spi.Request) *spi.Fault {
	body := str(req.Input["Message"])
	if body == "" {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Message", HTTPStatus: 400, Fault: "client"}
	}
	if publishMessageSize(body, messageAttrs(req.Input)) > 262144 {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Message too long", HTTPStatus: 400, Fault: "client"}
	}
	if subject, present := req.Input["Subject"]; present && str(subject) == "" {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Subject", HTTPStatus: 400, Fault: "client"}
	}
	structure := str(req.Input["MessageStructure"])
	if structure != "" && structure != "json" {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: MessageStructure", HTTPStatus: 400, Fault: "client"}
	}
	if structure == "json" {
		var values map[string]any
		if json.Unmarshal([]byte(body), &values) != nil {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: MessageStructure", HTTPStatus: 400, Fault: "client"}
		}
		if _, ok := values["default"]; !ok {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: MessageStructure must contain a default message", HTTPStatus: 400, Fault: "client"}
		}
		if _, ok := values["default"].(string); !ok {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: MessageStructure values must be strings", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func structuredMessage(body, structure, protocol string) string {
	if structure != "json" {
		return body
	}
	var values map[string]any
	if json.Unmarshal([]byte(body), &values) != nil {
		return body
	}
	if value, ok := values[protocol].(string); ok && value != "" {
		return value
	}
	value, _ := values["default"].(string)
	return value
}

func topicNotFoundFault() *spi.Fault {
	return &spi.Fault{Code: "NotFound", Message: "Topic does not exist", HTTPStatus: 404, Fault: "client"}
}

func (p *Pack) deliverLambda(ctx context.Context, req *spi.Request, sub map[string]any, body, messageID string, attrs map[string]any) (*spi.Response, error) {
	endpoint := str(sub["Endpoint"])
	name := endpoint
	if _, rest, ok := strings.Cut(endpoint, ":function:"); ok {
		name = rest
	}
	if i := strings.IndexByte(name, ':'); i >= 0 {
		name = name[:i]
	}
	in := p.lambdaNotification(req, sub, body, messageID, attrs)
	in["FunctionName"] = name
	in["InvocationType"] = "Event"
	return lambda.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "Invoke", Input: in})
}

func (p *Pack) lambdaNotification(req *spi.Request, sub map[string]any, body, messageID string, attrs map[string]any) map[string]any {
	return map[string]any{
		"Records": []any{map[string]any{
			"EventSource": "aws:sns", "EventVersion": "1.0", "EventSubscriptionArn": sub["SubscriptionArn"],
			"Sns": map[string]any{
				"Type": "Notification", "MessageId": messageID, "TopicArn": sub["TopicArn"], "Subject": req.Input["Subject"],
				"Message": body, "Timestamp": p.deps.Clock.Now().UTC().Format(time.RFC3339Nano), "MessageAttributes": attrs,
				"SignatureVersion": "1", "Signature": "", "SigningCertUrl": "https://sns." + req.Identity.Region + ".amazonaws.com/SimpleNotificationService.pem",
				"UnsubscribeUrl": "http://127.0.0.1:4566/?Action=Unsubscribe&SubscriptionArn=" + str(sub["SubscriptionArn"]),
			},
		}},
	}
}

func (p *Pack) deliverSQS(ctx context.Context, req *spi.Request, endpoint, body string) {
	name := endpoint
	if i := strings.LastIndexAny(endpoint, "/:"); i >= 0 {
		name = endpoint[i+1:]
	}
	in := map[string]any{"QueueName": name, "MessageBody": body}
	for _, key := range []string{"MessageGroupId", "MessageDeduplicationId"} {
		if value := str(req.Input[key]); value != "" {
			in[key] = value
		}
	}
	_, _ = sqs.New(p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "SendMessage", Input: in})
}

func (p *Pack) httpPost(endpoint string, payload map[string]any) {
	if endpoint == "" {
		return
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	cli := &http.Client{Timeout: 2 * time.Second}
	res, err := cli.Do(req)
	if err != nil {
		return
	}
	ioDiscard(res)
}

func ioDiscard(res *http.Response) {
	if res != nil && res.Body != nil {
		_, _ = io.Copy(io.Discard, res.Body)
		_ = res.Body.Close()
	}
}

func matchFilter(policy any, attrs map[string]any) bool {
	raw := str(policy)
	if raw == "" && policy != nil {
		b, _ := json.Marshal(policy)
		raw = string(b)
	}
	if raw == "" || raw == "null" {
		return true
	}
	var pol map[string]any
	if json.Unmarshal([]byte(raw), &pol) != nil || len(pol) == 0 {
		return true
	}
	if alternatives, ok := pol["$or"].([]any); ok {
		matched := false
		for _, alternative := range alternatives {
			if child, ok := alternative.(map[string]any); ok && matchPolicy(child, attrs) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return matchPolicy(pol, attrs)
}

func matchPolicy(pol map[string]any, attrs map[string]any) bool {
	for key, want := range pol {
		if key == "$or" {
			continue
		}
		if nested, ok := want.(map[string]any); ok {
			child, present := attrs[key].(map[string]any)
			if !present || !matchPolicy(nested, child) {
				return false
			}
			continue
		}
		value, present := attrs[key]
		if !clauseMatch(want, attrValue(value), present) {
			return false
		}
	}
	return true
}

func attrValue(v any) string {
	m := asMap(v)
	if s := str(m["Value"]); s != "" {
		return s
	}
	if s := str(m["StringValue"]); s != "" {
		return s
	}
	return str(v)
}

func clauseMatch(want any, got string, present bool) bool {
	arr, ok := want.([]any)
	if !ok {
		return fmt.Sprint(want) == got
	}
	for _, w := range arr {
		if op := asMap(w); len(op) > 0 {
			if exists, ok := op["exists"].(bool); ok {
				if exists == present {
					return true
				}
				continue
			}
		}
		if s := str(w); s != "" && s == got {
			return true
		}
		m := asMap(w)
		if p := str(m["prefix"]); p != "" && strings.HasPrefix(got, p) {
			return true
		}
		if s := str(m["suffix"]); s != "" && strings.HasSuffix(got, s) {
			return true
		}
		if s := str(m["equals-ignore-case"]); s != "" && strings.EqualFold(got, s) {
			return true
		}
		if ab, ok := m["anything-but"]; ok {
			if !present {
				continue
			}
			blocked := false
			if values, ok := ab.([]any); ok {
				for _, value := range values {
					if str(value) == got {
						blocked = true
						break
					}
				}
			} else {
				blocked = str(ab) == got
			}
			if !blocked {
				return true
			}
			continue
		}
		if num, ok := m["numeric"].([]any); ok && numericMatch(num, got) {
			return true
		}
	}
	return false
}

func numericMatch(rule []any, got string) bool {
	if len(rule) < 2 || len(rule)%2 != 0 {
		return false
	}
	g, err := strconv.ParseFloat(got, 64)
	if err != nil {
		return false
	}
	for i := 0; i < len(rule); i += 2 {
		n, err := strconv.ParseFloat(fmt.Sprint(rule[i+1]), 64)
		if err != nil {
			return false
		}
		ok := false
		switch str(rule[i]) {
		case ">":
			ok = g > n
		case ">=":
			ok = g >= n
		case "<":
			ok = g < n
		case "<=":
			ok = g <= n
		case "=":
			ok = g == n
		}
		if !ok {
			return false
		}
	}
	return true
}

func messageAttrs(in map[string]any) map[string]any {
	if m := asMap(in["MessageAttributes"]); len(m) > 0 && m["entry"] == nil {
		return m
	}
	by := map[int]map[string]any{}
	for k, v := range in {
		rest, ok := strings.CutPrefix(k, "MessageAttributes.entry.")
		if !ok {
			continue
		}
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			continue
		}
		n, _ := strconv.Atoi(parts[0])
		slot := by[n]
		if slot == nil {
			slot = map[string]any{}
			by[n] = slot
		}
		switch parts[1] {
		case "Name":
			slot["Name"] = str(v)
		case "Value.StringValue", "Value":
			slot["StringValue"] = str(v)
			slot["Value"] = str(v)
		}
	}
	out := map[string]any{}
	for _, slot := range by {
		name := str(slot["Name"])
		if name != "" {
			out[name] = slot
		}
	}
	return out
}

func flattenMembers(in map[string]any, prefix string) []any {
	by := map[int]map[string]any{}
	p := prefix + ".member."
	for k, v := range in {
		rest, ok := strings.CutPrefix(k, p)
		if !ok {
			continue
		}
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			continue
		}
		n, _ := strconv.Atoi(parts[0])
		slot := by[n]
		if slot == nil {
			slot = map[string]any{}
			by[n] = slot
		}
		slot[parts[1]] = v
	}
	var out []any
	for i := 1; i <= len(by)+8; i++ {
		if m, ok := by[i]; ok {
			out = append(out, m)
		}
	}
	return out
}

func flattenValues(in map[string]any, prefix string) []any {
	by := map[int]any{}
	p := prefix + ".member."
	for k, v := range in {
		n, ok := strings.CutPrefix(k, p)
		if !ok {
			continue
		}
		i, err := strconv.Atoi(n)
		if err == nil {
			by[i] = v
		}
	}
	values := make([]any, 0, len(by))
	for i := 1; i <= len(by); i++ {
		if value, ok := by[i]; ok {
			values = append(values, value)
		}
	}
	return values
}

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func asSlice(v any) []any {
	values, _ := v.([]any)
	return values
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func topicName(arn string) string {
	if i := strings.LastIndex(arn, ":"); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func topicARN(in map[string]any) string {
	if arn := str(in["TopicArn"]); arn != "" {
		return arn
	}
	return str(in["TargetArn"])
}

func topicResourceARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 6 && parts[0] == "arn" && parts[2] == "sns" && !strings.Contains(parts[5], "/")
}

func validTopicName(name string) bool {
	if len(name) < 1 || len(name) > 256 || strings.HasSuffix(name, ".fifo") {
		return len(name) >= 5 && len(name) <= 256 && strings.HasSuffix(name, ".fifo") && validTopicChars(strings.TrimSuffix(name, ".fifo"))
	}
	return validTopicChars(name)
}

func validSMSNumber(number string) bool {
	if len(number) < 3 || len(number) > 16 || number[0] != '+' || number[1] < '1' || number[1] > '9' {
		return false
	}
	for _, r := range number[2:] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func validSubscriptionARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 7 && parts[0] == "arn" && parts[2] == "sns" && parts[3] != "" && parts[4] != "" && parts[5] != "" && parts[6] != ""
}

func validTopicChars(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func str(v any) string { s, _ := v.(string); return s }

func snsPage(input map[string]any, items []any) ([]any, string, *spi.Fault) {
	offset := 0
	if token := str(input["NextToken"]); token != "" {
		decoded, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return nil, "", &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: NextToken", HTTPStatus: 400, Fault: "client"}
		}
		offset, err = strconv.Atoi(string(decoded))
		if err != nil || offset < 0 {
			return nil, "", &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: NextToken", HTTPStatus: 400, Fault: "client"}
		}
	}
	if offset > len(items) {
		return nil, "", &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: NextToken", HTTPStatus: 400, Fault: "client"}
	}
	end := offset + 100
	if end > len(items) {
		end = len(items)
	}
	var next string
	if end < len(items) {
		next = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return items[offset:end], next, nil
}
