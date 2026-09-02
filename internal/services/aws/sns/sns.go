// Package sns is the emulate-tier SNS pack.
package sns

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
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
type Pack struct{ deps spi.Deps }

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
		arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", req.Identity.Region, req.Identity.Account, name)
		b, _ := json.Marshal(map[string]any{"arn": arn, "name": name})
		_ = p.col(req, "topics").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"TopicArn": arn}}, nil
	case "ListTopics":
		kvs, _, _ := p.col(req, "topics").List(ctx, "", "", 0)
		var topics []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			topics = append(topics, map[string]any{"TopicArn": m["arn"]})
		}
		return &spi.Response{Output: map[string]any{"Topics": topics}}, nil
	case "DeleteTopic":
		arn := str(req.Input["TopicArn"])
		name := topicName(arn)
		_ = p.col(req, "topics").Delete(ctx, name)
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
		m := map[string]any{"arn": arn, "name": name}
		if ok {
			_ = json.Unmarshal(b, &m)
		}
		if m["attrs"] == nil {
			m["attrs"] = map[string]any{}
		}
		attrs, _ := m["attrs"].(map[string]any)
		if attrs == nil {
			attrs = map[string]any{}
		}
		attrs[str(req.Input["AttributeName"])] = req.Input["AttributeValue"]
		m["attrs"] = attrs
		nb, _ := json.Marshal(m)
		_ = p.col(req, "topics").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "Publish":
		return p.publishOne(ctx, req, str(req.Input["Message"]), messageAttrs(req.Input))
	case "PublishBatch":
		entries, _ := req.Input["Entries"].([]any)
		var ok []any
		for _, e := range entries {
			m := asMap(e)
			attrs := asMap(m["MessageAttributes"])
			if len(attrs) == 0 {
				attrs = messageAttrs(m)
			}
			resp, err := p.publishOne(ctx, req, str(m["Message"]), attrs)
			if err != nil {
				continue
			}
			ok = append(ok, map[string]any{"Id": m["Id"], "MessageId": resp.Output["MessageId"]})
		}
		if len(ok) == 0 && str(req.Input["Message"]) != "" {
			return p.publishOne(ctx, req, str(req.Input["Message"]), asMap(req.Input["MessageAttributes"]))
		}
		return &spi.Response{Output: map[string]any{"Successful": ok}}, nil
	case "Subscribe":
		sub := str(req.Input["TopicArn"]) + ":" + p.deps.Rand.UUID()
		attrs := asMap(req.Input["Attributes"])
		if str(req.Input["FilterPolicy"]) != "" {
			attrs["FilterPolicy"] = req.Input["FilterPolicy"]
		}
		if str(req.Input["RawMessageDelivery"]) != "" {
			attrs["RawMessageDelivery"] = req.Input["RawMessageDelivery"]
		}
		rec := map[string]any{
			"SubscriptionArn":    sub,
			"TopicArn":           str(req.Input["TopicArn"]),
			"Protocol":           str(req.Input["Protocol"]),
			"Endpoint":           str(req.Input["Endpoint"]),
			"FilterPolicy":       attrs["FilterPolicy"],
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
		_ = p.col(req, "subs").Delete(ctx, str(req.Input["SubscriptionArn"]))
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
		return &spi.Response{Output: map[string]any{"Subscriptions": subs}}, nil
	case "ConfirmSubscription":
		tok := str(req.Input["Token"])
		b, ok, _ := p.col(req, "pending").Get(ctx, tok)
		if !ok {
			return &spi.Response{Output: map[string]any{"SubscriptionArn": tok}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		rec["Confirmed"] = true
		arn := str(rec["SubscriptionArn"])
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "subs").Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{"SubscriptionArn": arn}}, nil
	case "TagResource":
		arn := str(req.Input["ResourceArn"])
		tags := req.Input["Tags"]
		if tags == nil {
			tags = flattenMembers(req.Input, "Tags")
		}
		b, _ := json.Marshal(tags)
		_ = p.col(req, "tags").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		arn := str(req.Input["ResourceArn"])
		_ = p.col(req, "tags").Delete(ctx, arn)
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

func (p *Pack) publishOne(ctx context.Context, req *spi.Request, body string, msgAttrs map[string]any) (*spi.Response, error) {
	arn := str(req.Input["TopicArn"])
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
		if !matchFilter(sub["FilterPolicy"], msgAttrs) {
			continue
		}
		payload := body
		if str(sub["RawMessageDelivery"]) != "true" {
			notification := map[string]any{"Type": "Notification", "Message": body, "TopicArn": arn, "MessageId": mid}
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
			_, _ = p.deliverLambda(ctx, req, sub, body, mid, msgAttrs)
		case "http", "https":
			notification := map[string]any{"Type": "Notification", "Message": body, "TopicArn": arn, "MessageId": mid}
			if subject := str(req.Input["Subject"]); subject != "" {
				notification["Subject"] = subject
			}
			p.httpPost(str(sub["Endpoint"]), notification)
		}
	}
	return &spi.Response{Output: map[string]any{"MessageId": mid}}, nil
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
	for key, want := range pol {
		got := attrValue(attrs[key])
		if !clauseMatch(want, got) {
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

func clauseMatch(want any, got string) bool {
	arr, ok := want.([]any)
	if !ok {
		return fmt.Sprint(want) == got
	}
	for _, w := range arr {
		if s := str(w); s != "" && s == got {
			return true
		}
		m := asMap(w)
		if p := str(m["prefix"]); p != "" && strings.HasPrefix(got, p) {
			return true
		}
		if ab, ok := m["anything-but"]; ok {
			if str(ab) != got {
				return true
			}
			continue
		}
		if num, ok := m["numeric"].([]any); ok && len(num) >= 2 {
			op := str(num[0])
			n, _ := strconv.ParseFloat(fmt.Sprint(num[1]), 64)
			g, err := strconv.ParseFloat(got, 64)
			if err != nil {
				continue
			}
			switch op {
			case ">":
				if g > n {
					return true
				}
			case ">=":
				if g >= n {
					return true
				}
			case "<":
				if g < n {
					return true
				}
			case "=":
				if g == n {
					return true
				}
			}
		}
	}
	return false
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

func asMap(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func topicName(arn string) string {
	if i := strings.LastIndex(arn, ":"); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func str(v any) string { s, _ := v.(string); return s }
