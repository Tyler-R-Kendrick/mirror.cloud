// Package sqs is the emulate-tier SQS pack.
package sqs

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

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

func (p *Pack) ServiceID() string { return "aws.sqs" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{"CreateQueue", "DeleteQueue", "GetQueueUrl", "ListQueues", "GetQueueAttributes",
		"SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage",
		"DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility",
		"ChangeMessageVisibilityBatch", "PurgeQueue", "TagQueue", "UntagQueue", "ListQueueTags"}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	base := advertise(req)
	switch req.Operation {
	case "CreateQueue":
		name := str(req.Input["QueueName"])
		url := fmt.Sprintf("%s/%s/%s", base, req.Identity.Account, name)
		meta, _ := json.Marshal(map[string]any{"url": url, "name": name})
		_ = p.col(req, "queues").Put(ctx, name, meta)
		return &spi.Response{Output: map[string]any{"QueueUrl": url}}, nil
	case "GetQueueUrl":
		name := str(req.Input["QueueName"])
		b, ok, _ := p.col(req, "queues").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "QueueDoesNotExist", HTTPStatus: 400, Fault: "client"}
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
		name := queueName(req)
		body := str(req.Input["MessageBody"])
		id := p.deps.Rand.Hex(16)
		rh := p.deps.Rand.Hex(64)
		sum := md5.Sum([]byte(body))
		md5hex := hex.EncodeToString(sum[:])
		msg, _ := json.Marshal(map[string]any{"id": id, "body": body, "handle": rh, "md5": md5hex})
		_ = p.col(req, "msgs:"+name).Put(ctx, rh, msg)
		return &spi.Response{Output: map[string]any{"MessageId": id, "MD5OfMessageBody": md5hex}}, nil
	case "ReceiveMessage":
		name := queueName(req)
		kvs, _, _ := p.col(req, "msgs:"+name).List(ctx, "", "", 1)
		var msgs []any
		for _, kv := range kvs {
			var m map[string]any
			_ = json.Unmarshal(kv.Value, &m)
			msgs = append(msgs, map[string]any{"MessageId": m["id"], "ReceiptHandle": m["handle"], "Body": m["body"]})
		}
		return &spi.Response{Output: map[string]any{"Messages": msgs}}, nil
	case "DeleteMessage":
		name := queueName(req)
		_ = p.col(req, "msgs:"+name).Delete(ctx, str(req.Input["ReceiptHandle"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetQueueAttributes":
		return &spi.Response{Output: map[string]any{"Attributes": map[string]any{
			"ApproximateNumberOfMessages": "0", "VisibilityTimeout": "30",
		}}}, nil
	case "SetQueueAttributes", "PurgeQueue", "TagQueue", "UntagQueue",
		"ListQueueTags", "SendMessageBatch", "DeleteMessageBatch",
		"ChangeMessageVisibility", "ChangeMessageVisibilityBatch":
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.sqs", req.Operation, "emulate")
	}
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
