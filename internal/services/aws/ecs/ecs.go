// Package ecs holds the task lifecycle ECS's bundle declares native: the
// operations that start or stop a task, because a task behind a load balancer
// registers with ELB as it runs and deregisters as it stops.
package ecs

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	for _, op := range []string{"CreateService", "RunTask", "StartTask", "StopTask",
		"SubmitTaskStateChange", "SubmitContainerStateChange", "SubmitAttachmentStateChanges"} {
		bundled.RegisterNative("aws.ecs", op, func(ctx context.Context, deps spi.Deps, req *spi.Request) (*spi.Response, error) {
			return tasks{deps}.invoke(ctx, req)
		})
	}
}

// tasks writes services and tasks in the layout the bundle reads them in:
// ecssvc and ecstask records keyed cluster/name and cluster/id.
type tasks struct{ deps spi.Deps }

func (p tasks) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p tasks) invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	acct, region := req.Identity.Account, req.Identity.Region
	cluster := lastSlash(first(req.Input, "cluster"))
	if cluster == "" {
		cluster = "default"
	}
	switch req.Operation {
	case "CreateService":
		name := first(req.Input, "serviceName")
		rec := map[string]any{"serviceName": name, "serviceArn": "arn:aws:ecs:" + region + ":" + acct + ":service/" + cluster + "/" + name,
			"clusterArn": "arn:aws:ecs:" + region + ":" + acct + ":cluster/" + cluster, "desiredCount": req.Input["desiredCount"],
			"taskDefinition": req.Input["taskDefinition"], "loadBalancers": req.Input["loadBalancers"], "status": "ACTIVE"}
		p.put(ctx, req, "ecssvc", cluster+"/"+name, rec)
		for i := 0; i < intValue(req.Input["desiredCount"]); i++ {
			p.createTask(ctx, req, cluster, req.Input["taskDefinition"], "PENDING", name, req.Input["loadBalancers"])
		}
		return &spi.Response{Output: map[string]any{"service": rec}}, nil
	case "RunTask", "StartTask":
		rec := p.createTask(ctx, req, cluster, req.Input["taskDefinition"], "RUNNING", "", nil)
		return &spi.Response{Output: map[string]any{"tasks": []any{rec}, "failures": []any{}}}, nil
	case "StopTask":
		id := lastSlash(first(req.Input, "task"))
		b, ok, _ := p.col(req, "ecstask").Get(ctx, cluster+"/"+id)
		rec := map[string]any{"taskArn": id, "lastStatus": "STOPPED"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			p.syncTargets(ctx, req, rec, false)
			rec["lastStatus"] = "STOPPED"
			rec["desiredStatus"] = "STOPPED"
			p.put(ctx, req, "ecstask", cluster+"/"+id, rec)
		}
		return &spi.Response{Output: map[string]any{"task": rec}}, nil
	default: // Submit*StateChange: the agent reports where a task got to
		id := lastSlash(first(req.Input, "task"))
		if id == "" {
			id = lastSlash(first(req.Input, "containerInstance"))
		}
		key := cluster + "/" + id
		b, ok, _ := p.col(req, "ecstask").Get(ctx, key)
		rec := map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if st := first(req.Input, "status"); st != "" {
			if st == "RUNNING" {
				p.syncTargets(ctx, req, rec, true)
			} else if st == "STOPPED" || st == "FAILED" {
				p.syncTargets(ctx, req, rec, false)
			}
			rec["lastStatus"] = st
		}
		if req.Operation == "SubmitAttachmentStateChanges" {
			rec["attachments"] = req.Input["attachments"]
		}
		p.put(ctx, req, "ecstask", key, rec)
		return &spi.Response{Output: map[string]any{"acknowledgment": "OK"}}, nil
	}
}

func (p tasks) put(ctx context.Context, req *spi.Request, col, key string, rec map[string]any) {
	b, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, key, b)
}

func (p tasks) createTask(ctx context.Context, req *spi.Request, cluster string, taskDefinition any, status, service string, loadBalancers any) map[string]any {
	id := p.deps.Rand.Hex(8)
	arn := "arn:aws:ecs:" + req.Identity.Region + ":" + req.Identity.Account + ":task/" + cluster + "/" + id
	rec := map[string]any{
		"taskArn": arn, "clusterArn": "arn:aws:ecs:" + req.Identity.Region + ":" + req.Identity.Account + ":cluster/" + cluster,
		"taskDefinitionArn": taskDefinition, "lastStatus": status, "desiredStatus": "RUNNING",
		"privateIPv4Address": "10.0.0." + strconv.Itoa(p.next(ctx, req, "taskip")%254+1),
	}
	if service != "" {
		rec["group"] = "service:" + service
		rec["loadBalancers"] = loadBalancers
	}
	p.put(ctx, req, "ecstask", cluster+"/"+id, rec)
	if status == "RUNNING" {
		p.syncTargets(ctx, req, rec, true)
	}
	return rec
}

func (p tasks) syncTargets(ctx context.Context, req *spi.Request, task map[string]any, register bool) {
	operation := "DeregisterTargets"
	if register {
		operation = "RegisterTargets"
	}
	items, _ := task["loadBalancers"].([]any)
	for _, raw := range items {
		lb, _ := raw.(map[string]any)
		arn := first(lb, "targetGroupArn", "TargetGroupArn")
		if arn == "" {
			continue
		}
		target := map[string]any{"Id": first(task, "privateIPv4Address")}
		if port := lb["containerPort"]; port != nil {
			target["Port"] = port
		}
		// ELB owns its targets; ECS registers a task the way a caller would.
		_, _ = bundled.Handler("aws.elasticloadbalancing", p.deps).Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: operation,
			Input: map[string]any{"TargetGroupArn": arn, "Targets": []any{target}}})
	}
}

func intValue(value any) int {
	switch n := value.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}

func (p tasks) next(ctx context.Context, req *spi.Request, key string) int {
	b, ok, _ := p.col(req, "seq").Get(ctx, key)
	n := 1
	if ok {
		n, _ = strconv.Atoi(string(b))
		n++
	}
	_ = p.col(req, "seq").Put(ctx, key, []byte(strconv.Itoa(n)))
	return n
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func lastSlash(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
