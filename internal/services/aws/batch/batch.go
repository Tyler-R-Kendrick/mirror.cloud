// Package batch stores compute environments, queues, job defs, and jobs (no ECS run).
package batch

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.batch", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Batch-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.batch" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateComputeEnvironment", "DescribeComputeEnvironments", "DeleteComputeEnvironment",
		"CreateJobQueue", "DescribeJobQueues", "DeleteJobQueue",
		"RegisterJobDefinition", "DescribeJobDefinitions", "DeregisterJobDefinition",
		"SubmitJob", "DescribeJobs", "ListJobs", "TerminateJob",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateComputeEnvironment":
		name := first(req.Input, "computeEnvironmentName")
		arn := "arn:aws:batch:" + req.Identity.Region + ":" + req.Identity.Account + ":compute-environment/" + name
		rec := map[string]any{"computeEnvironmentName": name, "computeEnvironmentArn": arn, "state": "ENABLED", "status": "VALID"}
		return putOut(ctx, p.col(req, "bce"), name, rec, map[string]any{"computeEnvironmentName": name, "computeEnvironmentArn": arn})
	case "DescribeComputeEnvironments":
		return listCol(ctx, p.col(req, "bce"), "computeEnvironments")
	case "DeleteComputeEnvironment":
		_ = p.col(req, "bce").Delete(ctx, first(req.Input, "computeEnvironment"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateJobQueue":
		name := first(req.Input, "jobQueueName")
		arn := "arn:aws:batch:" + req.Identity.Region + ":" + req.Identity.Account + ":job-queue/" + name
		rec := map[string]any{"jobQueueName": name, "jobQueueArn": arn, "state": "ENABLED", "status": "VALID"}
		return putOut(ctx, p.col(req, "bjq"), name, rec, map[string]any{"jobQueueName": name, "jobQueueArn": arn})
	case "DescribeJobQueues":
		return listCol(ctx, p.col(req, "bjq"), "jobQueues")
	case "DeleteJobQueue":
		_ = p.col(req, "bjq").Delete(ctx, first(req.Input, "jobQueue"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "RegisterJobDefinition":
		name := first(req.Input, "jobDefinitionName")
		arn := "arn:aws:batch:" + req.Identity.Region + ":" + req.Identity.Account + ":job-definition/" + name + ":1"
		rec := map[string]any{"jobDefinitionName": name, "jobDefinitionArn": arn, "revision": 1}
		return putOut(ctx, p.col(req, "bjd"), name, rec, rec)
	case "DescribeJobDefinitions":
		return listCol(ctx, p.col(req, "bjd"), "jobDefinitions")
	case "DeregisterJobDefinition":
		_ = p.col(req, "bjd").Delete(ctx, first(req.Input, "jobDefinition"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SubmitJob":
		id := p.deps.Rand.Hex(16)
		rec := map[string]any{"jobId": id, "jobName": first(req.Input, "jobName", "JobName"), "jobQueue": first(req.Input, "jobQueue", "JobQueue"), "status": "SUCCEEDED"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "bjob").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"jobId": id, "jobName": rec["jobName"]}}, nil
	case "DescribeJobs":
		return listCol(ctx, p.col(req, "bjob"), "jobs")
	case "ListJobs":
		return listCol(ctx, p.col(req, "bjob"), "jobSummaryList")
	case "TerminateJob":
		_ = p.col(req, "bjob").Delete(ctx, first(req.Input, "jobId"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.batch", req.Operation, "emulate")
	}
}

func putOut(ctx context.Context, c spi.Collection, id string, rec, out map[string]any) (*spi.Response, error) {
	b, _ := json.Marshal(rec)
	_ = c.Put(ctx, id, b)
	return &spi.Response{Output: out}, nil
}

func listCol(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
	kvs, _, _ := c.List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
