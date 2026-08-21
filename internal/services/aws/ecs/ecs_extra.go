package ecs

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"ContinueServiceDeployment", "CreateCapacityProvider", "CreateDaemon", "CreateExpressGatewayService",
		"DeleteCapacityProvider", "DeleteDaemon", "DeleteDaemonTaskDefinition", "DeleteExpressGatewayService",
		"DeleteTaskDefinitions", "DescribeCapacityProviders", "DescribeDaemon", "DescribeDaemonDeployments",
		"DescribeDaemonRevisions", "DescribeDaemonTaskDefinition", "DescribeExpressGatewayService", "DescribeServiceDeployments",
		"DescribeServiceRevisions", "DiscoverPollEndpoint", "ExecuteCommand", "GetTaskProtection",
		"ListDaemonDeployments", "ListDaemonTaskDefinitions", "ListDaemons", "ListServiceDeployments",
		"ListServicesByNamespace", "ListTaskDefinitionFamilies", "RegisterDaemonTaskDefinition", "StopServiceDeployment",
		"SubmitAttachmentStateChanges", "SubmitContainerStateChange", "SubmitTaskStateChange", "UpdateCapacityProvider",
		"UpdateContainerAgent", "UpdateContainerInstancesState", "UpdateDaemon", "UpdateExpressGatewayService",
		"UpdateServicePrimaryTaskSet", "UpdateTaskProtection",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	acct, region := req.Identity.Account, req.Identity.Region
	switch op {
	case "CreateCapacityProvider", "UpdateCapacityProvider":
		name := first(req.Input, "name", "capacityProvider")
		arn := "arn:aws:ecs:" + region + ":" + acct + ":capacity-provider/" + name
		rec := map[string]any{"name": name, "capacityProviderArn": arn, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecscp").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"capacityProvider": rec}}, nil
	case "DescribeCapacityProviders":
		names := asStrings(req.Input["capacityProviders"])
		return p.describeMany(ctx, req, "ecscp", names, "capacityProviders")
	case "DeleteCapacityProvider":
		name := lastSlash(first(req.Input, "capacityProvider"))
		_ = p.col(req, "ecscp").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{"capacityProvider": map[string]any{"name": name, "status": "INACTIVE"}}}, nil
	case "CreateDaemon", "UpdateDaemon":
		name := first(req.Input, "daemonName", "name")
		rec := map[string]any{"daemonName": name, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsdaemon").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"daemon": rec}}, nil
	case "DescribeDaemon":
		return p.describeOne(ctx, req, "ecsdaemon", first(req.Input, "daemonName", "name"), "daemon")
	case "ListDaemons":
		return p.listWrap(ctx, req, "ecsdaemon", "daemons")
	case "DeleteDaemon":
		_ = p.col(req, "ecsdaemon").Delete(ctx, first(req.Input, "daemonName", "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeDaemonDeployments", "ListDaemonDeployments":
		return p.listWrap(ctx, req, "ecsdaemon", "daemonDeployments")
	case "DescribeDaemonRevisions":
		return p.listWrap(ctx, req, "ecsdaemon", "daemonRevisions")
	case "RegisterDaemonTaskDefinition":
		fam := first(req.Input, "family", "daemonName")
		arn := "arn:aws:ecs:" + region + ":" + acct + ":daemon-task-definition/" + fam
		rec := map[string]any{"family": fam, "taskDefinitionArn": arn, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsdaemontd").Put(ctx, fam, b)
		return &spi.Response{Output: map[string]any{"daemonTaskDefinition": rec}}, nil
	case "DescribeDaemonTaskDefinition":
		return p.describeOne(ctx, req, "ecsdaemontd", first(req.Input, "family", "daemonName"), "daemonTaskDefinition")
	case "ListDaemonTaskDefinitions":
		return p.listWrap(ctx, req, "ecsdaemontd", "daemonTaskDefinitions")
	case "DeleteDaemonTaskDefinition":
		_ = p.col(req, "ecsdaemontd").Delete(ctx, first(req.Input, "family", "daemonName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateExpressGatewayService", "UpdateExpressGatewayService":
		name := first(req.Input, "serviceName", "name")
		arn := "arn:aws:ecs:" + region + ":" + acct + ":express-gateway-service/" + name
		rec := map[string]any{"serviceName": name, "serviceArn": arn, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsexgw").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"service": rec}}, nil
	case "DescribeExpressGatewayService":
		return p.describeOne(ctx, req, "ecsexgw", first(req.Input, "serviceName", "name"), "service")
	case "DeleteExpressGatewayService":
		_ = p.col(req, "ecsexgw").Delete(ctx, first(req.Input, "serviceName", "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListServiceDeployments":
		return p.listWrap(ctx, req, "ecsdeploy", "serviceDeployments")
	case "DescribeServiceDeployments":
		ids := asStrings(req.Input["serviceDeploymentArns"])
		if len(ids) == 0 {
			ids = asStrings(req.Input["serviceDeployments"])
		}
		return p.describeMany(ctx, req, "ecsdeploy", ids, "serviceDeployments")
	case "DescribeServiceRevisions":
		return p.listWrap(ctx, req, "ecsdeploy", "serviceRevisions")
	case "ContinueServiceDeployment", "StopServiceDeployment":
		id := first(req.Input, "serviceDeploymentArn", "service")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		st := "IN_PROGRESS"
		if op == "StopServiceDeployment" {
			st = "STOPPED"
		}
		rec := map[string]any{"serviceDeploymentArn": id, "status": st}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsdeploy").Put(ctx, lastSlash(id), b)
		return &spi.Response{Output: map[string]any{"serviceDeployment": rec}}, nil
	case "DiscoverPollEndpoint":
		return &spi.Response{Output: map[string]any{
			"endpoint":          "http://127.0.0.1/ecs-agent",
			"telemetryEndpoint": "http://127.0.0.1/ecs-telemetry",
		}}, nil
	case "ExecuteCommand":
		// ponytail: no SSM session; returns a local session id + URL only.
		id := p.deps.Rand.Hex(8)
		return &spi.Response{Output: map[string]any{"session": map[string]any{
			"sessionId": id, "streamUrl": "http://127.0.0.1/ecs-exec/" + id,
		}}}, nil
	case "GetTaskProtection", "UpdateTaskProtection":
		cluster := lastSlash(first(req.Input, "cluster"))
		on := op == "UpdateTaskProtection"
		if v, ok := req.Input["protectionEnabled"].(bool); ok {
			on = v
		}
		var protected []any
		ids := asStrings(req.Input["tasks"])
		if len(ids) == 0 {
			kvs, _, _ := p.col(req, "ecstask").List(ctx, cluster+"/", "", 0)
			for _, kv := range kvs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				if op == "UpdateTaskProtection" {
					rec["protectionEnabled"] = on
					nb, _ := json.Marshal(rec)
					_ = p.col(req, "ecstask").Put(ctx, kv.Key, nb)
				}
				protected = append(protected, rec)
			}
		} else {
			for _, n := range ids {
				key := cluster + "/" + lastSlash(n)
				b, ok, _ := p.col(req, "ecstask").Get(ctx, key)
				rec := map[string]any{"taskArn": n}
				if ok {
					_ = json.Unmarshal(b, &rec)
				}
				if op == "UpdateTaskProtection" {
					rec["protectionEnabled"] = on
					nb, _ := json.Marshal(rec)
					_ = p.col(req, "ecstask").Put(ctx, key, nb)
				}
				protected = append(protected, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"protectedTasks": protected, "failures": []any{}}}, nil
	case "ListServicesByNamespace":
		kvs, _, _ := p.col(req, "ecssvc").List(ctx, "", "", 0)
		var arns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["serviceArn"])
		}
		return &spi.Response{Output: map[string]any{"serviceArns": arns}}, nil
	case "ListTaskDefinitionFamilies":
		kvs, _, _ := p.col(req, "taskdef").List(ctx, "", "", 0)
		var fams []any
		seen := map[string]bool{}
		for _, kv := range kvs {
			fam := kv.Key
			if i := strings.Index(fam, ":"); i >= 0 {
				fam = fam[:i]
			}
			if seen[fam] {
				continue
			}
			seen[fam] = true
			fams = append(fams, fam)
		}
		return &spi.Response{Output: map[string]any{"families": fams}}, nil
	case "DeleteTaskDefinitions":
		for _, n := range asStrings(req.Input["taskDefinitions"]) {
			_ = p.col(req, "taskdef").Delete(ctx, lastSlash(n))
		}
		return &spi.Response{Output: map[string]any{"taskDefinitions": []any{}, "failures": []any{}}}, nil
	case "SubmitTaskStateChange", "SubmitContainerStateChange", "SubmitAttachmentStateChanges":
		cluster := lastSlash(first(req.Input, "cluster"))
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
		if op == "SubmitAttachmentStateChanges" {
			rec["attachments"] = req.Input["attachments"]
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecstask").Put(ctx, key, nb)
		return &spi.Response{Output: map[string]any{"acknowledgment": "OK"}}, nil
	case "UpdateContainerAgent", "UpdateContainerInstancesState":
		cluster := lastSlash(first(req.Input, "cluster"))
		var out []any
		ids := asStrings(req.Input["containerInstances"])
		if len(ids) == 0 {
			if n := first(req.Input, "containerInstance"); n != "" {
				ids = []string{n}
			}
		}
		st := first(req.Input, "status")
		if st == "" {
			st = "ACTIVE"
		}
		for _, n := range ids {
			key := cluster + "/" + lastSlash(n)
			b, ok, _ := p.col(req, "ecsci").Get(ctx, key)
			rec := map[string]any{"containerInstanceArn": n, "status": st}
			if ok {
				_ = json.Unmarshal(b, &rec)
				rec["status"] = st
			}
			if op == "UpdateContainerAgent" {
				rec["agentUpdateStatus"] = "UPDATED"
			}
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ecsci").Put(ctx, key, nb)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"containerInstances": out, "failures": []any{}}}, nil
	case "UpdateServicePrimaryTaskSet":
		cluster := lastSlash(first(req.Input, "cluster"))
		name := lastSlash(first(req.Input, "service"))
		b, ok, _ := p.col(req, "ecssvc").Get(ctx, cluster+"/"+name)
		rec := map[string]any{"serviceName": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["taskSets"] = req.Input["primaryTaskSet"]
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecssvc").Put(ctx, cluster+"/"+name, nb)
		return &spi.Response{Output: map[string]any{"taskSet": rec}}, nil
	default:
		return nil, spi.NotImplemented("aws.ecs", op, "emulate")
	}
}

func (p *Pack) describeOne(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, lastSlash(id))
	if !ok {
		return &spi.Response{Output: map[string]any{wrap: map[string]any{"name": id}}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) describeMany(ctx context.Context, req *spi.Request, col string, names []string, key string) (*spi.Response, error) {
	var out []any
	if len(names) == 0 {
		kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
	} else {
		for _, n := range names {
			b, ok, _ := p.col(req, col).Get(ctx, lastSlash(n))
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			out = append(out, rec)
		}
	}
	return &spi.Response{Output: map[string]any{key: out, "failures": []any{}}}, nil
}

func (p *Pack) listWrap(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}
