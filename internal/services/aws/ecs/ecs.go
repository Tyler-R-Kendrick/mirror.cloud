// Package ecs stores clusters, task definitions, services, and tasks (no containers).
package ecs

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ecs", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ECS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ecs" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateCluster", "DescribeClusters", "ListClusters", "DeleteCluster",
		"UpdateCluster", "UpdateClusterSettings", "PutClusterCapacityProviders",
		"RegisterTaskDefinition", "DescribeTaskDefinition", "ListTaskDefinitions", "DeregisterTaskDefinition",
		"CreateService", "DescribeServices", "ListServices", "UpdateService", "DeleteService",
		"RunTask", "StartTask", "StopTask", "DescribeTasks", "ListTasks",
		"TagResource", "UntagResource", "ListTagsForResource",
		"PutAccountSetting", "PutAccountSettingDefault", "ListAccountSettings", "DeleteAccountSetting",
		"CreateTaskSet", "DescribeTaskSets", "UpdateTaskSet", "DeleteTaskSet",
		"PutAttributes", "ListAttributes", "DeleteAttributes",
		"RegisterContainerInstance", "DescribeContainerInstances", "ListContainerInstances", "DeregisterContainerInstance",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	acct, region := req.Identity.Account, req.Identity.Region
	switch req.Operation {
	case "CreateCluster":
		name := first(req.Input, "clusterName", "cluster")
		if name == "" {
			name = "default"
		}
		arn := "arn:aws:ecs:" + region + ":" + acct + ":cluster/" + name
		rec := map[string]any{"clusterName": name, "clusterArn": arn, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecscluster").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"cluster": rec}}, nil
	case "DescribeClusters":
		names := asStrings(req.Input["clusters"])
		var out []any
		if len(names) == 0 {
			kvs, _, _ := p.col(req, "ecscluster").List(ctx, "", "", 0)
			for _, kv := range kvs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				out = append(out, rec)
			}
		} else {
			for _, n := range names {
				n = lastSlash(n)
				b, ok, _ := p.col(req, "ecscluster").Get(ctx, n)
				if !ok {
					continue
				}
				var rec map[string]any
				_ = json.Unmarshal(b, &rec)
				out = append(out, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"clusters": out, "failures": []any{}}}, nil
	case "ListClusters":
		kvs, _, _ := p.col(req, "ecscluster").List(ctx, "", "", 0)
		var arns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["clusterArn"])
		}
		return &spi.Response{Output: map[string]any{"clusterArns": arns}}, nil
	case "DeleteCluster":
		name := lastSlash(first(req.Input, "cluster"))
		_ = p.col(req, "ecscluster").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{"cluster": map[string]any{"clusterName": name, "status": "INACTIVE"}}}, nil
	case "RegisterTaskDefinition":
		fam := first(req.Input, "family")
		rev := p.next(ctx, req, "tdrev:"+fam)
		arn := "arn:aws:ecs:" + region + ":" + acct + ":task-definition/" + fam + ":" + strconv.Itoa(rev)
		rec := map[string]any{"family": fam, "revision": rev, "taskDefinitionArn": arn, "containerDefinitions": req.Input["containerDefinitions"], "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "taskdef").Put(ctx, fam+":"+strconv.Itoa(rev), b)
		_ = p.col(req, "taskdef").Put(ctx, fam, b)
		return &spi.Response{Output: map[string]any{"taskDefinition": rec}}, nil
	case "DescribeTaskDefinition":
		td := lastSlash(first(req.Input, "taskDefinition"))
		b, ok, _ := p.col(req, "taskdef").Get(ctx, td)
		if !ok {
			return nil, &spi.Fault{Code: "ClientException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"taskDefinition": rec}}, nil
	case "ListTaskDefinitions":
		kvs, _, _ := p.col(req, "taskdef").List(ctx, "", "", 0)
		var arns []any
		for _, kv := range kvs {
			if !strings.Contains(kv.Key, ":") {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["taskDefinitionArn"])
		}
		return &spi.Response{Output: map[string]any{"taskDefinitionArns": arns}}, nil
	case "DeregisterTaskDefinition":
		td := lastSlash(first(req.Input, "taskDefinition"))
		_ = p.col(req, "taskdef").Delete(ctx, td)
		return &spi.Response{Output: map[string]any{"taskDefinition": map[string]any{"taskDefinitionArn": td, "status": "INACTIVE"}}}, nil
	case "CreateService", "UpdateService":
		name := first(req.Input, "serviceName", "service")
		cluster := lastSlash(first(req.Input, "cluster"))
		if cluster == "" {
			cluster = "default"
		}
		arn := "arn:aws:ecs:" + region + ":" + acct + ":service/" + cluster + "/" + name
		rec := map[string]any{"serviceName": name, "serviceArn": arn, "clusterArn": "arn:aws:ecs:" + region + ":" + acct + ":cluster/" + cluster, "desiredCount": req.Input["desiredCount"], "taskDefinition": req.Input["taskDefinition"], "loadBalancers": req.Input["loadBalancers"], "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecssvc").Put(ctx, cluster+"/"+name, b)
		if req.Operation == "CreateService" {
			for i := 0; i < intValue(req.Input["desiredCount"]); i++ {
				p.createTask(ctx, req, cluster, req.Input["taskDefinition"], "PENDING", name, req.Input["loadBalancers"])
			}
		}
		return &spi.Response{Output: map[string]any{"service": rec}}, nil
	case "DescribeServices":
		cluster := lastSlash(first(req.Input, "cluster"))
		var out []any
		for _, n := range asStrings(req.Input["services"]) {
			b, ok, _ := p.col(req, "ecssvc").Get(ctx, cluster+"/"+lastSlash(n))
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"services": out, "failures": []any{}}}, nil
	case "ListServices":
		cluster := lastSlash(first(req.Input, "cluster"))
		kvs, _, _ := p.col(req, "ecssvc").List(ctx, cluster+"/", "", 0)
		var arns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["serviceArn"])
		}
		return &spi.Response{Output: map[string]any{"serviceArns": arns}}, nil
	case "DeleteService":
		cluster := lastSlash(first(req.Input, "cluster"))
		name := lastSlash(first(req.Input, "service"))
		_ = p.col(req, "ecssvc").Delete(ctx, cluster+"/"+name)
		return &spi.Response{Output: map[string]any{"service": map[string]any{"serviceName": name, "status": "INACTIVE"}}}, nil
	case "RunTask":
		cluster := lastSlash(first(req.Input, "cluster"))
		if cluster == "" {
			cluster = "default"
		}
		rec := p.createTask(ctx, req, cluster, req.Input["taskDefinition"], "RUNNING", "", nil)
		return &spi.Response{Output: map[string]any{"tasks": []any{rec}, "failures": []any{}}}, nil
	case "StopTask":
		cluster := lastSlash(first(req.Input, "cluster"))
		id := lastSlash(first(req.Input, "task"))
		b, ok, _ := p.col(req, "ecstask").Get(ctx, cluster+"/"+id)
		rec := map[string]any{"taskArn": id, "lastStatus": "STOPPED"}
		if ok {
			_ = json.Unmarshal(b, &rec)
			p.syncTargets(ctx, req, rec, false)
			rec["lastStatus"] = "STOPPED"
			rec["desiredStatus"] = "STOPPED"
			nb, _ := json.Marshal(rec)
			_ = p.col(req, "ecstask").Put(ctx, cluster+"/"+id, nb)
		}
		return &spi.Response{Output: map[string]any{"task": rec}}, nil
	case "DescribeTasks":
		cluster := lastSlash(first(req.Input, "cluster"))
		var out []any
		for _, n := range asStrings(req.Input["tasks"]) {
			b, ok, _ := p.col(req, "ecstask").Get(ctx, cluster+"/"+lastSlash(n))
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"tasks": out, "failures": []any{}}}, nil
	case "ListTasks":
		cluster := lastSlash(first(req.Input, "cluster"))
		kvs, _, _ := p.col(req, "ecstask").List(ctx, cluster+"/", "", 0)
		var arns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["taskArn"])
		}
		return &spi.Response{Output: map[string]any{"taskArns": arns}}, nil
	case "StartTask":
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "RunTask", Input: req.Input, HTTP: req.HTTP})
	case "UpdateCluster", "UpdateClusterSettings", "PutClusterCapacityProviders":
		name := lastSlash(first(req.Input, "cluster"))
		b, ok, _ := p.col(req, "ecscluster").Get(ctx, name)
		rec := map[string]any{"clusterName": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if v := req.Input["capacityProviders"]; v != nil {
			rec["capacityProviders"] = v
		}
		if v := req.Input["settings"]; v != nil {
			rec["settings"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecscluster").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"cluster": rec}}, nil
	case "TagResource":
		arn := first(req.Input, "resourceArn")
		b, _ := json.Marshal(req.Input["tags"])
		_ = p.col(req, "ecstags").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req, "ecstags").Delete(ctx, first(req.Input, "resourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		arn := first(req.Input, "resourceArn")
		b, ok, _ := p.col(req, "ecstags").Get(ctx, arn)
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	case "PutAccountSetting", "PutAccountSettingDefault":
		n := first(req.Input, "name")
		rec := map[string]any{"name": n, "value": first(req.Input, "value")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsacct").Put(ctx, n, b)
		return &spi.Response{Output: map[string]any{"setting": rec}}, nil
	case "ListAccountSettings":
		kvs, _, _ := p.col(req, "ecsacct").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"settings": items}}, nil
	case "DeleteAccountSetting":
		_ = p.col(req, "ecsacct").Delete(ctx, first(req.Input, "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTaskSet":
		id := p.deps.Rand.Hex(8)
		cluster, svc := lastSlash(first(req.Input, "cluster")), lastSlash(first(req.Input, "service"))
		arn := "arn:aws:ecs:" + region + ":" + acct + ":task-set/" + cluster + "/" + svc + "/" + id
		rec := map[string]any{"id": id, "taskSetArn": arn, "clusterArn": "arn:aws:ecs:" + region + ":" + acct + ":cluster/" + cluster, "serviceArn": first(req.Input, "service"), "taskDefinition": req.Input["taskDefinition"], "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsts").Put(ctx, cluster+"/"+svc+"/"+id, b)
		return &spi.Response{Output: map[string]any{"taskSet": rec}}, nil
	case "DescribeTaskSets":
		cluster, svc := lastSlash(first(req.Input, "cluster")), lastSlash(first(req.Input, "service"))
		var out []any
		ids := asStrings(req.Input["taskSets"])
		if len(ids) == 0 {
			kvs, _, _ := p.col(req, "ecsts").List(ctx, cluster+"/"+svc+"/", "", 0)
			for _, kv := range kvs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				out = append(out, rec)
			}
		} else {
			for _, id := range ids {
				b, ok, _ := p.col(req, "ecsts").Get(ctx, cluster+"/"+svc+"/"+lastSlash(id))
				if !ok {
					continue
				}
				var rec map[string]any
				_ = json.Unmarshal(b, &rec)
				out = append(out, rec)
			}
		}
		return &spi.Response{Output: map[string]any{"taskSets": out, "failures": []any{}}}, nil
	case "UpdateTaskSet":
		cluster, svc, id := lastSlash(first(req.Input, "cluster")), lastSlash(first(req.Input, "service")), lastSlash(first(req.Input, "taskSet"))
		b, ok, _ := p.col(req, "ecsts").Get(ctx, cluster+"/"+svc+"/"+id)
		rec := map[string]any{"id": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if v := req.Input["scale"]; v != nil {
			rec["scale"] = v
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecsts").Put(ctx, cluster+"/"+svc+"/"+id, nb)
		return &spi.Response{Output: map[string]any{"taskSet": rec}}, nil
	case "DeleteTaskSet":
		cluster, svc, id := lastSlash(first(req.Input, "cluster")), lastSlash(first(req.Input, "service")), lastSlash(first(req.Input, "taskSet"))
		_ = p.col(req, "ecsts").Delete(ctx, cluster+"/"+svc+"/"+id)
		return &spi.Response{Output: map[string]any{"taskSet": map[string]any{"id": id, "status": "DRAINING"}}}, nil
	case "PutAttributes":
		cluster := lastSlash(first(req.Input, "cluster"))
		b, _ := json.Marshal(req.Input["attributes"])
		_ = p.col(req, "ecsattr").Put(ctx, cluster, b)
		return &spi.Response{Output: map[string]any{"attributes": req.Input["attributes"]}}, nil
	case "ListAttributes":
		cluster := lastSlash(first(req.Input, "cluster"))
		b, ok, _ := p.col(req, "ecsattr").Get(ctx, cluster)
		var attrs any = []any{}
		if ok {
			_ = json.Unmarshal(b, &attrs)
		}
		return &spi.Response{Output: map[string]any{"attributes": attrs}}, nil
	case "DeleteAttributes":
		_ = p.col(req, "ecsattr").Delete(ctx, lastSlash(first(req.Input, "cluster")))
		return &spi.Response{Output: map[string]any{"attributes": []any{}}}, nil
	case "RegisterContainerInstance":
		cluster := lastSlash(first(req.Input, "cluster"))
		if cluster == "" {
			cluster = "default"
		}
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:ecs:" + region + ":" + acct + ":container-instance/" + cluster + "/" + id
		rec := map[string]any{"containerInstanceArn": arn, "ec2InstanceId": "i-" + id, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecsci").Put(ctx, cluster+"/"+id, b)
		return &spi.Response{Output: map[string]any{"containerInstance": rec}}, nil
	case "DescribeContainerInstances":
		cluster := lastSlash(first(req.Input, "cluster"))
		var out []any
		for _, n := range asStrings(req.Input["containerInstances"]) {
			b, ok, _ := p.col(req, "ecsci").Get(ctx, cluster+"/"+lastSlash(n))
			if !ok {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"containerInstances": out, "failures": []any{}}}, nil
	case "ListContainerInstances":
		cluster := lastSlash(first(req.Input, "cluster"))
		kvs, _, _ := p.col(req, "ecsci").List(ctx, cluster+"/", "", 0)
		var arns []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			arns = append(arns, rec["containerInstanceArn"])
		}
		return &spi.Response{Output: map[string]any{"containerInstanceArns": arns}}, nil
	case "DeregisterContainerInstance":
		cluster := lastSlash(first(req.Input, "cluster"))
		id := lastSlash(first(req.Input, "containerInstance"))
		_ = p.col(req, "ecsci").Delete(ctx, cluster+"/"+id)
		return &spi.Response{Output: map[string]any{"containerInstance": map[string]any{"containerInstanceArn": id, "status": "INACTIVE"}}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) createTask(ctx context.Context, req *spi.Request, cluster string, taskDefinition any, status, service string, loadBalancers any) map[string]any {
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
	b, _ := json.Marshal(rec)
	_ = p.col(req, "ecstask").Put(ctx, cluster+"/"+id, b)
	if status == "RUNNING" {
		p.syncTargets(ctx, req, rec, true)
	}
	return rec
}

func (p *Pack) syncTargets(ctx context.Context, req *spi.Request, task map[string]any, register bool) {
	for _, raw := range asAnySlice(task["loadBalancers"]) {
		lb, _ := raw.(map[string]any)
		arn := first(lb, "targetGroupArn", "TargetGroupArn")
		if arn == "" {
			continue
		}
		col := p.col(req, "targets")
		stored, _, _ := col.Get(ctx, arn)
		var targets []any
		_ = json.Unmarshal(stored, &targets)
		id := first(task, "privateIPv4Address")
		filtered := targets[:0]
		for _, rawTarget := range targets {
			target, _ := rawTarget.(map[string]any)
			if first(target, "Id") != id {
				filtered = append(filtered, rawTarget)
			}
		}
		if register {
			target := map[string]any{"Id": id}
			if port := lb["containerPort"]; port != nil {
				target["Port"] = port
			}
			filtered = append(filtered, target)
		}
		updated, _ := json.Marshal(filtered)
		_ = col.Put(ctx, arn, updated)
	}
}

func asAnySlice(value any) []any {
	items, _ := value.([]any)
	return items
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

func (p *Pack) next(ctx context.Context, req *spi.Request, key string) int {
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

func asStrings(v any) []string {
	switch t := v.(type) {
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	case string:
		if t != "" {
			return []string{t}
		}
	}
	return nil
}

func lastSlash(s string) string {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
