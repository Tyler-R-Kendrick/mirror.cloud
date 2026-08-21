package autoscaling

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"AttachInstances", "AttachLoadBalancerTargetGroups", "AttachLoadBalancers", "AttachTrafficSources",
		"BatchDeleteScheduledAction", "BatchPutScheduledUpdateGroupAction", "CancelInstanceRefresh",
		"CompleteLifecycleAction", "DeleteLifecycleHook", "DeleteNotificationConfiguration", "DeletePolicy",
		"DeleteScheduledAction", "DeleteWarmPool", "DescribeAccountLimits", "DescribeAdjustmentTypes",
		"DescribeAutoScalingInstances", "DescribeAutoScalingNotificationTypes", "DescribeInstanceRefreshes",
		"DescribeLifecycleHookTypes", "DescribeLifecycleHooks", "DescribeLoadBalancerTargetGroups",
		"DescribeLoadBalancers", "DescribeMetricCollectionTypes", "DescribeNotificationConfigurations",
		"DescribePolicies", "DescribeScalingActivities", "DescribeScalingProcessTypes", "DescribeScheduledActions",
		"DescribeTerminationPolicyTypes", "DescribeTrafficSources", "DescribeWarmPool",
		"DetachInstances", "DetachLoadBalancerTargetGroups", "DetachLoadBalancers", "DetachTrafficSources",
		"DisableMetricsCollection", "EnableMetricsCollection", "EnterStandby", "ExecutePolicy", "ExitStandby",
		"GetPredictiveScalingForecast", "LaunchInstances", "PutLifecycleHook", "PutNotificationConfiguration",
		"PutScalingPolicy", "PutScheduledUpdateGroupAction", "PutWarmPool", "RecordLifecycleActionHeartbeat",
		"ResumeProcesses", "RollbackInstanceRefresh", "SetInstanceHealth", "SetInstanceProtection",
		"StartInstanceRefresh", "SuspendProcesses",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	g := first(req.Input, "AutoScalingGroupName")
	switch op {
	case "PutScalingPolicy":
		name := first(req.Input, "PolicyName")
		arn := "arn:aws:autoscaling:" + req.Identity.Region + ":" + req.Identity.Account + ":policy:" + g + ":policyName/" + name
		rec := map[string]any{"PolicyName": name, "AutoScalingGroupName": g, "PolicyARN": arn, "PolicyType": first(req.Input, "PolicyType")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asgpol").Put(ctx, g+"/"+name, b)
		return &spi.Response{Output: map[string]any{"PolicyARN": arn}}, nil
	case "DescribePolicies":
		return p.listPref(ctx, req, "asgpol", g+"/", "ScalingPolicies")
	case "DeletePolicy":
		_ = p.col(req, "asgpol").Delete(ctx, g+"/"+first(req.Input, "PolicyName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ExecutePolicy":
		act := map[string]any{"ActivityId": p.deps.Rand.Hex(8), "StatusCode": "Successful", "AutoScalingGroupName": g, "Description": "ExecutePolicy"}
		b, _ := json.Marshal(act)
		_ = p.col(req, "asgact").Put(ctx, str(act["ActivityId"]), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutScheduledUpdateGroupAction", "BatchPutScheduledUpdateGroupAction":
		name := first(req.Input, "ScheduledActionName")
		if name == "" {
			name = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"ScheduledActionName": name, "AutoScalingGroupName": g}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asgsched").Put(ctx, g+"/"+name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeScheduledActions":
		return p.listPref(ctx, req, "asgsched", g+"/", "ScheduledUpdateGroupActions")
	case "DeleteScheduledAction", "BatchDeleteScheduledAction":
		_ = p.col(req, "asgsched").Delete(ctx, g+"/"+first(req.Input, "ScheduledActionName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutLifecycleHook":
		name := first(req.Input, "LifecycleHookName")
		rec := map[string]any{"LifecycleHookName": name, "AutoScalingGroupName": g, "LifecycleTransition": first(req.Input, "LifecycleTransition")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asglife").Put(ctx, g+"/"+name, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeLifecycleHooks":
		return p.listPref(ctx, req, "asglife", g+"/", "LifecycleHooks")
	case "DeleteLifecycleHook":
		_ = p.col(req, "asglife").Delete(ctx, g+"/"+first(req.Input, "LifecycleHookName"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CompleteLifecycleAction", "RecordLifecycleActionHeartbeat":
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutNotificationConfiguration":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "asgnote").Put(ctx, g, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeNotificationConfigurations":
		return p.listPref(ctx, req, "asgnote", "", "NotificationConfigurations")
	case "DeleteNotificationConfiguration":
		_ = p.col(req, "asgnote").Delete(ctx, g)
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutWarmPool":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "asgwarm").Put(ctx, g, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeWarmPool":
		b, ok, _ := p.col(req, "asgwarm").Get(ctx, g)
		rec := map[string]any{"WarmPoolConfiguration": map[string]any{"MinSize": 0}, "Instances": []any{}}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: rec}, nil
	case "DeleteWarmPool":
		_ = p.col(req, "asgwarm").Delete(ctx, g)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartInstanceRefresh":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"InstanceRefreshId": id, "AutoScalingGroupName": g, "Status": "InProgress"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "asgref").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"InstanceRefreshId": id}}, nil
	case "DescribeInstanceRefreshes":
		return p.listPref(ctx, req, "asgref", "", "InstanceRefreshes")
	case "CancelInstanceRefresh", "RollbackInstanceRefresh":
		id := first(req.Input, "InstanceRefreshId")
		st := "Cancelled"
		if op == "RollbackInstanceRefresh" {
			st = "RollbackInProgress"
		}
		if id == "" {
			kvs, _, _ := p.col(req, "asgref").List(ctx, "", "", 0)
			if len(kvs) > 0 {
				id = kvs[0].Key
			}
		}
		b, ok, _ := p.col(req, "asgref").Get(ctx, id)
		rec := map[string]any{"InstanceRefreshId": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["Status"] = st
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "asgref").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"InstanceRefreshId": id}}, nil
	case "AttachInstances", "DetachInstances", "EnterStandby", "ExitStandby", "SetInstanceProtection", "SetInstanceHealth":
		return p.patchASGList(ctx, req, g, "Instances", "InstanceIds")
	case "AttachLoadBalancers", "DetachLoadBalancers":
		return p.patchASGList(ctx, req, g, "LoadBalancerNames", "LoadBalancerNames")
	case "DescribeLoadBalancers":
		return p.fromASG(ctx, req, g, "LoadBalancerNames", "LoadBalancers")
	case "AttachLoadBalancerTargetGroups", "DetachLoadBalancerTargetGroups":
		return p.patchASGList(ctx, req, g, "TargetGroupARNs", "TargetGroupARNs")
	case "DescribeLoadBalancerTargetGroups":
		return p.fromASG(ctx, req, g, "TargetGroupARNs", "LoadBalancerTargetGroups")
	case "AttachTrafficSources", "DetachTrafficSources":
		return p.patchASGList(ctx, req, g, "TrafficSources", "TrafficSources")
	case "DescribeTrafficSources":
		return p.fromASG(ctx, req, g, "TrafficSources", "TrafficSources")
	case "EnableMetricsCollection", "DisableMetricsCollection":
		name := "Enabled"
		if op == "DisableMetricsCollection" {
			name = "Disabled"
		}
		return p.patchASGField(ctx, req, g, "MetricsCollection", name)
	case "SuspendProcesses", "ResumeProcesses":
		st := "Suspended"
		if op == "ResumeProcesses" {
			st = "Resumed"
		}
		return p.patchASGField(ctx, req, g, "ProcessState", st)
	case "LaunchInstances":
		act := map[string]any{"ActivityId": p.deps.Rand.Hex(8), "StatusCode": "Successful", "AutoScalingGroupName": g, "Description": "LaunchInstances"}
		b, _ := json.Marshal(act)
		_ = p.col(req, "asgact").Put(ctx, str(act["ActivityId"]), b)
		return &spi.Response{Output: map[string]any{"Activities": []any{act}}}, nil
	case "DescribeScalingActivities":
		return p.listPref(ctx, req, "asgact", "", "Activities")
	case "DescribeAutoScalingInstances":
		kvs, _, _ := p.col(req, "asg").List(ctx, "", "", 0)
		var inst []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			for _, i := range asAny(rec["Instances"]) {
				inst = append(inst, i)
			}
		}
		return &spi.Response{Output: map[string]any{"AutoScalingInstances": inst}}, nil
	case "DescribeAccountLimits":
		return &spi.Response{Output: map[string]any{"MaxNumberOfAutoScalingGroups": 200, "MaxNumberOfLaunchConfigurations": 200, "NumberOfAutoScalingGroups": 0, "NumberOfLaunchConfigurations": 0}}, nil
	case "DescribeAdjustmentTypes":
		return &spi.Response{Output: map[string]any{"AdjustmentTypes": []any{map[string]any{"AdjustmentType": "ChangeInCapacity"}, map[string]any{"AdjustmentType": "ExactCapacity"}, map[string]any{"AdjustmentType": "PercentChangeInCapacity"}}}}, nil
	case "DescribeAutoScalingNotificationTypes":
		return &spi.Response{Output: map[string]any{"AutoScalingNotificationTypes": []any{"autoscaling:EC2_INSTANCE_LAUNCH", "autoscaling:EC2_INSTANCE_TERMINATE"}}}, nil
	case "DescribeLifecycleHookTypes":
		return &spi.Response{Output: map[string]any{"LifecycleHookTypes": []any{"autoscaling:EC2_INSTANCE_LAUNCHING", "autoscaling:EC2_INSTANCE_TERMINATING"}}}, nil
	case "DescribeMetricCollectionTypes":
		return &spi.Response{Output: map[string]any{"Metrics": []any{map[string]any{"Metric": "GroupMinSize"}}, "Granularities": []any{map[string]any{"Granularity": "1Minute"}}}}, nil
	case "DescribeScalingProcessTypes":
		return &spi.Response{Output: map[string]any{"Processes": []any{map[string]any{"ProcessName": "Launch"}, map[string]any{"ProcessName": "Terminate"}}}}, nil
	case "DescribeTerminationPolicyTypes":
		return &spi.Response{Output: map[string]any{"TerminationPolicyTypes": []any{"OldestInstance", "NewestInstance", "Default"}}}, nil
	case "GetPredictiveScalingForecast":
		// ponytail: no real forecast; empty timestamps.
		return &spi.Response{Output: map[string]any{"LoadForecast": []any{}, "CapacityForecast": []any{}}}, nil
	default:
		return nil, spi.NotImplemented("aws.autoscaling", op, "emulate")
	}
}

func (p *Pack) listPref(ctx context.Context, req *spi.Request, col, prefix, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, prefix, "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func (p *Pack) patchASGList(ctx context.Context, req *spi.Request, g, field, inputKey string) (*spi.Response, error) {
	b, ok, _ := p.col(req, "asg").Get(ctx, g)
	rec := map[string]any{"AutoScalingGroupName": g}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	items := asAny(rec[field])
	add := asAny(req.Input[inputKey])
	if len(add) == 0 {
		if s := first(req.Input, inputKey+".member.1"); s != "" {
			add = []any{s}
		}
	}
	if stringsHasPrefix(req.Operation, "Detach") {
		want := map[string]bool{}
		for _, a := range add {
			want[str(a)] = true
		}
		var keep []any
		for _, i := range items {
			if !want[str(i)] {
				keep = append(keep, i)
			}
		}
		items = keep
	} else {
		items = append(items, add...)
	}
	if req.Operation == "SetInstanceHealth" || req.Operation == "SetInstanceProtection" || req.Operation == "EnterStandby" || req.Operation == "ExitStandby" {
		st := "InService"
		switch req.Operation {
		case "EnterStandby":
			st = "Standby"
		case "SetInstanceHealth":
			st = first(req.Input, "HealthStatus")
			if st == "" {
				st = "Healthy"
			}
		}
		rec["InstanceState"] = st
		if req.Operation == "SetInstanceProtection" {
			rec["ProtectedFromScaleIn"] = true
		}
	}
	rec[field] = items
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "asg").Put(ctx, g, nb)
	return &spi.Response{Output: map[string]any{"Activities": []any{map[string]any{"ActivityId": p.deps.Rand.Hex(8), "StatusCode": "Successful"}}}}, nil
}

func (p *Pack) fromASG(ctx context.Context, req *spi.Request, g, field, outKey string) (*spi.Response, error) {
	b, ok, _ := p.col(req, "asg").Get(ctx, g)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	return &spi.Response{Output: map[string]any{outKey: rec[field]}}, nil
}

func (p *Pack) patchASGField(ctx context.Context, req *spi.Request, g, field string, val any) (*spi.Response, error) {
	b, ok, _ := p.col(req, "asg").Get(ctx, g)
	rec := map[string]any{"AutoScalingGroupName": g}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	rec[field] = val
	nb, _ := json.Marshal(rec)
	_ = p.col(req, "asg").Put(ctx, g, nb)
	return &spi.Response{Output: map[string]any{}}, nil
}

func asAny(v any) []any {
	a, _ := v.([]any)
	return a
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func stringsHasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
