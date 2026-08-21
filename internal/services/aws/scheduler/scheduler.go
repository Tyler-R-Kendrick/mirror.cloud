// Package scheduler stores EventBridge Scheduler groups and schedules (no EventBridge invoke).
package scheduler

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.scheduler", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements Scheduler-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.scheduler" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateSchedule", "GetSchedule", "ListSchedules", "UpdateSchedule", "DeleteSchedule",
		"CreateScheduleGroup", "GetScheduleGroup", "ListScheduleGroups", "DeleteScheduleGroup",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name", "name")
	group := first(req.Input, "GroupName", "groupName")
	if group == "" {
		group = "default"
	}
	switch req.Operation {
	case "CreateSchedule":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:scheduler:" + req.Identity.Region + ":" + req.Identity.Account + ":schedule/" + group + "/" + name
		rec := map[string]any{
			"Name": name, "GroupName": group, "Arn": arn, "State": "ENABLED",
			"ScheduleExpression": first(req.Input, "ScheduleExpression", "scheduleExpression"),
			"Target":             req.Input["Target"],
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sch:"+group).Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ScheduleArn": arn}}, nil
	case "GetSchedule":
		b, ok, _ := p.col(req, "sch:"+group).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListSchedules":
		return listCol(ctx, p.col(req, "sch:"+group), "Schedules")
	case "UpdateSchedule":
		b, ok, _ := p.col(req, "sch:"+group).Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if e := first(req.Input, "ScheduleExpression", "scheduleExpression"); e != "" {
			rec["ScheduleExpression"] = e
		}
		if t, ok := req.Input["Target"]; ok {
			rec["Target"] = t
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "sch:"+group).Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"ScheduleArn": rec["Arn"]}}, nil
	case "DeleteSchedule":
		_ = p.col(req, "sch:"+group).Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateScheduleGroup":
		if name == "" {
			return nil, &spi.Fault{Code: "ValidationException", HTTPStatus: 400, Fault: "client"}
		}
		arn := "arn:aws:scheduler:" + req.Identity.Region + ":" + req.Identity.Account + ":schedule-group/" + name
		rec := map[string]any{"Name": name, "Arn": arn, "State": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "schg").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"ScheduleGroupArn": arn}}, nil
	case "GetScheduleGroup":
		b, ok, _ := p.col(req, "schg").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "ListScheduleGroups":
		return listCol(ctx, p.col(req, "schg"), "ScheduleGroups")
	case "DeleteScheduleGroup":
		_ = p.col(req, "schg").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return nil, spi.NotImplemented("aws.scheduler", req.Operation, "emulate")
	}
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
