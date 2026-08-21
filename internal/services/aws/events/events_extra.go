package events

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"ActivateEventSource", "CancelReplay", "CreateApiDestination", "CreateArchive", "CreateConnection",
		"CreateEndpoint", "CreateEventBus", "CreatePartnerEventSource", "DeactivateEventSource",
		"DeauthorizeConnection", "DeleteApiDestination", "DeleteArchive", "DeleteConnection", "DeleteEndpoint",
		"DeleteEventBus", "DeletePartnerEventSource", "DescribeApiDestination", "DescribeArchive",
		"DescribeConnection", "DescribeEndpoint", "DescribeEventBus", "DescribeEventSource",
		"DescribePartnerEventSource", "DescribeReplay", "DescribeRule", "DisableRule", "EnableRule",
		"ListApiDestinations", "ListArchives", "ListConnections", "ListEndpoints", "ListEventBuses",
		"ListEventSources", "ListPartnerEventSourceAccounts", "ListPartnerEventSources", "ListReplays",
		"ListRuleNamesByTarget", "ListTagsForResource", "PutPartnerEvents", "PutPermission", "RemovePermission",
		"StartReplay", "TagResource", "TestEventPattern", "UntagResource", "UpdateApiDestination",
		"UpdateArchive", "UpdateConnection", "UpdateEndpoint", "UpdateEventBus",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
	case "CreateEventBus", "UpdateEventBus":
		return p.putARN(ctx, req, "buses", first(req.Input, "Name"), "event-bus", "EventBusArn")
	case "DescribeEventBus":
		name := first(req.Input, "Name")
		if name == "" {
			name = "default"
		}
		return p.getRec(ctx, req, "buses", name)
	case "ListEventBuses":
		return p.listRec(ctx, req, "buses", "EventBuses")
	case "DeleteEventBus":
		_ = p.col(req, "buses").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateArchive", "UpdateArchive":
		return p.putARN(ctx, req, "archives", first(req.Input, "ArchiveName", "Name"), "archive", "ArchiveArn")
	case "DescribeArchive":
		return p.getRec(ctx, req, "archives", first(req.Input, "ArchiveName", "Name"))
	case "ListArchives":
		return p.listRec(ctx, req, "archives", "Archives")
	case "DeleteArchive":
		_ = p.col(req, "archives").Delete(ctx, first(req.Input, "ArchiveName", "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateConnection", "UpdateConnection":
		name := first(req.Input, "Name")
		out, err := p.putARN(ctx, req, "connections", name, "connection", "ConnectionArn")
		if err != nil {
			return nil, err
		}
		out.Output["ConnectionState"] = "AUTHORIZED"
		out.Output["Name"] = name
		return out, nil
	case "DescribeConnection":
		return p.getRec(ctx, req, "connections", first(req.Input, "Name"))
	case "ListConnections":
		return p.listRec(ctx, req, "connections", "Connections")
	case "DeleteConnection", "DeauthorizeConnection":
		name := first(req.Input, "Name")
		if op == "DeauthorizeConnection" {
			return p.patch(ctx, req, "connections", name, map[string]any{"ConnectionState": "DEAUTHORIZED"})
		}
		_ = p.col(req, "connections").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateApiDestination", "UpdateApiDestination":
		return p.putARN(ctx, req, "apidest", first(req.Input, "Name"), "api-destination", "ApiDestinationArn")
	case "DescribeApiDestination":
		return p.getRec(ctx, req, "apidest", first(req.Input, "Name"))
	case "ListApiDestinations":
		return p.listRec(ctx, req, "apidest", "ApiDestinations")
	case "DeleteApiDestination":
		_ = p.col(req, "apidest").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEndpoint", "UpdateEndpoint":
		name := first(req.Input, "Name")
		out, err := p.putARN(ctx, req, "endpoints", name, "endpoint", "Arn")
		if err != nil {
			return nil, err
		}
		out.Output["State"] = "ACTIVE"
		out.Output["Name"] = name
		return out, nil
	case "DescribeEndpoint":
		return p.getRec(ctx, req, "endpoints", first(req.Input, "Name"))
	case "ListEndpoints":
		return p.listRec(ctx, req, "endpoints", "Endpoints")
	case "DeleteEndpoint":
		_ = p.col(req, "endpoints").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePartnerEventSource":
		return p.putARN(ctx, req, "partners", first(req.Input, "Name"), "event-source", "EventSourceArn")
	case "DescribePartnerEventSource", "DescribeEventSource":
		col := "partners"
		if op == "DescribeEventSource" {
			col = "sources"
		}
		id := first(req.Input, "Name")
		if rec, ok := p.load(ctx, req, col, id); ok {
			return &spi.Response{Output: rec}, nil
		}
		if rec, ok := p.load(ctx, req, "partners", id); ok {
			return &spi.Response{Output: rec}, nil
		}
		return &spi.Response{Output: map[string]any{"Name": id}}, nil
	case "ListPartnerEventSources":
		return p.listRec(ctx, req, "partners", "PartnerEventSources")
	case "ListPartnerEventSourceAccounts":
		return &spi.Response{Output: map[string]any{"PartnerEventSourceAccounts": []any{}}}, nil
	case "DeletePartnerEventSource":
		_ = p.col(req, "partners").Delete(ctx, first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ActivateEventSource", "DeactivateEventSource":
		st := "ACTIVE"
		if op == "DeactivateEventSource" {
			st = "INACTIVE"
		}
		name := first(req.Input, "Name")
		rec := map[string]any{"Name": name, "State": st}
		if prev, ok := p.load(ctx, req, "sources", name); ok {
			for k, v := range prev {
				rec[k] = v
			}
			rec["State"] = st
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sources").Put(ctx, name, b)
		return &spi.Response{Output: rec}, nil
	case "ListEventSources":
		return p.listRec(ctx, req, "sources", "EventSources")
	case "StartReplay":
		name := first(req.Input, "ReplayName", "Name")
		out, err := p.putARN(ctx, req, "replays", name, "replay", "ReplayArn")
		if err != nil {
			return nil, err
		}
		_, _ = p.patch(ctx, req, "replays", name, map[string]any{"State": "RUNNING"})
		out.Output["State"] = "RUNNING"
		return out, nil
	case "CancelReplay":
		return p.patch(ctx, req, "replays", first(req.Input, "ReplayName", "Name"), map[string]any{"State": "CANCELLED"})
	case "DescribeReplay":
		return p.getRec(ctx, req, "replays", first(req.Input, "ReplayName", "Name"))
	case "ListReplays":
		return p.listRec(ctx, req, "replays", "Replays")
	case "DescribeRule":
		return p.getRec(ctx, req, "rules", first(req.Input, "Name"))
	case "EnableRule", "DisableRule":
		st := "ENABLED"
		if op == "DisableRule" {
			st = "DISABLED"
		}
		return p.patch(ctx, req, "rules", first(req.Input, "Name"), map[string]any{"State": st})
	case "ListRuleNamesByTarget":
		want := first(req.Input, "TargetArn")
		kvs, _, _ := p.col(req, "targets").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			var tgs []any
			_ = json.Unmarshal(kv.Value, &tgs)
			for _, t := range tgs {
				m, _ := t.(map[string]any)
				if str(m["Arn"]) == want || want == "" {
					names = append(names, kv.Key)
					break
				}
			}
		}
		return &spi.Response{Output: map[string]any{"RuleNames": names}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req, "evtags").Put(ctx, first(req.Input, "ResourceARN", "ResourceArn"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req, "evtags").Delete(ctx, first(req.Input, "ResourceARN", "ResourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "evtags").Get(ctx, first(req.Input, "ResourceARN", "ResourceArn"))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "PutPermission":
		bus := first(req.Input, "EventBusName", "Name")
		if bus == "" {
			bus = "default"
		}
		rec, _ := p.load(ctx, req, "buses", bus)
		if rec == nil {
			rec = map[string]any{"Name": bus}
		}
		stmts, _ := rec["Statements"].([]any)
		if stmts == nil {
			stmts = []any{}
		}
		stmts = append(stmts, map[string]any{
			"Sid": first(req.Input, "StatementId"), "Principal": req.Input["Principal"], "Action": req.Input["Action"],
		})
		rec["Statements"] = stmts
		b, _ := json.Marshal(rec)
		_ = p.col(req, "buses").Put(ctx, bus, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "RemovePermission":
		bus := first(req.Input, "EventBusName", "Name")
		if bus == "" {
			bus = "default"
		}
		sid := first(req.Input, "StatementId")
		rec, ok := p.load(ctx, req, "buses", bus)
		if ok {
			var keep []any
			for _, s := range asSlice(rec["Statements"]) {
				m, _ := s.(map[string]any)
				if str(m["Sid"]) != sid {
					keep = append(keep, s)
				}
			}
			rec["Statements"] = keep
			b, _ := json.Marshal(rec)
			_ = p.col(req, "buses").Put(ctx, bus, b)
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "TestEventPattern":
		ok := matchEventPattern(str(req.Input["EventPattern"]), req.Input["Event"])
		return &spi.Response{Output: map[string]any{"Result": ok}}, nil
	case "PutPartnerEvents":
		entries, _ := req.Input["Entries"].([]any)
		return p.putEvents(ctx, req, entries), nil
	default:
		return nil, spi.NotImplemented("aws.events", op, "emulate")
	}
}

func (p *Pack) putARN(ctx context.Context, req *spi.Request, col, name, kind, arnKey string) (*spi.Response, error) {
	if name == "" {
		name = p.deps.Rand.Hex(8)
	}
	arn := "arn:aws:events:" + req.Identity.Region + ":" + req.Identity.Account + ":" + kind + "/" + name
	rec := map[string]any{}
	for k, v := range req.Input {
		rec[k] = v
	}
	rec["Name"] = name
	rec[arnKey] = arn
	b, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, name, b)
	return &spi.Response{Output: map[string]any{arnKey: arn, "Name": name}}, nil
}

func (p *Pack) getRec(ctx context.Context, req *spi.Request, col, id string) (*spi.Response, error) {
	if rec, ok := p.load(ctx, req, col, id); ok {
		return &spi.Response{Output: rec}, nil
	}
	return &spi.Response{Output: map[string]any{"Name": id}}, nil
}

func (p *Pack) listRec(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func (p *Pack) patch(ctx context.Context, req *spi.Request, col, id string, fields map[string]any) (*spi.Response, error) {
	rec, _ := p.load(ctx, req, col, id)
	if rec == nil {
		rec = map[string]any{"Name": id}
	}
	for k, v := range fields {
		rec[k] = v
	}
	b, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, id, b)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) load(ctx context.Context, req *spi.Request, col, id string) (map[string]any, bool) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	if !ok {
		return nil, false
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return rec, true
}

func matchEventPattern(pattern string, event any) bool {
	var p map[string]any
	if pattern == "" {
		return true
	}
	if json.Unmarshal([]byte(pattern), &p) != nil {
		return false
	}
	var e map[string]any
	switch t := event.(type) {
	case string:
		if json.Unmarshal([]byte(t), &e) != nil {
			return false
		}
	case map[string]any:
		e = t
	default:
		b, _ := json.Marshal(event)
		_ = json.Unmarshal(b, &e)
	}
	return matchFields(p, e)
}

func matchFields(pattern, event map[string]any) bool {
	for key, want := range pattern {
		if key == "$or" {
			matched := false
			for _, alternative := range asSlice(want) {
				if fields, ok := alternative.(map[string]any); ok && matchFields(fields, event) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
			continue
		}
		got, exists := event[key]
		if nested, ok := want.(map[string]any); ok {
			if !matchNested(nested, got) {
				return false
			}
			continue
		}
		clauses, ok := want.([]any)
		if !ok || !matchClauses(clauses, got, exists) {
			return false
		}
	}
	return true
}

func matchNested(pattern map[string]any, value any) bool {
	if event, ok := value.(map[string]any); ok {
		return matchFields(pattern, event)
	}
	for _, item := range asSlice(value) {
		if event, ok := item.(map[string]any); ok && matchFields(pattern, event) {
			return true
		}
	}
	return false
}

func matchClauses(clauses []any, value any, exists bool) bool {
	for _, clause := range clauses {
		if values, ok := value.([]any); ok {
			for _, item := range values {
				if matchClause(clause, item, true) {
					return true
				}
			}
			continue
		}
		if matchClause(clause, value, exists) {
			return true
		}
	}
	return false
}

func matchClause(clause, value any, exists bool) bool {
	op, ok := clause.(map[string]any)
	if !ok {
		return exists && reflect.DeepEqual(clause, value)
	}
	if want, ok := op["exists"].(bool); ok {
		return want == exists
	}
	if !exists {
		return false
	}
	got := str(value)
	if prefix, ok := op["prefix"].(string); ok {
		return strings.HasPrefix(got, prefix)
	}
	if cidr, ok := op["cidr"].(string); ok {
		ip, block, err := net.ParseCIDR(cidr)
		return err == nil && ip != nil && block.Contains(net.ParseIP(got))
	}
	if reject, ok := op["anything-but"]; ok {
		values := asSlice(reject)
		if values == nil {
			values = []any{reject}
		}
		for _, unwanted := range values {
			if matchClause(unwanted, value, true) {
				return false
			}
		}
		return true
	}
	if numeric, ok := op["numeric"].([]any); ok {
		return matchNumeric(numeric, value)
	}
	return false
}

func matchNumeric(conditions []any, value any) bool {
	got, err := strconv.ParseFloat(strings.TrimSpace(toString(value)), 64)
	if err != nil || len(conditions) < 2 || len(conditions)%2 != 0 {
		return false
	}
	for i := 0; i < len(conditions); i += 2 {
		want, err := strconv.ParseFloat(toString(conditions[i+1]), 64)
		if err != nil {
			return false
		}
		switch str(conditions[i]) {
		case "=":
			if got != want {
				return false
			}
		case ">":
			if got <= want {
				return false
			}
		case ">=":
			if got < want {
				return false
			}
		case "<":
			if got >= want {
				return false
			}
		case "<=":
			if got > want {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func asSlice(v any) []any {
	a, _ := v.([]any)
	return a
}
