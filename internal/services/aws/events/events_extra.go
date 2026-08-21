package events

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

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
	case "CreateConnection":
		return p.createConnection(ctx, req)
	case "UpdateConnection":
		return p.updateConnection(ctx, req)
	case "DescribeConnection":
		return p.describeConnection(ctx, req)
	case "ListConnections":
		return p.listConnections(ctx, req)
	case "DeleteConnection", "DeauthorizeConnection":
		name := first(req.Input, "Name")
		if op == "DeauthorizeConnection" {
			return p.deauthorizeConnection(ctx, req)
		}
		_ = p.col(req, "connections").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateApiDestination":
		return p.createAPIDestination(ctx, req)
	case "UpdateApiDestination":
		return p.updateAPIDestination(ctx, req)
	case "DescribeApiDestination":
		return p.describeAPIDestination(ctx, req)
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
		return p.getRec(ctx, req, "rules", eventKey(eventBus(req.Input), first(req.Input, "Name")))
	case "EnableRule", "DisableRule":
		st := "ENABLED"
		if op == "DisableRule" {
			st = "DISABLED"
		}
		return p.patch(ctx, req, "rules", eventKey(eventBus(req.Input), first(req.Input, "Name")), map[string]any{"State": st})
	case "ListRuleNamesByTarget":
		want := first(req.Input, "TargetArn")
		bus := eventBus(req.Input)
		kvs, _, _ := p.col(req, "targets").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			keyBus, _, _ := strings.Cut(kv.Key, "\x00")
			if keyBus != bus {
				continue
			}
			var tgs []any
			_ = json.Unmarshal(kv.Value, &tgs)
			for _, t := range tgs {
				m, _ := t.(map[string]any)
				if str(m["Arn"]) == want || want == "" {
					names = append(names, eventName(kv.Key))
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

func (p *Pack) createConnection(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	if err := validateConnection(req.Input); err != nil {
		return nil, err
	}
	if _, exists := p.load(ctx, req, "connections", name); exists {
		return nil, resourceExists("Connection")
	}
	now := eventTimestamp(p.deps.Clock.Now())
	record := map[string]any{}
	mergeEventRecord(record, req.Input)
	record["ConnectionArn"] = eventResourceARN(req, "connection", name, p.deps.Rand.Hex(8))
	record["ConnectionState"] = "AUTHORIZED"
	record["CreationTime"], record["LastModifiedTime"], record["LastAuthorizedTime"] = now, now, now
	p.saveEventRecord(ctx, req, "connections", name, record)
	return &spi.Response{Output: connectionResult(record)}, nil
}

func (p *Pack) updateConnection(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	previous, exists := p.load(ctx, req, "connections", name)
	if !exists {
		return nil, resourceMissing("Connection")
	}
	record := map[string]any{}
	mergeEventRecord(record, previous)
	if authorizationType := str(req.Input["AuthorizationType"]); authorizationType != "" && authorizationType != str(previous["AuthorizationType"]) {
		delete(record, "AuthParameters")
	}
	mergeEventRecord(record, req.Input)
	record["Name"], record["ConnectionArn"] = previous["Name"], previous["ConnectionArn"]
	_, authChanged := req.Input["AuthParameters"]
	_, typeChanged := req.Input["AuthorizationType"]
	if authChanged || typeChanged {
		if err := validateConnection(record); err != nil {
			return nil, err
		}
		record["ConnectionState"] = "AUTHORIZED"
		record["LastAuthorizedTime"] = eventTimestamp(p.deps.Clock.Now())
	}
	record["LastModifiedTime"] = eventTimestamp(p.deps.Clock.Now())
	p.saveEventRecord(ctx, req, "connections", name, record)
	return &spi.Response{Output: connectionResult(record)}, nil
}

func (p *Pack) describeConnection(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	record, exists := p.load(ctx, req, "connections", first(req.Input, "Name"))
	if !exists {
		return nil, resourceMissing("Connection")
	}
	return &spi.Response{Output: publicConnection(record)}, nil
}

func (p *Pack) listConnections(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kvs, _, _ := p.col(req, "connections").List(ctx, "", "", 0)
	connections := make([]any, 0, len(kvs))
	for _, kv := range kvs {
		var record map[string]any
		_ = json.Unmarshal(kv.Value, &record)
		public := publicConnection(record)
		delete(public, "AuthParameters")
		connections = append(connections, public)
	}
	return &spi.Response{Output: map[string]any{"Connections": connections}}, nil
}

func (p *Pack) deauthorizeConnection(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	record, exists := p.load(ctx, req, "connections", name)
	if !exists {
		return nil, resourceMissing("Connection")
	}
	delete(record, "AuthParameters")
	record["ConnectionState"] = "DEAUTHORIZED"
	record["LastModifiedTime"] = eventTimestamp(p.deps.Clock.Now())
	p.saveEventRecord(ctx, req, "connections", name, record)
	return &spi.Response{Output: connectionResult(record)}, nil
}

func (p *Pack) createAPIDestination(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	if err := validateAPIDestination(req.Input); err != nil {
		return nil, err
	}
	if _, exists := p.load(ctx, req, "apidest", name); exists {
		return nil, resourceExists("API destination")
	}
	if err := p.validateDestinationConnection(ctx, req, str(req.Input["ConnectionArn"])); err != nil {
		return nil, err
	}
	now := eventTimestamp(p.deps.Clock.Now())
	record := map[string]any{}
	mergeEventRecord(record, req.Input)
	if _, exists := record["InvocationRateLimitPerSecond"]; !exists {
		record["InvocationRateLimitPerSecond"] = 300
	}
	record["ApiDestinationArn"] = eventResourceARN(req, "api-destination", name, p.deps.Rand.Hex(8))
	record["ApiDestinationState"] = "ACTIVE"
	record["CreationTime"], record["LastModifiedTime"] = now, now
	p.saveEventRecord(ctx, req, "apidest", name, record)
	return &spi.Response{Output: apiDestinationResult(record)}, nil
}

func (p *Pack) updateAPIDestination(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	name := first(req.Input, "Name")
	previous, exists := p.load(ctx, req, "apidest", name)
	if !exists {
		return nil, resourceMissing("API destination")
	}
	record := map[string]any{}
	mergeEventRecord(record, previous)
	mergeEventRecord(record, req.Input)
	record["Name"], record["ApiDestinationArn"] = previous["Name"], previous["ApiDestinationArn"]
	if err := validateAPIDestination(record); err != nil {
		return nil, err
	}
	if _, changed := req.Input["ConnectionArn"]; changed {
		if err := p.validateDestinationConnection(ctx, req, str(record["ConnectionArn"])); err != nil {
			return nil, err
		}
	}
	record["LastModifiedTime"] = eventTimestamp(p.deps.Clock.Now())
	p.saveEventRecord(ctx, req, "apidest", name, record)
	return &spi.Response{Output: apiDestinationResult(record)}, nil
}

func (p *Pack) describeAPIDestination(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	record, exists := p.load(ctx, req, "apidest", first(req.Input, "Name"))
	if !exists {
		return nil, resourceMissing("API destination")
	}
	return &spi.Response{Output: record}, nil
}

func (p *Pack) validateDestinationConnection(ctx context.Context, req *spi.Request, arn string) error {
	connection, exists := p.load(ctx, req, "connections", arnResourceName(arn, "connection/"))
	if !exists {
		return resourceMissing("Connection")
	}
	if connection["ConnectionState"] != "AUTHORIZED" {
		return validationFault("Connection must be authorized.")
	}
	return nil
}

func validateConnection(record map[string]any) error {
	if !validEventResourceName(first(record, "Name")) {
		return validationFault("Name is required and must be a valid EventBridge resource name.")
	}
	authorizationType := str(record["AuthorizationType"])
	auth, ok := record["AuthParameters"].(map[string]any)
	key := map[string]string{"API_KEY": "ApiKeyAuthParameters", "BASIC": "BasicAuthParameters", "OAUTH_CLIENT_CREDENTIALS": "OAuthParameters"}[authorizationType]
	if !ok || key == "" {
		return validationFault("AuthorizationType and AuthParameters are required.")
	}
	for _, other := range []string{"ApiKeyAuthParameters", "BasicAuthParameters", "OAuthParameters"} {
		if _, exists := auth[other]; exists && other != key {
			return validationFault("AuthParameters must match AuthorizationType.")
		}
	}
	parameters, ok := auth[key].(map[string]any)
	if !ok {
		return validationFault("AuthParameters must match AuthorizationType.")
	}
	switch authorizationType {
	case "API_KEY":
		if str(parameters["ApiKeyName"]) == "" || str(parameters["ApiKeyValue"]) == "" {
			return validationFault("ApiKeyName and ApiKeyValue are required.")
		}
	case "BASIC":
		if str(parameters["Username"]) == "" || str(parameters["Password"]) == "" {
			return validationFault("Username and Password are required.")
		}
	case "OAUTH_CLIENT_CREDENTIALS":
		clients, _ := parameters["ClientParameters"].(map[string]any)
		method := str(parameters["HttpMethod"])
		if str(parameters["AuthorizationEndpoint"]) == "" || (method != "GET" && method != "POST" && method != "PUT") || str(clients["ClientID"]) == "" || str(clients["ClientSecret"]) == "" {
			return validationFault("Complete OAuth client credentials are required.")
		}
	}
	return nil
}

func validateAPIDestination(record map[string]any) error {
	method := str(record["HttpMethod"])
	if !validEventResourceName(first(record, "Name")) || str(record["ConnectionArn"]) == "" || str(record["InvocationEndpoint"]) == "" || !oneOf(method, "POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE") {
		return validationFault("Name, ConnectionArn, InvocationEndpoint, and a supported HttpMethod are required.")
	}
	if value, exists := record["InvocationRateLimitPerSecond"]; exists {
		rate, err := strconv.Atoi(toString(value))
		if err != nil || rate < 1 {
			return validationFault("InvocationRateLimitPerSecond must be a positive integer.")
		}
	}
	return nil
}

func validEventResourceName(name string) bool {
	matched, _ := regexp.MatchString(`^[.\-_A-Za-z0-9]{1,64}$`, name)
	return matched
}

func oneOf(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
}

func mergeEventRecord(destination, source map[string]any) {
	for key, value := range source {
		if sourceMap, ok := value.(map[string]any); ok {
			if destinationMap, ok := destination[key].(map[string]any); ok {
				mergeEventRecord(destinationMap, sourceMap)
				continue
			}
		}
		destination[key] = value
	}
}

func publicConnection(record map[string]any) map[string]any {
	body, _ := json.Marshal(record)
	var public map[string]any
	_ = json.Unmarshal(body, &public)
	redactConnectionSecrets(public)
	return public
}

func redactConnectionSecrets(value any) {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{"ApiKeyValue", "Password", "ClientSecret"} {
			delete(typed, key)
		}
		if secret, _ := typed["IsValueSecret"].(bool); secret {
			delete(typed, "Value")
		}
		for _, child := range typed {
			redactConnectionSecrets(child)
		}
	case []any:
		for _, child := range typed {
			redactConnectionSecrets(child)
		}
	}
}

func connectionResult(record map[string]any) map[string]any {
	return map[string]any{
		"ConnectionArn": record["ConnectionArn"], "ConnectionState": record["ConnectionState"], "CreationTime": record["CreationTime"],
		"LastAuthorizedTime": record["LastAuthorizedTime"], "LastModifiedTime": record["LastModifiedTime"],
	}
}

func apiDestinationResult(record map[string]any) map[string]any {
	return map[string]any{
		"ApiDestinationArn": record["ApiDestinationArn"], "ApiDestinationState": record["ApiDestinationState"],
		"CreationTime": record["CreationTime"], "LastModifiedTime": record["LastModifiedTime"],
	}
}

func eventTimestamp(now time.Time) float64 { return float64(now.UnixNano()) / 1e9 }

func eventResourceARN(req *spi.Request, kind, name, suffix string) string {
	return "arn:aws:events:" + req.Identity.Region + ":" + req.Identity.Account + ":" + kind + "/" + name + "/" + suffix
}

func validationFault(message string) error {
	return &spi.Fault{Code: "ValidationException", Message: message, HTTPStatus: 400, Fault: "client"}
}

func resourceMissing(kind string) error {
	return &spi.Fault{Code: "ResourceNotFoundException", Message: kind + " does not exist.", HTTPStatus: 400, Fault: "client"}
}

func resourceExists(kind string) error {
	return &spi.Fault{Code: "ResourceAlreadyExistsException", Message: kind + " already exists.", HTTPStatus: 400, Fault: "client"}
}

func (p *Pack) saveEventRecord(ctx context.Context, req *spi.Request, collection, name string, record map[string]any) {
	body, _ := json.Marshal(record)
	_ = p.col(req, collection).Put(ctx, name, body)
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

// MatchEventPattern reports whether an event matches an EventBridge pattern.
func MatchEventPattern(pattern string, event any) bool { return matchEventPattern(pattern, event) }

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
	if suffix, ok := op["suffix"].(string); ok {
		return strings.HasSuffix(got, suffix)
	}
	if equal, ok := op["equals-ignore-case"].(string); ok {
		return strings.EqualFold(got, equal)
	}
	if wildcard, ok := op["wildcard"].(string); ok {
		return matchWildcard(wildcard, got)
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

func matchWildcard(pattern, value string) bool {
	expression := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*") + "$"
	matched, _ := regexp.MatchString(expression, value)
	return matched
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
