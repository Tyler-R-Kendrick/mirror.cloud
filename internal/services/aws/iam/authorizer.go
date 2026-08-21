package iam

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// NewAuthorizer evaluates user, group, and role policies: explicit Deny wins, then Allow.
// No policies still allows (drop-in for unsigned local clients).
func NewAuthorizer(st spi.Store) spi.Authorizer {
	return &authorizer{st: st}
}

type authorizer struct{ st spi.Store }

func (a *authorizer) Authorize(ctx context.Context, id spi.Identity, serviceID, operation, resource string) error {
	return a.authorize(ctx, id, serviceID, operation, resource, map[string]string{"aws:RequestedRegion": id.Region})
}

// AuthorizeRequest evaluates condition keys derived from the decoded request.
func (a *authorizer) AuthorizeRequest(ctx context.Context, req *spi.Request, resource string) error {
	if req == nil {
		return nil
	}
	return a.authorize(ctx, req.Identity, req.ServiceID, req.Operation, resource, requestConditionValues(req))
}

func (a *authorizer) authorize(ctx context.Context, id spi.Identity, serviceID, operation, resource string, values map[string]string) error {
	if a == nil || a.st == nil {
		return nil
	}
	action := iamAction(serviceID, operation)
	col := a.st.Scope(id.Account, id.Region).Collection("iam")
	kind, name, active := principal(ctx, col, id)
	if !active {
		return denied(id.ARN, action)
	}
	docs := loadPrincipalDocs(ctx, col, kind, name)
	if decideWithContext(docs, action, resource, values) != "allowed" {
		return denied(id.ARN, action)
	}
	if scps := loadSCPDocs(ctx, a.st, id); len(scps) > 0 && decideWithContext(scps, action, resource, values) != "allowed" {
		return denied(id.ARN, action)
	}
	return nil
}

func requestConditionValues(req *spi.Request) map[string]string {
	values := map[string]string{"aws:RequestedRegion": req.Identity.Region}
	if options, ok := req.Input["MetadataOptions"].(map[string]any); ok {
		if token := str(options["HttpTokens"]); token != "" {
			values["ec2:MetadataHttpTokens"] = token
		}
	}
	if arn := str(req.Input["PolicyArn"]); arn != "" {
		values["iam:PolicyArn"] = arn
	}
	addRequestTags(values, req.Input["Tags"])
	addRequestTags(values, req.Input["Tagging"])
	if req.HTTP != nil {
		if tags, err := url.ParseQuery(req.HTTP.Header.Get("x-amz-tagging")); err == nil {
			for key, vals := range tags {
				if len(vals) > 0 {
					addRequestTag(values, key, vals[0])
				}
			}
		}
	}
	return values
}

func addRequestTags(values map[string]string, raw any) {
	switch tags := raw.(type) {
	case map[string]any:
		for key, value := range tags {
			addRequestTag(values, key, str(value))
		}
	case map[string]string:
		for key, value := range tags {
			addRequestTag(values, key, value)
		}
	case []any:
		for _, rawTag := range tags {
			tag, _ := rawTag.(map[string]any)
			key, value := str(tag["Key"]), str(tag["Value"])
			if key == "" {
				key, value = str(tag["key"]), str(tag["value"])
			}
			addRequestTag(values, key, value)
		}
	case string:
		if tags, err := url.ParseQuery(tags); err == nil {
			for key, vals := range tags {
				if len(vals) > 0 {
					addRequestTag(values, key, vals[0])
				}
			}
		}
	}
}

func addRequestTag(values map[string]string, key, value string) {
	if key == "" {
		return
	}
	values["aws:RequestTag/"+key] = value
	values["s3:RequestObjectTag/"+key] = value
}

func principal(ctx context.Context, col spi.Collection, id spi.Identity) (kind, name string, active bool) {
	if role := roleFromARN(id.ARN); role != "" {
		return "role", role, true
	}
	if id.AccessKeyID != "" {
		kvs, _, _ := col.List(ctx, "ak:", "", 0)
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if str(rec["AccessKeyId"]) == id.AccessKeyID {
				return "user", str(rec["UserName"]), !strings.EqualFold(str(rec["Status"]), "Inactive")
			}
		}
	}
	return "user", userFromARN(id.ARN), true
}

func decide(docs []map[string]any, action, resource string) string {
	return decideWithContext(docs, action, resource, nil)
}

func decideWithContext(docs []map[string]any, action, resource string, values map[string]string) string {
	if effectMatch(docs, "Deny", action, resource, values) {
		return "explicitDeny"
	}
	if len(docs) == 0 {
		return "allowed"
	}
	if effectMatch(docs, "Allow", action, resource, values) {
		return "allowed"
	}
	return "implicitDeny"
}

func denied(arn, action string) error {
	return &spi.Fault{Code: "AccessDenied", Message: "User: " + arn + " is not authorized to perform: " + action, HTTPStatus: 403, Fault: "client"}
}

func roleFromARN(arn string) string {
	const p = ":assumed-role/"
	i := strings.Index(arn, p)
	if i < 0 {
		return ""
	}
	rest := arn[i+len(p):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

func userFromARN(arn string) string {
	const p = ":user/"
	i := strings.Index(arn, p)
	if i < 0 {
		return ""
	}
	return strings.TrimPrefix(arn[i+len(p):], "/")
}

func iamAction(serviceID, operation string) string {
	svc := strings.TrimPrefix(serviceID, "aws.")
	if i := strings.LastIndex(svc, "."); i >= 0 {
		svc = svc[i+1:]
	}
	return svc + ":" + operation
}

func loadDocs(ctx context.Context, col spi.Collection, role string) []map[string]any {
	return loadPrincipalDocs(ctx, col, "role", role)
}

func loadPrincipalDocs(ctx context.Context, col spi.Collection, kind, name string) []map[string]any {
	if name == "" {
		return nil
	}
	inline, attached := "rolepolicy:", "attached:"
	if kind == "user" {
		inline, attached = "userpolicy:", "uattached:"
	}
	docs := loadInlineDocs(ctx, col, inline+name+":")
	docs = append(docs, loadAttachedDocs(ctx, col, attached+name+":")...)
	if kind == "user" {
		groups, _, _ := col.List(ctx, "ug:"+name+":", "", 0)
		for _, kv := range groups {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			group := str(rec["GroupName"])
			groupDocs := loadInlineDocs(ctx, col, "grouppolicy:"+group+":")
			docs = append(docs, groupDocs...)
			docs = append(docs, loadAttachedDocs(ctx, col, "gattached:"+group+":")...)
		}
	}
	return docs
}

func loadInlineDocs(ctx context.Context, col spi.Collection, prefix string) []map[string]any {
	var docs []map[string]any
	kvs, _, _ := col.List(ctx, prefix, "", 0)
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		if d := parseDoc(rec["PolicyDocument"]); len(statements(d)) > 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

func loadAttachedDocs(ctx context.Context, col spi.Collection, prefix string) []map[string]any {
	var docs []map[string]any
	atts, _, _ := col.List(ctx, prefix, "", 0)
	for _, kv := range atts {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		arn, _ := rec["PolicyArn"].(string)
		name := arn
		if i := strings.LastIndex(arn, "/"); i >= 0 {
			name = arn[i+1:]
		}
		b, ok, _ := col.Get(ctx, "policy:"+name)
		if !ok {
			continue
		}
		var pol map[string]any
		_ = json.Unmarshal(b, &pol)
		doc := pol["PolicyDocument"]
		if version := str(pol["DefaultVersionId"]); version != "" && version != "v1" {
			if vb, found, _ := col.Get(ctx, "polver:"+name+":"+version); found {
				var rec map[string]any
				_ = json.Unmarshal(vb, &rec)
				doc = rec["Document"]
			}
		}
		if d := parseDoc(doc); len(statements(d)) > 0 {
			docs = append(docs, d)
		}
	}
	return docs
}

func loadSCPDocs(ctx context.Context, st spi.Store, id spi.Identity) []map[string]any {
	b, ok, _ := st.Scope("_mirror", "global").Collection("orgmembers").Get(ctx, id.Account)
	if !ok {
		return nil
	}
	var member map[string]any
	_ = json.Unmarshal(b, &member)
	scope := st.Scope(str(member["ManagementAccount"]), str(member["Region"]))
	col := scope.Collection("oattach")
	policies := scope.Collection("opolicy")
	targets := []string{str(member["RootId"])}
	for parent, seen := str(member["ParentId"]), map[string]bool{}; parent != "" && parent != str(member["RootId"]) && !seen[parent]; {
		seen[parent] = true
		targets = append(targets, parent)
		ou, found, _ := scope.Collection("oou").Get(ctx, parent)
		if !found {
			break
		}
		var rec map[string]any
		_ = json.Unmarshal(ou, &rec)
		parent = str(rec["ParentId"])
	}
	targets = append(targets, id.Account)
	var docs []map[string]any
	for _, target := range targets {
		attachments, _, _ := col.List(ctx, target+"/", "", 0)
		for _, attachment := range attachments {
			var link map[string]any
			_ = json.Unmarshal(attachment.Value, &link)
			policy, found, _ := policies.Get(ctx, str(link["PolicyId"]))
			if !found {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(policy, &rec)
			if doc := parseDoc(rec["Content"]); len(statements(doc)) > 0 {
				docs = append(docs, doc)
			}
		}
	}
	return docs
}

func parseDoc(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		return t
	case string:
		var m map[string]any
		_ = json.Unmarshal([]byte(t), &m)
		return m
	}
	return map[string]any{}
}

func denyMatch(docs []map[string]any, action, resource string) bool {
	return effectMatch(docs, "Deny", action, resource, nil)
}

func allowMatch(docs []map[string]any, action, resource string) bool {
	return effectMatch(docs, "Allow", action, resource, nil)
}

func effectMatch(docs []map[string]any, effect, action, resource string, values map[string]string) bool {
	for _, doc := range docs {
		for _, st := range statements(doc) {
			if strings.EqualFold(str(st["Effect"]), effect) && statementActionMatches(st, action) && statementResourceMatches(st, resource) && conditionsMatch(st["Condition"], values) {
				return true
			}
		}
	}
	return false
}

func statementActionMatches(st map[string]any, action string) bool {
	if _, ok := st["Action"]; ok {
		return actionMatches(st["Action"], action)
	}
	return !actionMatches(st["NotAction"], action)
}

func statementResourceMatches(st map[string]any, resource string) bool {
	if _, ok := st["Resource"]; ok {
		return resourceMatches(st["Resource"], resource)
	}
	return !resourceMatches(st["NotResource"], resource)
}

func conditionsMatch(spec any, values map[string]string) bool {
	conditions, _ := spec.(map[string]any)
	for operator, raw := range conditions {
		entries, _ := raw.(map[string]any)
		for key, expected := range entries {
			actual, present := conditionValue(values, key)
			equal := false
			for _, value := range asStrings(expected) {
				if actual == value {
					equal = true
					break
				}
			}
			op := strings.TrimSuffix(operator, "IfExists")
			if !present && op != "Null" && strings.HasSuffix(operator, "IfExists") {
				continue
			}
			switch op {
			case "StringEquals", "ArnEquals":
				if !present || !equal {
					return false
				}
			case "StringNotEquals", "ArnNotEquals":
				if present && equal {
					return false
				}
			case "StringLike", "ArnLike":
				if !present || !matchesAny(asStrings(expected), actual) {
					return false
				}
			case "StringNotLike", "ArnNotLike":
				if present && matchesAny(asStrings(expected), actual) {
					return false
				}
			case "NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals", "NumericGreaterThan", "NumericGreaterThanEquals":
				if !present || !numericMatch(op, actual, asStrings(expected)) {
					return false
				}
			case "Null":
				wantNull := strings.EqualFold(str(expected), "true")
				if wantNull == present {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func conditionValue(values map[string]string, key string) (string, bool) {
	for candidate, value := range values {
		if strings.EqualFold(candidate, key) {
			return value, true
		}
	}
	return "", false
}

func matchesAny(patterns []string, actual string) bool {
	for _, pattern := range patterns {
		if globMatch(pattern, actual) {
			return true
		}
	}
	return false
}

func numericMatch(operator, actual string, expected []string) bool {
	a, err := strconv.ParseFloat(actual, 64)
	if err != nil {
		return false
	}
	if operator == "NumericNotEquals" {
		for _, value := range expected {
			if b, err := strconv.ParseFloat(value, 64); err == nil && a == b {
				return false
			}
		}
		return true
	}
	for _, value := range expected {
		b, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		switch operator {
		case "NumericEquals":
			if a == b {
				return true
			}
		case "NumericLessThan":
			if a < b {
				return true
			}
		case "NumericLessThanEquals":
			if a <= b {
				return true
			}
		case "NumericGreaterThan":
			if a > b {
				return true
			}
		case "NumericGreaterThanEquals":
			if a >= b {
				return true
			}
		}
	}
	return false
}

func statements(doc map[string]any) []map[string]any {
	switch t := doc["Statement"].(type) {
	case []any:
		var out []map[string]any
		for _, s := range t {
			if m, ok := s.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	case map[string]any:
		return []map[string]any{t}
	}
	return nil
}

func actionMatches(spec any, action string) bool {
	pats := asStrings(spec)
	if len(pats) == 0 {
		return false
	}
	for _, p := range pats {
		if globMatch(strings.ToLower(p), strings.ToLower(action)) {
			return true
		}
	}
	return false
}

func resourceMatches(spec any, resource string) bool {
	ss := asStrings(spec)
	if len(ss) == 0 {
		return true
	}
	for _, p := range ss {
		if globMatch(p, resource) {
			return true
		}
	}
	return false
}

func globMatch(pat, s string) bool {
	p, i, star, retry := 0, 0, -1, 0
	for i < len(s) {
		if p < len(pat) && (pat[p] == '?' || pat[p] == s[i]) {
			p, i = p+1, i+1
		} else if p < len(pat) && pat[p] == '*' {
			star, retry, p = p, i, p+1
		} else if star >= 0 {
			retry++
			i, p = retry, star+1
		} else {
			return false
		}
	}
	for p < len(pat) && pat[p] == '*' {
		p++
	}
	return p == len(pat)
}

func asStrings(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		var out []string
		for _, x := range t {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func str(v any) string { s, _ := v.(string); return s }
