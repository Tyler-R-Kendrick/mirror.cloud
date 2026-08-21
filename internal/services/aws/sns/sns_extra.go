package sns

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) topicPermission(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := str(req.Input["TopicArn"])
	name := topicName(arn)
	b, ok, _ := p.col(req, "topics").Get(ctx, name)
	m := map[string]any{"arn": arn, "name": name}
	if ok {
		_ = json.Unmarshal(b, &m)
	}
	attrs, _ := m["attrs"].(map[string]any)
	if attrs == nil {
		attrs = map[string]any{}
	}
	pol := map[string]any{"Version": "2012-10-17", "Statement": []any{}}
	if raw := str(attrs["Policy"]); raw != "" {
		_ = json.Unmarshal([]byte(raw), &pol)
	}
	stmts, _ := pol["Statement"].([]any)
	if req.Operation == "AddPermission" {
		label := str(req.Input["Label"])
		if label == "" {
			return nil, &spi.Fault{Code: "MissingParameter", Message: "Label", HTTPStatus: 400, Fault: "client"}
		}
		accts := stringList(req.Input, "AWSAccountIds", "AWSAccountId")
		acts := stringList(req.Input, "ActionNames", "ActionName")
		stmts = append(stmts, map[string]any{"Sid": label, "Effect": "Allow", "Principal": map[string]any{"AWS": accts}, "Action": acts, "Resource": arn})
		pol["Statement"] = stmts
		raw, _ := json.Marshal(pol)
		attrs["Policy"] = string(raw)
		m["attrs"] = attrs
		nb, _ := json.Marshal(m)
		_ = p.col(req, "topics").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	label := str(req.Input["Label"])
	var kept []any
	found := false
	for _, s := range stmts {
		if str(asMap(s)["Sid"]) == label {
			found = true
			continue
		}
		kept = append(kept, s)
	}
	if !found {
		return nil, &spi.Fault{Code: "NotFound", Message: "Label", HTTPStatus: 404, Fault: "client"}
	}
	pol["Statement"] = kept
	raw, _ := json.Marshal(pol)
	attrs["Policy"] = string(raw)
	m["attrs"] = attrs
	nb, _ := json.Marshal(m)
	_ = p.col(req, "topics").Put(ctx, name, nb)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) subAttrs(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := str(req.Input["SubscriptionArn"])
	b, ok, _ := p.col(req, "subs").Get(ctx, arn)
	var rec map[string]any
	if ok {
		_ = json.Unmarshal(b, &rec)
	} else {
		rec = map[string]any{"SubscriptionArn": arn}
	}
	if req.Operation == "SetSubscriptionAttributes" {
		k := str(req.Input["AttributeName"])
		rec[k] = req.Input["AttributeValue"]
		if rec["attrs"] == nil {
			rec["attrs"] = map[string]any{}
		}
		if attrs, ok := rec["attrs"].(map[string]any); ok {
			attrs[k] = req.Input["AttributeValue"]
			rec["attrs"] = attrs
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "subs").Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	attrs := map[string]any{
		"SubscriptionArn":              rec["SubscriptionArn"],
		"TopicArn":                     rec["TopicArn"],
		"Protocol":                     rec["Protocol"],
		"Endpoint":                     rec["Endpoint"],
		"FilterPolicy":                 rec["FilterPolicy"],
		"RawMessageDelivery":           rec["RawMessageDelivery"],
		"ConfirmationWasAuthenticated": "true",
	}
	if extra, ok := rec["attrs"].(map[string]any); ok {
		for k, v := range extra {
			attrs[k] = v
		}
	}
	return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
}

func (p *Pack) listTags(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := str(req.Input["ResourceArn"])
	b, ok, _ := p.col(req, "tags").Get(ctx, arn)
	var tags any = []any{}
	if ok {
		_ = json.Unmarshal(b, &tags)
	}
	return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
}

func (p *Pack) dataProtection(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := str(req.Input["ResourceArn"])
	if arn == "" {
		arn = str(req.Input["TopicArn"])
	}
	if req.Operation == "PutDataProtectionPolicy" {
		_ = p.col(req, "dpp").Put(ctx, arn, []byte(str(req.Input["DataProtectionPolicy"])))
		return &spi.Response{Output: map[string]any{}}, nil
	}
	b, ok, _ := p.col(req, "dpp").Get(ctx, arn)
	if !ok {
		return &spi.Response{Output: map[string]any{"DataProtectionPolicy": ""}}, nil
	}
	return &spi.Response{Output: map[string]any{"DataProtectionPolicy": string(b)}}, nil
}

func (p *Pack) platformApp(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "platapps")
	switch req.Operation {
	case "CreatePlatformApplication":
		name := str(req.Input["Name"])
		plat := str(req.Input["Platform"])
		arn := "arn:aws:sns:" + req.Identity.Region + ":" + req.Identity.Account + ":app/" + plat + "/" + name
		rec := map[string]any{"PlatformApplicationArn": arn, "Name": name, "Platform": plat, "Attributes": req.Input["Attributes"]}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"PlatformApplicationArn": arn}}, nil
	case "ListPlatformApplications":
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"PlatformApplications": out}}, nil
	case "DeletePlatformApplication":
		_ = col.Delete(ctx, str(req.Input["PlatformApplicationArn"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetPlatformApplicationAttributes":
		arn := str(req.Input["PlatformApplicationArn"])
		b, ok, _ := col.Get(ctx, arn)
		rec := map[string]any{"PlatformApplicationArn": arn}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["Attributes"] = flattenAttrEntries(req.Input, "Attributes")
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		arn := str(req.Input["PlatformApplicationArn"])
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Attributes": rec["Attributes"]}}, nil
	}
}

func (p *Pack) platformEndpoint(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "platend")
	switch req.Operation {
	case "CreatePlatformEndpoint":
		app := str(req.Input["PlatformApplicationArn"])
		tok := str(req.Input["Token"])
		arn := app + "/endpoint/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"EndpointArn": arn, "PlatformApplicationArn": app, "Token": tok, "CustomUserData": req.Input["CustomUserData"], "Enabled": "true"}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"EndpointArn": arn}}, nil
	case "DeleteEndpoint":
		_ = col.Delete(ctx, str(req.Input["EndpointArn"]))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetEndpointAttributes":
		arn := str(req.Input["EndpointArn"])
		b, ok, _ := col.Get(ctx, arn)
		rec := map[string]any{"EndpointArn": arn}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		attrs := flattenAttrEntries(req.Input, "Attributes")
		for k, v := range attrs {
			rec[k] = v
		}
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListEndpointsByPlatformApplication":
		app := str(req.Input["PlatformApplicationArn"])
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if str(rec["PlatformApplicationArn"]) != app {
				continue
			}
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Endpoints": out}}, nil
	default:
		arn := str(req.Input["EndpointArn"])
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Attributes": rec}}, nil
	}
}

func flattenAttrEntries(in map[string]any, prefix string) map[string]any {
	attrs := asMap(in[prefix])
	if len(attrs) > 0 && attrs["entry"] == nil {
		return attrs
	}
	out := map[string]any{}
	p := prefix + ".entry."
	by := map[int]map[string]string{}
	for k, v := range in {
		rest, ok := strings.CutPrefix(k, p)
		if !ok {
			continue
		}
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			continue
		}
		n := 0
		for _, c := range parts[0] {
			if c >= '0' && c <= '9' {
				n = n*10 + int(c-'0')
			}
		}
		slot := by[n]
		if slot == nil {
			slot = map[string]string{}
			by[n] = slot
		}
		slot[strings.ToLower(parts[1])] = str(v)
	}
	for _, slot := range by {
		k := slot["key"]
		if k == "" {
			k = slot["name"]
		}
		if k != "" {
			out[k] = slot["value"]
		}
	}
	return out
}

func (p *Pack) smsAttrs(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.Operation == "SetSMSAttributes" {
		attrs := flattenAttrEntries(req.Input, "Attributes")
		b, _ := json.Marshal(attrs)
		_ = p.col(req, "smsattrs").Put(ctx, "default", b)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	b, ok, _ := p.col(req, "smsattrs").Get(ctx, "default")
	attrs := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &attrs)
	}
	return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
}

func (p *Pack) smsOpt(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	phone := str(req.Input["phoneNumber"])
	if phone == "" {
		phone = str(req.Input["PhoneNumber"])
	}
	col := p.col(req, "smsopt")
	switch req.Operation {
	case "OptInPhoneNumber":
		_ = col.Delete(ctx, phone)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CheckIfPhoneNumberIsOptedOut":
		_, ok, _ := col.Get(ctx, phone)
		return &spi.Response{Output: map[string]any{"isOptedOut": ok}}, nil
	default:
		kvs, _, _ := col.List(ctx, "", "", 0)
		var nums []any
		for _, kv := range kvs {
			nums = append(nums, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"phoneNumbers": nums}}, nil
	}
}

func (p *Pack) smsSandbox(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	phone := str(req.Input["PhoneNumber"])
	col := p.col(req, "smssandbox")
	switch req.Operation {
	case "CreateSMSSandboxPhoneNumber":
		rec := map[string]any{"PhoneNumber": phone, "Status": "Pending"}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, phone, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "VerifySMSSandboxPhoneNumber":
		b, ok, _ := col.Get(ctx, phone)
		rec := map[string]any{"PhoneNumber": phone}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["Status"] = "Verified"
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, phone, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "DeleteSMSSandboxPhoneNumber":
		_ = col.Delete(ctx, phone)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListSMSSandboxPhoneNumbers":
		kvs, _, _ := col.List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"PhoneNumbers": out}}, nil
	default:
		kvs, _, _ := col.List(ctx, "", "", 0)
		verified := 0
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if str(rec["Status"]) == "Verified" {
				verified++
			}
		}
		return &spi.Response{Output: map[string]any{"IsInSandbox": true, "Verified": verified}}, nil
	}
}

func stringList(in map[string]any, names ...string) []any {
	for _, n := range names {
		if v, ok := in[n]; ok {
			switch t := v.(type) {
			case []any:
				return t
			case string:
				if t != "" {
					return []any{t}
				}
			}
		}
	}
	var out []any
	for k, v := range in {
		for _, n := range names {
			if strings.HasPrefix(k, n+".") {
				out = append(out, v)
			}
		}
	}
	return out
}
