package sns

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) topicPermission(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	arn := str(req.Input["TopicArn"])
	if !validTopicARN(arn) {
		return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: TopicArn", HTTPStatus: 400, Fault: "client"}
	}
	parts := strings.Split(arn, ":")
	if parts[3] != req.Identity.Region || parts[4] != req.Identity.Account {
		return nil, topicNotFoundFault()
	}
	name := topicName(arn)
	if _, ok, _ := p.col(req, "topics").Get(ctx, name); !ok {
		return nil, topicNotFoundFault()
	}
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
		for _, statement := range stmts {
			if str(asMap(statement)["Sid"]) == label {
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "A policy statement with this label already exists", HTTPStatus: 400, Fault: "client"}
			}
		}
		for _, action := range acts {
			switch str(action) {
			case "Publish", "Subscribe", "Receive", "SendMessage", "GetTopicAttributes", "SetTopicAttributes", "DeleteTopic", "ListSubscriptionsByTopic", "AddPermission", "RemovePermission", "GetDataProtectionPolicy", "PutDataProtectionPolicy", "TagResource", "UntagResource":
			default:
				return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid action", HTTPStatus: 400, Fault: "client"}
			}
		}
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
	if !validSubscriptionARN(arn) {
		return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: SubscriptionArn", HTTPStatus: 400, Fault: "client"}
	}
	b, ok, _ := p.col(req, "subs").Get(ctx, arn)
	if !ok {
		return nil, &spi.Fault{Code: "NotFound", Message: "Subscription does not exist", HTTPStatus: 404, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	if req.Operation == "SetSubscriptionAttributes" {
		k := str(req.Input["AttributeName"])
		value := str(req.Input["AttributeValue"])
		if fault := validateSubscriptionAttribute(k, value); fault != nil {
			return nil, fault
		}
		if k == "FilterPolicy" && value != "" {
			if fault := validateFilterPolicy(value); fault != nil {
				return nil, fault
			}
		}
		if k == "FilterPolicy" && value == "" {
			delete(rec, k)
		} else if k == "RawMessageDelivery" {
			rec[k] = strings.ToLower(value)
		} else {
			rec[k] = req.Input["AttributeValue"]
		}
		if rec["attrs"] == nil {
			rec["attrs"] = map[string]any{}
		}
		if attrs, ok := rec["attrs"].(map[string]any); ok {
			if k == "FilterPolicy" && value == "" {
				delete(attrs, k)
			} else if k == "RawMessageDelivery" {
				attrs[k] = strings.ToLower(value)
			} else {
				attrs[k] = req.Input["AttributeValue"]
			}
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
		"ConfirmationWasAuthenticated": "true",
		"PendingConfirmation":          "false",
	}
	if confirmed, ok := rec["Confirmed"].(bool); ok && !confirmed {
		attrs["ConfirmationWasAuthenticated"] = "false"
		attrs["PendingConfirmation"] = "true"
	}
	for _, key := range []string{"FilterPolicy", "RawMessageDelivery"} {
		if value := rec[key]; value != nil && str(value) != "" {
			attrs[key] = value
		}
	}
	if extra, ok := rec["attrs"].(map[string]any); ok {
		for k, v := range extra {
			if k == "FilterPolicyScope" && rec["FilterPolicy"] == nil {
				continue
			}
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
		if !validTopicARN(arn) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: ResourceArn", HTTPStatus: 400, Fault: "client"}
		}
		parts := strings.Split(arn, ":")
		if parts[3] != req.Identity.Region || parts[4] != req.Identity.Account {
			return nil, topicNotFoundFault()
		}
		name := topicName(arn)
		if _, found, _ := p.col(req, "topics").Get(ctx, name); !found {
			return nil, topicNotFoundFault()
		}
		policy := str(req.Input["DataProtectionPolicy"])
		_ = p.col(req, "dpp").Put(ctx, arn, []byte(policy))
		b, _, _ := p.col(req, "topics").Get(ctx, name)
		m := map[string]any{"arn": arn, "name": name}
		_ = json.Unmarshal(b, &m)
		attrs := asMap(m["attrs"])
		if attrs == nil {
			attrs = map[string]any{}
		}
		attrs["DataProtectionPolicy"] = policy
		m["attrs"] = attrs
		nb, _ := json.Marshal(m)
		_ = p.col(req, "topics").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	b, ok, _ := p.col(req, "dpp").Get(ctx, arn)
	if !ok {
		return &spi.Response{Output: map[string]any{"DataProtectionPolicy": ""}}, nil
	}
	return &spi.Response{Output: map[string]any{"DataProtectionPolicy": string(b)}}, nil
}

func validateFilterPolicy(raw string) *spi.Fault {
	var policy map[string]any
	if json.Unmarshal([]byte(raw), &policy) != nil || policy == nil {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: FilterPolicy", HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func validateSubscriptionAttribute(name, value string) *spi.Fault {
	switch name {
	case "FilterPolicy":
		if value != "" {
			return validateFilterPolicy(value)
		}
	case "FilterPolicyScope":
		if value != "MessageAttributes" && value != "MessageBody" {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: FilterPolicyScope", HTTPStatus: 400, Fault: "client"}
		}
	case "RawMessageDelivery":
		if !strings.EqualFold(value, "true") && !strings.EqualFold(value, "false") {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: RawMessageDelivery", HTTPStatus: 400, Fault: "client"}
		}
	case "RedrivePolicy":
		if value != "" {
			var policy map[string]any
			if json.Unmarshal([]byte(value), &policy) != nil || !validSQSARN(str(policy["deadLetterTargetArn"])) {
				return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: RedrivePolicy", HTTPStatus: 400, Fault: "client"}
			}
		}
	case "DeliveryPolicy", "SubscriptionRoleArn":
	default:
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: " + name, HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func validSQSARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 6 && parts[0] == "arn" && parts[2] == "sqs" && parts[3] != "" && parts[4] != "" && parts[5] != ""
}

func (p *Pack) platformApp(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "platapps")
	switch req.Operation {
	case "CreatePlatformApplication":
		name := str(req.Input["Name"])
		plat := str(req.Input["Platform"])
		attrs := flattenAttrEntries(req.Input, "Attributes")
		if len(attrs) == 0 && req.Input["Attributes"] != nil {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Attributes", HTTPStatus: 400, Fault: "client"}
		}
		if fault := validatePlatformApplication(name, plat, attrs); fault != nil {
			return nil, fault
		}
		arn := "arn:aws:sns:" + req.Identity.Region + ":" + req.Identity.Account + ":app/" + plat + "/" + name
		rec := map[string]any{"PlatformApplicationArn": arn, "Name": name, "Platform": plat, "Attributes": attrs}
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
		app := str(req.Input["PlatformApplicationArn"])
		_ = col.Delete(ctx, app)
		endpoints := p.col(req, "platend")
		kvs, _, _ := endpoints.List(ctx, "", "", 0)
		for _, kv := range kvs {
			var endpoint map[string]any
			if json.Unmarshal(kv.Value, &endpoint) == nil && str(endpoint["PlatformApplicationArn"]) == app {
				_ = endpoints.Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetPlatformApplicationAttributes":
		arn := str(req.Input["PlatformApplicationArn"])
		if !validPlatformApplicationARN(arn) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PlatformApplicationArn", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", Message: "Platform application does not exist", HTTPStatus: 404, Fault: "client"}
		}
		rec := map[string]any{"PlatformApplicationArn": arn}
		_ = json.Unmarshal(b, &rec)
		rec["Attributes"] = flattenAttrEntries(req.Input, "Attributes")
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		arn := str(req.Input["PlatformApplicationArn"])
		if !validPlatformApplicationARN(arn) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PlatformApplicationArn", HTTPStatus: 400, Fault: "client"}
		}
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Attributes": rec["Attributes"]}}, nil
	}
}

func validPlatformApplicationARN(arn string) bool {
	parts := strings.Split(arn, ":")
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "sns" || parts[3] == "" || parts[4] == "" {
		return false
	}
	resource := strings.Split(parts[5], "/")
	return len(resource) == 3 && resource[0] == "app" && resource[1] != "" && resource[2] != ""
}

func validatePlatformApplication(name, platform string, attrs map[string]any) *spi.Fault {
	if len(name) < 1 || len(name) > 256 || !validTopicChars(name) {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Name", HTTPStatus: 400, Fault: "client"}
	}
	switch platform {
	case "ADM", "APNS", "APNS_SANDBOX", "BAIDU", "FCM", "GCM", "MPNS", "WNS":
	default:
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Platform", HTTPStatus: 400, Fault: "client"}
	}
	if len(attrs) > 0 && (str(attrs["PlatformCredential"]) == "" || (platform == "ADM" && str(attrs["PlatformPrincipal"]) == "")) {
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Attributes", HTTPStatus: 400, Fault: "client"}
	}
	for key := range attrs {
		if key != "PlatformPrincipal" && key != "PlatformCredential" {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Attributes", HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
}

func (p *Pack) platformEndpoint(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.col(req, "platend")
	switch req.Operation {
	case "CreatePlatformEndpoint":
		app := str(req.Input["PlatformApplicationArn"])
		tok := str(req.Input["Token"])
		if _, ok, _ := p.col(req, "platapps").Get(ctx, app); !ok {
			return nil, &spi.Fault{Code: "NotFound", Message: "Platform application does not exist", HTTPStatus: 404, Fault: "client"}
		}
		if tok == "" {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Token", HTTPStatus: 400, Fault: "client"}
		}
		attrs := flattenAttrEntries(req.Input, "Attributes")
		if fault := validateEndpointAttributes(attrs); fault != nil {
			return nil, fault
		}
		customUserData := req.Input["CustomUserData"]
		if customUserData == nil {
			customUserData = attrs["CustomUserData"]
		}
		requestedAttrs := map[string]any{}
		for key, value := range attrs {
			requestedAttrs[key] = value
		}
		if customUserData != nil {
			requestedAttrs["CustomUserData"] = customUserData
		}
		kvs, _, _ := col.List(ctx, "", "", 0)
		for _, kv := range kvs {
			var existing map[string]any
			if json.Unmarshal(kv.Value, &existing) == nil && str(existing["PlatformApplicationArn"]) == app && str(existing["Token"]) == tok {
				for key, value := range requestedAttrs {
					if key == "Enabled" && strings.EqualFold(str(existing[key]), str(value)) {
						continue
					}
					if str(existing[key]) != str(value) {
						return nil, &spi.Fault{Code: "InvalidParameter", Message: "Endpoint already exists with a different attribute value.", HTTPStatus: 400, Fault: "client"}
					}
				}
				return &spi.Response{Output: map[string]any{"EndpointArn": existing["EndpointArn"]}}, nil
			}
		}
		arn := app + "/endpoint/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"EndpointArn": arn, "PlatformApplicationArn": app, "Token": tok, "CustomUserData": customUserData, "Enabled": "true"}
		for key, value := range requestedAttrs {
			rec[key] = value
		}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"EndpointArn": arn}}, nil
	case "DeleteEndpoint":
		arn := str(req.Input["EndpointArn"])
		_ = col.Delete(ctx, arn)
		kvs, _, _ := p.col(req, "subs").List(ctx, "", "", 0)
		for _, kv := range kvs {
			var sub map[string]any
			if json.Unmarshal(kv.Value, &sub) == nil && str(sub["Endpoint"]) == arn {
				_ = p.col(req, "subs").Delete(ctx, kv.Key)
			}
		}
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetEndpointAttributes":
		arn := str(req.Input["EndpointArn"])
		b, ok, _ := col.Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", Message: "Endpoint does not exist", HTTPStatus: 404, Fault: "client"}
		}
		rec := map[string]any{"EndpointArn": arn}
		_ = json.Unmarshal(b, &rec)
		attrs := flattenAttrEntries(req.Input, "Attributes")
		if len(attrs) == 0 {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Attributes", HTTPStatus: 400, Fault: "client"}
		}
		if fault := validateEndpointAttributes(attrs); fault != nil {
			return nil, fault
		}
		for k, v := range attrs {
			rec[k] = v
		}
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, arn, nb)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListEndpointsByPlatformApplication":
		app := str(req.Input["PlatformApplicationArn"])
		if _, ok, _ := p.col(req, "platapps").Get(ctx, app); !ok {
			return nil, &spi.Fault{Code: "NotFound", Message: "Platform application does not exist", HTTPStatus: 404, Fault: "client"}
		}
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

func validateEndpointAttributes(attrs map[string]any) *spi.Fault {
	for key, value := range attrs {
		switch key {
		case "Enabled":
			if !strings.EqualFold(str(value), "true") && !strings.EqualFold(str(value), "false") {
				return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Enabled", HTTPStatus: 400, Fault: "client"}
			}
		case "Token":
			if str(value) == "" {
				return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: Token", HTTPStatus: 400, Fault: "client"}
			}
		case "CustomUserData":
			if len(str(value)) > 2048 {
				return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: CustomUserData", HTTPStatus: 400, Fault: "client"}
			}
		default:
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: " + key, HTTPStatus: 400, Fault: "client"}
		}
	}
	return nil
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
		for key, value := range attrs {
			if fault := validateSMSAttribute(key, str(value)); fault != nil {
				return nil, fault
			}
		}
		b, _ := json.Marshal(attrs)
		_ = p.col(req, "smsattrs").Put(ctx, "default", b)
		return &spi.Response{Output: map[string]any{}}, nil
	}
	b, ok, _ := p.col(req, "smsattrs").Get(ctx, "default")
	attrs := map[string]any{"MonthlySpendLimit": "1"}
	if ok {
		_ = json.Unmarshal(b, &attrs)
	}
	requested := stringList(req.Input, "attributes", "Attributes")
	if len(requested) > 0 {
		filtered := map[string]any{}
		for _, key := range requested {
			name := str(key)
			if value, found := attrs[name]; found {
				filtered[name] = value
			}
		}
		attrs = filtered
	}
	return &spi.Response{Output: map[string]any{"Attributes": attrs}}, nil
}

func validateSMSAttribute(name, value string) *spi.Fault {
	switch name {
	case "DeliveryStatusSuccessSamplingRate":
		rate, err := strconv.Atoi(value)
		if err != nil || rate < 0 || rate > 100 {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: DeliveryStatusSuccessSamplingRate", HTTPStatus: 400, Fault: "client"}
		}
	case "DefaultSenderID":
		if len(value) > 11 || value == "" {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: DefaultSenderID", HTTPStatus: 400, Fault: "client"}
		}
		letter := false
		for _, r := range value {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				letter = true
			}
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != ' ' {
				return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: DefaultSenderID", HTTPStatus: 400, Fault: "client"}
			}
		}
		if !letter {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: DefaultSenderID", HTTPStatus: 400, Fault: "client"}
		}
	case "DefaultSMSType":
		if value != "Promotional" && value != "Transactional" {
			return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: DefaultSMSType", HTTPStatus: 400, Fault: "client"}
		}
	default:
		return &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: " + name, HTTPStatus: 400, Fault: "client"}
	}
	return nil
}

func (p *Pack) smsOpt(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	phone := str(req.Input["phoneNumber"])
	if phone == "" {
		phone = str(req.Input["PhoneNumber"])
	}
	col := p.col(req, "smsopt")
	switch req.Operation {
	case "OptInPhoneNumber":
		if !validSMSNumber(phone) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PhoneNumber", HTTPStatus: 400, Fault: "client"}
		}
		_ = col.Delete(ctx, phone)
		return &spi.Response{Output: map[string]any{}}, nil
	case "CheckIfPhoneNumberIsOptedOut":
		if !validSMSNumber(phone) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PhoneNumber", HTTPStatus: 400, Fault: "client"}
		}
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
		if !validSMSNumber(phone) {
			return nil, &spi.Fault{Code: "InvalidParameter", Message: "Invalid parameter: PhoneNumber", HTTPStatus: 400, Fault: "client"}
		}
		rec := map[string]any{"PhoneNumber": phone, "Status": "Pending"}
		b, _ := json.Marshal(rec)
		_ = col.Put(ctx, phone, b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "VerifySMSSandboxPhoneNumber":
		b, ok, _ := col.Get(ctx, phone)
		if !ok {
			return nil, &spi.Fault{Code: "NotFound", Message: "Phone number is not registered", HTTPStatus: 404, Fault: "client"}
		}
		rec := map[string]any{"PhoneNumber": phone}
		_ = json.Unmarshal(b, &rec)
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
