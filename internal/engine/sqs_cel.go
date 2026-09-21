package engine

import (
	"fmt"
	neturl "net/url"
	"sort"
	"strings"
	"unicode"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// validMessageAttributes mirrors the pack's attribute gate: name, type, and
// exactly one value carrier that is non-empty and free of control characters.
func validMessageAttributes(attrs any) bool {
	src, _ := attrs.(map[string]any)
	for name, raw := range src {
		if !validMessageAttributeName(name) {
			return false
		}
		attribute, _ := raw.(map[string]any)
		if attribute == nil {
			return false
		}
		dataType := anyString(attribute["DataType"])
		if !validMessageAttributeDataType(dataType) {
			return false
		}
		_, hasString := attribute["StringValue"]
		_, hasBinary := attribute["BinaryValue"]
		_, hasStringList := attribute["StringListValues"]
		_, hasBinaryList := attribute["BinaryListValues"]
		count := 0
		for _, present := range []bool{hasString, hasBinary, hasStringList, hasBinaryList} {
			if present {
				count++
			}
		}
		if count != 1 {
			return false
		}
		if hasString {
			value := anyString(attribute["StringValue"])
			if value == "" || !messageContentsOK(value) {
				return false
			}
		}
		if hasBinary && attribute["BinaryValue"] == nil {
			return false
		}
		if hasStringList {
			list, _ := attribute["StringListValues"].([]any)
			if len(list) == 0 {
				return false
			}
		}
		if hasBinaryList {
			list, _ := attribute["BinaryListValues"].([]any)
			if len(list) == 0 {
				return false
			}
		}
	}
	return true
}

func validMessageAttributeName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || len(runes) > 256 || strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
		return false
	}
	if !unicode.IsLetter(runes[0]) && !unicode.IsDigit(runes[0]) {
		return false
	}
	if !unicode.IsLetter(runes[len(runes)-1]) && !unicode.IsDigit(runes[len(runes)-1]) {
		return false
	}
	for _, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func validMessageAttributeDataType(dataType string) bool {
	runes := []rune(dataType)
	if len(runes) == 0 || len(runes) > 256 {
		return false
	}
	base, suffix, hasSuffix := strings.Cut(dataType, ".")
	if base != "String" && base != "Number" && base != "Binary" {
		return false
	}
	if !hasSuffix || suffix == "" {
		return !hasSuffix
	}
	for _, r := range suffix {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '.' && r != '_' && r != '-' {
			return false
		}
	}
	return true
}

func messageContentsOK(value string) bool {
	for _, r := range value {
		if r != '\t' && r != '\n' && r != '\r' && (r < 0x20 || r > 0xD7FF && r < 0xE000 || r > 0xFFFD && r < 0x10000) {
			return false
		}
	}
	return true
}

// sqsEndpoint applies the LocalStack SQS endpoint strategy on top of the
// advertise base, matching the pack's sqsAdvertise.
func sqsEndpoint(base, strategy, region string) string {
	strategy = strings.ToLower(strategy)
	if strategy == "" || strategy == "off" {
		return base
	}
	parsed, err := neturl.Parse(base)
	if err != nil || parsed.Host == "" {
		return base
	}
	host := parsed.Hostname()
	if host == "localhost" {
		host = "localhost.localstack.cloud"
	}
	port := parsed.Port()
	switch strategy {
	case "standard":
		host = "sqs." + region + "." + host
	case "domain":
		if region == "us-east-1" {
			host = "queue." + host
		} else {
			host = region + ".queue." + host
		}
	case "path":
		parsed.Path = "/queue/" + region
	default:
		return base
	}
	parsed.Host = host
	if port != "" {
		parsed.Host += ":" + port
	}
	return parsed.String()
}

// sqsQueueAttrConflict returns the first requested attribute whose value
// differs from the effective queue attributes, or "" when CreateQueue may
// reuse the existing queue.
func sqsQueueAttrConflict(effective, requested any) string {
	want, _ := requested.(map[string]any)
	have, _ := effective.(map[string]any)
	if have == nil {
		have = map[string]any{}
	}
	names := make([]string, 0, len(want))
	for k := range want {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if fmt.Sprint(have[k]) != fmt.Sprint(want[k]) {
			return k
		}
	}
	return ""
}

// sqsMergeQueueAttrs applies SetQueueAttributes updates the way the pack did:
// empty RedrivePolicy/Policy/KmsMasterKeyId delete the key; default KMS reuse
// period 300 deletes that key; setting a KMS key without SSE flag forces
// SqsManagedSseEnabled=false.
func sqsMergeQueueAttrs(existing, updates any) any {
	out := map[string]any{}
	if m, _ := existing.(map[string]any); m != nil {
		for k, v := range m {
			out[k] = v
		}
	}
	upd, _ := updates.(map[string]any)
	if upd == nil {
		return out
	}
	for key, value := range upd {
		sv := anyString(value)
		if (key == "RedrivePolicy" || key == "Policy") && sv == "" {
			delete(out, key)
			continue
		}
		if key == "KmsMasterKeyId" && sv == "" {
			delete(out, key)
			continue
		}
		if key == "KmsDataKeyReusePeriodSeconds" && sv == "300" {
			delete(out, key)
			continue
		}
		out[key] = value
	}
	if anyString(upd["KmsMasterKeyId"]) != "" {
		if _, ok := upd["SqsManagedSseEnabled"]; !ok || anyString(upd["SqsManagedSseEnabled"]) == "" {
			out["SqsManagedSseEnabled"] = "false"
		}
	}
	return out
}

func validQueueName(value string) bool {
	if strings.HasSuffix(value, ".fifo") {
		value = strings.TrimSuffix(value, ".fifo")
	}
	if value == "" || len(value) > 80 {
		return false
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

// sqsIAMPrincipals mirrors pack AddPermission: one account is a bare ARN
// string, several stay a list — Policy JSON allows either.
func sqsIAMPrincipals(accounts any, region string) any {
	list, _ := accounts.([]any)
	out := make([]any, 0, len(list))
	for _, a := range list {
		out = append(out, fmt.Sprintf("arn:%s:iam::%s:root", partitionOf(region), fmt.Sprint(a)))
	}
	if len(out) == 1 {
		return out[0]
	}
	return out
}

// sqsSQSActions qualifies bare action names; one action is a string, several a list.
func sqsSQSActions(actions any) any {
	list, _ := actions.([]any)
	out := make([]any, 0, len(list))
	for _, a := range list {
		s := fmt.Sprint(a)
		if !strings.Contains(s, ":") {
			s = "SQS:" + s
		}
		out = append(out, s)
	}
	if len(out) == 1 {
		return out[0]
	}
	return out
}

func partitionOf(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	case strings.HasPrefix(region, "us-iso-b-"):
		return "aws-iso-b"
	case strings.HasPrefix(region, "us-iso-"):
		return "aws-iso"
	default:
		return "aws"
	}
}

// rewriteQueueOwner swaps Identity.Account when the request addresses another
// account's queue — QueueOwnerAWSAccountId or the account segment of QueueUrl.
func rewriteQueueOwner(req *spi.Request) *spi.Request {
	owner := ""
	if v, ok := req.Input["QueueOwnerAWSAccountId"]; ok {
		owner = fmt.Sprint(v)
	}
	if owner == "" {
		u := ""
		if v, ok := req.Input["QueueUrl"]; ok {
			u = fmt.Sprint(v)
		}
		if u != "" {
			parsed, err := neturl.Parse(u)
			if err == nil {
				parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
				if len(parts) >= 2 {
					cand := parts[len(parts)-2]
					if len(cand) == 12 {
						ok := true
						for _, r := range cand {
							if r < '0' || r > '9' {
								ok = false
								break
							}
						}
						if ok {
							owner = cand
						}
					}
				}
			}
		}
	}
	if owner == "" || owner == req.Identity.Account {
		return req
	}
	effective := *req
	effective.Identity = req.Identity
	effective.Identity.Account = owner
	return &effective
}

// normalizeSQSQueryTags lifts query-style Tag.N.Key / Tags.member.N into the
// Tags map the model requires, before validateInput runs.
func normalizeSQSQueryTags(req *spi.Request) {
	if req == nil || req.Input == nil {
		return
	}
	if _, ok := req.Input["Tags"]; ok {
		return
	}
	tags := map[string]any{}
	keys, values := map[string]string{}, map[string]string{}
	for key, value := range req.Input {
		if strings.HasPrefix(key, "Tag.") || strings.HasPrefix(key, "Tags.member.") {
			base := strings.TrimSuffix(strings.TrimSuffix(key, ".Key"), ".Value")
			switch {
			case strings.HasSuffix(key, ".Key"):
				keys[base] = fmt.Sprint(value)
			case strings.HasSuffix(key, ".Value"):
				values[base] = fmt.Sprint(value)
			}
		}
	}
	for base, name := range keys {
		tags[name] = values[base]
	}
	if len(tags) > 0 {
		req.Input["Tags"] = tags
	}
	if _, ok := req.Input["TagKeys"]; !ok {
		var out []any
		for key, value := range req.Input {
			if strings.HasPrefix(key, "TagKey.") || strings.HasPrefix(key, "TagKeys.member.") {
				out = append(out, fmt.Sprint(value))
			}
		}
		if len(out) > 0 {
			req.Input["TagKeys"] = out
		}
	}
}
