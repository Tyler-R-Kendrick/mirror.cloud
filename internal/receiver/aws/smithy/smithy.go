// Package smithy ingests Smithy 2.0 JSON AST into canonical model.Service values.
package smithy

import (
	"context"
	"encoding/json"
	"path"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/receiver"
)

// Receiver implements receiver.Receiver for Smithy 2.0 JSON.
type Receiver struct{}

func (Receiver) Name() string { return "smithy" }

func (Receiver) Detect(_ string, head []byte) bool {
	return strings.Contains(string(head), `"smithy"`)
}

// Ingest parses one Smithy JSON AST file.
func (Receiver) Ingest(ctx context.Context, src model.SourceRef, data []byte) ([]model.Service, error) {
	_ = ctx
	var doc struct {
		Smithy   string                     `json:"smithy"`
		Shapes   map[string]json.RawMessage `json:"shapes"`
		Metadata map[string]json.RawMessage `json:"metadata"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	type shape struct {
		Type    string          `json:"type"`
		Traits  json.RawMessage `json:"traits"`
		Members map[string]struct {
			Target string          `json:"target"`
			Traits json.RawMessage `json:"traits"`
		} `json:"members"`
		Member *struct {
			Target string `json:"target"`
		} `json:"member"`
		Key *struct {
			Target string `json:"target"`
		} `json:"key"`
		Value *struct {
			Target string `json:"target"`
		} `json:"value"`
		Input *struct {
			Target string `json:"target"`
		} `json:"input"`
		Output *struct {
			Target string `json:"target"`
		} `json:"output"`
		Errors []struct {
			Target string `json:"target"`
		} `json:"errors"`
		Operations []struct {
			Target string `json:"target"`
		} `json:"operations"`
		Resources []struct {
			Target string `json:"target"`
		} `json:"resources"`
		CollectionOperations []struct {
			Target string `json:"target"`
		} `json:"collectionOperations"`
		Create *struct {
			Target string `json:"target"`
		} `json:"create"`
		Put *struct {
			Target string `json:"target"`
		} `json:"put"`
		Read *struct {
			Target string `json:"target"`
		} `json:"read"`
		Update *struct {
			Target string `json:"target"`
		} `json:"update"`
		Delete *struct {
			Target string `json:"target"`
		} `json:"delete"`
		List *struct {
			Target string `json:"target"`
		} `json:"list"`
		Mixins []struct {
			Target string `json:"target"`
		} `json:"mixins"`
		Version string `json:"version"`
		Target  string `json:"target"`
	}
	parsed := map[string]shape{}
	unknown := map[string]bool{}
	for id, raw := range doc.Shapes {
		var s shape
		if err := json.Unmarshal(raw, &s); err != nil {
			continue
		}
		parsed[id] = s
		if s.Traits != nil {
			var traits map[string]json.RawMessage
			if json.Unmarshal(s.Traits, &traits) == nil {
				for t := range traits {
					if !knownTrait(t) {
						unknown[t] = true
					}
				}
			}
		}
	}
	_ = unknown // recorded by not failing
	var out []model.Service
	for id, s := range parsed {
		if s.Type != "service" {
			continue
		}
		proto, prefix, _, _, xmlns := protocols(s.Traits)
		var awsSvc struct {
			SdkID          string `json:"sdkId"`
			EndpointPrefix string `json:"endpointPrefix"`
			ArnNamespace   string `json:"arnNamespace"`
		}
		decodeNamed(s.Traits, "aws.api#service", &awsSvc)
		if prefix == "" {
			prefix = awsSvc.EndpointPrefix
		}
		// The SigV4 signing name is what a client puts in the credential
		// scope, and for seventy-seven upstream models it is not the endpoint
		// prefix: Lex Model Building signs as `lex` and is reached at
		// `models.lex`, ECR signs as `ecr` and is reached at `api.ecr`. A
		// server that knows only the prefix cannot recognise the request.
		aliases := signingAliases(s.Traits, prefix)
		target := localName(id)
		ver := s.Version
		svcID := awsServiceID(prefix, awsSvc.SdkID, id)
		svc := model.Service{
			ID:             svcID,
			Namespace:      namespace(id),
			Protocol:       proto,
			EndpointPrefix: prefix,
			TargetPrefix:   target,
			QueryVersion:   ver,
			XMLNamespace:   xmlns,
			Aliases:        aliases,
			Shapes:         map[string]model.Shape{},
			Source:         src,
		}
		opSeen := map[string]bool{}
		addOp := func(id string) {
			if id != "" {
				opSeen[id] = true
			}
		}
		for _, r := range s.Operations {
			addOp(r.Target)
		}
		var walkRes func(string)
		walkRes = func(rid string) {
			rs, ok := parsed[rid]
			if !ok || rs.Type != "resource" {
				return
			}
			if rs.Create != nil {
				addOp(rs.Create.Target)
			}
			if rs.Put != nil {
				addOp(rs.Put.Target)
			}
			if rs.Read != nil {
				addOp(rs.Read.Target)
			}
			if rs.Update != nil {
				addOp(rs.Update.Target)
			}
			if rs.Delete != nil {
				addOp(rs.Delete.Target)
			}
			if rs.List != nil {
				addOp(rs.List.Target)
			}
			for _, r := range rs.Operations {
				addOp(r.Target)
			}
			for _, r := range rs.CollectionOperations {
				addOp(r.Target)
			}
			for _, r := range rs.Resources {
				walkRes(r.Target)
			}
		}
		for _, r := range s.Resources {
			walkRes(r.Target)
		}
		for opID := range opSeen {
			opSh, ok := parsed[opID]
			if !ok || opSh.Type != "operation" {
				continue
			}
			op := model.Operation{
				Name:       localName(opID),
				Confidence: model.ConfDeclared,
				Source:     src,
			}
			if opSh.Input != nil {
				op.Input = opSh.Input.Target
			}
			if opSh.Output != nil {
				op.Output = opSh.Output.Target
			}
			for _, e := range opSh.Errors {
				op.Errors = append(op.Errors, e.Target)
			}
			httpBind(opSh.Traits, &op)
			if op.HTTP.Method == "" {
				op.HTTP.Method = "POST"
			}
			if op.HTTP.URI == "" {
				op.HTTP.URI = "/"
			}
			if op.HTTP.Code == 0 {
				op.HTTP.Code = 200
			}
			if hasTrait(opSh.Traits, "smithy.api#readonly") {
				op.Readonly = true
			}
			if hasTrait(opSh.Traits, "smithy.api#idempotent") {
				op.Idempotent = true
			}
			if pag := paginated(opSh.Traits); pag != nil {
				op.Pagination = pag
			} else if pag := paginated(s.Traits); pag != nil {
				op.Pagination = pag
			}
			if target != "" {
				op.Target = target + "." + op.Name
			}
			op.QueryAction = op.Name
			svc.Operations = append(svc.Operations, op)
		}
		for sid, sh := range parsed {
			if ns := namespace(sid); ns != namespace(id) && sh.Type != "service" {
				continue
			}
			ms := model.Shape{ID: sid, Kind: kindOf(sh.Type), Members: map[string]model.Member{}}
			for n, m := range sh.Members {
				ms.Members[n] = model.Member{Shape: m.Target, Required: hasTrait(m.Traits, "smithy.api#required")}
			}
			if sh.Member != nil {
				ms.Member = sh.Member.Target
			}
			if sh.Value != nil && ms.Member == "" {
				ms.Member = sh.Value.Target
			}
			if sh.Key != nil {
				ms.Key = sh.Key.Target
			}
			svc.Shapes[sid] = ms
		}
		out = append(out, svc)
	}
	if len(out) == 0 && path.Ext(src.Path) != "" {
		// no service shape — not fatal
		return nil, nil
	}
	return out, nil
}

func decodeNamed(traits json.RawMessage, name string, dst any) {
	var t map[string]json.RawMessage
	if json.Unmarshal(traits, &t) != nil {
		return
	}
	if raw, ok := t[name]; ok {
		_ = json.Unmarshal(raw, dst)
	}
}

func awsServiceID(prefix, sdk, id string) string {
	if prefix != "" {
		return "aws." + strings.ToLower(prefix)
	}
	if sdk != "" {
		return "aws." + strings.ToLower(strings.ReplaceAll(sdk, " ", ""))
	}
	return "aws." + strings.ToLower(localName(id))
}

func paginated(traits json.RawMessage) *model.Pagination {
	var t map[string]json.RawMessage
	if json.Unmarshal(traits, &t) != nil {
		return nil
	}
	raw, ok := t["smithy.api#paginated"]
	if !ok {
		return nil
	}
	var p struct {
		InputToken  string `json:"inputToken"`
		OutputToken string `json:"outputToken"`
		Items       string `json:"items"`
		PageSize    string `json:"pageSize"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	return &model.Pagination{InputToken: p.InputToken, OutputToken: p.OutputToken, Items: p.Items, PageSize: p.PageSize}
}

func kindOf(t string) model.ShapeKind {
	switch t {
	case "structure":
		return model.KindStructure
	case "list":
		return model.KindList
	case "map":
		return model.KindMap
	case "string", "enum", "intEnum":
		return model.KindString
	case "integer", "long", "short", "byte", "bigInteger":
		return model.KindInteger
	case "float", "double", "bigDecimal":
		return model.KindDouble
	case "boolean":
		return model.KindBoolean
	case "timestamp":
		return model.KindTimestamp
	case "blob":
		return model.KindBlob
	case "document":
		return model.KindDocument
	case "union":
		return model.KindUnion
	default:
		return model.KindString
	}
}

func protocols(traits json.RawMessage) (model.Protocol, string, string, string, string) {
	var t map[string]json.RawMessage
	_ = json.Unmarshal(traits, &t)
	p := model.ProtoAWSJSON11
	switch {
	case t["aws.protocols#restXml"] != nil:
		p = model.ProtoRESTXML
	case t["aws.protocols#restJson1"] != nil:
		p = model.ProtoRESTJSON1
	case t["aws.protocols#awsJson1_0"] != nil:
		p = model.ProtoAWSJSON10
	case t["aws.protocols#awsJson1_1"] != nil:
		p = model.ProtoAWSJSON11
	case t["aws.protocols#awsQuery"] != nil:
		p = model.ProtoAWSQuery
	case t["aws.protocols#ec2Query"] != nil:
		p = model.ProtoEC2Query
	}
	prefix, target, ver, xmlns := "", "", "", ""
	if raw, ok := t["aws.api#service"]; ok {
		var s struct {
			SdkID          string `json:"sdkId"`
			EndpointPrefix string `json:"endpointPrefix"`
			ArnNamespace   string `json:"arnNamespace"`
		}
		_ = json.Unmarshal(raw, &s)
		prefix = s.EndpointPrefix
		if prefix == "" {
			prefix = strings.ToLower(strings.ReplaceAll(s.SdkID, " ", ""))
		}
	}
	if raw, ok := t["aws.protocols#awsJson1_0"]; ok {
		var s struct {
			Target string `json:"http"`
		}
		_ = json.Unmarshal(raw, &s)
	}
	_ = target
	if raw, ok := t["smithy.api#xmlNamespace"]; ok {
		var s struct {
			Uri string `json:"uri"`
		}
		_ = json.Unmarshal(raw, &s)
		xmlns = s.Uri
	}
	return p, prefix, target, ver, xmlns
}

func httpBind(traits json.RawMessage, op *model.Operation) {
	var t map[string]json.RawMessage
	if json.Unmarshal(traits, &t) != nil {
		return
	}
	if raw, ok := t["smithy.api#http"]; ok {
		var h struct {
			Method string `json:"method"`
			URI    string `json:"uri"`
			Code   int    `json:"code"`
		}
		_ = json.Unmarshal(raw, &h)
		op.HTTP = model.HTTPBinding{Method: h.Method, URI: h.URI, Code: h.Code}
	}
}

func hasTrait(traits json.RawMessage, name string) bool {
	var t map[string]json.RawMessage
	if json.Unmarshal(traits, &t) != nil {
		return false
	}
	_, ok := t[name]
	return ok
}

func knownTrait(t string) bool {
	return strings.HasPrefix(t, "smithy.api#") || strings.HasPrefix(t, "aws.")
}

func localName(id string) string {
	if i := strings.LastIndex(id, "#"); i >= 0 {
		return id[i+1:]
	}
	return id
}

func namespace(id string) string {
	if i := strings.LastIndex(id, "#"); i >= 0 {
		return id[:i]
	}
	return id
}

var _ receiver.Receiver = Receiver{}

// signingAliases reports the other names a client may address this service by.
// Today that is the SigV4 signing name when it differs from the endpoint
// prefix; the field is a list because a service can carry more than one name
// and because the same question will be asked of other providers.
func signingAliases(traits json.RawMessage, prefix string) []string {
	var t map[string]json.RawMessage
	if err := json.Unmarshal(traits, &t); err != nil {
		return nil
	}
	raw, ok := t["aws.auth#sigv4"]
	if !ok {
		return nil
	}
	var sig struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &sig); err != nil || sig.Name == "" {
		return nil
	}
	if strings.EqualFold(sig.Name, prefix) {
		return nil
	}
	return []string{strings.ToLower(sig.Name)}
}
