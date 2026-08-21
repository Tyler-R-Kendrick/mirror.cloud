// Package mock is a generic BehaviorPack constructed from any model.Service.
package mock

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Pack serves mock-tier synthesis for one service.
type Pack struct {
	svc    *model.Service
	deps   spi.Deps
	strict bool
	mu     sync.Mutex
	crud   map[string]map[string]any
	warned map[string]bool
}

// New constructs a mock pack. If strict, Invoke returns NotImplemented.
func New(svc *model.Service, deps spi.Deps, strict bool) *Pack {
	return &Pack{svc: svc, deps: deps, strict: strict, crud: map[string]map[string]any{}, warned: map[string]bool{}}
}

func (p *Pack) ServiceID() string { return p.svc.ID }
func (p *Pack) Tier() model.Tier  { return model.TierMock }

func (p *Pack) Operations() []string {
	out := make([]string, len(p.svc.Operations))
	for i, op := range p.svc.Operations {
		out[i] = op.Name
	}
	return out
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := p.svc.OperationByName(req.Operation)
	if op == nil {
		return nil, spi.NotImplemented(p.svc.ID, req.Operation, "mock")
	}
	if p.strict {
		return nil, spi.NotImplemented(p.svc.ID, req.Operation, "emulate")
	}
	if err := validate(p.svc, op, req.Input); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if !p.warned[op.Name] {
		p.warned[op.Name] = true
		p.deps.Journal.Record(spi.Entry{ServiceID: p.svc.ID, Operation: op.Name, Tier: model.TierMock, Note: "first mock-tier use"})
	}
	p.mu.Unlock()

	seed := hash(p.svc.ID, op.Name, req.Input)
	r := p.deps.Rand.Derive(seed)
	out := synthesize(p.svc, op, r)

	// CRUD-by-convention
	id := identifier(req.Input)
	switch {
	case hasPrefix(op.Name, "Create") || hasPrefix(op.Name, "Put"):
		if id != "" {
			p.mu.Lock()
			if p.crud[op.Name[:3]] == nil {
				p.crud["rec"] = map[string]any{}
			}
			if p.crud["rec"] == nil {
				p.crud["rec"] = map[string]any{}
			}
			p.crud["rec"][id] = req.Input
			p.mu.Unlock()
		}
	case hasPrefix(op.Name, "Get") || hasPrefix(op.Name, "Describe"):
		if id != "" {
			p.mu.Lock()
			if rec, ok := p.crud["rec"][id]; ok {
				if m, ok := rec.(map[string]any); ok {
					for k, v := range m {
						out[k] = v
					}
				}
			}
			p.mu.Unlock()
		}
	case hasPrefix(op.Name, "Delete"):
		if id != "" {
			p.mu.Lock()
			delete(p.crud["rec"], id)
			p.mu.Unlock()
		}
	case hasPrefix(op.Name, "List"):
		p.mu.Lock()
		var items []any
		for _, v := range p.crud["rec"] {
			items = append(items, v)
		}
		p.mu.Unlock()
		out["Items"] = items
	}
	h := make(map[string][]string)
	h["x-mirror-fidelity"] = []string{"mock"}
	return &spi.Response{Output: out, Headers: h, Status: op.HTTP.Code}, nil
}

func validate(svc *model.Service, op *model.Operation, in map[string]any) error {
	sh, ok := svc.Shapes[op.Input]
	if !ok {
		return nil
	}
	for name, mem := range sh.Members {
		if mem.Required {
			if _, ok := in[name]; !ok {
				return &spi.Fault{Code: "ValidationException", Message: "missing required member " + name, HTTPStatus: 400, Fault: "client"}
			}
		}
	}
	return nil
}

func synthesize(svc *model.Service, op *model.Operation, r spi.Rand) map[string]any {
	out := map[string]any{}
	sh, ok := svc.Shapes[op.Output]
	if !ok {
		out["RequestId"] = r.Hex(16)
		return out
	}
	for _, name := range memberNames(sh.Members) {
		out[name] = synthShape(svc, sh.Members[name].Shape, r)
	}
	return out
}

func synthShape(svc *model.Service, id string, r spi.Rand) any {
	sh, ok := svc.Shapes[id]
	if !ok {
		return r.Hex(8)
	}
	switch sh.Kind {
	case model.KindString, model.KindEnum:
		if len(sh.EnumValues) > 0 {
			return sh.EnumValues[r.Intn(len(sh.EnumValues))]
		}
		return r.Hex(12)
	case model.KindBoolean:
		return r.Intn(2) == 1
	case model.KindInteger, model.KindLong:
		return float64(r.Intn(1000))
	case model.KindFloat, model.KindDouble:
		return float64(r.Intn(1000)) / 10
	case model.KindList:
		return []any{}
	case model.KindMap:
		return map[string]any{}
	case model.KindStructure:
		m := map[string]any{}
		for _, n := range memberNames(sh.Members) {
			m[n] = synthShape(svc, sh.Members[n].Shape, r)
		}
		return m
	default:
		return r.Hex(8)
	}
}

func identifier(in map[string]any) string {
	for _, k := range []string{"Name", "Bucket", "QueueName", "TableName", "TopicArn", "RoleName", "SecretId", "Name"} {
		if v, ok := in[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
	}
	return ""
}

func hasPrefix(s, p string) bool { return len(s) >= len(p) && s[:len(p)] == p }

func memberNames(m map[string]model.Member) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func hash(svc, op string, in map[string]any) string {
	b, _ := json.Marshal(in)
	h := sha256.Sum256(append(append([]byte(svc+"\x00"+op+"\x00"), b...), 0))
	return hex.EncodeToString(h[:])
}

func (p *Pack) String() string { return fmt.Sprintf("mock:%s", p.svc.ID) }
