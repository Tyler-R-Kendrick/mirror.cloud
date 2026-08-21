// Package acmpca stores private CA and issued-cert records (local untrusted PEMs).
package acmpca

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.acm-pca", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ACM PCA-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.acm-pca" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateCertificateAuthority", "DescribeCertificateAuthority", "ListCertificateAuthorities", "DeleteCertificateAuthority",
		"IssueCertificate", "GetCertificate",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateCertificateAuthority":
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:acm-pca:" + req.Identity.Region + ":" + req.Identity.Account + ":certificate-authority/" + id
		rec := map[string]any{"Arn": arn, "Status": "ACTIVE", "Type": first(req.Input, "Type")}
		if rec["Type"] == "" {
			rec["Type"] = "ROOT"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "pca").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"CertificateAuthorityArn": arn}}, nil
	case "DescribeCertificateAuthority":
		arn := first(req.Input, "CertificateAuthorityArn")
		b, ok, _ := p.col(req, "pca").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"CertificateAuthority": rec}}, nil
	case "ListCertificateAuthorities":
		return listWrap(ctx, p.col(req, "pca"), "CertificateAuthorities")
	case "DeleteCertificateAuthority":
		_ = p.col(req, "pca").Delete(ctx, first(req.Input, "CertificateAuthorityArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "IssueCertificate":
		id := p.deps.Rand.Hex(8)
		ca := first(req.Input, "CertificateAuthorityArn")
		arn := ca + "/certificate/" + id
		rec := map[string]any{"CertificateArn": arn, "Certificate": "-----BEGIN CERTIFICATE-----\nMIRROR\n-----END CERTIFICATE-----\n"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "pcacert").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"CertificateArn": arn}}, nil
	case "GetCertificate":
		arn := first(req.Input, "CertificateArn")
		b, ok, _ := p.col(req, "pcacert").Get(ctx, arn)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	default:
		return nil, spi.NotImplemented("aws.acm-pca", req.Operation, "emulate")
	}
}

func listWrap(ctx context.Context, c spi.Collection, key string) (*spi.Response, error) {
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
	if in == nil {
		return ""
	}
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
