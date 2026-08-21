// Package acm issues local certificates (not publicly trusted).
package acm

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.acm", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements ACM-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.acm" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{"RequestCertificate", "DescribeCertificate", "ListCertificates", "DeleteCertificate",
		"GetCertificate", "AddTagsToCertificate", "ListTagsForCertificate", "RemoveTagsFromCertificate",
		"CreateAcmeDomainValidation", "CreateAcmeEndpoint", "CreateAcmeExternalAccountBinding", "DeleteAcmeDomainValidation",
		"DeleteAcmeEndpoint", "DeleteAcmeExternalAccountBinding", "DescribeAcmeAccount", "DescribeAcmeDomainValidation",
		"DescribeAcmeEndpoint", "DescribeAcmeExternalAccountBinding", "ExportCertificate", "GetAccountConfiguration",
		"GetAcmeExternalAccountBindingCredentials", "ImportCertificate", "ListAcmeAccounts", "ListAcmeDomainValidations",
		"ListAcmeEndpoints", "ListAcmeExternalAccountBindings", "ListCertificateDomainValidations", "ListTagsForResource",
		"PutAccountConfiguration", "RenewCertificate", "ResendValidationEmail", "RevokeAcmeAccount",
		"RevokeAcmeExternalAccountBinding", "RevokeCertificate", "SearchCertificates", "TagResource",
		"UntagResource", "UpdateAcmeDomainValidation", "UpdateAcmeEndpoint", "UpdateCertificateOptions"}
	return core
}

func (p *Pack) col(req *spi.Request) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection("acm")
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "RequestCertificate":
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:acm:" + req.Identity.Region + ":" + req.Identity.Account + ":certificate/" + id
		rec := map[string]any{
			"CertificateArn": arn, "DomainName": first(req.Input, "DomainName"),
			"Status": "ISSUED", "Type": "AMAZON_ISSUED",
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CertificateArn": arn}}, nil
	case "DescribeCertificate":
		id := certID(first(req.Input, "CertificateArn"))
		b, ok, _ := p.col(req).Get(ctx, id)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"Certificate": rec}}, nil
	case "ListCertificates":
		kvs, _, _ := p.col(req).List(ctx, "", "", 0)
		var sums []any
		for _, kv := range kvs {
			if strings.HasPrefix(kv.Key, "tags:") {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if rec["CertificateArn"] == nil {
				continue
			}
			sums = append(sums, map[string]any{"CertificateArn": rec["CertificateArn"], "DomainName": rec["DomainName"], "Status": rec["Status"]})
		}
		return &spi.Response{Output: map[string]any{"CertificateSummaryList": sums}}, nil
	case "DeleteCertificate":
		_ = p.col(req).Delete(ctx, certID(first(req.Input, "CertificateArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetCertificate":
		id := certID(first(req.Input, "CertificateArn"))
		if _, ok, _ := p.col(req).Get(ctx, id); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{
			"Certificate":      "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
			"CertificateChain": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
		}}, nil
	case "AddTagsToCertificate":
		arn := first(req.Input, "CertificateArn")
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req).Put(ctx, "tags:"+certID(arn), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForCertificate":
		b, ok, _ := p.col(req).Get(ctx, "tags:"+certID(first(req.Input, "CertificateArn")))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "RemoveTagsFromCertificate":
		_ = p.col(req).Delete(ctx, "tags:"+certID(first(req.Input, "CertificateArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}

func certID(arn string) string {
	if i := lastSlash(arn); i >= 0 {
		return arn[i+1:]
	}
	return arn
}

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}
