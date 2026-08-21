package acm

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) extraCol(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "ImportCertificate":
		id := p.deps.Rand.Hex(8)
		arn := "arn:aws:acm:" + req.Identity.Region + ":" + req.Identity.Account + ":certificate/" + id
		dom := first(req.Input, "DomainName")
		if dom == "" {
			dom = "imported.local"
		}
		rec := map[string]any{
			"CertificateArn": arn, "DomainName": dom, "Status": "ISSUED", "Type": "IMPORTED",
			"Certificate": req.Input["Certificate"],
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req).Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"CertificateArn": arn}}, nil
	case "ExportCertificate":
		id := certID(first(req.Input, "CertificateArn"))
		if _, ok, _ := p.col(req).Get(ctx, id); !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		return &spi.Response{Output: map[string]any{
			"Certificate": "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n",
			"PrivateKey":  "mirror-private-key",
		}}, nil
	case "RenewCertificate":
		return p.patchCert(ctx, req, map[string]any{"Status": "ISSUED", "Renewed": true})
	case "RevokeCertificate":
		return p.patchCert(ctx, req, map[string]any{"Status": "REVOKED", "RevocationReason": first(req.Input, "RevocationReason")})
	case "ResendValidationEmail":
		return p.patchCert(ctx, req, map[string]any{"ValidationEmailSent": true})
	case "UpdateCertificateOptions":
		return p.patchCert(ctx, req, map[string]any{"Options": req.Input["Options"]})
	case "SearchCertificates":
		dom := first(req.Input, "DomainName")
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
			if dom != "" && first(rec, "DomainName") != dom {
				continue
			}
			sums = append(sums, map[string]any{"CertificateArn": rec["CertificateArn"], "DomainName": rec["DomainName"], "Status": rec["Status"]})
		}
		return &spi.Response{Output: map[string]any{"CertificateSummaryList": sums}}, nil
	case "ListCertificateDomainValidations":
		id := certID(first(req.Input, "CertificateArn"))
		b, ok, _ := p.col(req).Get(ctx, id)
		dom := ""
		if ok {
			var rec map[string]any
			_ = json.Unmarshal(b, &rec)
			dom = first(rec, "DomainName")
		}
		return &spi.Response{Output: map[string]any{"DomainValidations": []any{map[string]any{
			"DomainName": dom, "ValidationStatus": "SUCCESS", "ValidationMethod": "DNS",
		}}}}, nil
	case "TagResource":
		arn := first(req.Input, "ResourceArn", "CertificateArn")
		b, _ := json.Marshal(req.Input["Tags"])
		_ = p.col(req).Put(ctx, "tags:"+certID(arn), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "UntagResource":
		_ = p.col(req).Delete(ctx, "tags:"+certID(first(req.Input, "ResourceArn", "CertificateArn")))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req).Get(ctx, "tags:"+certID(first(req.Input, "ResourceArn", "CertificateArn")))
		var tags any = []any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"Tags": tags}}, nil
	case "PutAccountConfiguration":
		b, _ := json.Marshal(req.Input)
		_ = p.extraCol(req, "acm-acct").Put(ctx, "default", b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetAccountConfiguration":
		b, ok, _ := p.extraCol(req, "acm-acct").Get(ctx, "default")
		if !ok {
			return &spi.Response{Output: map[string]any{"ExpiryEvents": map[string]any{"DaysBeforeExpiry": 45}}}, nil
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: rec}, nil
	case "CreateAcmeDomainValidation", "CreateAcmeEndpoint", "CreateAcmeExternalAccountBinding":
		return p.acmeCreate(ctx, req)
	case "DescribeAcmeAccount", "DescribeAcmeDomainValidation", "DescribeAcmeEndpoint", "DescribeAcmeExternalAccountBinding":
		return p.acmeDescribe(ctx, req)
	case "ListAcmeAccounts", "ListAcmeDomainValidations", "ListAcmeEndpoints", "ListAcmeExternalAccountBindings":
		return p.acmeList(ctx, req)
	case "DeleteAcmeDomainValidation", "DeleteAcmeEndpoint", "DeleteAcmeExternalAccountBinding":
		return p.acmeDelete(ctx, req)
	case "UpdateAcmeDomainValidation", "UpdateAcmeEndpoint":
		return p.acmeUpdate(ctx, req)
	case "RevokeAcmeAccount", "RevokeAcmeExternalAccountBinding":
		return p.acmeRevoke(ctx, req)
	case "GetAcmeExternalAccountBindingCredentials":
		return p.acmeEABCreds(ctx, req)
	default:
		return nil, spi.NotImplemented("aws.acm", req.Operation, "emulate")
	}
}

func (p *Pack) patchCert(ctx context.Context, req *spi.Request, patch map[string]any) (*spi.Response, error) {
	id := certID(first(req.Input, "CertificateArn"))
	b, ok, _ := p.col(req).Get(ctx, id)
	if !ok {
		return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	for k, v := range patch {
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req).Put(ctx, id, nb)
	return &spi.Response{Output: map[string]any{"CertificateArn": rec["CertificateArn"], "Certificate": rec}}, nil
}

func acmeKind(op string) string {
	switch {
	case strings.Contains(op, "DomainValidation"):
		return "dv"
	case strings.Contains(op, "Endpoint"):
		return "ep"
	case strings.Contains(op, "ExternalAccount"):
		return "eab"
	default:
		return "acct"
	}
}

func acmeID(req *spi.Request, kind string) string {
	switch kind {
	case "dv":
		return first(req.Input, "DomainValidationArn", "DomainName")
	case "ep":
		return first(req.Input, "EndpointArn", "EndpointName")
	case "eab":
		return first(req.Input, "ExternalAccountBindingArn", "KeyId")
	default:
		return first(req.Input, "AccountArn", "AccountId")
	}
}

func (p *Pack) acmeCreate(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	id := p.deps.Rand.Hex(8)
	arn := "arn:aws:acm:" + req.Identity.Region + ":" + req.Identity.Account + ":acme/" + kind + "/" + id
	rec := map[string]any{"Arn": arn, "Kind": kind, "Status": "ACTIVE"}
	for k, v := range req.Input {
		rec[k] = v
	}
	b, _ := json.Marshal(rec)
	_ = p.extraCol(req, "acme").Put(ctx, kind+":"+id, b)
	out := map[string]any{"Arn": arn}
	switch kind {
	case "dv":
		out["DomainValidationArn"] = arn
	case "ep":
		out["EndpointArn"] = arn
	case "eab":
		out["ExternalAccountBindingArn"] = arn
		out["KeyId"] = id
	}
	return &spi.Response{Output: out}, nil
}

func (p *Pack) acmeDescribe(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	if kind == "acct" {
		return p.acmeAccount(ctx, req)
	}
	id := certID(acmeID(req, kind))
	b, ok, _ := p.extraCol(req, "acme").Get(ctx, kind+":"+id)
	if !ok {
		kvs, _, _ := p.extraCol(req, "acme").List(ctx, kind+":", "", 0)
		if len(kvs) == 0 {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 400, Fault: "client"}
		}
		b = kvs[0].Value
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) acmeAccount(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	col := p.extraCol(req, "acme")
	b, ok, _ := col.Get(ctx, "acct:default")
	if !ok {
		rec := map[string]any{"AccountArn": "arn:aws:acm:" + req.Identity.Region + ":" + req.Identity.Account + ":acme/acct/default", "Status": "ACTIVE"}
		nb, _ := json.Marshal(rec)
		_ = col.Put(ctx, "acct:default", nb)
		return &spi.Response{Output: rec}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) acmeList(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	if kind == "acct" {
		out, _ := p.acmeAccount(ctx, req)
		return &spi.Response{Output: map[string]any{"Accounts": []any{out.Output}}}, nil
	}
	kvs, _, _ := p.extraCol(req, "acme").List(ctx, kind+":", "", 0)
	var items []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	key := "Items"
	switch kind {
	case "dv":
		key = "DomainValidations"
	case "ep":
		key = "Endpoints"
	case "eab":
		key = "ExternalAccountBindings"
	}
	return &spi.Response{Output: map[string]any{key: items}}, nil
}

func (p *Pack) acmeDelete(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	id := certID(acmeID(req, kind))
	_ = p.extraCol(req, "acme").Delete(ctx, kind+":"+id)
	return &spi.Response{Output: map[string]any{}}, nil
}

func (p *Pack) acmeUpdate(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	id := certID(acmeID(req, kind))
	col := p.extraCol(req, "acme")
	b, ok, _ := col.Get(ctx, kind+":"+id)
	rec := map[string]any{"Kind": kind}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	for k, v := range req.Input {
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = col.Put(ctx, kind+":"+id, nb)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) acmeRevoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	kind := acmeKind(req.Operation)
	key := kind + ":" + certID(acmeID(req, kind))
	if kind == "acct" {
		key = "acct:default"
	}
	col := p.extraCol(req, "acme")
	b, ok, _ := col.Get(ctx, key)
	rec := map[string]any{"Status": "REVOKED"}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	rec["Status"] = "REVOKED"
	nb, _ := json.Marshal(rec)
	_ = col.Put(ctx, key, nb)
	return &spi.Response{Output: rec}, nil
}

func (p *Pack) acmeEABCreds(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	id := certID(first(req.Input, "ExternalAccountBindingArn", "KeyId"))
	b, ok, _ := p.extraCol(req, "acme").Get(ctx, "eab:"+id)
	kid := id
	if ok {
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		if s := first(rec, "KeyId"); s != "" {
			kid = s
		}
	}
	return &spi.Response{Output: map[string]any{"KeyId": kid, "MacKey": p.deps.Rand.Derive("eab/" + kid).Hex(32)}}, nil
}
