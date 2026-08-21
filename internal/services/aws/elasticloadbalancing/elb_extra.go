package elasticloadbalancing

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateRule":
		id := p.deps.Rand.Hex(8)
		arn := first(req.Input, "ListenerArn") + "/rule/" + id
		rec := map[string]any{"RuleArn": arn, "ListenerArn": first(req.Input, "ListenerArn"), "Priority": req.Input["Priority"], "Conditions": req.Input["Conditions"], "Actions": req.Input["Actions"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "elbrules").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Rules": []any{rec}}}, nil
	case "DescribeRules":
		want := first(req.Input, "ListenerArn")
		rule := first(req.Input, "RuleArns")
		kvs, _, _ := p.col(req, "elbrules").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			if rule != "" && rec["RuleArn"] != rule {
				continue
			}
			if want != "" && rec["ListenerArn"] != want {
				continue
			}
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"Rules": out}}, nil
	case "ModifyRule":
		arn := first(req.Input, "RuleArn")
		return p.patch(ctx, req, "elbrules", arn, "Rules")
	case "DeleteRule":
		_ = p.col(req, "elbrules").Delete(ctx, first(req.Input, "RuleArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "SetRulePriorities":
		prios := req.Input["RulePriorities"]
		return &spi.Response{Output: map[string]any{"Rules": prios}}, nil
	case "AddListenerCertificates":
		arn := first(req.Input, "ListenerArn")
		certs := req.Input["Certificates"]
		b, _ := json.Marshal(certs)
		_ = p.col(req, "lcerts").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Certificates": certs}}, nil
	case "DescribeListenerCertificates":
		arn := first(req.Input, "ListenerArn")
		b, ok, _ := p.col(req, "lcerts").Get(ctx, arn)
		var certs any = []any{}
		if ok {
			_ = json.Unmarshal(b, &certs)
		}
		return &spi.Response{Output: map[string]any{"Certificates": certs}}, nil
	case "RemoveListenerCertificates":
		_ = p.col(req, "lcerts").Delete(ctx, first(req.Input, "ListenerArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTrustStore":
		name := first(req.Input, "Name")
		arn := "arn:aws:elasticloadbalancing:" + req.Identity.Region + ":" + req.Identity.Account + ":truststore/" + name + "/" + p.deps.Rand.Hex(8)
		rec := map[string]any{"Name": name, "TrustStoreArn": arn, "Status": "ACTIVE", "CaCertificatesBundleS3Bucket": req.Input["CaCertificatesBundleS3Bucket"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "tstore").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"TrustStores": []any{rec}}}, nil
	case "DescribeTrustStores":
		kvs, _, _ := p.col(req, "tstore").List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"TrustStores": out}}, nil
	case "ModifyTrustStore":
		return p.patch(ctx, req, "tstore", first(req.Input, "TrustStoreArn"), "TrustStores")
	case "DeleteTrustStore":
		_ = p.col(req, "tstore").Delete(ctx, first(req.Input, "TrustStoreArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AddTrustStoreRevocations":
		arn := first(req.Input, "TrustStoreArn")
		b, _ := json.Marshal(req.Input["RevocationContents"])
		_ = p.col(req, "tsrev").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"TrustStoreRevocations": req.Input["RevocationContents"]}}, nil
	case "DescribeTrustStoreRevocations":
		arn := first(req.Input, "TrustStoreArn")
		b, ok, _ := p.col(req, "tsrev").Get(ctx, arn)
		var rev any = []any{}
		if ok {
			_ = json.Unmarshal(b, &rev)
		}
		return &spi.Response{Output: map[string]any{"TrustStoreRevocations": rev}}, nil
	case "RemoveTrustStoreRevocations":
		_ = p.col(req, "tsrev").Delete(ctx, first(req.Input, "TrustStoreArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeTrustStoreAssociations":
		return &spi.Response{Output: map[string]any{"TrustStoreAssociations": []any{}}}, nil
	case "DeleteSharedTrustStoreAssociation":
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTrustStoreCaCertificatesBundle":
		return &spi.Response{Output: map[string]any{"Location": "s3://mirror/ca.pem"}}, nil
	case "GetTrustStoreRevocationContent":
		return &spi.Response{Output: map[string]any{"Location": "s3://mirror/rev.crl"}}, nil
	case "DescribeListenerAttributes":
		return p.getJSON(ctx, req, "lattr", first(req.Input, "ListenerArn"), "Attributes")
	case "ModifyListenerAttributes":
		arn := first(req.Input, "ListenerArn")
		b, _ := json.Marshal(req.Input["Attributes"])
		_ = p.col(req, "lattr").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Attributes": req.Input["Attributes"]}}, nil
	case "ModifyListener":
		return p.patch(ctx, req, "listener", first(req.Input, "ListenerArn"), "Listeners")
	case "DescribeTargetGroupAttributes":
		return p.getJSON(ctx, req, "tgattr", first(req.Input, "TargetGroupArn"), "Attributes")
	case "ModifyTargetGroupAttributes":
		arn := first(req.Input, "TargetGroupArn")
		b, _ := json.Marshal(req.Input["Attributes"])
		_ = p.col(req, "tgattr").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"Attributes": req.Input["Attributes"]}}, nil
	case "DescribeSSLPolicies":
		return &spi.Response{Output: map[string]any{"SslPolicies": []any{map[string]any{"Name": "ELBSecurityPolicy-2016-08", "SslProtocols": []any{"TLSv1.2"}}}}}, nil
	case "DescribeAccountLimits":
		return &spi.Response{Output: map[string]any{"Limits": []any{map[string]any{"Name": "application-load-balancers", "Max": "1000"}}}}, nil
	case "DescribeCapacityReservation":
		return p.getJSON(ctx, req, "elbcap", first(req.Input, "LoadBalancerArn"), "CapacityReservationState")
	case "ModifyCapacityReservation":
		arn := first(req.Input, "LoadBalancerArn")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "elbcap").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"CapacityReservationState": req.Input}}, nil
	case "ModifyIpPools":
		arn := first(req.Input, "LoadBalancerArn")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "elbip").Put(ctx, arn, b)
		return &spi.Response{Output: map[string]any{"IpamPools": req.Input["IpamPools"]}}, nil
	case "GetResourcePolicy":
		b, ok, _ := p.col(req, "elbpol").Get(ctx, first(req.Input, "ResourceArn"))
		if !ok {
			return &spi.Response{Output: map[string]any{"Policy": ""}}, nil
		}
		return &spi.Response{Output: map[string]any{"Policy": string(b)}}, nil
	case "SetIpAddressType", "SetSecurityGroups", "SetSubnets":
		arn := first(req.Input, "LoadBalancerArn")
		name := string(mustGet(p.col(req, "elbarn"), ctx, arn))
		if name == "" {
			name = arn
		}
		return p.patch(ctx, req, "elb", name, "LoadBalancers")
	default:
		return nil, spi.NotImplemented("aws.elasticloadbalancing", req.Operation, "emulate")
	}
}

func (p *Pack) getJSON(ctx context.Context, req *spi.Request, col, key, out string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, key)
	var v any = []any{}
	if ok {
		_ = json.Unmarshal(b, &v)
	}
	return &spi.Response{Output: map[string]any{out: v}}, nil
}
