package route53

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"ActivateKeySigningKey", "AssociateVPCWithHostedZone", "ChangeCidrCollection", "ChangeTagsForResource",
		"CreateCidrCollection", "CreateHealthCheck", "CreateKeySigningKey", "CreateQueryLoggingConfig",
		"CreateReusableDelegationSet", "CreateTrafficPolicy", "CreateTrafficPolicyInstance",
		"CreateTrafficPolicyVersion", "CreateVPCAssociationAuthorization",
		"DeactivateKeySigningKey", "DeleteCidrCollection", "DeleteHealthCheck", "DeleteKeySigningKey",
		"DeleteQueryLoggingConfig", "DeleteReusableDelegationSet", "DeleteTrafficPolicy",
		"DeleteTrafficPolicyInstance", "DeleteVPCAssociationAuthorization",
		"DisableHostedZoneDNSSEC", "DisassociateVPCFromHostedZone", "EnableHostedZoneDNSSEC",
		"GetAccountLimit", "GetChange", "GetCheckerIpRanges", "GetDNSSEC", "GetGeoLocation",
		"GetHealthCheck", "GetHealthCheckCount", "GetHealthCheckLastFailureReason", "GetHealthCheckStatus",
		"GetHostedZoneCount", "GetHostedZoneLimit", "GetQueryLoggingConfig", "GetReusableDelegationSet",
		"GetReusableDelegationSetLimit", "GetTrafficPolicy", "GetTrafficPolicyInstance",
		"GetTrafficPolicyInstanceCount",
		"ListCidrBlocks", "ListCidrCollections", "ListCidrLocations", "ListGeoLocations", "ListHealthChecks",
		"ListHostedZonesByName", "ListHostedZonesByVPC", "ListQueryLoggingConfigs", "ListReusableDelegationSets",
		"ListTagsForResource", "ListTagsForResources", "ListTrafficPolicies", "ListTrafficPolicyInstances",
		"ListTrafficPolicyInstancesByHostedZone", "ListTrafficPolicyInstancesByPolicy", "ListTrafficPolicyVersions",
		"ListVPCAssociationAuthorizations", "TestDNSAnswer",
		"UpdateHealthCheck", "UpdateHostedZoneComment", "UpdateHostedZoneFeatures",
		"UpdateTrafficPolicyComment", "UpdateTrafficPolicyInstance",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	switch op {
	case "CreateHealthCheck", "UpdateHealthCheck":
		id := first(req.Input, "HealthCheckId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "HealthCheckConfig": map[string]any{"Type": "HTTP", "FullyQualifiedDomainName": first(req.Input, "Name", "FullyQualifiedDomainName")}}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["Id"] = id
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53hc").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"HealthCheck": rec}}, nil
	case "GetHealthCheck":
		return p.getWrap(ctx, req, "r53hc", first(req.Input, "HealthCheckId", "Id"), "HealthCheck")
	case "ListHealthChecks":
		return p.listCol(ctx, req, "r53hc", "HealthChecks")
	case "DeleteHealthCheck":
		_ = p.col(req, "r53hc").Delete(ctx, first(req.Input, "HealthCheckId", "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetHealthCheckStatus":
		return &spi.Response{Output: map[string]any{"HealthCheckObservations": []any{map[string]any{"StatusReport": map[string]any{"Status": "Success"}}}}}, nil
	case "GetHealthCheckLastFailureReason":
		return &spi.Response{Output: map[string]any{"HealthCheckObservations": []any{}}}, nil
	case "GetHealthCheckCount":
		kvs, _, _ := p.col(req, "r53hc").List(ctx, "", "", 0)
		return &spi.Response{Output: map[string]any{"HealthCheckCount": len(kvs)}}, nil
	case "CreateTrafficPolicy", "UpdateTrafficPolicyComment":
		id := first(req.Input, "Id")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name"), "Document": first(req.Input, "Document"), "Version": 1}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["Id"] = id
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53tp").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"TrafficPolicy": rec}}, nil
	case "CreateTrafficPolicyVersion":
		id := first(req.Input, "Id")
		b, ok, _ := p.col(req, "r53tp").Get(ctx, id)
		rec := map[string]any{"Id": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		n := 1
		if v, ok := rec["Version"].(float64); ok {
			n = int(v) + 1
		}
		rec["Version"] = n
		rec["Document"] = first(req.Input, "Document")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "r53tp").Put(ctx, id, nb)
		_ = p.col(req, "r53tpver").Put(ctx, id+":"+strconv.Itoa(n), nb)
		return &spi.Response{Output: map[string]any{"TrafficPolicy": rec}}, nil
	case "GetTrafficPolicy":
		return p.getWrap(ctx, req, "r53tp", first(req.Input, "Id"), "TrafficPolicy")
	case "ListTrafficPolicies":
		return p.listCol(ctx, req, "r53tp", "TrafficPolicySummaries")
	case "ListTrafficPolicyVersions":
		return p.listCol(ctx, req, "r53tpver", "TrafficPolicies")
	case "DeleteTrafficPolicy":
		_ = p.col(req, "r53tp").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateTrafficPolicyInstance", "UpdateTrafficPolicyInstance":
		id := first(req.Input, "Id")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "HostedZoneId": first(req.Input, "HostedZoneId"), "Name": first(req.Input, "Name"), "TrafficPolicyId": first(req.Input, "TrafficPolicyId"), "State": "Applied"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53tpi").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"TrafficPolicyInstance": rec}}, nil
	case "GetTrafficPolicyInstance":
		return p.getWrap(ctx, req, "r53tpi", first(req.Input, "Id"), "TrafficPolicyInstance")
	case "ListTrafficPolicyInstances", "ListTrafficPolicyInstancesByHostedZone", "ListTrafficPolicyInstancesByPolicy":
		return p.listCol(ctx, req, "r53tpi", "TrafficPolicyInstances")
	case "DeleteTrafficPolicyInstance":
		_ = p.col(req, "r53tpi").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "GetTrafficPolicyInstanceCount":
		kvs, _, _ := p.col(req, "r53tpi").List(ctx, "", "", 0)
		return &spi.Response{Output: map[string]any{"TrafficPolicyInstanceCount": len(kvs)}}, nil
	case "CreateQueryLoggingConfig":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "HostedZoneId": first(req.Input, "HostedZoneId"), "CloudWatchLogsLogGroupArn": first(req.Input, "CloudWatchLogsLogGroupArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53ql").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"QueryLoggingConfig": rec}}, nil
	case "GetQueryLoggingConfig":
		return p.getWrap(ctx, req, "r53ql", first(req.Input, "Id"), "QueryLoggingConfig")
	case "ListQueryLoggingConfigs":
		return p.listCol(ctx, req, "r53ql", "QueryLoggingConfigs")
	case "DeleteQueryLoggingConfig":
		_ = p.col(req, "r53ql").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCidrCollection", "ChangeCidrCollection":
		id := first(req.Input, "Id")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"Id": id, "Name": first(req.Input, "Name")}
		for k, v := range req.Input {
			rec[k] = v
		}
		rec["Id"] = id
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53cidr").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"Collection": rec}}, nil
	case "ListCidrCollections":
		return p.listCol(ctx, req, "r53cidr", "CidrCollections")
	case "ListCidrBlocks", "ListCidrLocations":
		return p.listCol(ctx, req, "r53cidr", strings.TrimPrefix(op, "List"))
	case "DeleteCidrCollection":
		_ = p.col(req, "r53cidr").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateReusableDelegationSet":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"Id": id, "NameServers": []any{"ns-1.mirror.local", "ns-2.mirror.local"}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53ds").Put(ctx, id, b)
		return &spi.Response{Status: 201, Output: map[string]any{"DelegationSet": rec}}, nil
	case "GetReusableDelegationSet":
		return p.getWrap(ctx, req, "r53ds", first(req.Input, "Id"), "DelegationSet")
	case "ListReusableDelegationSets":
		return p.listCol(ctx, req, "r53ds", "DelegationSets")
	case "DeleteReusableDelegationSet":
		_ = p.col(req, "r53ds").Delete(ctx, first(req.Input, "Id"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateKeySigningKey", "ActivateKeySigningKey", "DeactivateKeySigningKey":
		name := first(req.Input, "Name")
		st := "ACTIVE"
		if op == "DeactivateKeySigningKey" {
			st = "INACTIVE"
		}
		rec := map[string]any{"Name": name, "Status": st, "HostedZoneId": first(req.Input, "HostedZoneId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53ksk").Put(ctx, first(req.Input, "HostedZoneId")+"/"+name, b)
		return &spi.Response{Output: map[string]any{"KeySigningKey": rec, "ChangeInfo": map[string]any{"Status": "INSYNC"}}}, nil
	case "DeleteKeySigningKey":
		_ = p.col(req, "r53ksk").Delete(ctx, first(req.Input, "HostedZoneId")+"/"+first(req.Input, "Name"))
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Status": "INSYNC"}}}, nil
	case "EnableHostedZoneDNSSEC", "DisableHostedZoneDNSSEC":
		st := "ENABLED"
		if op == "DisableHostedZoneDNSSEC" {
			st = "DISABLED"
		}
		_ = p.col(req, "r53dnssec").Put(ctx, first(req.Input, "HostedZoneId", "Id"), []byte(st))
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Status": "INSYNC"}}}, nil
	case "GetDNSSEC":
		b, ok, _ := p.col(req, "r53dnssec").Get(ctx, first(req.Input, "HostedZoneId", "Id"))
		st := "DISABLED"
		if ok {
			st = string(b)
		}
		return &spi.Response{Output: map[string]any{"Status": map[string]any{"ServeSignature": st}}}, nil
	case "AssociateVPCWithHostedZone", "CreateVPCAssociationAuthorization":
		id := first(req.Input, "HostedZoneId", "Id") + "/" + first(req.Input, "VPCId")
		rec := map[string]any{"HostedZoneId": first(req.Input, "HostedZoneId", "Id"), "VPCId": first(req.Input, "VPCId")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "r53vpc").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Status": "INSYNC"}}}, nil
	case "DisassociateVPCFromHostedZone", "DeleteVPCAssociationAuthorization":
		_ = p.col(req, "r53vpc").Delete(ctx, first(req.Input, "HostedZoneId", "Id")+"/"+first(req.Input, "VPCId"))
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Status": "INSYNC"}}}, nil
	case "ListVPCAssociationAuthorizations", "ListHostedZonesByVPC":
		return p.listCol(ctx, req, "r53vpc", "VPCs")
	case "ChangeTagsForResource":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "r53tags").Put(ctx, first(req.Input, "ResourceId"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "r53tags").Get(ctx, first(req.Input, "ResourceId"))
		var rec any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: map[string]any{"ResourceTagSet": rec}}, nil
	case "ListTagsForResources":
		return p.listCol(ctx, req, "r53tags", "ResourceTagSets")
	case "GetChange":
		return &spi.Response{Output: map[string]any{"ChangeInfo": map[string]any{"Id": first(req.Input, "Id"), "Status": "INSYNC"}}}, nil
	case "GetCheckerIpRanges":
		return &spi.Response{Output: map[string]any{"CheckerIpRanges": []any{"127.0.0.1/32"}}}, nil
	case "GetGeoLocation":
		return &spi.Response{Output: map[string]any{"GeoLocationDetails": map[string]any{"CountryCode": first(req.Input, "CountryCode")}}}, nil
	case "ListGeoLocations":
		return &spi.Response{Output: map[string]any{"GeoLocationDetailsList": []any{map[string]any{"CountryCode": "US"}}}}, nil
	case "GetAccountLimit", "GetHostedZoneLimit", "GetReusableDelegationSetLimit":
		return &spi.Response{Output: map[string]any{"Limit": map[string]any{"Type": first(req.Input, "Type"), "Value": 500}, "Count": 0}}, nil
	case "GetHostedZoneCount":
		kvs, _, _ := p.col(req, "hz").List(ctx, "", "", 0)
		return &spi.Response{Output: map[string]any{"HostedZoneCount": len(kvs)}}, nil
	case "ListHostedZonesByName":
		return p.Invoke(ctx, &spi.Request{Identity: req.Identity, Operation: "ListHostedZones", Input: req.Input})
	case "UpdateHostedZoneComment", "UpdateHostedZoneFeatures":
		id := zoneID(first(req.Input, "Id", "HostedZoneId"))
		b, ok, _ := p.col(req, "hz").Get(ctx, id)
		rec := map[string]any{"Id": id}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		if c := first(req.Input, "Comment"); c != "" {
			rec["Comment"] = c
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "hz").Put(ctx, id, nb)
		return &spi.Response{Output: map[string]any{"HostedZone": rec}}, nil
	case "TestDNSAnswer":
		// ponytail: answers from in-memory RR sets, else 127.0.0.1. Not a recursive resolver.
		name, typ := first(req.Input, "RecordName", "Name"), first(req.Input, "RecordType", "Type")
		if typ == "" {
			typ = "A"
		}
		data := []any{"127.0.0.1"}
		kvs, _, _ := p.col(req, "hz").List(ctx, "", "", 0)
		for _, z := range kvs {
			rrs, _, _ := p.col(req, "rr:"+z.Key).List(ctx, "", "", 0)
			for _, kv := range rrs {
				var rec map[string]any
				_ = json.Unmarshal(kv.Value, &rec)
				if str(rec["Name"]) == name && (typ == "" || str(rec["Type"]) == typ) {
					if v := rec["ResourceRecords"]; v != nil {
						data = []any{}
						for _, rr := range asAny(v) {
							m, _ := rr.(map[string]any)
							data = append(data, str(m["Value"]))
						}
					}
				}
			}
		}
		return &spi.Response{Output: map[string]any{"RecordData": data, "ResponseCode": "NOERROR", "RecordName": name, "RecordType": typ}}, nil
	default:
		return nil, spi.NotImplemented("aws.route53", op, "emulate")
	}
}

func (p *Pack) getWrap(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) listCol(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := str(in[k]); s != "" {
			return s
		}
	}
	return ""
}

func asAny(v any) []any {
	a, _ := v.([]any)
	return a
}
