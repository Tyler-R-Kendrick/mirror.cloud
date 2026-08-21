package eks

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"ActivateCertificateAuthority", "AssociateAccessPolicy", "AssociateEncryptionConfig", "AssociateIdentityProviderConfig",
		"CancelUpdate", "CreateAccessEntry", "CreateAddon", "CreateCapability",
		"CreateCertificateAuthority", "CreateEksAnywhereSubscription", "CreatePodIdentityAssociation", "DeleteAccessEntry",
		"DeleteAddon", "DeleteCapability", "DeleteCertificateAuthority", "DeleteEksAnywhereSubscription",
		"DeletePodIdentityAssociation", "DeregisterCluster", "DescribeAccessEntry", "DescribeAddon",
		"DescribeAddonConfiguration", "DescribeAddonVersions", "DescribeCapability", "DescribeCertificateAuthority",
		"DescribeClusterVersions", "DescribeEksAnywhereSubscription", "DescribeIdentityProviderConfig", "DescribeInsight",
		"DescribeInsightsRefresh", "DescribePodIdentityAssociation", "DescribeUpdate", "DisassociateAccessPolicy",
		"DisassociateIdentityProviderConfig", "ListAccessEntries", "ListAccessPolicies", "ListAddons",
		"ListAssociatedAccessPolicies", "ListCapabilities", "ListCertificateAuthorities", "ListEksAnywhereSubscriptions",
		"ListIdentityProviderConfigs", "ListInsights", "ListPodIdentityAssociations", "ListUpdates",
		"RegisterCluster", "StartInsightsRefresh", "UpdateAccessEntry", "UpdateAddon",
		"UpdateCapability", "UpdateClusterVersion", "UpdateEksAnywhereSubscription", "UpdateNodegroupConfig",
		"UpdateNodegroupVersion", "UpdatePodIdentityAssociation",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	cluster := first(req.Input, "clusterName", "name")
	switch op {
	case "CreateAddon", "UpdateAddon":
		name := first(req.Input, "addonName")
		rec := map[string]any{"addonName": name, "clusterName": cluster, "status": "ACTIVE", "addonVersion": first(req.Input, "addonVersion")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksaddon").Put(ctx, cluster+"/"+name, b)
		return &spi.Response{Output: map[string]any{"addon": rec}}, nil
	case "DescribeAddon":
		return p.wrapGet(ctx, req, "eksaddon", cluster+"/"+first(req.Input, "addonName"), "addon")
	case "ListAddons":
		return p.listNames(ctx, req, "eksaddon", cluster+"/", "addons")
	case "DeleteAddon":
		_ = p.col(req, "eksaddon").Delete(ctx, cluster+"/"+first(req.Input, "addonName"))
		return &spi.Response{Output: map[string]any{"addon": map[string]any{"status": "DELETING"}}}, nil
	case "DescribeAddonVersions":
		return &spi.Response{Output: map[string]any{"addons": []any{map[string]any{"addonName": first(req.Input, "addonName"), "addonVersions": []any{map[string]any{"addonVersion": "v1"}}}}}}, nil
	case "DescribeAddonConfiguration":
		return &spi.Response{Output: map[string]any{"addonName": first(req.Input, "addonName"), "configurationSchema": "{}"}}, nil
	case "CreateAccessEntry", "UpdateAccessEntry":
		principal := first(req.Input, "principalArn")
		rec := map[string]any{"principalArn": principal, "clusterName": cluster, "type": first(req.Input, "type")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksaccess").Put(ctx, cluster+"/"+principal, b)
		return &spi.Response{Output: map[string]any{"accessEntry": rec}}, nil
	case "DescribeAccessEntry":
		return p.wrapGet(ctx, req, "eksaccess", cluster+"/"+first(req.Input, "principalArn"), "accessEntry")
	case "ListAccessEntries":
		return p.listNames(ctx, req, "eksaccess", cluster+"/", "accessEntries")
	case "DeleteAccessEntry":
		_ = p.col(req, "eksaccess").Delete(ctx, cluster+"/"+first(req.Input, "principalArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AssociateAccessPolicy":
		key := cluster + "/" + first(req.Input, "principalArn") + "/" + first(req.Input, "policyArn")
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "eksapol").Put(ctx, key, b)
		return &spi.Response{Output: map[string]any{"associatedAccessPolicy": req.Input}}, nil
	case "DisassociateAccessPolicy":
		_ = p.col(req, "eksapol").Delete(ctx, cluster+"/"+first(req.Input, "principalArn")+"/"+first(req.Input, "policyArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListAssociatedAccessPolicies":
		return p.listWrap(ctx, req, "eksapol", "associatedAccessPolicies")
	case "ListAccessPolicies":
		return &spi.Response{Output: map[string]any{"accessPolicies": []any{map[string]any{"name": "AmazonEKSAdminPolicy", "arn": "arn:aws:eks::aws:cluster-access-policy/AmazonEKSAdminPolicy"}}}}, nil
	case "CreateCertificateAuthority":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "status": "PENDING_CERTIFICATE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksca").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"certificateAuthority": rec}}, nil
	case "ActivateCertificateAuthority":
		id := first(req.Input, "id", "certificateAuthorityId")
		return p.patch(ctx, req, "eksca", id, map[string]any{"status": "ACTIVE"}, "certificateAuthority")
	case "DescribeCertificateAuthority":
		return p.wrapGet(ctx, req, "eksca", first(req.Input, "id", "certificateAuthorityId"), "certificateAuthority")
	case "ListCertificateAuthorities":
		return p.listWrap(ctx, req, "eksca", "certificateAuthorities")
	case "DeleteCertificateAuthority":
		_ = p.col(req, "eksca").Delete(ctx, first(req.Input, "id", "certificateAuthorityId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateEksAnywhereSubscription", "UpdateEksAnywhereSubscription":
		id := first(req.Input, "id", "name")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"id": id, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ekssub").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"subscription": rec}}, nil
	case "DescribeEksAnywhereSubscription":
		return p.wrapGet(ctx, req, "ekssub", first(req.Input, "id", "name"), "subscription")
	case "ListEksAnywhereSubscriptions":
		return p.listWrap(ctx, req, "ekssub", "subscriptions")
	case "DeleteEksAnywhereSubscription":
		_ = p.col(req, "ekssub").Delete(ctx, first(req.Input, "id", "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreatePodIdentityAssociation", "UpdatePodIdentityAssociation":
		id := first(req.Input, "associationId")
		if id == "" {
			id = p.deps.Rand.Hex(8)
		}
		rec := map[string]any{"associationId": id, "clusterName": cluster, "namespace": first(req.Input, "namespace"), "serviceAccount": first(req.Input, "serviceAccount")}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ekspodid").Put(ctx, cluster+"/"+id, b)
		return &spi.Response{Output: map[string]any{"association": rec}}, nil
	case "DescribePodIdentityAssociation":
		return p.wrapGet(ctx, req, "ekspodid", cluster+"/"+first(req.Input, "associationId"), "association")
	case "ListPodIdentityAssociations":
		return p.listWrap(ctx, req, "ekspodid", "associations")
	case "DeletePodIdentityAssociation":
		_ = p.col(req, "ekspodid").Delete(ctx, cluster+"/"+first(req.Input, "associationId"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "CreateCapability", "UpdateCapability":
		name := first(req.Input, "capabilityName", "name")
		rec := map[string]any{"name": name, "clusterName": cluster, "status": "ACTIVE"}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ekscap").Put(ctx, cluster+"/"+name, b)
		return &spi.Response{Output: map[string]any{"capability": rec}}, nil
	case "DescribeCapability":
		return p.wrapGet(ctx, req, "ekscap", cluster+"/"+first(req.Input, "capabilityName", "name"), "capability")
	case "ListCapabilities":
		return p.listWrap(ctx, req, "ekscap", "capabilities")
	case "DeleteCapability":
		_ = p.col(req, "ekscap").Delete(ctx, cluster+"/"+first(req.Input, "capabilityName", "name"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "AssociateEncryptionConfig":
		return p.patch(ctx, req, "ekscluster", cluster, map[string]any{"encryptionConfig": req.Input["encryptionConfig"]}, "cluster")
	case "AssociateIdentityProviderConfig":
		name := first(req.Input, "name")
		if name == "" {
			name = p.deps.Rand.Hex(8)
		}
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "eksidp").Put(ctx, cluster+"/"+name, b)
		return &spi.Response{Output: map[string]any{"update": p.newUpdate(ctx, req, "AssociateIdentityProviderConfig")}}, nil
	case "DisassociateIdentityProviderConfig":
		_ = p.col(req, "eksidp").Delete(ctx, cluster+"/"+first(req.Input, "identityProviderConfigName", "name"))
		return &spi.Response{Output: map[string]any{"update": p.newUpdate(ctx, req, "DisassociateIdentityProviderConfig")}}, nil
	case "DescribeIdentityProviderConfig":
		return p.wrapGet(ctx, req, "eksidp", cluster+"/"+first(req.Input, "identityProviderConfigName", "name"), "identityProviderConfig")
	case "ListIdentityProviderConfigs":
		return p.listWrap(ctx, req, "eksidp", "identityProviderConfigs")
	case "RegisterCluster":
		name := first(req.Input, "name")
		rec := map[string]any{"name": name, "status": "ACTIVE", "arn": "arn:aws:eks:" + req.Identity.Region + ":" + req.Identity.Account + ":cluster/" + name, "connectorConfig": req.Input["connectorConfig"]}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ekscluster").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"cluster": rec}}, nil
	case "DeregisterCluster":
		_ = p.col(req, "ekscluster").Delete(ctx, first(req.Input, "name"))
		return &spi.Response{Output: map[string]any{"cluster": map[string]any{"name": first(req.Input, "name"), "status": "DELETING"}}}, nil
	case "DescribeClusterVersions":
		return &spi.Response{Output: map[string]any{"clusterVersions": []any{map[string]any{"clusterVersion": "1.29", "status": "STANDARD_SUPPORT"}, map[string]any{"clusterVersion": "1.30", "status": "STANDARD_SUPPORT"}}}}, nil
	case "UpdateClusterVersion":
		ver := first(req.Input, "version")
		_, _ = p.patch(ctx, req, "ekscluster", cluster, map[string]any{"version": ver}, "cluster")
		return &spi.Response{Output: map[string]any{"update": p.newUpdate(ctx, req, "VersionUpdate")}}, nil
	case "UpdateNodegroupConfig", "UpdateNodegroupVersion":
		key := first(req.Input, "clusterName") + "/" + first(req.Input, "nodegroupName")
		fields := map[string]any{}
		if op == "UpdateNodegroupVersion" {
			fields["version"] = first(req.Input, "version")
		}
		if v := req.Input["scalingConfig"]; v != nil {
			fields["scalingConfig"] = v
		}
		_, _ = p.patch(ctx, req, "eksng", key, fields, "nodegroup")
		return &spi.Response{Output: map[string]any{"update": p.newUpdate(ctx, req, op)}}, nil
	case "DescribeUpdate":
		return p.wrapGet(ctx, req, "eksupdate", first(req.Input, "updateId", "id"), "update")
	case "ListUpdates":
		return p.listNames(ctx, req, "eksupdate", "", "updateIds")
	case "CancelUpdate":
		return p.patch(ctx, req, "eksupdate", first(req.Input, "updateId", "id"), map[string]any{"status": "Cancelled"}, "update")
	case "StartInsightsRefresh":
		id := p.deps.Rand.Hex(8)
		rec := map[string]any{"id": id, "status": "IN_PROGRESS", "clusterName": cluster}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksinsight").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"message": "started", "status": "IN_PROGRESS"}}, nil
	case "DescribeInsightsRefresh":
		return p.listWrap(ctx, req, "eksinsight", "insightRefresh")
	case "DescribeInsight":
		return p.wrapGet(ctx, req, "eksinsight", first(req.Input, "id"), "insight")
	case "ListInsights":
		return p.listWrap(ctx, req, "eksinsight", "insights")
	default:
		return nil, spi.NotImplemented("aws.eks", op, "emulate")
	}
}

func (p *Pack) newUpdate(ctx context.Context, req *spi.Request, kind string) map[string]any {
	id := p.deps.Rand.Hex(8)
	rec := map[string]any{"id": id, "status": "Successful", "type": kind}
	b, _ := json.Marshal(rec)
	_ = p.col(req, "eksupdate").Put(ctx, id, b)
	return rec
}

func (p *Pack) wrapGet(ctx context.Context, req *spi.Request, col, id, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	if !ok {
		return &spi.Response{Output: map[string]any{wrap: map[string]any{"name": id}}}, nil
	}
	var rec map[string]any
	_ = json.Unmarshal(b, &rec)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func (p *Pack) listWrap(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func (p *Pack) listNames(ctx context.Context, req *spi.Request, col, prefix, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, prefix, "", 0)
	var names []any
	for _, kv := range kvs {
		n := kv.Key
		if i := lastSlashIdx(n); i >= 0 {
			n = n[i+1:]
		}
		names = append(names, n)
	}
	return &spi.Response{Output: map[string]any{key: names}}, nil
}

func (p *Pack) patch(ctx context.Context, req *spi.Request, col, id string, fields map[string]any, wrap string) (*spi.Response, error) {
	b, ok, _ := p.col(req, col).Get(ctx, id)
	rec := map[string]any{}
	if ok {
		_ = json.Unmarshal(b, &rec)
	}
	for k, v := range fields {
		rec[k] = v
	}
	nb, _ := json.Marshal(rec)
	_ = p.col(req, col).Put(ctx, id, nb)
	return &spi.Response{Output: map[string]any{wrap: rec}}, nil
}

func lastSlashIdx(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}
