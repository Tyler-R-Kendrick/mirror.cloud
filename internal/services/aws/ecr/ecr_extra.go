package ecr

import (
	"context"
	"encoding/json"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func extraOps() []string {
	return []string{
		"BatchCheckLayerAvailability", "BatchGetRepositoryScanningConfiguration", "CompleteLayerUpload", "CreatePullThroughCacheRule",
		"CreateRepositoryCreationTemplate", "DeleteLifecyclePolicy", "DeletePullThroughCacheRule", "DeleteRegistryPolicy",
		"DeleteRepositoryCreationTemplate", "DeleteSigningConfiguration", "DeregisterPullTimeUpdateExclusion", "DescribeImageReplicationStatus",
		"DescribeImageScanFindings", "DescribeImageSigningStatus", "DescribeImages", "DescribePullThroughCacheRules",
		"DescribeRegistry", "DescribeRepositoryCreationTemplates", "GetAccountSetting", "GetDownloadUrlForLayer",
		"GetLifecyclePolicy", "GetLifecyclePolicyPreview", "GetRegistryPolicy", "GetRegistryScanningConfiguration",
		"GetSigningConfiguration", "InitiateLayerUpload", "ListImageReferrers", "ListPullTimeUpdateExclusions",
		"PutAccountSetting", "PutImageScanningConfiguration", "PutImageTagMutability", "PutLifecyclePolicy",
		"PutRegistryPolicy", "PutRegistryScanningConfiguration", "PutReplicationConfiguration", "PutSigningConfiguration",
		"RegisterPullTimeUpdateExclusion", "StartImageScan", "StartLifecyclePolicyPreview", "UpdateImageStorageClass",
		"UpdatePullThroughCacheRule", "UpdateRepositoryCreationTemplate", "UploadLayerPart", "ValidatePullThroughCacheRule",
	}
}

func (p *Pack) extra(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	op := req.Operation
	repo := first(req.Input, "repositoryName")
	switch op {
	case "InitiateLayerUpload":
		uid := p.deps.Rand.Hex(8)
		rec := map[string]any{"uploadId": uid, "repositoryName": repo, "parts": []any{}}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecrlayer").Put(ctx, uid, b)
		return &spi.Response{Output: map[string]any{"uploadId": uid, "partSize": 5242880}}, nil
	case "UploadLayerPart":
		uid := first(req.Input, "uploadId")
		b, ok, _ := p.col(req, "ecrlayer").Get(ctx, uid)
		rec := map[string]any{"uploadId": uid, "parts": []any{}}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		parts, _ := rec["parts"].([]any)
		parts = append(parts, map[string]any{
			"partFirstByte": req.Input["partFirstByte"], "partLastByte": req.Input["partLastByte"],
			"layerPartBlob": req.Input["layerPartBlob"],
		})
		rec["parts"] = parts
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecrlayer").Put(ctx, uid, nb)
		return &spi.Response{Output: map[string]any{"uploadId": uid, "lastByteReceived": req.Input["partLastByte"]}}, nil
	case "CompleteLayerUpload":
		uid := first(req.Input, "uploadId")
		digest := first(req.Input, "layerDigest")
		if digest == "" {
			digest = "sha256:" + p.deps.Rand.Hex(16)
		}
		b, ok, _ := p.col(req, "ecrlayer").Get(ctx, uid)
		rec := map[string]any{"uploadId": uid}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["layerDigest"] = digest
		rec["status"] = "COMPLETE"
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecrlayer").Put(ctx, uid, nb)
		_ = p.col(req, "ecrlayerdone:"+repo).Put(ctx, digest, nb)
		return &spi.Response{Output: map[string]any{"layerDigest": digest, "uploadId": uid, "registryId": req.Identity.Account}}, nil
	case "BatchCheckLayerAvailability":
		var layers []any
		for _, d := range asAny(req.Input["layerDigests"]) {
			ds := str(d)
			_, ok, _ := p.col(req, "ecrlayerdone:"+repo).Get(ctx, ds)
			layers = append(layers, map[string]any{"layerDigest": ds, "layerAvailability": map[bool]string{true: "AVAILABLE", false: "UNAVAILABLE"}[ok]})
		}
		return &spi.Response{Output: map[string]any{"layers": layers, "failures": []any{}}}, nil
	case "GetDownloadUrlForLayer":
		d := first(req.Input, "layerDigest")
		return &spi.Response{Output: map[string]any{"downloadUrl": "http://127.0.0.1/ecr-layers/" + d, "layerDigest": d}}, nil
	case "DescribeImages":
		kvs, _, _ := p.col(req, "ecrimg:"+repo).List(ctx, "", "", 0)
		var out []any
		for _, kv := range kvs {
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			out = append(out, rec)
		}
		return &spi.Response{Output: map[string]any{"imageDetails": out}}, nil
	case "DescribeImageScanFindings", "StartImageScan":
		tag := imageTag(req)
		findings := []any{}
		if op == "StartImageScan" {
			findings = []any{}
			b, _ := json.Marshal(map[string]any{"imageTag": tag, "status": "COMPLETE", "findings": findings})
			_ = p.col(req, "ecrscan:"+repo).Put(ctx, tag, b)
		} else {
			if b, ok, _ := p.col(req, "ecrscan:"+repo).Get(ctx, tag); ok {
				var rec map[string]any
				_ = json.Unmarshal(b, &rec)
				return &spi.Response{Output: map[string]any{"imageScanFindings": rec, "imageId": map[string]any{"imageTag": tag}}}, nil
			}
		}
		return &spi.Response{Output: map[string]any{
			"imageScanStatus":   map[string]any{"status": "COMPLETE"},
			"imageScanFindings": map[string]any{"findings": findings, "findingSeverityCounts": map[string]any{}},
			"imageId":           map[string]any{"imageTag": tag},
		}}, nil
	case "DescribeImageReplicationStatus", "DescribeImageSigningStatus":
		key := "replicationStatus"
		if op == "DescribeImageSigningStatus" {
			key = "signingStatus"
		}
		return &spi.Response{Output: map[string]any{key: "COMPLETE", "repositoryName": repo}}, nil
	case "ListImageReferrers":
		return &spi.Response{Output: map[string]any{"referrers": []any{}}}, nil
	case "UpdateImageStorageClass":
		tag := imageTag(req)
		b, ok, _ := p.col(req, "ecrimg:"+repo).Get(ctx, tag)
		rec := map[string]any{"imageTag": tag}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["imageStorageClass"] = first(req.Input, "targetStorageClass", "imageStorageClass")
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecrimg:"+repo).Put(ctx, tag, nb)
		return &spi.Response{Output: map[string]any{"image": rec}}, nil
	case "PutLifecyclePolicy":
		_ = p.col(req, "ecrlc").Put(ctx, repo, []byte(first(req.Input, "lifecyclePolicyText")))
		return &spi.Response{Output: map[string]any{"lifecyclePolicyText": first(req.Input, "lifecyclePolicyText"), "repositoryName": repo}}, nil
	case "GetLifecyclePolicy":
		b, ok, _ := p.col(req, "ecrlc").Get(ctx, repo)
		txt := ""
		if ok {
			txt = string(b)
		}
		return &spi.Response{Output: map[string]any{"lifecyclePolicyText": txt, "repositoryName": repo}}, nil
	case "DeleteLifecyclePolicy":
		_ = p.col(req, "ecrlc").Delete(ctx, repo)
		return &spi.Response{Output: map[string]any{}}, nil
	case "StartLifecyclePolicyPreview":
		id := p.deps.Rand.Hex(8)
		_ = p.col(req, "ecrlcprev").Put(ctx, repo, []byte(id))
		return &spi.Response{Output: map[string]any{"registryId": req.Identity.Account, "repositoryName": repo, "lifecyclePolicyText": first(req.Input, "lifecyclePolicyText"), "status": "COMPLETE"}}, nil
	case "GetLifecyclePolicyPreview":
		return &spi.Response{Output: map[string]any{"status": "COMPLETE", "previewResults": []any{}, "repositoryName": repo}}, nil
	case "CreatePullThroughCacheRule", "UpdatePullThroughCacheRule":
		pref := first(req.Input, "ecrRepositoryPrefix")
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecrptc").Put(ctx, pref, b)
		return &spi.Response{Output: rec}, nil
	case "DescribePullThroughCacheRules":
		return p.listCol(ctx, req, "ecrptc", "pullThroughCacheRules")
	case "DeletePullThroughCacheRule":
		_ = p.col(req, "ecrptc").Delete(ctx, first(req.Input, "ecrRepositoryPrefix"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ValidatePullThroughCacheRule":
		pref := first(req.Input, "ecrRepositoryPrefix")
		_, ok, _ := p.col(req, "ecrptc").Get(ctx, pref)
		return &spi.Response{Output: map[string]any{"ecrRepositoryPrefix": pref, "isValid": ok}}, nil
	case "CreateRepositoryCreationTemplate", "UpdateRepositoryCreationTemplate":
		pref := first(req.Input, "prefix")
		rec := map[string]any{}
		for k, v := range req.Input {
			rec[k] = v
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ecrtmpl").Put(ctx, pref, b)
		return &spi.Response{Output: map[string]any{"registryId": req.Identity.Account, "repositoryCreationTemplate": rec}}, nil
	case "DescribeRepositoryCreationTemplates":
		return p.listCol(ctx, req, "ecrtmpl", "repositoryCreationTemplates")
	case "DeleteRepositoryCreationTemplate":
		_ = p.col(req, "ecrtmpl").Delete(ctx, first(req.Input, "prefix"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "DescribeRegistry":
		return &spi.Response{Output: map[string]any{"registryId": req.Identity.Account, "replicationConfiguration": map[string]any{"rules": []any{}}}}, nil
	case "PutRegistryPolicy":
		_ = p.col(req, "ecrreg").Put(ctx, "policy", []byte(first(req.Input, "policyText")))
		return &spi.Response{Output: map[string]any{"registryId": req.Identity.Account, "policyText": first(req.Input, "policyText")}}, nil
	case "GetRegistryPolicy":
		b, ok, _ := p.col(req, "ecrreg").Get(ctx, "policy")
		txt := ""
		if ok {
			txt = string(b)
		}
		return &spi.Response{Output: map[string]any{"registryId": req.Identity.Account, "policyText": txt}}, nil
	case "DeleteRegistryPolicy":
		_ = p.col(req, "ecrreg").Delete(ctx, "policy")
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutRegistryScanningConfiguration":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "ecrreg").Put(ctx, "scan", b)
		return &spi.Response{Output: map[string]any{"registryScanningConfiguration": req.Input}}, nil
	case "GetRegistryScanningConfiguration":
		b, ok, _ := p.col(req, "ecrreg").Get(ctx, "scan")
		var rec any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: map[string]any{"registryScanningConfiguration": rec}}, nil
	case "PutReplicationConfiguration":
		b, _ := json.Marshal(req.Input["replicationConfiguration"])
		_ = p.col(req, "ecrreg").Put(ctx, "repl", b)
		return &spi.Response{Output: map[string]any{"replicationConfiguration": req.Input["replicationConfiguration"]}}, nil
	case "PutSigningConfiguration":
		b, _ := json.Marshal(req.Input)
		_ = p.col(req, "ecrreg").Put(ctx, "sign", b)
		return &spi.Response{Output: map[string]any{"signingConfiguration": req.Input}}, nil
	case "GetSigningConfiguration":
		b, ok, _ := p.col(req, "ecrreg").Get(ctx, "sign")
		var rec any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		return &spi.Response{Output: map[string]any{"signingConfiguration": rec}}, nil
	case "DeleteSigningConfiguration":
		_ = p.col(req, "ecrreg").Delete(ctx, "sign")
		return &spi.Response{Output: map[string]any{}}, nil
	case "PutAccountSetting":
		name := first(req.Input, "name")
		_ = p.col(req, "ecracct").Put(ctx, name, []byte(first(req.Input, "value")))
		return &spi.Response{Output: map[string]any{"name": name, "value": first(req.Input, "value")}}, nil
	case "GetAccountSetting":
		name := first(req.Input, "name")
		b, ok, _ := p.col(req, "ecracct").Get(ctx, name)
		val := ""
		if ok {
			val = string(b)
		}
		return &spi.Response{Output: map[string]any{"name": name, "value": val}}, nil
	case "PutImageScanningConfiguration":
		b, _ := json.Marshal(req.Input["imageScanningConfiguration"])
		_ = p.col(req, "ecrscan").Put(ctx, repo, b)
		return &spi.Response{Output: map[string]any{"repositoryName": repo, "imageScanningConfiguration": req.Input["imageScanningConfiguration"]}}, nil
	case "BatchGetRepositoryScanningConfiguration":
		var cfgs []any
		for _, n := range asAny(req.Input["repositoryNames"]) {
			b, ok, _ := p.col(req, "ecrscan").Get(ctx, str(n))
			cfg := map[string]any{"repositoryName": n}
			if ok {
				var sc any
				_ = json.Unmarshal(b, &sc)
				cfg["imageScanningConfiguration"] = sc
			}
			cfgs = append(cfgs, cfg)
		}
		return &spi.Response{Output: map[string]any{"scanningConfigurations": cfgs, "failures": []any{}}}, nil
	case "PutImageTagMutability":
		mut := first(req.Input, "imageTagMutability")
		b, ok, _ := p.col(req, "ecrrepo").Get(ctx, repo)
		rec := map[string]any{"repositoryName": repo}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		rec["imageTagMutability"] = mut
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ecrrepo").Put(ctx, repo, nb)
		return &spi.Response{Output: map[string]any{"repositoryName": repo, "imageTagMutability": mut}}, nil
	case "RegisterPullTimeUpdateExclusion":
		n := first(req.Input, "principalArn", "exclusion")
		_ = p.col(req, "ecrexcl").Put(ctx, n, []byte(n))
		return &spi.Response{Output: map[string]any{"principalArn": n}}, nil
	case "DeregisterPullTimeUpdateExclusion":
		_ = p.col(req, "ecrexcl").Delete(ctx, first(req.Input, "principalArn", "exclusion"))
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListPullTimeUpdateExclusions":
		return p.listCol(ctx, req, "ecrexcl", "exclusions")
	default:
		return nil, spi.NotImplemented("aws.api.ecr", op, "emulate")
	}
}

func (p *Pack) listCol(ctx context.Context, req *spi.Request, col, key string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var out []any
	for _, kv := range kvs {
		var rec any
		if json.Unmarshal(kv.Value, &rec) != nil {
			rec = map[string]any{"id": kv.Key, "value": string(kv.Value)}
		}
		out = append(out, rec)
	}
	return &spi.Response{Output: map[string]any{key: out}}, nil
}

func imageTag(req *spi.Request) string {
	if t := first(req.Input, "imageTag"); t != "" {
		return t
	}
	if m, ok := req.Input["imageId"].(map[string]any); ok {
		if t := first(m, "imageTag"); t != "" {
			return t
		}
	}
	return "latest"
}

func asAny(v any) []any {
	a, _ := v.([]any)
	return a
}

func str(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return string(t)
	}
	return ""
}
