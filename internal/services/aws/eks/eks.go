// Package eks emulates cluster and nodegroup records (no Kubernetes API).
package eks

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.eks", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EKS-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.eks" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	core := []string{
		"CreateCluster", "DescribeCluster", "ListClusters", "DeleteCluster", "UpdateClusterConfig",
		"CreateNodegroup", "DescribeNodegroup", "ListNodegroups", "DeleteNodegroup",
		"CreateFargateProfile", "DescribeFargateProfile", "ListFargateProfiles", "DeleteFargateProfile",
		"ListTagsForResource", "TagResource", "UntagResource",
	}
	return append(core, extraOps()...)
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	if req.HTTP != nil {
		req.Operation = route(req)
		p.fill(req)
	}
	acct, region := req.Identity.Account, req.Identity.Region
	switch req.Operation {
	case "CreateCluster":
		name := first(req.Input, "name")
		arn := "arn:aws:eks:" + region + ":" + acct + ":cluster/" + name
		rec := map[string]any{"name": name, "arn": arn, "status": "ACTIVE", "version": first(req.Input, "version"), "roleArn": first(req.Input, "roleArn")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "ekscluster").Put(ctx, name, b)
		return &spi.Response{Output: map[string]any{"cluster": rec}}, nil
	case "DescribeCluster":
		name := first(req.Input, "name")
		b, ok, _ := p.col(req, "ekscluster").Get(ctx, name)
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"cluster": rec}}, nil
	case "ListClusters":
		kvs, _, _ := p.col(req, "ekscluster").List(ctx, "", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, kv.Key)
		}
		return &spi.Response{Output: map[string]any{"clusters": names}}, nil
	case "DeleteCluster":
		name := first(req.Input, "name")
		_ = p.col(req, "ekscluster").Delete(ctx, name)
		return &spi.Response{Output: map[string]any{"cluster": map[string]any{"name": name, "status": "DELETING"}}}, nil
	case "UpdateClusterConfig":
		name := first(req.Input, "name")
		b, ok, _ := p.col(req, "ekscluster").Get(ctx, name)
		rec := map[string]any{"name": name}
		if ok {
			_ = json.Unmarshal(b, &rec)
		}
		nb, _ := json.Marshal(rec)
		_ = p.col(req, "ekscluster").Put(ctx, name, nb)
		return &spi.Response{Output: map[string]any{"update": map[string]any{"id": p.deps.Rand.Hex(8), "status": "Successful"}}}, nil
	case "CreateNodegroup":
		cluster, ng := first(req.Input, "clusterName"), first(req.Input, "nodegroupName")
		rec := map[string]any{"nodegroupName": ng, "clusterName": cluster, "status": "ACTIVE", "nodegroupArn": "arn:aws:eks:" + region + ":" + acct + ":nodegroup/" + cluster + "/" + ng}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksng").Put(ctx, cluster+"/"+ng, b)
		return &spi.Response{Output: map[string]any{"nodegroup": rec}}, nil
	case "DescribeNodegroup":
		b, ok, _ := p.col(req, "eksng").Get(ctx, first(req.Input, "clusterName")+"/"+first(req.Input, "nodegroupName"))
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"nodegroup": rec}}, nil
	case "ListNodegroups":
		cluster := first(req.Input, "clusterName")
		kvs, _, _ := p.col(req, "eksng").List(ctx, cluster+"/", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, strings.TrimPrefix(kv.Key, cluster+"/"))
		}
		return &spi.Response{Output: map[string]any{"nodegroups": names}}, nil
	case "DeleteNodegroup":
		_ = p.col(req, "eksng").Delete(ctx, first(req.Input, "clusterName")+"/"+first(req.Input, "nodegroupName"))
		return &spi.Response{Output: map[string]any{"nodegroup": map[string]any{"status": "DELETING"}}}, nil
	case "CreateFargateProfile":
		cluster, fp := first(req.Input, "clusterName"), first(req.Input, "fargateProfileName")
		rec := map[string]any{"fargateProfileName": fp, "clusterName": cluster, "status": "ACTIVE"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "eksfp").Put(ctx, cluster+"/"+fp, b)
		return &spi.Response{Output: map[string]any{"fargateProfile": rec}}, nil
	case "DescribeFargateProfile":
		b, ok, _ := p.col(req, "eksfp").Get(ctx, first(req.Input, "clusterName")+"/"+first(req.Input, "fargateProfileName"))
		if !ok {
			return nil, &spi.Fault{Code: "ResourceNotFoundException", HTTPStatus: 404, Fault: "client"}
		}
		var rec map[string]any
		_ = json.Unmarshal(b, &rec)
		return &spi.Response{Output: map[string]any{"fargateProfile": rec}}, nil
	case "ListFargateProfiles":
		cluster := first(req.Input, "clusterName")
		kvs, _, _ := p.col(req, "eksfp").List(ctx, cluster+"/", "", 0)
		var names []any
		for _, kv := range kvs {
			names = append(names, strings.TrimPrefix(kv.Key, cluster+"/"))
		}
		return &spi.Response{Output: map[string]any{"fargateProfileNames": names}}, nil
	case "DeleteFargateProfile":
		_ = p.col(req, "eksfp").Delete(ctx, first(req.Input, "clusterName")+"/"+first(req.Input, "fargateProfileName"))
		return &spi.Response{Output: map[string]any{"fargateProfile": map[string]any{"status": "DELETING"}}}, nil
	case "TagResource":
		b, _ := json.Marshal(req.Input["tags"])
		_ = p.col(req, "ekstags").Put(ctx, first(req.Input, "resourceArn"), b)
		return &spi.Response{Output: map[string]any{}}, nil
	case "ListTagsForResource":
		b, ok, _ := p.col(req, "ekstags").Get(ctx, first(req.Input, "resourceArn"))
		var tags any = map[string]any{}
		if ok {
			_ = json.Unmarshal(b, &tags)
		}
		return &spi.Response{Output: map[string]any{"tags": tags}}, nil
	case "UntagResource":
		_ = p.col(req, "ekstags").Delete(ctx, first(req.Input, "resourceArn"))
		return &spi.Response{Output: map[string]any{}}, nil
	default:
		return p.extra(ctx, req)
	}
}

func (p *Pack) fill(req *spi.Request) {
	if req.Input == nil {
		req.Input = map[string]any{}
	}
	parts := strings.Split(strings.Trim(req.HTTP.URL.Path, "/"), "/")
	// clusters/{name}/node-groups/{ng}
	if len(parts) >= 2 && parts[0] == "clusters" {
		req.Input["name"] = parts[1]
		req.Input["clusterName"] = parts[1]
	}
	if len(parts) >= 4 && parts[2] == "node-groups" {
		req.Input["nodegroupName"] = parts[3]
	}
	if len(parts) >= 4 && parts[2] == "fargate-profiles" {
		req.Input["fargateProfileName"] = parts[3]
	}
}

func route(req *spi.Request) string {
	if a := req.HTTP.URL.Query().Get("Action"); a != "" {
		return a
	}
	path, m := req.HTTP.URL.Path, req.HTTP.Method
	switch {
	case strings.Contains(path, "/node-groups") && m == http.MethodPost:
		return "CreateNodegroup"
	case strings.Contains(path, "/node-groups") && strings.Count(path, "/") >= 4 && m == http.MethodGet:
		return "DescribeNodegroup"
	case strings.Contains(path, "/node-groups") && m == http.MethodGet:
		return "ListNodegroups"
	case strings.Contains(path, "/node-groups") && m == http.MethodDelete:
		return "DeleteNodegroup"
	case strings.Contains(path, "/fargate-profiles") && m == http.MethodPost:
		return "CreateFargateProfile"
	case strings.Contains(path, "/fargate-profiles") && strings.Count(path, "/") >= 4 && m == http.MethodGet:
		return "DescribeFargateProfile"
	case strings.Contains(path, "/fargate-profiles") && m == http.MethodGet:
		return "ListFargateProfiles"
	case strings.Contains(path, "/fargate-profiles") && m == http.MethodDelete:
		return "DeleteFargateProfile"
	case strings.Contains(path, "/tags") && m == http.MethodPost:
		return "TagResource"
	case strings.Contains(path, "/tags") && m == http.MethodGet:
		return "ListTagsForResource"
	case strings.Contains(path, "/tags") && m == http.MethodDelete:
		return "UntagResource"
	case path == "/clusters" && m == http.MethodPost:
		return "CreateCluster"
	case path == "/clusters" && m == http.MethodGet:
		return "ListClusters"
	case strings.HasPrefix(path, "/clusters/") && m == http.MethodGet:
		return "DescribeCluster"
	case strings.HasPrefix(path, "/clusters/") && m == http.MethodDelete:
		return "DeleteCluster"
	case strings.HasPrefix(path, "/clusters/") && m == http.MethodPost:
		return "UpdateClusterConfig"
	default:
		return req.Operation
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
