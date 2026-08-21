// Package ec2 is EC2 control-plane records (no hypervisor).
package ec2

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func init() {
	registry.Register(registry.Factory{ServiceID: "aws.ec2", Tier: model.TierEmulate, New: func(d spi.Deps) (spi.BehaviorPack, error) {
		return New(d), nil
	}})
}

// Pack implements EC2-lite.
type Pack struct{ deps spi.Deps }

// New constructs the pack.
func New(d spi.Deps) *Pack { return &Pack{deps: d} }

func (p *Pack) ServiceID() string { return "aws.ec2" }
func (p *Pack) Tier() model.Tier  { return model.TierEmulate }
func (p *Pack) Operations() []string {
	return []string{
		"CreateVpc", "DescribeVpcs", "DeleteVpc",
		"CreateSubnet", "DescribeSubnets", "DeleteSubnet",
		"CreateSecurityGroup", "DescribeSecurityGroups", "DeleteSecurityGroup",
		"RunInstances", "DescribeInstances", "TerminateInstances",
	}
}

func (p *Pack) col(req *spi.Request, n string) spi.Collection {
	return p.deps.Store.Scope(req.Identity.Account, req.Identity.Region).Collection(n)
}

func (p *Pack) Invoke(ctx context.Context, req *spi.Request) (*spi.Response, error) {
	switch req.Operation {
	case "CreateVpc":
		id := "vpc-" + p.deps.Rand.Hex(8)
		cidr := first(req.Input, "CidrBlock")
		if cidr == "" {
			cidr = "10.0.0.0/16"
		}
		rec := map[string]any{"VpcId": id, "CidrBlock": cidr, "State": "available"}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "vpc").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"vpc": rec}}, nil
	case "DescribeVpcs":
		return p.listNamed(ctx, req, "vpc", "vpcSet", first(req.Input, "VpcId", "VpcId.1"))
	case "DeleteVpc":
		id := first(req.Input, "VpcId")
		_ = p.col(req, "vpc").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"return": "true"}}, nil
	case "CreateSubnet":
		id := "subnet-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"SubnetId": id, "VpcId": first(req.Input, "VpcId"), "CidrBlock": first(req.Input, "CidrBlock"), "State": "available"}
		if rec["CidrBlock"] == "" {
			rec["CidrBlock"] = "10.0.1.0/24"
		}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "subnet").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"subnet": rec}}, nil
	case "DescribeSubnets":
		return p.listNamed(ctx, req, "subnet", "subnetSet", first(req.Input, "SubnetId", "SubnetId.1"))
	case "DeleteSubnet":
		_ = p.col(req, "subnet").Delete(ctx, first(req.Input, "SubnetId"))
		return &spi.Response{Output: map[string]any{"return": "true"}}, nil
	case "CreateSecurityGroup":
		id := "sg-" + p.deps.Rand.Hex(8)
		rec := map[string]any{"GroupId": id, "GroupName": first(req.Input, "GroupName"), "VpcId": first(req.Input, "VpcId"), "Description": first(req.Input, "GroupDescription", "Description")}
		b, _ := json.Marshal(rec)
		_ = p.col(req, "sg").Put(ctx, id, b)
		return &spi.Response{Output: map[string]any{"groupId": id, "securityGroup": rec}}, nil
	case "DescribeSecurityGroups":
		return p.listNamed(ctx, req, "sg", "securityGroupInfo", first(req.Input, "GroupId", "GroupId.1"))
	case "DeleteSecurityGroup":
		id := first(req.Input, "GroupId")
		if id == "" {
			id = first(req.Input, "GroupName")
		}
		_ = p.col(req, "sg").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"return": "true"}}, nil
	case "RunInstances":
		n := 1
		if s := first(req.Input, "MinCount", "MaxCount"); s != "" {
			if v, err := strconv.Atoi(s); err == nil && v > 0 {
				n = v
			}
		}
		var items []any
		for i := 0; i < n; i++ {
			id := "i-" + p.deps.Rand.Hex(8)
			rec := map[string]any{
				"InstanceId": id, "ImageId": first(req.Input, "ImageId"), "InstanceType": first(req.Input, "InstanceType"),
				"SubnetId": first(req.Input, "SubnetId"), "VpcId": first(req.Input, "VpcId"), "State": map[string]any{"Name": "running", "Code": 16},
			}
			if rec["InstanceType"] == "" {
				rec["InstanceType"] = "t3.micro"
			}
			b, _ := json.Marshal(rec)
			_ = p.col(req, "inst").Put(ctx, id, b)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"instancesSet": items, "reservationId": "r-" + p.deps.Rand.Hex(8)}}, nil
	case "DescribeInstances":
		want := first(req.Input, "InstanceId", "InstanceId.1")
		kvs, _, _ := p.col(req, "inst").List(ctx, "", "", 0)
		var items []any
		for _, kv := range kvs {
			if want != "" && kv.Key != want {
				continue
			}
			var rec map[string]any
			_ = json.Unmarshal(kv.Value, &rec)
			items = append(items, rec)
		}
		return &spi.Response{Output: map[string]any{"reservationSet": []any{map[string]any{"instancesSet": items}}}}, nil
	case "TerminateInstances":
		id := first(req.Input, "InstanceId", "InstanceId.1")
		_ = p.col(req, "inst").Delete(ctx, id)
		return &spi.Response{Output: map[string]any{"instancesSet": []any{map[string]any{"InstanceId": id, "CurrentState": map[string]any{"Name": "terminated"}}}}}, nil
	default:
		return nil, spi.NotImplemented("aws.ec2", req.Operation, "emulate")
	}
}

func (p *Pack) listNamed(ctx context.Context, req *spi.Request, col, set, want string) (*spi.Response, error) {
	kvs, _, _ := p.col(req, col).List(ctx, "", "", 0)
	var items []any
	for _, kv := range kvs {
		if want != "" && kv.Key != want {
			continue
		}
		var rec map[string]any
		_ = json.Unmarshal(kv.Value, &rec)
		items = append(items, rec)
	}
	return &spi.Response{Output: map[string]any{set: items}}, nil
}

func first(in map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := in[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
