package sdk_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	mcfg "github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	// aws.ec2 is a bundle; the engine registers it from internal/bundled.
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/bundled"
)

// TestAWSSDKRoundTripEC2 is the ec2Query protocol's first real client. Every
// other protocol mirror serves has one; ec2Query -- the protocol whose wire
// names diverge furthest from its member names -- had none, so nothing noticed
// that a response carrying `<VpcId>` cannot be read by a client looking for
// `<vpcId>`.
func TestAWSSDKRoundTripEC2(t *testing.T) {
	cfg := mcfg.Default()
	cfg.Services = []string{"aws.ec2"}
	cfg.Seed = "sdk-ec2"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	awscfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatal(err)
	}
	c := ec2.NewFromConfig(awscfg, func(o *ec2.Options) { o.BaseEndpoint = aws.String(ts.URL) })
	ctx := context.Background()

	created, err := c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.7.0.0/16")})
	if err != nil {
		t.Fatalf("create vpc: %v", err)
	}
	if created.Vpc == nil || aws.ToString(created.Vpc.VpcId) == "" {
		t.Fatalf("create vpc returned no id: %#v", created.Vpc)
	}
	if got := aws.ToString(created.Vpc.CidrBlock); got != "10.7.0.0/16" {
		t.Fatalf("create vpc cidr is %q, want the requested block", got)
	}
	id := aws.ToString(created.Vpc.VpcId)

	listed, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{})
	if err != nil {
		t.Fatalf("describe vpcs: %v", err)
	}
	if len(listed.Vpcs) != 1 || aws.ToString(listed.Vpcs[0].VpcId) != id {
		t.Fatalf("describe vpcs returned %#v, want the created vpc", listed.Vpcs)
	}

	// The request list arrives flattened and under the member's own wire name
	// (`VpcId.1`, not `VpcIds.member.1`), which is what the decoder has to
	// understand for a filter to reach the pack at all.
	filtered, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{id}})
	if err != nil {
		t.Fatalf("describe vpcs by id: %v", err)
	}
	if len(filtered.Vpcs) != 1 || aws.ToString(filtered.Vpcs[0].VpcId) != id {
		t.Fatalf("describe vpcs by id returned %#v, want the created vpc", filtered.Vpcs)
	}
	if _, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{VpcIds: []string{"vpc-absent"}}); err != nil {
		t.Fatalf("describe absent vpc: %v", err)
	}

	ran, err := c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String("ami-1"), MinCount: aws.Int32(2), MaxCount: aws.Int32(2)})
	if err != nil {
		t.Fatalf("run instances: %v", err)
	}
	if len(ran.Instances) != 2 {
		t.Fatalf("run instances returned %d instances, want 2", len(ran.Instances))
	}
	if aws.ToString(ran.Instances[0].InstanceId) == "" || aws.ToString(ran.Instances[0].ImageId) != "ami-1" {
		t.Fatalf("run instances lost members: %#v", ran.Instances[0])
	}

	described, err := c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
	if err != nil {
		t.Fatalf("describe instances: %v", err)
	}
	if len(described.Reservations) != 1 || len(described.Reservations[0].Instances) != 2 {
		t.Fatalf("describe instances returned %#v, want one reservation of two", described.Reservations)
	}

	if _, err := c.DeleteVpc(ctx, &ec2.DeleteVpcInput{VpcId: aws.String(id)}); err != nil {
		t.Fatalf("delete vpc: %v", err)
	}
	if left, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{}); err != nil || len(left.Vpcs) != 0 {
		t.Fatalf("describe after delete returned %#v (%v), want none", left.Vpcs, err)
	}
}
