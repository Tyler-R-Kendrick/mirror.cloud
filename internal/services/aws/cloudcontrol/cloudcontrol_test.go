package cloudcontrol

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/apigatewayv2"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/cloudformation"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/rds"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestReadsCloudFormationS3BucketConfiguration(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	template := `{"Resources":{"Bucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"configured","VersioningConfiguration":{"Status":"Enabled"},"CorsConfiguration":{"CorsRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"]}]},"ReplicationConfiguration":{"Role":"arn:aws:iam::000000000000:role/replication","Rules":[]}}}}}`
	if _, err := cloudformation.New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateStack", Input: map[string]any{"StackName": "bucket", "TemplateBody": template}}); err != nil {
		t.Fatal(err)
	}
	resp, err := New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "GetResource", Input: map[string]any{"TypeName": "AWS::S3::Bucket", "Identifier": "configured"}})
	if err != nil {
		t.Fatal(err)
	}
	properties := resp.Output["ResourceDescription"].(map[string]any)["Properties"].(string)
	for _, field := range []string{"VersioningConfiguration", "CorsConfiguration", "ReplicationConfiguration"} {
		if !strings.Contains(properties, field) {
			t.Fatalf("missing %s in %s", field, properties)
		}
	}
}

func TestReadsAPIGatewayV2AndRDSResources(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	api, err := apigatewayv2.New(deps).Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateApi", Input: map[string]any{"Name": "api", "ProtocolType": "HTTP"}})
	if err != nil {
		t.Fatal(err)
	}
	apiID := api.Output["ApiId"].(string)
	rdsPack := rds.New(deps)
	for operation, input := range map[string]map[string]any{
		"CreateDBInstance": {"DBInstanceIdentifier": "database", "Engine": "postgres"},
		"CreateDBCluster":  {"DBClusterIdentifier": "cluster", "Engine": "aurora-postgresql"},
	} {
		if _, err := rdsPack.Invoke(ctx, &spi.Request{Identity: id, Operation: operation, Input: input}); err != nil {
			t.Fatal(err)
		}
	}
	p := New(deps)
	for typeName, identifier := range map[string]string{
		"AWS::ApiGatewayV2::Api": apiID,
		"AWS::RDS::DBInstance":   "database",
		"AWS::RDS::DBCluster":    "cluster",
	} {
		resp, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "GetResource", Input: map[string]any{"TypeName": typeName, "Identifier": identifier}})
		if err != nil {
			t.Fatal(err)
		}
		description := resp.Output["ResourceDescription"].(map[string]any)
		if description["Identifier"] != identifier || !strings.Contains(description["Properties"].(string), identifier) {
			t.Fatalf("%s description = %#v", typeName, description)
		}
		listed, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListResources", Input: map[string]any{"TypeName": typeName}})
		if err != nil || len(listed.Output["ResourceDescriptions"].([]any)) != 1 {
			t.Fatalf("%s list = %#v, %v", typeName, listed, err)
		}
	}
}

func TestCloudControlHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 6 {
		t.Fatalf("cloudcontrol Operations() %d want 6", n)
	}
}

func TestCreatedResourceRoundTrip(t *testing.T) {
	p := New(spitest.Deps(t))
	ctx := context.Background()
	req := func(operation string, input map[string]any) (*spi.Response, error) {
		return p.Invoke(ctx, &spi.Request{Identity: spi.Identity{Account: "000000000000", Region: "us-east-1"}, Operation: operation, Input: input})
	}
	created, err := req("CreateResource", map[string]any{"TypeName": "AWS::S3::Bucket", "DesiredState": `{"BucketName":"b"}`})
	if err != nil {
		t.Fatal(err)
	}
	id := created.Output["ProgressEvent"].(map[string]any)["Identifier"].(string)
	got, err := req("GetResource", map[string]any{"TypeName": "AWS::S3::Bucket", "Identifier": id})
	if err != nil || got.Output["ResourceDescription"].(map[string]any)["Identifier"] != id {
		t.Fatalf("get = %#v, %v", got, err)
	}
	listed, err := req("ListResources", map[string]any{"TypeName": "AWS::S3::Bucket"})
	if err != nil || len(listed.Output["ResourceDescriptions"].([]any)) != 1 {
		t.Fatalf("list = %#v, %v", listed, err)
	}
}

func TestBootedServerCloudControlCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.cloudcontrol"}
	cfg.Seed = "cc-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudcontrol/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "CloudApiService."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	created := call("CreateResource", `{"TypeName":"AWS::S3::Bucket","DesiredState":"{\"BucketName\":\"b\"}"}`)
	pe, _ := created["ProgressEvent"].(map[string]any)
	id, _ := pe["Identifier"].(string)
	if id == "" {
		t.Fatalf("create %v", created)
	}
	got := call("GetResource", `{"TypeName":"AWS::S3::Bucket","Identifier":"`+id+`"}`)
	if got["ResourceDescription"] == nil {
		t.Fatalf("get %v", got)
	}
	call("DeleteResource", `{"TypeName":"AWS::S3::Bucket","Identifier":"`+id+`"}`)
	listed := call("ListResources", `{"TypeName":"AWS::S3::Bucket"}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"`+id+`"`) {
		t.Fatalf("still present %s", raw)
	}
}
