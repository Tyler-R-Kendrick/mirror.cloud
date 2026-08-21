package organizations

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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/iam"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestOrganizationsHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 24 {
		t.Fatalf("organizations Operations() %d want 24", n)
	}
}

func TestServiceControlPolicyEnforcesMemberAccount(t *testing.T) {
	deps := spitest.Deps(t)
	p := New(deps)
	ctx := context.Background()
	management := spi.Identity{Account: "000000000000", Region: "us-east-1", ARN: "arn:aws:iam::000000000000:user/admin"}
	invoke := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		resp, err := p.Invoke(ctx, &spi.Request{Identity: management, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	organization := invoke("CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	rootID := organization.Output["Organization"].(map[string]any)["RootId"].(string)
	account := invoke("CreateAccount", map[string]any{"Email": "member@example.com", "AccountName": "member"})
	accountID := account.Output["CreateAccountStatus"].(map[string]any)["AccountId"].(string)
	ou := invoke("CreateOrganizationalUnit", map[string]any{"Name": "sandbox", "ParentId": rootID})
	ouID := ou.Output["OrganizationalUnit"].(map[string]any)["Id"].(string)
	invoke("MoveAccount", map[string]any{"AccountId": accountID, "SourceParentId": rootID, "DestinationParentId": ouID})
	parents := invoke("ListParents", map[string]any{"ChildId": accountID})
	if parents.Output["Parents"].([]any)[0].(map[string]any)["Id"] != ouID {
		t.Fatalf("account parent = %#v", parents.Output["Parents"])
	}
	policy := invoke("CreatePolicy", map[string]any{
		"Name": "deny-delete", "Type": "SERVICE_CONTROL_POLICY",
		"Content": `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:Delete*","Resource":"*"},{"Effect":"Deny","NotAction":["iam:*","sts:*","organizations:*"],"Resource":"*","Condition":{"StringNotEquals":{"aws:RequestedRegion":["eu-central-1"]}}},{"Effect":"Deny","Action":"sqs:CreateQueue","Resource":"*","Condition":{"Null":{"aws:RequestTag/team":"true"}}},{"Effect":"Deny","Action":"ec2:RunInstances","Resource":"*","Condition":{"StringNotEquals":{"ec2:MetadataHttpTokens":"required"}}}]}`,
	})
	policyID := policy.Output["Policy"].(map[string]any)["PolicySummary"].(map[string]any)["Id"].(string)
	invoke("AttachPolicy", map[string]any{"PolicyId": policyID, "TargetId": ouID})
	listed := invoke("ListPoliciesForTarget", map[string]any{"TargetId": ouID, "Filter": "SERVICE_CONTROL_POLICY"})
	if len(listed.Output["Policies"].([]any)) != 1 {
		t.Fatalf("attached policies = %#v", listed.Output["Policies"])
	}
	authorizer := iam.NewAuthorizer(deps.Store)
	member := spi.Identity{Account: accountID, Region: "eu-central-1", ARN: "arn:aws:iam::" + accountID + ":user/root"}
	if err := authorizer.Authorize(ctx, member, "aws.s3", "GetObject", "*"); err != nil {
		t.Fatalf("FullAWSAccess should allow GetObject: %v", err)
	}
	if err := authorizer.Authorize(ctx, member, "aws.s3", "DeleteBucket", "*"); err == nil {
		t.Fatal("member DeleteBucket should be denied by SCP")
	}
	wrongRegion := member
	wrongRegion.Region = "us-east-1"
	if err := authorizer.Authorize(ctx, wrongRegion, "aws.s3", "GetObject", "*"); err == nil {
		t.Fatal("GetObject outside the approved region should be denied")
	}
	if err := authorizer.Authorize(ctx, wrongRegion, "aws.organizations", "DescribeAccount", "*"); err != nil {
		t.Fatalf("NotAction should exempt Organizations: %v", err)
	}
	requestAuthorizer := authorizer.(spi.RequestAuthorizer)
	queue := &spi.Request{Identity: member, ServiceID: "aws.sqs", Operation: "CreateQueue", Input: map[string]any{"Tags": map[string]any{"team": "data-eng"}}}
	if err := requestAuthorizer.AuthorizeRequest(ctx, queue, "*"); err != nil {
		t.Fatalf("tagged queue should be allowed: %v", err)
	}
	queue.Input = map[string]any{}
	if err := requestAuthorizer.AuthorizeRequest(ctx, queue, "*"); err == nil {
		t.Fatal("untagged queue should be denied")
	}
	instance := &spi.Request{Identity: member, ServiceID: "aws.ec2", Operation: "RunInstances", Input: map[string]any{"MetadataOptions": map[string]any{"HttpTokens": "optional"}}}
	if err := requestAuthorizer.AuthorizeRequest(ctx, instance, "*"); err == nil {
		t.Fatal("IMDSv1 instance should be denied")
	}
	instance.Input["MetadataOptions"] = map[string]any{"HttpTokens": "required"}
	if err := requestAuthorizer.AuthorizeRequest(ctx, instance, "*"); err != nil {
		t.Fatalf("IMDSv2 instance should be allowed: %v", err)
	}
	if err := authorizer.Authorize(ctx, management, "aws.s3", "DeleteBucket", "*"); err != nil {
		t.Fatalf("management account must be exempt from SCPs: %v", err)
	}
	iamPack := iam.New(deps)
	invokeIAM := func(operation string, input map[string]any) *spi.Response {
		t.Helper()
		resp, err := iamPack.Invoke(ctx, &spi.Request{Identity: member, Operation: operation, Input: input})
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	invokeIAM("CreateUser", map[string]any{"UserName": "ci-runner"})
	invokeIAM("PutUserPolicy", map[string]any{"UserName": "ci-runner", "PolicyName": "allow-s3", "PolicyDocument": `{"Statement":{"Effect":"Allow","Action":"s3:*","Resource":"*"}}`})
	simulate := func(region string) map[string]any {
		resp := invokeIAM("SimulatePrincipalPolicy", map[string]any{
			"PolicySourceArn": "arn:aws:iam::" + accountID + ":user/ci-runner", "ActionNames": []any{"s3:GetObject"},
			"ContextEntries": []any{map[string]any{"ContextKeyName": "aws:RequestedRegion", "ContextKeyValues": []any{region}}},
		})
		return resp.Output["EvaluationResults"].([]any)[0].(map[string]any)
	}
	denied := simulate("us-east-1")
	if denied["EvalDecision"] != "explicitDeny" || denied["OrganizationsDecisionDetail"].(map[string]any)["AllowedByOrganizations"] != false {
		t.Fatalf("wrong-region simulation = %#v", denied)
	}
	allowed := simulate("eu-central-1")
	if allowed["EvalDecision"] != "allowed" || allowed["OrganizationsDecisionDetail"].(map[string]any)["AllowedByOrganizations"] != true {
		t.Fatalf("approved-region simulation = %#v", allowed)
	}
}

func TestBootedServerOrganizationsCreateGetDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.organizations"}
	cfg.Seed = "org-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/organizations/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128."+op)
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
	created := call("CreateOrganization", `{"FeatureSet":"ALL"}`)
	org, _ := created["Organization"].(map[string]any)
	if org["Id"] == nil {
		t.Fatalf("create %v", created)
	}
	got := call("DescribeOrganization", `{}`)
	if got["Organization"] == nil {
		t.Fatalf("describe %v", got)
	}
	acct := call("CreateAccount", `{"Email":"a@example.com","AccountName":"dev"}`)
	st, _ := acct["CreateAccountStatus"].(map[string]any)
	id, _ := st["AccountId"].(string)
	if id == "" {
		t.Fatalf("acct %v", acct)
	}
	d := call("DescribeAccount", `{"AccountId":"`+id+`"}`)
	if d["Account"] == nil {
		t.Fatalf("describe acct %v", d)
	}
	call("DeleteOrganization", `{}`)
	listed := call("ListAccounts", `{}`)
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), `"Name":"missing-org-should-not-matter"`) {
		t.Fatalf("unexpected %s", raw)
	}
}
