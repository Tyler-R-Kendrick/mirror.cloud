package spine

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	rtpkg "github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/ssm"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestBootedServerSSMSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ssm"}
	cfg.Seed = "ssm-48"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ssm/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonSSM."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 && op != "GetParameter" {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if op != "GetParameter" && res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q %s", res.Header.Get("x-mirror-fidelity"), op)
		}
		return res.StatusCode, out
	}

	_, put := call("PutParameter", `{"Name":"/app/a","Value":"one","Type":"String"}`)
	if asInt(put["Version"]) != 1 {
		t.Fatalf("ver1 %v", put)
	}
	call("PutParameter", `{"Name":"/app/a","Value":"two","Type":"String"}`)
	_, got := call("GetParameter", `{"Name":"/app/a"}`)
	if str(asM(got["Parameter"])["Value"]) != "two" {
		t.Fatalf("get %v", got)
	}
	if asInt(asM(got["Parameter"])["Version"]) != 2 {
		t.Fatalf("ver2 %v", got)
	}
	_, hist := call("GetParameterHistory", `{"Name":"/app/a"}`)
	if len(asSlice(hist["Parameters"])) < 2 {
		t.Fatalf("history %v", hist)
	}

	call("PutParameter", `{"Name":"/app/secret","Value":"plain","Type":"SecureString"}`)
	_, sec := call("GetParameter", `{"Name":"/app/secret"}`)
	if str(asM(sec["Parameter"])["Value"]) != "plain" {
		t.Fatalf("secure decode %v", sec)
	}

	call("PutParameter", `{"Name":"/app/b","Value":"b","Type":"String"}`)
	call("PutParameter", `{"Name":"/app/n/c","Value":"c","Type":"String"}`)
	_, byPath := call("GetParametersByPath", `{"Path":"/app","Recursive":false}`)
	names := map[string]bool{}
	for _, p := range asSlice(byPath["Parameters"]) {
		names[str(asM(p)["Name"])] = true
	}
	if !names["/app/a"] || !names["/app/b"] || names["/app/n/c"] {
		t.Fatalf("non-recursive %v", byPath)
	}
	_, rec := call("GetParametersByPath", `{"Path":"/app","Recursive":true}`)
	rnames := map[string]bool{}
	for _, p := range asSlice(rec["Parameters"]) {
		rnames[str(asM(p)["Name"])] = true
	}
	if !rnames["/app/n/c"] {
		t.Fatalf("recursive %v", rec)
	}
	_, page1 := call("GetParametersByPath", `{"Path":"/app","Recursive":true,"MaxResults":1}`)
	if len(asSlice(page1["Parameters"])) != 1 || page1["NextToken"] == nil {
		t.Fatalf("page1 %v", page1)
	}
	tok, _ := json.Marshal(page1["NextToken"])
	_, page2 := call("GetParametersByPath", `{"Path":"/app","Recursive":true,"MaxResults":1,"NextToken":`+string(tok)+`}`)
	if len(asSlice(page2["Parameters"])) != 1 {
		t.Fatalf("page2 %v", page2)
	}
	if str(asM(asSlice(page1["Parameters"])[0])["Name"]) == str(asM(asSlice(page2["Parameters"])[0])["Name"]) {
		t.Fatalf("path pagination repeated %v %v", page1, page2)
	}

	_, multi := call("GetParameters", `{"Names":["/app/a","/app/b"]}`)
	if len(asSlice(multi["Parameters"])) != 2 {
		t.Fatalf("get parameters %v", multi)
	}
	_, desc := call("DescribeParameters", `{}`)
	if len(asSlice(desc["Parameters"])) < 2 {
		t.Fatalf("describe %v", desc)
	}

	call("LabelParameterVersion", `{"Name":"/app/a","Labels":["prod"]}`)
	call("AddTagsToResource", `{"ResourceId":"/app/a","Tags":[{"Key":"k","Value":"v"}]}`)
	_, tags := call("ListTagsForResource", `{"ResourceId":"/app/a"}`)
	if tags["TagList"] == nil {
		t.Fatalf("tags %v", tags)
	}
	call("RemoveTagsFromResource", `{"ResourceId":"/app/a"}`)
	call("DeleteParameter", `{"Name":"/app/b"}`)
	code, miss := call("GetParameter", `{"Name":"/app/b"}`)
	if code != 400 && asM(miss["Parameter"])["Name"] != "" {
		t.Fatalf("deleted still there %d %v", code, miss)
	}
	call("DeleteParameters", `{"Names":["/app/secret"]}`)

	call("UnlabelParameterVersion", `{"Name":"/app/a","Labels":["prod"]}`)
	_, doc := call("CreateDocument", `{"Name":"doc1","Content":"{}","DocumentType":"Command"}`)
	if asM(doc["DocumentDescription"])["Name"] != "doc1" && str(asM(doc["DocumentDescription"])["Name"]) != "doc1" {
		if _, ok := doc["DocumentDescription"]; !ok {
			t.Fatalf("create doc %v", doc)
		}
	}
	call("GetDocument", `{"Name":"doc1"}`)
	call("DescribeDocument", `{"Name":"doc1"}`)
	call("ListDocuments", `{}`)
	call("UpdateDocument", `{"Name":"doc1","Content":"{\"v\":2}"}`)
	call("ListDocumentVersions", `{"Name":"doc1"}`)
	call("UpdateDocumentDefaultVersion", `{"Name":"doc1","DocumentVersion":"1"}`)
	_, assoc := call("CreateAssociation", `{"Name":"doc1"}`)
	aid := str(asM(assoc["AssociationDescription"])["AssociationId"])
	call("DescribeAssociation", `{"AssociationId":"`+aid+`"}`)
	call("UpdateAssociation", `{"AssociationId":"`+aid+`"}`)
	call("ListAssociations", `{}`)
	call("DeleteAssociation", `{"AssociationId":"`+aid+`"}`)
	_, cmd := call("SendCommand", `{"DocumentName":"doc1","InstanceIds":["i-1"]}`)
	cid := str(asM(cmd["Command"])["CommandId"])
	call("ListCommands", `{}`)
	call("ListCommandInvocations", `{"CommandId":"`+cid+`"}`)
	call("GetCommandInvocation", `{"CommandId":"`+cid+`","InstanceId":"i-1"}`)
	call("CancelCommand", `{"CommandId":"`+cid+`"}`)
	_, bl := call("CreatePatchBaseline", `{"Name":"pb","OperatingSystem":"AMAZON_LINUX_2"}`)
	bid := str(bl["BaselineId"])
	call("GetPatchBaseline", `{"BaselineId":"`+bid+`"}`)
	call("UpdatePatchBaseline", `{"BaselineId":"`+bid+`","Name":"pb2"}`)
	call("DescribePatchBaselines", `{}`)
	call("RegisterDefaultPatchBaseline", `{"BaselineId":"`+bid+`"}`)
	call("GetDefaultPatchBaseline", `{}`)
	call("DeletePatchBaseline", `{"BaselineId":"`+bid+`"}`)
	_, mw := call("CreateMaintenanceWindow", `{"Name":"mw","Schedule":"cron(0 0 * * ? *)","Duration":1,"Cutoff":0}`)
	wid := str(mw["WindowId"])
	call("GetMaintenanceWindow", `{"WindowId":"`+wid+`"}`)
	call("UpdateMaintenanceWindow", `{"WindowId":"`+wid+`","Name":"mw2"}`)
	call("DescribeMaintenanceWindows", `{}`)
	_, tgt := call("RegisterTargetWithMaintenanceWindow", `{"WindowId":"`+wid+`","ResourceType":"INSTANCE"}`)
	tid := str(tgt["WindowTargetId"])
	call("DescribeMaintenanceWindowTargets", `{"WindowId":"`+wid+`"}`)
	call("DeregisterTargetFromMaintenanceWindow", `{"WindowId":"`+wid+`","WindowTargetId":"`+tid+`"}`)
	_, task := call("RegisterTaskWithMaintenanceWindow", `{"WindowId":"`+wid+`","TaskArn":"AWS-RunShellScript"}`)
	tkid := str(task["WindowTaskId"])
	call("DescribeMaintenanceWindowTasks", `{"WindowId":"`+wid+`"}`)
	call("DeregisterTaskFromMaintenanceWindow", `{"WindowId":"`+wid+`","WindowTaskId":"`+tkid+`"}`)
	call("DeleteMaintenanceWindow", `{"WindowId":"`+wid+`"}`)
	_, auto := call("StartAutomationExecution", `{"DocumentName":"AWS-HelloWorld"}`)
	aeid := str(auto["AutomationExecutionId"])
	call("GetAutomationExecution", `{"AutomationExecutionId":"`+aeid+`"}`)
	call("DescribeAutomationExecutions", `{}`)
	call("StopAutomationExecution", `{"AutomationExecutionId":"`+aeid+`"}`)
	_, ops := call("CreateOpsItem", `{"Title":"t","Source":"mirror"}`)
	oid := str(ops["OpsItemId"])
	call("GetOpsItem", `{"OpsItemId":"`+oid+`"}`)
	call("UpdateOpsItem", `{"OpsItemId":"`+oid+`","Status":"Resolved"}`)
	call("DescribeOpsItems", `{}`)
	call("DeleteOpsItem", `{"OpsItemId":"`+oid+`"}`)
	call("CreateResourceDataSync", `{"SyncName":"sync1"}`)
	call("ListResourceDataSync", `{}`)
	call("DeleteResourceDataSync", `{"SyncName":"sync1"}`)
	call("UpdateServiceSetting", `{"SettingId":"/ssm/managed-instance/default-ec2-instance-management-role","SettingValue":"role"}`)
	call("GetServiceSetting", `{"SettingId":"/ssm/managed-instance/default-ec2-instance-management-role"}`)
	call("ResetServiceSetting", `{"SettingId":"/ssm/managed-instance/default-ec2-instance-management-role"}`)
	call("DeleteDocument", `{"Name":"doc1"}`)
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(`{"IamRole":"role"}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonSSM.CreateActivation")
	req.Header.Set("Authorization", auth)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("CreateActivation %d %s %s", res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
	}
}

func asInt(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()
		return int(i)
	case string:
		i, _ := json.Number(n).Int64()
		return int(i)
	}
	return 0
}

func TestSSMHTTPProvenOps(t *testing.T) {
	want := []string{
		"PutParameter", "GetParameter", "GetParameters", "GetParametersByPath",
		"DeleteParameter", "DeleteParameters", "DescribeParameters", "LabelParameterVersion",
		"UnlabelParameterVersion", "GetParameterHistory",
		"AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
		"CreateDocument", "GetDocument", "DeleteDocument", "UpdateDocument", "DescribeDocument",
		"ListDocuments", "ListDocumentVersions", "UpdateDocumentDefaultVersion",
		"CreateAssociation", "DescribeAssociation", "UpdateAssociation", "DeleteAssociation",
		"ListAssociations",
		"SendCommand", "ListCommands", "ListCommandInvocations", "GetCommandInvocation", "CancelCommand",
		"CreatePatchBaseline", "GetPatchBaseline", "UpdatePatchBaseline", "DeletePatchBaseline",
		"DescribePatchBaselines", "RegisterDefaultPatchBaseline", "GetDefaultPatchBaseline",
		"CreateMaintenanceWindow", "GetMaintenanceWindow", "UpdateMaintenanceWindow", "DeleteMaintenanceWindow",
		"DescribeMaintenanceWindows",
		"RegisterTargetWithMaintenanceWindow", "DeregisterTargetFromMaintenanceWindow", "DescribeMaintenanceWindowTargets",
		"RegisterTaskWithMaintenanceWindow", "DeregisterTaskFromMaintenanceWindow", "DescribeMaintenanceWindowTasks",
		"StartAutomationExecution", "GetAutomationExecution", "StopAutomationExecution", "DescribeAutomationExecutions",
		"CreateOpsItem", "GetOpsItem", "UpdateOpsItem", "DeleteOpsItem", "DescribeOpsItems",
		"CreateResourceDataSync", "ListResourceDataSync", "DeleteResourceDataSync",
		"GetServiceSetting", "UpdateServiceSetting", "ResetServiceSetting",
	}

	assertSame(t, "ssm", ssm.New(spitest.Deps(t)).Operations(), append(want, ssm.ExtraOps()...))
}
