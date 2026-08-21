package ecs

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
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/elasticloadbalancing"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func TestServiceTargetsFollowTaskState(t *testing.T) {
	deps := spitest.Deps(t)
	ctx := context.Background()
	id := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	elb := elasticloadbalancing.New(deps)
	tg, err := elb.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateTargetGroup", Input: map[string]any{"Name": "svc", "Port": 80, "Protocol": "HTTP", "TargetType": "ip"}})
	if err != nil {
		t.Fatal(err)
	}
	targetGroup := tg.Output["TargetGroups"].([]any)[0].(map[string]any)["TargetGroupArn"].(string)
	p := New(deps)
	service, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "CreateService", Input: map[string]any{
		"cluster": "default", "serviceName": "web", "taskDefinition": "web:1", "desiredCount": 1,
		"loadBalancers": []any{map[string]any{"targetGroupArn": targetGroup, "containerName": "web", "containerPort": 8080}},
	}})
	if err != nil || service.Output["service"] == nil {
		t.Fatalf("CreateService: %#v %v", service, err)
	}
	listed, _ := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "ListTasks", Input: map[string]any{"cluster": "default"}})
	task := listed.Output["taskArns"].([]any)[0].(string)
	health := func() []any {
		resp, err := elb.Invoke(ctx, &spi.Request{Identity: id, Operation: "DescribeTargetHealth", Input: map[string]any{"TargetGroupArn": targetGroup}})
		if err != nil {
			t.Fatal(err)
		}
		return resp.Output["TargetHealthDescriptions"].([]any)
	}
	if targets := health(); len(targets) != 0 {
		t.Fatalf("PENDING task registered early: %#v", targets)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SubmitTaskStateChange", Input: map[string]any{"cluster": "default", "task": task, "status": "RUNNING"}}); err != nil {
		t.Fatal(err)
	}
	if targets := health(); len(targets) != 1 {
		t.Fatalf("RUNNING task targets: %#v", targets)
	}
	if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "StopTask", Input: map[string]any{"cluster": "default", "task": task}}); err != nil {
		t.Fatal(err)
	}
	if targets := health(); len(targets) != 0 {
		t.Fatalf("STOPPED task still registered: %#v", targets)
	}
	for _, status := range []string{"RUNNING", "FAILED"} {
		if _, err := p.Invoke(ctx, &spi.Request{Identity: id, Operation: "SubmitTaskStateChange", Input: map[string]any{"cluster": "default", "task": task, "status": status}}); err != nil {
			t.Fatal(err)
		}
	}
	if targets := health(); len(targets) != 0 {
		t.Fatalf("FAILED task still registered: %#v", targets)
	}
}

func TestBootedServerECSClusterTask(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ecs"}
	cfg.Seed = "ecs-1"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ecs/aws4_request, SignedHeaders=host, Signature=00"
	call := func(op, body string) map[string]any {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+op)
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
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return out
	}
	c := call("CreateCluster", `{"clusterName":"c1"}`)
	if c["cluster"] == nil {
		t.Fatalf("cluster %v", c)
	}
	listed := call("ListClusters", `{}`)
	if listed["clusterArns"] == nil {
		t.Fatalf("list %v", listed)
	}
	td := call("RegisterTaskDefinition", `{"family":"web","containerDefinitions":[{"name":"n","image":"nginx"}]}`)
	if td["taskDefinition"] == nil {
		t.Fatalf("td %v", td)
	}
	run := call("RunTask", `{"cluster":"c1","taskDefinition":"web"}`)
	tasks, _ := run["tasks"].([]any)
	if len(tasks) < 1 {
		t.Fatalf("run %v", run)
	}
	call("DescribeClusters", `{"clusters":["c1"]}`)
	call("DescribeTaskDefinition", `{"taskDefinition":"web"}`)
	call("ListTaskDefinitions", `{}`)
	call("CreateService", `{"cluster":"c1","serviceName":"svc","taskDefinition":"web","desiredCount":1}`)
	call("DescribeServices", `{"cluster":"c1","services":["svc"]}`)
	call("ListServices", `{"cluster":"c1"}`)
	call("UpdateService", `{"cluster":"c1","service":"svc","desiredCount":2}`)
	call("UpdateCluster", `{"cluster":"c1"}`)
	call("UpdateClusterSettings", `{"cluster":"c1","settings":[]}`)
	call("PutClusterCapacityProviders", `{"cluster":"c1","capacityProviders":["FARGATE"]}`)
	call("TagResource", `{"resourceArn":"arn:aws:ecs:us-east-1:000000000000:cluster/c1","tags":[{"key":"k","value":"v"}]}`)
	call("ListTagsForResource", `{"resourceArn":"arn:aws:ecs:us-east-1:000000000000:cluster/c1"}`)
	call("UntagResource", `{"resourceArn":"arn:aws:ecs:us-east-1:000000000000:cluster/c1","tagKeys":["k"]}`)
	call("PutAccountSetting", `{"name":"containerInsights","value":"enabled"}`)
	call("PutAccountSettingDefault", `{"name":"awsvpcTrunking","value":"enabled"}`)
	call("ListAccountSettings", `{}`)
	call("DeleteAccountSetting", `{"name":"containerInsights"}`)
	tsSet := call("CreateTaskSet", `{"cluster":"c1","service":"svc","taskDefinition":"web"}`)
	set, _ := tsSet["taskSet"].(map[string]any)
	setID, _ := set["id"].(string)
	call("DescribeTaskSets", `{"cluster":"c1","service":"svc"}`)
	call("UpdateTaskSet", `{"cluster":"c1","service":"svc","taskSet":"`+setID+`","scale":{"value":100,"unit":"PERCENT"}}`)
	call("DeleteTaskSet", `{"cluster":"c1","service":"svc","taskSet":"`+setID+`"}`)
	call("PutAttributes", `{"cluster":"c1","attributes":[{"name":"a","value":"1"}]}`)
	call("ListAttributes", `{"cluster":"c1","targetType":"container-instance"}`)
	call("DeleteAttributes", `{"cluster":"c1","attributes":[{"name":"a"}]}`)
	ci := call("RegisterContainerInstance", `{"cluster":"c1"}`)
	inst, _ := ci["containerInstance"].(map[string]any)
	ciArn, _ := inst["containerInstanceArn"].(string)
	call("ListContainerInstances", `{"cluster":"c1"}`)
	call("DescribeContainerInstances", `{"cluster":"c1","containerInstances":["`+ciArn+`"]}`)
	call("DeregisterContainerInstance", `{"cluster":"c1","containerInstance":"`+ciArn+`"}`)
	st := call("StartTask", `{"cluster":"c1","taskDefinition":"web","containerInstances":["i-1"]}`)
	if st["tasks"] == nil {
		t.Fatalf("start %v", st)
	}
	tarn, _ := tasks[0].(map[string]any)["taskArn"].(string)
	call("DescribeTasks", `{"cluster":"c1","tasks":["`+tarn+`"]}`)
	call("ListTasks", `{"cluster":"c1"}`)
	call("StopTask", `{"cluster":"c1","task":"`+tarn+`"}`)
	call("DeleteService", `{"cluster":"c1","service":"svc"}`)
	call("DeregisterTaskDefinition", `{"taskDefinition":"web:1"}`)
	call("DeleteCluster", `{"cluster":"c1"}`)
	cp := call("CreateCapacityProvider", `{"name":"cp1"}`)
	if cp["capacityProvider"] == nil {
		t.Fatalf("cp %v", cp)
	}
	call("DescribeCapacityProviders", `{"capacityProviders":["cp1"]}`)
	call("UpdateCapacityProvider", `{"name":"cp1"}`)
	call("CreateDaemon", `{"daemonName":"d1"}`)
	call("DescribeDaemon", `{"daemonName":"d1"}`)
	call("ListDaemons", `{}`)
	call("RegisterDaemonTaskDefinition", `{"family":"dd"}`)
	call("ListDaemonTaskDefinitions", `{}`)
	call("CreateExpressGatewayService", `{"serviceName":"eg1"}`)
	call("DescribeExpressGatewayService", `{"serviceName":"eg1"}`)
	call("ContinueServiceDeployment", `{"service":"svc"}`)
	call("ListServiceDeployments", `{}`)
	call("StopServiceDeployment", `{"service":"svc"}`)
	call("DiscoverPollEndpoint", `{"cluster":"c1"}`)
	call("ExecuteCommand", `{"cluster":"c1","task":"`+tarn+`","command":"ls"}`)
	call("UpdateTaskProtection", `{"cluster":"c1","tasks":["`+tarn+`"],"protectionEnabled":true}`)
	call("GetTaskProtection", `{"cluster":"c1","tasks":["`+tarn+`"]}`)
	call("ListTaskDefinitionFamilies", `{}`)
	call("ListServicesByNamespace", `{"namespace":"ns"}`)
	call("SubmitTaskStateChange", `{"cluster":"c1","task":"`+tarn+`","status":"RUNNING"}`)
	call("UpdateContainerInstancesState", `{"cluster":"c1","containerInstances":["`+ciArn+`"],"status":"DRAINING"}`)
	call("DeleteCapacityProvider", `{"capacityProvider":"cp1"}`)
	call("DeleteDaemon", `{"daemonName":"d1"}`)
	call("DeleteExpressGatewayService", `{"serviceName":"eg1"}`)
}

func TestECSHTTPProvenOps(t *testing.T) {
	p := New(spitest.Deps(t))
	if n := len(p.Operations()); n != 77 {
		t.Fatalf("ecs Operations() %d want 77", n)
	}
}

func TestBootedServerECSExtraOps(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.ecs"}
	cfg.Seed = "ecs-extra"
	rt, err := rtpkg.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/ecs/aws4_request, SignedHeaders=host, Signature=00"
	soft := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q %s", op, res.Header.Get("x-mirror-fidelity"), raw)
		}
		if res.StatusCode >= 500 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		return string(raw)
	}
	hard := func(op, body string) string {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonEC2ContainerServiceV20141113."+op)
		req.Header.Set("Authorization", auth)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 || res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s %d %s %s", op, res.StatusCode, res.Header.Get("x-mirror-fidelity"), raw)
		}
		return string(raw)
	}
	created := hard("CreateCapacityProvider", `{"name":"cpboot"}`)
	if !strings.Contains(created, "cpboot") {
		t.Fatalf("create cp %s", created)
	}
	got := hard("DescribeCapacityProviders", `{"capacityProviders":["cpboot"]}`)
	if !strings.Contains(got, "cpboot") {
		t.Fatalf("describe cp %s", got)
	}
	hard("DeleteCapacityProvider", `{"capacityProvider":"cpboot"}`)
	gone := hard("DescribeCapacityProviders", `{"capacityProviders":["cpboot"]}`)
	if strings.Contains(gone, `"name":"cpboot"`) {
		t.Fatalf("cp still present %s", gone)
	}
	payload := `{"name":"cpboot","capacityProvider":"cpboot","daemonName":"d1","serviceName":"eg1","cluster":"c1","service":"svc","task":"t1","family":"web","namespace":"ns","containerInstance":"i-1","containerInstances":["i-1"],"tasks":["t1"],"taskDefinitions":["web"],"protectionEnabled":true,"status":"ACTIVE"}`
	for _, op := range extraOps() {
		soft(op, payload)
	}
}
