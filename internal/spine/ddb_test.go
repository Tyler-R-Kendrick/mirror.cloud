package spine

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/runtime"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/dynamodb"
)

func TestBootedServerDynamoDBQueryAndDelete(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb"}
	cfg.Seed = "ddb-spine"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()

	call := func(op, body string) (int, map[string]any, http.Header) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		return res.StatusCode, out, res.Header
	}

	if code, out, h := call("CreateTable", `{"TableName":"T","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`); code >= 300 {
		t.Fatalf("create %d %v", code, out)
	} else if h.Get("x-mirror-fidelity") != "emulate" {
		t.Fatalf("fidelity %q", h.Get("x-mirror-fidelity"))
	}
	if code, out, _ := call("PutItem", `{"TableName":"T","Item":{"id":{"S":"a"},"n":{"N":"1"}}}`); code >= 300 {
		t.Fatalf("put a %d %v", code, out)
	}
	if code, out, _ := call("PutItem", `{"TableName":"T","Item":{"id":{"S":"b"},"n":{"N":"9"}}}`); code >= 300 {
		t.Fatalf("put b %d %v", code, out)
	}
	code, got, _ := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"a"}}}`)
	if code != 200 {
		t.Fatalf("get %d %v", code, got)
	}
	item, _ := got["Item"].(map[string]any)
	idAttr, _ := item["id"].(map[string]any)
	if str(idAttr["S"]) != "a" {
		t.Fatalf("getitem %v", got)
	}
	code, q, _ := call("Query", `{"TableName":"T","KeyConditionExpression":"id = :id","ExpressionAttributeValues":{":id":{"S":"a"}}}`)
	if code != 200 {
		t.Fatalf("query %d %v", code, q)
	}
	qitems, _ := q["Items"].([]any)
	if len(qitems) != 1 {
		t.Fatalf("query dumped table %v", q)
	}
	code, sc, _ := call("Scan", `{"TableName":"T","FilterExpression":"n < :n","ExpressionAttributeValues":{":n":{"N":"5"}}}`)
	if code != 200 {
		t.Fatalf("scan %d %v", code, sc)
	}
	sitems, _ := sc["Items"].([]any)
	if len(sitems) != 1 {
		t.Fatalf("scan filter %v", sc)
	}
	if code, out, _ := call("DeleteItem", `{"TableName":"T","Key":{"id":{"S":"a"}}}`); code >= 300 {
		t.Fatalf("delete %d %v", code, out)
	}
	code, miss, _ := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"a"}}}`)
	if code != 200 {
		t.Fatalf("get deleted %d %v", code, miss)
	}
	if miss["Item"] != nil {
		t.Fatalf("delete left item %v", miss)
	}

	s3, _ := http.NewRequest(http.MethodPut, ts.URL+"/bucket", nil)
	s3.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(s3)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 501 {
		t.Fatalf("expected 501 for s3 on ddb-only, got %d %s", res.StatusCode, b)
	}
	if res.Header.Get("x-mirror-not-implemented") == "" && !bytes.Contains(b, []byte("MirrorNotImplemented")) {
		t.Fatalf("not 501 envelope: %s %v", b, res.Header)
	}
}

func TestBootedServerDynamoDBExtraEngines(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb"}
	cfg.Seed = "ddb-extra"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q", op, res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, out
	}
	call("CreateTable", `{"TableName":"T","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}`)
	call("ExecuteStatement", `{"Statement":"INSERT INTO T VALUE {'id': 'k1', 'n': '7'}"}`)
	_, got := call("ExecuteStatement", `{"Statement":"SELECT * FROM T WHERE id = 'k1'"}`)
	item := asM(got["Item"])
	if str(asM(item["id"])["S"]) != "k1" && str(asM(asM(got["Item"])["id"])["S"]) != "k1" {
		if str(asM(item["n"])["S"]) != "7" && fmtJSON(got) != "" && !strings.Contains(fmtJSON(got), "k1") {
			t.Fatalf("partiql get %v", got)
		}
	}
	call("CreateGlobalTable", `{"GlobalTableName":"gt","ReplicationGroup":[{"RegionName":"us-east-1"}]}`)
	call("UpdateGlobalTable", `{"GlobalTableName":"gt","ReplicaUpdates":[{"Create":{"RegionName":"us-west-2"}}]}`)
	_, desc := call("DescribeGlobalTable", `{"GlobalTableName":"gt"}`)
	if asM(desc["GlobalTableDescription"])["GlobalTableName"] != "gt" && str(asM(desc["GlobalTableDescription"])["GlobalTableName"]) != "gt" {
		t.Fatalf("gt %v", desc)
	}
	_, listed := call("ListGlobalTables", `{}`)
	if len(asSlice(listed["GlobalTables"])) == 0 {
		t.Fatalf("list gt %v", listed)
	}
	call("UpdateContributorInsights", `{"TableName":"T","ContributorInsightsAction":"ENABLE"}`)
	_, ins := call("DescribeContributorInsights", `{"TableName":"T"}`)
	if str(ins["ContributorInsightsStatus"]) != "ENABLED" {
		t.Fatalf("insights %v", ins)
	}
	_, exp := call("ExportTableToPointInTime", `{"TableName":"T","S3Bucket":"b"}`)
	arn := str(asM(exp["ExportDescription"])["ExportArn"])
	if arn == "" {
		t.Fatalf("export %v", exp)
	}
	call("DescribeExport", `{"ExportArn":"`+arn+`"}`)
	call("ListExports", `{}`)
	_, imp := call("ImportTable", `{"TableCreationParameters":{"TableName":"Timp","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}}`)
	iarn := str(asM(imp["ImportTableDescription"])["ImportArn"])
	call("DescribeImport", `{"ImportArn":"`+iarn+`"}`)
	call("RestoreTableToPointInTime", `{"SourceTableName":"T","TargetTableName":"Tpitr"}`)
	_, rest := call("DescribeTable", `{"TableName":"Tpitr"}`)
	if rest["Table"] == nil {
		t.Fatalf("pitr %v", rest)
	}
	_, hit := call("SearchVectors", `{"TableName":"T","Query":"k1"}`)
	if len(asSlice(hit["Items"])) == 0 {
		t.Fatalf("vectors %v", hit)
	}
	call("UpdateTableReplicaAutoScaling", `{"TableName":"T"}`)
	call("DescribeTableReplicaAutoScaling", `{"TableName":"T"}`)
	call("UpdateKinesisStreamingDestination", `{"TableName":"T","StreamArn":"arn:k"}`)
	call("UpdateGlobalTableSettings", `{"GlobalTableName":"gt"}`)
	call("DescribeGlobalTableSettings", `{"GlobalTableName":"gt"}`)
	call("ListContributorInsights", `{}`)
	call("ListImports", `{}`)
	call("ExecuteTransaction", `{"TransactStatements":[{"Statement":"SELECT * FROM T WHERE id = 'k1'"}]}`)
	call("BatchExecuteStatement", `{"Statements":[{"Statement":"SELECT * FROM T WHERE id = 'k1'"}]}`)
}

func TestBootedServerDynamoDBSection48(t *testing.T) {
	cfg := config.Default()
	cfg.Services = []string{"aws.dynamodb"}
	cfg.Seed = "ddb-48"
	rt, err := runtime.Boot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(rt.Handler())
	defer ts.Close()
	call := func(op, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-amz-json-1.0")
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=00")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(res.Body)
		res.Body.Close()
		out := map[string]any{}
		_ = json.Unmarshal(raw, &out)
		if res.StatusCode >= 300 && op != "UpdateItem" && op != "DescribeTable" {
			t.Fatalf("%s %d %s", op, res.StatusCode, raw)
		}
		if res.StatusCode < 300 && res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("%s fidelity %q", op, res.Header.Get("x-mirror-fidelity"))
		}
		return res.StatusCode, out
	}
	ids := func(out map[string]any) []string {
		var got []string
		for _, it := range asItems(out) {
			got = append(got, str(asM(asM(it)["id"])["S"]))
		}
		return got
	}

	call("CreateTable", `{"TableName":"T","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"GlobalSecondaryIndexes":[{"IndexName":"gsi1","KeySchema":[{"AttributeName":"gsi","KeyType":"HASH"}],"Projection":{"ProjectionType":"KEYS_ONLY"}}]}`)
	for _, row := range []string{
		`{"id":{"S":"a"},"n":{"N":"1"},"gsi":{"S":"x"},"extra":{"S":"keep"}}`,
		`{"id":{"S":"b"},"n":{"N":"5"},"gsi":{"S":"x"},"extra":{"S":"drop"}}`,
		`{"id":{"S":"c"},"n":{"N":"9"},"gsi":{"S":"y"},"extra":{"S":"y"}}`,
	} {
		call("PutItem", `{"TableName":"T","Item":`+row+`}`)
	}

	_, qeq := call("Query", `{"TableName":"T","KeyConditionExpression":"id = :id","ExpressionAttributeValues":{":id":{"S":"a"}}}`)
	if len(ids(qeq)) != 1 || ids(qeq)[0] != "a" {
		t.Fatalf("eq %v", qeq)
	}
	_, qlt := call("Query", `{"TableName":"T","KeyConditionExpression":"id < :id","ExpressionAttributeValues":{":id":{"S":"b"}}}`)
	if len(ids(qlt)) != 1 || ids(qlt)[0] != "a" {
		t.Fatalf("< %v", qlt)
	}
	_, qgt := call("Query", `{"TableName":"T","KeyConditionExpression":"id > :id","ExpressionAttributeValues":{":id":{"S":"b"}}}`)
	if len(ids(qgt)) != 1 || ids(qgt)[0] != "c" {
		t.Fatalf("> %v", qgt)
	}
	_, qbt := call("Query", `{"TableName":"T","KeyConditionExpression":"n BETWEEN :lo AND :hi","ExpressionAttributeValues":{":lo":{"N":"4"},":hi":{"N":"6"}}}`)
	if len(ids(qbt)) != 1 || ids(qbt)[0] != "b" {
		t.Fatalf("between %v", qbt)
	}
	_, qbw := call("Query", `{"TableName":"T","KeyConditionExpression":"begins_with(id, :p)","ExpressionAttributeValues":{":p":{"S":"a"}}}`)
	if len(ids(qbw)) != 1 {
		t.Fatalf("begins_with %v", qbw)
	}
	_, sc := call("Scan", `{"TableName":"T","FilterExpression":"n < :n","ExpressionAttributeValues":{":n":{"N":"5"}}}`)
	if len(ids(sc)) != 1 || ids(sc)[0] != "a" {
		t.Fatalf("scan filter %v", sc)
	}

	code, fail := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"ConditionExpression":"n > :n","UpdateExpression":"SET extra = :e","ExpressionAttributeValues":{":n":{"N":"50"},":e":{"S":"no"}}}`)
	if code != 400 {
		t.Fatalf("cond status %d %v", code, fail)
	}
	if str(fail["__type"]) != "ConditionalCheckFailedException" {
		t.Fatalf("cond type %v", fail)
	}
	if fail["Item"] == nil {
		t.Fatalf("cond missing Item %v", fail)
	}

	_, old := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"UpdateExpression":"SET extra = :e ADD n :one","ExpressionAttributeValues":{":e":{"S":"new"},":one":{"N":"1"}},"ReturnValues":"ALL_OLD"}`)
	if str(asM(asM(old["Attributes"])["extra"])["S"]) != "keep" {
		t.Fatalf("ALL_OLD %v", old)
	}
	_, neu := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"UpdateExpression":"SET extra = :e","ExpressionAttributeValues":{":e":{"S":"newer"}},"ReturnValues":"UPDATED_NEW"}`)
	if str(asM(asM(neu["Attributes"])["extra"])["S"]) != "newer" || neu["Attributes"].(map[string]any)["id"] != nil {
		t.Fatalf("UPDATED_NEW %v", neu)
	}
	call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"UpdateExpression":"REMOVE extra"}`)
	_, got := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"ProjectionExpression":"id, n"}`)
	item := asM(got["Item"])
	if item["extra"] != nil || item["id"] == nil {
		t.Fatalf("projection %v", got)
	}

	_, page1 := call("Scan", `{"TableName":"T","Limit":1}`)
	if len(asItems(page1)) != 1 || page1["LastEvaluatedKey"] == nil {
		t.Fatalf("page1 %v", page1)
	}
	lek, _ := json.Marshal(page1["LastEvaluatedKey"])
	_, page2 := call("Scan", `{"TableName":"T","Limit":1,"ExclusiveStartKey":`+string(lek)+`}`)
	if len(asItems(page2)) != 1 {
		t.Fatalf("page2 %v", page2)
	}
	if ids(page1)[0] == ids(page2)[0] {
		t.Fatalf("pagination repeated %v %v", page1, page2)
	}

	_, gsi := call("Query", `{"TableName":"T","IndexName":"gsi1","KeyConditionExpression":"gsi = :g","ExpressionAttributeValues":{":g":{"S":"x"}}}`)
	if len(asItems(gsi)) != 2 {
		t.Fatalf("gsi count %v", gsi)
	}
	for _, it := range asItems(gsi) {
		if asM(it)["extra"] != nil {
			t.Fatalf("KEYS_ONLY leaked extra %v", it)
		}
		if asM(it)["id"] == nil || asM(it)["gsi"] == nil {
			t.Fatalf("KEYS_ONLY missing keys %v", it)
		}
	}
	_, qp1 := call("Query", `{"TableName":"T","IndexName":"gsi1","KeyConditionExpression":"gsi = :g","ExpressionAttributeValues":{":g":{"S":"x"}},"Limit":1}`)
	if len(asItems(qp1)) != 1 || qp1["LastEvaluatedKey"] == nil {
		t.Fatalf("query page1 %v", qp1)
	}
	qlek, _ := json.Marshal(qp1["LastEvaluatedKey"])
	_, qp2 := call("Query", `{"TableName":"T","IndexName":"gsi1","KeyConditionExpression":"gsi = :g","ExpressionAttributeValues":{":g":{"S":"x"}},"Limit":1,"ExclusiveStartKey":`+string(qlek)+`}`)
	if len(asItems(qp2)) != 1 {
		t.Fatalf("query page2 %v", qp2)
	}
	if ids(qp1)[0] == ids(qp2)[0] {
		t.Fatalf("query pagination repeated %v %v", qp1, qp2)
	}

	_, qle := call("Query", `{"TableName":"T","KeyConditionExpression":"id <= :id","ExpressionAttributeValues":{":id":{"S":"a"}}}`)
	if len(ids(qle)) != 1 || ids(qle)[0] != "a" {
		t.Fatalf("<= %v", qle)
	}
	_, qge := call("Query", `{"TableName":"T","KeyConditionExpression":"id >= :id","ExpressionAttributeValues":{":id":{"S":"c"}}}`)
	if len(ids(qge)) != 1 || ids(qge)[0] != "c" {
		t.Fatalf(">= %v", qge)
	}
	_, sand := call("Scan", `{"TableName":"T","FilterExpression":"attribute_exists(n) AND contains(extra, :c)","ExpressionAttributeValues":{":c":{"S":"y"}}}`)
	if len(ids(sand)) != 1 {
		t.Fatalf("and/contains %v", sand)
	}
	_, sin := call("Scan", `{"TableName":"T","FilterExpression":"#i IN (:a, :z)","ExpressionAttributeNames":{"#i":"id"},"ExpressionAttributeValues":{":a":{"S":"a"},":z":{"S":"z"}}}`)
	if len(ids(sin)) != 1 || ids(sin)[0] != "a" {
		t.Fatalf("IN/names %v", sin)
	}
	_, sor := call("Scan", `{"TableName":"T","FilterExpression":"id = :a OR id = :nope","ExpressionAttributeValues":{":a":{"S":"a"},":nope":{"S":"nope"}}}`)
	if len(ids(sor)) != 1 || ids(sor)[0] != "a" {
		t.Fatalf("OR %v", sor)
	}
	_, snot := call("Scan", `{"TableName":"T","FilterExpression":"NOT attribute_exists(nogo) AND attribute_not_exists(nogo)"}`)
	if len(ids(snot)) < 1 {
		t.Fatalf("NOT/attribute_not_exists %v", snot)
	}
	_, stype := call("Scan", `{"TableName":"T","FilterExpression":"attribute_type(id, S) AND size(id) > :z","ExpressionAttributeValues":{":z":{"N":"0"}}}`)
	if len(ids(stype)) < 1 {
		t.Fatalf("attribute_type/size %v", stype)
	}
	_, spar := call("Scan", `{"TableName":"T","FilterExpression":"(id = :a) OR (id = :z)","ExpressionAttributeValues":{":a":{"S":"a"},":z":{"S":"z"}}}`)
	if len(ids(spar)) != 1 || ids(spar)[0] != "a" {
		t.Fatalf("parens %v", spar)
	}

	call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"ADD tags :t","ExpressionAttributeValues":{":t":{"SS":["red"]}}}`)
	call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"DELETE tags :t","ExpressionAttributeValues":{":t":{"SS":["red"]}}}`)
	_, afterDel := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"b"}}}`)
	tags := asM(asM(afterDel["Item"])["tags"])
	if ss, _ := tags["SS"].([]any); len(ss) != 0 {
		t.Fatalf("DELETE set %v", afterDel)
	}
	call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"SET maybe = if_not_exists(maybe, :v), n = n + :one, lst = :empty","ExpressionAttributeValues":{":v":{"S":"def"},":one":{"N":"1"},":empty":{"L":[]}}}`)
	_, up := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"b"}}}`)
	if str(asM(asM(up["Item"])["maybe"])["S"]) != "def" {
		t.Fatalf("if_not_exists %v", up)
	}
	nBefore := str(asM(asM(up["Item"])["n"])["N"])
	call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"SET lst = list_append(lst, :l), n = n - :one","ExpressionAttributeValues":{":l":{"L":[{"S":"x"}]},":one":{"N":"1"}}}`)
	_, up2 := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"b"}}}`)
	lst, _ := asM(asM(up2["Item"])["lst"])["L"].([]any)
	if len(lst) != 1 || str(asM(lst[0])["S"]) != "x" {
		t.Fatalf("list_append %v", up2)
	}
	if str(asM(asM(up2["Item"])["n"])["N"]) == nBefore {
		t.Fatalf("n - :one no-op %v -> %v", nBefore, up2)
	}
	call("PutItem", `{"TableName":"T","Item":{"id":{"S":"nest"},"doc":{"M":{"k":{"S":"v"},"hide":{"S":"no"}}}}}`)
	_, fullNest := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"nest"}}}`)
	if asM(fullNest["Item"])["doc"] == nil {
		t.Fatalf("put nest %v", fullNest)
	}
	_, nest := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"nest"}},"ProjectionExpression":"doc.k"}`)
	nitem := asM(nest["Item"])
	if nitem["id"] != nil {
		t.Fatalf("nested projection leaked id %v", nest)
	}
	nestJSON := fmtJSON(nitem)
	if !strings.Contains(nestJSON, `"v"`) || strings.Contains(nestJSON, "hide") {
		t.Fatalf("nested projection %v", nest)
	}

	_, none := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"SET extra = :e","ExpressionAttributeValues":{":e":{"S":"z"}},"ReturnValues":"NONE"}`)
	if none["Attributes"] != nil && len(asM(none["Attributes"])) != 0 {
		t.Fatalf("NONE %v", none)
	}
	_, allNew := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"SET extra = :e","ExpressionAttributeValues":{":e":{"S":"zz"}},"ReturnValues":"ALL_NEW"}`)
	if str(asM(asM(allNew["Attributes"])["id"])["S"]) != "b" {
		t.Fatalf("ALL_NEW %v", allNew)
	}
	_, updOld := call("UpdateItem", `{"TableName":"T","Key":{"id":{"S":"b"}},"UpdateExpression":"SET extra = :e","ExpressionAttributeValues":{":e":{"S":"zzz"}},"ReturnValues":"UPDATED_OLD"}`)
	if str(asM(asM(updOld["Attributes"])["extra"])["S"]) != "zz" || asM(updOld["Attributes"])["id"] != nil {
		t.Fatalf("UPDATED_OLD %v", updOld)
	}

	_, desc := call("DescribeTable", `{"TableName":"T"}`)
	if str(asM(desc["Table"])["TableName"]) != "T" {
		t.Fatalf("describe %v", desc)
	}
	_, listed := call("ListTables", `{}`)
	names, _ := listed["TableNames"].([]any)
	if len(names) == 0 {
		t.Fatalf("list tables %v", listed)
	}
	call("TagResource", `{"ResourceArn":"arn:t","Tags":[{"Key":"k","Value":"v"}]}`)
	_, tagsOut := call("ListTagsOfResource", `{"ResourceArn":"arn:t"}`)
	if tagsOut["Tags"] == nil {
		t.Fatalf("tags %v", tagsOut)
	}
	call("UntagResource", `{"ResourceArn":"arn:t"}`)
	_, ttl := call("DescribeTimeToLive", `{"TableName":"T"}`)
	if asM(ttl["TimeToLiveDescription"])["TimeToLiveStatus"] != "DISABLED" {
		t.Fatalf("ttl %v", ttl)
	}
	_, cb := call("DescribeContinuousBackups", `{"TableName":"T"}`)
	if str(asM(asM(cb["ContinuousBackupsDescription"])["PointInTimeRecoveryDescription"])["PointInTimeRecoveryStatus"]) != "DISABLED" {
		t.Fatalf("backups %v", cb)
	}
	call("UpdateTable", `{"TableName":"T","GlobalSecondaryIndexUpdates":[{"Create":{"IndexName":"gsi2","KeySchema":[{"AttributeName":"n","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}}]}`)
	call("BatchWriteItem", `{"RequestItems":{"T":[{"PutRequest":{"Item":{"id":{"S":"d"},"n":{"N":"2"}}}}]}}`)
	_, bg := call("BatchGetItem", `{"RequestItems":{"T":{"Keys":[{"id":{"S":"d"}}]}}}`)
	if len(asM(bg["Responses"])) == 0 {
		t.Fatalf("batch get %v", bg)
	}
	call("TransactWriteItems", `{"TransactItems":[{"Put":{"TableName":"T","Item":{"id":{"S":"e"},"n":{"N":"3"}}}}]}`)
	_, tg := call("TransactGetItems", `{"TransactItems":[{"Get":{"TableName":"T","Key":{"id":{"S":"e"}}}}]}`)
	if tg["Responses"] == nil {
		t.Fatalf("transact get %v", tg)
	}
	_, gget := call("GetItem", `{"TableName":"T","Key":{"id":{"S":"a"}},"ConsistentRead":true}`)
	if gget["Item"] == nil {
		t.Fatalf("consistent read %v", gget)
	}

	call("CreateTable", `{"TableName":"L","KeySchema":[{"AttributeName":"hk","KeyType":"HASH"},{"AttributeName":"rk","KeyType":"RANGE"}],"LocalSecondaryIndexes":[{"IndexName":"lsi1","KeySchema":[{"AttributeName":"hk","KeyType":"HASH"},{"AttributeName":"lsk","KeyType":"RANGE"}],"Projection":{"ProjectionType":"INCLUDE","NonKeyAttributes":["keepme"]}}]}`)
	call("PutItem", `{"TableName":"L","Item":{"hk":{"S":"h"},"rk":{"S":"r1"},"lsk":{"S":"s"},"keepme":{"S":"yes"},"dropme":{"S":"no"}}}`)
	_, lsi := call("Query", `{"TableName":"L","IndexName":"lsi1","KeyConditionExpression":"hk = :h","ExpressionAttributeValues":{":h":{"S":"h"}}}`)
	if len(asItems(lsi)) != 1 {
		t.Fatalf("lsi %v", lsi)
	}
	if asM(asItems(lsi)[0])["dropme"] != nil || asM(asItems(lsi)[0])["keepme"] == nil {
		t.Fatalf("INCLUDE %v", lsi)
	}
	call("CreateTable", `{"TableName":"G","KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],"GlobalSecondaryIndexes":[{"IndexName":"gall","KeySchema":[{"AttributeName":"gsi","KeyType":"HASH"}],"Projection":{"ProjectionType":"ALL"}}]}`)
	call("PutItem", `{"TableName":"G","Item":{"id":{"S":"1"},"gsi":{"S":"x"},"extra":{"S":"full"}}}`)
	_, gall := call("Query", `{"TableName":"G","IndexName":"gall","KeyConditionExpression":"gsi = :g","ExpressionAttributeValues":{":g":{"S":"x"}}}`)
	if asM(asItems(gall)[0])["extra"] == nil {
		t.Fatalf("GSI ALL %v", gall)
	}
	call("DeleteTable", `{"TableName":"G"}`)
	code, gone := call("DescribeTable", `{"TableName":"G"}`)
	if code != 400 && gone["Table"] != nil {
		t.Fatalf("delete table %d %v", code, gone)
	}

	call("UpdateTimeToLive", `{"TableName":"T","TimeToLiveSpecification":{"Enabled":true,"AttributeName":"exp"}}`)
	_, ttlOn := call("DescribeTimeToLive", `{"TableName":"T"}`)
	if asM(ttlOn["TimeToLiveDescription"])["TimeToLiveStatus"] != "ENABLED" {
		t.Fatalf("ttl on %v", ttlOn)
	}
	call("UpdateContinuousBackups", `{"TableName":"T","PointInTimeRecoverySpecification":{"PointInTimeRecoveryEnabled":true}}`)
	_, pitr := call("DescribeContinuousBackups", `{"TableName":"T"}`)
	if str(asM(asM(pitr["ContinuousBackupsDescription"])["PointInTimeRecoveryDescription"])["PointInTimeRecoveryStatus"]) != "ENABLED" {
		t.Fatalf("pitr %v", pitr)
	}
	_, ep := call("DescribeEndpoints", `{}`)
	if ep["Endpoints"] == nil {
		t.Fatalf("endpoints %v", ep)
	}
	_, lim := call("DescribeLimits", `{}`)
	if lim["AccountMaxReadCapacityUnits"] == nil {
		t.Fatalf("limits %v", lim)
	}
	arn := "arn:aws:dynamodb:us-east-1:000000000000:table/T"
	call("PutResourcePolicy", `{"ResourceArn":"`+arn+`","Policy":"{\"Version\":\"2012-10-17\"}"}`)
	_, pol := call("GetResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	if str(pol["Policy"]) == "" {
		t.Fatalf("policy %v", pol)
	}
	call("DeleteResourcePolicy", `{"ResourceArn":"`+arn+`"}`)
	_, bak := call("CreateBackup", `{"TableName":"T","BackupName":"b1"}`)
	bArn := str(asM(bak["BackupDetails"])["BackupArn"])
	if bArn == "" {
		t.Fatalf("backup %v", bak)
	}
	_, bl := call("ListBackups", `{}`)
	if bl["BackupSummaries"] == nil {
		t.Fatalf("list backups %v", bl)
	}
	_, bd := call("DescribeBackup", `{"BackupArn":"`+bArn+`"}`)
	if bd["BackupDescription"] == nil {
		t.Fatalf("describe backup %v", bd)
	}
	call("RestoreTableFromBackup", `{"BackupArn":"`+bArn+`","TargetTableName":"Trestored"}`)
	_, rest := call("DescribeTable", `{"TableName":"Trestored"}`)
	if str(asM(rest["Table"])["TableName"]) != "Trestored" && rest["Table"] == nil {
		t.Fatalf("restore %v", rest)
	}
	call("DeleteBackup", `{"BackupArn":"`+bArn+`"}`)
	call("EnableKinesisStreamingDestination", `{"TableName":"T","StreamArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
	_, kd := call("DescribeKinesisStreamingDestination", `{"TableName":"T"}`)
	if kd["KinesisDataStreamDestinations"] == nil {
		t.Fatalf("kinesis dest %v", kd)
	}
	call("DisableKinesisStreamingDestination", `{"TableName":"T","StreamArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s"}`)
}

func asItems(out map[string]any) []any {
	s, _ := out["Items"].([]any)
	return s
}

func asM(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

func fmtJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
