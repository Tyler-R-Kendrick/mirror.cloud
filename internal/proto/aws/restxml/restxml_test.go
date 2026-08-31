package restxml

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

func TestRouteNameQueryOps(t *testing.T) {
	cases := []struct {
		method, path, copySrc, want string
	}{
		{http.MethodPut, "/b?tagging", "", "PutBucketTagging"},
		{http.MethodGet, "/b?tagging", "", "GetBucketTagging"},
		{http.MethodDelete, "/b?tagging", "", "DeleteBucketTagging"},
		{http.MethodPut, "/b/k?tagging", "", "PutObjectTagging"},
		{http.MethodGet, "/b/k?tagging", "", "GetObjectTagging"},
		{http.MethodDelete, "/b/k?tagging", "", "DeleteObjectTagging"},
		{http.MethodPut, "/b?notification", "", "PutBucketNotificationConfiguration"},
		{http.MethodGet, "/b?notification", "", "GetBucketNotificationConfiguration"},
		{http.MethodPut, "/b?versioning", "", "PutBucketVersioning"},
		{http.MethodGet, "/b?versioning", "", "GetBucketVersioning"},
		{http.MethodPut, "/b?acl", "", "PutBucketAcl"},
		{http.MethodGet, "/b/k?acl", "", "GetObjectAcl"},
		{http.MethodDelete, "/b?policy", "", "DeleteBucketPolicy"},
		{http.MethodGet, "/b?policy", "", "GetBucketPolicy"},
		{http.MethodPut, "/b?encryption", "", "PutBucketEncryption"},
		{http.MethodDelete, "/b?encryption", "", "DeleteBucketEncryption"},
		{http.MethodPut, "/b?cors", "", "PutBucketCors"},
		{http.MethodDelete, "/b?cors", "", "DeleteBucketCors"},
		{http.MethodDelete, "/b?website", "", "DeleteBucketWebsite"},
		{http.MethodGet, "/b?logging", "", "GetBucketLogging"},
		{http.MethodDelete, "/b?lifecycle", "", "DeleteBucketLifecycle"},
		{http.MethodDelete, "/b?replication", "", "DeleteBucketReplication"},
		{http.MethodPost, "/b?session", "", "CreateSession"},
		{http.MethodPost, "/b/k?select", "", "SelectObjectContent"},
		{http.MethodGet, "/b/k?torrent", "", "GetObjectTorrent"},
		{http.MethodPut, "/b?abac", "", "PutBucketAbac"},
		{http.MethodPut, "/b?metadataTable", "", "CreateBucketMetadataTableConfiguration"},
		{http.MethodPost, "/b?metadataConfiguration&inventory", "", "UpdateBucketMetadataInventoryTableConfiguration"},
		{http.MethodPost, "/b?metadataConfiguration&journal", "", "UpdateBucketMetadataJournalTableConfiguration"},
		{http.MethodPost, "/b?metadataConfiguration", "", "UpdateBucketMetadataAnnotationTableConfiguration"},
		{http.MethodDelete, "/b?metadata", "", "DeleteBucketMetadataConfiguration"},
		{http.MethodGet, "/b?annotation", "", "ListObjectAnnotations"},
		{http.MethodPut, "/b/k?annotation", "", "PutObjectAnnotation"},
		{http.MethodPost, "/b/k?rename", "", "RenameObject"},
		{http.MethodPut, "/b?object-lock", "", "PutObjectLockConfiguration"},
		{http.MethodGet, "/b?requestPayment", "", "GetBucketRequestPayment"},
		{http.MethodPut, "/b?accelerate", "", "PutBucketAccelerateConfiguration"},
		{http.MethodDelete, "/b?publicAccessBlock", "", "DeletePublicAccessBlock"},
		{http.MethodGet, "/b?ownershipControls", "", "GetBucketOwnershipControls"},
		{http.MethodGet, "/b?policyStatus", "", "GetBucketPolicyStatus"},
		{http.MethodGet, "/b/k?attributes", "", "GetObjectAttributes"},
		{http.MethodPut, "/b/k?legal-hold", "", "PutObjectLegalHold"},
		{http.MethodGet, "/b/k?retention", "", "GetObjectRetention"},
		{http.MethodPost, "/b/k?restore", "", "RestoreObject"},
		{http.MethodPut, "/b?analytics&id=a", "", "PutBucketAnalyticsConfiguration"},
		{http.MethodGet, "/b?analytics", "", "ListBucketAnalyticsConfigurations"},
		{http.MethodDelete, "/b?inventory&id=a", "", "DeleteBucketInventoryConfiguration"},
		{http.MethodGet, "/b?inventory", "", "ListBucketInventoryConfigurations"},
		{http.MethodGet, "/b?metrics&id=a", "", "GetBucketMetricsConfiguration"},
		{http.MethodGet, "/b?metrics", "", "ListBucketMetricsConfigurations"},
		{http.MethodPut, "/b?intelligent-tiering&id=a", "", "PutBucketIntelligentTieringConfiguration"},
		{http.MethodGet, "/b?intelligent-tiering", "", "ListBucketIntelligentTieringConfigurations"},
		{http.MethodGet, "/b?location", "", "GetBucketLocation"},
		{http.MethodPost, "/b?delete", "", "DeleteObjects"},
		{http.MethodPost, "/b/k?uploads", "", "CreateMultipartUpload"},
		{http.MethodGet, "/b?uploads", "", "ListMultipartUploads"},
		{http.MethodGet, "/b/k?uploadId=abc", "", "ListParts"},
		{http.MethodPost, "/b/k?uploadId=abc", "", "CompleteMultipartUpload"},
		{http.MethodDelete, "/b/k?uploadId=abc", "", "AbortMultipartUpload"},
		{http.MethodPut, "/b/k?partNumber=1&uploadId=abc", "", "UploadPart"},
		{http.MethodPut, "/b/k?partNumber=2&uploadId=abc", "b/src", "UploadPartCopy"},
		{http.MethodPut, "/b/k", "b/src", "CopyObject"},
		{http.MethodPut, "/b", "", "CreateBucket"},
		{http.MethodPut, "/b/k", "", "PutObject"},
		{http.MethodGet, "/b/k", "", "GetObject"},
		{http.MethodHead, "/b/k", "", "HeadObject"},
		{http.MethodDelete, "/b/k", "", "DeleteObject"},
		{http.MethodHead, "/b", "", "HeadBucket"},
		{http.MethodDelete, "/b", "", "DeleteBucket"},
		{http.MethodGet, "/", "", "ListBuckets"},
		{http.MethodGet, "/b?list-type=1", "", "ListObjects"},
		{http.MethodGet, "/b", "", "ListObjectsV2"},
		{http.MethodGet, "/b?versions", "", "ListObjectVersions"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, "http://127.0.0.1"+tc.path, nil)
		if tc.copySrc != "" {
			r.Header.Set("x-amz-copy-source", tc.copySrc)
		}
		if got := RouteName(r); got != tc.want {
			t.Errorf("%s %s copy=%q: got %q want %q", tc.method, tc.path, tc.copySrc, got, tc.want)
		}
	}
}

func TestDecodeDeleteObjectsXML(t *testing.T) {
	body := `<Delete><Object><Key>k</Key><VersionId>v1</VersionId></Object><Object><Key>src</Key></Object><Quiet>true</Quiet></Delete>`
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/b?delete", strings.NewReader(body))
	op := &model.Operation{Name: "DeleteObjects"}
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, op, r)
	if err != nil {
		t.Fatal(err)
	}
	objs, _ := req.Input["Objects"].([]any)
	if len(objs) != 2 {
		t.Fatalf("Objects %v", req.Input)
	}
	if str(objs[0].(map[string]any)["Key"]) != "k" {
		t.Fatalf("first key %v", objs[0])
	}
	if str(objs[0].(map[string]any)["VersionId"]) != "v1" || req.Input["Quiet"] != true || req.Input["_body"] != body {
		t.Fatalf("version and quiet %v", req.Input)
	}
}

func TestDecodeCreateBucketXML(t *testing.T) {
	body := `<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint><Tags><Tag><Key>team</Key><Value>storage</Value></Tag><Tag><Key>env</Key><Value>test</Value></Tag></Tags></CreateBucketConfiguration>`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/tagged", strings.NewReader(body))
	req, err := (Codec{}).Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "CreateBucket"}, r)
	if err != nil {
		t.Fatal(err)
	}
	configuration := req.Input["CreateBucketConfiguration"].(map[string]any)
	tags := configuration["Tags"].([]any)
	if req.Input["LocationConstraint"] != "us-west-2" || configuration["LocationConstraint"] != "us-west-2" || len(tags) != 2 || tags[0].(map[string]any)["Key"] != "team" || tags[1].(map[string]any)["Value"] != "test" {
		t.Fatalf("configuration = %#v", configuration)
	}
}

func TestEncodeDeleteObjectsXML(t *testing.T) {
	w := httptest.NewRecorder()
	err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "DeleteObjects"}, w, &spi.Response{Output: map[string]any{
		"Deleted": []any{map[string]any{"Key": "k", "VersionId": "v1"}},
		"Errors":  []any{map[string]any{"Key": "missing", "Code": "NoSuchVersion"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := `<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Deleted><Key>k</Key><VersionId>v1</VersionId></Deleted><Error><Code>NoSuchVersion</Code><Key>missing</Key></Error></DeleteResult>`
	if w.Body.String() != want {
		t.Fatalf("body %q want %q", w.Body.String(), want)
	}
}

func TestDecodeObjectLockXML(t *testing.T) {
	tests := []struct {
		op, body, field string
		want            map[string]any
	}{
		{"PutObjectLegalHold", `<LegalHold><Status>ON</Status></LegalHold>`, "LegalHold", map[string]any{"Status": "ON"}},
		{"PutObjectRetention", `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>`, "Retention", map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "2030-01-01T00:00:00Z"}},
		{"PutObjectLockConfiguration", `<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Years>2</Years></DefaultRetention></Rule></ObjectLockConfiguration>`, "ObjectLockConfiguration", map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": map[string]any{"Mode": "COMPLIANCE", "Years": 2}}}},
	}
	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b/k?versionId=v1", strings.NewReader(test.body))
			req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: test.op}, r)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(req.Input[test.field], test.want) || req.Input["VersionId"] != "v1" {
				t.Fatalf("input %#v", req.Input)
			}
		})
	}
}

func TestEncodeObjectLockXML(t *testing.T) {
	tests := []struct {
		op, root string
		value    map[string]any
	}{
		{"GetObjectLockConfiguration", "ObjectLockConfiguration", map[string]any{"ObjectLockEnabled": "Enabled"}},
		{"GetObjectLegalHold", "LegalHold", map[string]any{"Status": "ON"}},
		{"GetObjectRetention", "Retention", map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "2030-01-01T00:00:00Z"}},
	}
	for _, test := range tests {
		t.Run(test.op, func(t *testing.T) {
			w := httptest.NewRecorder()
			err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: test.op}, w, &spi.Response{Output: map[string]any{test.root: test.value}})
			if err != nil {
				t.Fatal(err)
			}
			want := `<?xml version="1.0" encoding="UTF-8"?><` + test.root + ` xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
			if !strings.HasPrefix(w.Body.String(), want) || !strings.HasSuffix(w.Body.String(), "</"+test.root+">") {
				t.Fatalf("body %q", w.Body.String())
			}
		})
	}
}

func TestDecodeCompleteMultipartUploadXML(t *testing.T) {
	body := `<CompleteMultipartUpload><Part><ETag>"first"</ETag><PartNumber>1</PartNumber></Part><Part><ETag>"third"</ETag><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>`
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/b/k?uploadId=id", strings.NewReader(body))
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "CompleteMultipartUpload"}, r)
	if err != nil {
		t.Fatal(err)
	}
	parts := req.Input["MultipartUpload"].(map[string]any)["Parts"].([]any)
	if len(parts) != 2 || parts[1].(map[string]any)["PartNumber"] != 3 || parts[1].(map[string]any)["ETag"] != `"third"` {
		t.Fatalf("parts = %#v", parts)
	}
}

func TestDecodeRestoreObjectXML(t *testing.T) {
	body := `<RestoreRequest><Days>3</Days></RestoreRequest>`
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/b/k?restore", strings.NewReader(body))
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "RestoreObject"}, r)
	if err != nil {
		t.Fatal(err)
	}
	if req.Input["Days"] != 3 || req.Input["RestoreRequest"].(map[string]any)["Days"] != 3 {
		t.Fatalf("restore request = %#v", req.Input)
	}
}

func TestDecodeTaggingXML(t *testing.T) {
	body := `<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?tagging", strings.NewReader(body))
	op := &model.Operation{Name: "PutBucketTagging"}
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, op, r)
	if err != nil {
		t.Fatal(err)
	}
	tags, _ := req.Input["TagSet"].([]any)
	if len(tags) != 1 || str(tags[0].(map[string]any)["Key"]) != "a" {
		t.Fatalf("TagSet %v", req.Input)
	}
	r = httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?tagging", strings.NewReader(`<Tagging/>`))
	req, err = Codec{}.Decode(&model.Service{ID: "aws.s3"}, op, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := req.Input["TagSet"]; ok {
		t.Fatalf("missing TagSet decoded as %#v", req.Input["TagSet"])
	}
}

func TestRESTXMLServiceRoutes(t *testing.T) {
	codec := Codec{}
	if codec.Protocol() != model.ProtoRESTXML {
		t.Fatal(codec.Protocol())
	}
	for _, test := range []struct{ service, method, path, want string }{
		{"aws.route53", http.MethodPost, "/2013-04-01/hostedzone/Z/rrset", "ChangeResourceRecordSets"},
		{"aws.route53", http.MethodGet, "/2013-04-01/hostedzone/Z/rrset", "ListResourceRecordSets"},
		{"aws.route53", http.MethodPost, "/2013-04-01/hostedzone", "CreateHostedZone"},
		{"aws.route53", http.MethodGet, "/2013-04-01/hostedzone", "ListHostedZones"},
		{"aws.route53", http.MethodDelete, "/2013-04-01/hostedzone/Z", "DeleteHostedZone"},
		{"aws.route53", http.MethodGet, "/2013-04-01/hostedzone/Z", "GetHostedZone"},
		{"aws.cloudfront", http.MethodPost, "/2020-05-31/distribution/D/invalidation", "CreateInvalidation"},
		{"aws.cloudfront", http.MethodGet, "/2020-05-31/distribution/D/invalidation", "ListInvalidations"},
		{"aws.cloudfront", http.MethodGet, "/2020-05-31/distribution/D/invalidation/I", "GetInvalidation"},
		{"aws.cloudfront", http.MethodPut, "/2020-05-31/distribution/D/config", "UpdateDistribution"},
		{"aws.cloudfront", http.MethodGet, "/2020-05-31/distribution/D/config", "GetDistributionConfig"},
		{"aws.cloudfront", http.MethodPost, "/2020-05-31/distribution", "CreateDistribution"},
		{"aws.cloudfront", http.MethodGet, "/2020-05-31/distribution", "ListDistributions"},
		{"aws.cloudfront", http.MethodDelete, "/2020-05-31/distribution/D", "DeleteDistribution"},
		{"aws.cloudfront", http.MethodGet, "/2020-05-31/distribution/D", "GetDistribution"},
	} {
		op, err := codec.Route(&model.Service{ID: test.service}, httptest.NewRequest(test.method, test.path, nil))
		if err != nil || op.Name != test.want {
			t.Errorf("%s %s %s: %#v %v, want %s", test.service, test.method, test.path, op, err, test.want)
		}
	}
	svc := &model.Service{ID: "aws.s3", Operations: []model.Operation{{Name: "Modeled"}, {Name: "Fallback"}}}
	for _, test := range []struct {
		request *http.Request
		want    string
	}{
		{httptest.NewRequest(http.MethodPost, "/?Action=Modeled", nil), "Modeled"},
		{httptest.NewRequest(http.MethodPost, "/?Action=Synthetic", nil), "Synthetic"},
		{func() *http.Request {
			r := httptest.NewRequest(http.MethodOptions, "/unknown", nil)
			r.Header.Set("X-Mirror-Operation", "Modeled")
			return r
		}(), "Modeled"},
		{httptest.NewRequest(http.MethodOptions, "/unknown", nil), "Modeled"},
	} {
		op, err := codec.Route(svc, test.request)
		if err != nil || op.Name != test.want {
			t.Fatalf("generic route %#v %v, want %s", op, err, test.want)
		}
	}
	if _, err := codec.Route(&model.Service{ID: "aws.empty"}, httptest.NewRequest(http.MethodOptions, "/unknown", nil)); err == nil {
		t.Fatal("routed empty unknown service")
	}
	virtual := httptest.NewRequest(http.MethodGet, "https://bucket.s3.us-east-1.amazonaws.com/key", nil)
	if got := RouteName(virtual); got != "GetObject" {
		t.Fatalf("virtual-host route %q", got)
	}
}

func TestRESTXMLServiceDecodeContracts(t *testing.T) {
	codec := Codec{}
	route53 := &model.Service{ID: "aws.route53"}
	request := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/Z1/rrset", strings.NewReader("<Change/>"))
	decoded, err := codec.Decode(route53, &model.Operation{Name: "ChangeResourceRecordSets"}, request)
	if err != nil || decoded.Input["Id"] != "Z1" || decoded.Input["_body"] != "<Change/>" {
		t.Fatalf("Route53 decode %#v %v", decoded, err)
	}
	cloudfront := &model.Service{ID: "aws.cloudfront"}
	request = httptest.NewRequest(http.MethodGet, "/2020-05-31/distribution/D/invalidation/I?Marker=m", strings.NewReader("<Invalidation/>"))
	decoded, err = codec.Decode(cloudfront, &model.Operation{Name: "GetInvalidation"}, request)
	if err != nil || decoded.Input["Id"] != "D" || decoded.Input["InvalidationId"] != "I" || decoded.Input["Marker"] != "m" || decoded.Input["_body"] != "<Invalidation/>" {
		t.Fatalf("CloudFront decode %#v %v", decoded, err)
	}

	s3 := &model.Service{ID: "aws.s3"}
	request = httptest.NewRequest(http.MethodPut, "https://bucket.s3.us-east-1.amazonaws.com/key?partNumber=1", strings.NewReader("payload"))
	request.Header.Set("x-amz-copy-source", "/source/object")
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutObject"}, request)
	if err != nil || decoded.Input["Bucket"] != "bucket" || decoded.Input["Key"] != "key" || decoded.Input["CopySource"] != "source/object" || decoded.Input["partNumber"] != "1" {
		t.Fatalf("S3 stream decode %#v %v", decoded, err)
	}
	body, _ := io.ReadAll(decoded.Body)
	if string(body) != "payload" {
		t.Fatalf("stream body %q", body)
	}
	for _, test := range []struct{ operation, body, key, want string }{
		{"CreateBucket", `<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>`, "LocationConstraint", "us-west-2"},
		{"PutBucketPolicy", `{"allow":true}`, "Policy", `{"allow":true}`},
		{"PutBucketCors", `<Cors/>`, "Document", `<Cors/>`},
		{"PutBucketVersioning", `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`, "Status", "Enabled"},
		{"PutBucketVersioning", `<broken`, "_body", `<broken`},
		{"Unknown", `<Other/>`, "_body", `<Other/>`},
	} {
		request = httptest.NewRequest(http.MethodPut, "/bucket", strings.NewReader(test.body))
		decoded, err = codec.Decode(s3, &model.Operation{Name: test.operation}, request)
		if err != nil || decoded.Input[test.key] != test.want {
			t.Errorf("%s decode %#v %v", test.operation, decoded, err)
		}
	}
	notification := `<NotificationConfiguration><QueueConfiguration><Queue>arn:q</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration><TopicConfiguration><Topic>arn:t</Topic><Event>s3:ObjectRemoved:*</Event></TopicConfiguration></NotificationConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketNotificationConfiguration"}, httptest.NewRequest(http.MethodPut, "/bucket?notification", strings.NewReader(notification)))
	if err != nil || len(decoded.Input["QueueConfigurations"].([]any)) != 1 || len(decoded.Input["TopicConfigurations"].([]any)) != 1 {
		t.Fatalf("notification decode %#v %v", decoded, err)
	}
}

func TestPostObjectProtocolContract(t *testing.T) {
	codec := Codec{}
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/bucket", strings.NewReader("multipart"))
	request.Header.Set("Content-Type", "multipart/form-data; boundary=boundary")
	op, err := codec.Route(&model.Service{ID: "aws.s3"}, request)
	if err != nil || op.Name != "PostObject" {
		t.Fatalf("route %#v %v", op, err)
	}
	decoded, err := codec.Decode(&model.Service{ID: "aws.s3"}, op, request)
	if err != nil || decoded.Body == nil {
		t.Fatalf("decode %#v %v", decoded, err)
	}
	w := httptest.NewRecorder()
	err = codec.Encode(&model.Service{ID: "aws.s3"}, op, w, &spi.Response{Status: http.StatusCreated, Output: map[string]any{"Location": "http://s3.test/bucket/key", "Bucket": "bucket", "Key": "key", "ETag": `"etag"`}})
	if err != nil || w.Code != http.StatusCreated || !strings.Contains(w.Body.String(), "<PostResponse>") || !strings.Contains(w.Body.String(), "<Key>key</Key>") {
		t.Fatalf("encode %d %q %v", w.Code, w.Body.String(), err)
	}
}

func TestRESTXMLEncodeAndFaultContracts(t *testing.T) {
	codec := Codec{}
	svc := &model.Service{ID: "aws.s3"}
	w := httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetObject"}, w, &spi.Response{Status: http.StatusPartialContent, Headers: http.Header{"ETag": {"one"}}, Stream: io.NopCloser(strings.NewReader("object"))}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusPartialContent || w.Header().Get("ETag") != "one" || w.Body.String() != "object" {
		t.Fatalf("stream response %d %#v %q", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "HeadBucket"}, w, &spi.Response{}); err != nil || w.Body.Len() != 0 {
		t.Fatalf("head response %v %q", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "PutObject"}, w, &spi.Response{}); err != nil || w.Body.Len() != 0 || w.Header().Get("Content-Type") != "application/xml" {
		t.Fatalf("empty response %v %#v %q", err, w.Header(), w.Body.String())
	}
	for _, test := range []struct{ operation, root string }{
		{"ListBuckets", "ListAllMyBucketsResult"},
		{"ListObjectsV2", "ListBucketResult"},
		{"Custom", "CustomResult"},
	} {
		w = httptest.NewRecorder()
		err := codec.Encode(svc, &model.Operation{Name: test.operation}, w, &spi.Response{Output: map[string]any{
			"Name": "a&<b>", "Items": []any{map[string]any{"Key": "one"}}, "Empty": nil,
		}})
		if err != nil || !strings.Contains(w.Body.String(), "<"+test.root+">") || !strings.Contains(w.Body.String(), "a&amp;&lt;b&gt;") || !strings.Contains(w.Body.String(), "<member><Key>one</Key></member>") {
			t.Errorf("%s response %v %s", test.operation, err, w.Body.String())
		}
	}
	w = httptest.NewRecorder()
	err := codec.Encode(svc, &model.Operation{Name: "ListBuckets"}, w, &spi.Response{Output: map[string]any{
		"Buckets": []any{map[string]any{"Name": "one", "CreationDate": "date", "BucketRegion": "us-west-2"}},
	}})
	if body := w.Body.String(); err != nil || !strings.Contains(body, "<Buckets><Bucket><BucketRegion>us-west-2</BucketRegion><CreationDate>date</CreationDate><Name>one</Name></Bucket></Buckets>") || strings.Contains(body, "<member>") {
		t.Fatalf("bucket list response %v %s", err, body)
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketLocation"}, w, &spi.Response{Output: map[string]any{"LocationConstraint": "EU"}}); err != nil || !strings.Contains(w.Body.String(), `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">EU</LocationConstraint>`) {
		t.Fatalf("bucket location response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, &model.Operation{Name: "Missing"}, w, spi.NotImplemented(svc.ID, "Missing", "emulate"), "r<&"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusNotImplemented || w.Header().Get("x-mirror-not-implemented") != "aws.s3.Missing" || !strings.Contains(w.Body.String(), "r&lt;&amp;") {
		t.Fatalf("fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
