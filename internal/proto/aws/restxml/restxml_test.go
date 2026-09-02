package restxml

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
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
		{http.MethodPut, "/b?logging", "", "PutBucketLogging"},
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

func TestBucketPolicyPayload(t *testing.T) {
	policy := `{"Version":"2012-10-17","Statement":[]}`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?policy", strings.NewReader(policy))
	decoded, err := (Codec{}).Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "PutBucketPolicy"}, r)
	if err != nil || decoded.Input["Policy"] != policy {
		t.Fatalf("decoded policy = %#v, err=%v", decoded.Input, err)
	}
	w := httptest.NewRecorder()
	err = (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetBucketPolicy"}, w, &spi.Response{Output: map[string]any{"Policy": policy}})
	if err != nil || w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" || w.Body.String() != policy {
		t.Fatalf("encoded policy = code %d headers=%v body=%q err=%v", w.Code, w.Header(), w.Body.String(), err)
	}
}

func TestBucketEncryptionXML(t *testing.T) {
	body := `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>key-id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?encryption", strings.NewReader(body))
	decoded, err := (Codec{}).Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "PutBucketEncryption"}, r)
	configuration := decoded.Input["ServerSideEncryptionConfiguration"].(map[string]any)
	rules := configuration["Rules"].([]any)
	rule := rules[0].(map[string]any)
	defaults := rule["ApplyServerSideEncryptionByDefault"].(map[string]any)
	if err != nil || len(rules) != 1 || defaults["SSEAlgorithm"] != "aws:kms" || defaults["KMSMasterKeyID"] != "key-id" || rule["BucketKeyEnabled"] != true {
		t.Fatalf("decoded encryption = %#v, err=%v", decoded.Input, err)
	}
	w := httptest.NewRecorder()
	err = (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetBucketEncryption"}, w, &spi.Response{Output: map[string]any{"Rules": rules}})
	want := `<?xml version="1.0" encoding="UTF-8"?><ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>key-id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>`
	if err != nil || w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/xml" || w.Body.String() != want {
		t.Fatalf("encoded encryption = code %d headers=%v body=%q err=%v", w.Code, w.Header(), w.Body.String(), err)
	}
	empty := httptest.NewRecorder()
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetBucketEncryption"}, empty, &spi.Response{Output: map[string]any{}}); err != nil || empty.Body.Len() != 0 {
		t.Fatalf("empty encryption = code %d body=%q err=%v", empty.Code, empty.Body.String(), err)
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

func TestLifecycleXML(t *testing.T) {
	body := `<LifecycleConfiguration><Rule><ID>expire</ID><Filter><And><Prefix>images/</Prefix><Tag><Key>class</Key><Value>temporary</Value></Tag><ObjectSizeGreaterThan>10</ObjectSizeGreaterThan></And></Filter><Status>Enabled</Status><Expiration><Days>7</Days></Expiration><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition><NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays></NoncurrentVersionExpiration><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?lifecycle", strings.NewReader(body))
	r.Header.Set("x-amz-transition-default-minimum-object-size", "varies_by_storage_class")
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "PutBucketLifecycleConfiguration"}, r)
	if err != nil {
		t.Fatal(err)
	}
	configuration := req.Input["LifecycleConfiguration"].(map[string]any)
	rules := configuration["Rules"].([]any)
	rule := rules[0].(map[string]any)
	and := rule["Filter"].(map[string]any)["And"].(map[string]any)
	if req.Input["TransitionDefaultMinimumObjectSize"] != "varies_by_storage_class" || rule["ID"] != "expire" || rule["Status"] != "Enabled" || and["Prefix"] != "images/" || and["ObjectSizeGreaterThan"] != int64(10) {
		t.Fatalf("decoded lifecycle = %#v", req.Input)
	}
	w := httptest.NewRecorder()
	response := &spi.Response{Headers: http.Header{"x-amz-transition-default-minimum-object-size": []string{"varies_by_storage_class"}}, Output: map[string]any{"Rules": rules}}
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetBucketLifecycleConfiguration"}, w, response); err != nil {
		t.Fatal(err)
	}
	if w.Header().Get("x-amz-transition-default-minimum-object-size") != "varies_by_storage_class" || !strings.Contains(w.Body.String(), "<LifecycleConfiguration") || strings.Contains(w.Body.String(), "<member>") || !strings.Contains(w.Body.String(), "<Rule>") || !strings.Contains(w.Body.String(), "<Transition>") || !strings.Contains(w.Body.String(), "<Tag>") {
		t.Fatalf("encoded lifecycle = headers %#v body %q", w.Header(), w.Body.String())
	}
}

func TestNamedConfigurationXML(t *testing.T) {
	tests := []struct {
		operation, query, field, body string
		want                          map[string]any
	}{
		{
			"PutBucketAnalyticsConfiguration", "analytics&id=analysis", "AnalyticsConfiguration",
			`<AnalyticsConfiguration><Id>analysis</Id><Filter><And><Prefix>logs/</Prefix><Tag><Key>team</Key><Value>storage</Value></Tag></And></Filter></AnalyticsConfiguration>`,
			map[string]any{"Id": "analysis", "Filter": map[string]any{"And": map[string]any{"Prefix": "logs/", "Tags": []any{map[string]any{"Key": "team", "Value": "storage"}}}}},
		},
		{
			"PutBucketInventoryConfiguration", "inventory&id=inventory", "InventoryConfiguration",
			`<InventoryConfiguration><Destination><S3BucketDestination><Bucket>arn:aws:s3:::destination</Bucket><Format>CSV</Format><Encryption><SSE-S3/></Encryption></S3BucketDestination></Destination><Id>inventory</Id><IncludedObjectVersions>All</IncludedObjectVersions><IsEnabled>true</IsEnabled><OptionalFields><Field>Size</Field><Field>ETag</Field></OptionalFields><Schedule><Frequency>Daily</Frequency></Schedule></InventoryConfiguration>`,
			map[string]any{"Destination": map[string]any{"S3BucketDestination": map[string]any{"Bucket": "arn:aws:s3:::destination", "Format": "CSV", "Encryption": map[string]any{"SSE-S3": ""}}}, "Id": "inventory", "IncludedObjectVersions": "All", "IsEnabled": true, "OptionalFields": []any{"Size", "ETag"}, "Schedule": map[string]any{"Frequency": "Daily"}},
		},
		{
			"PutBucketIntelligentTieringConfiguration", "intelligent-tiering&id=tiering", "IntelligentTieringConfiguration",
			`<IntelligentTieringConfiguration><Id>tiering</Id><Status>Enabled</Status><Tiering><Days>90</Days><AccessTier>ARCHIVE_ACCESS</AccessTier></Tiering><Tiering><Days>180</Days><AccessTier>DEEP_ARCHIVE_ACCESS</AccessTier></Tiering></IntelligentTieringConfiguration>`,
			map[string]any{"Id": "tiering", "Status": "Enabled", "Tierings": []any{map[string]any{"Days": 90, "AccessTier": "ARCHIVE_ACCESS"}, map[string]any{"Days": 180, "AccessTier": "DEEP_ARCHIVE_ACCESS"}}},
		},
		{
			"PutBucketMetricsConfiguration", "metrics&id=metrics", "MetricsConfiguration",
			`<MetricsConfiguration><Id>metrics</Id><Filter><Prefix>images/</Prefix></Filter></MetricsConfiguration>`,
			map[string]any{"Id": "metrics", "Filter": map[string]any{"Prefix": "images/"}},
		},
	}
	for _, test := range tests {
		t.Run(test.operation, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?"+test.query, strings.NewReader(test.body))
			req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: test.operation}, r)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(req.Input[test.field], test.want) {
				t.Fatalf("configuration = %#v", req.Input[test.field])
			}
		})
	}

	w := httptest.NewRecorder()
	inventory := tests[1]
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetBucketInventoryConfiguration"}, w, &spi.Response{Output: map[string]any{inventory.field: inventory.want}}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`<InventoryConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`, `<SSE-S3></SSE-S3>`, `<OptionalFields><Field>Size</Field><Field>ETag</Field></OptionalFields>`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Fatalf("inventory XML %q missing %q", w.Body.String(), want)
		}
	}

	w = httptest.NewRecorder()
	tiering := tests[2]
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "ListBucketIntelligentTieringConfigurations"}, w, &spi.Response{Output: map[string]any{"IsTruncated": false, "IntelligentTieringConfigurationList": []any{tiering.want}}}); err != nil {
		t.Fatal(err)
	}
	if body := w.Body.String(); strings.Contains(body, "<member>") || strings.Contains(body, "<Tierings>") || strings.Count(body, "<Tiering>") != 2 || !strings.Contains(body, `<ListBucketIntelligentTieringConfigurationsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`) {
		t.Fatalf("tiering XML %q", body)
	}

	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?analytics&id=a", strings.NewReader(`<MetricsConfiguration><Id>a</Id></MetricsConfiguration>`))
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "PutBucketAnalyticsConfiguration"}, r)
	if err != nil || req.Input["_body"] == nil || req.Input["AnalyticsConfiguration"] != nil {
		t.Fatalf("wrong root = %#v, %v", req.Input, err)
	}
}

func TestACLXML(t *testing.T) {
	body := `<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>000000000000</ID><DisplayName>mirror</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>000000000000</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>`
	r := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/b?acl", strings.NewReader(body))
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "PutBucketAcl"}, r)
	if err != nil {
		t.Fatal(err)
	}
	policy := req.Input["AccessControlPolicy"].(map[string]any)
	grants := policy["Grants"].([]any)
	if policy["Owner"].(map[string]any)["ID"] != "000000000000" || len(grants) != 2 || grants[0].(map[string]any)["Permission"] != "FULL_CONTROL" || grants[1].(map[string]any)["Grantee"].(map[string]any)["Type"] != "Group" {
		t.Fatalf("decoded ACL = %#v", policy)
	}
	w := httptest.NewRecorder()
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetObjectAcl"}, w, &spi.Response{Output: policy}); err != nil {
		t.Fatal(err)
	}
	encoded := w.Body.String()
	for _, want := range []string{`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`, `xsi:type="CanonicalUser"`, `<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>`, `<Permission>READ</Permission>`} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("encoded ACL %q missing %q", encoded, want)
		}
	}
	if strings.Contains(encoded, "<member>") || strings.Contains(encoded, "GetObjectAclResult") {
		t.Fatalf("encoded ACL = %q", encoded)
	}
}

func TestDecodeCompleteMultipartUploadXML(t *testing.T) {
	body := `<CompleteMultipartUpload><Part><ETag>"first"</ETag><PartNumber>1</PartNumber><ChecksumCRC32>crc32</ChecksumCRC32><ChecksumCRC32C>crc32c</ChecksumCRC32C><ChecksumCRC64NVME>crc64</ChecksumCRC64NVME><ChecksumMD5>md5</ChecksumMD5><ChecksumSHA1>sha1</ChecksumSHA1><ChecksumSHA256>sha256</ChecksumSHA256><ChecksumXXHASH64>xx64</ChecksumXXHASH64><ChecksumXXHASH3>xx3</ChecksumXXHASH3><ChecksumXXHASH128>xx128</ChecksumXXHASH128></Part><Part><ETag>"third"</ETag><PartNumber>3</PartNumber></Part></CompleteMultipartUpload>`
	r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/b/k?uploadId=id", strings.NewReader(body))
	req, err := Codec{}.Decode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "CompleteMultipartUpload"}, r)
	if err != nil {
		t.Fatal(err)
	}
	parts := req.Input["MultipartUpload"].(map[string]any)["Parts"].([]any)
	if len(parts) != 2 || parts[1].(map[string]any)["PartNumber"] != 3 || parts[1].(map[string]any)["ETag"] != `"third"` {
		t.Fatalf("parts = %#v", parts)
	}
	want := map[string]any{"ETag": `"first"`, "PartNumber": 1, "ChecksumCRC32": "crc32", "ChecksumCRC32C": "crc32c", "ChecksumCRC64NVME": "crc64", "ChecksumMD5": "md5", "ChecksumSHA1": "sha1", "ChecksumSHA256": "sha256", "ChecksumXXHASH64": "xx64", "ChecksumXXHASH3": "xx3", "ChecksumXXHASH128": "xx128"}
	if !reflect.DeepEqual(parts[0], want) {
		t.Fatalf("checksums = %#v", parts[0])
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
	preflight, err := codec.Route(&model.Service{ID: "aws.s3", Operations: []model.Operation{{Name: "GetObject"}}}, httptest.NewRequest(http.MethodOptions, "/bucket/key", nil))
	if err != nil || preflight.Name != "GetObject" {
		t.Fatalf("S3 preflight route %#v %v", preflight, err)
	}
	virtual := httptest.NewRequest(http.MethodGet, "https://bucket.s3.us-east-1.amazonaws.com/key", nil)
	if got := RouteName(virtual); got != "GetObject" {
		t.Fatalf("virtual-host route %q", got)
	}
	website := httptest.NewRequest(http.MethodPost, "http://bucket.s3-website.localhost.localstack.cloud/", nil)
	if got := RouteName(website); got != "GetObject" {
		t.Fatalf("website route %q", got)
	}
	decoded, err := codec.Decode(svc, &model.Operation{Name: "GetObject"}, website)
	if err != nil || decoded.Input["Bucket"] != "bucket" {
		t.Fatalf("website decode %#v %v", decoded, err)
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
	notification := `<NotificationConfiguration><QueueConfiguration><Id>queue</Id><Queue>arn:aws:sqs:us-east-1:111111111111:q</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter></QueueConfiguration><TopicConfiguration><Topic>arn:aws:sns:us-east-1:111111111111:t</Topic><Event>s3:ObjectRemoved:*</Event></TopicConfiguration><CloudFunctionConfiguration><CloudFunction>arn:aws:lambda:us-east-1:111111111111:function:f</CloudFunction><Event>s3:ObjectCreated:Put</Event></CloudFunctionConfiguration><EventBridgeConfiguration/></NotificationConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketNotificationConfiguration"}, httptest.NewRequest(http.MethodPut, "/bucket?notification", strings.NewReader(notification)))
	notificationConfiguration, _ := decoded.Input["NotificationConfiguration"].(map[string]any)
	queues, _ := notificationConfiguration["QueueConfigurations"].([]any)
	queue, _ := queues[0].(map[string]any)
	filter, _ := queue["Filter"].(map[string]any)
	keyFilter, _ := filter["Key"].(map[string]any)
	filterRules, _ := keyFilter["FilterRules"].([]any)
	filterRule, _ := filterRules[0].(map[string]any)
	topics, _ := notificationConfiguration["TopicConfigurations"].([]any)
	lambdas, _ := notificationConfiguration["LambdaFunctionConfigurations"].([]any)
	if err != nil || queue["Id"] != "queue" || filterRule["Value"] != "images/" || len(topics) != 1 || len(lambdas) != 1 || !reflect.DeepEqual(notificationConfiguration["EventBridgeConfiguration"], map[string]any{}) {
		t.Fatalf("notification decode %#v %v", decoded, err)
	}
	ownership := `<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketOwnershipControls"}, httptest.NewRequest(http.MethodPut, "/bucket?ownershipControls", strings.NewReader(ownership)))
	controls, _ := decoded.Input["OwnershipControls"].(map[string]any)
	rules, _ := controls["Rules"].([]any)
	if err != nil || len(rules) != 1 || rules[0].(map[string]any)["ObjectOwnership"] != "ObjectWriter" {
		t.Fatalf("ownership controls decode %#v %v", decoded, err)
	}
	publicAccessBlock := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutPublicAccessBlock"}, httptest.NewRequest(http.MethodPut, "/bucket?publicAccessBlock", strings.NewReader(publicAccessBlock)))
	configuration, _ := decoded.Input["PublicAccessBlockConfiguration"].(map[string]any)
	if err != nil || !reflect.DeepEqual(configuration, map[string]any{"BlockPublicAcls": true, "RestrictPublicBuckets": false}) {
		t.Fatalf("public access block decode %#v %v", decoded, err)
	}
	requestPayment := `<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketRequestPayment"}, httptest.NewRequest(http.MethodPut, "/bucket?requestPayment", strings.NewReader(requestPayment)))
	configuration, _ = decoded.Input["RequestPaymentConfiguration"].(map[string]any)
	if err != nil || configuration["Payer"] != "Requester" {
		t.Fatalf("request payment decode %#v %v", decoded, err)
	}
	accelerate := `<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketAccelerateConfiguration"}, httptest.NewRequest(http.MethodPut, "/bucket?accelerate", strings.NewReader(accelerate)))
	configuration, _ = decoded.Input["AccelerateConfiguration"].(map[string]any)
	if err != nil || configuration["Status"] != "Enabled" {
		t.Fatalf("accelerate decode %#v %v", decoded, err)
	}
	logging := `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LoggingEnabled><TargetBucket>target</TargetBucket><TargetGrants><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>id</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></TargetGrants><TargetObjectKeyFormat><PartitionedPrefix><PartitionDateSource>EventTime</PartitionDateSource></PartitionedPrefix></TargetObjectKeyFormat></LoggingEnabled></BucketLoggingStatus>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketLogging"}, httptest.NewRequest(http.MethodPut, "/bucket?logging", strings.NewReader(logging)))
	configuration, _ = decoded.Input["BucketLoggingStatus"].(map[string]any)
	wantLogging := map[string]any{"LoggingEnabled": map[string]any{
		"TargetBucket": "target", "TargetPrefix": "",
		"TargetGrants":          []any{map[string]any{"Grantee": map[string]any{"ID": "id", "Type": "CanonicalUser"}, "Permission": "FULL_CONTROL"}},
		"TargetObjectKeyFormat": map[string]any{"PartitionedPrefix": map[string]any{"PartitionDateSource": "EventTime"}},
	}}
	if err != nil || !reflect.DeepEqual(configuration, wantLogging) {
		t.Fatalf("logging decode %#v %v", decoded, err)
	}
	for _, test := range []struct {
		body string
		want map[string]any
	}{
		{`<BucketLoggingStatus/>`, map[string]any{}},
		{`<BucketLoggingStatus><LoggingEnabled><TargetBucket>target</TargetBucket><TargetObjectKeyFormat><SimplePrefix/></TargetObjectKeyFormat></LoggingEnabled></BucketLoggingStatus>`, map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "target", "TargetPrefix": "", "TargetObjectKeyFormat": map[string]any{"SimplePrefix": map[string]any{}}}}},
	} {
		decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketLogging"}, httptest.NewRequest(http.MethodPut, "/bucket?logging", strings.NewReader(test.body)))
		if err != nil || !reflect.DeepEqual(decoded.Input["BucketLoggingStatus"], test.want) {
			t.Errorf("logging decode %#v %v", decoded, err)
		}
	}
	for _, body := range []string{`<broken`, `<LoggingEnabled/>`} {
		decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketLogging"}, httptest.NewRequest(http.MethodPut, "/bucket?logging", strings.NewReader(body)))
		if err != nil || decoded.Input["_body"] != body {
			t.Errorf("invalid logging decode %#v %v", decoded, err)
		}
	}
	cors := `<CORSConfiguration><CORSRule><ID>read</ID><AllowedHeader>*</AllowedHeader><AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>https://example.test</AllowedOrigin><ExposeHeader>ETag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketCors"}, httptest.NewRequest(http.MethodPut, "/bucket?cors", strings.NewReader(cors)))
	wantCors := map[string]any{"CORSRules": []any{map[string]any{"ID": "read", "AllowedHeaders": []any{"*"}, "AllowedMethods": []any{"GET", "HEAD"}, "AllowedOrigins": []any{"https://example.test"}, "ExposeHeaders": []any{"ETag"}, "MaxAgeSeconds": 300}}}
	if err != nil || !reflect.DeepEqual(decoded.Input["CORSConfiguration"], wantCors) {
		t.Fatalf("CORS decode %#v %v", decoded, err)
	}
	for _, body := range []string{`<broken`, `<CORSRule/>`} {
		decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketCors"}, httptest.NewRequest(http.MethodPut, "/bucket?cors", strings.NewReader(body)))
		if err != nil || decoded.Input["_body"] != body {
			t.Errorf("invalid CORS decode %#v %v", decoded, err)
		}
	}
	website := `<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><HostName>example.test</HostName><Protocol>https</Protocol><ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`
	decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketWebsite"}, httptest.NewRequest(http.MethodPut, "/bucket?website", strings.NewReader(website)))
	wantWebsite := map[string]any{
		"IndexDocument": map[string]any{"Suffix": "index.html"},
		"ErrorDocument": map[string]any{"Key": "error.html"},
		"RoutingRules":  []any{map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "docs/"}, "Redirect": map[string]any{"HostName": "example.test", "Protocol": "https", "ReplaceKeyPrefixWith": "manual/"}}},
	}
	if err != nil || !reflect.DeepEqual(decoded.Input["WebsiteConfiguration"], wantWebsite) {
		t.Fatalf("website decode %#v %v", decoded, err)
	}
	for _, test := range []struct {
		body string
		want map[string]any
	}{
		{`<WebsiteConfiguration><RedirectAllRequestsTo><HostName></HostName></RedirectAllRequestsTo></WebsiteConfiguration>`, map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": ""}}},
		{`<WebsiteConfiguration><IndexDocument><Suffix/></IndexDocument><ErrorDocument/><RoutingRules><RoutingRule><Condition/><Redirect><ReplaceKeyPrefixWith/><ReplaceKeyWith/></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`, map[string]any{"IndexDocument": map[string]any{"Suffix": ""}, "ErrorDocument": map[string]any{}, "RoutingRules": []any{map[string]any{"Condition": map[string]any{}, "Redirect": map[string]any{"ReplaceKeyPrefixWith": "", "ReplaceKeyWith": ""}}}}},
		{`<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><RoutingRules/></WebsiteConfiguration>`, map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{}}},
	} {
		decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketWebsite"}, httptest.NewRequest(http.MethodPut, "/bucket?website", strings.NewReader(test.body)))
		if err != nil || !reflect.DeepEqual(decoded.Input["WebsiteConfiguration"], test.want) {
			t.Errorf("website decode %#v %v", decoded, err)
		}
	}
	for _, body := range []string{`<broken`, `<IndexDocument/>`} {
		decoded, err = codec.Decode(s3, &model.Operation{Name: "PutBucketWebsite"}, httptest.NewRequest(http.MethodPut, "/bucket?website", strings.NewReader(body)))
		if err != nil || decoded.Input["_body"] != body {
			t.Errorf("invalid website decode %#v %v", decoded, err)
		}
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

func TestEmptyResponseHeadersCharacterization(t *testing.T) {
	codec := Codec{}
	svc := &model.Service{ID: "aws.s3"}
	characterization := map[string]any{}
	for _, test := range []struct {
		name, operation string
		status          int
		contentLength   string
	}{{"upload_part", "UploadPart", http.StatusOK, "0"}, {"delete_object_tagging", "DeleteObjectTagging", http.StatusNoContent, ""}} {
		w := httptest.NewRecorder()
		response := &spi.Response{Status: test.status, Headers: http.Header{"Content-Type": {"application/xml"}, "Content-Length": {"7"}}}
		if err := codec.Encode(svc, &model.Operation{Name: test.operation}, w, response); err != nil {
			t.Fatal(err)
		}
		if w.Code != test.status || w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" || w.Header().Get("Content-Length") != test.contentLength {
			t.Fatalf("%s response %d %#v %q", test.operation, w.Code, w.Header(), w.Body.String())
		}
		characterization[test.name] = map[string]any{"content_length": w.Header().Get("Content-Length"), "content_type": w.Header().Get("Content-Type"), "status": w.Code}
	}
	golden.AssertJSON(t, characterization)
}

func TestETagHeaderCasingCharacterization(t *testing.T) {
	w := httptest.NewRecorder()
	response := &spi.Response{Headers: http.Header{"Etag": {`"etag"`}}, Stream: io.NopCloser(strings.NewReader("body"))}
	if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: "GetObject"}, w, response); err != nil {
		t.Fatal(err)
	}
	_, exact := w.Header()["ETag"]
	_, canonicalized := w.Header()["Etag"]
	values := w.Header()["ETag"]
	if !exact || canonicalized || len(values) != 1 || values[0] != `"etag"` {
		t.Fatalf("headers = %#v", w.Header())
	}
	golden.AssertJSON(t, map[string]any{"exact_etag": exact, "go_canonicalized_etag": canonicalized, "value": values[0]})
}

func FuzzEmptyResponseHeaders(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(1))
	f.Fuzz(func(t *testing.T, selector uint8) {
		operation, status, contentLength := "UploadPart", http.StatusOK, "0"
		if selector%2 != 0 {
			operation, status, contentLength = "DeleteObjectTagging", http.StatusNoContent, ""
		}
		w := httptest.NewRecorder()
		if err := (Codec{}).Encode(&model.Service{ID: "aws.s3"}, &model.Operation{Name: operation}, w, &spi.Response{Status: status}); err != nil {
			t.Fatal(err)
		}
		if w.Body.Len() != 0 || w.Header().Get("Content-Type") != "" || w.Header().Get("Content-Length") != contentLength {
			t.Fatalf("%s headers %#v body %q", operation, w.Header(), w.Body.String())
		}
	})
}

func TestRESTXMLEncodeAndFaultContracts(t *testing.T) {
	codec := Codec{}
	svc := &model.Service{ID: "aws.s3"}
	w := httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetObject"}, w, &spi.Response{Status: http.StatusPartialContent, Headers: http.Header{"ETag": {"one"}}, Stream: io.NopCloser(strings.NewReader("object"))}); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusPartialContent || len(w.Header()["ETag"]) != 1 || w.Header()["ETag"][0] != "one" || w.Body.String() != "object" {
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
	for _, operation := range []string{"ListObjects", "ListObjectsV2"} {
		w = httptest.NewRecorder()
		err := codec.Encode(svc, &model.Operation{Name: operation}, w, &spi.Response{Output: map[string]any{
			"Contents": []any{map[string]any{"Key": "folder/file", "ChecksumAlgorithm": []any{"SHA256", "CRC32"}}}, "CommonPrefixes": []any{map[string]any{"Prefix": "folder/subfolder/"}}, "BucketRegion": "us-west-2",
		}})
		body := w.Body.String()
		if err != nil || !strings.Contains(body, "<ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumAlgorithm>CRC32</ChecksumAlgorithm><Key>folder/file</Key>") || !strings.Contains(body, "<CommonPrefixes><Prefix>folder/subfolder/</Prefix></CommonPrefixes>") || !strings.Contains(body, "<BucketRegion>us-west-2</BucketRegion>") || strings.Contains(body, "<member>") {
			t.Fatalf("%s flattened response %v %s", operation, err, body)
		}
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketLocation"}, w, &spi.Response{Output: map[string]any{"LocationConstraint": "EU"}}); err != nil || !strings.Contains(w.Body.String(), `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">EU</LocationConstraint>`) {
		t.Fatalf("bucket location response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketOwnershipControls"}, w, &spi.Response{Output: map[string]any{"OwnershipControls": map[string]any{"Rules": []any{map[string]any{"ObjectOwnership": "BucketOwnerPreferred"}}}}}); err != nil || !strings.Contains(w.Body.String(), `<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>`) || strings.Contains(w.Body.String(), "<member>") {
		t.Fatalf("bucket ownership response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetPublicAccessBlock"}, w, &spi.Response{Output: map[string]any{"PublicAccessBlockConfiguration": map[string]any{"BlockPublicAcls": true, "BlockPublicPolicy": false, "IgnorePublicAcls": false, "RestrictPublicBuckets": true}}}); err != nil || !strings.Contains(w.Body.String(), `<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><BlockPublicAcls>true</BlockPublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><IgnorePublicAcls>false</IgnorePublicAcls><RestrictPublicBuckets>true</RestrictPublicBuckets></PublicAccessBlockConfiguration>`) {
		t.Fatalf("public access block response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketRequestPayment"}, w, &spi.Response{Output: map[string]any{"Payer": "Requester"}}); err != nil || !strings.Contains(w.Body.String(), `<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>Requester</Payer></RequestPaymentConfiguration>`) {
		t.Fatalf("request payment response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketAccelerateConfiguration"}, w, &spi.Response{Output: map[string]any{"Status": "Enabled"}}); err != nil || !strings.Contains(w.Body.String(), `<AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></AccelerateConfiguration>`) {
		t.Fatalf("accelerate response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketLogging"}, w, &spi.Response{Output: map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "target", "TargetPrefix": "logs/"}}}); err != nil || !strings.Contains(w.Body.String(), `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LoggingEnabled><TargetBucket>target</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`) {
		t.Fatalf("logging response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	cors := map[string]any{"CORSRules": []any{map[string]any{"ID": "read", "AllowedHeaders": []any{"*"}, "AllowedMethods": []any{"GET", "HEAD"}, "AllowedOrigins": []any{"https://example.test"}, "ExposeHeaders": []any{"ETag"}, "MaxAgeSeconds": 300}}}
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketCors"}, w, &spi.Response{Output: cors}); err != nil || !strings.Contains(w.Body.String(), `<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><CORSRule><ID>read</ID><AllowedHeader>*</AllowedHeader><AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>https://example.test</AllowedOrigin><ExposeHeader>ETag</ExposeHeader><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>`) || strings.Contains(w.Body.String(), "<member>") {
		t.Fatalf("CORS response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	website := map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "ErrorDocument": map[string]any{"Key": "error.html"}, "RoutingRules": []any{map[string]any{"Condition": map[string]any{"KeyPrefixEquals": "docs/"}, "Redirect": map[string]any{"Protocol": "https", "ReplaceKeyPrefixWith": "manual/"}}}}
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketWebsite"}, w, &spi.Response{Output: website}); err != nil || !strings.Contains(w.Body.String(), `<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><Protocol>https</Protocol><ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`) || strings.Contains(w.Body.String(), "<member>") {
		t.Fatalf("website response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	notifications := map[string]any{
		"QueueConfigurations":          []any{map[string]any{"Id": "queue", "QueueArn": "arn:aws:sqs:us-east-1:111111111111:q", "Events": []any{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{map[string]any{"Name": "Prefix", "Value": "images/"}}}}}},
		"LambdaFunctionConfigurations": []any{map[string]any{"Id": "lambda", "LambdaFunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:f", "Events": []any{"s3:ObjectCreated:Put"}}},
		"EventBridgeConfiguration":     map[string]any{},
	}
	if err := codec.Encode(svc, &model.Operation{Name: "GetBucketNotificationConfiguration"}, w, &spi.Response{Output: notifications}); err != nil || !strings.Contains(w.Body.String(), `<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><QueueConfiguration><Id>queue</Id><Queue>arn:aws:sqs:us-east-1:111111111111:q</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>Prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter></QueueConfiguration><CloudFunctionConfiguration><Id>lambda</Id><CloudFunction>arn:aws:lambda:us-east-1:111111111111:function:f</CloudFunction><Event>s3:ObjectCreated:Put</Event></CloudFunctionConfiguration><EventBridgeConfiguration></EventBridgeConfiguration></NotificationConfiguration>`) || strings.Contains(w.Body.String(), "<member>") || strings.Contains(w.Body.String(), "GetBucketNotificationConfigurationResult") {
		t.Fatalf("notification response %v %s", err, w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, &model.Operation{Name: "Missing"}, w, spi.NotImplemented(svc.ID, "Missing", "emulate"), "r<&"); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusNotImplemented || w.Header().Get("x-mirror-not-implemented") != "aws.s3.Missing" || !strings.Contains(w.Body.String(), "r&lt;&amp;") {
		t.Fatalf("fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	fault := &spi.Fault{Code: "AuthorizationHeaderMalformed", Message: "wrong region", HTTPStatus: http.StatusBadRequest, Fields: map[string]any{"Region": "us-east-1", "BucketName": "bucket<&"}}
	if err := codec.EncodeFault(svc, &model.Operation{Name: "HeadBucket"}, w, fault, "request"); err != nil {
		t.Fatal(err)
	}
	if w.Header().Get("x-amz-bucket-region") != "us-east-1" || !strings.Contains(w.Body.String(), "<BucketName>bucket&lt;&amp;</BucketName><Region>us-east-1</Region>") {
		t.Fatalf("structured fault %d %#v %s", w.Code, w.Header(), w.Body.String())
	}
	w = httptest.NewRecorder()
	if err := codec.EncodeFault(svc, &model.Operation{Name: "PutBucketVersioning"}, w, &spi.Fault{Code: "MalformedXML", HTTPStatus: http.StatusBadRequest}, "request"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(w.Body.String(), "<Message>The XML you provided was not well-formed or did not validate against our published schema</Message>") {
		t.Fatalf("malformed XML fault %s", w.Body.String())
	}
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
