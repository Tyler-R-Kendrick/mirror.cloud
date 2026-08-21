package restxml

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/model"
)

func TestRouteNameQueryOps(t *testing.T) {
	cases := []struct {
		method, path, copySrc, want string
	}{
		{http.MethodPut, "/b?tagging", "", "PutBucketTagging"},
		{http.MethodGet, "/b?tagging", "", "GetBucketTagging"},
		{http.MethodPut, "/b/k?tagging", "", "PutObjectTagging"},
		{http.MethodGet, "/b/k?tagging", "", "GetObjectTagging"},
		{http.MethodPut, "/b?notification", "", "PutBucketNotificationConfiguration"},
		{http.MethodPut, "/b?encryption", "", "PutBucketEncryption"},
		{http.MethodPut, "/b?cors", "", "PutBucketCors"},
		{http.MethodPost, "/b?delete", "", "DeleteObjects"},
		{http.MethodPost, "/b/k?uploads", "", "CreateMultipartUpload"},
		{http.MethodGet, "/b?uploads", "", "ListMultipartUploads"},
		{http.MethodGet, "/b/k?uploadId=abc", "", "ListParts"},
		{http.MethodPut, "/b/k?partNumber=1&uploadId=abc", "", "UploadPart"},
		{http.MethodPut, "/b/k?partNumber=2&uploadId=abc", "b/src", "UploadPartCopy"},
		{http.MethodPut, "/b/k", "b/src", "CopyObject"},
		{http.MethodPut, "/b", "", "CreateBucket"},
		{http.MethodPut, "/b/k", "", "PutObject"},
		{http.MethodGet, "/b/k", "", "GetObject"},
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
	body := `<Delete><Object><Key>k</Key></Object><Object><Key>src</Key></Object></Delete>`
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
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
