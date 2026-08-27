package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/golden"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
)

func ident() spi.Identity {
	return spi.Identity{Account: "123456789012", Region: "us-east-1"}
}

func invoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) (*spi.Response, error) {
	return invokeAs(t, p, ident(), op, in, body)
}

func invokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) (*spi.Response, error) {
	t.Helper()
	var rc io.ReadCloser
	if body != nil {
		rc = io.NopCloser(bytes.NewReader(body))
	}
	if in == nil {
		in = map[string]any{}
	}
	return p.Invoke(context.Background(), &spi.Request{
		ServiceID: "aws.s3",
		Operation: op,
		Input:     in,
		Identity:  id,
		Body:      rc,
	})
}

func mustInvokeAs(t *testing.T, p *s3.Pack, id spi.Identity, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invokeAs(t, p, id, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func mustInvoke(t *testing.T, p *s3.Pack, op string, in map[string]any, body []byte) *spi.Response {
	t.Helper()
	resp, err := invoke(t, p, op, in, body)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	return resp
}

func asFault(t *testing.T, err error) *spi.Fault {
	t.Helper()
	f, ok := err.(*spi.Fault)
	if !ok {
		t.Fatalf("got %T %v, want *spi.Fault", err, err)
	}
	return f
}

func readStream(t *testing.T, resp *spi.Response) []byte {
	t.Helper()
	if resp.Stream == nil {
		t.Fatal("nil stream")
	}
	defer resp.Stream.Close()
	b, err := io.ReadAll(resp.Stream)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func completedPart(number int, response *spi.Response) any {
	return map[string]any{"PartNumber": number, "ETag": response.Headers.Get("ETag")}
}

func completeInput(uploadID string, parts ...any) map[string]any {
	return map[string]any{"UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": parts}}
}

func TestCreatePutGetBytesMatch(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("payload-bytes")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, body)
	resp := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	if got := readStream(t, resp); !bytes.Equal(got, body) {
		t.Fatalf("get bytes %q want %q", got, body)
	}
}

func TestCreateBucketGlobalCollisions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	owner := ident()
	other := spi.Identity{Account: "999999999999", Region: owner.Region}
	west := spi.Identity{Account: owner.Account, Region: "us-west-2"}
	input := map[string]any{"Bucket": "shared-bucket"}
	if response, err := invokeAs(t, p, owner, "CreateBucket", input, nil); err != nil || response.Status != http.StatusOK || response.Headers.Get("Location") != "/shared-bucket" {
		t.Fatalf("initial create = %#v %v", response, err)
	}
	mustInvokeAs(t, p, owner, "PutObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, []byte("preserved"))
	if response, err := invokeAs(t, p, owner, "CreateBucket", input, nil); err != nil || response.Status != http.StatusOK {
		t.Fatalf("us-east-1 recreate = %#v %v", response, err)
	}
	preserved := ""
	if got := mustInvokeAs(t, p, owner, "GetObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, nil); string(readStream(t, got)) != "preserved" {
		t.Fatal("us-east-1 recreation replaced bucket contents")
	} else {
		preserved = "preserved"
	}
	collisions := map[string]any{}
	for name, identity := range map[string]spi.Identity{"owner-other-region": west, "other-account": other} {
		collisionInput := input
		if identity.Region != "us-east-1" {
			collisionInput = map[string]any{"Bucket": "shared-bucket", "LocationConstraint": identity.Region}
		}
		_, err := invokeAs(t, p, identity, "CreateBucket", collisionInput, nil)
		fault := asFault(t, err)
		want := "BucketAlreadyExists"
		if identity.Account == owner.Account {
			want = "BucketAlreadyOwnedByYou"
		}
		if fault.Code != want || fault.HTTPStatus != http.StatusConflict || fault.Fields["BucketName"] != "shared-bucket" {
			t.Fatalf("%s collision = %#v", name, fault)
		}
		if _, err := invokeAs(t, p, identity, "HeadBucket", input, nil); asFault(t, err).Code != "NoSuchBucket" {
			t.Fatalf("%s collision created local bucket: %v", name, err)
		}
		collisions[name] = fault.Code
	}
	western := map[string]any{"Bucket": "western-bucket", "LocationConstraint": "us-west-2"}
	mustInvokeAs(t, p, west, "CreateBucket", western, nil)
	_, err := invokeAs(t, p, west, "CreateBucket", western, nil)
	if fault := asFault(t, err); fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict {
		t.Fatalf("non-us-east-1 recreate = %#v", fault)
	} else {
		collisions["owner-non-us-east-recreate"] = fault.Code
	}
	_, err = invokeAs(t, p, owner, "CreateBucket", map[string]any{"Bucket": "western-bucket"}, nil)
	if fault := asFault(t, err); fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict {
		t.Fatalf("stored region collision = %#v", fault)
	} else {
		collisions["owner-stored-other-region"] = fault.Code
	}
	mustInvokeAs(t, p, owner, "DeleteObject", map[string]any{"Bucket": "shared-bucket", "Key": "object"}, nil)
	mustInvokeAs(t, p, owner, "DeleteBucket", input, nil)
	if _, err := invokeAs(t, p, other, "CreateBucket", input, nil); err != nil {
		t.Fatalf("create after delete: %v", err)
	}
	golden.AssertJSON(t, map[string]any{"collisions": collisions, "recreate": map[string]any{"status": http.StatusOK, "object": preserved}, "reuse_after_delete": "created"})
}

func TestCreateBucketAccountRegionalNamespace(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	east := ident()
	characterization := map[string]any{}
	name := "team-" + east.Account + "-" + east.Region + "-an"
	input := map[string]any{"Bucket": name, "BucketNamespace": "account-regional"}
	if got, err := invokeAs(t, p, east, "CreateBucket", input, nil); err != nil || got.Headers.Get("Location") != "/"+name {
		t.Fatalf("create = %#v %v", got, err)
	} else {
		characterization["east-location"] = got.Headers.Get("Location")
	}
	_, err := invokeAs(t, p, east, "CreateBucket", input, nil)
	fault := asFault(t, err)
	if fault.Code != "BucketAlreadyOwnedByYou" || fault.HTTPStatus != http.StatusConflict || fault.Fields["BucketName"] != name {
		t.Fatalf("recreate = %#v", fault)
	}
	characterization["recreate"] = fault.Code
	mustInvokeAs(t, p, east, "PutObject", map[string]any{"Bucket": name, "Key": "key"}, []byte("value"))

	other := spi.Identity{Account: "999999999999", Region: east.Region}
	_, err = invokeAs(t, p, other, "CreateBucket", input, nil)
	fault = asFault(t, err)
	if fault.Code != "InvalidBucketName" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["BucketName"] != name {
		t.Fatalf("foreign suffix = %#v", fault)
	}
	characterization["foreign-suffix"] = fault.Code
	if _, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": name, "BucketNamespace": "global"}, nil); asFault(t, err).Code != "InvalidBucketName" {
		t.Fatalf("global -an name = %v", err)
	} else {
		characterization["global-an"] = asFault(t, err).Code
	}
	_, err = invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "bucket", "BucketNamespace": "regional"}, nil)
	fault = asFault(t, err)
	if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["ArgumentName"] != "x-amz-bucket-namespace" || fault.Fields["ArgumentValue"] != "regional" {
		t.Fatalf("unknown namespace = %#v", fault)
	}
	characterization["unknown"] = fault.Code
	if _, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "explicit-global", "BucketNamespace": "global"}, nil); err != nil {
		t.Fatalf("explicit global namespace: %v", err)
	}
	for _, region := range []string{"me-central-1", "me-south-1"} {
		id := spi.Identity{Account: east.Account, Region: region}
		bucket := "team-" + id.Account + "-" + region + "-an"
		_, err := invokeAs(t, p, id, "CreateBucket", map[string]any{"Bucket": bucket, "BucketNamespace": "account-regional", "LocationConstraint": region}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("unsupported region %s = %#v", region, fault)
		}
	}

	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	westName := "team-" + west.Account + "-" + west.Region + "-an"
	westInput := map[string]any{"Bucket": westName, "BucketNamespace": "account-regional", "LocationConstraint": west.Region}
	if got, err := invokeAs(t, p, west, "CreateBucket", westInput, nil); err != nil || got.Headers.Get("Location") != "/"+westName {
		t.Fatalf("west create = %#v %v", got, err)
	} else {
		characterization["west-location"] = got.Headers.Get("Location")
	}
	if got := mustInvokeAs(t, p, west, "GetBucketLocation", map[string]any{"Bucket": westName}, nil); got.Output["LocationConstraint"] != west.Region {
		t.Fatalf("west location = %#v", got.Output)
	} else {
		characterization["west-constraint"] = got.Output["LocationConstraint"]
	}
	golden.AssertJSON(t, characterization)
}

func TestListBucketsPaginationAndFilters(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	east := spi.Identity{Account: "123456789012", Region: "us-east-1"}
	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	other := spi.Identity{Account: "999999999999", Region: east.Region}
	create := func(id spi.Identity, name string) {
		t.Helper()
		input := map[string]any{"Bucket": name}
		if id.Region != "us-east-1" {
			input["LocationConstraint"] = id.Region
		}
		mustInvokeAs(t, p, id, "CreateBucket", input, nil)
		if err := deps.Clock.Advance(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	create(east, "alpha-bucket")
	create(east, "team-alpha")
	create(west, "team-beta")
	create(west, "team-charlie")
	create(other, "team-private")
	stringValue := func(value any) string { text, _ := value.(string); return text }
	names := func(response *spi.Response) []string {
		t.Helper()
		var got []string
		for _, item := range response.Output["Buckets"].([]any) {
			got = append(got, stringValue(asMapForTest(item)["Name"]))
		}
		return got
	}

	all := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{}, nil)
	if got := strings.Join(names(all), ","); got != "alpha-bucket,team-alpha,team-beta,team-charlie" {
		t.Fatalf("all buckets = %s", got)
	}
	firstCreated := stringValue(asMapForTest(all.Output["Buckets"].([]any)[0])["CreationDate"])
	if firstCreated == "" || asMapForTest(all.Output["Buckets"].([]any)[0])["BucketRegion"] != nil {
		t.Fatalf("unpaginated bucket = %#v", all.Output["Buckets"].([]any)[0])
	}
	if err := deps.Clock.Advance(time.Hour); err != nil {
		t.Fatal(err)
	}
	listedAgain := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{}, nil)
	if created := stringValue(asMapForTest(listedAgain.Output["Buckets"].([]any)[0])["CreationDate"]); created != firstCreated {
		t.Fatalf("creation date changed from %q to %q", firstCreated, created)
	}

	page := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{"MaxBuckets": 2, "Prefix": "team-"}, nil)
	if got := strings.Join(names(page), ","); got != "team-alpha,team-beta" || page.Output["Prefix"] != "team-" {
		t.Fatalf("first page = %#v", page.Output)
	}
	for _, item := range page.Output["Buckets"].([]any) {
		if asMapForTest(item)["BucketRegion"] == "" {
			t.Fatalf("paginated bucket = %#v", item)
		}
	}
	token := stringValue(page.Output["ContinuationToken"])
	if token == "" || token == "team-beta" {
		t.Fatalf("continuation token = %q", token)
	}
	last := mustInvokeAs(t, p, east, "ListBuckets", map[string]any{"MaxBuckets": 2, "Prefix": "team-", "ContinuationToken": token}, nil)
	if got := strings.Join(names(last), ","); got != "team-charlie" || last.Output["ContinuationToken"] != nil {
		t.Fatalf("last page = %#v", last.Output)
	}
	regional := mustInvokeAs(t, p, west, "ListBuckets", map[string]any{"BucketRegion": west.Region, "Prefix": "team-"}, nil)
	if got := strings.Join(names(regional), ","); got != "team-beta,team-charlie" {
		t.Fatalf("regional buckets = %#v", regional.Output)
	}

	for _, input := range []map[string]any{{"MaxBuckets": 0}, {"MaxBuckets": 10001}, {"MaxBuckets": "invalid"}, {"ContinuationToken": "!"}, {"ContinuationToken": strings.Repeat("a", 1025)}} {
		_, err := invokeAs(t, p, east, "ListBuckets", input, nil)
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid input %#v = %#v", input, fault)
		}
	}
	golden.AssertJSON(t, map[string]any{"all": all.Output, "page": page.Output, "last": last.Output, "regional": regional.Output})
}

func TestCreateBucketLocationConstraints(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	east := ident()
	west := spi.Identity{Account: east.Account, Region: "us-west-2"}
	characterization := map[string]any{}

	if got := mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "east-location"}, nil).Headers.Get("Location"); got != "/east-location" {
		t.Fatalf("default create location = %q", got)
	}
	if got := mustInvokeAs(t, p, east, "GetBucketLocation", map[string]any{"Bucket": "east-location"}, nil); got.Output["LocationConstraint"] != "" {
		t.Fatalf("default location = %#v", got.Output)
	} else {
		characterization["default"] = got.Output["LocationConstraint"]
	}

	for name, input := range map[string]map[string]any{
		"missing":  {"Bucket": "west-missing"},
		"mismatch": {"Bucket": "west-mismatch", "LocationConstraint": "eu-west-1"},
	} {
		_, err := invokeAs(t, p, west, "CreateBucket", input, nil)
		fault := asFault(t, err)
		if fault.Code != "IllegalLocationConstraintException" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s constraint = %#v", name, fault)
		}
		if _, err := invokeAs(t, p, west, "HeadBucket", map[string]any{"Bucket": input["Bucket"]}, nil); asFault(t, err).Code != "NoSuchBucket" {
			t.Fatalf("%s created bucket: %v", name, err)
		}
		characterization[name] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	}

	if got := mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": "west-match", "CreateBucketConfiguration": map[string]any{"LocationConstraint": "us-west-2"}}, nil).Headers.Get("Location"); got != "http://west-match.s3.amazonaws.com/" {
		t.Fatalf("regional create location = %q", got)
	} else {
		characterization["matching-header"] = got
	}
	if got := mustInvokeAs(t, p, west, "GetBucketLocation", map[string]any{"Bucket": "west-match"}, nil); got.Output["LocationConstraint"] != "us-west-2" {
		t.Fatalf("west location = %#v", got.Output)
	} else {
		characterization["matching"] = got.Output["LocationConstraint"]
	}
	secure, secureErr := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CreateBucket", Identity: west, Input: map[string]any{"Bucket": "secure-location", "LocationConstraint": "us-west-2"}, HTTP: &http.Request{Method: http.MethodPut, URL: &url.URL{Path: "/secure-location"}, Host: "s3.test", TLS: &tls.ConnectionState{}}})
	if secureErr != nil || secure.Headers.Get("Location") != "https://s3.test/secure-location/" {
		t.Fatalf("secure create location = %#v %v", secure, secureErr)
	}
	characterization["secure-header"] = secure.Headers.Get("Location")

	_, err := invokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "invalid-location", "LocationConstraint": "moon-west-1"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidLocationConstraint" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["LocationConstraint"] != "moon-west-1" {
		t.Fatalf("invalid constraint = %#v", fault)
	} else {
		characterization["invalid"] = map[string]any{"code": fault.Code, "field": fault.Fields["LocationConstraint"], "message": fault.Message, "status": fault.HTTPStatus}
	}

	if got := mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "eu-alias", "LocationConstraint": "EU"}, nil).Headers.Get("Location"); got != "http://eu-alias.s3.amazonaws.com/" {
		t.Fatalf("EU create location = %q", got)
	} else {
		characterization["EU-header"] = got
	}
	europe := spi.Identity{Account: east.Account, Region: "eu-west-1"}
	if got := mustInvokeAs(t, p, europe, "GetBucketLocation", map[string]any{"Bucket": "eu-alias"}, nil); got.Output["LocationConstraint"] != "EU" {
		t.Fatalf("EU alias = %#v", got.Output)
	} else {
		characterization["EU"] = map[string]any{"reported": got.Output["LocationConstraint"], "stored_region": europe.Region}
	}
	golden.AssertJSON(t, characterization)
}

func TestCreateBucketValidatesGlobalNames(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{"invalid": map[string]any{}, "valid": map[string]any{}}
	invalid := []string{"", "ab", strings.Repeat("a", 64), "Uppercase", "under_score", "-starts", "ends-", "adjacent..dots", "192.168.5.4", "999.999.999.999", "192.168.005.4", "xn--reserved", "sthree-reserved", "amzn-s3-demo-reserved", "reserved-s3alias", "reserved--ol-s3", "reserved.mrap", "reserved--x-s3", "reserved--table-s3", "reserved-an"}
	for _, name := range invalid {
		_, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketName" || fault.Message != "The specified bucket is not valid." || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["BucketName"] != name {
			t.Fatalf("name %q = %#v", name, fault)
		}
		characterization["invalid"].(map[string]any)[name] = fault.Code
	}
	for _, name := range []string{"123", "abc", "bucket-name", "example.com", "abc.def.ghi.jkl", strings.Repeat("a", 63)} {
		response, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name}, nil)
		if err != nil {
			t.Fatalf("valid name %q: %v", name, err)
		}
		characterization["valid"].(map[string]any)[name] = response.Headers.Get("Location")
	}
	golden.AssertJSON(t, characterization)
}

func TestDeleteBucketRequiresEmptyBucket(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	input := map[string]any{"Bucket": "non-empty-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "object"}, []byte("body"))
	characterization := map[string]any{}

	_, err := invoke(t, p, "DeleteBucket", input, nil)
	fault := asFault(t, err)
	if fault.Code != "BucketNotEmpty" || fault.HTTPStatus != http.StatusConflict || fault.Message != "The bucket you tried to delete is not empty" || fault.Fields["BucketName"] != "non-empty-bucket" {
		t.Fatalf("unversioned delete = %#v", fault)
	}
	if got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "object"}, nil); string(readStream(t, got)) != "body" {
		t.Fatal("failed bucket deletion changed object")
	}
	characterization["unversioned"] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus, "preserved": true}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "non-empty-bucket", "Status": "Enabled"}, nil)
	_, err = invoke(t, p, "DeleteBucket", input, nil)
	fault = asFault(t, err)
	if fault.Message != "The bucket you tried to delete is not empty. You must delete all versions in the bucket." {
		t.Fatalf("versioned delete = %#v", fault)
	}
	characterization["versioned"] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "non-empty-bucket", "Key": "historical"}, []byte("version"))
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("objects").Delete(context.Background(), "non-empty-bucket/historical"); err != nil {
		t.Fatal(err)
	}
	if err := deps.Store.Scope(ident().Account, ident().Region).Collection("objects").Delete(context.Background(), "non-empty-bucket/object"); err != nil {
		t.Fatal(err)
	}
	_, err = invoke(t, p, "DeleteBucket", input, nil)
	if fault := asFault(t, err); fault.Code != "BucketNotEmpty" {
		t.Fatalf("historical-only version delete = %#v", fault)
	}
	characterization["historical-only-version"] = "BucketNotEmpty"
	golden.AssertJSON(t, characterization)
}

func TestDeleteBucketClearsBucketState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "recreated-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "recreated-bucket", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "recreated-bucket", "TagSet": []any{map[string]any{"Key": "old", "Value": "state"}}}, nil)
	mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": "recreated-bucket", "CORSRules": []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}}, nil)
	mustInvoke(t, p, "PutBucketAnalyticsConfiguration", map[string]any{"Bucket": "recreated-bucket", "Id": "old"}, nil)
	mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "recreated-bucket", "Key": "unfinished"}, nil)
	mustInvoke(t, p, "DeleteBucket", input, nil)
	if _, err := invoke(t, p, "HeadBucket", input, nil); asFault(t, err).Code != "NoSuchBucket" {
		t.Fatalf("deleted bucket remained registered: %v", err)
	}
	mustInvoke(t, p, "CreateBucket", input, nil)

	if got := mustInvoke(t, p, "GetBucketVersioning", input, nil); got.Output["Status"] == "Enabled" {
		t.Fatalf("recreated bucket inherited versioning: %#v", got.Output)
	}
	if _, err := invoke(t, p, "GetBucketTagging", input, nil); asFault(t, err).Code != "NoSuchTagSet" {
		t.Fatalf("recreated bucket inherited tags: %v", err)
	}
	if _, err := invoke(t, p, "GetBucketCors", input, nil); asFault(t, err).Code != "NoSuchCORSConfiguration" {
		t.Fatalf("recreated bucket inherited CORS: %v", err)
	}
	if got := mustInvoke(t, p, "ListBucketAnalyticsConfigurations", input, nil); len(got.Output["List"].([]any)) != 0 {
		t.Fatalf("recreated bucket inherited named configuration: %#v", got.Output)
	}
	if got := mustInvoke(t, p, "ListMultipartUploads", input, nil); len(got.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("recreated bucket inherited multipart uploads: %#v", got.Output)
	}
}

func TestBucketVersioningState(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	input := map[string]any{"Bucket": "versioning-bucket"}
	mustInvoke(t, p, "CreateBucket", input, nil)
	characterization := map[string]any{}
	if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output; len(got) != 0 {
		t.Fatalf("unset versioning = %#v", got)
	} else {
		characterization["unset"] = got
	}

	for _, test := range []struct {
		status  string
		code    string
		message string
	}{
		{"", "IllegalVersioningConfigurationException", "The Versioning element must be specified"},
		{"Invalid", "MalformedXML", ""},
	} {
		_, err := invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "versioning-bucket", "Status": test.status}, nil)
		fault := asFault(t, err)
		if fault.Code != test.code || fault.Message != test.message || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("status %q = %#v", test.status, fault)
		}
		characterization["rejected-"+test.code] = map[string]any{"code": fault.Code, "message": fault.Message, "status": fault.HTTPStatus}
		if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output; len(got) != 0 {
			t.Fatalf("status %q persisted: %#v", test.status, got)
		}
	}

	for _, status := range []string{"Enabled", "Suspended"} {
		if got := mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "versioning-bucket", "Status": status}, nil).Output; len(got) != 0 {
			t.Fatalf("put %s output = %#v", status, got)
		}
		if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output["Status"]; got != status {
			t.Fatalf("get %s = %v", status, got)
		} else {
			characterization[status] = got
		}
	}
	golden.AssertJSON(t, characterization)
}

func TestObjectMetadata(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{
		"Bucket": "bucket", "Key": "source", "CacheControl": "max-age=60", "ContentDisposition": `attachment; filename="one.txt"`,
		"ContentEncoding": "gzip", "ContentLanguage": "en-US", "ContentType": "text/plain", "Expires": "Wed, 21 Oct 2026 07:28:00 GMT",
		"Metadata": map[string]any{"Owner": "mirror", "Empty": ""}, "WebsiteRedirectLocation": "/old",
	}, []byte("first"))
	assert := func(name string, response *spi.Response, contentType, owner string) {
		t.Helper()
		if response.Headers.Get("Content-Type") != contentType || response.Headers.Get("x-amz-meta-owner") != owner {
			t.Fatalf("%s metadata = %v", name, response.Headers)
		}
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("get", get, "text/plain", "mirror")
	if get.Headers.Get("Cache-Control") != "max-age=60" || get.Headers.Get("Content-Disposition") != `attachment; filename="one.txt"` || get.Headers.Get("Content-Encoding") != "gzip" || get.Headers.Get("Content-Language") != "en-US" || get.Headers.Get("Expires") != "Wed, 21 Oct 2026 07:28:00 GMT" || get.Headers.Get("x-amz-website-redirect-location") != "/old" {
		t.Fatalf("get system metadata = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": first.Headers.Get("x-amz-version-id")}, nil)
	assert("head", head, "text/plain", "mirror")

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source"}, nil)
	copied := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	assert("copied", copied, "text/plain", "mirror")
	if copied.Headers.Get("x-amz-website-redirect-location") != "" {
		t.Fatalf("copy inherited website redirect = %v", copied.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "redirected", "CopySource": "bucket/source", "WebsiteRedirectLocation": "/new"}, nil)
	if redirected := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "redirected"}, nil); redirected.Headers.Get("x-amz-website-redirect-location") != "/new" {
		t.Fatalf("explicit copy redirect = %v", redirected.Headers)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "replaced", "CopySource": "bucket/source", "MetadataDirective": "REPLACE",
		"ContentType": "application/json", "Metadata": map[string]any{"Owner": "new"},
	}, nil)
	replaced := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "replaced"}, nil)
	assert("replaced", replaced, "application/json", "new")
	if replaced.Headers.Get("Cache-Control") != "" || replaced.Headers.Get("Content-Encoding") != "" {
		t.Fatalf("replace inherited system metadata = %v", replaced.Headers)
	}

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "default"}, []byte("body"))
	defaultHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "default"}, nil)
	assert("default", defaultHead, "binary/octet-stream", "")
	golden.AssertJSON(t, map[string]any{
		"get":      map[string]any{"contentType": get.Headers.Get("Content-Type"), "cacheControl": get.Headers.Get("Cache-Control"), "owner": get.Headers.Get("x-amz-meta-owner"), "redirect": get.Headers.Get("x-amz-website-redirect-location")},
		"head":     map[string]any{"contentType": head.Headers.Get("Content-Type"), "owner": head.Headers.Get("x-amz-meta-owner")},
		"replaced": map[string]any{"contentType": replaced.Headers.Get("Content-Type"), "cacheControl": replaced.Headers.Get("Cache-Control"), "owner": replaced.Headers.Get("x-amz-meta-owner")},
		"default":  map[string]any{"contentType": defaultHead.Headers.Get("Content-Type")},
	})
}

func TestCopyObjectTaggingDirective(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source", "Tagging": "team=data"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source"}, nil)
	copied := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	if tags := copied.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "team" {
		t.Fatalf("copied tags = %#v", tags)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "replaced", "CopySource": "bucket/source", "TaggingDirective": "REPLACE", "Tagging": "owner=mirror"}, nil)
	replaced := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "replaced"}, nil)
	if tags := replaced.Output["TagSet"].([]any); len(tags) != 1 || tags[0].(map[string]any)["Key"] != "owner" {
		t.Fatalf("replaced tags = %#v", tags)
	}
}

func TestCopyObjectDirectiveValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("body"))
	errors := map[string]any{}
	for _, test := range []struct{ input, value string }{
		{"MetadataDirective", "INVALID"},
		{"MetadataDirective", "copy"},
		{"TaggingDirective", "INVALID"},
		{"TaggingDirective", "replace"},
	} {
		key := test.input + "-" + test.value
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": key, "CopySource": "bucket/source", test.input: test.value}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s=%s fault = %#v", test.input, test.value, fault)
		}
		errors[key] = fault.Code
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("invalid directive created %s: %v", key, err)
		}
	}
	golden.AssertJSON(t, errors)
}

func TestCopyObjectRejectsUnchangedSelfCopy(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k", "Metadata": map[string]any{"owner": "old"}}, []byte("body"))
	selfCopy := func(input map[string]any) (*spi.Response, error) {
		t.Helper()
		input["Bucket"], input["Key"], input["CopySource"] = "bucket", "k", "bucket/k"
		return invoke(t, p, "CopyObject", input, nil)
	}
	characterization := map[string]any{}
	for _, test := range []struct {
		name  string
		input map[string]any
	}{
		{"unchanged", map[string]any{}},
		{"copyMetadata", map[string]any{"MetadataDirective": "COPY"}},
		{"replaceTags", map[string]any{"TaggingDirective": "REPLACE", "Tagging": "stage=new"}},
	} {
		_, err := selfCopy(test.input)
		fault := asFault(t, err)
		if fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s self-copy = %#v", test.name, fault)
		}
		characterization[test.name] = fault.Code
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil))); body != "body" {
		t.Fatalf("rejected self-copy changed body: %q", body)
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "other", "CopySource": "bucket/k"}, nil); err != nil {
		t.Fatalf("same-bucket copy to a different key: %v", err)
	}
	characterization["differentKey"] = "allowed"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "k", "CopySource": "bucket/k"}, nil); err != nil {
		t.Fatalf("cross-bucket copy to the same key: %v", err)
	}
	characterization["differentBucket"] = "allowed"
	for _, test := range []struct {
		name  string
		input map[string]any
	}{
		{"replaceMetadata", map[string]any{"MetadataDirective": "REPLACE", "Metadata": map[string]any{"owner": "new"}}},
		{"storageClass", map[string]any{"StorageClass": "STANDARD_IA"}},
		{"websiteRedirect", map[string]any{"WebsiteRedirectLocation": "/new"}},
		{"serverEncryption", map[string]any{"ServerSideEncryption": "AES256"}},
		{"customerEncryption", map[string]any{"SSECustomerKeyMD5": "digest"}},
	} {
		if _, err := selfCopy(test.input); err != nil {
			t.Fatalf("%s self-copy: %v", test.name, err)
		}
		characterization[test.name] = "allowed"
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "encrypted"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "encrypted", "Key": "k"}, []byte("body"))
	mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": "encrypted", "Rules": []any{}}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "encrypted", "Key": "k", "CopySource": "encrypted/k"}, nil); err != nil {
		t.Fatalf("default-encrypted bucket self-copy: %v", err)
	}
	characterization["bucketEncryption"] = "allowed"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "restored"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "restored", "Key": "k", "StorageClass": "GLACIER"}, []byte("body"))
	mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "restored", "Key": "k", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "restored", "Key": "k", "CopySource": "restored/k"}, nil); err != nil {
		t.Fatalf("restored source self-copy: %v", err)
	}
	characterization["restoredSource"] = "allowed"
	golden.AssertJSON(t, characterization)
}

func TestArchiveRestoreCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "archive"}, nil)
	for key, storageClass := range map[string]string{"glacier": "GLACIER", "deep": "DEEP_ARCHIVE", "instant": "GLACIER_IR", "standard": "STANDARD"} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": key, "StorageClass": storageClass}, []byte(key))
	}

	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if head.Headers.Get("x-amz-storage-class") != "GLACIER" || head.Headers.Get("x-amz-restore") != "" {
		t.Fatalf("archived head before restore = %v", head.Headers)
	}
	before := map[string]any{"head": "allowed"}
	for _, key := range []string{"glacier", "deep"} {
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": key}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidObjectState" || fault.HTTPStatus != http.StatusForbidden || fault.Fields["StorageClass"] != map[string]string{"glacier": "GLACIER", "deep": "DEEP_ARCHIVE"}[key] {
			t.Fatalf("%s before restore = %#v", key, fault)
		}
		before[key] = fault.Code
	}
	instant := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "instant"}, nil)
	_ = instant.Stream.Close()
	before["instant"] = "allowed"

	_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "archive", "Key": "rejected-copy", "CopySource": "archive/glacier"}, nil)
	copyFault := asFault(t, err)
	if copyFault.Code != "InvalidObjectState" {
		t.Fatalf("unrestored copy = %#v", copyFault)
	}
	mpu := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "archive", "Key": "multipart-copy"}, nil)
	uploadID := mpu.Output["UploadId"].(string)
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID, "PartNumber": 1, "CopySource": "archive/glacier"}, nil)
	partFault := asFault(t, err)
	parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID}, nil)
	if partFault.Code != "InvalidObjectState" || len(parts.Output["Parts"].([]any)) != 0 {
		t.Fatalf("unrestored part copy = %#v parts=%#v", partFault, parts.Output)
	}

	_, err = invoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "missing", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	missing := asFault(t, err)
	_, err = invoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "standard", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	standard := asFault(t, err)
	if missing.Code != "NoSuchKey" || standard.Code != "InvalidObjectState" || standard.Fields["StorageClass"] != "STANDARD" {
		t.Fatalf("restore boundaries missing=%#v standard=%#v", missing, standard)
	}
	withoutDays := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if withoutDays.Status != http.StatusOK {
		t.Fatalf("restore without days = %d", withoutDays.Status)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil); asFault(t, err).Code != "InvalidObjectState" {
		t.Fatalf("restore without days unlocked object: %v", err)
	}
	first := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier", "RestoreRequest": map[string]any{"Days": 2}}, nil)
	second := mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "glacier", "Days": 2}, nil)
	if first.Status != http.StatusAccepted || second.Status != http.StatusOK {
		t.Fatalf("restore statuses = %d, %d", first.Status, second.Status)
	}
	restoredHead := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	restoreHeader := restoredHead.Headers.Get("x-amz-restore")
	if restoreHeader != `ongoing-request="false", expiry-date="Sun, 04 Jan 1970 00:00:00 GMT"` {
		t.Fatalf("restore header = %q", restoreHeader)
	}
	restoredGet := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "glacier"}, nil)
	if body := string(readStream(t, restoredGet)); body != "glacier" || restoredGet.Headers.Get("x-amz-restore") != restoreHeader {
		t.Fatalf("restored get body=%q headers=%v", body, restoredGet.Headers)
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "archive", "Key": "copied", "CopySource": "archive/glacier"}, nil); err != nil {
		t.Fatalf("restored copy: %v", err)
	}
	if _, err := invoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "archive", "Key": "multipart-copy", "UploadId": uploadID, "PartNumber": 1, "CopySource": "archive/glacier"}, nil); err != nil {
		t.Fatalf("restored part copy: %v", err)
	}

	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "archive", "Status": "Enabled"}, nil)
	old := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": "versioned", "StorageClass": "GLACIER"}, []byte("old"))
	oldVersion := old.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "versioned", "RestoreRequest": map[string]any{"Days": 1}}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": "versioned", "StorageClass": "GLACIER"}, []byte("new"))
	_, err = invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "versioned"}, nil)
	currentFault := asFault(t, err)
	oldGet := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "versioned", "VersionId": oldVersion}, nil)
	if body := string(readStream(t, oldGet)); body != "old" || oldGet.Headers.Get("x-amz-restore") == "" || currentFault.Code != "InvalidObjectState" {
		t.Fatalf("version restore body=%q header=%q current=%#v", body, oldGet.Headers.Get("x-amz-restore"), currentFault)
	}

	golden.AssertJSON(t, map[string]any{
		"before": before,
		"copy":   map[string]any{"object": copyFault.Code, "part": partFault.Code, "partsWritten": 0},
		"restore": map[string]any{
			"missing": missing.Code, "standard": standard.Code, "withoutDays": withoutDays.Status,
			"first": first.Status, "second": second.Status, "header": restoreHeader,
		},
		"version": map[string]any{"current": currentFault.Code, "old": "allowed"},
	})
}

func TestStorageClassValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "classes"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": "source"}, []byte("source"))
	allowed := map[string]any{}
	for _, storageClass := range []string{"STANDARD", "REDUCED_REDUNDANCY", "STANDARD_IA", "ONEZONE_IA", "INTELLIGENT_TIERING", "GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "SNOW", "EXPRESS_ONEZONE"} {
		key := "allowed-" + storageClass
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": key, "StorageClass": storageClass}, []byte(storageClass))
		head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": key}, nil)
		header := head.Headers.Get("x-amz-storage-class")
		if storageClass == "STANDARD" {
			if header != "" {
				t.Fatalf("STANDARD header = %q", header)
			}
		} else if header != storageClass {
			t.Fatalf("%s header = %q", storageClass, header)
		}
		allowed[storageClass] = header
	}
	defaulted := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": "defaulted"}, []byte("default"))
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": "defaulted"}, nil); defaulted.Headers.Get("x-amz-storage-class") != "" || head.Headers.Get("x-amz-storage-class") != "" {
		t.Fatalf("default storage class headers put=%v head=%v", defaulted.Headers, head.Headers)
	}

	invalid := map[string]any{}
	for _, storageClass := range []string{"INVALID", "standard", "OUTPOSTS", " STANDARD"} {
		key := "invalid-" + storageClass
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": key, "StorageClass": storageClass}, []byte("rejected"))
		fault := asFault(t, err)
		if fault.Code != "InvalidStorageClass" || fault.Message != "The storage class you specified is not valid" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["StorageClassRequested"] != storageClass {
			t.Fatalf("put %q = %#v", storageClass, fault)
		}
		if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("invalid put %q created object: %v", storageClass, err)
		}
		invalid[storageClass] = fault.Code
	}
	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "classes", "Key": "multipart", "StorageClass": "OUTPOSTS"}, nil)
	mpuFault := asFault(t, err)
	uploads := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "classes"}, nil)
	if mpuFault.Code != "InvalidStorageClass" || len(uploads.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("invalid multipart = %#v uploads=%#v", mpuFault, uploads.Output)
	}
	_, err = invoke(t, p, "CopyObject", map[string]any{"Bucket": "classes", "Key": "copy", "CopySource": "classes/source", "StorageClass": "invalid"}, nil)
	copyFault := asFault(t, err)
	if copyFault.Code != "InvalidStorageClass" {
		t.Fatalf("invalid copy = %#v", copyFault)
	}
	if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "classes", "Key": "copy"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("invalid copy created object: %v", err)
	}

	golden.AssertJSON(t, map[string]any{
		"allowed":    allowed,
		"default":    "STANDARD",
		"invalid":    invalid,
		"operations": map[string]any{"copy": copyFault.Code, "multipart": mpuFault.Code},
	})
}

func TestObjectKeyLengthValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "keys"}, nil)
	valid := map[string]any{}
	for name, key := range map[string]string{"ascii": strings.Repeat("a", 1024), "utf8": strings.Repeat("é", 512)} {
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "keys", "Key": key}, []byte(name))
		if got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "keys", "Key": key}, nil); string(readStream(t, got)) != name {
			t.Fatalf("%s boundary key was not stored", name)
		}
		valid[name] = len(key)
	}
	invalid := map[string]any{}
	for name, key := range map[string]string{"ascii": strings.Repeat("a", 1025), "utf8": strings.Repeat("é", 513)} {
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "keys", "Key": key}, []byte("rejected"))
		fault := asFault(t, err)
		if fault.Code != "KeyTooLongError" || fault.Message != "Your key is too long" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["MaxSizeAllowed"] != "1024" || fault.Fields["Size"] != strconv.Itoa(len(key)) {
			t.Fatalf("%s oversized key = %#v", name, fault)
		}
		if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "keys", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("%s oversized key created object: %v", name, err)
		}
		invalid[name] = map[string]any{"code": fault.Code, "max": fault.Fields["MaxSizeAllowed"], "size": fault.Fields["Size"]}
	}
	longKey := strings.Repeat("x", 1025)
	operations := map[string]any{}
	for operation, input := range map[string]map[string]any{
		"CopyObject":            {"Bucket": "keys", "Key": longKey, "CopySource": "missing/source"},
		"CreateMultipartUpload": {"Bucket": "keys", "Key": longKey},
	} {
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "KeyTooLongError" {
			t.Fatalf("%s oversized key = %#v", operation, fault)
		}
		operations[operation] = fault.Code
	}
	if uploads := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "keys"}, nil); len(uploads.Output["Uploads"].([]any)) != 0 {
		t.Fatalf("oversized key created multipart upload: %#v", uploads.Output)
	}
	golden.AssertJSON(t, map[string]any{"valid_bytes": valid, "invalid": invalid, "operations": operations})
}

func TestExpectedBucketOwnerAndDeleteBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("body"))
	if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "bucket", "ExpectedBucketOwner": ident().Account}, nil); err != nil {
		t.Fatalf("matching owner: %v", err)
	}
	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "bucket", "ExpectedBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"HeadBucket", map[string]any{"Bucket": "bucket"}},
		{"GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
		{"HeadObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
		{"PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "k", "TagSet": []any{}}},
		{"DeleteObject", map[string]any{"Bucket": "bucket", "Key": "k"}},
	} {
		test.input["ExpectedBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, operation := range []string{"DeleteObject", "DeleteObjects", "GetObjectTagging"} {
		input := map[string]any{"Bucket": "missing", "Key": "k", "Objects": []any{map[string]any{"Key": "k"}}}
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		errors[operation+"Missing"] = fault.Code
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedSourceBucketOwnerAndCopyBoundary(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source"}, nil)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "destination"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "key"}, []byte("body"))
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "copy", "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)

	upload := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "destination", "Key": "multipart"}, nil)
	uploadID := upload.Output["UploadId"].(string)
	mustInvoke(t, p, "UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1, "CopySource": "source/key", "ExpectedSourceBucketOwner": ident().Account}, nil)
	httpReq := httptest.NewRequest(http.MethodPut, "/destination/header-denied", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", "999999999999")
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-denied", "CopySource": "source/key"}}); asFault(t, err).Code != "AccessDenied" {
		t.Fatalf("mismatched source owner header: %v", err)
	}
	httpReq = httptest.NewRequest(http.MethodPut, "/destination/header-copy", nil)
	httpReq.Header.Set("x-amz-copy-source", "source/key")
	httpReq.Header.Set("x-amz-source-expected-bucket-owner", ident().Account)
	if _, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Identity: ident(), HTTP: httpReq, Input: map[string]any{"Bucket": "destination", "Key": "header-copy", "CopySource": "source/key"}}); err != nil {
		t.Fatalf("matching source owner header: %v", err)
	}

	errors := map[string]any{}
	for _, expected := range []string{"12345678901", "12345678901x"} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "destination", "Key": "invalid-" + expected, "CopySource": "source/key", "ExpectedSourceBucketOwner": expected}, nil)
		fault := asFault(t, err)
		if fault.Code != "InvalidBucketOwnerAWSAccountID" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("expected source owner %q = %#v", expected, fault)
		}
		errors[expected] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "denied", "CopySource": "source/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "source/key"}},
	} {
		test.input["ExpectedSourceBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
	}
	for _, test := range []struct {
		operation string
		input     map[string]any
	}{
		{"CopyObject", map[string]any{"Bucket": "destination", "Key": "missing", "CopySource": "missing/key"}},
		{"UploadPartCopy", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID, "PartNumber": 2, "CopySource": "missing/key"}},
	} {
		_, err := invoke(t, p, test.operation, test.input, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing source = %#v", test.operation, fault)
		}
		errors[test.operation+"Missing"] = fault.Code
	}
	for _, key := range []string{"denied", "header-denied"} {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "destination", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("denied copy %q created object: %v", key, err)
		}
	}
	if parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "destination", "Key": "multipart", "UploadId": uploadID}, nil).Output["Parts"].([]any); len(parts) != 1 {
		t.Fatalf("rejected part mutated upload: %#v", parts)
	}
	golden.AssertJSON(t, errors)
}

func TestExpectedBucketOwnerAcrossBucketScopedOperations(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("id,name\n1,Ada\n"))
	uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart"}, nil).Output["UploadId"].(string)
	tests := []struct {
		operation string
		input     map[string]any
		body      []byte
	}{
		{"PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil},
		{"CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "missing/k"}, nil},
		{"DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{}}, nil},
		{"UploadPart", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1}, []byte("part")},
		{"UploadPartCopy", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "PartNumber": 1, "CopySource": "missing/k"}, nil},
		{"CompleteMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID, "MultipartUpload": map[string]any{"Parts": []any{}}}, nil},
		{"AbortMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil},
		{"ListParts", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil},
		{"SelectObjectContent", map[string]any{"Bucket": "bucket", "Key": "k", "Expression": "SELECT * FROM S3Object"}, nil},
		{"GetObjectTorrent", map[string]any{"Bucket": "bucket", "Key": "k"}, nil},
		{"PutObjectAnnotation", map[string]any{"Bucket": "bucket", "Key": "k", "AnnotationId": "a"}, nil},
	}
	errors := map[string]any{}
	for _, test := range tests {
		test.input["ExpectedBucketOwner"] = "999999999999"
		_, err := invoke(t, p, test.operation, test.input, test.body)
		fault := asFault(t, err)
		if fault.Code != "AccessDenied" || fault.HTTPStatus != http.StatusForbidden {
			t.Fatalf("%s mismatch = %#v", test.operation, fault)
		}
		errors[test.operation] = fault.Code
		delete(test.input, "ExpectedBucketOwner")
		test.input["Bucket"] = "missing"
		_, err = invoke(t, p, test.operation, test.input, test.body)
		fault = asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", test.operation, fault)
		}
		errors[test.operation+"Missing"] = fault.Code
	}
	if versioning := mustInvoke(t, p, "GetBucketVersioning", map[string]any{"Bucket": "bucket"}, nil).Output; len(versioning) != 0 {
		t.Fatalf("rejected versioning persisted: %#v", versioning)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "copy"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("rejected copy persisted: %v", err)
	}
	if body := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil))); body != "id,name\n1,Ada\n" {
		t.Fatalf("rejected delete changed source: %q", body)
	}
	if parts := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": uploadID}, nil).Output["Parts"].([]any); len(parts) != 0 {
		t.Fatalf("rejected multipart operations persisted parts: %#v", parts)
	}
	if _, err := invoke(t, p, "GetObjectAnnotation", map[string]any{"Bucket": "bucket", "Key": "k", "AnnotationId": "a"}, nil); asFault(t, err).Code != "NoSuchAnnotation" {
		t.Fatalf("rejected annotation persisted: %v", err)
	}
	golden.AssertJSON(t, errors)
}

func TestTagValidationAndBucketSemantics(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	characterization := map[string]any{}
	for _, operation := range []string{"PutBucketTagging", "GetBucketTagging", "DeleteBucketTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "missing", "TagSet": []any{}}, nil)
		fault := asFault(t, err)
		if fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing bucket = %#v", operation, fault)
		}
		characterization[operation+"MissingBucket"] = fault.Code
	}
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "bucket"}, nil)
	if asFault(t, err).Code != "NoSuchTagSet" {
		t.Fatalf("untagged bucket = %v", err)
	}
	characterization["untaggedBucket"] = asFault(t, err).Code
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("body"))
	valid := []any{map[string]any{"Key": "team α", "Value": ""}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": valid}, nil)

	tags := func(count int) []any {
		out := make([]any, count)
		for i := range out {
			out[i] = map[string]any{"Key": fmt.Sprintf("key%d", i), "Value": "value"}
		}
		return out
	}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": tags(10)}, nil)
	mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "bucket", "TagSet": tags(50)}, nil)
	characterization["acceptedObjectTags"] = 10
	characterization["acceptedBucketTags"] = 50
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": valid}, nil)
	mustInvoke(t, p, "DeleteBucketTagging", map[string]any{"Bucket": "bucket"}, nil)
	for _, test := range []struct {
		name string
		set  any
		code string
	}{
		{"missing-tag-set", nil, "MalformedXML"},
		{"missing-value", []any{map[string]any{"Key": "key"}}, "MalformedXML"},
		{"duplicate-key", []any{map[string]any{"Key": "key", "Value": "one"}, map[string]any{"Key": "key", "Value": "two"}}, "InvalidTag"},
		{"reserved-key", []any{map[string]any{"Key": "aws:team", "Value": "one"}}, "InvalidTag"},
		{"empty-key", []any{map[string]any{"Key": "", "Value": "one"}}, "InvalidTag"},
		{"long-key", []any{map[string]any{"Key": strings.Repeat("k", 129), "Value": "one"}}, "InvalidTag"},
		{"invalid-key", []any{map[string]any{"Key": "team?", "Value": "one"}}, "InvalidTag"},
		{"long-value", []any{map[string]any{"Key": "team", "Value": strings.Repeat("v", 257)}}, "InvalidTag"},
		{"too-many-object-tags", tags(11), "InvalidTag"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := invoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": test.set}, nil)
			fault := asFault(t, err)
			if fault.Code != test.code || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %#v", fault)
			}
			characterization[test.name] = fault.Code
			got := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
			if set := asSliceForTest(got.Output["TagSet"]); len(set) != 1 || asMapForTest(set[0])["Key"] != "team α" {
				t.Fatalf("rejected write changed tags: %#v", got.Output)
			}
		})
	}
	if _, err := invoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "bucket", "TagSet": tags(51)}, nil); asFault(t, err).Code != "InvalidTag" {
		t.Fatalf("too many bucket tags = %v", err)
	}
	for _, operation := range []string{"PutObject", "CreateMultipartUpload"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": operation, "Tagging": "key=one&key=two"}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s duplicate header = %#v", operation, fault)
		}
		characterization[operation+"DuplicateHeader"] = fault.Code
	}
	headerTags := url.Values{}
	for i := range 11 {
		headerTags.Set(fmt.Sprintf("key%d", i), "value")
	}
	rejectedKeys := []string{"PutObject", "copy"}
	for _, test := range []struct{ name, tagging string }{
		{"malformed-header", "%zz=value"},
		{"invalid-header-key", "team%3F=value"},
		{"too-many-header-tags", headerTags.Encode()},
	} {
		key := "rejected-" + test.name
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key, "Tagging": test.tagging}, []byte("body"))
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s = %#v", test.name, fault)
		}
		characterization[test.name] = fault.Code
		rejectedKeys = append(rejectedKeys, key)
	}
	_, err = invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/source", "TaggingDirective": "REPLACE", "Tagging": "key=one&key=two"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("copy duplicate header = %#v", fault)
	}
	for _, key := range rejectedKeys {
		if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key}, nil); asFault(t, err).Code != "NoSuchKey" {
			t.Fatalf("rejected %s created object: %v", key, err)
		}
	}
	characterization["storedTags"] = mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil).Output["TagSet"]
	golden.AssertJSON(t, characterization)
}

func TestCopyObjectConditions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("source"))
	etag := source.Headers.Get("ETag")
	copyObject := func(key string, input map[string]any, headers map[string]string) (*spi.Response, error) {
		t.Helper()
		in := map[string]any{"Bucket": "bucket", "Key": key, "CopySource": "bucket/source"}
		for name, value := range input {
			in[name] = value
		}
		httpReq := httptest.NewRequest(http.MethodPut, "/bucket/"+key, nil)
		httpReq.Header.Set("x-amz-copy-source", "bucket/source")
		for name, value := range headers {
			httpReq.Header.Set(name, value)
		}
		return p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "CopyObject", Input: in, Identity: ident(), HTTP: httpReq})
	}
	wantPrecondition := func(err error) {
		t.Helper()
		fault := asFault(t, err)
		if fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
			t.Fatalf("fault = %#v", fault)
		}
	}

	_, err := copyObject("wrong-etag", nil, map[string]string{"x-amz-copy-source-if-match": `"wrong"`})
	wantPrecondition(err)
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "wrong-etag"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("failed copy wrote destination")
	}

	before := time.Unix(-1, 0).UTC().Format(http.TimeFormat)
	after := time.Unix(1, 0).UTC().Format(http.TimeFormat)
	if _, err := copyObject("matched", nil, map[string]string{
		"x-amz-copy-source-if-match":            etag,
		"x-amz-copy-source-if-unmodified-since": before,
	}); err != nil {
		t.Fatal(err)
	}
	_, err = copyObject("none-matched", nil, map[string]string{
		"x-amz-copy-source-if-none-match":     etag,
		"x-amz-copy-source-if-modified-since": before,
	})
	wantPrecondition(err)

	for _, test := range []struct {
		name, header, value string
		ok                  bool
	}{
		{"modified", "x-amz-copy-source-if-modified-since", before, true},
		{"not-modified", "x-amz-copy-source-if-modified-since", after, false},
		{"unmodified", "x-amz-copy-source-if-unmodified-since", after, true},
		{"changed", "x-amz-copy-source-if-unmodified-since", before, false},
	} {
		_, err := copyObject(test.name, nil, map[string]string{test.header: test.value})
		if test.ok && err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		if !test.ok {
			wantPrecondition(err)
		}
	}

	destination := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, []byte("old"))
	_, err = copyObject("destination", map[string]any{"IfNoneMatch": "*"}, nil)
	wantPrecondition(err)
	_, err = copyObject("destination", map[string]any{"IfMatch": `"wrong"`}, nil)
	wantPrecondition(err)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, nil)); string(got) != "old" {
		t.Fatalf("failed condition replaced destination with %q", got)
	}
	if _, err := copyObject("destination", map[string]any{"IfMatch": destination.Headers.Get("ETag")}, nil); err != nil {
		t.Fatal(err)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "destination"}, nil)); string(got) != "source" {
		t.Fatalf("conditional copy = %q", got)
	}
}

func TestObjectReadConditions(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "conditional"}, []byte("body"))
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "conditional"}, nil)
	modified, err := http.ParseTime(head.Headers.Get("Last-Modified"))
	if err != nil {
		t.Fatal(err)
	}
	past, future := modified.Add(-time.Hour).Format(http.TimeFormat), modified.Add(time.Hour).Format(http.TimeFormat)
	etag := put.Headers.Get("ETag")
	call := func(operation string, conditions map[string]any) (*spi.Response, error) {
		t.Helper()
		input := map[string]any{"Bucket": "bucket", "Key": "conditional"}
		if operation == "GetObjectAttributes" {
			input["ObjectAttributes"] = []string{"ETag"}
		}
		for key, value := range conditions {
			input[key] = value
		}
		response, err := invoke(t, p, operation, input, nil)
		if response != nil && response.Stream != nil {
			_ = response.Stream.Close()
		}
		return response, err
	}
	for _, operation := range []string{"GetObject", "HeadObject", "GetObjectAttributes"} {
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong"`},
			{"IfUnmodifiedSince": past},
		} {
			_, err := call(operation, conditions)
			if fault := asFault(t, err); fault.Code != "PreconditionFailed" || fault.HTTPStatus != http.StatusPreconditionFailed {
				t.Fatalf("%s %#v fault = %#v", operation, conditions, fault)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfNoneMatch": etag},
			{"IfNoneMatch": "*"},
			{"IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status != http.StatusNotModified {
				t.Fatalf("%s %#v = %#v %v", operation, conditions, response, err)
			}
		}
		for _, conditions := range []map[string]any{
			{"IfMatch": `"wrong", ` + etag, "IfUnmodifiedSince": past},
			{"IfNoneMatch": `"wrong"`, "IfModifiedSince": future},
		} {
			response, err := call(operation, conditions)
			if err != nil || response.Status == http.StatusNotModified {
				t.Fatalf("%s precedence %#v: %#v %v", operation, conditions, response, err)
			}
		}
	}
}

func TestCopyObjectSourceVersions(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	key := "reports/a b+c?.json"
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	_ = deps.Clock.Advance(time.Second)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": key}, []byte("second"))
	versioned := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": key, "VersionId": firstVersion}, nil)
	if versioned.Headers.Get("ETag") != first.Headers.Get("ETag") || versioned.Headers.Get("x-amz-version-id") != firstVersion || string(readStream(t, versioned)) != "first" {
		t.Fatalf("versioned get headers = %v", versioned.Headers)
	}
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": key, "VersionId": firstVersion}, nil); head.Headers.Get("ETag") != first.Headers.Get("ETag") || head.Headers.Get("x-amz-version-id") != firstVersion || head.Headers.Get("Content-Length") != "5" {
		t.Fatalf("versioned head headers = %v", head.Headers)
	}
	source := "bucket/" + url.PathEscape(key)

	copyVersion := mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "version-copy", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	if got := copyVersion.Headers.Get("x-amz-copy-source-version-id"); got != firstVersion {
		t.Fatalf("source version header = %q want %q", got, firstVersion)
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "version-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version copy = %q", got)
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "current-copy", "CopySource": source}, nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "current-copy"}, nil)); string(got) != "second" {
		t.Fatalf("current copy = %q", got)
	}

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "part-copy"}, nil)
	uploadID := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "part-copy"}, nil)); string(got) != "first" {
		t.Fatalf("version part copy = %q", got)
	}

	deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": key}, nil)
	markerVersion := deleted.Headers.Get("x-amz-version-id")
	for _, operation := range []string{"GetObject", "HeadObject"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": key}, nil)
		if fault := asFault(t, err); fault.HTTPStatus != http.StatusNotFound || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s current marker fault = %#v", operation, fault)
		}
		_, err = invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": key, "VersionId": markerVersion}, nil)
		if fault := asFault(t, err); fault.Code != "MethodNotAllowed" || fault.HTTPStatus != http.StatusMethodNotAllowed || fault.Headers.Get("Last-Modified") == "" || fault.Headers.Get("x-amz-delete-marker") != "true" || fault.Headers.Get("x-amz-version-id") != markerVersion {
			t.Fatalf("%s explicit marker fault = %#v", operation, fault)
		}
	}
	if _, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "deleted", "CopySource": source}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatal("copied current delete marker")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{
		"Bucket": "bucket", "Key": "restored", "CopySource": source + "?versionId=" + url.PathEscape(firstVersion),
	}, nil)
	for _, invalid := range []struct {
		source, code string
	}{
		{"bucket/bad%zz", "InvalidArgument"},
		{source + "?versionId=missing", "NoSuchKey"},
		{source + "?versionId=", "InvalidArgument"},
		{source + "?versionId=" + markerVersion, "InvalidRequest"},
	} {
		_, err := invoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "invalid", "CopySource": invalid.source}, nil)
		if fault := asFault(t, err); fault.Code != invalid.code {
			t.Fatalf("%q fault = %#v", invalid.source, fault)
		}
	}
}

func TestDeleteObjectRestoresPreviousVersion(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=first"}, []byte("first"))
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=second"}, []byte("second"))
	third := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key", "Tagging": "stage=third"}, []byte("third"))

	deletedSecond := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": second.Headers.Get("x-amz-version-id")}, nil)
	if deletedSecond.Headers.Get("x-amz-version-id") != second.Headers.Get("x-amz-version-id") || string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil))) != "third" {
		t.Fatalf("noncurrent delete = %#v", deletedSecond)
	}

	deletedThird := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": third.Headers.Get("x-amz-version-id")}, nil)
	restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	restoredBody := string(readStream(t, restored))
	if deletedThird.Headers.Get("x-amz-version-id") != third.Headers.Get("x-amz-version-id") || restored.Headers.Get("x-amz-version-id") != first.Headers.Get("x-amz-version-id") || restoredBody != "first" {
		t.Fatalf("restored object = %#v", restored)
	}
	tags := asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "key"}, nil).Output["TagSet"])
	if len(tags) != 1 || tags[0].(map[string]any)["Value"] != "first" {
		t.Fatalf("restored tags = %#v", tags)
	}

	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	deletedMarker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": marker.Headers.Get("x-amz-version-id")}, nil)
	markerRestoredBody := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)))
	if deletedMarker.Headers.Get("x-amz-delete-marker") != "true" || deletedMarker.Headers.Get("x-amz-version-id") != marker.Headers.Get("x-amz-version-id") || markerRestoredBody != "first" {
		t.Fatalf("delete marker restore = %#v", deletedMarker)
	}

	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": "missing"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "versionId" {
		t.Fatalf("missing version fault = %#v", fault)
	}
	golden.AssertJSON(t, map[string]any{
		"deletedNoncurrentVersion": deletedSecond.Headers.Get("x-amz-version-id"),
		"deletedCurrentVersion":    deletedThird.Headers.Get("x-amz-version-id"),
		"restoredVersion":          restored.Headers.Get("x-amz-version-id"),
		"restoredBody":             restoredBody,
		"restoredTags":             tags,
		"deletedMarker":            deletedMarker.Headers.Get("x-amz-delete-marker"),
		"deletedMarkerVersion":     deletedMarker.Headers.Get("x-amz-version-id"),
		"markerRestoredBody":       markerRestoredBody,
	})
}

func TestDeleteObjectsVersionAndQuietSemantics(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("first"))
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("second"))

	deleted := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{
		map[string]any{"Key": "key", "VersionId": second.Headers.Get("x-amz-version-id")},
	}}, nil)
	item := deleted.Output["Deleted"].([]any)[0].(map[string]any)
	if item["VersionId"] != second.Headers.Get("x-amz-version-id") || item["DeleteMarker"] != nil {
		t.Fatalf("deleted version %#v", item)
	}
	restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	restoredBody := string(readStream(t, restored))
	if restored.Headers.Get("x-amz-version-id") != first.Headers.Get("x-amz-version-id") || restoredBody != "first" {
		t.Fatalf("restored version %#v", restored)
	}

	quietVersion := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "quiet"}, []byte("quiet")).Headers.Get("x-amz-version-id")
	result := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Quiet": true, "Objects": []any{
		map[string]any{"Key": "key", "VersionId": "missing"},
		map[string]any{"Key": "quiet", "VersionId": quietVersion},
	}}, nil)
	if result.Output["Deleted"] != nil {
		t.Fatalf("quiet response %#v", result.Output)
	}
	failure := result.Output["Errors"].([]any)[0].(map[string]any)
	if failure["Code"] != "NoSuchVersion" || failure["VersionId"] != "missing" {
		t.Fatalf("failure %#v", failure)
	}
	if _, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "quiet", "VersionId": quietVersion}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("quiet delete did not run: %v", err)
	}
	_, err := invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{}}, nil)
	emptyFault := asFault(t, err)
	if emptyFault.Code != "MalformedXML" {
		t.Fatalf("empty delete: %v", err)
	}
	objects := make([]any, 1001)
	for index := range objects {
		objects[index] = map[string]any{"Key": fmt.Sprintf("limit-%d", index)}
	}
	if _, err := invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Quiet": true, "Objects": objects[:1000]}, nil); err != nil {
		t.Fatalf("maximum delete: %v", err)
	}
	_, err = invoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": objects}, nil)
	oversizedFault := asFault(t, err)
	if oversizedFault.Code != "MalformedXML" {
		t.Fatalf("oversized delete: %v", err)
	}
	checksumBody := []byte("<Delete><Object><Key>checksum</Key></Object></Delete>")
	checksumDigest := md5.Sum(checksumBody)
	checksumFault := func(contentMD5, algorithm string) string {
		httpReq := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/bucket?delete", bytes.NewReader(checksumBody))
		if contentMD5 != "" {
			httpReq.Header.Set("Content-MD5", contentMD5)
		}
		if algorithm != "" {
			httpReq.Header.Set("x-amz-sdk-checksum-algorithm", algorithm)
		}
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "DeleteObjects", Input: map[string]any{
			"Bucket": "bucket", "Objects": []any{map[string]any{"Key": "checksum"}}, "_body": string(checksumBody),
		}, HTTP: httpReq})
		return asFault(t, err).Code
	}
	golden.AssertJSON(t, map[string]any{
		"verbose":      deleted.Output,
		"quiet":        result.Output,
		"restoredBody": restoredBody,
		"empty":        emptyFault.Code,
		"oversized":    oversizedFault.Code,
		"checksums": map[string]any{
			"missing":                 checksumFault("", ""),
			"mismatched":              checksumFault("AA==", ""),
			"algorithm without value": checksumFault(base64.StdEncoding.EncodeToString(checksumDigest[:]), "CRC32"),
		},
	})
}

func TestObjectLockPreventsPermanentDeletion(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket", "ObjectLockEnabledForBucket": true}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("first")).Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "key"}, []byte("second")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "LegalHold": map[string]any{"Status": "ON"}}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": second}, nil)
	_, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first}, nil)
	legalHoldFault := asFault(t, err)
	if legalHoldFault.Code != "AccessDenied" {
		t.Fatalf("legal hold delete: %v", err)
	}
	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key"}, nil)
	if marker.Headers.Get("x-amz-delete-marker") != "true" {
		t.Fatalf("simple delete did not create marker: %#v", marker.Headers)
	}

	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "LegalHold": map[string]any{"Status": "OFF"}}, nil)
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "Retention": map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "9999-01-01T00:00:00Z"}}, nil)
	locked := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "bucket", "Objects": []any{map[string]any{"Key": "key", "VersionId": first}}}, nil)
	if failure := locked.Output["Errors"].([]any)[0].(map[string]any); failure["Code"] != "AccessDenied" {
		t.Fatalf("governance delete: %#v", failure)
	}
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "key", "VersionId": first, "BypassGovernanceRetention": true}, nil)

	compliance := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "compliance"}, []byte("locked")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "compliance", "VersionId": compliance, "Retention": map[string]any{"Mode": "COMPLIANCE", "RetainUntilDate": "9999-01-01T00:00:00Z"}}, nil)
	_, err = invoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "compliance", "VersionId": compliance, "BypassGovernanceRetention": true}, nil)
	complianceFault := asFault(t, err)
	if complianceFault.Code != "AccessDenied" {
		t.Fatalf("compliance bypass: %v", err)
	}

	expired := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "expired"}, []byte("expired")).Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObjectRetention", map[string]any{"Bucket": "bucket", "Key": "expired", "VersionId": expired, "Retention": map[string]any{"Mode": "GOVERNANCE", "RetainUntilDate": "1960-01-01T00:00:00Z"}}, nil)
	mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "expired", "VersionId": expired}, nil)

	golden.AssertJSON(t, map[string]any{
		"legalHold":            legalHoldFault.Code,
		"otherVersion":         "deleted",
		"simpleDeleteMarker":   marker.Headers.Get("x-amz-delete-marker"),
		"governance":           locked.Output,
		"governanceWithBypass": "deleted",
		"complianceWithBypass": complianceFault.Code,
		"expired":              "deleted",
	})
}

func TestObjectLockBucketGuards(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "plain"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "plain", "Key": "key"}, []byte("plain"))
	_, err := invoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "plain", "Key": "key", "LegalHold": map[string]any{"Status": "ON"}}, nil)
	legalHoldFault := asFault(t, err)
	if legalHoldFault.Code != "InvalidRequest" {
		t.Fatalf("legal hold without bucket configuration: %v", err)
	}
	_, err = invoke(t, p, "DeleteObject", map[string]any{"Bucket": "plain", "Key": "key", "BypassGovernanceRetention": false}, nil)
	bypassFault := asFault(t, err)
	if bypassFault.Code != "InvalidArgument" {
		t.Fatalf("bypass without bucket configuration: %v", err)
	}

	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "locked", "ObjectLockEnabledForBucket": true}, nil)
	versioning := mustInvoke(t, p, "GetBucketVersioning", map[string]any{"Bucket": "locked"}, nil)
	if versioning.Output["Status"] != "Enabled" {
		t.Fatalf("object lock did not enable versioning: %#v", versioning.Output)
	}
	_, err = invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "locked", "Status": "Suspended"}, nil)
	suspendFault := asFault(t, err)
	if suspendFault.Code != "InvalidBucketState" {
		t.Fatalf("suspend object-lock versioning: %v", err)
	}
	golden.AssertJSON(t, map[string]any{
		"plainLegalHold": legalHoldFault.Code,
		"plainBypass":    bypassFault.Code,
		"lockedVersion":  versioning.Output,
		"lockedSuspend":  suspendFault.Code,
	})
}

func TestVersionedObjectTaggingCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	first := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source", "Tagging": "stage=first&team=storage"}, []byte("first"))
	firstVersion := first.Headers.Get("x-amz-version-id")
	second := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "source"}, []byte("second"))
	secondVersion := second.Headers.Get("x-amz-version-id")

	firstTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil)
	if firstTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(firstTags.Output["TagSet"])) != 2 {
		t.Fatalf("first version tags = %#v headers %v", firstTags.Output, firstTags.Headers)
	}
	if current := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "source"}, nil); current.Headers.Get("x-amz-tagging-count") != "" {
		t.Fatalf("new untagged version inherited tags: %v", current.Headers)
	}
	if old := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil); old.Headers.Get("x-amz-tagging-count") != "2" {
		t.Fatalf("old version tag count = %v", old.Headers)
	} else {
		_ = old.Stream.Close()
	}

	currentTag := []any{map[string]any{"Key": "stage", "Value": "second"}}
	putCurrent := mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "TagSet": currentTag}, nil)
	if putCurrent.Headers.Get("x-amz-version-id") != secondVersion {
		t.Fatalf("current tag version = %v", putCurrent.Headers)
	}
	explicitTag := []any{map[string]any{"Key": "stage", "Value": "retagged"}}
	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion, "TagSet": explicitTag}, nil)

	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copied", "CopySource": "bucket/source?versionId=" + firstVersion}, nil)
	copiedTags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "copied"}, nil)
	if tags := asSliceForTest(copiedTags.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "retagged" {
		t.Fatalf("version copy tags = %#v", copiedTags.Output)
	}

	deletedTags := mustInvoke(t, p, "DeleteObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil)
	if deletedTags.Headers.Get("x-amz-version-id") != firstVersion || len(asSliceForTest(mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": firstVersion}, nil).Output["TagSet"])) != 0 {
		t.Fatalf("deleted version tags = %v", deletedTags.Headers)
	}
	current := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
	if tags := asSliceForTest(current.Output["TagSet"]); current.Headers.Get("x-amz-version-id") != secondVersion || len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("current tags changed with old version: %#v headers %v", current.Output, current.Headers)
	}

	marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "bucket", "Key": "source"}, nil).Headers.Get("x-amz-version-id")
	retained := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": secondVersion}, nil)
	if tags := asSliceForTest(retained.Output["TagSet"]); len(tags) != 1 || asMapForTest(tags[0])["Value"] != "second" {
		t.Fatalf("delete marker lost version tags: %#v", retained.Output)
	}
	_, currentErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source"}, nil)
	_, markerErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "source", "VersionId": marker}, nil)
	for _, operation := range []string{"GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging"} {
		_, err := invoke(t, p, operation, map[string]any{"Bucket": "bucket", "Key": "missing", "TagSet": currentTag}, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchKey" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("%s missing object fault = %#v", operation, fault)
		}
	}

	golden.AssertJSON(t, map[string]any{
		"firstVersionTags": firstTags.Output["TagSet"],
		"currentTags":      current.Output["TagSet"],
		"retainedTags":     retained.Output["TagSet"],
		"copiedTags":       copiedTags.Output["TagSet"],
		"currentMarker":    asFault(t, currentErr).Code,
		"explicitMarker":   asFault(t, markerErr).Code,
	})
}

func TestUploadPartCopyConditionsAndRange(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := bytes.Repeat([]byte("0123456789"), 600000)
	source := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "large"}, body)
	createUpload := func(key string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key}, nil)
		return response.Output["UploadId"].(string)
	}

	_, err := invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("rejected"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceIfMatch": `"wrong"`,
	}, nil)
	if fault := asFault(t, err); fault.Code != "PreconditionFailed" {
		t.Fatalf("condition fault = %#v", fault)
	}

	uploadID := createUpload("range")
	part := mustInvoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": uploadID, "PartNumber": 1, "CopySource": "bucket/large",
		"CopySourceIfMatch": source.Headers.Get("ETag"), "CopySourceRange": "bytes=10-19",
	}, nil)
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "range"}, nil)); string(got) != "0123456789" {
		t.Fatalf("range copy = %q", got)
	}

	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("invalid-range"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceRange": "bytes=7000000-7000001",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("range fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("malformed-range"), "PartNumber": 1, "CopySource": "bucket/large", "CopySourceRange": "0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("malformed range fault = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "small"}, []byte("small"))
	_, err = invoke(t, p, "UploadPartCopy", map[string]any{
		"UploadId": createUpload("too-small"), "PartNumber": 1, "CopySource": "bucket/small", "CopySourceRange": "bytes=0-1",
	}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("small range fault = %#v", fault)
	}
}

func TestListObjectsV2Prefix(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "a/1", "StorageClass": "STANDARD_IA"}, []byte("1"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "a/2"}, []byte("2"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "z/9"}, []byte("9"))
	resp := mustInvoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "bucket", "Prefix": "a/"}, nil)
	contents, _ := resp.Output["Contents"].([]any)
	keys := map[string]bool{}
	for _, item := range contents {
		m, _ := item.(map[string]any)
		keys[m["Key"].(string)] = true
		if m["LastModified"] == "" || m["StorageClass"] == "" || m["Key"] == "a/1" && m["StorageClass"] != "STANDARD_IA" {
			t.Fatalf("object metadata: %#v", m)
		}
	}
	if !keys["a/1"] || !keys["a/2"] || keys["z/9"] || len(keys) != 2 {
		t.Fatalf("prefix list: %v", keys)
	}
}

func TestMultipartETagForm(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	if id == "" {
		t.Fatal("missing UploadId")
	}
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("BBB"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second)), nil)
	etag, _ := done.Output["ETag"].(string)
	if !regexp.MustCompile(`^"[0-9a-f]{32}-2"$`).MatchString(etag) {
		t.Fatalf("multipart etag form: %q", etag)
	}
	object := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	if object.Headers.Get("ETag") != etag || mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "k"}, nil).Headers.Get("ETag") != etag {
		t.Fatal("multipart ETag was not persisted")
	}
	mustInvoke(t, p, "CopyObject", map[string]any{"Bucket": "bucket", "Key": "copy", "CopySource": "bucket/k", "CopySourceIfMatch": etag}, nil)
	got := readStream(t, object)
	if len(got) != len(firstBody)+3 || !bytes.Equal(got[:len(firstBody)], firstBody) || string(got[len(firstBody):]) != "BBB" {
		t.Fatalf("assembled %d bytes", len(got))
	}
}

func TestMultipartPartReads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k", "ChecksumAlgorithm": "SHA256"}, nil)
	id := created.Output["UploadId"].(string)
	firstBody := bytes.Repeat([]byte("A"), 5<<20)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, firstBody)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second)), nil)
	version := done.Headers.Get("x-amz-version-id")
	if version == "" {
		t.Fatal("missing multipart version")
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "k"}, []byte("newer"))

	input := map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": 2, "ChecksumMode": "ENABLED"}
	get := mustInvoke(t, p, "GetObject", input, nil)
	if body := readStream(t, get); string(body) != "tail" {
		t.Fatalf("part body = %q", body)
	}
	if get.Status != http.StatusPartialContent || get.Headers.Get("Content-Length") != "4" || get.Headers.Get("Content-Range") != "bytes 5242880-5242883/5242884" || get.Headers.Get("x-amz-mp-parts-count") != "2" {
		t.Fatalf("part headers = status %d %v", get.Status, get.Headers)
	}
	if get.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") || get.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("part checksum = %v", get.Headers)
	}
	head := mustInvoke(t, p, "HeadObject", input, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "4" || head.Headers.Get("Content-Range") != get.Headers.Get("Content-Range") || head.Headers.Get("x-amz-mp-parts-count") != "2" || head.Headers.Get("x-amz-checksum-sha256") != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("head part = status %d %v", head.Status, head.Headers)
	}

	whole := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "PartNumber": 1}, nil)
	if body := readStream(t, whole); string(body) != "newer" || whole.Status != http.StatusPartialContent || whole.Headers.Get("x-amz-mp-parts-count") != "" {
		t.Fatalf("ordinary part one = %q status %d %v", body, whole.Status, whole.Headers)
	}
	for _, number := range []int{0, 3, 10001} {
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": number}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPartNumber" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable {
			t.Fatalf("part %d fault = %#v", number, fault)
		}
	}
	_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "k", "VersionId": version, "PartNumber": 1, "Range": "bytes=0-1"}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("part and range fault = %#v", fault)
	}

	golden.AssertJSON(t, map[string]any{
		"get":  map[string]any{"body": "tail", "status": get.Status, "length": get.Headers.Get("Content-Length"), "range": get.Headers.Get("Content-Range"), "parts": get.Headers.Get("x-amz-mp-parts-count"), "checksum": get.Headers.Get("x-amz-checksum-sha256")},
		"head": map[string]any{"status": head.Status, "length": head.Headers.Get("Content-Length"), "range": head.Headers.Get("Content-Range"), "parts": head.Headers.Get("x-amz-mp-parts-count"), "checksum": head.Headers.Get("x-amz-checksum-sha256")},
	})
}

func TestObjectByteRanges(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("0123456789")
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	checksum := base64.StdEncoding.EncodeToString(sum)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "range", "ChecksumCRC32": checksum}, body)
	get := func(value string) (*spi.Response, []byte, error) {
		response, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "range", "Range": value, "ChecksumMode": "ENABLED"}, nil)
		if err != nil {
			return response, nil, err
		}
		return response, readStream(t, response), nil
	}
	for _, test := range []struct {
		value, body, contentRange string
		checksum                  bool
	}{
		{"bytes=2-5", "2345", "bytes 2-5/10", false},
		{"bytes=7-", "789", "bytes 7-9/10", false},
		{"bytes=-3", "789", "bytes 7-9/10", false},
		{"bytes=-20", "0123456789", "bytes 0-9/10", true},
		{"bytes=8-99", "89", "bytes 8-9/10", false},
	} {
		response, got, err := get(test.value)
		if err != nil || response.Status != http.StatusPartialContent || string(got) != test.body || response.Headers.Get("Content-Range") != test.contentRange || response.Headers.Get("Content-Length") != strconv.Itoa(len(test.body)) || response.Headers.Get("Accept-Ranges") != "bytes" {
			t.Fatalf("range %q = %q %#v %v", test.value, got, response, err)
		}
		if hasChecksum := response.Headers.Get("x-amz-checksum-crc32") != ""; hasChecksum != test.checksum {
			t.Fatalf("range %q checksum headers = %v", test.value, response.Headers)
		}
	}
	for _, value := range []string{"2-5", "items=0-1", "bytes=bad", "bytes=5-2", "bytes=0-1,3-4"} {
		response, got, err := get(value)
		if err != nil || response.Status != http.StatusOK || string(got) != string(body) || response.Headers.Get("Content-Range") != "" {
			t.Fatalf("ignored range %q = %q %#v %v", value, got, response, err)
		}
	}
	for _, value := range []string{"bytes=10-", "bytes=-0"} {
		_, _, err := get(value)
		fault := asFault(t, err)
		if fault.Code != "InvalidRange" || fault.HTTPStatus != http.StatusRequestedRangeNotSatisfiable || fault.Headers.Get("Content-Range") != "bytes */10" {
			t.Fatalf("range %q fault = %#v", value, fault)
		}
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "range", "Range": "bytes=-3", "ChecksumMode": "ENABLED"}, nil)
	if head.Status != http.StatusPartialContent || head.Headers.Get("Content-Length") != "3" || head.Headers.Get("Content-Range") != "bytes 7-9/10" || head.Headers.Get("Accept-Ranges") != "bytes" || head.Headers.Get("x-amz-checksum-crc32") != "" {
		t.Fatalf("head range = %#v", head)
	}
}

func TestGetObjectAttributesContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "missing", "Key": "k", "ObjectAttributes": []string{"ETag"}}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("missing attributes bucket = %#v", fault)
	}
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, []byte("body"))
	if standard := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "standard", "ObjectAttributes": []string{"StorageClass"}}, nil); len(standard.Output) != 0 {
		t.Fatalf("standard storage class attributes = %#v", standard.Output)
	}
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "bucket", "Status": "Enabled"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "composite", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA"}, nil)
	id := created.Output["UploadId"].(string)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 1}, bytes.Repeat([]byte("A"), 5<<20))
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 2}, bytes.Repeat([]byte("B"), 5<<20))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": id, "PartNumber": 3}, []byte("tail"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, first), completedPart(2, second), completedPart(3, third)), nil)
	version := done.Headers.Get("x-amz-version-id")
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "composite"}, []byte("newer"))

	attrs := []string{"ETag", "Checksum", "ObjectParts", "StorageClass", "ObjectSize"}
	page := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": attrs, "MaxParts": 2,
	}, nil)
	if page.Output["ETag"] != done.Output["ETag"] || page.Output["ObjectSize"] != 10<<20+4 || page.Output["StorageClass"] != "STANDARD_IA" || page.Headers.Get("x-amz-version-id") != version || page.Headers.Get("Last-Modified") == "" {
		t.Fatalf("object attributes = %#v %v", page.Output, page.Headers)
	}
	checksum := asMapForTest(page.Output["Checksum"])
	if checksum["ChecksumSHA256"] != strings.SplitN(done.Output["ChecksumSHA256"].(string), "-", 2)[0] || checksum["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("object checksum = %#v", checksum)
	}
	objectParts := asMapForTest(page.Output["ObjectParts"])
	listed := objectParts["Parts"].([]any)
	if objectParts["TotalPartsCount"] != 3 || objectParts["IsTruncated"] != true || objectParts["MaxParts"] != 2 || objectParts["PartNumberMarker"] != "0" || objectParts["NextPartNumberMarker"] != "2" || len(listed) != 2 || asMapForTest(listed[0])["PartNumber"] != 1 || asMapForTest(listed[1])["ChecksumSHA256"] != second.Headers.Get("x-amz-checksum-sha256") {
		t.Fatalf("object parts page = %#v", objectParts)
	}
	lastPage := mustInvoke(t, p, "GetObjectAttributes", map[string]any{
		"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []any{"ObjectParts"}, "PartNumberMarker": "2", "MaxParts": 2,
	}, nil).Output
	lastParts := asMapForTest(lastPage["ObjectParts"])
	if lastParts["IsTruncated"] != false || lastParts["PartNumberMarker"] != "2" || lastParts["NextPartNumberMarker"] != "3" || len(lastParts["Parts"].([]any)) != 1 || asMapForTest(lastParts["Parts"].([]any)[0])["PartNumber"] != 3 {
		t.Fatalf("object parts final page = %#v", lastParts)
	}
	selected := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectSize"}}, nil)
	if len(selected.Output) != 1 || selected.Output["ObjectSize"] == nil {
		t.Fatalf("selected attributes = %#v", selected.Output)
	}
	for field, value := range map[string]any{"MaxParts": 1001, "PartNumberMarker": "invalid"} {
		_, err := invoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "composite", "VersionId": version, "ObjectAttributes": []string{"ObjectParts"}, field: value}, nil)
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid %s fault = %#v", field, fault)
		}
	}

	full := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	fullID := full.Output["UploadId"].(string)
	fullFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 1}, bytes.Repeat([]byte("C"), 5<<20))
	fullSecond := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": fullID, "PartNumber": 2}, []byte("end"))
	fullDone := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(fullID, completedPart(1, fullFirst), completedPart(2, fullSecond)), nil)
	fullAttrs := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "bucket", "Key": "full", "ObjectAttributes": []string{"Checksum", "ObjectParts"}}, nil).Output
	if fullChecksum := asMapForTest(fullAttrs["Checksum"]); fullChecksum["ChecksumCRC32"] != fullDone.Output["ChecksumCRC32"] || fullChecksum["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("full checksum attributes = %#v", fullChecksum)
	}
	if fullParts := asMapForTest(fullAttrs["ObjectParts"]); len(fullParts) != 1 || fullParts["TotalPartsCount"] != 2 {
		t.Fatalf("full object parts = %#v", fullParts)
	}

	golden.AssertJSON(t, map[string]any{"page": page.Output, "lastPage": lastPage, "full": fullAttrs})
}

func TestWriteChecksumValidation(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	md5sum, sha1sum, sha256sum, sha512sum := md5.Sum(body), sha1.Sum(body), sha256.Sum256(body), sha512.Sum512(body)
	crc32sum, crc32csum := make([]byte, 4), make([]byte, 4)
	binary.BigEndian.PutUint32(crc32sum, crc32.ChecksumIEEE(body))
	binary.BigEndian.PutUint32(crc32csum, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
	b64 := func(sum []byte) string { return base64.StdEncoding.EncodeToString(sum) }
	checksums := map[string]string{
		"ContentMD5":        b64(md5sum[:]),
		"ChecksumMD5":       b64(md5sum[:]),
		"ChecksumCRC32":     b64(crc32sum),
		"ChecksumCRC32C":    b64(crc32csum),
		"ChecksumCRC64NVME": "rosUhgp5mIg=",
		"ChecksumSHA1":      b64(sha1sum[:]),
		"ChecksumSHA256":    b64(sha256sum[:]),
		"ChecksumSHA512":    b64(sha512sum[:]),
		"ChecksumXXHASH64":  "jLhB20DmroM=",
		"ChecksumXXHASH3":   "ctyxi2ehff8=",
		"ChecksumXXHASH128": "MxGUd+3l3NXpcWQnaB1YYA==",
	}
	responseHeaders := map[string]string{
		"ChecksumMD5": "x-amz-checksum-md5", "ChecksumCRC32": "x-amz-checksum-crc32", "ChecksumCRC32C": "x-amz-checksum-crc32c",
		"ChecksumCRC64NVME": "x-amz-checksum-crc64nvme", "ChecksumSHA1": "x-amz-checksum-sha1",
		"ChecksumSHA256": "x-amz-checksum-sha256", "ChecksumSHA512": "x-amz-checksum-sha512", "ChecksumXXHASH64": "x-amz-checksum-xxhash64",
		"ChecksumXXHASH3": "x-amz-checksum-xxhash3", "ChecksumXXHASH128": "x-amz-checksum-xxhash128",
	}
	for name, value := range checksums {
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": name, name: value}, body)
		if header := responseHeaders[name]; header != "" {
			if put.Headers.Get(header) != value || put.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s put checksum headers = %v", name, put.Headers)
			}
			get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": name, "ChecksumMode": "ENABLED"}, nil)
			if get.Headers.Get(header) != value || get.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
				t.Fatalf("%s get checksum headers = %v", name, get.Headers)
			}
			_ = get.Stream.Close()
			if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": name, "ChecksumMode": "ENABLED"}, nil); head.Headers.Get(header) != value {
				t.Fatalf("%s head checksum headers = %v", name, head.Headers)
			}
		}
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": name + "-bad", name: "AA=="}, body)
		if fault := asFault(t, err); fault.Code != "BadDigest" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("%s fault = %#v", name, fault)
		}
	}
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "malformed", "ChecksumMD5": "!"}, body)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("malformed checksum fault = %#v", fault)
	}
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumAlgorithm": "MD5"}, nil)
	uploadID := created.Output["UploadId"].(string)
	_, err = invoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("upload checksum fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1, "ChecksumMD5": checksums["ChecksumMD5"]}, body)
	if part.Headers.Get("x-amz-checksum-md5") != checksums["ChecksumMD5"] {
		t.Fatalf("upload checksum headers = %v", part.Headers)
	}
	complete := completeInput(uploadID, completedPart(1, part))
	complete["ChecksumMD5"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum fault = %#v", fault)
	}
	partDigest := md5.Sum(body)
	compositeDigest := md5.Sum(partDigest[:])
	composite := base64.StdEncoding.EncodeToString(compositeDigest[:]) + "-1"
	complete["ChecksumMD5"] = composite
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumMD5"] != composite || done.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("complete checksum output = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-md5") != composite || head.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
		t.Fatalf("multipart checksum metadata = %v", head.Headers)
	}
}

func TestMultipartChecksumContract(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "missing", "Key": "k"}, nil)
	if fault := asFault(t, err); fault.Code != "NoSuchBucket" || fault.HTTPStatus != http.StatusNotFound {
		t.Fatalf("create missing bucket fault = %#v", fault)
	}
	wantCreateFault := func(input map[string]any, code string) {
		t.Helper()
		input["Bucket"], input["Key"] = "bucket", code
		_, err := invoke(t, p, "CreateMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != code || fault.HTTPStatus < http.StatusBadRequest {
			t.Fatalf("create checksum fault = %#v want %s", fault, code)
		}
	}
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "SHA256", "ChecksumType": "FULL_OBJECT"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC64NVME", "ChecksumType": "COMPOSITE"}, "InvalidRequest")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "CRC32", "ChecksumType": "invalid"}, "InvalidArgument")
	wantCreateFault(map[string]any{"ChecksumAlgorithm": "XXHASH64", "ChecksumType": "FULL_OBJECT"}, "InvalidRequest")

	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
	if created.Headers.Get("x-amz-checksum-algorithm") != "CRC32" || created.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("create checksum headers = %v", created.Headers)
	}
	id := created.Output["UploadId"].(string)
	body := []byte("full object")
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumAlgorithm": "SHA1"}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("requested part algorithm fault = %#v", fault)
	}
	_, err = invoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1, "ChecksumSHA1": "AA=="}, body)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("part algorithm fault = %#v", fault)
	}
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id, "PartNumber": 1}, body)
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "full", "UploadId": id}, nil)
	if listed.Output["ChecksumAlgorithm"] != "CRC32" || listed.Output["ChecksumType"] != "FULL_OBJECT" || listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] == "" {
		t.Fatalf("listed checksum contract = %#v", listed.Output)
	}
	complete := completeInput(id, completedPart(1, part))
	complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "InvalidPart" {
		t.Fatalf("manifest checksum fault = %#v", fault)
	}
	delete(complete["MultipartUpload"].(map[string]any)["Parts"].([]any)[0].(map[string]any), "ChecksumCRC32")
	complete["ChecksumType"] = "COMPOSITE"
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum type fault = %#v", fault)
	}
	complete["ChecksumType"] = "FULL_OBJECT"
	complete["ChecksumCRC32"] = "AA=="
	_, err = invoke(t, p, "CompleteMultipartUpload", complete, nil)
	if fault := asFault(t, err); fault.Code != "BadDigest" {
		t.Fatalf("complete checksum fault = %#v", fault)
	}
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	want := base64.StdEncoding.EncodeToString(sum)
	complete["ChecksumCRC32"] = want
	done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
	if done.Output["ChecksumCRC32"] != want || done.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("complete checksum = %#v", done.Output)
	}
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "full", "ChecksumMode": "ENABLED"}, nil)
	if head.Headers.Get("x-amz-checksum-crc32") != want || head.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
		t.Fatalf("stored checksum = %v", head.Headers)
	}

	composite := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "gap", "ChecksumAlgorithm": "SHA256"}, nil)
	compositeID := composite.Output["UploadId"].(string)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "gap", "UploadId": compositeID, "PartNumber": 2}, []byte("second"))
	_, err = invoke(t, p, "CompleteMultipartUpload", completeInput(compositeID, completedPart(2, second)), nil)
	if fault := asFault(t, err); fault.Code != "InternalError" || fault.HTTPStatus != http.StatusInternalServerError {
		t.Fatalf("nonconsecutive composite fault = %#v", fault)
	}
}

func TestXXHashMultipartChecksums(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	for _, tc := range []struct {
		algorithm, input, header, part, composite string
	}{
		{"XXHASH64", "ChecksumXXHASH64", "x-amz-checksum-xxhash64", "jLhB20DmroM=", "aIYCMYPSWcc=-1"},
		{"XXHASH3", "ChecksumXXHASH3", "x-amz-checksum-xxhash3", "ctyxi2ehff8=", "ksPmtVIgSbU=-1"},
		{"XXHASH128", "ChecksumXXHASH128", "x-amz-checksum-xxhash128", "MxGUd+3l3NXpcWQnaB1YYA==", "qhtapxAN/tUuBHXli2H9nQ==-1"},
	} {
		t.Run(tc.algorithm, func(t *testing.T) {
			_, err := invoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": tc.algorithm + "-full", "ChecksumAlgorithm": tc.algorithm, "ChecksumType": "FULL_OBJECT"}, nil)
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("full-object fault = %#v", fault)
			}
			created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumAlgorithm": tc.algorithm}, nil)
			id := created.Output["UploadId"].(string)
			part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "UploadId": id, "PartNumber": 1, tc.input: tc.part}, body)
			if part.Headers.Get(tc.header) != tc.part {
				t.Fatalf("part headers = %v", part.Headers)
			}
			complete := completeInput(id, completedPart(1, part))
			complete[tc.input] = tc.composite
			done := mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)
			if done.Output[tc.input] != tc.composite || done.Output["ChecksumType"] != "COMPOSITE" {
				t.Fatalf("complete output = %#v", done.Output)
			}
			head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumMode": "ENABLED"}, nil)
			if head.Headers.Get(tc.header) != tc.composite || head.Headers.Get("x-amz-checksum-type") != "COMPOSITE" {
				t.Fatalf("head headers = %v", head.Headers)
			}
		})
	}
}

func TestXXHashChecksumCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	body := []byte("123456789")
	snapshot := map[string]any{}
	for _, tc := range []struct{ algorithm, input, header, value string }{
		{"XXHASH64", "ChecksumXXHASH64", "x-amz-checksum-xxhash64", "jLhB20DmroM="},
		{"XXHASH3", "ChecksumXXHASH3", "x-amz-checksum-xxhash3", "ctyxi2ehff8="},
		{"XXHASH128", "ChecksumXXHASH128", "x-amz-checksum-xxhash128", "MxGUd+3l3NXpcWQnaB1YYA=="},
	} {
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, tc.input: tc.value}, body)
		get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": tc.algorithm, "ChecksumMode": "ENABLED"}, nil)
		_ = get.Stream.Close()
		snapshot[tc.algorithm] = map[string]any{"put": put.Headers.Get(tc.header), "get": get.Headers.Get(tc.header), "type": get.Headers.Get("x-amz-checksum-type")}
	}
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "multipart", "ChecksumAlgorithm": "XXHASH128"}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "multipart", "UploadId": id, "PartNumber": 1}, body)
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	snapshot["multipart"] = map[string]any{"part": part.Headers.Get("x-amz-checksum-xxhash128"), "complete": done.Output["ChecksumXXHASH128"], "type": done.Output["ChecksumType"]}
	golden.AssertJSON(t, snapshot)
}

func TestMultipartChecksumCharacterization(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "snapshot", "ChecksumAlgorithm": "SHA256", "StorageClass": "STANDARD_IA", "Tagging": "env=snapshot"}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "snapshot", "UploadId": id, "PartNumber": 1}, []byte("snapshot"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "snapshot", "UploadId": id}, nil)
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "snapshot"}, nil)
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "snapshot"}, nil).Output["TagSet"]
	golden.AssertJSON(t, map[string]any{
		"create":   map[string]any{"algorithm": created.Output["ChecksumAlgorithm"], "type": created.Output["ChecksumType"], "storageClass": "STANDARD_IA", "tags": "env=snapshot"},
		"part":     map[string]any{"checksum": part.Headers.Get("x-amz-checksum-sha256")},
		"list":     map[string]any{"algorithm": listed.Output["ChecksumAlgorithm"], "type": listed.Output["ChecksumType"], "part": listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumSHA256"]},
		"complete": map[string]any{"checksum": done.Output["ChecksumSHA256"], "type": done.Output["ChecksumType"]},
		"object":   map[string]any{"storageClass": head.Headers.Get("x-amz-storage-class"), "tags": tags},
	})
}

func TestMultipartCreationAttributes(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{
		"Bucket": "bucket", "Key": "attributes", "StorageClass": "STANDARD_IA", "Tagging": "team=storage&env=test",
	}, nil)
	id := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "attributes", "UploadId": id, "PartNumber": 1}, []byte("body"))
	complete := completeInput(id, completedPart(1, part))
	complete["StorageClass"], complete["Tagging"] = "STANDARD", "ignored=true"
	mustInvoke(t, p, "CompleteMultipartUpload", complete, nil)

	head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil)
	if head.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart storage class = %v", head.Headers)
	}
	get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil)
	if get.Headers.Get("x-amz-storage-class") != "STANDARD_IA" {
		t.Fatalf("multipart get storage class = %v", get.Headers)
	}
	_ = get.Stream.Close()
	tags := mustInvoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "bucket", "Key": "attributes"}, nil).Output["TagSet"].([]any)
	if len(tags) != 2 || asMapForTest(tags[0])["Key"] != "env" || asMapForTest(tags[0])["Value"] != "test" || asMapForTest(tags[1])["Key"] != "team" || asMapForTest(tags[1])["Value"] != "storage" {
		t.Fatalf("multipart tags = %#v", tags)
	}
	standard := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, []byte("body"))
	if head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "bucket", "Key": "standard"}, nil); standard.Headers.Get("x-amz-storage-class") != "" || head.Headers.Get("x-amz-storage-class") != "" {
		t.Fatalf("standard storage class headers = put %v head %v", standard.Headers, head.Headers)
	}
}

func TestCompleteMultipartUploadManifest(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	create := func(key string) string {
		t.Helper()
		return mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key}, nil).Output["UploadId"].(string)
	}
	wantFault := func(uploadID, code string, parts ...any) {
		t.Helper()
		_, err := invoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, parts...), nil)
		if fault := asFault(t, err); fault.Code != code || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete fault = %#v want %s", fault, code)
		}
	}

	noncontiguous := create("noncontiguous")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 1}, []byte("omitted"))
	third := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": noncontiguous, "PartNumber": 3}, []byte("third"))
	done := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(noncontiguous, completedPart(3, third)), nil)
	if !regexp.MustCompile(`-1"$`).MatchString(done.Headers.Get("ETag")) {
		t.Fatalf("selected part ETag = %q", done.Headers.Get("ETag"))
	}
	if got := readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "bucket", "Key": "noncontiguous"}, nil)); string(got) != "third" {
		t.Fatalf("noncontiguous completion = %q", got)
	}

	wrongETag := create("wrong-etag")
	mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": wrongETag, "PartNumber": 1}, []byte("one"))
	wantFault(wrongETag, "InvalidPart", map[string]any{"PartNumber": 1, "ETag": `"wrong"`})
	missing := create("missing")
	wantFault(missing, "InvalidPart", map[string]any{"PartNumber": 9, "ETag": `"missing"`})

	badOrder := create("order")
	large := bytes.Repeat([]byte("A"), 5<<20)
	second := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 2}, large)
	first := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": badOrder, "PartNumber": 1}, []byte("last"))
	wantFault(badOrder, "InvalidPartOrder", completedPart(2, second), completedPart(1, first))

	tooSmall := create("small")
	smallFirst := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 1}, []byte("small"))
	smallLast := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": tooSmall, "PartNumber": 2}, []byte("last"))
	wantFault(tooSmall, "EntityTooSmall", completedPart(1, smallFirst), completedPart(2, smallLast))

	sized := create("sized")
	sizedPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": sized, "PartNumber": 1}, []byte("sized"))
	sizedInput := completeInput(sized, completedPart(1, sizedPart))
	sizedInput["MpuObjectSize"] = "4"
	_, err := invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("invalid object size fault = %#v", fault)
	}
	sizedInput["MpuObjectSize"] = "5"
	mustInvoke(t, p, "CompleteMultipartUpload", sizedInput, nil)
	zero := create("zero")
	zeroPart := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": zero, "PartNumber": 1}, []byte{})
	zeroInput := completeInput(zero, completedPart(1, zeroPart))
	zeroInput["MpuObjectSize"] = "invalid"
	_, err = invoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
	if fault := asFault(t, err); fault.Code != "InvalidRequest" {
		t.Fatalf("zero object size fault = %#v", fault)
	}
	zeroInput["MpuObjectSize"] = "0"
	mustInvoke(t, p, "CompleteMultipartUpload", zeroInput, nil)
	wantFault(create("empty"), "InvalidPart")
}

func TestListPartsAndMultipartUploads(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	id, _ := created.Output["UploadId"].(string)
	part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": id, "PartNumber": 1}, []byte("AAA"))
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": id}, nil)
	parts, _ := listed.Output["Parts"].([]any)
	if len(parts) != 1 || listed.Output["ChecksumAlgorithm"] != "CRC64NVME" || listed.Output["ChecksumType"] != "FULL_OBJECT" {
		t.Fatalf("ListParts %v", listed.Output)
	}
	paged := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "paged", "StorageClass": "STANDARD_IA", "ChecksumAlgorithm": "CRC32"}, nil)
	pagedID := paged.Output["UploadId"].(string)
	for _, number := range []int{3, 1, 2} {
		input := map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "PartNumber": number}
		if number == 3 {
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE([]byte("CCC")))
			input["ChecksumCRC32"] = base64.StdEncoding.EncodeToString(sum)
		}
		mustInvoke(t, p, "UploadPart", input, bytes.Repeat([]byte{byte('A' + number - 1)}, 3))
	}
	firstPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "MaxParts": 2}, nil)
	firstParts := firstPage.Output["Parts"].([]any)
	if len(firstParts) != 2 || firstParts[0].(map[string]any)["PartNumber"] != 1 || firstParts[1].(map[string]any)["PartNumber"] != 2 || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextPartNumberMarker"] != 2 || firstPage.Output["StorageClass"] != "STANDARD_IA" || firstPage.Output["ChecksumAlgorithm"] != "CRC32" || firstPage.Output["ChecksumType"] != "COMPOSITE" {
		t.Fatalf("ListParts first page %v", firstPage.Output)
	}
	secondPage := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "PartNumberMarker": 2, "MaxParts": 2}, nil)
	last := secondPage.Output["Parts"].([]any)[0].(map[string]any)
	if last["PartNumber"] != 3 || last["LastModified"] == "" || last["ChecksumCRC32"] == nil || secondPage.Output["IsTruncated"] != false || secondPage.Output["PartNumberMarker"] != 2 {
		t.Fatalf("ListParts second page %v", secondPage.Output)
	}
	for _, input := range []map[string]any{
		{"Bucket": "bucket", "Key": "paged", "UploadId": "missing"},
		{"Bucket": "bucket", "Key": "wrong", "UploadId": pagedID},
	} {
		_, err := invoke(t, p, "ListParts", input, nil)
		if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.HTTPStatus != http.StatusNotFound {
			t.Fatalf("ListParts missing upload fault = %#v", fault)
		}
	}
	_, err := invoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID, "MaxParts": 1001}, nil)
	if fault := asFault(t, err); fault.Code != "InvalidArgument" {
		t.Fatalf("ListParts max fault = %#v", fault)
	}
	ups := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket"}, nil)
	uploads, _ := ups.Output["Uploads"].([]any)
	if len(uploads) != 2 {
		t.Fatalf("ListMultipartUploads %v", ups.Output)
	}
	mustInvoke(t, p, "CompleteMultipartUpload", completeInput(id, completedPart(1, part)), nil)
	mustInvoke(t, p, "AbortMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "paged", "UploadId": pagedID}, nil)
	after := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket"}, nil)
	uploads, _ = after.Output["Uploads"].([]any)
	if len(uploads) != 0 {
		t.Fatalf("completed upload still listed: %v", after.Output)
	}
}

func TestListMultipartUploadsPaginationAndDelimiter(t *testing.T) {
	deps := spitest.Deps(t)
	p := s3.New(deps)
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	create := func(key, storageClass string) string {
		t.Helper()
		response := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": key, "StorageClass": storageClass}, nil)
		_ = deps.Clock.Advance(time.Second)
		return response.Output["UploadId"].(string)
	}
	create("photos/2026/b.jpg", "STANDARD")
	firstSame := create("same", "STANDARD_IA")
	create("alpha", "STANDARD")
	secondSame := create("same", "STANDARD")
	create("space key", "STANDARD")

	firstPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "MaxUploads": 3}, nil)
	first := firstPage.Output["Uploads"].([]any)
	if len(first) != 3 || first[0].(map[string]any)["Key"] != "alpha" || first[1].(map[string]any)["Key"] != "photos/2026/b.jpg" || first[2].(map[string]any)["UploadId"] != firstSame || first[2].(map[string]any)["StorageClass"] != "STANDARD_IA" || first[2].(map[string]any)["Initiated"] == "" || firstPage.Output["IsTruncated"] != true || firstPage.Output["NextKeyMarker"] != "same" || firstPage.Output["NextUploadIdMarker"] != firstSame {
		t.Fatalf("first multipart page = %v", firstPage.Output)
	}
	secondPage := mustInvoke(t, p, "ListMultipartUploads", map[string]any{
		"Bucket": "bucket", "KeyMarker": "same", "UploadIdMarker": firstSame, "MaxUploads": 3,
	}, nil)
	second := secondPage.Output["Uploads"].([]any)
	if len(second) != 2 || second[0].(map[string]any)["UploadId"] != secondSame || second[1].(map[string]any)["Key"] != "space key" || secondPage.Output["IsTruncated"] != false {
		t.Fatalf("second multipart page = %v", secondPage.Output)
	}
	grouped := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "Prefix": "photos/", "Delimiter": "/"}, nil)
	groups := grouped.Output["CommonPrefixes"].([]any)
	if len(grouped.Output["Uploads"].([]any)) != 0 || len(groups) != 1 || groups[0].(map[string]any)["Prefix"] != "photos/2026/" {
		t.Fatalf("grouped multipart uploads = %v", grouped.Output)
	}
	encoded := mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "bucket", "Prefix": "space", "EncodingType": "url"}, nil)
	if encoded.Output["Uploads"].([]any)[0].(map[string]any)["Key"] != "space%20key" || encoded.Output["EncodingType"] != "url" {
		t.Fatalf("encoded multipart uploads = %v", encoded.Output)
	}
	for _, test := range []struct {
		input      map[string]any
		code       string
		httpStatus int
	}{
		{map[string]any{"Bucket": "missing"}, "NoSuchBucket", http.StatusNotFound},
		{map[string]any{"Bucket": "bucket", "MaxUploads": 0}, "InvalidArgument", http.StatusBadRequest},
	} {
		_, err := invoke(t, p, "ListMultipartUploads", test.input, nil)
		if fault := asFault(t, err); fault.Code != test.code || fault.HTTPStatus != test.httpStatus {
			t.Fatalf("invalid multipart listing fault = %#v", fault)
		}
	}
}

func TestMultipartOperationsRejectMissingUpload(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, operation := range []string{"UploadPart", "CompleteMultipartUpload", "ListParts", "AbortMultipartUpload"} {
		for _, input := range []map[string]any{
			{"Bucket": "bucket", "Key": "k", "UploadId": "missing", "PartNumber": 1},
			{"Bucket": "bucket", "Key": "wrong", "UploadId": uploadID, "PartNumber": 1},
			{"Bucket": "wrong", "Key": "k", "UploadId": uploadID, "PartNumber": 1},
		} {
			if operation == "CompleteMultipartUpload" {
				input["MultipartUpload"] = map[string]any{"Parts": []any{}}
			}
			_, err := invoke(t, p, operation, input, []byte("part"))
			expected := "NoSuchUpload"
			if input["Bucket"] == "wrong" {
				expected = "NoSuchBucket"
			}
			if fault := asFault(t, err); fault.Code != expected || fault.HTTPStatus != http.StatusNotFound {
				t.Fatalf("%s fault = %#v", operation, fault)
			}
		}
	}
	mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": uploadID}, nil)
}

func TestMultipartPartNumberBounds(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "bucket"}, nil)
	created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "bucket", "Key": "k"}, nil)
	uploadID := created.Output["UploadId"].(string)
	for _, input := range []map[string]any{
		{"UploadId": uploadID},
		{"UploadId": uploadID, "PartNumber": -1},
		{"UploadId": uploadID, "PartNumber": 0},
		{"UploadId": uploadID, "PartNumber": 10001},
	} {
		_, err := invoke(t, p, "UploadPart", input, []byte("part"))
		if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("UploadPart %#v fault = %#v", input, fault)
		}
	}
	last := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 10000}, []byte("last"))
	for _, number := range []int{0, 10001} {
		input := completeInput(uploadID, map[string]any{"PartNumber": number, "ETag": last.Headers.Get("ETag")})
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != "InvalidPart" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("complete part %d fault = %#v", number, fault)
		}
	}
	listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "bucket", "Key": "k", "UploadId": uploadID}, nil)
	if listed.Output["Parts"].([]any)[0].(map[string]any)["PartNumber"] != 10000 {
		t.Fatalf("valid boundary part = %v", listed.Output)
	}
}

func TestMissingBucket404(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "nope", "Key": "k"}, []byte("x"))
	f := asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("put missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "ListObjectsV2", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("list missing bucket: %+v", f)
	}
	_, err = invoke(t, p, "HeadBucket", map[string]any{"Bucket": "nope"}, nil)
	f = asFault(t, err)
	if f.HTTPStatus != 404 || f.Code != "NoSuchBucket" {
		t.Fatalf("head missing bucket: %+v", f)
	}
}

func TestReplicationFiltersStatusMetadataAndDeleteMarker(t *testing.T) {
	p := s3.New(spitest.Deps(t))
	west := ident()
	west.Region = "us-west-2"
	mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "source", "ObjectLockEnabledForBucket": true}, nil)
	mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "source", "Status": "Enabled"}, nil)
	mustInvokeAs(t, p, west, "CreateBucket", map[string]any{"Bucket": "destination", "LocationConstraint": "us-west-2", "ObjectLockEnabledForBucket": true}, nil)
	mustInvokeAs(t, p, west, "PutBucketVersioning", map[string]any{"Bucket": "destination", "Status": "Enabled"}, nil)
	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source",
		"ReplicationConfiguration": map[string]any{"Rules": []any{map[string]any{
			"Status": "Enabled",
			"Filter": map[string]any{"And": map[string]any{
				"Prefix": "logs/",
				"Tags":   []any{map[string]any{"Key": "environment", "Value": "test"}},
			}},
			"DeleteMarkerReplication": map[string]any{"Status": "Enabled"},
			"Destination":             map[string]any{"Bucket": "arn:aws:s3:::destination", "StorageClass": "STANDARD_IA"},
		}}},
	}, nil)

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "other/file", "Tagging": "environment=test"}, []byte("skip"))
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "other/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("unmatched object was replicated: %v", err)
	}

	put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/file", "Tagging": "environment=test"}, []byte("replicated"))
	if got := put.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("source replication status %q", got)
	}
	dst := mustInvokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	if got := string(readStream(t, dst)); got != "replicated" {
		t.Fatalf("replica body %q", got)
	}
	if got := dst.Headers.Get("x-amz-replication-status"); got != "REPLICA" {
		t.Fatalf("destination replication status %q", got)
	}
	if got := dst.Headers.Get("x-amz-storage-class"); got != "STANDARD_IA" {
		t.Fatalf("destination storage class %q", got)
	}

	mustInvoke(t, p, "PutObjectTagging", map[string]any{"Bucket": "source", "Key": "logs/file", "TagSet": []any{
		map[string]any{"Key": "environment", "Value": "test"},
		map[string]any{"Key": "owner", "Value": "mirror"},
	}}, nil)
	tags := mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	gotTags := tags.Output["TagSet"].([]any)
	if len(gotTags) != 2 || gotTags[1].(map[string]any)["Value"] != "mirror" {
		t.Fatalf("replica tags %v", gotTags)
	}
	mustInvoke(t, p, "PutObjectLegalHold", map[string]any{"Bucket": "source", "Key": "logs/file", "LegalHold": map[string]any{"Status": "ON"}}, nil)
	legalHold := mustInvokeAs(t, p, west, "GetObjectLegalHold", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil)
	if got := asMapForTest(legalHold.Output["LegalHold"])["Status"]; got != "ON" {
		t.Fatalf("replica legal hold %v", legalHold.Output)
	}

	del := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "source", "Key": "logs/file"}, nil)
	if got := del.Headers.Get("x-amz-replication-status"); got != "COMPLETED" {
		t.Fatalf("delete-marker replication status %q", got)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/file"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("replica delete marker not visible: %v", err)
	}

	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "logs/batch", "Tagging": "environment=test"}, []byte("batch"))
	deleted := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "source", "Objects": []any{map[string]any{"Key": "logs/batch"}}}, nil)
	if got := deleted.Output["Deleted"].([]any)[0].(map[string]any)["DeleteMarker"]; got != true {
		t.Fatalf("batch delete marker %v", deleted.Output)
	}
	if _, err := invokeAs(t, p, west, "GetObject", map[string]any{"Bucket": "destination", "Key": "logs/batch"}, nil); asFault(t, err).Code != "NoSuchKey" {
		t.Fatalf("batch replica delete marker not visible: %v", err)
	}

	mustInvoke(t, p, "PutBucketReplication", map[string]any{
		"Bucket": "source", "ReplicationConfiguration": map[string]any{"Rules": []any{map[string]any{
			"Status": "Enabled", "Filter": map[string]any{"Prefix": "plain/"},
			"Destination": map[string]any{"Bucket": "arn:aws:s3:::destination"},
		}}},
	}, nil)
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file", "Tagging": "owner=mirror"}, []byte("tagged"))
	mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "source", "Key": "plain/file"}, []byte("untagged"))
	if tags := asSliceForTest(mustInvokeAs(t, p, west, "GetObjectTagging", map[string]any{"Bucket": "destination", "Key": "plain/file"}, nil).Output["TagSet"]); len(tags) != 0 {
		t.Fatalf("replica inherited overwritten tags: %#v", tags)
	}
}

func asMapForTest(value any) map[string]any {
	result, _ := value.(map[string]any)
	return result
}

func asSliceForTest(value any) []any {
	result, _ := value.([]any)
	return result
}
