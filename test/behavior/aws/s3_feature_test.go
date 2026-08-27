package behavior

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"

	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	_ "github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/sts"
)

func TestS3ObjectLifecycle(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.s3"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()
	auth := "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00"

	do := func(method, path string, body []byte, storageClass string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		if method == http.MethodPut && path == "/object-lock" {
			req.Header.Set("x-amz-bucket-object-lock-enabled", "true")
		}
		if strings.Contains(path, "?delete") {
			digest := md5.Sum(body)
			req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(digest[:]))
		}
		if storageClass != "" {
			req.Header.Set("x-amz-storage-class", storageClass)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("Given no bucket When PUT object Then not found or created after bucket", func(t *testing.T) {
		// Given a fresh emulator
		// When the user creates a bucket
		res := do(http.MethodPut, "/demo", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		// And uploads an object
		res = do(http.MethodPut, "/demo/readme.txt", []byte("hello"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("put %d", res.StatusCode)
		}
		// Then GET returns the same bytes
		res = do(http.MethodGet, "/demo/readme.txt", nil, "")
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != 200 || string(b) != "hello" {
			t.Fatalf("get %d %q", res.StatusCode, b)
		}
		// And fidelity is declared
		if res.Header.Get("x-mirror-fidelity") != "emulate" {
			t.Fatalf("fidelity %q", res.Header.Get("x-mirror-fidelity"))
		}
	})

	t.Run("Given a non-empty bucket When DELETE bucket Then it is rejected and preserved", func(t *testing.T) {
		res := do(http.MethodDelete, "/demo", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusConflict || !bytes.Contains(fault, []byte("BucketNotEmpty")) {
			t.Fatalf("delete non-empty bucket %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/demo/readme.txt", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "hello" {
			t.Fatalf("object after rejected delete %d %q", res.StatusCode, body)
		}
	})

	t.Run("Given a globally owned bucket When another identity creates it Then ownership errors are returned", func(t *testing.T) {
		create := func(account, region string) (int, []byte) {
			t.Helper()
			var requestBody io.Reader
			if region != "us-east-1" {
				requestBody = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + region + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(http.MethodPut, ts.URL+"/global-name", requestBody)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Account-Id", account)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, body
		}
		if status, _ := create("111111111111", "us-east-1"); status != http.StatusOK {
			t.Fatalf("initial create %d", status)
		}
		if status, _ := create("111111111111", "us-east-1"); status != http.StatusOK {
			t.Fatalf("idempotent create %d", status)
		}
		if status, body := create("222222222222", "us-east-1"); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyExists")) {
			t.Fatalf("cross-account create %d %s", status, body)
		}
		if status, body := create("111111111111", "us-west-2"); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("cross-region create %d %s", status, body)
		}
	})

	t.Run("Given a regional endpoint When creating a bucket Then its location constraint must match", func(t *testing.T) {
		request := func(method, bucket, region, constraint string) (int, []byte, string) {
			t.Helper()
			var body io.Reader
			if constraint != "" {
				body = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + constraint + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(method, ts.URL+"/"+bucket, body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, response, res.Header.Get("Location")
		}
		if status, body, _ := request(http.MethodPut, "regional-missing", "us-west-2", ""); status != http.StatusBadRequest || !bytes.Contains(body, []byte("IllegalLocationConstraintException")) {
			t.Fatalf("missing constraint %d %s", status, body)
		}
		if status, body, location := request(http.MethodPut, "regional-match", "us-west-2", "us-west-2"); status != http.StatusOK || location != ts.URL+"/regional-match/" {
			t.Fatalf("matching constraint %d %s location=%q", status, body, location)
		}
		if status, body, _ := request(http.MethodGet, "regional-match?location", "us-west-2", ""); status != http.StatusOK || !bytes.Contains(body, []byte(">us-west-2</LocationConstraint>")) {
			t.Fatalf("reported location %d %s", status, body)
		}
	})

	t.Run("Given an account regional namespace When creating a bucket Then it stays in that account and Region", func(t *testing.T) {
		create := func(account, region, name string) (int, []byte, string) {
			t.Helper()
			var body io.Reader
			if region != "us-east-1" {
				body = strings.NewReader("<CreateBucketConfiguration><LocationConstraint>" + region + "</LocationConstraint></CreateBucketConfiguration>")
			}
			req, err := http.NewRequest(http.MethodPut, ts.URL+"/"+name, body)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Account-Id", account)
			req.Header.Set("X-Mirror-Region", region)
			req.Header.Set("x-amz-bucket-namespace", "account-regional")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			response, _ := io.ReadAll(res.Body)
			res.Body.Close()
			return res.StatusCode, response, res.Header.Get("Location")
		}

		eastName := "behavior-111111111111-us-east-1-an"
		if status, body, location := create("111111111111", "us-east-1", eastName); status != http.StatusOK || location != "/"+eastName {
			t.Fatalf("east create %d %s location=%q", status, body, location)
		}
		if status, body, _ := create("111111111111", "us-east-1", eastName); status != http.StatusConflict || !bytes.Contains(body, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("east recreate %d %s", status, body)
		}
		if status, body, _ := create("222222222222", "us-east-1", eastName); status != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBucketName")) {
			t.Fatalf("foreign suffix %d %s", status, body)
		}
		westName := "behavior-111111111111-us-west-2-an"
		if status, body, location := create("111111111111", "us-west-2", westName); status != http.StatusOK || location != "/"+westName {
			t.Fatalf("west create %d %s location=%q", status, body, location)
		}
	})

	t.Run("Given buckets in multiple Regions When listing pages Then filters and cursors are honored", func(t *testing.T) {
		request := func(method, path, region, body string) *http.Response {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			req.Header.Set("X-Mirror-Region", region)
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return res
		}
		for _, bucket := range []struct{ name, region, body string }{
			{"list-behavior-east", "us-east-1", ""},
			{"list-behavior-west", "us-west-2", "<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>"},
		} {
			res := request(http.MethodPut, "/"+bucket.name, bucket.region, bucket.body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s: %d", bucket.name, res.StatusCode)
			}
		}
		type page struct {
			Buckets []struct {
				Name         string `xml:"Name"`
				BucketRegion string `xml:"BucketRegion"`
			} `xml:"Buckets>Bucket"`
			Prefix            string `xml:"Prefix"`
			ContinuationToken string `xml:"ContinuationToken"`
		}
		list := func(path string) page {
			t.Helper()
			res := request(http.MethodGet, path, "us-east-1", "")
			defer res.Body.Close()
			var got page
			if err := xml.NewDecoder(res.Body).Decode(&got); err != nil || res.StatusCode != http.StatusOK {
				t.Fatalf("list %s: %d %#v %v", path, res.StatusCode, got, err)
			}
			return got
		}
		first := list("/?max-buckets=1&prefix=list-behavior")
		if len(first.Buckets) != 1 || first.Buckets[0].Name != "list-behavior-east" || first.Buckets[0].BucketRegion != "us-east-1" || first.Prefix != "list-behavior" || first.ContinuationToken == "" {
			t.Fatalf("first page: %#v", first)
		}
		second := list("/?max-buckets=1&prefix=list-behavior&continuation-token=" + url.QueryEscape(first.ContinuationToken))
		if len(second.Buckets) != 1 || second.Buckets[0].Name != "list-behavior-west" || second.Buckets[0].BucketRegion != "us-west-2" || second.ContinuationToken != "" {
			t.Fatalf("second page: %#v", second)
		}
	})

	t.Run("Given a bucket When versioning changes Then its state matches AWS", func(t *testing.T) {
		res := do(http.MethodPut, "/versioning-state", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/versioning-state?versioning", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<Status>")) {
			t.Fatalf("unset versioning %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/versioning-state?versioning", []byte("<VersioningConfiguration/>"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("IllegalVersioningConfigurationException")) {
			t.Fatalf("missing versioning status %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/versioning-state?versioning", []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("enable versioning %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/versioning-state?versioning", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) {
			t.Fatalf("enabled versioning %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given object versions When current version is deleted Then previous version is current", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/version-restore", ""},
			{http.MethodPut, "/version-restore?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
			{http.MethodPut, "/version-restore/key", "old"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode >= 300 {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		newer := do(http.MethodPut, "/version-restore/key", []byte("new"), "")
		io.Copy(io.Discard, newer.Body)
		newer.Body.Close()
		version := newer.Header.Get("x-amz-version-id")
		deleted := do(http.MethodDelete, "/version-restore/key?versionId="+url.QueryEscape(version), nil, "")
		io.Copy(io.Discard, deleted.Body)
		deleted.Body.Close()
		if deleted.StatusCode != http.StatusNoContent || deleted.Header.Get("x-amz-version-id") != version {
			t.Fatalf("delete version = %d %v", deleted.StatusCode, deleted.Header)
		}
		restored := do(http.MethodGet, "/version-restore/key", nil, "")
		body, _ := io.ReadAll(restored.Body)
		restored.Body.Close()
		if restored.StatusCode != http.StatusOK || string(body) != "old" || restored.Header.Get("x-amz-version-id") == version {
			t.Fatalf("restored = %d %q %v", restored.StatusCode, body, restored.Header)
		}
	})

	t.Run("Given a legal hold When deleting Then only a delete marker is allowed", func(t *testing.T) {
		res := do(http.MethodPut, "/object-lock", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create object-lock bucket = %d", res.StatusCode)
		}
		put := do(http.MethodPut, "/object-lock/key", []byte("locked"), "")
		put.Body.Close()
		version := put.Header.Get("x-amz-version-id")
		hold := do(http.MethodPut, "/object-lock/key?legal-hold&versionId="+url.QueryEscape(version), []byte("<LegalHold><Status>ON</Status></LegalHold>"), "")
		hold.Body.Close()
		if hold.StatusCode != http.StatusOK {
			t.Fatalf("put legal hold = %d", hold.StatusCode)
		}
		permanent := do(http.MethodDelete, "/object-lock/key?versionId="+url.QueryEscape(version), nil, "")
		body, _ := io.ReadAll(permanent.Body)
		permanent.Body.Close()
		if permanent.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("AccessDenied")) {
			t.Fatalf("permanent delete = %d %s", permanent.StatusCode, body)
		}
		marker := do(http.MethodDelete, "/object-lock/key", nil, "")
		marker.Body.Close()
		if marker.StatusCode != http.StatusNoContent || marker.Header.Get("x-amz-delete-marker") != "true" {
			t.Fatalf("simple delete = %d %v", marker.StatusCode, marker.Header)
		}
	})

	t.Run("Given default retention When writing Then the new version is protected", func(t *testing.T) {
		configuration := []byte("<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>GOVERNANCE</Mode><Days>3</Days></DefaultRetention></Rule></ObjectLockConfiguration>")
		configured := do(http.MethodPut, "/object-lock?object-lock", configuration, "")
		configured.Body.Close()
		if configured.StatusCode != http.StatusOK {
			t.Fatalf("configure default retention = %d", configured.StatusCode)
		}
		put := do(http.MethodPut, "/object-lock/default-retention", []byte("locked"), "")
		put.Body.Close()
		version := put.Header.Get("x-amz-version-id")
		retention := do(http.MethodGet, "/object-lock/default-retention?retention&versionId="+url.QueryEscape(version), nil, "")
		body, _ := io.ReadAll(retention.Body)
		retention.Body.Close()
		if retention.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Mode>GOVERNANCE</Mode>")) || !bytes.Contains(body, []byte("<RetainUntilDate>1970-01-04T00:00:00Z</RetainUntilDate>")) {
			t.Fatalf("default retention = %d %s", retention.StatusCode, body)
		}
		deleted := do(http.MethodDelete, "/object-lock/default-retention?versionId="+url.QueryEscape(version), nil, "")
		fault, _ := io.ReadAll(deleted.Body)
		deleted.Body.Close()
		if deleted.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("AccessDenied")) {
			t.Fatalf("delete retained version = %d %s", deleted.StatusCode, fault)
		}
	})

	t.Run("Given object versions When multi-delete is quiet Then only errors are returned", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/multi-delete", ""},
			{http.MethodPut, "/multi-delete?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			res.Body.Close()
			if res.StatusCode >= 300 {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		first := do(http.MethodPut, "/multi-delete/key", []byte("old"), "")
		first.Body.Close()
		firstVersion := first.Header.Get("x-amz-version-id")
		second := do(http.MethodPut, "/multi-delete/key", []byte("new"), "")
		second.Body.Close()
		secondVersion := second.Header.Get("x-amz-version-id")
		probe := []byte("<Delete><Object><Key>key</Key></Object></Delete>")
		probeDigest := md5.Sum(probe)
		for _, test := range []struct{ name, checksum, algorithm, code string }{
			{"missing", "", "", "MissingContentMD5"},
			{"mismatched", "AA==", "", "BadDigest"},
			{"algorithm without value", base64.StdEncoding.EncodeToString(probeDigest[:]), "CRC32", "InvalidRequest"},
		} {
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/multi-delete?delete", bytes.NewReader(probe))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if test.checksum != "" {
				req.Header.Set("Content-MD5", test.checksum)
			}
			if test.algorithm != "" {
				req.Header.Set("x-amz-sdk-checksum-algorithm", test.algorithm)
			}
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte(test.code)) {
				t.Fatalf("%s checksum = %d %s", test.name, res.StatusCode, body)
			}
		}
		res := do(http.MethodPost, "/multi-delete?delete", []byte("<Delete><Object><Key>key</Key><VersionId>"+secondVersion+"</VersionId></Object></Delete>"), "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)) || !bytes.Contains(body, []byte("<VersionId>"+secondVersion+"</VersionId>")) || bytes.Contains(body, []byte("<DeleteMarker>")) {
			t.Fatalf("verbose multi-delete = %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/multi-delete/key", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "old" {
			t.Fatalf("restored multi-delete = %d %q", res.StatusCode, body)
		}
		res = do(http.MethodPost, "/multi-delete?delete", []byte("<Delete><Object><Key>key</Key><VersionId>missing</VersionId></Object><Object><Key>key</Key><VersionId>"+firstVersion+"</VersionId></Object><Quiet>true</Quiet></Delete>"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Code>NoSuchVersion</Code>")) || !bytes.Contains(body, []byte("<VersionId>missing</VersionId>")) || bytes.Contains(body, []byte("<Deleted>")) {
			t.Fatalf("quiet multi-delete = %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an invalid bucket name When creating it Then it is rejected", func(t *testing.T) {
		for _, name := range []string{"ab", "192.168.5.4", "reserved--table-s3"} {
			res := do(http.MethodPut, "/"+name, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidBucketName")) {
				t.Fatalf("name %q = %d %s", name, res.StatusCode, body)
			}
		}
		res := do(http.MethodPut, "/valid-after-invalid", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("valid create after invalid names %d", res.StatusCode)
		}
	})

	t.Run("Given an invalid storage class When PUT object Then it is rejected", func(t *testing.T) {
		res := do(http.MethodPut, "/classes", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/classes/object", []byte("body"), "glacier")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidStorageClass")) {
			t.Fatalf("invalid storage class %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/classes/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid storage class created object: %d", res.StatusCode)
		}
	})

	t.Run("Given an XXHash checksum When PUT and GET object Then the checksum round trips", func(t *testing.T) {
		res := do(http.MethodPut, "/xxhash-behavior", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		request := func(method, path string, body []byte, checksum string) *http.Response {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if checksum != "" {
				req.Header.Set("x-amz-checksum-xxhash64", checksum)
			} else if method == http.MethodGet {
				req.Header.Set("x-amz-checksum-mode", "ENABLED")
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		checksum := "jLhB20DmroM="
		res = request(http.MethodPut, "/xxhash-behavior/object", []byte("123456789"), checksum)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-checksum-xxhash64") != checksum || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("put xxhash %d %v", res.StatusCode, res.Header)
		}
		res = request(http.MethodGet, "/xxhash-behavior/object", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "123456789" || res.Header.Get("x-amz-checksum-xxhash64") != checksum || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("get xxhash %d %q %v", res.StatusCode, body, res.Header)
		}
		res = request(http.MethodPut, "/xxhash-behavior/bad", []byte("123456789"), "AA==")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("BadDigest")) {
			t.Fatalf("bad xxhash %d %s", res.StatusCode, fault)
		}
	})

	t.Run("Given an oversized object key When PUT object Then it is rejected", func(t *testing.T) {
		res := do(http.MethodPut, "/key-limits", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		path := "/key-limits/" + strings.Repeat("x", 1025)
		res = do(http.MethodPut, path, []byte("body"), "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("KeyTooLongError")) {
			t.Fatalf("oversized object key %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, path, nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("oversized object key created object: %d", res.StatusCode)
		}
	})

	t.Run("Given an archived object When restored Then reads become available", func(t *testing.T) {
		res := do(http.MethodPut, "/cold", nil, "")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/cold/archive.txt", []byte("cold-data"), "GLACIER")
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("put archive %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/cold/archive.txt", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-storage-class") != "GLACIER" || res.Header.Get("x-amz-restore") != "" {
			t.Fatalf("head unrestored archive %d %v", res.StatusCode, res.Header)
		}
		res = do(http.MethodGet, "/cold/archive.txt", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("InvalidObjectState")) {
			t.Fatalf("get unrestored archive %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodPost, "/cold/archive.txt?restore", []byte(`<RestoreRequest><Days>1</Days></RestoreRequest>`), "")
		res.Body.Close()
		if res.StatusCode != http.StatusAccepted {
			t.Fatalf("restore archive %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cold/archive.txt", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "cold-data" || res.Header.Get("x-amz-restore") == "" {
			t.Fatalf("get restored archive %d %q %v", res.StatusCode, body, res.Header)
		}
	})
}

func TestSTSGetCallerIdentity(t *testing.T) {
	deps := spitest.Deps(t)
	cfg := config.Default()
	cfg.Services = []string{"aws.sts"}
	reg, err := registry.New(deps, cfg.Services, nil)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(edge.New(cfg, deps, reg, "test").Handler())
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader([]byte("Action=GetCallerIdentity&Version=2011-06-15")))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=00")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 {
		t.Fatalf("%d %s", res.StatusCode, b)
	}
	if !bytes.Contains(b, []byte("Account")) && !bytes.Contains(b, []byte("account")) && !bytes.Contains(b, []byte("000000000000")) {
		t.Fatalf("body %s", b)
	}
}
