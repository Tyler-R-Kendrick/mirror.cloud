package behavior

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"io"
	"maps"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/config"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/edge"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/registry"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
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
	testIdentity := spi.Identity{Account: "000000000000", Region: "us-east-1"}
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/multipart-behavior", "Enabled")
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/kms-bdd", "Enabled")
	spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/disabled-bdd", "Disabled")

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
		if method == http.MethodPut && path == "/create-owned" {
			req.Header.Set("x-amz-object-ownership", "BucketOwnerPreferred")
		}
		if method == http.MethodPut && path == "/invalid-create-owned" {
			req.Header.Set("x-amz-object-ownership", "")
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

	t.Run("Given a bucket in another Region When accessed Then S3 resolves it and reports its Region", func(t *testing.T) {
		configuration := []byte(`<CreateBucketConfiguration><LocationConstraint>us-west-2</LocationConstraint></CreateBucketConfiguration>`)
		res := do(http.MethodPut, "/cross-region-bdd", configuration, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create cross-region bucket %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/cross-region-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/xml" || res.Header.Get("x-amz-access-point-alias") != "false" || res.Header.Get("x-amz-bucket-region") != "us-west-2" || res.Header.Get("x-amz-bucket-arn") != "arn:aws:s3:::cross-region-bdd" || res.Header.Get("x-amz-request-id") == "" || res.Header.Get("x-amz-id-2") == "" {
			t.Fatalf("cross-region head %d %#v", res.StatusCode, res.Header)
		}
		res = do(http.MethodPut, "/cross-region-bdd/key", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("cross-region put %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cross-region-bdd?list-type=2", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-bucket-region") != "us-west-2" || !bytes.Contains(body, []byte("<Key>key</Key>")) || !bytes.Contains(body, []byte("<BucketRegion>us-west-2</BucketRegion>")) {
			t.Fatalf("cross-region list %d %#v %s", res.StatusCode, res.Header, body)
		}
		res = do(http.MethodGet, "/cross-region-bdd", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<BucketRegion>us-west-2</BucketRegion>")) {
			t.Fatalf("cross-region list V1 %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given delimited objects When listing V1 and V2 Then prefixes consume pagination slots", func(t *testing.T) {
		res := do(http.MethodPut, "/list-pagination-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		for _, key := range []string{"folder/aSubfolder/subFile1", "folder/aSubfolder/subFile2", "folder/file1", "folder/file2"} {
			res = do(http.MethodPut, "/list-pagination-bdd/"+key, []byte("content"), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put %q: %d", key, res.StatusCode)
			}
		}
		list := func(query string) string {
			t.Helper()
			response := do(http.MethodGet, "/list-pagination-bdd?"+query, nil, "")
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("list %q: %d %s", query, response.StatusCode, body)
			}
			return string(body)
		}
		first := list("prefix=folder%2F&delimiter=%2F&max-keys=1")
		if !strings.Contains(first, "<CommonPrefixes><Prefix>folder/aSubfolder/</Prefix></CommonPrefixes>") || !strings.Contains(first, "<NextMarker>folder/aSubfolder/</NextMarker>") || strings.Contains(first, "<Contents>") {
			t.Fatalf("first V1 page %s", first)
		}
		next := list("prefix=folder%2F&delimiter=%2F&max-keys=1&marker=" + url.QueryEscape("folder/aSubfolder/"))
		if !strings.Contains(next, "<Contents><ETag>") || !strings.Contains(next, "<Key>folder/file1</Key>") || !strings.Contains(next, "<Owner><ID>000000000000</ID></Owner>") || strings.Contains(next, "<DisplayName>") || !strings.Contains(next, "<Marker>folder/aSubfolder/</Marker>") {
			t.Fatalf("next V1 page %s", next)
		}
		firstV2 := list("list-type=2&prefix=folder%2F&delimiter=%2F&max-keys=1")
		if !strings.Contains(firstV2, "<CommonPrefixes><Prefix>folder/aSubfolder/</Prefix></CommonPrefixes>") || !strings.Contains(firstV2, "<NextContinuationToken>folder/aSubfolder/</NextContinuationToken>") || !strings.Contains(firstV2, "<KeyCount>1</KeyCount>") {
			t.Fatalf("first V2 page %s", firstV2)
		}
		defaultOwnerV2 := list("list-type=2&prefix=folder%2Ffile&max-keys=1")
		if strings.Contains(defaultOwnerV2, "<Owner>") {
			t.Fatalf("default V2 owner %s", defaultOwnerV2)
		}
		fetchedOwnerV2 := list("list-type=2&prefix=folder%2Ffile&max-keys=1&fetch-owner=true")
		if !strings.Contains(fetchedOwnerV2, "<Owner><ID>000000000000</ID></Owner>") || strings.Contains(fetchedOwnerV2, "<DisplayName>") {
			t.Fatalf("fetched V2 owner %s", fetchedOwnerV2)
		}
	})

	t.Run("Given a checksummed object When listing V1 and V2 Then checksum metadata is returned", func(t *testing.T) {
		res := do(http.MethodPut, "/list-checksum-bdd", nil, "")
		res.Body.Close()
		body := []byte("checksummed")
		sum := sha256.Sum256(body)
		request, _ := http.NewRequest(http.MethodPut, ts.URL+"/list-checksum-bdd/checksummed", bytes.NewReader(body))
		request.Header.Set("Authorization", auth)
		request.Header.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
		res, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		for _, query := range []string{"", "list-type=2"} {
			res = do(http.MethodGet, "/list-checksum-bdd?"+query, nil, "")
			listed, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK || !bytes.Contains(listed, []byte("<ChecksumAlgorithm>SHA256</ChecksumAlgorithm><ChecksumType>FULL_OBJECT</ChecksumType>")) || bytes.Contains(listed, []byte("<member>")) {
				t.Fatalf("list %q: %d %s", query, res.StatusCode, listed)
			}
		}
	})

	t.Run("Given invalid list encoding When listing Then S3 rejects every list operation", func(t *testing.T) {
		res := do(http.MethodPut, "/list-encoding-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		for _, route := range []string{"", "list-type=2&", "versions&", "uploads&"} {
			for _, value := range []string{"value", ""} {
				res = do(http.MethodGet, "/list-encoding-bdd?"+route+"encoding-type="+value, nil, "")
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidArgument</Code>")) || !bytes.Contains(body, []byte("<Message>Invalid Encoding Method specified in Request</Message>")) || !bytes.Contains(body, []byte("<ArgumentName>encoding-type</ArgumentName>")) {
					t.Fatalf("route %q encoding %q: %d %s", route, value, res.StatusCode, body)
				}
			}
		}
	})

	t.Run("Given object versions When listing pages Then markers include common prefixes", func(t *testing.T) {
		res := do(http.MethodPut, "/version-list-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/version-list-bdd?versioning", []byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("enable versioning %d", res.StatusCode)
		}
		for _, key := range []string{"folder/a/one", "folder/file1", "folder/file2"} {
			res = do(http.MethodPut, "/version-list-bdd/"+key, []byte("body"), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put %q: %d", key, res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/version-list-bdd?versions&prefix=folder%2F&delimiter=%2F&max-keys=1", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>folder/a/</Prefix></CommonPrefixes>")) || !bytes.Contains(body, []byte("<NextKeyMarker>folder/a/</NextKeyMarker>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("<Version>")) {
			t.Fatalf("first version page %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/version-list-bdd?versions&prefix=folder%2F&delimiter=%2F&max-keys=1&key-marker=folder%2Fa%2F", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Version>")) || !bytes.Contains(body, []byte("<Key>folder/file1</Key>")) || !bytes.Contains(body, []byte("<LastModified>")) {
			t.Fatalf("next version page %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/version-list-bdd?versions&version-id-marker=orphan", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidArgument</Code>")) || !bytes.Contains(body, []byte("<ArgumentName>version-id-marker</ArgumentName>")) {
			t.Fatalf("orphan version marker %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given multipart uploads When listing pages Then markers match LocalStack", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-list-bdd", nil, "")
		res.Body.Close()
		create := func(key, checksum string) string {
			t.Helper()
			request, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-list-bdd/"+key+"?uploads", nil)
			request.Header.Set("Authorization", auth)
			if checksum != "" {
				request.Header.Set("x-amz-checksum-algorithm", checksum)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var output struct {
				UploadID string `xml:"UploadId"`
			}
			if response.StatusCode != http.StatusOK || xml.NewDecoder(response.Body).Decode(&output) != nil || output.UploadID == "" {
				t.Fatalf("create multipart upload %q: %s", key, response.Status)
			}
			return output.UploadID
		}
		firstID := create("folder/a/one", "")
		create("folder/file1", "CRC64NVME")
		res = do(http.MethodGet, "/multipart-list-bdd?uploads&prefix=folder%2F&delimiter=%2F&max-uploads=1", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<CommonPrefixes><Prefix>folder/a/</Prefix></CommonPrefixes>")) || !bytes.Contains(body, []byte("<IsTruncated>true</IsTruncated>")) || !bytes.Contains(body, []byte("<NextKeyMarker></NextKeyMarker>")) || !bytes.Contains(body, []byte("<NextUploadIdMarker></NextUploadIdMarker>")) {
			t.Fatalf("first multipart page %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/multipart-list-bdd?uploads&prefix=folder%2Ffile1", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ChecksumAlgorithm>CRC64NVME</ChecksumAlgorithm>")) || !bytes.Contains(body, []byte("<ChecksumType>FULL_OBJECT</ChecksumType>")) || !bytes.Contains(body, []byte("<DisplayName>webfile</DisplayName>")) {
			t.Fatalf("multipart listing metadata %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/multipart-list-bdd?uploads&key-marker=wrong&upload-id-marker="+url.QueryEscape(firstID), nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidArgument</Code>")) || !bytes.Contains(body, []byte("<Message>Invalid uploadId marker</Message>")) || !bytes.Contains(body, []byte("<ArgumentName>upload-id-marker</ArgumentName>")) {
			t.Fatalf("mismatched multipart marker %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a checksum-free multipart upload When completed Then checksum metadata stays absent", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-plain-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/multipart-plain-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/multipart-plain-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("plain"), "")
		etag := res.Header.Get("ETag")
		res.Body.Close()
		res = do(http.MethodGet, "/multipart-plain-bdd/object?uploadId="+url.QueryEscape(created.UploadID), nil, "")
		listed, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if bytes.Contains(listed, []byte("ChecksumAlgorithm")) || bytes.Contains(listed, []byte("ChecksumType")) {
			t.Fatalf("list exposed checksum metadata: %s", listed)
		}
		manifest := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`)
		res = do(http.MethodPost, "/multipart-plain-bdd/object?uploadId="+url.QueryEscape(created.UploadID), manifest, "")
		completed, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if bytes.Contains(completed, []byte("ChecksumCRC64NVME")) || bytes.Contains(completed, []byte("ChecksumType")) {
			t.Fatalf("completion exposed checksum metadata: %s", completed)
		}
	})

	t.Run("Given a composite multipart upload When a part checksum is omitted Then completion is rejected", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-composite-bdd", nil, "")
		res.Body.Close()
		initiate, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-composite-bdd/object?uploads", nil)
		initiate.Header.Set("Authorization", auth)
		initiate.Header.Set("x-amz-checksum-algorithm", "CRC32")
		res, err := http.DefaultClient.Do(initiate)
		if err != nil {
			t.Fatal(err)
		}
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/multipart-composite-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("checked"), "")
		etag, partChecksum := res.Header.Get("ETag"), res.Header.Get("x-amz-checksum-crc32")
		res.Body.Close()
		manifest := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`)
		res = do(http.MethodPost, "/multipart-composite-bdd/object?uploadId="+url.QueryEscape(created.UploadID), manifest, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(fault, []byte("missing for part 1")) {
			t.Fatalf("missing composite checksum: %d %s", res.StatusCode, fault)
		}
		manifest = []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag><ChecksumCRC32>` + partChecksum + `</ChecksumCRC32></Part></CompleteMultipartUpload>`)
		complete, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-composite-bdd/object?uploadId="+url.QueryEscape(created.UploadID), bytes.NewReader(manifest))
		complete.Header.Set("Authorization", auth)
		complete.Header.Set("x-amz-checksum-crc32", "AA==")
		res, err = http.DefaultClient.Do(complete)
		if err != nil {
			t.Fatal(err)
		}
		completed, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(completed, []byte("<ChecksumCRC32>")) || bytes.Contains(completed, []byte("<ChecksumCRC32>AA==</ChecksumCRC32>")) {
			t.Fatalf("ignored composite aggregate: %d %s", res.StatusCode, completed)
		}
	})

	t.Run("Given a composite multipart upload When an alternate object checksum is supplied Then completion returns BadDigest", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-alternate-bdd", nil, "")
		res.Body.Close()
		initiate, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-alternate-bdd/object?uploads", nil)
		initiate.Header.Set("Authorization", auth)
		initiate.Header.Set("x-amz-checksum-algorithm", "SHA256")
		res, err := http.DefaultClient.Do(initiate)
		if err != nil {
			t.Fatal(err)
		}
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/multipart-alternate-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("checked"), "")
		manifest := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + res.Header.Get("ETag") + `</ETag><ChecksumSHA256>` + res.Header.Get("x-amz-checksum-sha256") + `</ChecksumSHA256></Part></CompleteMultipartUpload>`)
		res.Body.Close()
		complete, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-alternate-bdd/object?uploadId="+url.QueryEscape(created.UploadID), bytes.NewReader(manifest))
		complete.Header.Set("Authorization", auth)
		complete.Header.Set("x-amz-checksum-crc32", "AAAAAA==")
		res, err = http.DefaultClient.Do(complete)
		if err != nil {
			t.Fatal(err)
		}
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(fault, []byte("The sha256 you specified did not match the calculated checksum.")) {
			t.Fatalf("alternate object checksum: %d %s", res.StatusCode, fault)
		}
	})

	t.Run("Given multipart object sizes When completed Then zero is ignored and mismatches are described", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-size-bdd", nil, "")
		res.Body.Close()
		createPart := func(key string) (string, string) {
			response := do(http.MethodPost, "/multipart-size-bdd/"+key+"?uploads", nil, "")
			var created struct {
				UploadID string `xml:"UploadId"`
			}
			if err := xml.NewDecoder(response.Body).Decode(&created); err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			response = do(http.MethodPut, "/multipart-size-bdd/"+key+"?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("sized"), "")
			defer response.Body.Close()
			return created.UploadID, response.Header.Get("ETag")
		}
		complete := func(key, uploadID, etag, size string) (int, []byte) {
			manifest := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
			request, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-size-bdd/"+key+"?uploadId="+url.QueryEscape(uploadID), strings.NewReader(manifest))
			request.Header.Set("Authorization", auth)
			request.Header.Set("x-amz-mp-object-size", size)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response.StatusCode, payload
		}
		zeroID, zeroETag := createPart("zero")
		if status, body := complete("zero", zeroID, zeroETag, "0"); status != http.StatusOK {
			t.Fatalf("zero object size: %d %s", status, body)
		}
		mismatchID, mismatchETag := createPart("mismatch")
		if status, body := complete("mismatch", mismatchID, mismatchETag, "4"); status != http.StatusBadRequest || !bytes.Contains(body, []byte("header value 4 does not match what was computed: 5")) {
			t.Fatalf("mismatched object size: %d %s", status, body)
		}
	})

	t.Run("Given multipart parts When listing pages Then empty values use S3 defaults", func(t *testing.T) {
		res := do(http.MethodPut, "/parts-list-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/parts-list-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		list := func(query string) string {
			t.Helper()
			response := do(http.MethodGet, "/parts-list-bdd/object?uploadId="+url.QueryEscape(created.UploadID)+"&"+query, nil, "")
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				t.Fatalf("list parts %q: %d %s", query, response.StatusCode, body)
			}
			return string(body)
		}
		empty := list("part-number-marker=&max-parts=")
		if !strings.Contains(empty, "<PartNumberMarker>0</PartNumberMarker>") || !strings.Contains(empty, "<NextPartNumberMarker>0</NextPartNumberMarker>") || !strings.Contains(empty, "<MaxParts>1000</MaxParts>") || !strings.Contains(empty, "<Initiator>") || !strings.Contains(empty, "<DisplayName>webfile</DisplayName>") || !strings.Contains(empty, "<Owner><ID>000000000000</ID></Owner>") {
			t.Fatalf("empty parts page %s", empty)
		}
		res = do(http.MethodPut, "/parts-list-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("part"), "")
		res.Body.Close()
		page := list("max-parts=1")
		if !strings.Contains(page, "<NextPartNumberMarker>1</NextPartNumberMarker>") || !strings.Contains(page, "<IsTruncated>false</IsTruncated>") || !strings.Contains(page, ".000Z</LastModified>") {
			t.Fatalf("final parts page %s", page)
		}
		beyond := list("max-parts=1&part-number-marker=10")
		if !strings.Contains(beyond, "<PartNumberMarker>10</PartNumberMarker>") || !strings.Contains(beyond, "<NextPartNumberMarker>0</NextPartNumberMarker>") {
			t.Fatalf("beyond parts page %s", beyond)
		}
	})

	t.Run("Given multipart metadata When completing the upload Then S3 preserves initiation headers", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-metadata-bdd", nil, "")
		res.Body.Close()
		request, _ := http.NewRequest(http.MethodPost, ts.URL+"/multipart-metadata-bdd/object?uploads", nil)
		request.Header.Set("Authorization", auth)
		request.Header.Set("Cache-Control", "max-age=60")
		request.Header.Set("Content-Type", "text/plain")
		request.Header.Set("x-amz-meta-team", "storage")
		request.Header.Set("x-amz-website-redirect-location", "/multipart")
		res, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/multipart-metadata-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("part"), "")
		etag := res.Header.Get("ETag")
		res.Body.Close()
		manifest := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`)
		res = do(http.MethodPost, "/multipart-metadata-bdd/object?uploadId="+url.QueryEscape(created.UploadID), manifest, "")
		res.Body.Close()
		res = do(http.MethodHead, "/multipart-metadata-bdd/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Cache-Control") != "max-age=60" || res.Header.Get("Content-Type") != "text/plain" || res.Header.Get("x-amz-meta-team") != "storage" || res.Header.Get("x-amz-website-redirect-location") != "/multipart" {
			t.Fatalf("multipart metadata %d headers=%v", res.StatusCode, res.Header)
		}
	})

	t.Run("Given a missing multipart upload When accessed Then S3 returns the modeled fault", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-fault-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodGet, "/multipart-fault-bdd/object?uploadId=missing", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<Code>NoSuchUpload</Code>")) || !bytes.Contains(body, []byte("<Message>The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.</Message>")) || !bytes.Contains(body, []byte("<UploadId>missing</UploadId>")) {
			t.Fatalf("missing multipart upload %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an invalid multipart part number When uploaded Then S3 returns the modeled fault", func(t *testing.T) {
		res := do(http.MethodPut, "/part-number-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/part-number-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/part-number-bdd/object?partNumber=10001&uploadId="+url.QueryEscape(created.UploadID), []byte("part"), "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidArgument</Code>")) || !bytes.Contains(body, []byte("<Message>Part number must be an integer between 1 and 10000, inclusive</Message>")) || !bytes.Contains(body, []byte("<ArgumentName>partNumber</ArgumentName>")) || !bytes.Contains(body, []byte("<ArgumentValue>10001</ArgumentValue>")) {
			t.Fatalf("invalid multipart part number %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/part-number-bdd/object?partNumber=0&uploadId=missing", []byte("part"), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<Code>NoSuchUpload</Code>")) {
			t.Fatalf("missing upload precedence %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an invalid upload part Content-MD5 When uploaded Then S3 returns modeled faults", func(t *testing.T) {
		res := do(http.MethodPut, "/upload-part-md5-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/upload-part-md5-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		upload := func(digest string) (int, []byte) {
			t.Helper()
			request, err := http.NewRequest(http.MethodPut, ts.URL+"/upload-part-md5-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), strings.NewReader("part"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-MD5", digest)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response.StatusCode, body
		}
		status, body := upload("!")
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidDigest</Code>")) || !bytes.Contains(body, []byte("<Message>The Content-MD5 you specified was invalid.</Message>")) || !bytes.Contains(body, []byte("<Content_MD5>!</Content_MD5>")) {
			t.Fatalf("malformed upload part Content-MD5 %d %s", status, body)
		}
		status, body = upload("AAAAAAAAAAAAAAAAAAAAAA==")
		sum := md5.Sum([]byte("part"))
		calculated := base64.StdEncoding.EncodeToString(sum[:])
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(body, []byte("<Message>The Content-MD5 you specified did not match what we received.</Message>")) || !bytes.Contains(body, []byte("<ExpectedDigest>AAAAAAAAAAAAAAAAAAAAAA==</ExpectedDigest>")) || !bytes.Contains(body, []byte("<CalculatedDigest>"+calculated+"</CalculatedDigest>")) {
			t.Fatalf("mismatched upload part Content-MD5 %d %s", status, body)
		}
	})

	t.Run("Given an invalid upload part checksum When uploaded Then S3 returns modeled faults", func(t *testing.T) {
		res := do(http.MethodPut, "/upload-part-checksum-bdd", nil, "")
		res.Body.Close()
		initiate, _ := http.NewRequest(http.MethodPost, ts.URL+"/upload-part-checksum-bdd/object?uploads", nil)
		initiate.Header.Set("Authorization", auth)
		initiate.Header.Set("x-amz-checksum-algorithm", "CRC64NVME")
		res, err := http.DefaultClient.Do(initiate)
		if err != nil {
			t.Fatal(err)
		}
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		upload := func(header, checksum string) (int, []byte) {
			t.Helper()
			request, err := http.NewRequest(http.MethodPut, ts.URL+"/upload-part-checksum-bdd/object?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), strings.NewReader("part"))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set(header, checksum)
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response.StatusCode, body
		}
		status, body := upload("x-amz-checksum-crc64nvme", "!")
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(body, []byte("<Message>Value for x-amz-checksum-crc64nvme header is invalid.</Message>")) {
			t.Fatalf("malformed upload part checksum %d %s", status, body)
		}
		status, body = upload("x-amz-checksum-crc64nvme", "AAAAAAAAAAA=")
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(body, []byte("<Message>The CRC64NVME you specified did not match the calculated checksum.</Message>")) {
			t.Fatalf("mismatched upload part checksum %d %s", status, body)
		}
		status, body = upload("x-amz-checksum-sha256", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		if status != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(body, []byte("<Message>Checksum Type mismatch occurred, expected checksum Type: crc64nvme, actual checksum Type: sha256</Message>")) {
			t.Fatalf("mismatched upload part checksum algorithm %d %s", status, body)
		}
	})

	t.Run("Given an invalid multipart completion When submitted Then S3 returns modeled faults", func(t *testing.T) {
		res := do(http.MethodPut, "/completion-fault-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/completion-fault-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		path := "/completion-fault-bdd/object?uploadId=" + url.QueryEscape(created.UploadID)
		res = do(http.MethodPost, path, []byte("<CompleteMultipartUpload/>"), "application/xml")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(body, []byte("<Message>You must specify at least one part</Message>")) {
			t.Fatalf("empty multipart completion %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPost, path, []byte(`<CompleteMultipartUpload><Part><PartNumber>9</PartNumber><ETag>"missing"</ETag></Part></CompleteMultipartUpload>`), "application/xml")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidPart</Code>")) || !bytes.Contains(body, []byte("<Message>One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.</Message>")) || !bytes.Contains(body, []byte("<ETag>missing</ETag>")) || !bytes.Contains(body, []byte("<PartNumber>9</PartNumber>")) || !bytes.Contains(body, []byte("<UploadId>"+created.UploadID+"</UploadId>")) {
			t.Fatalf("missing multipart completion part %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a mismatched multipart checksum type When completed Then S3 returns the selected mode", func(t *testing.T) {
		res := do(http.MethodPut, "/completion-checksum-type-bdd", nil, "")
		res.Body.Close()
		initiate, _ := http.NewRequest(http.MethodPost, ts.URL+"/completion-checksum-type-bdd/object?uploads", nil)
		initiate.Header.Set("Authorization", auth)
		initiate.Header.Set("x-amz-checksum-algorithm", "CRC64NVME")
		res, err := http.DefaultClient.Do(initiate)
		if err != nil {
			t.Fatal(err)
		}
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		partPath := "/completion-checksum-type-bdd/object?partNumber=1&uploadId=" + url.QueryEscape(created.UploadID)
		res = do(http.MethodPut, partPath, []byte("part"), "application/octet-stream")
		etag, checksum := res.Header.Get("ETag"), res.Header.Get("x-amz-checksum-crc64nvme")
		res.Body.Close()
		manifest := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
		request, _ := http.NewRequest(http.MethodPost, ts.URL+"/completion-checksum-type-bdd/object?uploadId="+url.QueryEscape(created.UploadID), strings.NewReader(manifest))
		request.Header.Set("x-amz-checksum-type", "COMPOSITE")
		res, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) || !bytes.Contains(body, []byte("<Message>The upload was created using the FULL_OBJECT checksum mode. The complete request must use the same checksum mode.</Message>")) {
			t.Fatalf("mismatched multipart checksum type %d %s", res.StatusCode, body)
		}
		request, _ = http.NewRequest(http.MethodPost, ts.URL+"/completion-checksum-type-bdd/object?uploadId="+url.QueryEscape(created.UploadID), strings.NewReader(manifest))
		request.Header.Set("x-amz-checksum-crc64nvme", checksum)
		res, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>BadDigest</Code>")) || !bytes.Contains(body, []byte("The crc64nvme you specified did not match the calculated checksum.")) {
			t.Fatalf("implicit full-object checksum type %d %s", res.StatusCode, body)
		}
		request, _ = http.NewRequest(http.MethodPost, ts.URL+"/completion-checksum-type-bdd/object?uploadId="+url.QueryEscape(created.UploadID), strings.NewReader(manifest))
		request.Header.Set("x-amz-checksum-crc64nvme", checksum)
		request.Header.Set("x-amz-checksum-type", "FULL_OBJECT")
		res, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ChecksumType>FULL_OBJECT</ChecksumType>")) {
			t.Fatalf("explicit full-object checksum type %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given unsupported multipart completion conditions When completed Then S3 returns LocalStack faults", func(t *testing.T) {
		res := do(http.MethodPut, "/completion-precondition-bdd", nil, "")
		res.Body.Close()
		res = do(http.MethodPost, "/completion-precondition-bdd/object?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		path := "/completion-precondition-bdd/object?partNumber=1&uploadId=" + url.QueryEscape(created.UploadID)
		res = do(http.MethodPut, path, []byte("part"), "application/octet-stream")
		etag := res.Header.Get("ETag")
		res.Body.Close()
		manifest := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
		for name, conditions := range map[string]struct{ match, noneMatch, header, detail string }{
			"combined":      {`"etag"`, "*", "If-Match,If-None-Match", "Multiple conditional request headers present in the request"},
			"if-none-match": {"", `"etag"`, "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
			"if-match-star": {"*", "", "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"},
		} {
			request, err := http.NewRequest(http.MethodPost, ts.URL+"/completion-precondition-bdd/object?uploadId="+url.QueryEscape(created.UploadID), strings.NewReader(manifest))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", auth)
			request.Header.Set("Content-Type", "application/xml")
			if conditions.match != "" {
				request.Header.Set("If-Match", conditions.match)
			}
			if conditions.noneMatch != "" {
				request.Header.Set("If-None-Match", conditions.noneMatch)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusNotImplemented || !bytes.Contains(body, []byte("<Code>NotImplemented</Code>")) || !bytes.Contains(body, []byte("<Message>A header you provided implies functionality that is not implemented</Message>")) || !bytes.Contains(body, []byte("<Header>"+conditions.header+"</Header>")) || !bytes.Contains(body, []byte("<additionalMessage>"+conditions.detail+"</additionalMessage>")) {
				t.Fatalf("%s multipart precondition fault %d %s", name, response.StatusCode, body)
			}
		}
	})

	t.Run("Given conflicting multipart completion conditions When completed Then S3 returns LocalStack faults", func(t *testing.T) {
		res := do(http.MethodPut, "/completion-conditional-bdd", nil, "")
		res.Body.Close()
		start := func(key string) (string, string) {
			t.Helper()
			response := do(http.MethodPost, "/completion-conditional-bdd/"+key+"?uploads", nil, "")
			var created struct {
				UploadID string `xml:"UploadId"`
			}
			if err := xml.NewDecoder(response.Body).Decode(&created); err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			response = do(http.MethodPut, "/completion-conditional-bdd/"+key+"?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("part"), "application/octet-stream")
			etag := response.Header.Get("ETag")
			response.Body.Close()
			return created.UploadID, `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`
		}
		complete := func(key, uploadID, manifest, match, noneMatch string) (int, []byte) {
			t.Helper()
			request, err := http.NewRequest(http.MethodPost, ts.URL+"/completion-conditional-bdd/"+key+"?uploadId="+url.QueryEscape(uploadID), strings.NewReader(manifest))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", auth)
			request.Header.Set("Content-Type", "application/xml")
			if match != "" {
				request.Header.Set("If-Match", match)
			}
			if noneMatch != "" {
				request.Header.Set("If-None-Match", noneMatch)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response.StatusCode, body
		}

		uploadID, manifest := start("missing")
		status, body := complete("missing", uploadID, manifest, `"missing"`, "")
		if status != http.StatusNotFound || !bytes.Contains(body, []byte("<Code>NoSuchKey</Code>")) || !bytes.Contains(body, []byte("<Message>The specified key does not exist.</Message>")) || !bytes.Contains(body, []byte("<Key>missing</Key>")) {
			t.Fatalf("missing complete If-Match %d %s", status, body)
		}
		res = do(http.MethodPut, "/completion-conditional-bdd/mismatch", []byte("old"), "")
		res.Body.Close()
		uploadID, manifest = start("mismatch")
		status, body = complete("mismatch", uploadID, manifest, `"wrong"`, "")
		if status != http.StatusPreconditionFailed || !bytes.Contains(body, []byte("<Code>PreconditionFailed</Code>")) || !bytes.Contains(body, []byte("<Message>At least one of the pre-conditions you specified did not hold</Message>")) || !bytes.Contains(body, []byte("<Condition>If-Match</Condition>")) {
			t.Fatalf("mismatched complete If-Match %d %s", status, body)
		}
		uploadID, manifest = start("created")
		res = do(http.MethodPut, "/completion-conditional-bdd/created", []byte("object"), "")
		res.Body.Close()
		status, body = complete("created", uploadID, manifest, "", "*")
		if status != http.StatusPreconditionFailed || !bytes.Contains(body, []byte("<Code>PreconditionFailed</Code>")) || !bytes.Contains(body, []byte("<Condition>If-None-Match</Condition>")) {
			t.Fatalf("created complete If-None-Match %d %s", status, body)
		}
		res = do(http.MethodPut, "/completion-conditional-bdd/deleted", []byte("object"), "")
		res.Body.Close()
		uploadID, manifest = start("deleted")
		res = do(http.MethodDelete, "/completion-conditional-bdd/deleted", nil, "")
		res.Body.Close()
		status, body = complete("deleted", uploadID, manifest, "", "*")
		if status != http.StatusConflict || !bytes.Contains(body, []byte("<Code>ConditionalRequestConflict</Code>")) || !bytes.Contains(body, []byte("<Message>The conditional request cannot succeed due to a conflicting operation against this resource.</Message>")) || !bytes.Contains(body, []byte("<Condition>If-None-Match</Condition>")) || !bytes.Contains(body, []byte("<Key>deleted</Key>")) {
			t.Fatalf("deleted complete If-None-Match %d %s", status, body)
		}
	})

	t.Run("Given an expired presigned URL When requested Then S3 returns a modeled access denial", func(t *testing.T) {
		request, err := http.NewRequest(http.MethodGet, ts.URL+"/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F19691231%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=19691231T235900Z&X-Amz-Expires=30&X-Amz-SignedHeaders=host&X-Amz-Signature=00", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || response.Header.Get("Content-Type") != "application/xml" || response.Header.Get("x-amz-request-id") == "" || response.Header.Get("x-amz-id-2") == "" || !bytes.Contains(body, []byte("<Code>AccessDenied</Code>")) || !bytes.Contains(body, []byte("<Message>Request has expired</Message>")) || !bytes.Contains(body, []byte("<X-Amz-Expires>30</X-Amz-Expires>")) {
			t.Fatalf("expired presign %d %#v %s", response.StatusCode, response.Header, body)
		}
	})

	t.Run("Given incomplete presigned authentication When requested Then S3 rejects the query", func(t *testing.T) {
		for name, target := range map[string]string{
			"sigv2":  "/bucket/key?AWSAccessKeyId=test&Signature=00",
			"sigv4":  "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test&X-Amz-Signature=00&X-Amz-Expires=60&X-Amz-SignedHeaders=host",
			"sigv4a": "/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
		} {
			response, err := http.Get(ts.URL + target)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if response.Header.Get("Content-Type") != "application/xml" || response.Header.Get("x-amz-request-id") == "" || response.Header.Get("x-amz-id-2") == "" || !bytes.Contains(body, []byte("Query-string authentication")) {
				t.Fatalf("%s incomplete presign %d %#v %s", name, response.StatusCode, response.Header, body)
			}
		}
	})

	t.Run("Given malformed aws-chunked framing When uploaded Then S3 rejects the request", func(t *testing.T) {
		response := do(http.MethodPut, "/chunk-errors-bdd", nil, "")
		response.Body.Close()
		raw := "5\r\nhello\r\n0\r\n\r\n"
		request, err := http.NewRequest(http.MethodPut, ts.URL+"/chunk-errors-bdd/object", strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Encoding", "aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("malformed stream: %d %s", response.StatusCode, body)
		}
	})

	t.Run("Given a cancelled chunked part When retried Then S3 stores only the valid part", func(t *testing.T) {
		response := do(http.MethodPut, "/chunk-part-bdd", nil, "")
		response.Body.Close()
		response = do(http.MethodPost, "/chunk-part-bdd/object?uploads", nil, "")
		var upload struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(response.Body).Decode(&upload); err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		path := "/chunk-part-bdd/object?partNumber=1&uploadId=" + url.QueryEscape(upload.UploadID)
		put := func(raw string) *http.Response {
			request, _ := http.NewRequest(http.MethodPut, ts.URL+path, strings.NewReader(raw))
			request.Header.Set("Content-Encoding", "aws-chunked")
			request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
			request.Header.Set("X-Amz-Decoded-Content-Length", "10")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		response = put("\r\nHello Blob\r\n0;chunk-signature=invalid\r\n")
		response.Body.Close()
		if response.StatusCode != http.StatusInternalServerError {
			t.Fatalf("cancelled part: %s", response.Status)
		}
		response = put("a;chunk-signature=first\r\nHello Blob\r\n0;chunk-signature=last\r\n")
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("valid retry: %s", response.Status)
		}
	})

	t.Run("Given aws-chunked transport encoding When stored Then S3 preserves only content encodings", func(t *testing.T) {
		response := do(http.MethodPut, "/chunk-encoding-bdd", nil, "")
		response.Body.Close()
		request, err := http.NewRequest(http.MethodPut, ts.URL+"/chunk-encoding-bdd/object", strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Encoding", "gzip, aws-chunked")
		request.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
		request.Header.Set("X-Amz-Decoded-Content-Length", "5")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		request, _ = http.NewRequest(http.MethodGet, ts.URL+"/chunk-encoding-bdd/object", nil)
		request.Header.Set("Accept-Encoding", "identity")
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != "hello" || response.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("stored encoding: %d %q %q", response.StatusCode, response.Header.Get("Content-Encoding"), body)
		}
	})

	t.Run("Given an object When fetched Then S3 emits exact ETag header casing", func(t *testing.T) {
		response := do(http.MethodPut, "/etag-casing-bdd", nil, "")
		response.Body.Close()
		response = do(http.MethodPut, "/etag-casing-bdd/object", []byte("body"), "")
		response.Body.Close()
		conn, err := net.Dial("tcp", ts.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "GET /etag-casing-bdd/object HTTP/1.1\r\nHost: "+ts.Listener.Addr().String()+"\r\nConnection: close\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(raw, []byte("\r\nETag: \"")) || bytes.Contains(raw, []byte("\r\nEtag:")) {
			t.Fatalf("raw response:\n%s", raw)
		}
	})

	t.Run("Given signature validation is enabled When a signature is tampered Then S3 rejects it", func(t *testing.T) {
		target := "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64)
		response, err := http.Get(ts.URL + target)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatal("default server unexpectedly validated the signature")
		}
		strict := cfg
		strict.S3ValidatePresignedSignatures = true
		strictDeps := spitest.Deps(t)
		if err := strictDeps.Clock.Advance(time.Date(2098, 12, 31, 23, 59, 30, 0, time.UTC).Sub(strictDeps.Clock.Now())); err != nil {
			t.Fatal(err)
		}
		strictReg, err := registry.New(strictDeps, strict.Services, nil)
		if err != nil {
			t.Fatal(err)
		}
		strictServer := httptest.NewServer(edge.New(strict, strictDeps, strictReg, "test").Handler())
		defer strictServer.Close()
		unsignedTarget := "/signed/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-SignedHeaders=host&X-Amz-Signature=8752c43939826ec5e949abb74845c6ac5a92ea98f5114bbdc6db1e78fe2b7e5e"
		unsignedRequest, _ := http.NewRequest(http.MethodGet, strictServer.URL+unsignedTarget, nil)
		unsignedRequest.Host = "s3.localhost.localstack.cloud"
		unsignedRequest.Header.Set("X-Amz-User-Agent", "test")
		response, err = http.DefaultClient.Do(unsignedRequest)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("unsigned x-amz header %d %s", response.StatusCode, body)
		}
		malformedRequest, _ := http.NewRequest(http.MethodGet, strictServer.URL+strings.ReplaceAll(unsignedTarget, "%2F", "%252F"), nil)
		malformedRequest.Host = "s3.localhost.localstack.cloud"
		response, err = http.DefaultClient.Do(malformedRequest)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>AuthorizationQueryParametersError</Code>")) || !bytes.Contains(body, []byte("Credential is mal-formed")) {
			t.Fatalf("malformed credential %d %s", response.StatusCode, body)
		}
		for name, strictTarget := range map[string]string{
			"sigv2":  "/bucket/key?AWSAccessKeyId=test&Expires=4070908800&Signature=AAAAAAAAAAAAAAAAAAAAAAAAAAA%3D",
			"sigv4":  target,
			"sigv4a": "/bucket/key?X-Amz-Algorithm=AWS4-ECDSA-P256-SHA256&X-Amz-Credential=test%2F20990101%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Region-Set=us-east-1&X-Amz-SignedHeaders=host&X-Amz-Signature=00",
		} {
			response, err = http.Get(strictServer.URL + strictTarget)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
				t.Fatalf("%s tampered presign %d %s", name, response.StatusCode, body)
			}
		}
		createPostBucket, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/post-signature", nil)
		response, err = http.DefaultClient.Do(createPostBucket)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("create POST signature bucket %d", response.StatusCode)
		}
		var postPayload bytes.Buffer
		postWriter := multipart.NewWriter(&postPayload)
		_ = postWriter.WriteField("key", "browser-upload")
		_ = postWriter.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T01:00:00Z","conditions":[{"bucket":"post-signature"}]}`)))
		_ = postWriter.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		_ = postWriter.WriteField("x-amz-credential", "test/20990101/us-east-1/s3/aws4_request")
		_ = postWriter.WriteField("x-amz-date", "20990101T000000Z")
		_ = postWriter.WriteField("x-amz-signature", strings.Repeat("0", 64))
		postFile, _ := postWriter.CreateFormFile("file", "file.txt")
		_, _ = postFile.Write([]byte("body"))
		_ = postWriter.Close()
		postRequest, _ := http.NewRequest(http.MethodPost, strictServer.URL+"/post-signature", &postPayload)
		postRequest.Header.Set("Content-Type", postWriter.FormDataContentType())
		response, err = http.DefaultClient.Do(postRequest)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered browser POST policy %d %s", response.StatusCode, body)
		}
		authorization, err := http.NewRequest(http.MethodGet, strictServer.URL+"/bucket/key", nil)
		if err != nil {
			t.Fatal(err)
		}
		authorization.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		authorization.Header.Set("X-Amz-Date", "20990101T000000Z")
		authorization.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature="+strings.Repeat("0", 64))
		response, err = http.DefaultClient.Do(authorization)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered authorization %d %s", response.StatusCode, body)
		}
		authorization, err = http.NewRequest(http.MethodGet, strictServer.URL+"/bucket/key", nil)
		if err != nil {
			t.Fatal(err)
		}
		authorization.Header.Set("Date", "Tue, 27 Mar 2007 19:36:42 +0000")
		authorization.Header.Set("Authorization", "AWS test:AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
		response, err = http.DefaultClient.Do(authorization)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || !bytes.Contains(body, []byte("<Code>SignatureDoesNotMatch</Code>")) {
			t.Fatalf("tampered SigV2 authorization %d %s", response.StatusCode, body)
		}
		createStreaming, err := http.NewRequest(http.MethodPut, strictServer.URL+"/streaming", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createStreaming)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			payload string
			status  int
		}{"valid stream": {"hello", http.StatusOK}, "tampered stream": {"jello", http.StatusForbidden}} {
			raw := "5;chunk-signature=87081aa8d08ebfccd3aa73e18ac88541cf2050c23a5a49a9e46d94a70d84f2a4\r\n" + tc.payload + "\r\n0;chunk-signature=eaf2700e23d624c531f0f9a0c7312b66470ab3aee81742bfa00dfc9cf6ca0f4e\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/streaming/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length,Signature=d32bab45d70b05d89ada2e57acc27c4117cf31f7ce3de470cf916b8f89558054")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createV4A, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/v4a", nil)
		response, err = http.DefaultClient.Do(createV4A)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			payload string
			status  int
		}{"valid SigV4A stream": {"hello", http.StatusOK}, "tampered SigV4A stream": {"jello", http.StatusForbidden}} {
			raw := "5;chunk-signature=**304502201ba0be85f07d901a715f28fbcd6d4ee4d14ab70abe11f5cfaff93a3c1961e4ae022100f5693b9c34d100107df15bd06cbc5c1a608d467761f97f26e048c240b21cc256\r\n" + tc.payload + "\r\n0;chunk-signature=**304502202bed57aec7b9b53cfebdf5163fbc5c61009c0f0b1e1b50848ac50641c6d0d14a022100806a00edfb80226cf9f2761851cd38cb9f33ee3fdafb597c723086655aad5cb9\r\n\r\n"
			stream, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/v4a/object", strings.NewReader(raw))
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Region-Set", "us-east-1")
			stream.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set,Signature=30450220292f2afead2f51323260a06fdfed3d88e0998b54f024a175f65e19bdbf970425022100e28adec0e230329184badd9bf335b18c8ad5373000bad0c47223b173ecd16d11")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createV4AUnsigned, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/v4a-unsigned", nil)
		response, err = http.DefaultClient.Do(createV4AUnsigned)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			checksum string
			status   int
		}{"valid SigV4A unsigned trailer": {"mnG7TA==", http.StatusOK}, "bad SigV4A unsigned checksum": {"AAAAAA==", http.StatusBadRequest}} {
			raw := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + tc.checksum + "\r\n\r\n"
			stream, _ := http.NewRequest(http.MethodPut, strictServer.URL+"/v4a-unsigned/object", strings.NewReader(raw))
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Region-Set", "us-east-1")
			stream.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			stream.Header.Set("Authorization", "AWS4-ECDSA-P256-SHA256 Credential=test/20990101/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-region-set;x-amz-trailer,Signature=304402201f09d982734f868ab87f6e305473f7ef74a6882095dbf5d0f0b97bede169993402204a4c59017095e2ffaf861e04fc6c73b5d1c9b0d8c041b7fd2acb05d0a4c356f3")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createTrailers, err := http.NewRequest(http.MethodPut, strictServer.URL+"/trailers", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createTrailers)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			checksum, signature string
			status              int
		}{
			"valid trailer":    {"mnG7TA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa", http.StatusOK},
			"tampered trailer": {"AAAAAA==", "67f7b779024ca973ddf6705b8ad24ecfc6f79f5242ff1d050fd8f830ae2071aa", http.StatusForbidden},
			"bad checksum":     {"AAAAAA==", "0ef78e944cd5e61df18db7d6b99929d6871152b34d021274be44c9b5b113eeda", http.StatusBadRequest},
		} {
			raw := "5;chunk-signature=c83b0404927860c2dfacb114cd53dfe5505c5b4ad4dc605cc4e53806d4bb0d74\r\nhello\r\n0;chunk-signature=ffc89ae66d2e00900ad958aa09d8ea91ab7e1cb1938d6f4a5a30821f8fbe297f\r\nx-amz-checksum-crc32c:" + tc.checksum + "\r\nx-amz-trailer-signature:" + tc.signature + "\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/trailers/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=378380e9501dea596cd83a9661c42fc2603dbd37872ab598316173a4d9244821")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		createUnsigned, err := http.NewRequest(http.MethodPut, strictServer.URL+"/unsigned", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err = http.DefaultClient.Do(createUnsigned)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		for name, tc := range map[string]struct {
			checksum string
			status   int
		}{"valid unsigned trailer": {"mnG7TA==", http.StatusOK}, "bad unsigned checksum": {"AAAAAA==", http.StatusBadRequest}} {
			raw := "5\r\nhello\r\n0\r\nx-amz-checksum-crc32c:" + tc.checksum + "\r\n\r\n"
			stream, err := http.NewRequest(http.MethodPut, strictServer.URL+"/unsigned/object", strings.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			stream.Host = "s3.localhost.localstack.cloud:4566"
			stream.Header.Set("Content-Encoding", "aws-chunked")
			stream.Header.Set("X-Amz-Content-Sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
			stream.Header.Set("X-Amz-Date", "20990101T000000Z")
			stream.Header.Set("X-Amz-Decoded-Content-Length", "5")
			stream.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")
			stream.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20990101/us-east-1/s3/aws4_request,SignedHeaders=content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer,Signature=fcefc9ae2b8230495738dd184bf82843d23e54dc536efdf1dcdd0acb7fe9277a")
			response, err = http.DefaultClient.Do(stream)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if response.StatusCode != tc.status {
				t.Fatalf("%s: %d %s", name, response.StatusCode, body)
			}
		}
		if err := strictDeps.Store.Scope("_mirror", "global").Collection("stsk").Put(context.Background(), "temporary", []byte("000000000000")); err != nil {
			t.Fatal(err)
		}
		tokenTarget := "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=temporary%2F20990101%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20990101T000000Z&X-Amz-Expires=60&X-Amz-Security-Token=wrong&X-Amz-SignedHeaders=host&X-Amz-Signature=" + strings.Repeat("0", 64)
		response, err = http.Get(strictServer.URL + tokenTarget)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidToken</Code>")) {
			t.Fatalf("invalid session token %d %s", response.StatusCode, body)
		}
	})

	t.Run("Given explicit KMS keys When writing and reading Then S3 validates their regional state", func(t *testing.T) {
		res := do(http.MethodPut, "/kms-validation-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		request := func(method, path, keyID string) (*http.Response, []byte) {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, strings.NewReader("body"))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			if method == http.MethodPut {
				req.Header.Set("x-amz-server-side-encryption", "aws:kms")
				req.Header.Set("x-amz-server-side-encryption-aws-kms-key-id", keyID)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		enabledARN := "arn:aws:kms:us-east-1:000000000000:key/kms-bdd"
		if response, body := request(http.MethodPut, "/kms-validation-bdd/enabled", enabledARN); response.StatusCode != http.StatusOK {
			t.Fatalf("enabled key %d %s", response.StatusCode, body)
		}
		if response, body := request(http.MethodPut, "/kms-validation-bdd/missing", "arn:aws:kms:us-east-1:000000000000:key/missing"); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.NotFoundException</Code>")) {
			t.Fatalf("missing key %d %s", response.StatusCode, body)
		}
		if response, body := request(http.MethodPut, "/kms-validation-bdd/disabled", "arn:aws:kms:us-east-1:000000000000:key/disabled-bdd"); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.DisabledException</Code>")) {
			t.Fatalf("disabled key %d %s", response.StatusCode, body)
		}
		spitest.SeedKMSKey(t, deps, testIdentity, enabledARN, "Disabled")
		if response, body := request(http.MethodGet, "/kms-validation-bdd/enabled", ""); response.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>KMS.DisabledException</Code>")) {
			t.Fatalf("disabled read %d %s", response.StatusCode, body)
		}
	})

	t.Run("Given signed GetObject response overrides When reading Then S3 returns the requested headers", func(t *testing.T) {
		res := do(http.MethodPut, "/response-overrides-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/response-overrides-bdd/object", []byte("body"), "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object %d", res.StatusCode)
		}
		query := url.Values{
			"response-cache-control":       {"max-age=74"},
			"response-content-disposition": {`attachment; filename="foo.jpg"`},
			"response-content-encoding":    {"identity"},
			"response-content-language":    {"de-DE"},
			"response-content-type":        {"image/jpeg"},
			"response-expires":             {"Wed, 21 Oct 2015 07:28:00 GMT"},
		}
		res = do(http.MethodGet, "/response-overrides-bdd/object?"+query.Encode(), nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "body" || res.Header.Get("Cache-Control") != "max-age=74" || res.Header.Get("Content-Disposition") != `attachment; filename="foo.jpg"` || res.Header.Get("Content-Encoding") != "identity" || res.Header.Get("Content-Language") != "de-DE" || res.Header.Get("Content-Type") != "image/jpeg" || res.Header.Get("Expires") != "Wed, 21 Oct 2015 07:28:00 GMT" {
			t.Fatalf("response overrides %d %v %q", res.StatusCode, res.Header, body)
		}
	})

	t.Run("Given RFC 2047 user metadata When reading Then S3 returns mail-safe decoded values", func(t *testing.T) {
		res := do(http.MethodPut, "/rfc2047-bdd", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		req, err := http.NewRequest(http.MethodPut, ts.URL+"/rfc2047-bdd/object", strings.NewReader("body"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("x-amz-meta-non-ascii", "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=")
		req.Header.Set("x-amz-meta-fake-encoded", "=?UTF-8?Q?actually-ascii?=")
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/rfc2047-bdd/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-meta-non-ascii") != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || res.Header.Get("x-amz-meta-fake-encoded") != "actually-ascii" {
			t.Fatalf("head metadata %d %v", res.StatusCode, res.Header)
		}

		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "post")
		_ = writer.WriteField("x-amz-meta-non-ascii", "ÄMÄZÕÑ S3")
		_ = writer.WriteField("x-amz-meta-q-encoded", "=?UTF-8?Q?actually-ascii?=")
		file, _ := writer.CreateFormFile("file", "post.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		req, err = http.NewRequest(http.MethodPost, ts.URL+"/rfc2047-bdd", &payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNoContent {
			t.Fatalf("post object %d", res.StatusCode)
		}
		res = do(http.MethodHead, "/rfc2047-bdd/post", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-meta-non-ascii") != "=?UTF-8?Q?=C3=84M=C3=84Z=C3=95=C3=91_S3?=" || res.Header.Get("x-amz-meta-q-encoded") != "=?UTF-8?Q?actually-ascii?=" {
			t.Fatalf("post metadata %d %v", res.StatusCode, res.Header)
		}
	})

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

	t.Run("Given a browser form When POSTing a file Then S3 stores it and returns the requested response", func(t *testing.T) {
		res := do(http.MethodPut, "/post-form", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "forms/${filename}")
		_ = writer.WriteField("success_action_status", "201")
		_ = writer.WriteField("tagging", "<Tagging><TagSet><Tag><Key>source</Key><Value>browser</Value></Tag></TagSet></Tagging>")
		_ = writer.WriteField("Expires", "Thu, 27 Aug 2026 12:00:00 GMT")
		_ = writer.WriteField("x-amz-checksum-algorithm", "SHA256")
		_ = writer.WriteField("x-amz-server-side-encryption", "aws:kms")
		_ = writer.WriteField("x-amz-server-side-encryption-aws-kms-key-id", "arn:aws:kms:us-east-1:000000000000:key/browser")
		_ = writer.WriteField("x-amz-server-side-encryption-bucket-key-enabled", "true")
		file, err := writer.CreateFormFile("file", "report.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.Write([]byte("from browser"))
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/post-form", &payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		response, _ := io.ReadAll(res.Body)
		res.Body.Close()
		checksum := sha256.Sum256([]byte("from browser"))
		if res.StatusCode != http.StatusCreated || res.Header.Get("ETag") == "" || res.Header.Get("x-amz-checksum-sha256") != base64.StdEncoding.EncodeToString(checksum[:]) || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/browser" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" || !bytes.Contains(response, []byte("<PostResponse>")) || !bytes.Contains(response, []byte("<Key>forms/report.txt</Key>")) {
			t.Fatalf("post form %d headers=%v body=%s", res.StatusCode, res.Header, response)
		}
		spitest.SeedKMSKey(t, deps, testIdentity, "arn:aws:kms:us-east-1:000000000000:key/browser", "Enabled")
		res = do(http.MethodGet, "/post-form/forms/report.txt", nil, "")
		stored, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(stored) != "from browser" || res.Header.Get("Expires") != "Thu, 27 Aug 2026 12:00:00 GMT" || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/browser" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
			t.Fatalf("stored form object %d %q", res.StatusCode, stored)
		}
		res = do(http.MethodGet, "/post-form/forms/report.txt?tagging", nil, "")
		tags, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(tags, []byte("<Key>source</Key>")) || !bytes.Contains(tags, []byte("<Value>browser</Value>")) {
			t.Fatalf("stored form tags %d %s", res.StatusCode, tags)
		}
	})

	t.Run("Given a customer encryption key When PUTting and GETting an object Then matching headers are required", func(t *testing.T) {
		res := do(http.MethodPut, "/customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		headers := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       base64.StdEncoding.EncodeToString(rawKey),
			"x-amz-server-side-encryption-customer-key-MD5":   base64.StdEncoding.EncodeToString(digest[:]),
		}
		request := func(method string, includeKey bool) (*http.Response, []byte) {
			t.Helper()
			payload := ""
			if method == http.MethodPut {
				payload = "secret"
			}
			req, _ := http.NewRequest(method, ts.URL+"/customer-encryption/object", strings.NewReader(payload))
			req.Header.Set("Authorization", auth)
			if includeKey {
				for key, value := range headers {
					req.Header.Set(key, value)
				}
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		res, _ = request(http.MethodPut, true)
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" || res.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] || res.Header.Get("x-amz-server-side-encryption") != "" {
			t.Fatalf("put customer encryption %d %v", res.StatusCode, res.Header)
		}
		if res, _ = request(http.MethodGet, false); res.StatusCode != http.StatusBadRequest {
			t.Fatalf("get without customer key %d", res.StatusCode)
		}
		res, body := request(http.MethodGet, true)
		if res.StatusCode != http.StatusOK || string(body) != "secret" || res.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] {
			t.Fatalf("get customer encryption %d %q %v", res.StatusCode, body, res.Header)
		}
	})

	t.Run("Given a customer-encrypted source When copying it Then source and destination keys are independent", func(t *testing.T) {
		res := do(http.MethodPut, "/copy-customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		key, keyMD5 := base64.StdEncoding.EncodeToString(rawKey), base64.StdEncoding.EncodeToString(digest[:])
		destinationHeaders := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       key,
			"x-amz-server-side-encryption-customer-key-MD5":   keyMD5,
		}
		request := func(method, path, body string, headers map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		if source, _ := request(http.MethodPut, "/copy-customer-encryption/source", "copy body", destinationHeaders); source.StatusCode != http.StatusOK {
			t.Fatalf("put copy source %d", source.StatusCode)
		}
		if copied, _ := request(http.MethodPut, "/copy-customer-encryption/missing-source-key", "", map[string]string{"x-amz-copy-source": "/copy-customer-encryption/source"}); copied.StatusCode != http.StatusBadRequest {
			t.Fatalf("copy without source key %d", copied.StatusCode)
		}
		copyHeaders := maps.Clone(destinationHeaders)
		copyHeaders["x-amz-copy-source"] = "/copy-customer-encryption/source"
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-algorithm"] = "AES256"
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-key"] = key
		copyHeaders["x-amz-copy-source-server-side-encryption-customer-key-MD5"] = keyMD5
		copied, _ := request(http.MethodPut, "/copy-customer-encryption/copied", "", copyHeaders)
		if copied.StatusCode != http.StatusOK || copied.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("copy customer encryption %d %v", copied.StatusCode, copied.Header)
		}
		stored, body := request(http.MethodGet, "/copy-customer-encryption/copied", "", destinationHeaders)
		if stored.StatusCode != http.StatusOK || string(body) != "copy body" || stored.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("stored customer copy %d %q %v", stored.StatusCode, body, stored.Header)
		}
	})

	t.Run("Given copy source preconditions When copying Then LocalStack evaluation order is preserved", func(t *testing.T) {
		res := do(http.MethodPut, "/copy-conditions", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		request := func(key string, headers map[string]string) *http.Response {
			t.Helper()
			req, _ := http.NewRequest(http.MethodPut, ts.URL+"/copy-conditions/"+key, nil)
			req.Header.Set("Authorization", auth)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			return response
		}
		source := request("source", nil)
		if source.StatusCode != http.StatusOK {
			t.Fatalf("put source %d", source.StatusCode)
		}
		copySource := "/copy-conditions/source"
		future := request("future", map[string]string{
			"x-amz-copy-source":                   copySource,
			"x-amz-copy-source-if-modified-since": time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC).Format(http.TimeFormat),
		})
		if future.StatusCode != http.StatusOK {
			t.Fatalf("future modified-since copy %d", future.StatusCode)
		}
		past := time.Unix(-1, 0).UTC().Format(http.TimeFormat)
		ordered := request("ordered", map[string]string{
			"x-amz-copy-source":                     copySource,
			"x-amz-copy-source-if-match":            source.Header.Get("ETag"),
			"x-amz-copy-source-if-none-match":       source.Header.Get("ETag"),
			"x-amz-copy-source-if-modified-since":   past,
			"x-amz-copy-source-if-unmodified-since": past,
		})
		if ordered.StatusCode != http.StatusOK {
			t.Fatalf("ordered copy %d", ordered.StatusCode)
		}
	})

	t.Run("Given KMS multipart encryption When completing the upload Then every stage preserves its headers", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		keyID := "arn:aws:kms:us-east-1:000000000000:key/multipart-behavior"
		request := func(method, path, body string, headers map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for key, value := range headers {
				req.Header.Set(key, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		assertEncryption := func(name string, response *http.Response) {
			t.Helper()
			if response.Header.Get("x-amz-server-side-encryption") != "aws:kms" || response.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || response.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
				t.Fatalf("%s encryption headers %v", name, response.Header)
			}
		}
		created, createdBody := request(http.MethodPost, "/multipart-encryption/object?uploads", "", map[string]string{"x-amz-checksum-algorithm": "CRC64NVME", "x-amz-server-side-encryption": "aws:kms", "x-amz-server-side-encryption-aws-kms-key-id": keyID, "x-amz-server-side-encryption-bucket-key-enabled": "true"})
		var upload struct {
			ID string `xml:"UploadId"`
		}
		if created.StatusCode != http.StatusOK || xml.Unmarshal(createdBody, &upload) != nil || upload.ID == "" {
			t.Fatalf("create upload %d %s", created.StatusCode, createdBody)
		}
		assertEncryption("create", created)
		part, _ := request(http.MethodPut, "/multipart-encryption/object?partNumber=1&uploadId="+upload.ID, "body", nil)
		if part.StatusCode != http.StatusOK || part.Header.Get("ETag") == "" || part.Header.Get("Content-Type") != "" || part.Header.Get("Content-Length") != "0" {
			t.Fatalf("upload part %d %v", part.StatusCode, part.Header)
		}
		assertEncryption("part", part)
		manifest := "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
		completed, completedBody := request(http.MethodPost, "/multipart-encryption/object?uploadId="+upload.ID, manifest, nil)
		if completed.StatusCode != http.StatusOK || completed.Header.Get("x-amz-checksum-crc64nvme") != "" || completed.Header.Get("x-amz-checksum-type") != "" || bytes.Contains(completedBody, []byte("ChecksumCRC64NVME")) || bytes.Contains(completedBody, []byte("ChecksumType")) {
			t.Fatalf("complete upload %d", completed.StatusCode)
		}
		assertEncryption("complete", completed)
		stored, body := request(http.MethodGet, "/multipart-encryption/object", "", map[string]string{"x-amz-checksum-mode": "ENABLED"})
		if stored.StatusCode != http.StatusOK || string(body) != "body" || stored.Header.Get("x-amz-checksum-crc64nvme") == "" || stored.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("stored object %d %q", stored.StatusCode, body)
		}
		assertEncryption("get", stored)
	})

	t.Run("Given customer multipart encryption When uploading parts Then matching headers are required", func(t *testing.T) {
		res := do(http.MethodPut, "/multipart-customer-encryption", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		rawKey := []byte("0123456789abcdef0123456789abcdef")
		digest := md5.Sum(rawKey)
		headers := map[string]string{
			"x-amz-server-side-encryption-customer-algorithm": "AES256",
			"x-amz-server-side-encryption-customer-key":       base64.StdEncoding.EncodeToString(rawKey),
			"x-amz-server-side-encryption-customer-key-MD5":   base64.StdEncoding.EncodeToString(digest[:]),
		}
		request := func(method, path, body string, extra map[string]string) (*http.Response, []byte) {
			t.Helper()
			req, _ := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
			req.Header.Set("Authorization", auth)
			for key, value := range extra {
				req.Header.Set(key, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, data
		}
		assertEncryption := func(name string, response *http.Response) {
			t.Helper()
			if response.Header.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" || response.Header.Get("x-amz-server-side-encryption-customer-key-MD5") != headers["x-amz-server-side-encryption-customer-key-MD5"] || response.Header.Get("x-amz-server-side-encryption") != "" {
				t.Fatalf("%s customer encryption headers %v", name, response.Header)
			}
		}
		created, createdBody := request(http.MethodPost, "/multipart-customer-encryption/object?uploads", "", headers)
		var upload struct {
			ID string `xml:"UploadId"`
		}
		if created.StatusCode != http.StatusOK || xml.Unmarshal(createdBody, &upload) != nil || upload.ID == "" {
			t.Fatalf("create customer upload %d %s", created.StatusCode, createdBody)
		}
		assertEncryption("create", created)
		partPath := "/multipart-customer-encryption/object?partNumber=1&uploadId=" + upload.ID
		if part, faultBody := request(http.MethodPut, partPath, "body", nil); part.StatusCode != http.StatusBadRequest || !bytes.Contains(faultBody, []byte("The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters.")) {
			t.Fatalf("part without customer key %d %s", part.StatusCode, faultBody)
		}
		otherKey := bytes.Repeat([]byte{'b'}, 32)
		otherDigest := md5.Sum(otherKey)
		wrongHeaders := maps.Clone(headers)
		wrongHeaders["x-amz-server-side-encryption-customer-key"] = base64.StdEncoding.EncodeToString(otherKey)
		wrongHeaders["x-amz-server-side-encryption-customer-key-MD5"] = base64.StdEncoding.EncodeToString(otherDigest[:])
		if part, faultBody := request(http.MethodPut, partPath, "body", wrongHeaders); part.StatusCode != http.StatusBadRequest || !bytes.Contains(faultBody, []byte("The provided encryption parameters did not match the ones used originally.")) {
			t.Fatalf("part with mismatched customer key %d %s", part.StatusCode, faultBody)
		}
		part, _ := request(http.MethodPut, partPath, "body", headers)
		if part.StatusCode != http.StatusOK || part.Header.Get("ETag") == "" {
			t.Fatalf("upload customer part %d %v", part.StatusCode, part.Header)
		}
		assertEncryption("part", part)
		manifest := "<CompleteMultipartUpload><Part><ETag>" + part.Header.Get("ETag") + "</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"
		completed, _ := request(http.MethodPost, "/multipart-customer-encryption/object?uploadId="+upload.ID, manifest, nil)
		if completed.StatusCode != http.StatusOK {
			t.Fatalf("complete customer upload %d", completed.StatusCode)
		}
		assertEncryption("complete", completed)
		stored, body := request(http.MethodGet, "/multipart-customer-encryption/object", "", headers)
		if stored.StatusCode != http.StatusOK || string(body) != "body" {
			t.Fatalf("stored customer object %d %q", stored.StatusCode, body)
		}
		assertEncryption("get", stored)
	})

	t.Run("Given an expired browser policy When POSTing a file Then S3 rejects it without storing the object", func(t *testing.T) {
		res := do(http.MethodPut, "/post-policy", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode >= 300 {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "forms/expired.txt")
		_ = writer.WriteField("policy", base64.StdEncoding.EncodeToString([]byte(`{"expiration":"1960-01-01T00:00:00Z","conditions":[]}`)))
		_ = writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
		_ = writer.WriteField("x-amz-credential", "test/20260827/us-east-1/s3/aws4_request")
		_ = writer.WriteField("x-amz-date", "20260827T000000Z")
		_ = writer.WriteField("x-amz-signature", "signature")
		file, _ := writer.CreateFormFile("file", "expired.txt")
		_, _ = file.Write([]byte("must not persist"))
		_ = writer.Close()
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/post-policy", &payload)
		req.Header.Set("Authorization", auth)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusForbidden || !bytes.Contains(fault, []byte("AccessDenied")) || !bytes.Contains(fault, []byte("Policy expired")) {
			t.Fatalf("expired form %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodGet, "/post-policy/forms/expired.txt", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("expired form persisted object: %d", res.StatusCode)
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

	t.Run("Given tags in CreateBucketConfiguration When creating a bucket Then the tags are persisted atomically", func(t *testing.T) {
		configuration := []byte(`<CreateBucketConfiguration><Tags><Tag><Key>team</Key><Value>storage</Value></Tag><Tag><Key>env</Key><Value>test</Value></Tag></Tags></CreateBucketConfiguration>`)
		res := do(http.MethodPut, "/create-tagged", configuration, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("tagged create %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/create-tagged?tagging", nil, "")
		tags, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(tags, []byte("<Key>team</Key>")) || !bytes.Contains(tags, []byte("<Value>test</Value>")) {
			t.Fatalf("created tags %d %s", res.StatusCode, tags)
		}
		res = do(http.MethodPut, "/create-tagged", configuration, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusConflict || !bytes.Contains(fault, []byte("BucketAlreadyOwnedByYou")) {
			t.Fatalf("tagged recreation %d %s", res.StatusCode, fault)
		}
		invalid := []byte(`<CreateBucketConfiguration><Tags><Tag><Key>duplicate</Key><Value>one</Value></Tag><Tag><Key>duplicate</Key><Value>two</Value></Tag></Tags></CreateBucketConfiguration>`)
		res = do(http.MethodPut, "/invalid-create-tags", invalid, "")
		fault, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidTag")) {
			t.Fatalf("invalid create tags %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodHead, "/invalid-create-tags", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid tags reserved bucket: %d", res.StatusCode)
		}
	})

	t.Run("Given object ownership When creating a bucket Then ownership controls are persisted", func(t *testing.T) {
		res := do(http.MethodPut, "/create-owned", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("owned create %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/create-owned?ownershipControls", nil, "")
		controls, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(controls, []byte("<Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule>")) || bytes.Contains(controls, []byte("<member>")) {
			t.Fatalf("created ownership %d %s", res.StatusCode, controls)
		}
		res = do(http.MethodPut, "/invalid-create-owned", nil, "")
		fault, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(fault, []byte("InvalidArgument")) {
			t.Fatalf("invalid ownership %d %s", res.StatusCode, fault)
		}
		res = do(http.MethodHead, "/invalid-create-owned", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("invalid ownership reserved bucket: %d", res.StatusCode)
		}
	})

	t.Run("Given ownership controls When replacing and deleting them Then S3 validates and persists the rule", func(t *testing.T) {
		res := do(http.MethodPut, "/ownership-controls", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create ownership-controls bucket %d", res.StatusCode)
		}
		valid := []byte(`<OwnershipControls><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></OwnershipControls>`)
		res = do(http.MethodPut, "/ownership-controls?ownershipControls", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put ownership controls %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule>")) {
			t.Fatalf("get ownership controls %d %s", res.StatusCode, body)
		}
		multiple := []byte(`<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>`)
		res = do(http.MethodPut, "/ownership-controls?ownershipControls", multiple, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("multiple ownership controls %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ObjectOwnership>ObjectWriter</ObjectOwnership>")) {
			t.Fatalf("controls after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/ownership-controls?ownershipControls", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete ownership controls %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/ownership-controls?ownershipControls", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("OwnershipControlsNotFoundError")) {
			t.Fatalf("get deleted ownership controls %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given a public access block When replacing and deleting it Then S3 validates and defaults the flags", func(t *testing.T) {
		res := do(http.MethodPut, "/public-access-block", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create public-access-block bucket %d", res.StatusCode)
		}
		valid := []byte(`<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`)
		res = do(http.MethodPut, "/public-access-block?publicAccessBlock", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put public access block %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<BlockPublicAcls>true</BlockPublicAcls>")) || !bytes.Contains(body, []byte("<BlockPublicPolicy>false</BlockPublicPolicy>")) || !bytes.Contains(body, []byte("<IgnorePublicAcls>false</IgnorePublicAcls>")) || !bytes.Contains(body, []byte("<RestrictPublicBuckets>false</RestrictPublicBuckets>")) || bytes.Contains(body, []byte("GetPublicAccessBlockResult")) {
			t.Fatalf("get public access block %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<PublicAccessBlockConfiguration><Unknown>true</Unknown></PublicAccessBlockConfiguration>`)
		res = do(http.MethodPut, "/public-access-block?publicAccessBlock", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid public access block %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<BlockPublicAcls>true</BlockPublicAcls>")) {
			t.Fatalf("public access block after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/public-access-block?publicAccessBlock", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete public access block %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/public-access-block?publicAccessBlock", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchPublicAccessBlockConfiguration")) {
			t.Fatalf("get deleted public access block %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket logging When enabling and disabling it Then S3 validates and persists the destination", func(t *testing.T) {
		for _, bucket := range []string{"logging-bdd-source", "logging-bdd-target"} {
			res := do(http.MethodPut, "/"+bucket, nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s bucket %d", bucket, res.StatusCode)
			}
		}
		res := do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<LoggingEnabled>")) {
			t.Fatalf("default logging %d %s", res.StatusCode, body)
		}
		valid := []byte(`<BucketLoggingStatus><LoggingEnabled><TargetBucket>logging-bdd-target</TargetBucket><TargetPrefix>logs/</TargetPrefix></LoggingEnabled></BucketLoggingStatus>`)
		res = do(http.MethodPut, "/logging-bdd-source?logging", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put logging %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<TargetBucket>logging-bdd-target</TargetBucket>")) || !bytes.Contains(body, []byte("<TargetPrefix>logs/</TargetPrefix>")) || bytes.Contains(body, []byte("GetBucketLoggingResult")) {
			t.Fatalf("get logging %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<BucketLoggingStatus><LoggingEnabled><TargetBucket>missing</TargetBucket></LoggingEnabled></BucketLoggingStatus>`)
		res = do(http.MethodPut, "/logging-bdd-source?logging", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidTargetBucketForLogging")) {
			t.Fatalf("invalid logging %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/logging-bdd-source?logging", []byte(`<BucketLoggingStatus/>`), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("disable logging %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/logging-bdd-source?logging", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("<LoggingEnabled>")) {
			t.Fatalf("disabled logging %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket CORS When replacing and deleting it Then S3 validates and persists the rules", func(t *testing.T) {
		res := do(http.MethodPut, "/cors-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create CORS bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchCORSConfiguration")) {
			t.Fatalf("default CORS %d %s", res.StatusCode, body)
		}
		valid := []byte(`<CORSConfiguration><CORSRule><ID>read</ID><AllowedMethod>GET</AllowedMethod><AllowedMethod>HEAD</AllowedMethod><AllowedOrigin>https://example.test</AllowedOrigin><MaxAgeSeconds>300</MaxAgeSeconds></CORSRule></CORSConfiguration>`)
		res = do(http.MethodPut, "/cors-bdd?cors", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put CORS %d", res.StatusCode)
		}
		preflight, _ := http.NewRequest(http.MethodOptions, ts.URL+"/key", nil)
		preflight.Host = "cors-bdd.s3.us-east-1.amazonaws.com"
		preflight.Header.Set("Origin", "https://example.test")
		preflight.Header.Set("Access-Control-Request-Method", http.MethodGet)
		res, err = http.DefaultClient.Do(preflight)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Access-Control-Allow-Origin") != "https://example.test" || res.Header.Get("Access-Control-Allow-Methods") != "GET, HEAD" || res.Header.Get("Access-Control-Max-Age") != "300" {
			t.Fatalf("CORS preflight %d %#v", res.StatusCode, res.Header)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<AllowedMethod>GET</AllowedMethod>")) || !bytes.Contains(body, []byte("<AllowedOrigin>https://example.test</AllowedOrigin>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketCorsResult")) {
			t.Fatalf("get CORS %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<CORSConfiguration><CORSRule><AllowedMethod>OPTIONS</AllowedMethod><AllowedOrigin>*</AllowedOrigin></CORSRule></CORSConfiguration>`)
		res = do(http.MethodPut, "/cors-bdd?cors", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidRequest")) {
			t.Fatalf("invalid CORS %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ID>read</ID>")) {
			t.Fatalf("CORS after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/cors-bdd?cors", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete CORS %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/cors-bdd?cors", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchCORSConfiguration")) {
			t.Fatalf("get deleted CORS %d %s", res.StatusCode, body)
		}
		defaultPreflight, _ := http.NewRequest(http.MethodOptions, ts.URL+"/key", nil)
		defaultPreflight.Host = "cors-bdd.s3.us-east-1.amazonaws.com"
		defaultPreflight.Header.Set("Origin", "https://app.localstack.cloud")
		res, err = http.DefaultClient.Do(defaultPreflight)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Access-Control-Allow-Origin") != "https://app.localstack.cloud" || res.Header.Get("Vary") != "Origin" {
			t.Fatalf("LocalStack default CORS %d %#v", res.StatusCode, res.Header)
		}
	})

	t.Run("Given a bucket website When replacing and deleting it Then S3 validates and persists the routing", func(t *testing.T) {
		websiteBucket := func(bucket, method, path string) (*http.Response, []byte) {
			t.Helper()
			req, err := http.NewRequest(method, ts.URL+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = bucket + ".s3-website.localhost.localstack.cloud"
			response, err := (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			return response, body
		}
		website := func(method, path string) (*http.Response, []byte) {
			return websiteBucket("website-bdd", method, path)
		}
		res := do(http.MethodPut, "/website-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create website bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchWebsiteConfiguration")) {
			t.Fatalf("default website %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/")
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<li>Code: NoSuchWebsiteConfiguration</li>")) {
			t.Fatalf("unconfigured website endpoint %d %s", res.StatusCode, body)
		}
		res, body = websiteBucket("missing-website-bdd", http.MethodGet, "/")
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("<li>Code: NoSuchBucket</li>")) {
			t.Fatalf("missing website bucket %d %s", res.StatusCode, body)
		}
		valid := []byte(`<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument><RoutingRules><RoutingRule><Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition><Redirect><Protocol>https</Protocol><ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith></Redirect></RoutingRule></RoutingRules></WebsiteConfiguration>`)
		res = do(http.MethodPut, "/website-bdd?website", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put website %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Suffix>index.html</Suffix>")) || !bytes.Contains(body, []byte("<ReplaceKeyPrefixWith>manual/</ReplaceKeyPrefixWith>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketWebsiteResult")) {
			t.Fatalf("get website %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<WebsiteConfiguration><IndexDocument><Suffix>dir/index.html</Suffix></IndexDocument></WebsiteConfiguration>`)
		res = do(http.MethodPut, "/website-bdd?website", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid website %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Suffix>index.html</Suffix>")) {
			t.Fatalf("website after invalid put %d %s", res.StatusCode, body)
		}
		for _, object := range []struct{ key, body string }{{"index.html", "index"}, {"error.html", "error"}} {
			res = do(http.MethodPut, "/website-bdd/"+object.key, []byte(object.body), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put website object %s = %d", object.key, res.StatusCode)
			}
		}
		res, body = website(http.MethodGet, "/")
		if res.StatusCode != http.StatusOK || string(body) != "index" {
			t.Fatalf("website index %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/missing")
		if res.StatusCode != http.StatusNotFound || string(body) != "error" {
			t.Fatalf("website error document %d %s", res.StatusCode, body)
		}
		res, body = website(http.MethodGet, "/docs/key")
		if res.StatusCode != http.StatusMovedPermanently || res.Header.Get("Location") != "https://website-bdd.s3-website.localhost.localstack.cloud/manual/key" || len(body) != 0 {
			t.Fatalf("website routing rule %d %#v %s", res.StatusCode, res.Header, body)
		}
		res, body = website(http.MethodPost, "/")
		if res.StatusCode != http.StatusMethodNotAllowed || !bytes.Contains(body, []byte("<li>Method: POST</li>")) {
			t.Fatalf("website method %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/website-bdd?website", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete website %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/website-bdd?website", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchWebsiteConfiguration")) {
			t.Fatalf("get deleted website %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket lifecycle When configuring objects Then S3 returns LocalStack expiration headers", func(t *testing.T) {
		res := do(http.MethodPut, "/lifecycle-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create lifecycle bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchLifecycleConfiguration")) {
			t.Fatalf("default lifecycle %d %s", res.StatusCode, body)
		}
		valid := []byte(`<LifecycleConfiguration><Rule><ID>expire</ID><Filter></Filter><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
		res = do(http.MethodPut, "/lifecycle-bdd?lifecycle", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 || res.Header.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" {
			t.Fatalf("put lifecycle %d %s headers=%v", res.StatusCode, body, res.Header)
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<LifecycleConfiguration")) || !bytes.Contains(body, []byte("<Rule>")) || bytes.Contains(body, []byte("<member>")) || res.Header.Get("x-amz-transition-default-minimum-object-size") != "all_storage_classes_128K" {
			t.Fatalf("get lifecycle %d %s headers=%v", res.StatusCode, body, res.Header)
		}
		res = do(http.MethodPut, "/lifecycle-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("x-amz-expiration"), `rule-id="expire"`) {
			t.Fatalf("put lifecycle object %d headers=%v", res.StatusCode, res.Header)
		}
		res = do(http.MethodHead, "/lifecycle-bdd/object", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("x-amz-expiration"), `rule-id="expire"`) {
			t.Fatalf("head lifecycle object %d headers=%v", res.StatusCode, res.Header)
		}
		res = do(http.MethodPost, "/lifecycle-bdd/multipart?uploads", nil, "")
		var created struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.NewDecoder(res.Body).Decode(&created); err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		res = do(http.MethodPut, "/lifecycle-bdd/multipart?partNumber=1&uploadId="+url.QueryEscape(created.UploadID), []byte("body"), "")
		etag := res.Header.Get("ETag")
		res.Body.Close()
		manifest := []byte(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>` + etag + `</ETag></Part></CompleteMultipartUpload>`)
		res = do(http.MethodPost, "/lifecycle-bdd/multipart?uploadId="+url.QueryEscape(created.UploadID), manifest, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !strings.Contains(res.Header.Get("x-amz-expiration"), `rule-id="expire"`) {
			t.Fatalf("complete lifecycle object %d %s headers=%v", res.StatusCode, body, res.Header)
		}
		invalid := []byte(`<LifecycleConfiguration><Rule><ID>invalid</ID><Filter><Prefix>a</Prefix><ObjectSizeGreaterThan>1</ObjectSizeGreaterThan></Filter><Status>Enabled</Status></Rule></LifecycleConfiguration>`)
		res = do(http.MethodPut, "/lifecycle-bdd?lifecycle", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid lifecycle %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/lifecycle-bdd?lifecycle", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete lifecycle %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/lifecycle-bdd?lifecycle", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchLifecycleConfiguration")) {
			t.Fatalf("get deleted lifecycle %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given named bucket configurations When managing them Then S3 preserves LocalStack ordering and errors", func(t *testing.T) {
		res := do(http.MethodPut, "/named-configuration-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create named configuration bucket %d", res.StatusCode)
		}
		for _, configurationID := range []string{"z-analysis", "a-analysis"} {
			body := []byte(`<AnalyticsConfiguration><Id>` + configurationID + `</Id><Filter><Prefix>` + configurationID + `/</Prefix></Filter></AnalyticsConfiguration>`)
			res = do(http.MethodPut, "/named-configuration-bdd?analytics&id="+configurationID, body, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put analytics configuration %d", res.StatusCode)
			}
		}
		res = do(http.MethodGet, "/named-configuration-bdd?analytics", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if first, second := bytes.Index(body, []byte("<Id>a-analysis</Id>")), bytes.Index(body, []byte("<Id>z-analysis</Id>")); res.StatusCode != http.StatusOK || first < 0 || second < first || strings.Count(string(body), "<AnalyticsConfiguration>") != 2 || bytes.Contains(body, []byte("<member>")) {
			t.Fatalf("list analytics configurations %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<AnalyticsConfiguration><Id>body-id</Id></AnalyticsConfiguration>`)
		res = do(http.MethodPut, "/named-configuration-bdd?analytics&id=request-id", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("mismatched analytics configuration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodDelete, "/named-configuration-bdd?analytics&id=missing", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchConfiguration")) {
			t.Fatalf("delete missing analytics configuration %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given bucket and object ACLs When replacing policies Then S3 validates and round trips AWS XML", func(t *testing.T) {
		res := do(http.MethodPut, "/acl-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create ACL bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd?acl", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Owner>")) || !bytes.Contains(body, []byte("<Permission>FULL_CONTROL</Permission>")) || bytes.Contains(body, []byte("<DisplayName>")) || bytes.Contains(body, []byte("GetBucketAclResult")) {
			t.Fatalf("default bucket ACL %d %s", res.StatusCode, body)
		}
		public := []byte(`<AccessControlPolicy><Owner><ID>000000000000</ID><DisplayName>mirror</DisplayName></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>000000000000</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="Group"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant></AccessControlList></AccessControlPolicy>`)
		res = do(http.MethodPut, "/acl-bdd?acl", public, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put bucket ACL %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd?acl", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>")) || strings.Count(string(body), "<Grant>") != 2 {
			t.Fatalf("public bucket ACL %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/acl-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put ACL object %d", res.StatusCode)
		}
		res = do(http.MethodPut, "/acl-bdd/object?acl", []byte(`<AccessControlPolicy/>`), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedACLError")) {
			t.Fatalf("invalid object ACL %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/acl-bdd/object?acl", public, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put object ACL %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/acl-bdd/object?acl", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || strings.Count(string(body), "<Grant>") != 2 || bytes.Contains(body, []byte("GetObjectAclResult")) {
			t.Fatalf("public object ACL %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given an object delete marker When reading or writing its ACL Then S3 returns LocalStack faults", func(t *testing.T) {
		for _, request := range []struct {
			method, path, body string
		}{
			{http.MethodPut, "/acl-marker-bdd", ""},
			{http.MethodPut, "/acl-marker-bdd?versioning", "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"},
			{http.MethodPut, "/acl-marker-bdd/object", "body"},
		} {
			res := do(request.method, request.path, []byte(request.body), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("%s %s = %d", request.method, request.path, res.StatusCode)
			}
		}
		res := do(http.MethodDelete, "/acl-marker-bdd/object", nil, "")
		res.Body.Close()
		versionID := res.Header.Get("x-amz-version-id")
		if res.StatusCode != http.StatusNoContent || res.Header.Get("x-amz-delete-marker") != "true" || versionID == "" {
			t.Fatalf("delete marker %d %#v", res.StatusCode, res.Header)
		}
		for _, request := range []struct {
			method, path string
			status       int
			code, field  string
		}{
			{http.MethodPut, "/acl-marker-bdd/object?acl", http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>PUT</Method>"},
			{http.MethodGet, "/acl-marker-bdd/object?acl", http.StatusNotFound, "NoSuchKey", "<Key>object</Key>"},
			{http.MethodPut, "/acl-marker-bdd/object?acl&versionId=" + url.QueryEscape(versionID), http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>PUT</Method>"},
			{http.MethodGet, "/acl-marker-bdd/object?acl&versionId=" + url.QueryEscape(versionID), http.StatusMethodNotAllowed, "MethodNotAllowed", "<Method>GET</Method>"},
		} {
			res = do(request.method, request.path, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != request.status || !bytes.Contains(body, []byte("<Code>"+request.code+"</Code>")) || !bytes.Contains(body, []byte(request.field)) {
				t.Fatalf("%s %s = %d %s", request.method, request.path, res.StatusCode, body)
			}
		}
	})

	t.Run("Given a bucket policy When replacing it Then S3 validates and returns the exact JSON", func(t *testing.T) {
		res := do(http.MethodPut, "/policy-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create policy bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound || !bytes.Contains(body, []byte("NoSuchBucketPolicy")) {
			t.Fatalf("missing policy %d %s", res.StatusCode, body)
		}
		policy := []byte(`{"Version":"2012-10-17", "Statement":[{"Effect":"Allow","Principal":"*"}]}`)
		res = do(http.MethodPut, "/policy-bdd?policy", policy, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put policy %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" || !bytes.Equal(body, policy) {
			t.Fatalf("get policy %d content-type=%q body=%s", res.StatusCode, res.Header.Get("Content-Type"), body)
		}
		res = do(http.MethodPut, "/policy-bdd?policy", append([]byte{' '}, policy...), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedPolicy")) {
			t.Fatalf("invalid policy %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/policy-bdd?policy", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Equal(body, policy) {
			t.Fatalf("policy after invalid put %d %s", res.StatusCode, body)
		}
		for range 2 {
			res = do(http.MethodDelete, "/policy-bdd?policy", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete policy %d", res.StatusCode)
			}
		}
	})

	t.Run("Given LocalStack-unsupported operations When requested Then S3 reports them explicitly", func(t *testing.T) {
		for _, test := range []struct {
			path      string
			operation string
		}{
			{"/unsupported-bdd?policyStatus", "GetBucketPolicyStatus"},
			{"/unsupported-bdd/key?torrent", "GetObjectTorrent"},
		} {
			res := do(http.MethodGet, test.path, nil, "")
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNotImplemented || res.Header.Get("x-mirror-not-implemented") != "aws.s3."+test.operation || !bytes.Contains(body, []byte("MirrorNotImplemented")) {
				t.Fatalf("%s: %d %#v %s", test.operation, res.StatusCode, res.Header, body)
			}
		}
	})

	t.Run("Given bucket encryption When replacing it Then S3 validates and applies the default", func(t *testing.T) {
		res := do(http.MethodPut, "/encryption-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create encryption bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/encryption-bdd?encryption", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("default encryption %d %s", res.StatusCode, body)
		}
		valid := []byte(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>arn:aws:kms:us-east-1:000000000000:key/bdd</KMSMasterKeyID></ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule></ServerSideEncryptionConfiguration>`)
		res = do(http.MethodPut, "/encryption-bdd?encryption", valid, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put encryption %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/encryption-bdd?encryption", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<ServerSideEncryptionConfiguration")) || !bytes.Contains(body, []byte("<SSEAlgorithm>aws:kms</SSEAlgorithm>")) || !bytes.Contains(body, []byte("<BucketKeyEnabled>true</BucketKeyEnabled>")) || bytes.Contains(body, []byte("<member>")) {
			t.Fatalf("get encryption %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>key-id</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
		res = do(http.MethodPut, "/encryption-bdd?encryption", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid encryption %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/encryption-bdd/object", []byte("body"), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-server-side-encryption") != "aws:kms" || res.Header.Get("x-amz-server-side-encryption-aws-kms-key-id") != "arn:aws:kms:us-east-1:000000000000:key/bdd" || res.Header.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
			t.Fatalf("inherited encryption %d %v", res.StatusCode, res.Header)
		}
		for range 2 {
			res = do(http.MethodDelete, "/encryption-bdd?encryption", nil, "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusNoContent {
				t.Fatalf("delete encryption %d", res.StatusCode)
			}
		}
	})

	t.Run("Given bucket notifications When configuring and clearing them Then matching objects reach the queue", func(t *testing.T) {
		res := do(http.MethodPut, "/notification-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create notification bucket %d", res.StatusCode)
		}
		if err := deps.Store.Scope("000000000000", "us-east-1").Collection("queues").Put(context.Background(), "notification-queue", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		valid := []byte(`<NotificationConfiguration><QueueConfiguration><Id>images</Id><Queue>arn:aws:sqs:us-east-1:000000000000:notification-queue</Queue><Event>s3:ObjectCreated:*</Event><Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule><FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter></QueueConfiguration></NotificationConfiguration>`)
		res = do(http.MethodPut, "/notification-bdd?notification", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put notifications %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/notification-bdd?notification", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<QueueConfiguration>")) || !bytes.Contains(body, []byte("<Name>Prefix</Name>")) || bytes.Contains(body, []byte("<member>")) || bytes.Contains(body, []byte("GetBucketNotificationConfigurationResult")) {
			t.Fatalf("get notifications %d %s", res.StatusCode, body)
		}
		for _, key := range []string{"images/photo.jpg", "images/photo.png", "docs/photo.jpg"} {
			res = do(http.MethodPut, "/notification-bdd/"+key, []byte(key), "")
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("put notification object %s: %d", key, res.StatusCode)
			}
		}
		messages, _, err := deps.Store.Scope("000000000000", "us-east-1").Collection("msgs:notification-queue").List(context.Background(), "", "", 0)
		matched := false
		for _, message := range messages {
			matched = matched || bytes.Contains(message.Value, []byte("images/photo.jpg"))
		}
		if err != nil || len(messages) != 2 || !matched {
			t.Fatalf("notification messages = %#v, err=%v", messages, err)
		}
		invalid := []byte(`<NotificationConfiguration><QueueConfiguration><Queue>arn:aws:sqs:us-east-1:000000000000:missing</Queue><Event>s3:ObjectCreated:*</Event></QueueConfiguration></NotificationConfiguration>`)
		res = do(http.MethodPut, "/notification-bdd?notification", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidArgument")) {
			t.Fatalf("invalid notifications %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/notification-bdd?notification", []byte(`<NotificationConfiguration/>`), "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("clear notifications %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/notification-bdd?notification", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || bytes.Contains(body, []byte("QueueConfiguration")) {
			t.Fatalf("cleared notifications %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given request payment When changing the payer Then S3 validates and persists it", func(t *testing.T) {
		res := do(http.MethodPut, "/request-payment", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create request-payment bucket %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/request-payment?requestPayment", nil, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Payer>BucketOwner</Payer>")) {
			t.Fatalf("default request payer %d %s", res.StatusCode, body)
		}
		valid := []byte(`<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>`)
		res = do(http.MethodPut, "/request-payment?requestPayment", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put request payer %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<RequestPaymentConfiguration><Payer>Invalid</Payer></RequestPaymentConfiguration>`)
		res = do(http.MethodPut, "/request-payment?requestPayment", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid request payer %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/request-payment?requestPayment", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Payer>Requester</Payer>")) || bytes.Contains(body, []byte("GetBucketRequestPaymentResult")) {
			t.Fatalf("request payer after invalid put %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given acceleration When changing its status Then S3 validates and persists it", func(t *testing.T) {
		res := do(http.MethodPut, "/accelerate-bdd", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create accelerate bucket %d", res.StatusCode)
		}
		valid := []byte(`<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>`)
		res = do(http.MethodPut, "/accelerate-bdd?accelerate", valid, "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("put acceleration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/accelerate-bdd?accelerate", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) || bytes.Contains(body, []byte("GetBucketAccelerateConfigurationResult")) {
			t.Fatalf("get acceleration %d %s", res.StatusCode, body)
		}
		invalid := []byte(`<AccelerateConfiguration><Status>Invalid</Status></AccelerateConfiguration>`)
		res = do(http.MethodPut, "/accelerate-bdd?accelerate", invalid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("MalformedXML")) {
			t.Fatalf("invalid acceleration %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/accelerate-bdd?accelerate", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) {
			t.Fatalf("acceleration after invalid put %d %s", res.StatusCode, body)
		}
		res = do(http.MethodPut, "/accelerate.with.period", nil, "")
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		res = do(http.MethodPut, "/accelerate.with.period?accelerate", valid, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("InvalidRequest")) {
			t.Fatalf("period bucket acceleration %d %s", res.StatusCode, body)
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
			if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-bucket-arn") != "arn:aws:s3:::"+bucket.name {
				t.Fatalf("create %s: %d", bucket.name, res.StatusCode)
			}
		}
		type page struct {
			Buckets []struct {
				Name         string `xml:"Name"`
				BucketArn    string `xml:"BucketArn"`
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
		if len(first.Buckets) != 1 || first.Buckets[0].Name != "list-behavior-east" || first.Buckets[0].BucketArn != "arn:aws:s3:::list-behavior-east" || first.Buckets[0].BucketRegion != "us-east-1" || first.Prefix != "list-behavior" || first.ContinuationToken == "" {
			t.Fatalf("first page: %#v", first)
		}
		second := list("/?max-buckets=1&prefix=list-behavior&continuation-token=" + url.QueryEscape(first.ContinuationToken))
		if len(second.Buckets) != 1 || second.Buckets[0].Name != "list-behavior-west" || second.Buckets[0].BucketArn != "arn:aws:s3:::list-behavior-west" || second.Buckets[0].BucketRegion != "us-west-2" || second.ContinuationToken != "" {
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
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || len(body) != 0 {
			t.Fatalf("enable versioning %d %s", res.StatusCode, body)
		}
		res = do(http.MethodGet, "/versioning-state?versioning", nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(body, []byte("<Status>Enabled</Status>")) {
			t.Fatalf("enabled versioning %d %s", res.StatusCode, body)
		}
	})

	t.Run("Given versioned replication When an object is written Then its version is readable from the replica", func(t *testing.T) {
		for _, bucket := range []string{"replication-source", "replication-destination"} {
			res := do(http.MethodPut, "/"+bucket, nil, "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("create %s: %d", bucket, res.StatusCode)
			}
			res = do(http.MethodPut, "/"+bucket+"?versioning", []byte("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"), "")
			res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Fatalf("version %s: %d", bucket, res.StatusCode)
			}
		}
		res := do(http.MethodPut, "/replication-unversioned", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create unversioned destination: %d", res.StatusCode)
		}
		unversionedConfiguration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Status>Enabled</Status><Destination><Bucket>arn:aws:s3:::replication-unversioned</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(unversionedConfiguration), "")
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) {
			t.Fatalf("unversioned destination validation: %d %s", res.StatusCode, body)
		}
		invalidConfiguration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Priority>1</Priority><Status>Enabled</Status><Filter><Tag><Key>environment</Key><Value>test</Value></Tag></Filter><DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replication-destination</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(invalidConfiguration), "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest || !bytes.Contains(body, []byte("<Code>InvalidRequest</Code>")) {
			t.Fatalf("tag delete-marker validation: %d %s", res.StatusCode, body)
		}
		configuration := `<ReplicationConfiguration><Role>arn:aws:iam::000000000000:role/replication</Role><Rule><Priority>1</Priority><Status>Enabled</Status><Filter><And><Prefix>logs/</Prefix><Tag><Key>environment</Key><Value>test</Value></Tag></And></Filter><DeleteMarkerReplication><Status>Disabled</Status></DeleteMarkerReplication><Destination><Bucket>arn:aws:s3:::replication-destination</Bucket></Destination></Rule></ReplicationConfiguration>`
		res = do(http.MethodPut, "/replication-source?replication", []byte(configuration), "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("configure replication: %d", res.StatusCode)
		}
		res = do(http.MethodGet, "/replication-source?replication", nil, "")
		gotConfiguration, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(gotConfiguration, []byte("<ReplicationConfiguration")) || !bytes.Contains(gotConfiguration, []byte("<Rule>")) || !bytes.Contains(gotConfiguration, []byte("<Prefix>logs/</Prefix>")) || !bytes.Contains(gotConfiguration, []byte("<Tag><Key>environment</Key><Value>test</Value></Tag>")) || bytes.Contains(gotConfiguration, []byte("<member>")) {
			t.Fatalf("get replication: %d %s", res.StatusCode, gotConfiguration)
		}
		req, err := http.NewRequest(http.MethodPut, ts.URL+"/replication-source/logs/key", strings.NewReader("replicated-version"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", auth)
		req.Header.Set("x-amz-tagging", "environment=test")
		res, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		version := res.Header.Get("x-amz-version-id")
		if res.StatusCode != http.StatusOK || version == "" || res.Header.Get("x-amz-replication-status") != "COMPLETED" {
			t.Fatalf("put replicated version: %d %v", res.StatusCode, res.Header)
		}
		res = do(http.MethodGet, "/replication-destination/logs/key?versionId="+url.QueryEscape(version), nil, "")
		body, _ = io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || string(body) != "replicated-version" || res.Header.Get("x-amz-version-id") != version || res.Header.Get("x-amz-replication-status") != "REPLICA" {
			t.Fatalf("get replicated version: %d %q %v", res.StatusCode, body, res.Header)
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

	t.Run("Given a checksummed object When copying it Then the checksum is preserved", func(t *testing.T) {
		res := do(http.MethodPut, "/copy-checksum-behavior", nil, "")
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("create bucket %d", res.StatusCode)
		}
		body := []byte("copy-checksum")
		digest := sha256.Sum256(body)
		checksum := base64.StdEncoding.EncodeToString(digest[:])
		request := func(method, path string, headers map[string]string) *http.Response {
			t.Helper()
			payload := body
			if method == http.MethodHead || headers["x-amz-copy-source"] != "" {
				payload = nil
			}
			req, err := http.NewRequest(method, ts.URL+path, bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", auth)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			response, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			return response
		}
		res = request(http.MethodPut, "/copy-checksum-behavior/source", map[string]string{"x-amz-checksum-sha256": checksum})
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("put source %d", res.StatusCode)
		}
		res = request(http.MethodPut, "/copy-checksum-behavior/destination", map[string]string{"x-amz-copy-source": "copy-checksum-behavior/source"})
		copyResult, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK || !bytes.Contains(copyResult, []byte("<ChecksumSHA256>"+checksum+"</ChecksumSHA256>")) || !bytes.Contains(copyResult, []byte("<ChecksumType>FULL_OBJECT</ChecksumType>")) {
			t.Fatalf("copy result %d %s", res.StatusCode, copyResult)
		}
		var copied struct {
			LastModified string `xml:"LastModified"`
		}
		if err := xml.Unmarshal(copyResult, &copied); err != nil {
			t.Fatal(err)
		}
		if _, err := time.Parse(time.RFC3339, copied.LastModified); err != nil {
			t.Fatalf("copy LastModified %q: %v", copied.LastModified, err)
		}
		res = request(http.MethodHead, "/copy-checksum-behavior/destination", map[string]string{"x-amz-checksum-mode": "ENABLED"})
		res.Body.Close()
		if res.StatusCode != http.StatusOK || res.Header.Get("x-amz-checksum-sha256") != checksum || res.Header.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("copied checksum %d %v", res.StatusCode, res.Header)
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
