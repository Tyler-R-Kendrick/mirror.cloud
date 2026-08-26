package s3_test

import (
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
	"github.com/zeebo/xxh3"
)

func FuzzArchiveRestore(f *testing.F) {
	for class := uint8(0); class < 4; class++ {
		f.Add(class, uint8(class+1))
	}
	classes := []string{"GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "STANDARD"}
	f.Fuzz(func(t *testing.T, classIndex, daysSeed uint8) {
		storageClass := classes[int(classIndex)%len(classes)]
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "archive"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "archive", "Key": "object", "StorageClass": storageClass}, []byte("body"))

		got, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "object"}, nil)
		archived := storageClass == "GLACIER" || storageClass == "DEEP_ARCHIVE"
		if !archived {
			if err != nil {
				t.Fatalf("read %s: %v", storageClass, err)
			}
			_ = got.Stream.Close()
			_, err = invoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "object", "RestoreRequest": map[string]any{"Days": int(daysSeed)%30 + 1}}, nil)
			if fault := asFault(t, err); fault.Code != "InvalidObjectState" || fault.HTTPStatus != http.StatusForbidden {
				t.Fatalf("restore %s = %#v", storageClass, fault)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "InvalidObjectState" || fault.Fields["StorageClass"] != storageClass {
			t.Fatalf("read %s = %#v", storageClass, fault)
		}
		mustInvoke(t, p, "RestoreObject", map[string]any{"Bucket": "archive", "Key": "object", "RestoreRequest": map[string]any{"Days": int(daysSeed)%30 + 1}}, nil)
		got = mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "archive", "Key": "object"}, nil)
		if body := string(readStream(t, got)); body != "body" || got.Headers.Get("x-amz-restore") == "" {
			t.Fatalf("restored %s body=%q headers=%v", storageClass, body, got.Headers)
		}
	})
}

func FuzzStorageClassValidation(f *testing.F) {
	for _, class := range []string{"", "STANDARD", "GLACIER", "EXPRESS_ONEZONE", "OUTPOSTS", "standard", "INVALID"} {
		f.Add(class)
	}
	valid := map[string]bool{"": true, "STANDARD": true, "REDUCED_REDUNDANCY": true, "STANDARD_IA": true, "ONEZONE_IA": true, "INTELLIGENT_TIERING": true, "GLACIER": true, "DEEP_ARCHIVE": true, "GLACIER_IR": true, "SNOW": true, "EXPRESS_ONEZONE": true}
	f.Fuzz(func(t *testing.T, class string) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "classes"}, nil)
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "classes", "Key": "object", "StorageClass": class}, []byte("body"))
		if valid[class] {
			if err != nil {
				t.Fatalf("valid storage class %q: %v", class, err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "InvalidStorageClass" || fault.HTTPStatus != http.StatusBadRequest || fault.Fields["StorageClassRequested"] != class {
			t.Fatalf("invalid storage class %q = %#v", class, fault)
		}
	})
}

func FuzzObjectKeyLength(f *testing.F) {
	for _, key := range []string{"", "key", strings.Repeat("a", 1024), strings.Repeat("é", 512), strings.Repeat("a", 1025), strings.Repeat("é", 513)} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key string) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "keys"}, nil)
		_, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "keys", "Key": key}, []byte("body"))
		if len(key) <= 1024 {
			if err != nil {
				t.Fatalf("valid %d-byte key: %v", len(key), err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "KeyTooLongError" || fault.Fields["Size"] != strconv.Itoa(len(key)) {
			t.Fatalf("invalid %d-byte key = %#v", len(key), fault)
		}
	})
}

func FuzzCreateBucketCollisions(f *testing.F) {
	for _, seed := range []struct{ account, region string }{{"123456789012", "us-east-1"}, {"123456789012", "us-west-2"}, {"999999999999", "us-east-1"}} {
		f.Add(seed.account, seed.region)
	}
	f.Fuzz(func(t *testing.T, account, region string) {
		p := s3.New(spitest.Deps(t))
		owner := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		mustInvokeAs(t, p, owner, "CreateBucket", map[string]any{"Bucket": "global-name"}, nil)
		requestRegion := owner.Region
		input := map[string]any{"Bucket": "global-name"}
		if region != owner.Region {
			requestRegion = "us-west-2"
			input["LocationConstraint"] = requestRegion
		}
		_, err := invokeAs(t, p, spi.Identity{Account: account, Region: requestRegion}, "CreateBucket", input, nil)
		if account == owner.Account && requestRegion == owner.Region {
			if err != nil {
				t.Fatalf("idempotent create: %v", err)
			}
			return
		}
		want := "BucketAlreadyExists"
		if account == owner.Account {
			want = "BucketAlreadyOwnedByYou"
		}
		if fault := asFault(t, err); fault.Code != want || fault.HTTPStatus != http.StatusConflict {
			t.Fatalf("account=%q region=%q want=%s got=%#v", account, region, want, fault)
		}
	})
}

func FuzzDeleteBucketEmptiness(f *testing.F) {
	for _, seed := range []struct {
		versioned bool
		objects   uint8
	}{{false, 0}, {false, 1}, {true, 1}, {true, 3}} {
		f.Add(seed.versioned, seed.objects)
	}
	f.Fuzz(func(t *testing.T, versioned bool, objectSeed uint8) {
		p := s3.New(spitest.Deps(t))
		input := map[string]any{"Bucket": "fuzz-delete"}
		mustInvoke(t, p, "CreateBucket", input, nil)
		if versioned {
			mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "fuzz-delete", "Status": "Enabled"}, nil)
		}
		objects := int(objectSeed % 4)
		for i := 0; i < objects; i++ {
			mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "fuzz-delete", "Key": strconv.Itoa(i)}, []byte("body"))
		}

		response, err := invoke(t, p, "DeleteBucket", input, nil)
		if objects == 0 {
			if err != nil || response.Status != http.StatusNoContent {
				t.Fatalf("empty versioned=%v: %#v %v", versioned, response, err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "BucketNotEmpty" || fault.HTTPStatus != http.StatusConflict {
			t.Fatalf("non-empty versioned=%v objects=%d: %#v", versioned, objects, fault)
		}
	})
}

func FuzzCreateBucketLocations(f *testing.F) {
	for endpoint := uint8(0); endpoint < 4; endpoint++ {
		for constraint := uint8(0); constraint < 6; constraint++ {
			f.Add(endpoint, constraint)
		}
	}
	endpoints := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-east-1"}
	constraints := []string{"", "us-west-2", "eu-west-1", "EU", "ap-east-1", "moon-west-1"}
	f.Fuzz(func(t *testing.T, endpointSeed, constraintSeed uint8) {
		endpoint := endpoints[int(endpointSeed)%len(endpoints)]
		constraint := constraints[int(constraintSeed)%len(constraints)]
		p := s3.New(spitest.Deps(t))
		identity := spi.Identity{Account: "123456789012", Region: endpoint}
		response, err := invokeAs(t, p, identity, "CreateBucket", map[string]any{"Bucket": "location-fuzz", "LocationConstraint": constraint}, nil)

		valid := endpoint == "us-east-1" && constraint != "moon-west-1" || endpoint == "eu-west-1" && (constraint == "EU" || constraint == endpoint) || endpoint != "us-east-1" && endpoint != "eu-west-1" && constraint == endpoint
		if valid {
			if err != nil || response.Status != http.StatusOK {
				t.Fatalf("valid endpoint=%q constraint=%q: %#v %v", endpoint, constraint, response, err)
			}
			bucketRegion := constraint
			if bucketRegion == "" {
				bucketRegion = "us-east-1"
			} else if bucketRegion == "EU" {
				bucketRegion = "eu-west-1"
			}
			location := "/location-fuzz"
			if bucketRegion != "us-east-1" {
				location = "http://location-fuzz.s3.amazonaws.com/"
			}
			if got := response.Headers.Get("Location"); got != location {
				t.Fatalf("endpoint=%q constraint=%q location=%q want=%q", endpoint, constraint, got, location)
			}
			return
		}
		want := "IllegalLocationConstraintException"
		if endpoint == "us-east-1" {
			want = "InvalidLocationConstraint"
		}
		if fault := asFault(t, err); fault.Code != want || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("endpoint=%q constraint=%q want=%s got=%#v", endpoint, constraint, want, fault)
		}
	})
}

func FuzzBucketVersioningState(f *testing.F) {
	for _, status := range []string{"", "Enabled", "Suspended", "Invalid", "enabled"} {
		f.Add(status)
	}
	f.Fuzz(func(t *testing.T, status string) {
		p := s3.New(spitest.Deps(t))
		input := map[string]any{"Bucket": "versioning-fuzz"}
		mustInvoke(t, p, "CreateBucket", input, nil)
		response, err := invoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "versioning-fuzz", "Status": status}, nil)
		if status == "Enabled" || status == "Suspended" {
			if err != nil || response.Status != http.StatusOK {
				t.Fatalf("valid status %q: %#v %v", status, response, err)
			}
			if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output["Status"]; got != status {
				t.Fatalf("stored status %q = %v", status, got)
			}
			return
		}
		want := "MalformedXML"
		if status == "" {
			want = "IllegalVersioningConfigurationException"
		}
		if fault := asFault(t, err); fault.Code != want || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid status %q = %#v", status, fault)
		}
		if got := mustInvoke(t, p, "GetBucketVersioning", input, nil).Output; len(got) != 0 {
			t.Fatalf("invalid status %q persisted: %#v", status, got)
		}
	})
}

func FuzzBucketNames(f *testing.F) {
	for _, name := range []string{"", "ab", "abc", "bucket-name", "example.com", "adjacent..dots", "192.168.5.4", "999.999.999.999", "reserved--table-s3", strings.Repeat("a", 63), strings.Repeat("a", 64)} {
		f.Add(name)
	}
	pattern := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	ipv4 := regexp.MustCompile(`^[0-9]{1,3}(\.[0-9]{1,3}){3}$`)
	prefixes := []string{"xn--", "sthree-", "amzn-s3-demo-"}
	suffixes := []string{"-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3", "-an"}
	f.Fuzz(func(t *testing.T, name string) {
		valid := pattern.MatchString(name) && !strings.Contains(name, "..") && !ipv4.MatchString(name)
		for _, prefix := range prefixes {
			valid = valid && !strings.HasPrefix(name, prefix)
		}
		for _, suffix := range suffixes {
			valid = valid && !strings.HasSuffix(name, suffix)
		}

		p := s3.New(spitest.Deps(t))
		response, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name}, nil)
		if valid {
			if err != nil || response.Status != http.StatusOK {
				t.Fatalf("valid name %q: %#v %v", name, response, err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "InvalidBucketName" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid name %q = %#v", name, fault)
		}
	})
}

func FuzzAccountRegionalBucketNames(f *testing.F) {
	for _, name := range []string{
		"a-123456789012-us-east-1-an",
		"bucket-123456789012-us-east-1-an",
		"bucket-999999999999-us-east-1-an",
		"bucket-123456789012-us-west-2-an",
		"Bucket-123456789012-us-east-1-an",
		strings.Repeat("a", 38) + "-123456789012-us-east-1-an",
	} {
		f.Add(name)
	}
	pattern := regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*-123456789012-us-east-1-an$`)
	prefixes := []string{"xn--", "sthree-", "amzn-s3-demo-"}
	f.Fuzz(func(t *testing.T, name string) {
		valid := len(name) <= 63 && pattern.MatchString(name) && !strings.Contains(name, "..")
		for _, prefix := range prefixes {
			valid = valid && !strings.HasPrefix(name, prefix)
		}

		p := s3.New(spitest.Deps(t))
		response, err := invoke(t, p, "CreateBucket", map[string]any{"Bucket": name, "BucketNamespace": "account-regional"}, nil)
		if valid {
			if err != nil || response.Status != http.StatusOK || response.Headers.Get("Location") != "/"+name {
				t.Fatalf("valid name %q: %#v %v", name, response, err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "InvalidBucketName" || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid name %q = %#v", name, fault)
		}
	})
}

func FuzzXXHashChecksums(f *testing.F) {
	for _, seed := range [][]byte{nil, []byte("123456789"), []byte("arbitrary body\x00bytes")} {
		for algorithm := uint8(0); algorithm < 3; algorithm++ {
			f.Add(seed, algorithm, false)
			f.Add(seed, algorithm, true)
		}
	}
	inputs := []string{"ChecksumXXHASH64", "ChecksumXXHASH3", "ChecksumXXHASH128"}
	headers := []string{"x-amz-checksum-xxhash64", "x-amz-checksum-xxhash3", "x-amz-checksum-xxhash128"}
	f.Fuzz(func(t *testing.T, body []byte, algorithm uint8, corrupt bool) {
		if len(body) > 1<<16 {
			body = body[:1<<16]
		}
		index := int(algorithm % 3)
		var sum []byte
		switch index {
		case 0:
			sum = make([]byte, 8)
			binary.BigEndian.PutUint64(sum, xxhash.Sum64(body))
		case 1:
			sum = make([]byte, 8)
			binary.BigEndian.PutUint64(sum, xxh3.Hash(body))
		case 2:
			value := xxh3.Hash128(body).Bytes()
			sum = value[:]
		}
		checksum := base64.StdEncoding.EncodeToString(sum)
		if corrupt {
			checksum = "AA=="
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "xxhash-fuzz"}, nil)
		response, err := invoke(t, p, "PutObject", map[string]any{"Bucket": "xxhash-fuzz", "Key": "object", inputs[index]: checksum}, body)
		if corrupt {
			if fault := asFault(t, err); fault.Code != "BadDigest" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("corrupt algorithm=%d = %#v", index, fault)
			}
			if _, err := invoke(t, p, "HeadObject", map[string]any{"Bucket": "xxhash-fuzz", "Key": "object"}, nil); err == nil {
				t.Fatal("corrupt checksum created object")
			}
			return
		}
		if err != nil || response.Headers.Get(headers[index]) != checksum {
			t.Fatalf("valid algorithm=%d response=%#v err=%v", index, response, err)
		}
		got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "xxhash-fuzz", "Key": "object", "ChecksumMode": "ENABLED"}, nil)
		if stored := readStream(t, got); string(stored) != string(body) || got.Headers.Get(headers[index]) != checksum {
			t.Fatalf("algorithm=%d body=%q headers=%v", index, stored, got.Headers)
		}
	})
}
