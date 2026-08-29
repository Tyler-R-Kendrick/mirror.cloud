package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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

func FuzzCreateBucketTags(f *testing.F) {
	for _, seed := range []struct {
		key, value string
		duplicate  bool
	}{{"team", "storage", false}, {"duplicate", "one", true}, {"aws:reserved", "value", false}, {"", "value", false}, {"unicode", "東京", false}} {
		f.Add(seed.key, seed.value, seed.duplicate)
	}
	f.Fuzz(func(t *testing.T, key, value string, duplicate bool) {
		p := s3.New(spitest.Deps(t))
		tags := []any{map[string]any{"Key": key, "Value": value}}
		if duplicate {
			tags = append(tags, map[string]any{"Key": key, "Value": "duplicate"})
		}
		input := map[string]any{"Bucket": "tagged-fuzz", "CreateBucketConfiguration": map[string]any{"Tags": tags}}
		_, err := invoke(t, p, "CreateBucket", input, nil)
		if err != nil {
			if fault := asFault(t, err); fault.Code != "InvalidTag" {
				t.Fatalf("key=%q value=%q duplicate=%t: %#v", key, value, duplicate, fault)
			}
			if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "tagged-fuzz"}, nil); asFault(t, err).Code != "NoSuchBucket" {
				t.Fatalf("invalid tags reserved bucket: %v", err)
			}
			return
		}
		response := mustInvoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "tagged-fuzz"}, nil)
		stored := response.Output["TagSet"].([]any)
		if len(stored) != len(tags) || stored[0].(map[string]any)["Key"] != key || stored[0].(map[string]any)["Value"] != value {
			t.Fatalf("stored tags = %#v", stored)
		}
		if _, err := invoke(t, p, "CreateBucket", input, nil); asFault(t, err).Code != "BucketAlreadyOwnedByYou" {
			t.Fatalf("tagged recreation = %v", err)
		}
	})
}

func FuzzCreateBucketObjectOwnership(f *testing.F) {
	for _, seed := range []struct {
		set       bool
		ownership string
	}{{false, ""}, {true, "BucketOwnerPreferred"}, {true, "ObjectWriter"}, {true, "BucketOwnerEnforced"}, {true, ""}, {true, "invalid"}} {
		f.Add(seed.set, seed.ownership)
	}
	f.Fuzz(func(t *testing.T, set bool, ownership string) {
		p := s3.New(spitest.Deps(t))
		input := map[string]any{"Bucket": "ownership-fuzz"}
		if set {
			input["ObjectOwnership"] = ownership
		}
		_, err := invoke(t, p, "CreateBucket", input, nil)
		valid := !set || ownership == "BucketOwnerPreferred" || ownership == "ObjectWriter" || ownership == "BucketOwnerEnforced"
		if !valid {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" {
				t.Fatalf("ownership=%q: %#v", ownership, fault)
			}
			if _, err := invoke(t, p, "HeadBucket", map[string]any{"Bucket": "ownership-fuzz"}, nil); asFault(t, err).Code != "NoSuchBucket" {
				t.Fatalf("invalid ownership reserved bucket: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		want := ownership
		if !set {
			want = "BucketOwnerEnforced"
		}
		response := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-fuzz"}, nil)
		controls := response.Output["OwnershipControls"].(map[string]any)
		rules := controls["Rules"].([]any)
		if len(rules) != 1 || rules[0].(map[string]any)["ObjectOwnership"] != want {
			t.Fatalf("stored ownership = %#v, want %q", response.Output, want)
		}
	})
}

func FuzzBucketOwnershipControls(f *testing.F) {
	for _, seed := range []struct {
		rules     uint8
		ownership string
	}{{1, "BucketOwnerPreferred"}, {1, "ObjectWriter"}, {1, "BucketOwnerEnforced"}, {0, ""}, {2, "ObjectWriter"}, {1, "invalid"}} {
		f.Add(seed.rules, seed.ownership)
	}
	f.Fuzz(func(t *testing.T, ruleSeed uint8, ownership string) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "ownership-controls-fuzz"}, nil)
		rules := make([]any, int(ruleSeed%4))
		for i := range rules {
			rules[i] = map[string]any{"ObjectOwnership": ownership}
		}
		_, err := invoke(t, p, "PutBucketOwnershipControls", map[string]any{
			"Bucket": "ownership-controls-fuzz", "OwnershipControls": map[string]any{"Rules": rules},
		}, nil)
		valid := len(rules) == 1 && (ownership == "BucketOwnerPreferred" || ownership == "ObjectWriter" || ownership == "BucketOwnerEnforced")
		if !valid {
			if fault := asFault(t, err); fault.Code != "MalformedXML" {
				t.Fatalf("rules=%d ownership=%q: %#v", len(rules), ownership, fault)
			}
			ownership = "BucketOwnerEnforced"
		} else if err != nil {
			t.Fatal(err)
		}
		response := mustInvoke(t, p, "GetBucketOwnershipControls", map[string]any{"Bucket": "ownership-controls-fuzz"}, nil)
		stored := asSliceForTest(asMapForTest(response.Output["OwnershipControls"])["Rules"])
		if len(stored) != 1 || asMapForTest(stored[0])["ObjectOwnership"] != ownership {
			t.Fatalf("stored ownership controls = %#v, want %q", response.Output, ownership)
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

func FuzzObjectLockDefaultRetention(f *testing.F) {
	for _, seed := range []struct {
		mode, amount uint8
		years        bool
	}{{0, 1, false}, {1, 7, false}, {0, 1, true}, {1, 5, true}} {
		f.Add(seed.mode, seed.amount, seed.years)
	}
	f.Fuzz(func(t *testing.T, modeSeed, amountSeed uint8, years bool) {
		deps := spitest.Deps(t)
		p := s3.New(deps)
		modes := []string{"GOVERNANCE", "COMPLIANCE"}
		mode := modes[int(modeSeed)%len(modes)]
		amount := int(amountSeed)%30 + 1
		retention := map[string]any{"Mode": mode}
		if years {
			retention["Years"] = amount
		} else {
			retention["Days"] = amount
		}
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "lock-fuzz", "ObjectLockEnabledForBucket": true}, nil)
		mustInvoke(t, p, "PutObjectLockConfiguration", map[string]any{"Bucket": "lock-fuzz", "ObjectLockConfiguration": map[string]any{"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": retention}}}, nil)
		now := deps.Clock.Now().UTC()
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "lock-fuzz", "Key": "key"}, []byte("body"))
		version := put.Headers.Get("x-amz-version-id")
		got := asMapForTest(mustInvoke(t, p, "GetObjectRetention", map[string]any{"Bucket": "lock-fuzz", "Key": "key", "VersionId": version}, nil).Output["Retention"])
		deadline, err := time.Parse(time.RFC3339, got["RetainUntilDate"].(string))
		want := now.AddDate(0, 0, amount)
		if years {
			want = now.AddDate(amount, 0, 0)
		}
		if err != nil || got["Mode"] != mode || !deadline.Equal(want) {
			t.Fatalf("mode=%q amount=%d years=%v retention=%#v", mode, amount, years, got)
		}
		_, err = invoke(t, p, "DeleteObject", map[string]any{"Bucket": "lock-fuzz", "Key": "key", "VersionId": version, "BypassGovernanceRetention": true}, nil)
		if mode == "GOVERNANCE" && err != nil {
			t.Fatalf("governance bypass: %v", err)
		}
		if mode == "COMPLIANCE" && asFault(t, err).Code != "AccessDenied" {
			t.Fatalf("compliance bypass: %v", err)
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

func FuzzListBucketsPagination(f *testing.F) {
	for _, seed := range []struct {
		max                   int64
		prefix, token, region string
		setToken, setRegion   bool
	}{
		{1, "team-", "", "", false, false},
		{2, "team-", base64.URLEncoding.EncodeToString([]byte("team-alpha")), "", true, false},
		{10000, "team-", "", "us-west-2", false, true},
		{0, "", "", "", false, false},
		{10001, "", "", "", false, false},
		{1, "", "!", "", true, false},
	} {
		f.Add(seed.max, seed.prefix, seed.token, seed.region, seed.setToken, seed.setRegion)
	}
	f.Fuzz(func(t *testing.T, max int64, prefix, token, region string, setToken, setRegion bool) {
		if len(prefix) > 64 {
			prefix = prefix[:64]
		}
		if len(token) > 1025 {
			token = token[:1025]
		}
		if len(region) > 64 {
			region = region[:64]
		}
		p := s3.New(spitest.Deps(t))
		east := spi.Identity{Account: "123456789012", Region: "us-east-1"}
		west := spi.Identity{Account: east.Account, Region: "us-west-2"}
		other := spi.Identity{Account: "999999999999", Region: east.Region}
		for _, bucket := range []struct {
			id   spi.Identity
			name string
		}{
			{east, "alpha-bucket"},
			{east, "team-alpha"},
			{west, "team-beta"},
			{west, "team-charlie"},
			{other, "team-private"},
		} {
			input := map[string]any{"Bucket": bucket.name}
			if bucket.id.Region != "us-east-1" {
				input["LocationConstraint"] = bucket.id.Region
			}
			mustInvokeAs(t, p, bucket.id, "CreateBucket", input, nil)
		}

		input := map[string]any{"MaxBuckets": max, "Prefix": prefix}
		if setToken {
			input["ContinuationToken"] = token
		}
		if setRegion {
			input["BucketRegion"] = region
		}
		page, err := invokeAs(t, p, east, "ListBuckets", input, nil)
		if max < 1 || max > 10000 {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("max %d = %#v", max, fault)
			}
			return
		}
		after := ""
		if setToken && token != "" {
			decoded, decodeErr := base64.URLEncoding.DecodeString(token)
			if len(token) > 1024 || decodeErr != nil {
				if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
					t.Fatalf("token %q = %#v", token, fault)
				}
				return
			}
			after = string(decoded)
		}
		if err != nil {
			t.Fatal(err)
		}

		type bucket struct{ name, region string }
		all := []bucket{{"alpha-bucket", "us-east-1"}, {"team-alpha", "us-east-1"}, {"team-beta", "us-west-2"}, {"team-charlie", "us-west-2"}}
		var want []bucket
		for _, bucket := range all {
			if strings.HasPrefix(bucket.name, prefix) && bucket.name > after && (!setRegion || bucket.region == region) {
				want = append(want, bucket)
			}
		}
		wantToken := ""
		if len(want) > int(max) {
			want = want[:int(max)]
			wantToken = base64.URLEncoding.EncodeToString([]byte(want[len(want)-1].name))
		}
		items := page.Output["Buckets"].([]any)
		if len(items) != len(want) || page.Output["Prefix"] != prefix {
			t.Fatalf("page=%#v want=%#v", page.Output, want)
		}
		for i, item := range items {
			got := item.(map[string]any)
			if got["Name"] != want[i].name || got["BucketRegion"] != want[i].region {
				t.Fatalf("bucket %d = %#v want=%#v", i, got, want[i])
			}
		}
		gotToken, _ := page.Output["ContinuationToken"].(string)
		if gotToken != wantToken {
			t.Fatalf("token = %q want %q", gotToken, wantToken)
		}
	})
}

func FuzzDeleteObjectVersionRestoration(f *testing.F) {
	f.Add("first", "second", "third", uint8(2))
	f.Add("", "same", "same", uint8(1))
	f.Fuzz(func(t *testing.T, first, second, third string, selected uint8) {
		if len(first)+len(second)+len(third) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-fuzz", "Status": "Enabled"}, nil)
		versions := make([]string, 0, 3)
		for _, body := range []string{first, second, third} {
			put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-fuzz", "Key": "key"}, []byte(body))
			versions = append(versions, put.Headers.Get("x-amz-version-id"))
		}
		index := int(selected % 3)
		mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "version-fuzz", "Key": "key", "VersionId": versions[index]}, nil)
		restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "version-fuzz", "Key": "key"}, nil)
		want := third
		if index == 2 {
			want = second
		}
		if got := string(readStream(t, restored)); got != want {
			t.Fatalf("deleted=%d body=%q want=%q", index, got, want)
		}
	})
}

func FuzzDeleteObjectsVersionSemantics(f *testing.F) {
	f.Add("key", "first", "second", false)
	f.Add("nested/key", "", "same", true)
	f.Fuzz(func(t *testing.T, key, first, second string, quiet bool) {
		if key == "" || len([]byte(key)) > 1024 || len(first)+len(second) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multi-delete-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "multi-delete-fuzz", "Status": "Enabled"}, nil)
		firstPut := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "multi-delete-fuzz", "Key": key}, []byte(first))
		secondPut := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "multi-delete-fuzz", "Key": key}, []byte(second))
		result := mustInvoke(t, p, "DeleteObjects", map[string]any{"Bucket": "multi-delete-fuzz", "Quiet": quiet, "Objects": []any{
			map[string]any{"Key": key, "VersionId": secondPut.Headers.Get("x-amz-version-id")},
			map[string]any{"Key": key, "VersionId": "missing"},
		}}, nil)
		failures, _ := result.Output["Errors"].([]any)
		if len(failures) != 1 || failures[0].(map[string]any)["Code"] != "NoSuchVersion" {
			t.Fatalf("errors %#v", result.Output)
		}
		deleted, _ := result.Output["Deleted"].([]any)
		if (quiet && deleted != nil) || (!quiet && (len(deleted) != 1 || deleted[0].(map[string]any)["VersionId"] != secondPut.Headers.Get("x-amz-version-id"))) {
			t.Fatalf("quiet=%v deleted %#v", quiet, result.Output)
		}
		restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "multi-delete-fuzz", "Key": key}, nil)
		if body := string(readStream(t, restored)); body != first || restored.Headers.Get("x-amz-version-id") != firstPut.Headers.Get("x-amz-version-id") {
			t.Fatalf("restored body=%q headers=%v", body, restored.Headers)
		}
	})
}

func FuzzReplicationVersions(f *testing.F) {
	f.Add("key", "first", "second", true)
	f.Add("nested/key", "", "same", false)
	f.Fuzz(func(t *testing.T, suffix, first, second string, marker bool) {
		key := "replica/" + suffix
		if len([]byte(key)) > 1024 || len(first)+len(second) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		for _, bucket := range []string{"replication-fuzz-source", "replication-fuzz-destination"} {
			mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
			mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}, nil)
		}
		mustInvoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "replication-fuzz-source", "ReplicationConfiguration": map[string]any{"Role": "arn:aws:iam::000000000000:role/replication", "Rules": []any{map[string]any{
			"Priority": 1, "Status": "Enabled", "Filter": map[string]any{"Prefix": "replica/"}, "DeleteMarkerReplication": map[string]any{"Status": "Enabled"}, "Destination": map[string]any{"Bucket": "replication-fuzz-destination"},
		}}}}, nil)

		var currentVersion string
		for _, body := range []string{first, second} {
			put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "replication-fuzz-source", "Key": key}, []byte(body))
			currentVersion = put.Headers.Get("x-amz-version-id")
			replica := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "replication-fuzz-destination", "Key": key, "VersionId": currentVersion}, nil)
			if got := string(readStream(t, replica)); got != body || replica.Headers.Get("x-amz-version-id") != currentVersion {
				t.Fatalf("version=%s body=%q want=%q", currentVersion, got, body)
			}
		}
		if !marker {
			return
		}
		deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "replication-fuzz-source", "Key": key}, nil)
		markerVersion := deleted.Headers.Get("x-amz-version-id")
		_, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "replication-fuzz-destination", "Key": key, "VersionId": markerVersion}, nil)
		if fault := asFault(t, err); fault.Code != "MethodNotAllowed" {
			t.Fatalf("delete marker version=%s fault=%#v", markerVersion, fault)
		}
		mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "replication-fuzz-destination", "Key": key, "VersionId": markerVersion}, nil)
		restored := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "replication-fuzz-destination", "Key": key}, nil)
		if got := string(readStream(t, restored)); got != second || restored.Headers.Get("x-amz-version-id") != currentVersion {
			t.Fatalf("restored body=%q version=%s want=%q %s", got, restored.Headers.Get("x-amz-version-id"), second, currentVersion)
		}
	})
}

func FuzzReplicationConfigurationValidation(f *testing.F) {
	f.Add("role", "Enabled", "destination", "Disabled", true, true)
	f.Add("", "Enabled", "destination", "Disabled", false, false)
	f.Add("role", "Pending", "destination", "Disabled", false, false)
	f.Add("role", "Disabled", "destination", "Pending", true, false)
	f.Add("role", "Enabled", "destination", "Enabled", true, true)
	f.Fuzz(func(t *testing.T, role, status, destination, deleteStatus string, filter, tag bool) {
		p := s3.New(spitest.Deps(t))
		for _, bucket := range []string{"replication-validation-fuzz", "replication-validation-destination"} {
			mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
			mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}, nil)
		}
		target := ""
		if destination != "" {
			target = "replication-validation-destination"
		}
		rule := map[string]any{"Status": status, "Destination": map[string]any{"Bucket": target}}
		if filter {
			rule["Priority"] = 1
			rule["DeleteMarkerReplication"] = map[string]any{"Status": deleteStatus}
			rule["Filter"] = map[string]any{"Prefix": "logs/"}
			if tag {
				rule["Filter"] = map[string]any{"Tag": map[string]any{"Key": "environment", "Value": "test"}}
			}
		}
		_, err := invoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "replication-validation-fuzz", "ReplicationConfiguration": map[string]any{"Role": role, "Rules": []any{rule}}}, nil)
		wantCode := ""
		if role == "" || destination == "" || status != "Enabled" && status != "Disabled" || filter && deleteStatus != "Enabled" && deleteStatus != "Disabled" {
			wantCode = "MalformedXML"
		} else if filter && tag && deleteStatus == "Enabled" {
			wantCode = "InvalidRequest"
		}
		if wantCode == "" {
			if err != nil {
				t.Fatalf("valid configuration: %v", err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != wantCode || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("fault=%+v want=%s/400", fault, wantCode)
		}
	})
}

func FuzzReplicationDestinations(f *testing.F) {
	f.Add(false, false)
	f.Add(true, false)
	f.Add(true, true)
	f.Fuzz(func(t *testing.T, exists, versioned bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "replication-destination-source"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "replication-destination-source", "Status": "Enabled"}, nil)
		if exists {
			mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "replication-destination-target"}, nil)
			if versioned {
				mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "replication-destination-target", "Status": "Enabled"}, nil)
			}
		}
		configuration := map[string]any{
			"Role": "role", "Rules": []any{map[string]any{"Status": "Enabled", "Destination": map[string]any{"Bucket": "replication-destination-target"}}},
		}
		_, err := invoke(t, p, "PutBucketReplication", map[string]any{"Bucket": "replication-destination-source", "ReplicationConfiguration": configuration}, nil)
		if !exists || !versioned {
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault=%+v", fault)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		rule := configuration["Rules"].([]any)[0].(map[string]any)
		if id, _ := rule["ID"].(string); len(id) != 8 {
			t.Fatalf("generated rule ID %q", id)
		}
	})
}

func FuzzPostObjectMultipart(f *testing.F) {
	f.Add("file.txt", "body", true)
	f.Add("", "", false)
	f.Fuzz(func(t *testing.T, filename, body string, created bool) {
		if len(filename) > 512 || len(body) > 4096 || !utf8.ValidString(filename) || strings.IndexFunc(filename, func(r rune) bool { return r < ' ' || r == 0x7f }) >= 0 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-fuzz"}, nil)
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "fuzz/${filename}")
		if created {
			_ = writer.WriteField("success_action_status", "201")
		}
		file, err := writer.CreateFormFile("file", filename)
		if err != nil {
			t.Skip()
		}
		_, _ = io.WriteString(file, body)
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-fuzz", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-fuzz"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		if err != nil {
			t.Fatal(err)
		}
		wantStatus := http.StatusNoContent
		if created {
			wantStatus = http.StatusCreated
		}
		if response.Status != wantStatus {
			t.Fatalf("status=%d want=%d", response.Status, wantStatus)
		}
		effectiveFilename := filename
		if effectiveFilename != "" {
			effectiveFilename = filepath.Base(effectiveFilename)
		}
		got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "post-fuzz", "Key": "fuzz/" + effectiveFilename}, nil)
		if stored := string(readStream(t, got)); stored != body {
			t.Fatalf("body=%q want=%q", stored, body)
		}
	})
}

func FuzzPostObjectPolicy(f *testing.F) {
	f.Add("", "body")
	f.Add(base64.StdEncoding.EncodeToString([]byte(`{"expiration":"2099-01-01T00:00:00Z","conditions":[["content-length-range",1,8]]}`)), "body")
	f.Add(base64.StdEncoding.EncodeToString([]byte(`{"expiration":"1960-01-01T00:00:00Z","conditions":[]}`)), "body")
	f.Add("not-base64", "body")
	f.Fuzz(func(t *testing.T, policy, body string) {
		if len(policy) > 8192 || len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-policy-fuzz"}, nil)
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "fuzz-policy")
		if policy != "" {
			_ = writer.WriteField("policy", policy)
			_ = writer.WriteField("x-amz-algorithm", "AWS4-HMAC-SHA256")
			_ = writer.WriteField("x-amz-credential", "test/20260827/us-east-1/s3/aws4_request")
			_ = writer.WriteField("x-amz-date", "20260827T000000Z")
			_ = writer.WriteField("x-amz-signature", "signature")
		}
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = io.WriteString(file, body)
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-policy-fuzz", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-policy-fuzz"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		got, getErr := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-policy-fuzz", "Key": "fuzz-policy"}, nil)
		if err != nil {
			if fault := asFault(t, getErr); fault.Code != "NoSuchKey" {
				t.Fatalf("rejected policy left object: %+v", fault)
			}
			return
		}
		if getErr != nil || string(readStream(t, got)) != body {
			t.Fatalf("accepted policy did not preserve body: %v", getErr)
		}
	})
}

func FuzzPostObjectTagging(f *testing.F) {
	f.Add("<Tagging><TagSet><Tag><Key>key</Key><Value>value</Value></Tag></TagSet></Tagging>")
	f.Add("<InvalidXmlTagging></InvalidXmlTagging>")
	f.Add("not-xml")
	f.Add("")
	f.Fuzz(func(t *testing.T, tagging string) {
		if len(tagging) > 8192 || !utf8.ValidString(tagging) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-tag-fuzz"}, nil)
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "fuzz-tags")
		_ = writer.WriteField("tagging", tagging)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-tag-fuzz", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-tag-fuzz"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		_, getErr := invoke(t, p, "GetObjectTagging", map[string]any{"Bucket": "post-tag-fuzz", "Key": "fuzz-tags"}, nil)
		if err != nil {
			if fault := asFault(t, getErr); fault.Code != "NoSuchKey" {
				t.Fatalf("rejected tagging left object: %+v", fault)
			}
			return
		}
		if getErr != nil {
			t.Fatalf("accepted tagging lost object: %v", getErr)
		}
	})
}

func FuzzPostObjectExpires(f *testing.F) {
	f.Add("Thu, 27 Aug 2026 12:00:00 GMT")
	f.Add("tomorrow")
	f.Add("")
	f.Fuzz(func(t *testing.T, expires string) {
		if len(expires) > 512 || !utf8.ValidString(expires) || strings.IndexFunc(expires, func(r rune) bool { return r < ' ' || r == 0x7f }) >= 0 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-expires-fuzz"}, nil)
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "fuzz-expires")
		_ = writer.WriteField("Expires", expires)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = file.Write([]byte("body"))
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-expires-fuzz", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		_, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-expires-fuzz"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		_, parseErr := time.Parse(http.TimeFormat, expires)
		if expires != "" && parseErr != nil {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" {
				t.Fatalf("fault = %+v", fault)
			}
			if _, getErr := invoke(t, p, "GetObject", map[string]any{"Bucket": "post-expires-fuzz", "Key": "fuzz-expires"}, nil); asFault(t, getErr).Code != "NoSuchKey" {
				t.Fatal("invalid Expires stored object")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if got := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "post-expires-fuzz", "Key": "fuzz-expires"}, nil).Headers.Get("Expires"); got != expires {
			t.Fatalf("Expires = %q, want %q", got, expires)
		}
	})
}

func FuzzPostObjectChecksums(f *testing.F) {
	f.Add(uint8(0), "body")
	f.Add(uint8(4), "")
	f.Fuzz(func(t *testing.T, algorithmIndex uint8, body string) {
		if len(body) > 4096 {
			t.Skip()
		}
		algorithms := []string{"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"}
		algorithm := algorithms[int(algorithmIndex)%len(algorithms)]
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "post-checksum-fuzz"}, nil)
		var payload bytes.Buffer
		writer := multipart.NewWriter(&payload)
		_ = writer.WriteField("key", "fuzz-checksum")
		_ = writer.WriteField("x-amz-checksum-algorithm", algorithm)
		file, _ := writer.CreateFormFile("file", "file.txt")
		_, _ = io.WriteString(file, body)
		_ = writer.Close()
		httpRequest := httptest.NewRequest(http.MethodPost, "http://s3.test/post-checksum-fuzz", &payload)
		httpRequest.Header.Set("Content-Type", writer.FormDataContentType())
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "PostObject", Input: map[string]any{"Bucket": "post-checksum-fuzz"}, Identity: ident(), Body: httpRequest.Body, HTTP: httpRequest})
		if err != nil {
			t.Fatal(err)
		}
		header := "x-amz-checksum-" + strings.ToLower(algorithm)
		checksum := response.Headers.Get(header)
		if checksum == "" || response.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("headers = %v", response.Headers)
		}
		head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "post-checksum-fuzz", "Key": "fuzz-checksum", "ChecksumMode": "ENABLED"}, nil)
		if head.Headers.Get(header) != checksum {
			t.Fatalf("stored checksum = %q, want %q", head.Headers.Get(header), checksum)
		}
	})
}

func FuzzObjectServerSideEncryption(f *testing.F) {
	f.Add(uint8(0), false, "body")
	f.Add(uint8(1), true, "kms")
	f.Add(uint8(2), false, "dsse")
	f.Add(uint8(3), true, "invalid")
	f.Fuzz(func(t *testing.T, algorithmIndex uint8, bucketKey bool, body string) {
		if len(body) > 4096 {
			t.Skip()
		}
		algorithms := []string{"AES256", "aws:kms", "aws:kms:dsse", "invalid"}
		algorithm := algorithms[int(algorithmIndex)%len(algorithms)]
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "encryption-fuzz"}, nil)
		input := map[string]any{"Bucket": "encryption-fuzz", "Key": "object", "ServerSideEncryption": algorithm, "BucketKeyEnabled": bucketKey}
		keyID := "arn:aws:kms:us-east-1:123456789012:key/fuzz"
		if algorithm == "aws:kms" {
			input["SSEKMSKeyId"] = keyID
		}
		response, err := invoke(t, p, "PutObject", input, []byte(body))
		if algorithm == "invalid" {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %+v", fault)
			}
			if _, getErr := invoke(t, p, "GetObject", map[string]any{"Bucket": "encryption-fuzz", "Key": "object"}, nil); asFault(t, getErr).Code != "NoSuchKey" {
				t.Fatal("invalid encryption stored object")
			}
			return
		}
		if err != nil || response.Headers.Get("x-amz-server-side-encryption") != algorithm {
			t.Fatalf("put %v headers=%v", err, response.Headers)
		}
		get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "encryption-fuzz", "Key": "object"}, nil)
		if get.Headers.Get("x-amz-server-side-encryption") != algorithm || string(readStream(t, get)) != body {
			t.Fatalf("stored encryption headers=%v", get.Headers)
		}
		if algorithm == "aws:kms" && (get.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || bucketKey && get.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true") {
			t.Fatalf("stored kms headers=%v", get.Headers)
		}
	})
}

func FuzzObjectSSECustomerKey(f *testing.F) {
	f.Add(uint8(0), []byte("0123456789abcdef0123456789abcdef"), "body")
	f.Add(uint8(1), []byte("short"), "short")
	f.Add(uint8(2), []byte("algorithm"), "invalid")
	f.Add(uint8(3), []byte("digest"), "mismatch")
	f.Add(uint8(4), []byte("mixed"), "incompatible")
	f.Add(uint8(5), []byte("base64"), "malformed")
	f.Fuzz(func(t *testing.T, kind uint8, key []byte, body string) {
		if len(key) > 4096 || len(body) > 4096 {
			t.Skip()
		}
		mode := kind % 6
		rawKey := key
		if mode != 1 {
			normalized := sha256.Sum256(key)
			rawKey = normalized[:]
		}
		digest := md5.Sum(rawKey)
		input := map[string]any{"Bucket": "sse-c-fuzz", "Key": "object", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(rawKey), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(digest[:])}
		valid := mode == 0 || mode == 1 && len(rawKey) == 32
		switch mode {
		case 2:
			input["SSECustomerAlgorithm"] = "AES128"
		case 3:
			digest[0] ^= 0xff
			input["SSECustomerKeyMD5"] = base64.StdEncoding.EncodeToString(digest[:])
		case 4:
			input["ServerSideEncryption"] = "AES256"
		case 5:
			input["SSECustomerKey"] = "*" + string(key)
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "sse-c-fuzz"}, nil)
		response, err := invoke(t, p, "PutObject", input, []byte(body))
		if !valid {
			if fault := asFault(t, err); fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %+v", fault)
			}
			if _, getErr := invoke(t, p, "GetObject", map[string]any{"Bucket": "sse-c-fuzz", "Key": "object"}, nil); asFault(t, getErr).Code != "NoSuchKey" {
				t.Fatal("invalid customer encryption stored object")
			}
			return
		}
		if err != nil || response.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != input["SSECustomerKeyMD5"] {
			t.Fatalf("put %v headers=%v", err, response.Headers)
		}
		get := mustInvoke(t, p, "GetObject", input, nil)
		if string(readStream(t, get)) != body || get.Headers.Get("x-amz-server-side-encryption-customer-algorithm") != "AES256" {
			t.Fatalf("stored customer encryption headers=%v", get.Headers)
		}
	})
}

func FuzzMultipartServerSideEncryption(f *testing.F) {
	f.Add(uint8(0), false, "body")
	f.Add(uint8(1), true, "kms")
	f.Add(uint8(2), false, "dsse")
	f.Add(uint8(3), true, "invalid")
	f.Fuzz(func(t *testing.T, algorithmIndex uint8, bucketKey bool, body string) {
		if len(body) > 4096 {
			t.Skip()
		}
		algorithms := []string{"AES256", "aws:kms", "aws:kms:dsse", "invalid"}
		algorithm := algorithms[int(algorithmIndex)%len(algorithms)]
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-encryption-fuzz"}, nil)
		input := map[string]any{"Bucket": "multipart-encryption-fuzz", "Key": "object", "ServerSideEncryption": algorithm, "BucketKeyEnabled": bucketKey}
		keyID := "arn:aws:kms:us-east-1:123456789012:key/multipart-fuzz"
		if algorithm == "aws:kms" {
			input["SSEKMSKeyId"] = keyID
		}
		created, err := invoke(t, p, "CreateMultipartUpload", input, nil)
		if algorithm == "invalid" {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.HTTPStatus != http.StatusBadRequest {
				t.Fatalf("fault = %+v", fault)
			}
			return
		}
		if err != nil || created.Headers.Get("x-amz-server-side-encryption") != algorithm {
			t.Fatalf("create %v headers=%v", err, created.Headers)
		}
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": uploadID, "PartNumber": 1}, []byte(body))
		if part.Headers.Get("x-amz-server-side-encryption") != algorithm {
			t.Fatalf("part headers=%v", part.Headers)
		}
		aes := map[string]any{"Rules": []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "AES256"}}}}
		mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": "multipart-encryption-fuzz", "ServerSideEncryptionConfiguration": aes}, nil)
		completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
		if completed.Headers.Get("x-amz-server-side-encryption") != algorithm {
			t.Fatalf("complete headers=%v", completed.Headers)
		}
		get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "multipart-encryption-fuzz", "Key": "object"}, nil)
		if get.Headers.Get("x-amz-server-side-encryption") != algorithm || string(readStream(t, get)) != body {
			t.Fatalf("stored multipart headers=%v", get.Headers)
		}
		if algorithm == "aws:kms" && (get.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || bucketKey && get.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true") {
			t.Fatalf("stored kms headers=%v", get.Headers)
		}
	})
}

func FuzzMultipartSSECustomerKey(f *testing.F) {
	f.Add(uint8(0), []byte("key"), "body")
	f.Add(uint8(1), []byte("digest-only"), "md5")
	f.Add(uint8(2), []byte("missing"), "reject")
	f.Add(uint8(3), []byte("mismatch"), "reject")
	f.Fuzz(func(t *testing.T, kind uint8, key []byte, body string) {
		if len(key) > 4096 || len(body) > 4096 {
			t.Skip()
		}
		rawKey := sha256.Sum256(key)
		digest := md5.Sum(rawKey[:])
		key64, keyMD5 := base64.StdEncoding.EncodeToString(rawKey[:]), base64.StdEncoding.EncodeToString(digest[:])
		encryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": key64, "SSECustomerKeyMD5": keyMD5}
		create := map[string]any{"Bucket": "multipart-sse-c-fuzz", "Key": "object", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": key64, "SSECustomerKeyMD5": keyMD5}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-sse-c-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", create, nil)
		uploadID := created.Output["UploadId"].(string)
		partInput := map[string]any{"UploadId": uploadID, "PartNumber": 1}
		mode := kind % 4
		switch mode {
		case 0:
			for name, value := range encryption {
				partInput[name] = value
			}
		case 1:
			partInput["SSECustomerKeyMD5"] = keyMD5
		case 3:
			digest[0] ^= 0xff
			partInput["SSECustomerKeyMD5"] = base64.StdEncoding.EncodeToString(digest[:])
		}
		part, err := invoke(t, p, "UploadPart", partInput, []byte(body))
		if mode >= 2 {
			if fault := asFault(t, err); fault.Code != "InvalidRequest" {
				t.Fatalf("part fault = %+v", fault)
			}
			return
		}
		if err != nil || part.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("part %v headers=%v", err, part.Headers)
		}
		completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
		if completed.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != keyMD5 {
			t.Fatalf("complete headers=%v", completed.Headers)
		}
		get := mustInvoke(t, p, "GetObject", create, nil)
		if string(readStream(t, get)) != body {
			t.Fatal("multipart customer body mismatch")
		}
	})
}

func FuzzCopyObjectSSECustomerKeys(f *testing.F) {
	f.Add(uint8(0), []byte("source"), []byte("destination"), "body")
	f.Add(uint8(1), []byte("missing"), []byte("destination"), "reject")
	f.Add(uint8(2), []byte("mismatch"), []byte("destination"), "reject")
	f.Add(uint8(3), []byte("plain"), []byte("destination"), "plain")
	f.Add(uint8(4), []byte("digest-only"), []byte("destination"), "md5")
	f.Fuzz(func(t *testing.T, kind uint8, sourceSeed, destinationSeed []byte, body string) {
		if len(sourceSeed) > 4096 || len(destinationSeed) > 4096 || len(body) > 4096 {
			t.Skip()
		}
		sourceKey, destinationKey := sha256.Sum256(sourceSeed), sha256.Sum256(destinationSeed)
		sourceDigest, destinationDigest := md5.Sum(sourceKey[:]), md5.Sum(destinationKey[:])
		sourceKey64, sourceMD5 := base64.StdEncoding.EncodeToString(sourceKey[:]), base64.StdEncoding.EncodeToString(sourceDigest[:])
		destinationKey64, destinationMD5 := base64.StdEncoding.EncodeToString(destinationKey[:]), base64.StdEncoding.EncodeToString(destinationDigest[:])
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "copy-sse-c-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "copy-sse-c-fuzz", "Key": "source", "SSECustomerAlgorithm": "AES256", "SSECustomerKey": sourceKey64, "SSECustomerKeyMD5": sourceMD5}, []byte(body))
		input := map[string]any{"Bucket": "copy-sse-c-fuzz", "Key": "destination", "CopySource": "copy-sse-c-fuzz/source", "CopySourceSSECustomerAlgorithm": "AES256", "CopySourceSSECustomerKey": sourceKey64, "CopySourceSSECustomerKeyMD5": sourceMD5, "SSECustomerAlgorithm": "AES256", "SSECustomerKey": destinationKey64, "SSECustomerKeyMD5": destinationMD5}
		mode := kind % 5
		switch mode {
		case 1:
			delete(input, "CopySourceSSECustomerAlgorithm")
			delete(input, "CopySourceSSECustomerKey")
			delete(input, "CopySourceSSECustomerKeyMD5")
		case 2:
			sourceDigest[0] ^= 0xff
			input["CopySourceSSECustomerKeyMD5"] = base64.StdEncoding.EncodeToString(sourceDigest[:])
		case 3:
			delete(input, "SSECustomerAlgorithm")
			delete(input, "SSECustomerKey")
			delete(input, "SSECustomerKeyMD5")
		case 4:
			delete(input, "CopySourceSSECustomerAlgorithm")
			delete(input, "CopySourceSSECustomerKey")
		}
		copied, err := invoke(t, p, "CopyObject", input, nil)
		if mode == 1 || mode == 2 {
			if fault := asFault(t, err); fault.Code != "InvalidRequest" && fault.Code != "InvalidArgument" {
				t.Fatalf("copy fault = %+v", fault)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		read := map[string]any{"Bucket": "copy-sse-c-fuzz", "Key": "destination"}
		if mode != 3 {
			read["SSECustomerAlgorithm"], read["SSECustomerKey"], read["SSECustomerKeyMD5"] = "AES256", destinationKey64, destinationMD5
			if copied.Headers.Get("x-amz-server-side-encryption-customer-key-MD5") != destinationMD5 {
				t.Fatalf("copy headers=%v", copied.Headers)
			}
		}
		if got := mustInvoke(t, p, "GetObject", read, nil); string(readStream(t, got)) != body {
			t.Fatal("copied customer body mismatch")
		}
	})
}
