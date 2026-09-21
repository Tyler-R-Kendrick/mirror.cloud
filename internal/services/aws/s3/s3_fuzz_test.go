package s3_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
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
		empty      bool
	}{{"team", "storage", false, false}, {"duplicate", "one", true, false}, {"aws:reserved", "value", false, false}, {"", "value", false, false}, {"unicode", "東京", false, false}, {"", "", false, true}} {
		f.Add(seed.key, seed.value, seed.duplicate, seed.empty)
	}
	f.Fuzz(func(t *testing.T, key, value string, duplicate, empty bool) {
		p := s3.New(spitest.Deps(t))
		tags := []any{map[string]any{"Key": key, "Value": value}}
		if empty {
			tags = []any{}
		} else if duplicate {
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
		if len(tags) == 0 {
			if _, err := invoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "tagged-fuzz"}, nil); asFault(t, err).Code != "NoSuchTagSet" {
				t.Fatalf("empty create tags = %v", err)
			}
			mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "tagged-fuzz"}, nil)
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
		mustInvoke(t, p, "PutBucketTagging", map[string]any{"Bucket": "tagged-fuzz", "TagSet": []any{}}, nil)
		if _, err := invoke(t, p, "GetBucketTagging", map[string]any{"Bucket": "tagged-fuzz"}, nil); asFault(t, err).Code != "NoSuchTagSet" {
			t.Fatalf("empty put tags = %v", err)
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
			if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Message != "Invalid x-amz-object-ownership header: "+ownership ||
				fault.HTTPStatus != http.StatusBadRequest || len(fault.Fields) != 1 || fault.Fields["ArgumentName"] != "x-amz-object-ownership" {
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

func FuzzPublicAccessBlock(f *testing.F) {
	for _, seed := range []struct {
		mode uint8
		flag bool
		text string
	}{{0, false, ""}, {1, true, ""}, {2, false, ""}, {3, true, "unknown"}, {4, false, "true"}, {5, false, ""}} {
		f.Add(seed.mode, seed.flag, seed.text)
	}
	f.Fuzz(func(t *testing.T, mode uint8, flag bool, text string) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "public-access-block-fuzz"}, nil)
		defaults := asMapForTest(mustInvoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block-fuzz"}, nil).Output["PublicAccessBlockConfiguration"])
		for _, field := range []string{"BlockPublicAcls", "BlockPublicPolicy", "IgnorePublicAcls", "RestrictPublicBuckets"} {
			if defaults[field] != true {
				t.Fatalf("default %s = %#v", field, defaults[field])
			}
		}
		baseline := map[string]any{"BlockPublicPolicy": true}
		mustInvoke(t, p, "PutPublicAccessBlock", map[string]any{"Bucket": "public-access-block-fuzz", "PublicAccessBlockConfiguration": baseline}, nil)
		var configuration any
		valid := true
		switch mode % 6 {
		case 0:
			configuration = map[string]any{}
		case 1:
			configuration = map[string]any{"BlockPublicAcls": flag}
		case 2:
			configuration = map[string]any{"BlockPublicAcls": flag, "BlockPublicPolicy": !flag, "IgnorePublicAcls": flag, "RestrictPublicBuckets": !flag}
		case 3:
			configuration, valid = map[string]any{"Unknown": flag}, false
		case 4:
			configuration, valid = map[string]any{"BlockPublicAcls": text}, false
		case 5:
			valid = false
		}
		_, err := invoke(t, p, "PutPublicAccessBlock", map[string]any{"Bucket": "public-access-block-fuzz", "PublicAccessBlockConfiguration": configuration}, nil)
		if !valid {
			if fault := asFault(t, err); fault.Code != "MalformedXML" {
				t.Fatalf("mode=%d configuration=%#v: %#v", mode%6, configuration, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		response := mustInvoke(t, p, "GetPublicAccessBlock", map[string]any{"Bucket": "public-access-block-fuzz"}, nil)
		stored := asMapForTest(response.Output["PublicAccessBlockConfiguration"])
		if len(stored) != 4 {
			t.Fatalf("stored configuration = %#v", stored)
		}
		if !valid && stored["BlockPublicPolicy"] != true || valid && mode%6 == 1 && stored["BlockPublicAcls"] != flag {
			t.Fatalf("stored configuration = %#v", stored)
		}
	})
}

func FuzzBucketRequestPayment(f *testing.F) {
	for _, payer := range []string{"Requester", "BucketOwner", "", "Invalid", "requester"} {
		f.Add(payer)
	}
	f.Fuzz(func(t *testing.T, payer string) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "request-payment-fuzz"}, nil)
		_, err := invoke(t, p, "PutBucketRequestPayment", map[string]any{"Bucket": "request-payment-fuzz", "RequestPaymentConfiguration": map[string]any{"Payer": payer}}, nil)
		valid := payer == "Requester" || payer == "BucketOwner"
		if !valid {
			if fault := asFault(t, err); fault.Code != "MalformedXML" {
				t.Fatalf("payer=%q: %#v", payer, fault)
			}
			payer = "BucketOwner"
		} else if err != nil {
			t.Fatal(err)
		}
		response := mustInvoke(t, p, "GetBucketRequestPayment", map[string]any{"Bucket": "request-payment-fuzz"}, nil)
		if response.Output["Payer"] != payer {
			t.Fatalf("stored payer = %#v, want %q", response.Output, payer)
		}
	})
}

func FuzzBucketAccelerateConfiguration(f *testing.F) {
	for _, seed := range []struct {
		status string
		dotted bool
	}{{"Enabled", false}, {"Suspended", false}, {"", false}, {"Invalid", false}, {"Enabled", true}, {"Invalid", true}} {
		f.Add(seed.status, seed.dotted)
	}
	f.Fuzz(func(t *testing.T, status string, dotted bool) {
		p := s3.New(spitest.Deps(t))
		bucket := "accelerate-fuzz"
		if dotted {
			bucket = "accelerate.fuzz"
		}
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
		_, err := invoke(t, p, "PutBucketAccelerateConfiguration", map[string]any{"Bucket": bucket, "AccelerateConfiguration": map[string]any{"Status": status}}, nil)
		valid := !dotted && (status == "Enabled" || status == "Suspended")
		if !valid {
			want := "MalformedXML"
			if dotted {
				want = "InvalidRequest"
			}
			if fault := asFault(t, err); fault.Code != want {
				t.Fatalf("status=%q dotted=%v: %#v", status, dotted, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		response := mustInvoke(t, p, "GetBucketAccelerateConfiguration", map[string]any{"Bucket": bucket}, nil)
		if valid && response.Output["Status"] != status || !valid && len(response.Output) != 0 {
			t.Fatalf("stored acceleration = %#v", response.Output)
		}
	})
}

func FuzzBucketLogging(f *testing.F) {
	for _, seed := range []struct {
		mode   uint8
		prefix string
	}{{0, ""}, {1, "logs/"}, {2, ""}, {3, "missing/"}, {4, "<broken"}} {
		f.Add(seed.mode, seed.prefix)
	}
	f.Fuzz(func(t *testing.T, mode uint8, prefix string) {
		if !utf8.ValidString(prefix) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		for _, bucket := range []string{"logging-fuzz-source", "logging-fuzz-target"} {
			mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
		}
		baseline := map[string]any{"TargetBucket": "logging-fuzz-target", "TargetPrefix": "baseline/"}
		mustInvoke(t, p, "PutBucketLogging", map[string]any{"Bucket": "logging-fuzz-source", "BucketLoggingStatus": map[string]any{"LoggingEnabled": baseline}}, nil)
		input := map[string]any{"Bucket": "logging-fuzz-source"}
		valid, disabled, wantFault := true, false, ""
		switch mode % 5 {
		case 0:
			input["BucketLoggingStatus"], disabled = map[string]any{}, true
		case 1:
			input["BucketLoggingStatus"] = map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "logging-fuzz-target", "TargetPrefix": prefix}}
		case 2:
			input["BucketLoggingStatus"], valid, wantFault = map[string]any{"LoggingEnabled": map[string]any{"TargetPrefix": "x" + prefix}}, false, "MalformedXML"
		case 3:
			input["BucketLoggingStatus"], valid, wantFault = map[string]any{"LoggingEnabled": map[string]any{"TargetBucket": "missing"}}, false, "InvalidTargetBucketForLogging"
		case 4:
			input["_body"], valid, wantFault = "<broken", false, "MalformedXML"
		}
		_, err := invoke(t, p, "PutBucketLogging", input, nil)
		if !valid {
			if fault := asFault(t, err); fault.Code != wantFault {
				t.Fatalf("mode=%d prefix=%q: %#v", mode%5, prefix, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		output := mustInvoke(t, p, "GetBucketLogging", map[string]any{"Bucket": "logging-fuzz-source"}, nil).Output
		if disabled && len(output) != 0 {
			t.Fatalf("disabled logging = %#v", output)
		}
		stored := asMapForTest(output["LoggingEnabled"])
		if valid && !disabled && (stored["TargetBucket"] != "logging-fuzz-target" || stored["TargetPrefix"] != prefix) || !valid && (stored["TargetBucket"] != baseline["TargetBucket"] || stored["TargetPrefix"] != baseline["TargetPrefix"]) {
			t.Fatalf("stored logging = %#v", output)
		}
	})
}

func FuzzBucketCors(f *testing.F) {
	for _, seed := range []struct {
		mode   uint8
		method string
		origin string
	}{{0, "GET", "*"}, {0, "DELETE", "https://example.test"}, {0, "OPTIONS", "*"}, {1, "GET", "*"}, {2, "GET", "*"}, {3, "GET", "*"}, {4, "GET", "*"}, {5, "GET", "*"}, {6, "GET", "*"}} {
		f.Add(seed.mode, seed.method, seed.origin)
	}
	f.Fuzz(func(t *testing.T, mode uint8, method, origin string) {
		if !utf8.ValidString(method) || !utf8.ValidString(origin) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		input := map[string]any{"Bucket": "cors-fuzz"}
		mustInvoke(t, p, "CreateBucket", input, nil)
		baseline := []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"*"}}}
		mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": input["Bucket"], "CORSConfiguration": map[string]any{"CORSRules": baseline}}, nil)
		valid, wantFault := false, "MalformedXML"
		switch mode % 7 {
		case 0:
			input["CORSRules"] = []any{map[string]any{"AllowedMethods": []any{method}, "AllowedOrigins": []any{origin}}}
			valid = method == "GET" || method == "PUT" || method == "HEAD" || method == "POST" || method == "DELETE"
			if !valid {
				wantFault = "InvalidRequest"
			}
		case 1:
			input["CORSConfiguration"] = map[string]any{}
		case 2:
			input["CORSRules"] = []any{map[string]any{"AllowedOrigins": []any{origin}}}
		case 3:
			input["CORSRules"] = []any{map[string]any{"AllowedMethods": []any{method}}}
		case 4:
			input["CORSRules"] = []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{origin}, "Unknown": true}}
		case 5:
			rules := make([]any, 101)
			for i := range rules {
				rules[i] = baseline[0]
			}
			input["CORSRules"] = rules
		case 6:
			input["_body"] = "<broken"
		}
		_, err := invoke(t, p, "PutBucketCors", input, nil)
		if !valid {
			if fault := asFault(t, err); fault.Code != wantFault {
				t.Fatalf("mode=%d method=%q origin=%q: %#v", mode%7, method, origin, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		stored := asSliceForTest(mustInvoke(t, p, "GetBucketCors", map[string]any{"Bucket": "cors-fuzz"}, nil).Output["CORSRules"])
		rule := asMapForTest(stored[0])
		if valid && (asSliceForTest(rule["AllowedMethods"])[0] != method || asSliceForTest(rule["AllowedOrigins"])[0] != origin) || !valid && !reflect.DeepEqual(stored, baseline) {
			t.Fatalf("stored CORS = %#v", stored)
		}
	})
}

func FuzzBucketCorsHTTP(f *testing.F) {
	for _, seed := range []struct{ origin, method, headers string }{
		{"https://app.example.test", "GET", ""},
		{"https://app.example.test", "GET", "x-amz-request-payer,x-AMZ-meta-team"},
		{"https://wrong.test", "GET", ""},
		{"https://app.example.test/", "GET", ""},
		{"https://app.example.test", "DELETE", ""},
		{"https://app.example.test", "GET", "content-type"},
		{"", "GET", ""},
	} {
		f.Add(seed.origin, seed.method, seed.headers)
	}
	f.Fuzz(func(t *testing.T, origin, method, requested string) {
		if !utf8.ValidString(origin) || !utf8.ValidString(method) || !utf8.ValidString(requested) || len(origin)+len(method)+len(requested) > 512 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "cors-http-fuzz"}, nil)
		rules := []any{map[string]any{"AllowedMethods": []any{"GET"}, "AllowedOrigins": []any{"https://*.example.test"}, "AllowedHeaders": []any{"x-amz-*"}}}
		mustInvoke(t, p, "PutBucketCors", map[string]any{"Bucket": "cors-http-fuzz", "CORSConfiguration": map[string]any{"CORSRules": rules}}, nil)
		request := httptest.NewRequest(http.MethodOptions, "https://cors-http-fuzz.s3.us-east-1.amazonaws.com/key", nil)
		request.Header.Set("Origin", origin)
		request.Header.Set("Access-Control-Request-Method", method)
		request.Header.Set("Access-Control-Request-Headers", requested)
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
		if origin == "" {
			if fault := asFault(t, err); fault.Code != "BadRequest" {
				t.Fatalf("missing origin = %#v", fault)
			}
			return
		}
		originMatches := strings.HasPrefix(origin, "https://") && strings.HasSuffix(origin, ".example.test") && len(origin) >= len("https://")+len(".example.test")
		headersMatch := true
		for _, header := range strings.Split(requested, ",") {
			header = strings.TrimSpace(header)
			if header != "" && !strings.HasPrefix(strings.ToLower(header), "x-amz-") {
				headersMatch = false
			}
		}
		if originMatches && method == http.MethodGet && headersMatch {
			if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != origin {
				t.Fatalf("matching request = %#v, %v", response, err)
			}
		} else if fault := asFault(t, err); fault.Code != "AccessForbidden" {
			t.Fatalf("rejected request = %#v", fault)
		}
	})
}

func FuzzLocalStackCORSOrigins(f *testing.F) {
	for mode := uint8(0); mode < 8; mode++ {
		f.Add(mode, uint16(4566), "https://wrong.test")
	}
	f.Fuzz(func(t *testing.T, mode uint8, portSeed uint16, arbitrary string) {
		if !utf8.ValidString(arbitrary) || len(arbitrary) > 256 {
			t.Skip()
		}
		port := strconv.Itoa(int(portSeed%65535) + 1)
		origin := arbitrary
		wantAllowed := true
		switch mode % 8 {
		case 0:
			origin = "https://app.localstack.cloud"
		case 1:
			origin = "http://app.localstack.cloud"
		case 2:
			origin = "https://localhost"
		case 3:
			origin = "https://localhost.localstack.cloud"
		case 4:
			origin = "file://"
		case 5:
			origin = "http://localhost:" + port
		case 6:
			origin = "https://bucket.s3-website.localhost.localstack.cloud:" + port
		case 7:
			wantAllowed = false
		}
		p := s3.New(spitest.Deps(t))
		request := httptest.NewRequest(http.MethodOptions, "https://missing.s3.us-east-1.amazonaws.com:"+port+"/key", nil)
		request.Header.Set("Origin", origin)
		response, err := p.Invoke(context.Background(), &spi.Request{ServiceID: "aws.s3", Operation: "GetObject", Input: map[string]any{}, Identity: ident(), HTTP: request})
		if wantAllowed {
			if err != nil || response.Headers.Get("Access-Control-Allow-Origin") != origin || response.Headers.Get("Access-Control-Allow-Headers") == "" {
				t.Fatalf("allowed default origin %q = %#v, %v", origin, response, err)
			}
		} else if arbitrary == "https://wrong.test" && err == nil {
			t.Fatalf("known forbidden origin accepted: %#v", response)
		} else if err == nil && response.Headers.Get("Access-Control-Allow-Origin") != origin {
			t.Fatalf("arbitrary origin response = %#v", response)
		}
	})
}

func FuzzBucketWebsite(f *testing.F) {
	for _, seed := range []struct {
		mode     uint8
		suffix   string
		protocol string
	}{{0, "index.html", ""}, {0, "", ""}, {0, "dir/index.html", ""}, {1, "", "https"}, {1, "", "ftp"}, {2, "", ""}, {3, "", ""}, {4, "", ""}, {5, "", ""}, {6, "", ""}, {7, "", "http"}, {7, "", "smtp"}, {8, "", ""}} {
		f.Add(seed.mode, seed.suffix, seed.protocol)
	}
	f.Fuzz(func(t *testing.T, mode uint8, suffix, protocol string) {
		if !utf8.ValidString(suffix) || !utf8.ValidString(protocol) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		input := map[string]any{"Bucket": "website-fuzz"}
		mustInvoke(t, p, "CreateBucket", input, nil)
		baseline := map[string]any{"IndexDocument": map[string]any{"Suffix": "baseline.html"}}
		mustInvoke(t, p, "PutBucketWebsite", map[string]any{"Bucket": input["Bucket"], "WebsiteConfiguration": baseline}, nil)
		valid, wantFault := false, "InvalidRequest"
		switch mode % 9 {
		case 0:
			input["WebsiteConfiguration"] = map[string]any{"IndexDocument": map[string]any{"Suffix": suffix}}
			valid = suffix != "" && !strings.Contains(suffix, "/")
			if !valid {
				wantFault = "InvalidArgument"
			}
		case 1:
			input["WebsiteConfiguration"] = map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test", "Protocol": protocol}}
			valid = protocol == "" || protocol == "http" || protocol == "https"
		case 2:
			input["WebsiteConfiguration"], wantFault = map[string]any{"RedirectAllRequestsTo": map[string]any{"Protocol": "https"}}, "MalformedXML"
		case 3:
			input["WebsiteConfiguration"], wantFault = map[string]any{"RedirectAllRequestsTo": map[string]any{"HostName": "example.test"}, "IndexDocument": map[string]any{"Suffix": "index.html"}}, "InvalidArgument"
		case 4:
			input["WebsiteConfiguration"], wantFault = map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{}}, "MalformedXML"
		case 5:
			input["WebsiteConfiguration"] = map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Condition": map[string]any{}, "Redirect": map[string]any{}}}}
		case 6:
			input["WebsiteConfiguration"] = map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Redirect": map[string]any{"ReplaceKeyPrefixWith": "a", "ReplaceKeyWith": "b"}}}}
		case 7:
			input["WebsiteConfiguration"] = map[string]any{"IndexDocument": map[string]any{"Suffix": "index.html"}, "RoutingRules": []any{map[string]any{"Redirect": map[string]any{"Protocol": protocol}}}}
			valid = protocol == "" || protocol == "http" || protocol == "https"
		case 8:
			input["_body"], wantFault = "<broken", "MalformedXML"
		}
		_, err := invoke(t, p, "PutBucketWebsite", input, nil)
		if !valid {
			if fault := asFault(t, err); fault.Code != wantFault {
				t.Fatalf("mode=%d suffix=%q protocol=%q: %#v", mode%9, suffix, protocol, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		stored := mustInvoke(t, p, "GetBucketWebsite", map[string]any{"Bucket": "website-fuzz"}, nil).Output
		if valid && !reflect.DeepEqual(stored, input["WebsiteConfiguration"]) || !valid && !reflect.DeepEqual(stored, baseline) {
			t.Fatalf("stored website = %#v", stored)
		}
		if !valid {
			return
		}
		if mode%9 == 0 && len([]byte(suffix)) > 1024 {
			return // valid website suffix, but no valid S3 object key can exercise it
		}
		path, wantStatus := "/key", http.StatusMovedPermanently
		if mode%9 == 0 {
			path, wantStatus = "/", http.StatusOK
			mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "website-fuzz", "Key": suffix}, []byte("index"))
		}
		httpRequest := httptest.NewRequest(http.MethodGet, "http://website-fuzz.s3-website.localhost.localstack.cloud"+path, nil)
		response, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "GetObject", Input: map[string]any{}, HTTP: httpRequest})
		if err != nil || response.Status != wantStatus {
			t.Fatalf("website mode=%d suffix=%q protocol=%q response=%#v err=%v", mode%9, suffix, protocol, response, err)
		}
		if response.Stream != nil {
			_ = response.Stream.Close()
		}
	})
}

func FuzzBucketLifecycle(f *testing.F) {
	for _, seed := range []struct {
		mode   uint8
		prefix string
		size   uint8
	}{{0, "images/", 5}, {0, "", 0}, {1, "images/", 5}, {2, "images/", 5}, {3, "images/", 5}, {4, "images/", 5}} {
		f.Add(seed.mode, seed.prefix, seed.size)
	}
	f.Add(uint8(0), strings.Repeat("a", 1022), uint8(0))
	f.Fuzz(func(t *testing.T, mode uint8, prefix string, sizeSeed uint8) {
		if !utf8.ValidString(prefix) || len(prefix) > 1021 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		bucket := map[string]any{"Bucket": "lifecycle-fuzz"}
		mustInvoke(t, p, "CreateBucket", bucket, nil)
		baseline := []any{map[string]any{"ID": "baseline", "Filter": map[string]any{}, "Status": "Enabled", "Expiration": map[string]any{"Days": float64(1)}}}
		mustInvoke(t, p, "PutBucketLifecycleConfiguration", map[string]any{"Bucket": bucket["Bucket"], "LifecycleConfiguration": map[string]any{"Rules": baseline}}, nil)
		rules := []any{map[string]any{"ID": "fuzz", "Filter": map[string]any{"Prefix": prefix}, "Status": "Enabled", "Expiration": map[string]any{"Days": 1}}}
		valid, wantFault := true, ""
		request := map[string]any{"Bucket": bucket["Bucket"], "LifecycleConfiguration": map[string]any{"Rules": rules}}
		switch mode % 5 {
		case 1:
			rules[0].(map[string]any)["Filter"] = map[string]any{"Prefix": prefix, "ObjectSizeGreaterThan": int(sizeSeed)}
			valid, wantFault = false, "MalformedXML"
		case 2:
			rules[0].(map[string]any)["Filter"] = map[string]any{"And": map[string]any{"Tags": []any{map[string]any{"Key": "k", "Value": "a"}, map[string]any{"Key": "k", "Value": "b"}}}}
			valid, wantFault = false, "InvalidRequest"
		case 3:
			request["TransitionDefaultMinimumObjectSize"] = prefix
			valid = prefix == "all_storage_classes_128K" || prefix == "varies_by_storage_class"
			if !valid {
				wantFault = "InvalidRequest"
			}
		case 4:
			rules[0].(map[string]any)["Filter"] = map[string]any{"ObjectSizeGreaterThan": int(sizeSeed)}
		}
		_, err := invoke(t, p, "PutBucketLifecycleConfiguration", request, nil)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid {
			if fault := asFault(t, err); fault.Code != wantFault {
				t.Fatalf("mode=%d prefix=%q size=%d fault=%#v", mode%5, prefix, sizeSeed, fault)
			}
			if got := mustInvoke(t, p, "GetBucketLifecycleConfiguration", bucket, nil).Output["Rules"]; !reflect.DeepEqual(got, baseline) {
				t.Fatalf("invalid lifecycle replaced baseline: %#v", got)
			}
			return
		}
		body := bytes.Repeat([]byte{'x'}, int(sizeSeed)+1)
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket["Bucket"], "Key": prefix + "key"}, body)
		if got := put.Headers.Get("x-amz-expiration"); got == "" {
			t.Fatalf("mode=%d prefix=%q size=%d missing expiration", mode%5, prefix, sizeSeed)
		}
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": bucket["Bucket"], "Key": prefix + "m"}, nil).Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": bucket["Bucket"], "Key": prefix + "m", "UploadId": uploadID, "PartNumber": 1}, body)
		completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
		if got := completed.Headers.Get("x-amz-expiration"); got == "" {
			t.Fatalf("mode=%d prefix=%q size=%d missing completion expiration", mode%5, prefix, sizeSeed)
		}
	})
}

func FuzzBucketPolicy(f *testing.F) {
	for _, policy := range []string{
		`{"Statement":[]}`,
		`{"Version":"2012-10-17"}`,
		`{}`,
		` {"Statement":[]}`,
		`{`,
		`[]`,
		``,
	} {
		f.Add(policy)
	}
	f.Fuzz(func(t *testing.T, policy string) {
		if !utf8.ValidString(policy) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		bucket := map[string]any{"Bucket": "policy-fuzz"}
		mustInvoke(t, p, "CreateBucket", bucket, nil)
		baseline := `{"Statement":[{"Effect":"Deny"}]}`
		mustInvoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": bucket["Bucket"], "Policy": baseline}, nil)

		var document map[string]any
		valid := policy != "" && policy[0] == '{' && json.Unmarshal([]byte(policy), &document) == nil && len(document) != 0
		_, err := invoke(t, p, "PutBucketPolicy", map[string]any{"Bucket": bucket["Bucket"], "Policy": policy}, nil)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid && asFault(t, err).Code != "MalformedPolicy" {
			t.Fatalf("policy=%q fault=%v", policy, err)
		}
		want := baseline
		if valid {
			want = policy
		}
		if got := mustInvoke(t, p, "GetBucketPolicy", bucket, nil).Output["Policy"]; got != want {
			t.Fatalf("policy=%q stored=%q want=%q", policy, got, want)
		}
	})
}

func FuzzBucketEncryption(f *testing.F) {
	for _, seed := range []struct {
		mode      uint8
		algorithm string
		withKey   bool
	}{{0, "AES256", false}, {0, "aws:kms", true}, {0, "aws:kms:dsse", false}, {0, "aws:fsx", false}, {0, "aws:backup", false}, {0, "invalid", false}, {0, "AES256", true}, {1, "AES256", false}, {2, "AES256", false}, {3, "AES256", false}} {
		f.Add(seed.mode, seed.algorithm, seed.withKey)
	}
	f.Fuzz(func(t *testing.T, mode uint8, algorithm string, withKey bool) {
		if !utf8.ValidString(algorithm) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		bucket := map[string]any{"Bucket": "encryption-fuzz"}
		mustInvoke(t, p, "CreateBucket", bucket, nil)
		baseline := []any{map[string]any{"ApplyServerSideEncryptionByDefault": map[string]any{"SSEAlgorithm": "aws:kms"}}}
		mustInvoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": map[string]any{"Rules": baseline}}, nil)

		defaults := map[string]any{"SSEAlgorithm": algorithm}
		if withKey {
			defaults["KMSMasterKeyID"] = "key-id"
		}
		rules := []any{map[string]any{"ApplyServerSideEncryptionByDefault": defaults}}
		configuration := map[string]any{"Rules": rules}
		valid, wantFault := false, "MalformedXML"
		switch mode % 4 {
		case 0:
			valid = map[string]bool{"AES256": true, "aws:fsx": true, "aws:backup": true, "aws:kms": true, "aws:kms:dsse": true}[algorithm] && (!withKey || algorithm == "aws:kms")
			if !valid && withKey && algorithm != "aws:kms" && map[string]bool{"AES256": true, "aws:fsx": true, "aws:backup": true, "aws:kms:dsse": true}[algorithm] {
				wantFault = "InvalidArgument"
			}
		case 1:
			configuration["Rules"] = []any{}
		case 2:
			configuration["Rules"] = append(rules, rules[0])
		case 3:
			configuration["Rules"] = []any{map[string]any{}}
		}
		_, err := invoke(t, p, "PutBucketEncryption", map[string]any{"Bucket": bucket["Bucket"], "ServerSideEncryptionConfiguration": configuration}, nil)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid && asFault(t, err).Code != wantFault {
			t.Fatalf("mode=%d algorithm=%q key=%v fault=%v", mode%4, algorithm, withKey, err)
		}
		stored := mustInvoke(t, p, "GetBucketEncryption", bucket, nil).Output["Rules"]
		if valid && !reflect.DeepEqual(stored, rules) || !valid && !reflect.DeepEqual(stored, baseline) {
			t.Fatalf("stored encryption = %#v", stored)
		}
	})
}

func FuzzNamedBucketConfigurations(f *testing.F) {
	for _, seed := range []struct {
		mode  uint8
		value string
	}{{0, "CSV"}, {0, "JSON"}, {1, "Daily"}, {1, "Hourly"}, {2, "Size"}, {2, "Unknown"}, {3, "inventory"}, {3, "other"}} {
		f.Add(seed.mode, seed.value)
	}
	f.Fuzz(func(t *testing.T, mode uint8, value string) {
		if !utf8.ValidString(value) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		bucket := map[string]any{"Bucket": "named-configuration-fuzz"}
		mustInvoke(t, p, "CreateBucket", bucket, nil)
		baseline := map[string]any{
			"Id": "inventory", "IsEnabled": true, "IncludedObjectVersions": "All",
			"Destination":    map[string]any{"S3BucketDestination": map[string]any{"Bucket": "arn:aws:s3:::destination", "Format": "CSV"}},
			"Schedule":       map[string]any{"Frequency": "Daily"},
			"OptionalFields": []any{"Size"},
		}
		mustInvoke(t, p, "PutBucketInventoryConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "inventory", "InventoryConfiguration": baseline}, nil)
		configuration := map[string]any{
			"Id": "inventory", "IsEnabled": true, "IncludedObjectVersions": "All",
			"Destination":    map[string]any{"S3BucketDestination": map[string]any{"Bucket": "arn:aws:s3:::destination", "Format": "CSV"}},
			"Schedule":       map[string]any{"Frequency": "Daily"},
			"OptionalFields": []any{"Size"},
		}
		valid, wantFault := false, "MalformedXML"
		switch mode % 4 {
		case 0:
			configuration["Destination"].(map[string]any)["S3BucketDestination"].(map[string]any)["Format"] = value
			valid = value == "CSV" || value == "ORC" || value == "Parquet"
		case 1:
			configuration["Schedule"].(map[string]any)["Frequency"] = value
			valid = value == "Daily" || value == "Weekly"
		case 2:
			configuration["OptionalFields"] = []any{value}
			valid = map[string]bool{
				"Size": true, "LastModifiedDate": true, "StorageClass": true, "ETag": true,
				"IsMultipartUploaded": true, "ReplicationStatus": true, "EncryptionStatus": true,
				"ObjectLockRetainUntilDate": true, "ObjectLockMode": true, "ObjectLockLegalHoldStatus": true,
				"IntelligentTieringAccessTier": true, "BucketKeyStatus": true, "ChecksumAlgorithm": true,
			}[value]
		case 3:
			configuration["Id"] = value
			valid = value == "inventory"
			wantFault = "IdMismatch"
		}
		_, err := invoke(t, p, "PutBucketInventoryConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "inventory", "InventoryConfiguration": configuration}, nil)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid && asFault(t, err).Code != wantFault {
			t.Fatalf("mode=%d value=%q fault=%v", mode%4, value, err)
		}
		stored := mustInvoke(t, p, "GetBucketInventoryConfiguration", map[string]any{"Bucket": bucket["Bucket"], "Id": "inventory"}, nil).Output["InventoryConfiguration"]
		if valid && !reflect.DeepEqual(stored, configuration) || !valid && !reflect.DeepEqual(stored, baseline) {
			t.Fatalf("stored inventory = %#v", stored)
		}
	})
}

func FuzzACLConfigurations(f *testing.F) {
	for _, seed := range []struct {
		mode  uint8
		value string
	}{{0, "private"}, {0, "public-read"}, {0, "invalid"}, {1, "http://acs.amazonaws.com/groups/global/AllUsers"}, {1, "invalid"}, {2, "123456789012"}, {2, "wrong-id"}, {3, "FULL_CONTROL"}, {3, "INVALID"}, {4, ""}, {5, ""}, {6, ""}, {7, ""}} {
		f.Add(seed.mode, seed.value)
	}
	f.Fuzz(func(t *testing.T, mode uint8, value string) {
		if !utf8.ValidString(value) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		bucket, key := "acl-fuzz", "object"
		account := ident().Account
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
		mode %= 8
		if mode >= 4 {
			mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": bucket, "Status": "Enabled"}, nil)
		}
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": key, "ACL": "public-read"}, []byte("body"))
		if mode >= 4 {
			marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": key}, nil)
			input := map[string]any{"Bucket": bucket, "Key": key}
			if mode%2 == 1 {
				input["VersionId"] = marker.Headers.Get("x-amz-version-id")
			}
			operation, wantCode := "GetObjectAcl", "MethodNotAllowed"
			if mode >= 6 {
				operation, input["ACL"] = "PutObjectAcl", "private"
			} else if mode == 4 {
				wantCode = "NoSuchKey"
			}
			_, err := invoke(t, p, operation, input, nil)
			if fault := asFault(t, err); fault.Code != wantCode {
				t.Fatalf("mode=%d fault=%#v", mode, fault)
			}
			return
		}
		input := map[string]any{"Bucket": bucket, "Key": key}
		valid, wantFault := false, "InvalidArgument"
		switch mode {
		case 0:
			input["ACL"] = value
			valid = value == "private" || value == "public-read" || value == "public-read-write" || value == "authenticated-read" || value == "bucket-owner-read" || value == "bucket-owner-full-control" || value == "aws-exec-read" || value == "log-delivery-write"
			if value == "" {
				wantFault = "MissingSecurityHeader"
			}
		case 1:
			input["GrantRead"] = `uri="` + value + `"`
			valid = value == "http://acs.amazonaws.com/groups/global/AllUsers" || value == "http://acs.amazonaws.com/groups/global/AuthenticatedUsers" || value == "http://acs.amazonaws.com/groups/s3/LogDelivery"
		case 2:
			input["GrantRead"] = `id="` + value + `"`
			valid = value == account || len(value) == 64 && strings.IndexFunc(value, func(r rune) bool { return !strings.ContainsRune("0123456789abcdefABCDEF", r) }) < 0
		case 3:
			input["AccessControlPolicy"] = map[string]any{"Owner": map[string]any{"ID": account}, "Grants": []any{map[string]any{"Grantee": map[string]any{"Type": "CanonicalUser", "ID": account}, "Permission": value}}}
			valid = value == "FULL_CONTROL" || value == "READ" || value == "WRITE" || value == "READ_ACP" || value == "WRITE_ACP"
			wantFault = "MalformedACLError"
		}
		_, err := invoke(t, p, "PutObjectAcl", input, nil)
		if valid && err != nil {
			t.Fatal(err)
		}
		if !valid && asFault(t, err).Code != wantFault {
			t.Fatalf("mode=%d value=%q fault=%v", mode, value, err)
		}
		acl := mustInvoke(t, p, "GetObjectAcl", map[string]any{"Bucket": bucket, "Key": key}, nil).Output
		grants := asSliceForTest(acl["Grants"])
		if valid && len(grants) == 0 || !valid && len(grants) != 2 {
			t.Fatalf("stored ACL = %#v", grants)
		}
		if asMapForTest(acl["Owner"])["DisplayName"] != nil {
			t.Fatalf("owner display name = %#v", acl["Owner"])
		}
		for _, grant := range grants {
			if asMapForTest(asMapForTest(grant)["Grantee"])["DisplayName"] != nil {
				t.Fatalf("grantee display name = %#v", grant)
			}
		}
	})
}

func FuzzBucketNotifications(f *testing.F) {
	for _, seed := range []struct {
		mode uint8
		name string
	}{{0, "prefix"}, {0, "suffix"}, {0, "contains"}, {1, "prefix"}, {2, "prefix"}, {3, "prefix"}, {4, "prefix"}, {5, "prefix"}} {
		f.Add(seed.mode, seed.name)
	}
	f.Fuzz(func(t *testing.T, mode uint8, name string) {
		if !utf8.ValidString(name) {
			t.Skip()
		}
		deps := spitest.Deps(t)
		p := s3.New(deps)
		input := map[string]any{"Bucket": "notification-fuzz"}
		mustInvoke(t, p, "CreateBucket", input, nil)
		if err := deps.Store.Scope("123456789012", "us-east-1").Collection("queues").Put(context.Background(), "queue", []byte("{}")); err != nil {
			t.Fatal(err)
		}
		baseline := map[string]any{"QueueConfigurations": []any{map[string]any{"Id": "baseline", "QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectCreated:*"}}}}
		mustInvoke(t, p, "PutBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"], "NotificationConfiguration": baseline}, nil)
		valid, cleared, wantFault := false, false, "InvalidArgument"
		configuration := map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectCreated:*"}, "Filter": map[string]any{"Key": map[string]any{"FilterRules": []any{map[string]any{"Name": name, "Value": "images/"}}}}}}}
		switch mode % 6 {
		case 0:
			valid = strings.EqualFold(name, "prefix") || strings.EqualFold(name, "suffix")
		case 1:
			configuration = map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:missing", "Events": []any{"s3:ObjectCreated:*"}}}}
		case 2:
			configuration = map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sns:us-east-1:123456789012:queue", "Events": []any{"s3:ObjectCreated:*"}}}}
		case 3:
			configuration, wantFault = map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:queue"}}}, "MalformedXML"
		case 4:
			configuration, valid = map[string]any{"QueueConfigurations": []any{map[string]any{"QueueArn": "arn:aws:sqs:us-east-1:123456789012:missing", "Events": []any{"s3:ObjectCreated:*"}}}}, true
			input["SkipDestinationValidation"] = true
		case 5:
			configuration, valid, cleared = map[string]any{}, true, true
		}
		input["NotificationConfiguration"] = configuration
		_, err := invoke(t, p, "PutBucketNotificationConfiguration", input, nil)
		if !valid {
			if fault := asFault(t, err); fault.Code != wantFault {
				t.Fatalf("mode=%d name=%q: %#v", mode%6, name, fault)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		stored := mustInvoke(t, p, "GetBucketNotificationConfiguration", map[string]any{"Bucket": input["Bucket"]}, nil).Output
		if cleared && len(stored) != 0 || !valid && !reflect.DeepEqual(stored, baseline) {
			t.Fatalf("stored notifications = %#v", stored)
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

func FuzzCrossRegionBucketResolution(f *testing.F) {
	f.Add("key", "body", uint8(0))
	f.Add("nested/object", "", uint8(1))
	f.Fuzz(func(t *testing.T, key, body string, regionSeed uint8) {
		if key == "" || len([]byte(key)) > 1024 || len(body) > 4096 || !utf8.ValidString(key) || !utf8.ValidString(body) {
			t.Skip()
		}
		regions := []string{"us-west-2", "eu-west-1", "ap-southeast-2"}
		region := regions[int(regionSeed)%len(regions)]
		p := s3.New(spitest.Deps(t))
		east := spi.Identity{Account: "000000000000", Region: "us-east-1"}
		mustInvokeAs(t, p, east, "CreateBucket", map[string]any{"Bucket": "cross-region-fuzz", "LocationConstraint": region}, nil)
		if got := mustInvokeAs(t, p, east, "HeadBucket", map[string]any{"Bucket": "cross-region-fuzz"}, nil).Headers.Get("x-amz-bucket-region"); got != region {
			t.Fatalf("head region = %q want %q", got, region)
		}
		mustInvokeAs(t, p, east, "PutObject", map[string]any{"Bucket": "cross-region-fuzz", "Key": key}, []byte(body))
		response := mustInvokeAs(t, p, east, "GetObject", map[string]any{"Bucket": "cross-region-fuzz", "Key": key}, nil)
		if stored := string(readStream(t, response)); stored != body {
			t.Fatalf("body = %q want %q", stored, body)
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
			if status == "Enabled" {
				versionID := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": input["Bucket"], "Key": "format"}, nil).Headers.Get("x-amz-version-id")
				if len(versionID) != 32 {
					t.Fatalf("version id = %q", versionID)
				}
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

func FuzzObjectLockConfigurationValidation(f *testing.F) {
	f.Add("GOVERNANCE", int8(1), int8(0), true, false)
	f.Add("COMPLIANCE", int8(0), int8(1), false, true)
	f.Add("INVALID", int8(1), int8(0), true, false)
	f.Add("GOVERNANCE", int8(1), int8(1), true, true)
	f.Fuzz(func(t *testing.T, mode string, days, years int8, includeDays, includeYears bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "lock-validation-fuzz", "ObjectLockEnabledForBucket": true}, nil)
		retention := map[string]any{"Mode": mode}
		if includeDays {
			retention["Days"] = int(days)
		}
		if includeYears {
			retention["Years"] = int(years)
		}
		_, err := invoke(t, p, "PutObjectLockConfiguration", map[string]any{"Bucket": "lock-validation-fuzz", "ObjectLockConfiguration": map[string]any{
			"ObjectLockEnabled": "Enabled", "Rule": map[string]any{"DefaultRetention": retention},
		}}, nil)
		validMode := mode == "GOVERNANCE" || mode == "COMPLIANCE"
		validDuration := includeDays != includeYears && (includeDays && days > 0 || includeYears && years > 0)
		if validMode && validDuration {
			if err != nil {
				t.Fatalf("valid configuration rejected: mode=%q days=%d years=%d include=(%t,%t): %v", mode, days, years, includeDays, includeYears, err)
			}
			return
		}
		if got := asFault(t, err); got.Code != "MalformedXML" || got.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("invalid configuration fault = %#v", got)
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
		{1, "", "", "0", false, true},
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
			if created := mustInvokeAs(t, p, bucket.id, "CreateBucket", input, nil); created.Output["BucketArn"] != "arn:aws:s3:::"+bucket.name {
				t.Fatalf("create ARN = %#v", created.Output)
			}
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
			fault := asFault(t, err)
			if setRegion && fault.Code == "InvalidArgument" && fault.Message == fmt.Sprintf("Argument value %s is not a valid AWS Region", region) && fault.Fields["ArgumentName"] == "bucket-region" {
				return
			}
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
			if got["Name"] != want[i].name || got["BucketArn"] != "arn:aws:s3:::"+want[i].name || got["BucketRegion"] != want[i].region {
				t.Fatalf("bucket %d = %#v want=%#v", i, got, want[i])
			}
		}
		gotToken, _ := page.Output["ContinuationToken"].(string)
		if gotToken != wantToken {
			t.Fatalf("token = %q want %q", gotToken, wantToken)
		}
	})
}

func FuzzListObjectsPagination(f *testing.F) {
	for _, seed := range []struct {
		v2, fetchOwner bool
		max            uint8
		marker         string
	}{{false, false, 0, ""}, {false, true, 1, "folder/a/"}, {true, false, 2, "folder/a/"}, {true, true, 3, "folder/b"}} {
		f.Add(seed.v2, seed.fetchOwner, seed.max, seed.marker)
	}
	f.Fuzz(func(t *testing.T, v2, fetchOwner bool, maxSeed uint8, marker string) {
		if !utf8.ValidString(marker) || len(marker) > 128 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-fuzz", "LocationConstraint": "us-west-2"}, nil)
		body := []byte("content")
		sum := sha256.Sum256(body)
		for _, key := range []string{"folder/a/one", "folder/a/two", "folder/b value+", "folder/c"} {
			input := map[string]any{"Bucket": "list-fuzz", "Key": key}
			if key == "folder/b value+" {
				input["ChecksumSHA256"] = base64.StdEncoding.EncodeToString(sum[:])
			}
			mustInvoke(t, p, "PutObject", input, body)
		}
		requestedMaxKeys := int(maxSeed % 4)
		maxKeys := requestedMaxKeys
		if maxKeys == 0 {
			maxKeys = 1000
		}
		operation := "ListObjects"
		input := map[string]any{"Bucket": "list-fuzz", "Prefix": "folder/", "Delimiter": "/", "MaxKeys": requestedMaxKeys, "Marker": marker, "EncodingType": "url"}
		if v2 {
			operation = "ListObjectsV2"
			delete(input, "Marker")
			if marker != "" {
				input["ContinuationToken"] = base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._").EncodeToString([]byte(marker))
			}
			input["FetchOwner"] = fetchOwner
		}
		page := mustInvoke(t, p, operation, input, nil).Output
		if page["BucketRegion"] != "us-west-2" {
			t.Fatalf("%s BucketRegion = %#v", operation, page["BucketRegion"])
		}
		all := []string{"folder/a/", "folder/b value+", "folder/c"}
		var want []string
		for _, value := range all {
			if value > marker || v2 && value == marker {
				want = append(want, value)
			}
		}
		truncated := len(want) > maxKeys
		next := ""
		if truncated {
			if v2 {
				next = want[maxKeys]
			} else {
				next = want[maxKeys-1]
			}
			want = want[:maxKeys]
		}
		var got []string
		for _, value := range asSliceForTest(page["CommonPrefixes"]) {
			got = append(got, asMapForTest(value)["Prefix"].(string))
		}
		for _, value := range asSliceForTest(page["Contents"]) {
			row := asMapForTest(value)
			got = append(got, row["Key"].(string))
			if checksummed := row["Key"] == "folder/b%20value%2B"; checksummed != reflect.DeepEqual(row["ChecksumAlgorithm"], []any{"SHA256"}) || checksummed != (row["ChecksumType"] == "FULL_OBJECT") {
				t.Fatalf("%s checksum metadata = %#v", operation, row)
			}
			owner := asMapForTest(row["Owner"])
			if wantOwner := !v2 || fetchOwner; (owner["ID"] == "123456789012") != wantOwner || owner["DisplayName"] != nil {
				t.Fatalf("%s fetchOwner=%v owner=%#v", operation, fetchOwner, owner)
			}
		}
		encode := func(value string) string {
			return strings.ReplaceAll(strings.ReplaceAll(url.QueryEscape(value), "+", "%20"), "%2F", "/")
		}
		wantEncoded := make([]string, len(want))
		for i, value := range want {
			wantEncoded[i] = encode(value)
		}
		if strings.Join(got, "\x00") != strings.Join(wantEncoded, "\x00") || page["MaxKeys"] != maxKeys || page["IsTruncated"] != truncated || page["KeyCount"] != len(want) {
			t.Fatalf("%s marker=%q max=%d page=%#v want=%v", operation, marker, maxKeys, page, want)
		}
		if truncated {
			field := "NextMarker"
			if v2 {
				field = "NextContinuationToken"
			}
			wantToken := next
			if v2 {
				wantToken = base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._").EncodeToString([]byte(next))
			} else {
				wantToken = encode(next)
			}
			if page[field] != wantToken {
				t.Fatalf("%s = %q want %q", field, page[field], wantToken)
			}
		}
	})
}

func FuzzListEncodingType(f *testing.F) {
	for _, seed := range []struct {
		operation uint8
		encoding  string
		provided  bool
	}{{0, "url", true}, {1, "value", true}, {2, "", true}, {3, "URL", true}, {0, "", false}} {
		f.Add(seed.operation, seed.encoding, seed.provided)
	}
	f.Fuzz(func(t *testing.T, operationSeed uint8, encoding string, provided bool) {
		if !utf8.ValidString(encoding) || len(encoding) > 128 {
			t.Skip()
		}
		operations := []string{"ListObjects", "ListObjectsV2", "ListObjectVersions", "ListMultipartUploads"}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "list-encoding-fuzz"}, nil)
		input := map[string]any{"Bucket": "list-encoding-fuzz"}
		if provided {
			input["EncodingType"] = encoding
		}
		_, err := invoke(t, p, operations[int(operationSeed)%len(operations)], input, nil)
		if !provided || encoding == "url" {
			if err != nil {
				t.Fatal(err)
			}
			return
		}
		fault := asFault(t, err)
		if fault.Code != "InvalidArgument" || fault.Message != "Invalid Encoding Method specified in Request" || fault.Fields["ArgumentName"] != "encoding-type" || fault.Fields["ArgumentValue"] != encoding {
			t.Fatalf("encoding %q fault = %#v", encoding, fault)
		}
	})
}

func FuzzListObjectVersionsPagination(f *testing.F) {
	for _, seed := range []struct {
		max, start uint8
		deleted    bool
	}{{0, 0, false}, {2, 1, true}, {3, 2, true}, {5, 4, false}} {
		f.Add(seed.max, seed.start, seed.deleted)
	}
	f.Fuzz(func(t *testing.T, maxSeed, startSeed uint8, deleted bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "version-list-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "version-list-fuzz", "Status": "Enabled"}, nil)
		body := []byte("body")
		sum := sha256.Sum256(body)
		for i := range 5 {
			input := map[string]any{"Bucket": "version-list-fuzz", "Key": "prefix/key"}
			if i == 2 {
				input["ChecksumSHA256"] = base64.StdEncoding.EncodeToString(sum[:])
			}
			mustInvoke(t, p, "PutObject", input, body)
		}
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list-fuzz", "Key": "url/k ey+"}, body)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "version-list-fuzz", "Key": "url/k!ey+"}, body)
		all := asSliceForTest(mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list-fuzz", "Prefix": "prefix/"}, nil).Output["Versions"])
		start, requestedMaxKeys := int(startSeed)%len(all), int(maxSeed%6)
		maxKeys := requestedMaxKeys
		if maxKeys == 0 {
			maxKeys = 1000
		}
		input := map[string]any{"Bucket": "version-list-fuzz", "Prefix": "prefix/", "MaxKeys": requestedMaxKeys}
		if start > 0 {
			input["KeyMarker"] = "prefix/key"
			input["VersionIdMarker"] = asMapForTest(all[start-1])["VersionId"]
			if deleted {
				mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "version-list-fuzz", "Key": "prefix/key", "VersionId": input["VersionIdMarker"]}, nil)
			}
		}
		page := mustInvoke(t, p, "ListObjectVersions", input, nil).Output
		got := asSliceForTest(page["Versions"])
		end := min(start+maxKeys, len(all))
		if len(got) != end-start || page["MaxKeys"] != maxKeys || page["IsTruncated"] != (end < len(all)) {
			t.Fatalf("start=%d max=%d page=%#v", start, maxKeys, page)
		}
		for index := range got {
			gotRow, wantRow := asMapForTest(got[index]), asMapForTest(all[start+index])
			if gotRow["VersionId"] != wantRow["VersionId"] || !reflect.DeepEqual(gotRow["ChecksumAlgorithm"], wantRow["ChecksumAlgorithm"]) || gotRow["ChecksumType"] != wantRow["ChecksumType"] {
				t.Fatalf("version %d = %#v want %#v", index, got[index], all[start+index])
			}
		}
		if end < len(all) && (page["NextKeyMarker"] != "prefix/key" || page["NextVersionIdMarker"] != asMapForTest(all[end-1])["VersionId"]) {
			t.Fatalf("next markers = %#v", page)
		}
		encoded := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list-fuzz", "Prefix": "url/", "MaxKeys": 1, "EncodingType": "url"}, nil).Output
		if rows := encoded["Versions"].([]any); len(rows) != 1 || asMapForTest(rows[0])["Key"] != "url/k%20ey%2B" || encoded["NextVersionIdMarker"] == nil {
			t.Fatalf("encoded versions = %#v", encoded)
		}
		next := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "version-list-fuzz", "Prefix": "url/", "MaxKeys": 1, "EncodingType": "url", "KeyMarker": encoded["NextKeyMarker"], "VersionIdMarker": encoded["NextVersionIdMarker"]}, nil).Output
		if rows := next["Versions"].([]any); len(rows) != 1 || asMapForTest(rows[0])["Key"] != "url/k%21ey%2B" || next["KeyMarker"] != "url/k%20ey%2B" {
			t.Fatalf("next encoded version page = %#v", next)
		}
	})
}

func FuzzGetObjectAttributesStorageClass(f *testing.F) {
	for _, attribute := range []string{"StorageClass", "storage_class", "ETag", "ETag, StorageClass"} {
		f.Add(attribute)
	}
	f.Fuzz(func(t *testing.T, attribute string) {
		if len(attribute) > 64 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "storage-attributes-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "storage-attributes-fuzz", "Key": "key"}, []byte("body"))
		output := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "storage-attributes-fuzz", "Key": "key", "ObjectAttributes": attribute}, nil).Output
		requested := false
		for _, value := range strings.Split(attribute, ",") {
			requested = requested || strings.EqualFold(strings.ReplaceAll(strings.TrimSpace(value), "_", ""), "StorageClass")
		}
		if got, present := output["StorageClass"]; requested != present || present && got != "STANDARD" {
			t.Fatalf("attribute %q output = %#v", attribute, output)
		}
	})
}

func FuzzGetObjectAttributesPartMarkers(f *testing.F) {
	for _, marker := range []uint8{0, 1, 2, 255} {
		f.Add(marker)
	}
	f.Fuzz(func(t *testing.T, marker uint8) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "object-parts-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "object-parts-fuzz", "Key": "key", "ChecksumAlgorithm": "CRC32"}, nil)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"UploadId": created.Output["UploadId"], "PartNumber": 1}, []byte("body"))
		mustInvoke(t, p, "CompleteMultipartUpload", completeInput(created.Output["UploadId"].(string), completedPartWithChecksum(1, part, "ChecksumCRC32", "x-amz-checksum-crc32")), nil)
		output := mustInvoke(t, p, "GetObjectAttributes", map[string]any{"Bucket": "object-parts-fuzz", "Key": "key", "ObjectAttributes": []string{"ObjectParts"}, "PartNumberMarker": int(marker), "MaxParts": 1}, nil).Output
		objectParts := asMapForTest(output["ObjectParts"])
		wantNext, wantParts := "0", 0
		if marker == 0 {
			wantNext, wantParts = "1", 1
		}
		if objectParts["NextPartNumberMarker"] != wantNext || len(asSliceForTest(objectParts["Parts"])) != wantParts {
			t.Fatalf("marker %d object parts = %#v", marker, objectParts)
		}
	})
}

func FuzzListMultipartUploadsMarkers(f *testing.F) {
	for _, seed := range []struct {
		max, start uint8
		invalid    bool
	}{{0, 0, false}, {2, 1, false}, {5, 4, false}, {1, 2, true}} {
		f.Add(seed.max, seed.start, seed.invalid)
	}
	f.Fuzz(func(t *testing.T, maxSeed, startSeed uint8, invalid bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-list-fuzz"}, nil)
		for range 5 {
			mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-list-fuzz", "Key": "prefix/key", "ChecksumAlgorithm": "CRC64NVME"}, nil)
		}
		all := asSliceForTest(mustInvoke(t, p, "ListMultipartUploads", map[string]any{"Bucket": "multipart-list-fuzz", "Prefix": "prefix/"}, nil).Output["Uploads"])
		start, requestedMaxUploads := int(startSeed)%len(all), int(maxSeed%6)
		maxUploads := requestedMaxUploads
		if maxUploads == 0 {
			maxUploads = 1000
		}
		input := map[string]any{"Bucket": "multipart-list-fuzz", "Prefix": "prefix/", "MaxUploads": requestedMaxUploads}
		if start > 0 {
			input["KeyMarker"] = "prefix/key"
			input["UploadIdMarker"] = asMapForTest(all[start-1])["UploadId"]
		}
		if invalid {
			input["KeyMarker"] = "wrong"
			input["UploadIdMarker"] = asMapForTest(all[start])["UploadId"]
			_, err := invoke(t, p, "ListMultipartUploads", input, nil)
			fault := asFault(t, err)
			if fault.Code != "InvalidArgument" || fault.Message != "Invalid uploadId marker" || fault.Fields["ArgumentName"] != "upload-id-marker" {
				t.Fatalf("invalid marker fault = %#v", fault)
			}
			return
		}
		page := mustInvoke(t, p, "ListMultipartUploads", input, nil).Output
		got := asSliceForTest(page["Uploads"])
		end := min(start+maxUploads, len(all))
		if len(got) != end-start || page["MaxUploads"] != maxUploads || page["IsTruncated"] != (end < len(all)) {
			t.Fatalf("start=%d max=%d page=%#v", start, maxUploads, page)
		}
		for index := range got {
			row := asMapForTest(got[index])
			if row["UploadId"] != asMapForTest(all[start+index])["UploadId"] || row["ChecksumAlgorithm"] != "CRC64NVME" || row["ChecksumType"] != "FULL_OBJECT" || asMapForTest(row["Initiator"])["DisplayName"] != "webfile" {
				t.Fatalf("upload %d = %#v want %#v", index, got[index], all[start+index])
			}
		}
		if len(got) > 0 && (page["NextKeyMarker"] != "prefix/key" || page["NextUploadIdMarker"] != asMapForTest(got[len(got)-1])["UploadId"]) {
			t.Fatalf("next markers = %#v", page)
		}
	})
}

func FuzzListPartsPagination(f *testing.F) {
	for _, seed := range []struct {
		max, marker uint8
		zero        bool
	}{{1, 0, false}, {2, 1, false}, {3, 7, false}, {0, 9, true}} {
		f.Add(seed.max, seed.marker, seed.zero)
	}
	f.Fuzz(func(t *testing.T, maxSeed, markerSeed uint8, zero bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "parts-list-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "parts-list-fuzz", "Key": "key"}, nil).Output["UploadId"]
		for _, number := range []int{1, 3, 7} {
			mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "parts-list-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": number}, []byte("part"))
		}
		marker, maxParts := int(markerSeed%10), int(maxSeed%3)+1
		if zero {
			maxParts = 0
		}
		input := map[string]any{"Bucket": "parts-list-fuzz", "Key": "key", "UploadId": uploadID, "PartNumberMarker": marker, "MaxParts": maxParts}
		var page map[string]any
		if zero {
			request := httptest.NewRequest(http.MethodGet, "http://s3.localhost/parts-list-fuzz/key?uploadId="+url.QueryEscape(fmt.Sprint(uploadID))+"&part-number-marker="+strconv.Itoa(marker)+"&max-parts=0", nil)
			response, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "ListParts", Input: input, HTTP: request})
			if err != nil {
				t.Fatal(err)
			}
			page = response.Output
		} else {
			page = mustInvoke(t, p, "ListParts", input, nil).Output
		}
		want := []int{}
		for _, number := range []int{1, 3, 7} {
			if number > marker {
				want = append(want, number)
			}
		}
		limit := maxParts
		if limit == 0 {
			limit = 1000
		}
		truncated := len(want) > limit
		if truncated {
			want = want[:limit]
		}
		got := asSliceForTest(page["Parts"])
		next := 0
		if len(want) > 0 {
			next = want[len(want)-1]
		}
		if len(got) != len(want) || page["IsTruncated"] != truncated || page["NextPartNumberMarker"] != next || page["MaxParts"] != limit || asMapForTest(page["Initiator"])["DisplayName"] != "webfile" {
			t.Fatalf("marker=%d max=%d page=%#v want=%v", marker, maxParts, page, want)
		}
		for index := range got {
			if asMapForTest(got[index])["PartNumber"] != want[index] {
				t.Fatalf("part %d = %#v want %d", index, got[index], want[index])
			}
		}
	})
}

func FuzzNoSuchUploadFaults(f *testing.F) {
	f.Add(uint8(0), "missing", false)
	f.Add(uint8(3), "", true)
	f.Fuzz(func(t *testing.T, operationSeed uint8, uploadID string, wrongKey bool) {
		if !utf8.ValidString(uploadID) || len(uploadID) > 128 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-fault-fuzz"}, nil)
		validID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-fault-fuzz", "Key": "key"}, nil).Output["UploadId"].(string)
		key := "key"
		if wrongKey {
			uploadID, key = validID, "wrong"
		} else {
			uploadID += "-missing"
		}
		operations := []string{"UploadPart", "CompleteMultipartUpload", "ListParts", "AbortMultipartUpload"}
		operation := operations[int(operationSeed)%len(operations)]
		input := map[string]any{"Bucket": "multipart-fault-fuzz", "Key": key, "UploadId": uploadID, "PartNumber": 1}
		if operation == "CompleteMultipartUpload" {
			input["MultipartUpload"] = map[string]any{"Parts": []any{}}
		}
		_, err := invoke(t, p, operation, input, []byte("part"))
		fault := asFault(t, err)
		if fault.Code != "NoSuchUpload" || fault.Message != "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed." || fault.Fields["UploadId"] != uploadID {
			t.Fatalf("%s fault = %#v", operation, fault)
		}
	})
}

func FuzzMultipartPartNumberFaults(f *testing.F) {
	f.Add(0, false)
	f.Add(1, false)
	f.Add(10000, false)
	f.Add(10001, true)
	f.Fuzz(func(t *testing.T, number int, missing bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "part-number-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "part-number-fuzz", "Key": "key"}, nil).Output["UploadId"].(string)
		if missing {
			uploadID = "missing"
		}
		_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "part-number-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": number}, []byte("part"))
		if missing {
			if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.Fields["UploadId"] != uploadID {
				t.Fatalf("missing upload fault = %#v", fault)
			}
			return
		}
		if number < 1 || number > 10000 {
			if fault := asFault(t, err); fault.Code != "InvalidArgument" || fault.Message != "Part number must be an integer between 1 and 10000, inclusive" || fault.Fields["ArgumentName"] != "partNumber" || fault.Fields["ArgumentValue"] != number {
				t.Fatalf("part number %d fault = %#v", number, fault)
			}
		} else if err != nil {
			t.Fatalf("valid part number %d: %v", number, err)
		}
	})
}

func FuzzMultipartCompletionFaults(f *testing.F) {
	f.Add(9, `"missing"`, false)
	f.Add(0, "", true)
	f.Add(10001, "wrong", false)
	f.Fuzz(func(t *testing.T, number int, etag string, empty bool) {
		if !utf8.ValidString(etag) || len(etag) > 128 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "completion-fault-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "completion-fault-fuzz", "Key": "key"}, nil).Output["UploadId"].(string)
		input := completeInput(uploadID)
		if !empty {
			input = completeInput(uploadID, map[string]any{"PartNumber": number, "ETag": etag})
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		if empty {
			if fault.Code != "InvalidRequest" || fault.Message != "You must specify at least one part" {
				t.Fatalf("empty completion fault = %#v", fault)
			}
		} else if fault.Code != "InvalidPart" || fault.Message != "One or more of the specified parts could not be found.  The part may not have been uploaded, or the specified entity tag may not match the part's entity tag." || fault.Fields["ETag"] != strings.Trim(strings.TrimSpace(etag), `"`) || fault.Fields["PartNumber"] != strconv.Itoa(number) || fault.Fields["UploadId"] != uploadID {
			t.Fatalf("missing part %d %q fault = %#v", number, etag, fault)
		}
	})
}

func FuzzCompleteMultipartChecksumTypeFault(f *testing.F) {
	f.Add([]byte("part"), false)
	f.Add([]byte("part"), true)
	f.Fuzz(func(t *testing.T, body []byte, composite bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "complete-checksum-type-fuzz"}, nil)
		selected, requested := "FULL_OBJECT", "COMPOSITE"
		if composite {
			selected, requested = requested, selected
		}
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "complete-checksum-type-fuzz", "Key": "key", "ChecksumAlgorithm": "CRC32", "ChecksumType": selected}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "complete-checksum-type-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": 1}, body)
		input := completeInput(uploadID, completedPart(1, part))
		input["ChecksumType"] = requested
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		want := "The upload was created using the " + selected + " checksum mode. The complete request must use the same checksum mode."
		if fault.Code != "InvalidRequest" || fault.Message != want || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("checksum type %s -> %s fault = %#v", selected, requested, fault)
		}
	})
}

func FuzzCompleteMultipartPreconditionFaults(f *testing.F) {
	f.Add([]byte("part"), uint8(0))
	f.Add([]byte("part"), uint8(1))
	f.Add([]byte("part"), uint8(2))
	f.Fuzz(func(t *testing.T, body []byte, mode uint8) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "complete-precondition-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "complete-precondition-fuzz", "Key": "key"}, nil).Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "complete-precondition-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": 1}, body)
		input := completeInput(uploadID, completedPart(1, part))
		header, detail := "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"
		switch mode % 3 {
		case 0:
			input["IfMatch"], input["IfNoneMatch"] = `"etag"`, "*"
			header, detail = "If-Match,If-None-Match", "Multiple conditional request headers present in the request"
		case 1:
			input["IfNoneMatch"] = `"etag"`
		case 2:
			input["IfMatch"] = "*"
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		if fault.Code != "NotImplemented" || fault.Message != "A header you provided implies functionality that is not implemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fault != "server" || fault.Fields["Header"] != header || fault.Fields["additionalMessage"] != detail {
			t.Fatalf("mode %d fault = %#v", mode%3, fault)
		}
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "complete-precondition-fuzz", "Key": "key", "UploadId": uploadID}, nil)
		if parts, _ := listed.Output["Parts"].([]any); len(parts) != 1 {
			t.Fatalf("mode %d parts = %#v", mode%3, listed.Output["Parts"])
		}
	})
}

func FuzzWritePreconditionFaults(f *testing.F) {
	for mode := uint8(0); mode < 3; mode++ {
		f.Add([]byte("new"), mode, false)
		f.Add([]byte("new"), mode, true)
	}
	f.Fuzz(func(t *testing.T, body []byte, mode uint8, copyObject bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "write-precondition-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-precondition-fuzz", "Key": "source"}, []byte("source"))
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-precondition-fuzz", "Key": "destination"}, []byte("old"))
		operation := "PutObject"
		input := map[string]any{"Bucket": "write-precondition-fuzz", "Key": "destination"}
		if copyObject {
			operation = "CopyObject"
			input["CopySource"] = "write-precondition-fuzz/source"
		}
		header, detail := "If-None-Match", "We don't accept the provided value of If-None-Match header for this API"
		switch mode % 3 {
		case 0:
			input["IfMatch"], input["IfNoneMatch"] = `"etag"`, "*"
			header, detail = "If-Match,If-None-Match", "Multiple conditional request headers present in the request"
		case 1:
			input["IfNoneMatch"] = `"etag"`
		case 2:
			input["IfMatch"] = "*"
		}
		_, err := invoke(t, p, operation, input, body)
		fault := asFault(t, err)
		if fault.Code != "NotImplemented" || fault.HTTPStatus != http.StatusNotImplemented || fault.Fields["Header"] != header || fault.Fields["additionalMessage"] != detail {
			t.Fatalf("%s mode %d fault = %#v", operation, mode%3, fault)
		}
		if got := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "write-precondition-fuzz", "Key": "destination"}, nil))); got != "old" {
			t.Fatalf("%s mode %d stored %q", operation, mode%3, got)
		}
	})
}

func FuzzWriteConditionFaultDetails(f *testing.F) {
	for mode := uint8(0); mode < 3; mode++ {
		f.Add([]byte("new"), mode, false)
		f.Add([]byte("new"), mode, true)
	}
	f.Fuzz(func(t *testing.T, body []byte, mode uint8, copyObject bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "write-condition-detail-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-condition-detail-fuzz", "Key": "source"}, []byte("source"))
		operation := "PutObject"
		input := map[string]any{"Bucket": "write-condition-detail-fuzz", "Key": "destination"}
		if copyObject {
			operation = "CopyObject"
			input["CopySource"] = "write-condition-detail-fuzz/source"
		}
		code, message, field, detail, status, existing := "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", "Condition", "If-Match", http.StatusPreconditionFailed, true
		switch mode % 3 {
		case 0:
			input["IfMatch"] = `"missing"`
			code, message, field, detail, status, existing = "NoSuchKey", "The specified key does not exist.", "Key", "destination", http.StatusNotFound, false
		case 1:
			input["IfMatch"] = `"wrong"`
		case 2:
			input["IfNoneMatch"] = "*"
			detail = "If-None-Match"
		}
		if existing {
			mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-condition-detail-fuzz", "Key": "destination"}, []byte("old"))
		}
		_, err := invoke(t, p, operation, input, body)
		fault := asFault(t, err)
		if fault.Code != code || fault.Message != message || fault.HTTPStatus != status || fault.Fields[field] != detail {
			t.Fatalf("%s mode %d fault = %#v", operation, mode%3, fault)
		}
		if existing {
			if got := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "write-condition-detail-fuzz", "Key": "destination"}, nil))); got != "old" {
				t.Fatalf("%s mode %d stored %q", operation, mode%3, got)
			}
		}
	})
}

func FuzzWriteIfMatchRequiresSingleETag(f *testing.F) {
	f.Add("wrong", []byte("new"), false)
	f.Add("other", []byte("new"), true)
	f.Fuzz(func(t *testing.T, decoy string, body []byte, copyObject bool) {
		if len(decoy)+len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "write-if-match-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-if-match-fuzz", "Key": "source"}, []byte("source"))
		seed := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "write-if-match-fuzz", "Key": "destination"}, []byte("old"))
		operation := "PutObject"
		input := map[string]any{"Bucket": "write-if-match-fuzz", "Key": "destination", "IfMatch": `"` + base64.RawURLEncoding.EncodeToString([]byte(decoy)) + `", ` + seed.Headers.Get("ETag")}
		if copyObject {
			operation = "CopyObject"
			input["CopySource"] = "write-if-match-fuzz/source"
		}
		_, err := invoke(t, p, operation, input, body)
		fault := asFault(t, err)
		if fault.Code != "PreconditionFailed" || fault.Fields["Condition"] != "If-Match" {
			t.Fatalf("%s list fault = %#v", operation, fault)
		}
		if got := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "write-if-match-fuzz", "Key": "destination"}, nil))); got != "old" {
			t.Fatalf("%s list stored %q", operation, got)
		}
	})
}

func FuzzCompleteMultipartConditionalConflicts(f *testing.F) {
	for mode := uint8(0); mode < 6; mode++ {
		f.Add([]byte("part"), mode)
	}
	f.Fuzz(func(t *testing.T, body []byte, mode uint8) {
		if len(body) > 4096 {
			t.Skip()
		}
		mode %= 6
		deps := spitest.Deps(t)
		p := s3.New(deps)
		const bucket, key = "complete-conditional-fuzz", "key"
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": bucket}, nil)
		put := func(value []byte) string {
			return mustInvoke(t, p, "PutObject", map[string]any{"Bucket": bucket, "Key": key}, value).Headers.Get("ETag")
		}
		seedETag := ""
		if mode == 1 || mode == 3 || mode == 4 || mode == 5 {
			seedETag = put([]byte("old"))
		}
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": bucket, "Key": key}, nil).Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": bucket, "Key": key, "UploadId": uploadID, "PartNumber": 1}, body)
		input := completeInput(uploadID, completedPart(1, part))
		code, message, status, condition, conflictKey := "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed, "If-Match", ""
		switch mode {
		case 0:
			input["IfMatch"] = `"missing"`
			code, message, status, condition, conflictKey = "NoSuchKey", "The specified key does not exist.", http.StatusNotFound, "", key
		case 1:
			input["IfMatch"] = `"wrong"`
		case 2:
			put([]byte("created"))
			input["IfNoneMatch"], condition = "*", "If-None-Match"
		case 3:
			mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": bucket, "Key": key}, nil)
			input["IfNoneMatch"] = "*"
			code, message, status, condition, conflictKey = "ConditionalRequestConflict", "The conditional request cannot succeed due to a conflicting operation against this resource.", http.StatusConflict, "If-None-Match", key
		case 4:
			_ = deps.Clock.Advance(2 * time.Second)
			input["IfMatch"] = put([]byte("changed"))
			code, message, status, conflictKey = "ConditionalRequestConflict", "The conditional request cannot succeed due to a conflicting operation against this resource.", http.StatusConflict, key
		case 5:
			input["IfMatch"] = `"wrong", ` + seedETag
			input["MultipartUpload"] = map[string]any{}
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		fault := asFault(t, err)
		if fault.Code != code || fault.Message != message || fault.HTTPStatus != status || fault.Fault != "client" || condition != "" && fault.Fields["Condition"] != condition || conflictKey != "" && fault.Fields["Key"] != conflictKey {
			t.Fatalf("mode %d fault = %#v", mode, fault)
		}
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": bucket, "Key": key, "UploadId": uploadID}, nil)
		if parts, _ := listed.Output["Parts"].([]any); len(parts) != 1 {
			t.Fatalf("mode %d parts = %#v", mode, listed.Output["Parts"])
		}
		if mode == 5 {
			if got := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": bucket, "Key": key}, nil))); got != "old" {
				t.Fatalf("If-Match list stored %q", got)
			}
		}
	})
}

func FuzzUploadPartContentMD5(f *testing.F) {
	f.Add([]byte("part"), "!", false)
	f.Add([]byte("part"), "AAAAAAAAAAAAAAAAAAAAAA==", false)
	sum := md5.Sum([]byte("part"))
	f.Add([]byte("part"), base64.StdEncoding.EncodeToString(sum[:]), false)
	f.Add([]byte("part"), "", false)
	f.Add([]byte("part"), "!", true)
	f.Fuzz(func(t *testing.T, body []byte, digest string, missing bool) {
		if len(body) > 4096 || len(digest) > 128 || !utf8.ValidString(digest) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-md5-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-md5-fuzz", "Key": "key"}, nil).Output["UploadId"].(string)
		if missing {
			uploadID = "missing"
		}
		_, err := invoke(t, p, "UploadPart", map[string]any{"Bucket": "upload-part-md5-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": 1, "ContentMD5": digest}, body)
		if missing {
			if fault := asFault(t, err); fault.Code != "NoSuchUpload" || fault.Fields["UploadId"] != uploadID {
				t.Fatalf("missing upload fault = %#v", fault)
			}
			return
		}
		if digest == "" {
			if err != nil {
				t.Fatalf("empty digest: %v", err)
			}
			return
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(digest)
		sum := md5.Sum(body)
		calculated := base64.StdEncoding.EncodeToString(sum[:])
		if decodeErr != nil || len(decoded) != md5.Size {
			if fault := asFault(t, err); fault.Code != "InvalidDigest" || fault.Message != "The Content-MD5 you specified was invalid." || fault.Fields["Content_MD5"] != digest {
				t.Fatalf("malformed digest %q fault = %#v", digest, fault)
			}
		} else if digest != calculated {
			if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The Content-MD5 you specified did not match what we received." || fault.Fields["ExpectedDigest"] != digest || fault.Fields["CalculatedDigest"] != calculated {
				t.Fatalf("mismatched digest %q fault = %#v", digest, fault)
			}
		} else if err != nil {
			t.Fatalf("valid digest %q: %v", digest, err)
		}
	})
}

func FuzzUploadPartChecksumFaults(f *testing.F) {
	body := []byte("part")
	sum := make([]byte, 4)
	binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
	f.Add(body, "!", uint8(0))
	f.Add(body, "AAAAAA==", uint8(0))
	f.Add(body, base64.StdEncoding.EncodeToString(sum), uint8(0))
	f.Add(body, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", uint8(1))
	f.Add(body, "", uint8(2))
	f.Fuzz(func(t *testing.T, body []byte, value string, mode uint8) {
		if len(body) > 4096 || len(value) > 128 || !utf8.ValidString(value) {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-checksum-fuzz"}, nil)
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "upload-part-checksum-fuzz", "Key": "key", "ChecksumAlgorithm": "CRC32"}, nil).Output["UploadId"].(string)
		input := map[string]any{"Bucket": "upload-part-checksum-fuzz", "Key": "key", "UploadId": uploadID, "PartNumber": 1}
		switch mode % 3 {
		case 0:
			input["ChecksumCRC32"] = value
		case 1:
			input["ChecksumSHA256"] = value
		case 2:
			input["ChecksumAlgorithm"] = "SHA256"
		}
		_, err := invoke(t, p, "UploadPart", input, body)
		if mode%3 == 2 || mode%3 == 1 && value != "" {
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "Checksum Type mismatch occurred, expected checksum Type: crc32, actual checksum Type: sha256" {
				t.Fatalf("algorithm mismatch fault = %#v", fault)
			}
			return
		}
		if value == "" {
			if err != nil {
				t.Fatalf("empty checksum: %v", err)
			}
			return
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(value)
		calculatedBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(calculatedBytes, crc32.ChecksumIEEE(body))
		calculated := base64.StdEncoding.EncodeToString(calculatedBytes)
		if decodeErr != nil || len(decoded) != len(calculatedBytes) {
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "Value for x-amz-checksum-crc32 header is invalid." {
				t.Fatalf("malformed checksum %q fault = %#v", value, fault)
			}
		} else if value != calculated {
			if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The CRC32 you specified did not match the calculated checksum." {
				t.Fatalf("mismatched checksum %q fault = %#v", value, fault)
			}
		} else if err != nil {
			t.Fatalf("valid checksum %q: %v", value, err)
		}
	})
}

func FuzzUploadPartSSECustomerKeyFaults(f *testing.F) {
	f.Add([]byte("part"), []byte("different-key"), uint8(0))
	f.Add([]byte("part"), []byte("different-key"), uint8(1))
	f.Add([]byte("part"), []byte("different-key"), uint8(2))
	f.Add([]byte("part"), []byte("different-key"), uint8(3))
	f.Fuzz(func(t *testing.T, body, seed []byte, mode uint8) {
		if len(body)+len(seed) > 8192 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "upload-part-sse-c-fuzz"}, nil)
		customerKey := bytes.Repeat([]byte{'a'}, 32)
		customerDigest := md5.Sum(customerKey)
		encryption := map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(customerKey), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(customerDigest[:])}
		key := "encrypted"
		create := map[string]any{"Bucket": "upload-part-sse-c-fuzz", "Key": key}
		if mode%4 != 1 {
			for name, value := range encryption {
				create[name] = value
			}
		} else {
			key = "plain"
			create["Key"] = key
		}
		uploadID := mustInvoke(t, p, "CreateMultipartUpload", create, nil).Output["UploadId"].(string)
		input := map[string]any{"Bucket": "upload-part-sse-c-fuzz", "Key": key, "UploadId": uploadID, "PartNumber": 1}
		if mode%4 != 0 {
			provided := encryption
			if mode%4 == 2 {
				wrong := sha256.Sum256(seed)
				if bytes.Equal(wrong[:], customerKey) {
					wrong[0] ^= 0xff
				}
				digest := md5.Sum(wrong[:])
				provided = map[string]any{"SSECustomerAlgorithm": "AES256", "SSECustomerKey": base64.StdEncoding.EncodeToString(wrong[:]), "SSECustomerKeyMD5": base64.StdEncoding.EncodeToString(digest[:])}
			}
			for name, value := range provided {
				input[name] = value
			}
		}
		_, err := invoke(t, p, "UploadPart", input, body)
		switch mode % 4 {
		case 0, 1:
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "The multipart upload initiate requested encryption. Subsequent part requests must include the appropriate encryption parameters." {
				t.Fatalf("missing or unexpected encryption fault = %#v", fault)
			}
		case 2:
			if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != "The provided encryption parameters did not match the ones used originally." {
				t.Fatalf("mismatched encryption fault = %#v", fault)
			}
		case 3:
			if err != nil {
				t.Fatalf("matching encryption: %v", err)
			}
		}
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "upload-part-sse-c-fuzz", "Key": key, "UploadId": uploadID}, nil)
		want := 0
		if mode%4 == 3 {
			want = 1
		}
		if got := len(listed.Output["Parts"].([]any)); got != want {
			t.Fatalf("stored parts = %d, want %d", got, want)
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

func FuzzDeleteObjectMissingKeyVersionIsIdempotent(f *testing.F) {
	f.Add("Enabled", "missing-version")
	f.Add("Suspended", "null")
	f.Add("Suspended", "missing-version")
	f.Fuzz(func(t *testing.T, status, version string) {
		if status != "Enabled" && status != "Suspended" {
			t.Skip()
		}
		if version == "" || len(version) > 1024 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "missing-version-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "missing-version-fuzz", "Status": status}, nil)
		deleted := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "missing-version-fuzz", "Key": "missing", "VersionId": version}, nil)
		if deleted.Status != http.StatusNoContent || deleted.Headers.Get("x-amz-version-id") != "" || deleted.Headers.Get("x-amz-delete-marker") != "" {
			t.Fatalf("delete response = %#v", deleted)
		}
		listed := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "missing-version-fuzz"}, nil).Output
		if len(asSliceForTest(listed["Versions"])) != 0 || len(asSliceForTest(listed["DeleteMarkers"])) != 0 {
			t.Fatalf("missing-key delete created state: %#v", listed)
		}
	})
}

func FuzzDeleteObjectUnversionedMissingKeyVersions(f *testing.F) {
	f.Add("null")
	f.Add("missing-version")
	f.Add("0")
	f.Fuzz(func(t *testing.T, version string) {
		if version == "" || len(version) > 1024 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "unversioned-delete-fuzz"}, nil)
		response, err := invoke(t, p, "DeleteObject", map[string]any{"Bucket": "unversioned-delete-fuzz", "Key": "missing", "VersionId": version}, nil)
		if version == "null" {
			if err != nil || response.Status != http.StatusNoContent || len(response.Headers) != 0 {
				t.Fatalf("null version response = %#v, %v", response, err)
			}
			return
		}
		fault := asFault(t, err)
		if response != nil || fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "versionId" || fault.Fields["ArgumentValue"] != version {
			t.Fatalf("version %q response = %#v, fault = %#v", version, response, fault)
		}
	})
}

func FuzzGetObjectUnversionedVersions(f *testing.F) {
	f.Add("null", "body")
	f.Add("missing-version", "content")
	f.Fuzz(func(t *testing.T, version, body string) {
		if version == "" || len(version) > 1024 || len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "unversioned-read-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "unversioned-read-fuzz", "Key": "key"}, []byte(body))
		response, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "unversioned-read-fuzz", "Key": "key", "VersionId": version}, nil)
		if version == "null" {
			if err != nil || string(readStream(t, response)) != body || response.Headers.Get("x-amz-version-id") != "" {
				t.Fatalf("null version response = %#v, %v", response, err)
			}
			return
		}
		fault := asFault(t, err)
		if response != nil || fault.Code != "InvalidArgument" || fault.Fields["ArgumentName"] != "versionId" || fault.Fields["ArgumentValue"] != version {
			t.Fatalf("version %q response = %#v, fault = %#v", version, response, fault)
		}
	})
}

func FuzzObjectByteRangeFaultDetails(f *testing.F) {
	f.Add("bytes=-0")
	f.Add("bytes=10-")
	f.Add("bytes=0--1")
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 256 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "range-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "range-fuzz", "Key": "key"}, []byte("0123456789"))
		response, err := invoke(t, p, "GetObject", map[string]any{"Bucket": "range-fuzz", "Key": "key", "Range": value}, nil)
		if err == nil {
			if body := readStream(t, response); len(body) > 10 {
				t.Fatalf("range %q returned %d bytes", value, len(body))
			}
			return
		}
		fault := asFault(t, err)
		if fault.Code != "InvalidRange" || fault.Fields["ActualObjectSize"] != "10" || fault.Fields["RangeRequested"] != value || fault.Headers.Get("Content-Range") != "bytes */10" {
			t.Fatalf("range %q fault = %#v", value, fault)
		}
	})
}

func FuzzSuspendedNullVersionReplacement(f *testing.F) {
	f.Add("null", uint8(1), false)
	f.Add("replacement", uint8(8), true)
	f.Fuzz(func(t *testing.T, body string, writes uint8, deleteCurrent bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "suspended-version-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "suspended-version-fuzz", "Status": "Enabled"}, nil)
		enabled := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "suspended-version-fuzz", "Key": "key"}, []byte("enabled"))
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "suspended-version-fuzz", "Status": "Suspended"}, nil)
		for range 1 + int(writes%8) {
			mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "suspended-version-fuzz", "Key": "key"}, []byte(body))
		}
		listed := mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "suspended-version-fuzz"}, nil).Output
		versions := asSliceForTest(listed["Versions"])
		if len(versions) != 2 || asMapForTest(versions[0])["VersionId"] != "null" || asMapForTest(versions[1])["VersionId"] != enabled.Headers.Get("x-amz-version-id") {
			t.Fatalf("suspended versions = %#v", listed)
		}
		if got := string(readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "suspended-version-fuzz", "Key": "key", "VersionId": "null"}, nil))); got != body {
			t.Fatalf("null body = %q, want %q", got, body)
		}
		if deleteCurrent {
			mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "suspended-version-fuzz", "Key": "key"}, nil)
			listed = mustInvoke(t, p, "ListObjectVersions", map[string]any{"Bucket": "suspended-version-fuzz"}, nil).Output
			if markers := asSliceForTest(listed["DeleteMarkers"]); len(markers) != 1 || asMapForTest(markers[0])["VersionId"] != "null" {
				t.Fatalf("null marker = %#v", listed)
			}
		}
	})
}

func FuzzDeleteObjectDirectoryPreconditions(f *testing.F) {
	f.Add(uint8(1), "etag")
	f.Add(uint8(2), "4")
	f.Add(uint8(4), "Sun, 06 Nov 1994 08:49:37 GMT")
	f.Add(uint8(7), "combined")
	f.Fuzz(func(t *testing.T, selected uint8, value string) {
		if len(value) > 1024 {
			t.Skip()
		}
		selected = 1 + selected%7
		if value == "" {
			value = "value"
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "delete-precondition-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "delete-precondition-fuzz", "Key": "key"}, []byte("body"))
		request := httptest.NewRequest(http.MethodDelete, "http://127.0.0.1/delete-precondition-fuzz/key", nil)
		expected := ""
		for index, header := range []string{"If-Match", "x-amz-if-match-size", "x-amz-if-match-last-modified-time"} {
			if selected&(1<<index) != 0 {
				request.Header.Set(header, value)
				expected = header
			}
		}
		_, err := p.Invoke(context.Background(), &spi.Request{Identity: ident(), Operation: "DeleteObject", Input: map[string]any{"Bucket": "delete-precondition-fuzz", "Key": "key"}, HTTP: request})
		fault := asFault(t, err)
		if fault.Code != "NotImplemented" || fault.Fields["Header"] != expected {
			t.Fatalf("selected=%d fault=%#v", selected, fault)
		}
		mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "delete-precondition-fuzz", "Key": "key"}, nil)
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
	f.Add("file.txt", "body", true, "metadata")
	f.Add("", "", false, "—_é")
	f.Fuzz(func(t *testing.T, filename, body string, created bool, metadata string) {
		if len(filename) > 512 || len(body) > 4096 || len(metadata) > 256 || !utf8.ValidString(filename) || !utf8.ValidString(metadata) || strings.IndexFunc(filename, func(r rune) bool { return r < ' ' || r == 0x7f }) >= 0 {
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
		metadata = "Ä" + metadata
		_ = writer.WriteField("x-amz-meta-value", metadata)
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
		decoded, err := new(mime.WordDecoder).DecodeHeader(got.Headers.Get("x-amz-meta-value"))
		if err != nil || decoded != metadata {
			t.Fatalf("metadata=%q decoded=%q: %v", metadata, decoded, err)
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

func FuzzObjectTaggingDeleteMarker(f *testing.F) {
	for operation := range 3 {
		f.Add(operation, false)
		f.Add(operation, true)
	}
	f.Fuzz(func(t *testing.T, choice int, explicit bool) {
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "tag-marker-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "tag-marker-fuzz", "Status": "Enabled"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "tag-marker-fuzz", "Key": "object"}, []byte("body"))
		marker := mustInvoke(t, p, "DeleteObject", map[string]any{"Bucket": "tag-marker-fuzz", "Key": "object"}, nil).Headers.Get("x-amz-version-id")
		operations := []string{"GetObjectTagging", "PutObjectTagging", "DeleteObjectTagging"}
		operation := operations[(choice%len(operations)+len(operations))%len(operations)]
		input := map[string]any{"Bucket": "tag-marker-fuzz", "Key": "object", "TagSet": []any{}}
		if explicit {
			input["VersionId"] = marker
		}
		_, err := invoke(t, p, operation, input, nil)
		fault := asFault(t, err)
		if fault.Code != "MethodNotAllowed" || fault.Fields["Method"] != strings.ToUpper(strings.TrimSuffix(operation, "ObjectTagging")) || fault.Fields["ResourceType"] != "DeleteMarker" {
			t.Fatalf("%s explicit=%t: %#v", operation, explicit, fault)
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
	f.Add(uint8(0), true, "body", false)
	f.Add(uint8(1), true, "kms", false)
	f.Add(uint8(1), true, "managed", true)
	f.Add(uint8(2), false, "dsse", false)
	f.Add(uint8(3), true, "invalid", false)
	f.Fuzz(func(t *testing.T, algorithmIndex uint8, bucketKey bool, body string, managed bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		algorithms := []string{"AES256", "aws:kms", "aws:kms:dsse", "invalid"}
		algorithm := algorithms[int(algorithmIndex)%len(algorithms)]
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "encryption-fuzz"}, nil)
		input := map[string]any{"Bucket": "encryption-fuzz", "Key": "object", "ServerSideEncryption": algorithm, "BucketKeyEnabled": bucketKey}
		keyID := "arn:aws:kms:us-east-1:123456789012:key/fuzz"
		if algorithm == "aws:kms" && !managed {
			spitest.SeedKMSKey(t, deps, ident(), keyID, "Enabled")
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
		if algorithm == "aws:kms" {
			gotKey := get.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id")
			if !managed && gotKey != keyID || managed && !strings.HasPrefix(gotKey, "arn:aws:kms:us-east-1:123456789012:key/") || bucketKey && get.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true" {
				t.Fatalf("stored kms headers=%v", get.Headers)
			}
		} else if get.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "" {
			t.Fatalf("non-KMS bucket key header=%v", get.Headers)
		}
	})
}

func FuzzGetObjectResponseOverrides(f *testing.F) {
	for index, value := range []string{"max-age=74", `attachment; filename="foo.jpg"`, "identity", "de-DE", "image/jpeg", "Wed, 21 Oct 2015 07:28:00 GMT"} {
		f.Add(uint8(index), value, index%2 == 0)
	}
	f.Fuzz(func(t *testing.T, fieldIndex uint8, value string, queryName bool) {
		if value == "" || len(value) > 256 || !utf8.ValidString(value) || strings.ContainsAny(value, "\r\n") {
			t.Skip()
		}
		fields := []struct{ input, query, header string }{
			{"ResponseCacheControl", "response-cache-control", "Cache-Control"},
			{"ResponseContentDisposition", "response-content-disposition", "Content-Disposition"},
			{"ResponseContentEncoding", "response-content-encoding", "Content-Encoding"},
			{"ResponseContentLanguage", "response-content-language", "Content-Language"},
			{"ResponseContentType", "response-content-type", "Content-Type"},
			{"ResponseExpires", "response-expires", "Expires"},
		}
		field := fields[int(fieldIndex)%len(fields)]
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "override-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "override-fuzz", "Key": "object", "ContentType": "application/json"}, []byte("body"))
		input := map[string]any{"Bucket": "override-fuzz", "Key": "object"}
		if queryName {
			input[field.query] = value
		} else {
			input[field.input] = value
		}
		response := mustInvoke(t, p, "GetObject", input, nil)
		if response.Headers.Get(field.header) != value || string(readStream(t, response)) != "body" {
			t.Fatalf("override %s = %q", field.header, response.Headers.Get(field.header))
		}
		stored := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "override-fuzz", "Key": "object"}, nil)
		if stored.Headers.Get("Content-Type") != "application/json" {
			t.Fatalf("stored content type = %q", stored.Headers.Get("Content-Type"))
		}
	})
}

func FuzzUserMetadataRFC2047(f *testing.F) {
	for _, value := range []string{"S3", "—_é_2?.pdf", "\x00\x01\x02\x03", "�������"} {
		f.Add(value)
	}
	f.Fuzz(func(t *testing.T, suffix string) {
		if len(suffix) > 256 || !utf8.ValidString(suffix) {
			t.Skip()
		}
		value := "Ä" + suffix
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "rfc2047-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{
			"Bucket": "rfc2047-fuzz", "Key": "object", "Metadata": map[string]any{"value": mime.BEncoding.Encode("UTF-8", value)},
		}, []byte("body"))
		response := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "rfc2047-fuzz", "Key": "object"}, nil)
		wire := response.Headers.Get("x-amz-meta-value")
		decoded, err := new(mime.WordDecoder).DecodeHeader(wire)
		if err != nil || decoded != value {
			t.Fatalf("metadata %q decoded to %q from %q: %v", value, decoded, wire, err)
		}
	})
}

func FuzzExplicitKMSKeyValidation(f *testing.F) {
	f.Add(false, "Enabled", uint8(0))
	f.Add(true, "Enabled", uint8(1))
	f.Add(false, "Disabled", uint8(2))
	f.Add(false, "PendingDeletion", uint8(0))
	f.Add(false, "PendingImport", uint8(1))
	f.Fuzz(func(t *testing.T, missing bool, state string, operationIndex uint8) {
		if len(state) > 32 {
			t.Skip()
		}
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "kms-validation-fuzz"}, nil)
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "kms-validation-fuzz", "Key": "source"}, []byte("source"))
		keyID := "arn:aws:kms:us-east-1:123456789012:key/fuzz-validation"
		if !missing {
			spitest.SeedKMSKey(t, deps, ident(), keyID, state)
		}
		operation := []string{"PutObject", "CreateMultipartUpload", "CopyObject"}[int(operationIndex)%3]
		input := map[string]any{"Bucket": "kms-validation-fuzz", "Key": "destination", "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": keyID}
		if operation == "CopyObject" {
			input["CopySource"] = "kms-validation-fuzz/source"
		}
		_, err := invoke(t, p, operation, input, []byte("body"))
		if missing {
			if fault := asFault(t, err); fault.Code != "KMS.NotFoundException" {
				t.Fatalf("missing key fault = %#v", fault)
			}
			return
		}
		switch state {
		case "Enabled":
			if err != nil {
				t.Fatal(err)
			}
		case "PendingDeletion":
			if fault := asFault(t, err); fault.Code != "KMS.KMSInvalidStateException" {
				t.Fatalf("pending deletion fault = %#v", fault)
			}
		default:
			if fault := asFault(t, err); fault.Code != "KMS.DisabledException" {
				t.Fatalf("disabled fault = %#v", fault)
			}
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
	f.Add(uint8(0), false, "body", "plain")
	f.Add(uint8(1), true, "kms", "metadata")
	f.Add(uint8(2), false, "dsse", "\x00\xff")
	f.Add(uint8(3), true, "invalid", "ignored")
	f.Fuzz(func(t *testing.T, algorithmIndex uint8, bucketKey bool, body, metadata string) {
		if len(body) > 4096 || len(metadata) > 512 {
			t.Skip()
		}
		algorithms := []string{"AES256", "aws:kms", "aws:kms:dsse", "invalid"}
		algorithm := algorithms[int(algorithmIndex)%len(algorithms)]
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-encryption-fuzz"}, nil)
		metadataValue := base64.RawStdEncoding.EncodeToString([]byte(metadata))
		input := map[string]any{"Bucket": "multipart-encryption-fuzz", "Key": "object", "ChecksumAlgorithm": "CRC64NVME", "ServerSideEncryption": algorithm, "BucketKeyEnabled": bucketKey, "ContentType": "application/octet-stream", "Metadata": map[string]any{"Case": metadataValue}, "WebsiteRedirectLocation": "/multipart"}
		keyID := "arn:aws:kms:us-east-1:123456789012:key/multipart-fuzz"
		if algorithm == "aws:kms" {
			spitest.SeedKMSKey(t, deps, ident(), keyID, "Enabled")
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
		kms := strings.HasPrefix(algorithm, "aws:kms")
		if kms && (completed.Headers.Get("x-amz-checksum-crc64nvme") != "" || completed.Headers.Get("x-amz-checksum-type") != "" || completed.Output["ChecksumCRC64NVME"] != nil || completed.Output["ChecksumType"] != nil) {
			t.Fatalf("KMS completion checksum response headers=%v output=%#v", completed.Headers, completed.Output)
		}
		get := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "multipart-encryption-fuzz", "Key": "object", "ChecksumMode": "ENABLED"}, nil)
		if get.Headers.Get("x-amz-server-side-encryption") != algorithm || get.Headers.Get("x-amz-checksum-crc64nvme") == "" || get.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" || get.Headers.Get("Content-Type") != "application/octet-stream" || get.Headers.Get("x-amz-meta-case") != metadataValue || get.Headers.Get("x-amz-website-redirect-location") != "/multipart" || string(readStream(t, get)) != body {
			t.Fatalf("stored multipart headers=%v", get.Headers)
		}
		if algorithm == "aws:kms" && (get.Headers.Get("x-amz-server-side-encryption-aws-kms-key-id") != keyID || bucketKey && get.Headers.Get("x-amz-server-side-encryption-bucket-key-enabled") != "true") {
			t.Fatalf("stored kms headers=%v", get.Headers)
		}
	})
}

func FuzzMultipartWithoutChecksum(f *testing.F) {
	f.Add("plain", false)
	f.Add("checked part", true)
	f.Fuzz(func(t *testing.T, body string, checksumPart bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-no-checksum-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-no-checksum-fuzz", "Key": "object"}, nil)
		if created.Output["ChecksumAlgorithm"] != nil || created.Output["ChecksumType"] != nil {
			t.Fatalf("create = %#v", created.Output)
		}
		uploadID := created.Output["UploadId"].(string)
		partInput := map[string]any{"Bucket": "multipart-no-checksum-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}
		if checksumPart {
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE([]byte(body)))
			partInput["ChecksumAlgorithm"], partInput["ChecksumCRC32"] = "CRC32", base64.StdEncoding.EncodeToString(sum)
		}
		part := mustInvoke(t, p, "UploadPart", partInput, []byte(body))
		listed := mustInvoke(t, p, "ListParts", map[string]any{"Bucket": "multipart-no-checksum-fuzz", "Key": "object", "UploadId": uploadID}, nil)
		if listed.Output["ChecksumAlgorithm"] != nil || listed.Output["ChecksumType"] != nil || listed.Output["Parts"].([]any)[0].(map[string]any)["ChecksumCRC32"] != nil {
			t.Fatalf("list = %#v", listed.Output)
		}
		completed := mustInvoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completedPart(1, part)), nil)
		if completed.Output["ChecksumCRC64NVME"] != nil || completed.Output["ChecksumType"] != nil {
			t.Fatalf("complete = %#v", completed.Output)
		}
		got := mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "multipart-no-checksum-fuzz", "Key": "object", "ChecksumMode": "ENABLED"}, nil)
		if got.Headers.Get("x-amz-checksum-crc32") != "" || got.Headers.Get("x-amz-checksum-crc64nvme") != "" || string(readStream(t, got)) != body {
			t.Fatalf("get headers=%v", got.Headers)
		}
	})
}

func FuzzCompositeMultipartPartChecksumRequired(f *testing.F) {
	f.Add("plain", false)
	f.Add("alternate", true)
	f.Fuzz(func(t *testing.T, body string, alternate bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-composite-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-composite-fuzz", "Key": "object", "ChecksumAlgorithm": "CRC32"}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-composite-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}, []byte(body))
		completed := completedPart(1, part).(map[string]any)
		wantCode := "InvalidRequest"
		wantMessage := "The upload was created using a crc32 checksum. The complete request must include the checksum for each part. It was missing for part 1 in the request."
		if alternate {
			completed["ChecksumSHA256"] = "AA=="
			wantCode, wantMessage = "BadDigest", "The sha256 you specified for part 1 did not match what we received."
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", completeInput(uploadID, completed), nil)
		if fault := asFault(t, err); fault.Code != wantCode || fault.Message != wantMessage || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("alternate=%t fault=%#v", alternate, fault)
		}
	})
}

func FuzzMultipartObjectSize(f *testing.F) {
	f.Add("sized", int64(0))
	f.Add("sized", int64(4))
	f.Add("", int64(-1))
	f.Fuzz(func(t *testing.T, body string, advertised int64) {
		if len(body) > 4096 || advertised < -4096 || advertised > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-size-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-size-fuzz", "Key": "object"}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-size-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}, []byte(body))
		input := completeInput(uploadID, completedPart(1, part))
		input["MpuObjectSize"] = strconv.FormatInt(advertised, 10)
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if advertised == 0 || advertised == int64(len(body)) {
			if err != nil {
				t.Fatalf("size=%d body=%d: %v", advertised, len(body), err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "InvalidRequest" || fault.Message != fmt.Sprintf("The provided 'x-amz-mp-object-size' header value %d does not match what was computed: %d", advertised, len(body)) || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("size=%d body=%d fault=%#v", advertised, len(body), fault)
		}
	})
}

func FuzzCompositeAggregateChecksumIgnored(f *testing.F) {
	f.Add("body", "AA==")
	f.Add("", "wrong")
	f.Fuzz(func(t *testing.T, body, aggregate string) {
		if len(body) > 4096 || len(aggregate) > 256 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-aggregate-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-aggregate-fuzz", "Key": "object", "ChecksumAlgorithm": "CRC32"}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-aggregate-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}, []byte(body))
		input := completeInput(uploadID, completedPartWithChecksum(1, part, "ChecksumCRC32", "x-amz-checksum-crc32"))
		input["ChecksumCRC32"] = aggregate
		completed := mustInvoke(t, p, "CompleteMultipartUpload", input, nil)
		if !strings.HasSuffix(fmt.Sprint(completed.Output["ChecksumCRC32"]), "-1") || completed.Output["ChecksumType"] != "COMPOSITE" {
			t.Fatalf("aggregate=%q output=%#v", aggregate, completed.Output)
		}
	})
}

func FuzzAlternateMultipartChecksum(f *testing.F) {
	f.Add("body", "AAAAAA==")
	f.Add("", "wrong")
	f.Add("body", "")
	f.Fuzz(func(t *testing.T, body, alternate string) {
		if len(body) > 4096 || len(alternate) > 256 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-alternate-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-alternate-fuzz", "Key": "object", "ChecksumAlgorithm": "SHA256"}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-alternate-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}, []byte(body))
		input := completeInput(uploadID, completedPartWithChecksum(1, part, "ChecksumSHA256", "x-amz-checksum-sha256"))
		input["ChecksumCRC32"] = alternate
		completed, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if alternate == "" {
			if err != nil || completed.Output["ChecksumType"] != "COMPOSITE" {
				t.Fatalf("empty alternate: output=%#v err=%v", completed, err)
			}
			return
		}
		if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The sha256 you specified did not match the calculated checksum." || fault.HTTPStatus != http.StatusBadRequest {
			t.Fatalf("alternate=%q fault=%#v", alternate, fault)
		}
	})
}

func FuzzFullObjectChecksumType(f *testing.F) {
	f.Add("body", false)
	f.Add("", true)
	f.Fuzz(func(t *testing.T, body string, explicit bool) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "multipart-full-type-fuzz"}, nil)
		created := mustInvoke(t, p, "CreateMultipartUpload", map[string]any{"Bucket": "multipart-full-type-fuzz", "Key": "object", "ChecksumAlgorithm": "CRC32", "ChecksumType": "FULL_OBJECT"}, nil)
		uploadID := created.Output["UploadId"].(string)
		part := mustInvoke(t, p, "UploadPart", map[string]any{"Bucket": "multipart-full-type-fuzz", "Key": "object", "UploadId": uploadID, "PartNumber": 1}, []byte(body))
		input := completeInput(uploadID, completedPart(1, part))
		input["ChecksumCRC32"] = part.Headers.Get("x-amz-checksum-crc32")
		if explicit {
			input["ChecksumType"] = "FULL_OBJECT"
			if completed := mustInvoke(t, p, "CompleteMultipartUpload", input, nil); completed.Output["ChecksumType"] != "FULL_OBJECT" {
				t.Fatalf("explicit output=%#v", completed.Output)
			}
			return
		}
		_, err := invoke(t, p, "CompleteMultipartUpload", input, nil)
		if fault := asFault(t, err); fault.Code != "BadDigest" || fault.Message != "The crc32 you specified did not match the calculated checksum." {
			t.Fatalf("implicit fault=%#v", fault)
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

func FuzzCopyObjectChecksums(f *testing.F) {
	f.Add(uint8(0), []byte("inherit"))
	f.Add(uint8(1), []byte("crc32"))
	f.Add(uint8(2), []byte("crc32c"))
	f.Fuzz(func(t *testing.T, mode uint8, body []byte) {
		if len(body) > 4096 {
			t.Skip()
		}
		p := s3.New(spitest.Deps(t))
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "copy-checksum-fuzz"}, nil)
		sourceSum := sha256.Sum256(body)
		sourceChecksum := base64.StdEncoding.EncodeToString(sourceSum[:])
		mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "copy-checksum-fuzz", "Key": "source", "ChecksumSHA256": sourceChecksum}, body)

		input := map[string]any{"Bucket": "copy-checksum-fuzz", "Key": "destination", "CopySource": "copy-checksum-fuzz/source"}
		output, header, expected := "ChecksumSHA256", "x-amz-checksum-sha256", sourceChecksum
		switch mode % 3 {
		case 1:
			input["ChecksumAlgorithm"], output, header = "CRC32", "ChecksumCRC32", "x-amz-checksum-crc32"
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.ChecksumIEEE(body))
			expected = base64.StdEncoding.EncodeToString(sum)
		case 2:
			input["ChecksumAlgorithm"], output, header = "CRC32C", "ChecksumCRC32C", "x-amz-checksum-crc32c"
			sum := make([]byte, 4)
			binary.BigEndian.PutUint32(sum, crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
			expected = base64.StdEncoding.EncodeToString(sum)
		}
		copied := mustInvoke(t, p, "CopyObject", input, nil)
		modifiedValue, ok := copied.Output["LastModified"].(string)
		modified, modifiedErr := time.Parse(time.RFC3339, modifiedValue)
		if copied.Output[output] != expected || copied.Output["ChecksumType"] != "FULL_OBJECT" || !ok || modifiedErr != nil {
			t.Fatalf("copy output = %#v", copied.Output)
		}
		head := mustInvoke(t, p, "HeadObject", map[string]any{"Bucket": "copy-checksum-fuzz", "Key": "destination", "ChecksumMode": "ENABLED"}, nil)
		stored, storedErr := http.ParseTime(head.Headers.Get("Last-Modified"))
		if storedErr != nil || !modified.Equal(stored) || head.Headers.Get(header) != expected || head.Headers.Get("x-amz-checksum-type") != "FULL_OBJECT" {
			t.Fatalf("copy headers = %v", head.Headers)
		}
	})
}

func FuzzCopySourcePreconditions(f *testing.F) {
	for mode := uint8(0); mode < 7; mode++ {
		f.Add(mode, uint16(mode+1))
	}
	f.Fuzz(func(t *testing.T, mode uint8, secondsSeed uint16) {
		deps := spitest.Deps(t)
		p := s3.New(deps)
		mustInvoke(t, p, "CreateBucket", map[string]any{"Bucket": "copy-condition-fuzz"}, nil)
		mustInvoke(t, p, "PutBucketVersioning", map[string]any{"Bucket": "copy-condition-fuzz", "Status": "Enabled"}, nil)
		put := mustInvoke(t, p, "PutObject", map[string]any{"Bucket": "copy-condition-fuzz", "Key": "source"}, []byte("source"))
		seconds := int(secondsSeed%60) + 1
		_ = deps.Clock.Advance(time.Duration(seconds) * time.Second)
		modified := time.Unix(0, 0).UTC()
		readStream(t, mustInvoke(t, p, "GetObject", map[string]any{"Bucket": "copy-condition-fuzz", "Key": "source", "IfModifiedSince": modified.Add(time.Duration(seconds+1) * time.Second).Format(http.TimeFormat)}, nil))
		conditions := map[string]any{}
		wantSuccess := false
		faultCondition := ""
		switch mode % 7 {
		case 0:
			conditions["CopySourceIfMatch"] = `"wrong", ` + put.Headers.Get("ETag")
			faultCondition = "x-amz-copy-source-If-Match"
		case 1:
			conditions["CopySourceIfModifiedSince"] = modified.Add(time.Duration(seconds+1) * time.Second).Format(http.TimeFormat)
			wantSuccess = true
		case 2:
			conditions["CopySourceIfNoneMatch"] = `"wrong"`
			conditions["CopySourceIfModifiedSince"] = modified.Format(http.TimeFormat)
			faultCondition = "x-amz-copy-source-If-Modified-Since"
		case 3:
			conditions["CopySourceIfMatch"] = put.Headers.Get("ETag")
			conditions["CopySourceIfNoneMatch"] = put.Headers.Get("ETag")
			conditions["CopySourceIfModifiedSince"] = modified.Add(-time.Second).Format(http.TimeFormat)
			conditions["CopySourceIfUnmodifiedSince"] = modified.Add(-time.Second).Format(http.TimeFormat)
			wantSuccess = true
		case 4:
			conditions["CopySourceIfUnmodifiedSince"] = modified.Add(-time.Second).Format(http.TimeFormat)
			faultCondition = "x-amz-copy-source-If-Unmodified-Since"
		case 5:
			conditions["CopySourceIfNoneMatch"] = put.Headers.Get("ETag")
			faultCondition = "x-amz-copy-source-If-None-Match"
		case 6:
			conditions["CopySourceIfModifiedSince"] = modified.Add(-time.Second).Format(http.TimeFormat)
			wantSuccess = true
		}
		input := map[string]any{"Bucket": "copy-condition-fuzz", "Key": "destination", "CopySource": "copy-condition-fuzz/source"}
		for key, value := range conditions {
			input[key] = value
		}
		_, err := invoke(t, p, "CopyObject", input, nil)
		if wantSuccess && err != nil {
			t.Fatalf("mode %d: %v", mode%7, err)
		}
		if !wantSuccess {
			fault := asFault(t, err)
			if fault.Code != "PreconditionFailed" || fault.Message != "At least one of the pre-conditions you specified did not hold" || fault.Fields["Condition"] != faultCondition {
				t.Fatalf("mode %d: %#v", mode%7, fault)
			}
		}
	})
}
