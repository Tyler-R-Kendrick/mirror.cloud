package s3_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/s3"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spitest"
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
		_, err := invokeAs(t, p, spi.Identity{Account: account, Region: region}, "CreateBucket", map[string]any{"Bucket": "global-name"}, nil)
		if account == owner.Account && region == owner.Region {
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
