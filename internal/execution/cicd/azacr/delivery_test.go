package azacr_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/azacr"
)

func TestAZ_ACR_TASK(t *testing.T) {
	reg, err := azacr.New("")
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("acr-" + t.Name())
	d, err := reg.QuickTask("app", "v1", nonce)
	if err != nil || d == "" {
		t.Fatalf("digest %q err %v", d, err)
	}
	got, err := reg.GetBlob("app", "v1")
	if err != nil || string(got) != string(nonce) {
		t.Fatalf("blob %q err %v", got, err)
	}
	if _, err := reg.QuickTask("app", "v2", nil); err == nil {
		t.Fatal("empty source must fail")
	}
}
