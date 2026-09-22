package oci_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/oci"
)

func TestOCI_PUSH_PULL(t *testing.T) {
	reg, err := oci.New("")
	if err != nil {
		t.Fatal(err)
	}
	nonce := []byte("oci-" + t.Name())
	d, err := reg.Push("app/web:v1", nonce)
	if err != nil || d == "" {
		t.Fatalf("%q %v", d, err)
	}
	body, dig, err := reg.Pull("app/web:v1")
	if err != nil || dig != d || string(body) != string(nonce) {
		t.Fatalf("pull body=%q dig=%q want=%q/%q err=%v", body, dig, nonce, d, err)
	}
	if _, _, err := reg.Pull("app/web:missing"); err == nil {
		t.Fatal("want miss")
	}
}
