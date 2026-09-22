package delivery_test

import (
	"testing"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/execution/cicd/delivery"
)

func TestPublishServesNonce(t *testing.T) {
	s, err := delivery.Publish(map[string][]byte{"index.html": []byte("hello-nonce")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	b, err := s.Get("index.html")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello-nonce" {
		t.Fatalf("got %q", b)
	}
}
