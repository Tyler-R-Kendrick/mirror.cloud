//go:build unix

package main

import (
	"net/http"
	"syscall"
	"testing"
	"time"
)

// A stop by signal must be a return from serve, so that the deferred save and
// close in cmdUp run. Before, SIGTERM ended the process outright and --persist
// wrote nothing, which is what `docker stop` does.
func TestServeReturnsWhenSignalled(t *testing.T) {
	up := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serve("127.0.0.1:0", http.NotFoundHandler(), nil, func() { close(up) })
	}()
	select {
	case <-up:
	case <-time.After(5 * time.Second):
		t.Fatal("serve never became ready")
	}
	// The handler is installed before ready runs, so this signal is caught.
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v after SIGTERM; want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after SIGTERM")
	}
}
