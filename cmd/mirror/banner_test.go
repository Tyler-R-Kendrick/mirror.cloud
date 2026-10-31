package main

import (
	"bufio"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestMain lets a test run this binary as `mirror` itself: with
// MIRROR_TEST_ARGS set, the process is main() with those arguments, so a test
// sees what a caller sees -- the exit status and everything written to stderr.
func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv("MIRROR_TEST_ARGS"); ok {
		os.Args = append([]string{"mirror"}, strings.Fields(args)...)
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// mirrorCommand is `mirror args...`, run as this test binary.
func mirrorCommand(t *testing.T, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "MIRROR_TEST_ARGS="+strings.Join(args, " "))
	return cmd
}

// A test runner that waits for the banner treats it as "the port is mine and
// accepting". So an instance that cannot bind its port prints no banner, only
// the bind error, and exits non-zero: printing the banner first told such a
// runner to talk to whatever already held the port.
func TestUpOnAnOccupiedPortPrintsNoBannerAndFails(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	out, err := mirrorCommand(t, "up", "s3", "--bind", held.Addr().String()).CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() == 0 {
		t.Fatalf("mirror up on an occupied port: %v, want a non-zero exit\n%s", err, out)
	}
	if strings.Contains(string(out), "listening on") || strings.Contains(string(out), "services:") {
		t.Errorf("mirror up on an occupied port printed the banner:\n%s", out)
	}
	if !strings.Contains(string(out), "address already in use") {
		t.Errorf("mirror up on an occupied port did not say why:\n%s", out)
	}
}

// The banner is printed once the port is bound, so a test runner that waits
// for it can connect the moment it appears, with no retry. The listener seam
// holds the bind until the test has seen which comes first, the banner or the
// bind, so a banner printed before the bind fails here every time rather than
// whenever the runner happens to dial inside the gap.
func TestUpBannerAppearsOnlyOnceItsPortAccepts(t *testing.T) {
	free, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := free.Addr().String()
	_ = free.Close()

	binding, proceed := make(chan struct{}), make(chan struct{})
	bound := make(chan net.Listener, 1)
	oldListen, oldStderr := listen, os.Stderr
	listen = func(network, address string) (net.Listener, error) {
		close(binding)
		<-proceed
		ln, err := net.Listen(network, address)
		if err == nil {
			bound <- ln
		}
		return ln, err
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	done := make(chan error, 1)
	go func() { done <- cmdUp([]string{"s3", "--bind", addr}) }()
	released := false
	release := func() {
		if !released {
			released = true
			close(proceed)
		}
	}
	defer func() {
		release()
		select {
		case ln := <-bound:
			_ = ln.Close()
		case <-done:
			done <- nil
		}
		<-done
		_ = w.Close()
		os.Stderr, listen = oldStderr, oldListen
	}()

	lines := make(chan string, 64)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
	}()
	for banner := ""; banner == ""; {
		select {
		case <-binding:
			binding = nil
			release()
		case line, ok := <-lines:
			if !ok {
				t.Fatal("stderr closed before the banner")
			}
			if strings.Contains(line, " listening on ") {
				banner = line
			}
		case err := <-done:
			done <- err
			t.Fatalf("mirror up returned before the banner: %v", err)
		}
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("the banner appeared, and %s refused a connection: %v", addr, err)
	}
	_ = conn.Close()
}
