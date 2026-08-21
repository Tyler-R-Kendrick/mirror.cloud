package proxy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordReplayRoundTrip(t *testing.T) {
	const wantBody = `{"bucket":"widgets","n":3}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/widgets" {
			t.Errorf("path %s", r.URL.Path)
		}
		got, _ := io.ReadAll(r.Body)
		if string(got) != `{"x":1}` {
			t.Errorf("upstream body %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(wantBody))
	}))

	path := filepath.Join(t.TempDir(), "cloud.cassette")
	cas, err := NewFileCassette(path)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := NewTransport(ModeRecord, cas, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: rec}
	resp, err := client.Post(srv.URL+"/v1/widgets?b=2&a=1", "application/json", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if string(body) != wantBody {
		t.Fatalf("body %s", body)
	}
	if err := cas.Flush(); err != nil {
		t.Fatal(err)
	}
	srv.Close()

	cas2, err := NewFileCassette(path)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := NewTransport(ModeReplay, cas2, "")
	if err != nil {
		t.Fatal(err)
	}
	client2 := &http.Client{Transport: rep}
	resp2, err := client2.Post("http://closed.invalid/v1/widgets?a=1&b=2", "application/json", strings.NewReader(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}
	body2, err := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp2.StatusCode != http.StatusCreated {
		t.Fatalf("replay status %d", resp2.StatusCode)
	}
	if !bytes.Equal(body2, []byte(wantBody)) {
		t.Fatalf("replay body %s", body2)
	}
}

func TestSecretsScrubbedFromCassette(t *testing.T) {
	const (
		secret  = "s3cr3t-value-do-not-leak"
		token   = "sts-session-token-ABCDEFGH"
		auth    = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef"
		pattern = "sk-live-42"
	)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_SESSION_TOKEN", token)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Echo-Auth", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"secret":"` + secret + `","pat":"` + pattern + `"}`))
	}))
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "secrets.cassette")
	cas, err := NewFileCassette(path)
	if err != nil {
		t.Fatal(err)
	}
	cas.Scrub = Scrub{Patterns: []string{`sk-live-\d+`}}
	tr, err := NewTransport(ModeRecord, cas, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	tr.Scrub = cas.Scrub

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/x?X-Amz-Security-Token="+token, strings.NewReader(`{"key":"`+secret+`","pat":"`+pattern+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("X-Amz-Security-Token", token)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err := cas.Flush(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, s := range []string{secret, token, auth, "AKIAIOSFODNN7EXAMPLE", pattern, "sk-live-42"} {
		if strings.Contains(text, s) {
			t.Errorf("cassette leaked %q\n%s", s, text)
		}
	}
	if !strings.Contains(text, redacted) {
		t.Errorf("expected %s in cassette", redacted)
	}
}

func TestHybridReplayElseRecord(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(r.URL.Path))
	}))
	defer srv.Close()

	cas, err := NewFileCassette(filepath.Join(t.TempDir(), "h.cassette"))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := NewTransport(ModeRecord, cas, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: rec}
	if _, err := client.Get(srv.URL + "/a"); err != nil {
		t.Fatal(err)
	}
	if hits != 1 {
		t.Fatalf("hits %d", hits)
	}

	hy, err := NewTransport(ModeHybrid, cas, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	clientH := &http.Client{Transport: hy}
	resp, err := clientH.Get(srv.URL + "/a")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "/a" {
		t.Fatalf("replayed %s", got)
	}
	resp, err = clientH.Get(srv.URL + "/b")
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "/b" {
		t.Fatalf("recorded %s", got)
	}
	if hits != 2 {
		t.Fatalf("hits %d, want 2 (replay /a, record /b)", hits)
	}
}

func TestModeOff(t *testing.T) {
	tr, err := NewTransport(ModeOff, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://127.0.0.1/x", nil)
	_, err = tr.RoundTrip(req)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPassthroughDoesNotRecord(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "p.cassette")
	cas, err := NewFileCassette(path)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := NewTransport(ModePassthrough, cas, srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL + "/z")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if _, ok := cas.Lookup(lookupKey(http.MethodGet, "/z", "", nil, nil)); ok {
		t.Fatal("passthrough recorded")
	}
}

func TestReplayMiss(t *testing.T) {
	cas, err := NewFileCassette(filepath.Join(t.TempDir(), "empty.cassette"))
	if err != nil {
		t.Fatal(err)
	}
	tr, err := NewTransport(ModeReplay, cas, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&http.Client{Transport: tr}).Get("http://closed.invalid/missing")
	if err == nil {
		t.Fatal("expected miss")
	}
}

func TestCassetteDeterministicOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ord.cassette")
	cas, err := NewFileCassette(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cas.Append(&Interaction{Key: "POST /z", Method: "POST", Path: "/z", Status: 200}); err != nil {
		t.Fatal(err)
	}
	if err := cas.Append(&Interaction{Key: "GET /a", Method: "GET", Path: "/a", Status: 200}); err != nil {
		t.Fatal(err)
	}
	if err := cas.Flush(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Index(data, []byte("=== GET /a")) > bytes.Index(data, []byte("=== POST /z")) {
		t.Fatalf("records not sorted by key:\n%s", data)
	}
}

func TestDiff(t *testing.T) {
	if Diff([]byte("same"), []byte("same")) != nil {
		t.Fatal("equal")
	}
	d := Diff([]byte("a\nb\nc"), []byte("a\nX\nc"))
	if len(d) != 1 {
		t.Fatalf("len %d", len(d))
	}
	if d[0].Path != "line[2]" || d[0].Emulated != "b" || d[0].Recorded != "X" {
		t.Fatalf("%+v", d[0])
	}
}
