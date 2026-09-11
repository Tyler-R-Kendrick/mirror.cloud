package check

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// specs/urls.tsv is the table of provider documents served from a URL rather
// than a git repository. These guard the property the table exists for: a
// document with no upstream revision is pinned by being committed, so a build
// never depends on what a vendor happened to be serving that minute.
//
// The table replaced a hardcoded branch for the one such document that existed
// (Google Discovery), written as a bespoke path with a positional-argument
// lock writer. A second provider could not be added without editing the shell,
// which is the shape of thing that ends up not being added at all -- and eight
// providers did arrive instead as hand-written Go packs.

type urlEntry struct{ serviceID, path, url string }

// declaredSet reads the service IDs specs/mirror.set asks to be generated.
func declaredSet(t *testing.T) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(findMod(t), "specs", "mirror.set"))
	if err != nil {
		t.Fatalf("specs/mirror.set: %v", err)
	}
	out := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out[strings.Fields(line)[0]] = true
	}
	return out
}

func urlTable(t *testing.T) []urlEntry {
	t.Helper()
	root := findMod(t)
	file, err := os.Open(filepath.Join(root, "specs", "urls.tsv"))
	if err != nil {
		t.Fatalf("specs/urls.tsv: %v", err)
	}
	defer file.Close()
	var out []urlEntry
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimRight(scanner.Text(), "\r")
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 3 {
			t.Fatalf("specs/urls.tsv:%d: want 3 tab-separated fields, got %d: %q", line, len(fields), text)
		}
		out = append(out, urlEntry{fields[0], fields[1], fields[2]})
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(out) == 0 {
		t.Fatal("specs/urls.tsv has no entries")
	}
	return out
}

// TestEveryURLServedSpecIsCommitted is the pin. A document listed here but not
// in the tree would be refetched on every build, and a build whose inputs move
// under it cannot produce a reproducible model -- which is the failure the
// committed Discovery document already exists to prevent, recorded in the
// script: CI went red because the document changed between two runs an hour
// apart.
func TestEveryURLServedSpecIsCommitted(t *testing.T) {
	root := findMod(t)
	for _, entry := range urlTable(t) {
		path := filepath.Join(root, "specs", entry.path)
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("%s: specs/%s is not committed; run: SPECS_REFRESH=1 make specs-sync (%v)",
				entry.serviceID, entry.path, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%s: specs/%s is empty", entry.serviceID, entry.path)
		}
	}
}

// TestTheLockRecordsEveryURLServedSpec checks the other half: the committed
// bytes are the ones the lock names. A document edited in place, or replaced
// without re-running specs-sync, would generate a model that no longer follows
// from its recorded input -- and the lock is what makes the committed models
// trustworthy as a build artifact.
func TestTheLockRecordsEveryURLServedSpec(t *testing.T) {
	root := findMod(t)
	raw, err := os.ReadFile(filepath.Join(root, "specs", "mirror.lock"))
	if err != nil {
		t.Fatalf("specs/mirror.lock: %v", err)
	}
	var lock struct {
		Pin   map[string]struct{ Source, Ref string } `json:"pin"`
		Files []struct {
			ServiceID string `json:"serviceId"`
			Source    string `json:"source"`
			Path      string `json:"path"`
			SHA256    string `json:"sha256"`
		} `json:"files"`
	}
	if err := json.Unmarshal(raw, &lock); err != nil {
		t.Fatal(err)
	}
	locked := map[string]struct{ source, sha string }{}
	for _, f := range lock.Files {
		locked[f.Path] = struct{ source, sha string }{f.Source, f.SHA256}
	}

	declared := declaredSet(t)
	for _, entry := range urlTable(t) {
		// Only a service the set asks for is fetched, so only that one is
		// locked. An entry the set does not name is a document available to be
		// added, not a document that must be present.
		if !declared[entry.serviceID] {
			continue
		}
		got, ok := locked[entry.path]
		if !ok {
			t.Errorf("%s is in specs/mirror.set and specs/urls.tsv but not in the lock; run: make specs-sync",
				entry.serviceID)
			continue
		}
		if got.source != entry.url {
			t.Errorf("%s: lock records source %q, table says %q", entry.serviceID, got.source, entry.url)
		}
		data, err := os.ReadFile(filepath.Join(root, "specs", entry.path))
		if err != nil {
			t.Errorf("%s: %v", entry.serviceID, err)
			continue
		}
		sum := sha256.Sum256(data)
		if want := hex.EncodeToString(sum[:]); want != got.sha {
			t.Errorf("%s: specs/%s hashes to %s, the lock says %s; run: make specs-sync",
				entry.serviceID, entry.path, want, got.sha)
		}
		if _, pinned := lock.Pin[entry.serviceID]; !pinned {
			t.Errorf("%s has no pin entry in the lock", entry.serviceID)
		}
	}
}

// TestNoURLServedSpecIsIgnored keeps these documents out of .gitignore. They
// are the pin, so ignoring one turns every build into a refetch -- silently,
// because a missing spec is indistinguishable from one not yet added.
func TestNoURLServedSpecIsIgnored(t *testing.T) {
	root := findMod(t)
	raw, err := os.ReadFile(filepath.Join(root, ".gitignore"))
	if err != nil {
		t.Skipf(".gitignore is unreadable: %v", err)
	}
	var patterns []string
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, strings.TrimSuffix(line, "/"))
	}
	for _, entry := range urlTable(t) {
		for _, pattern := range patterns {
			if pattern == "" {
				continue
			}
			target := "specs/" + entry.path
			if target == pattern || strings.HasPrefix(target, pattern+"/") {
				t.Errorf("%s: specs/%s is ignored by %q, but a URL-served document is the pin and must be committed",
					entry.serviceID, entry.path, pattern)
			}
		}
	}
}
