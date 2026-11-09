// Command affected prints the packages whose tests a set of changed files can
// reach, so a pull request runs the race detector, the fuzzers and the
// mutation suite on those and not on the whole module.
//
//	git diff --name-only HEAD^1 HEAD | go run ./cmd/affected
//
// A changed .go file selects its package and every package whose test binary
// links it (go list -test -deps). A file that is not Go selects the package
// whose directory holds it (embedded files, testdata), or what a rule in
// affected.go names for it (files tests read at run time, documentation that
// nothing reads). A path that no rule and no package accounts for selects
// every package, as do go.mod and go.sum: an input it cannot place is one it
// cannot rule out.
//
// The output is one package per line, as ./dir. -json prints
// {"all": bool, "reason": string, "packages": [...], "linked": [...],
// "jobs": {...}}: linked is the part of packages whose test binaries link
// changed code, leaving out the packages selected only because their tests
// read a changed file at run time, and jobs says which path-gated CI jobs the
// change opens (gates, in affected.go).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "affected:", err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("affected", flag.ContinueOnError)
	fs.SetOutput(stderr)
	files := fs.String("files", "-", "file listing the changed paths, one per line; - is stdin")
	root := fs.String("root", ".", "the module root")
	asJSON := fs.Bool("json", false, "print a JSON object instead of one package per line")
	all := fs.Bool("all", false, "select everything, whatever changed: what main runs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	in := stdin
	if *files != "-" {
		f, err := os.Open(*files)
		if err != nil {
			return err
		}
		defer f.Close()
		in = f
	}
	var changed []string
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		if line := strings.TrimSpace(sc.Text()); line != "" {
			changed = append(changed, line)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	g, err := load(*root)
	if err != nil {
		return err
	}
	r := g.selectFor(changed)
	if *all {
		r = result{all: true, reason: "-all", pkgs: g.pkgs}
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		return enc.Encode(map[string]any{"all": r.all, "reason": r.reason, "packages": nonNil(r.pkgs), "linked": r.linkedList(), "jobs": r.gates(changed)})
	}
	if r.all {
		fmt.Fprintf(stderr, "affected: every package (%s)\n", r.reason)
	} else {
		fmt.Fprintf(stderr, "affected: %d of %d packages\n", len(r.pkgs), len(g.pkgs))
	}
	for _, p := range r.pkgs {
		fmt.Fprintln(stdout, p)
	}
	return nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
