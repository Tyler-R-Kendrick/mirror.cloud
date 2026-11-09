// Command affected prints the packages whose tests a set of changed files can
// reach (internal/affected), so a pull request runs the race detector, the
// fuzzers and the mutation suite on those and not on the whole module.
//
//	git diff -z --name-only --no-renames HEAD^1 HEAD | go run ./cmd/affected -z
//
// The output is one package per line, as ./dir. -json prints
// {"all": bool, "reason": string, "packages": [...], "linked": [...],
// "jobs": {...}}: linked is the part of packages whose test binaries link
// changed code or are its own tests, leaving out the packages selected only
// because their tests read a changed file at run time, and jobs says which
// path-gated CI jobs the change opens.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/affected"
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
	nul := fs.Bool("z", false, "paths are NUL-terminated, as git diff -z --name-only writes them")
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
	raw, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	changed := affected.Split(raw, *nul)
	g, err := affected.Load(*root)
	if err != nil {
		return err
	}
	if *all {
		changed = []string{"go.mod"} // what selects everything
	}
	r := g.Select(changed)
	if *all {
		r.Reason = "-all"
	}
	if *asJSON {
		return json.NewEncoder(stdout).Encode(map[string]any{"all": r.All, "reason": r.Reason, "packages": r.Pkgs, "linked": r.Linked, "jobs": r.Jobs})
	}
	if r.All {
		fmt.Fprintf(stderr, "affected: every package (%s)\n", r.Reason)
	} else {
		fmt.Fprintf(stderr, "affected: %d of %d packages\n", len(r.Pkgs), len(g.Packages()))
	}
	for _, p := range r.Pkgs {
		fmt.Fprintln(stdout, p)
	}
	return nil
}
