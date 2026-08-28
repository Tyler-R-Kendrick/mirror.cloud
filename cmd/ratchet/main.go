// Command ratchet measures the hand-written service surface and, with
// -write, records it as the new baseline in ratchet.json.
//
// The baseline may only fall. Running -write after deleting a pack is the
// normal flow; running it after adding hand-written behavior is caught by
// TestRatchetBaselineOnlyFalls, which compares ratchet.json against the
// merge-base with the integration branch.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/check"
)

func main() {
	write := flag.Bool("write", false, "rewrite ratchet.json from the current tree")
	flag.Parse()

	root, err := check.ModRoot(".")
	if err != nil {
		fail(err)
	}
	current, err := check.Measure(root)
	if err != nil {
		fail(err)
	}

	if *write {
		if err := check.WriteBaseline(root, current); err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "ratchet.json updated: %d packs, %d case labels, %d LOC, %d fault sites\n",
			current.Packs, current.CaseLabels, current.ServicesLOC, current.FaultSites)
		return
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(current); err != nil {
		fail(err)
	}

	baseline, err := check.LoadBaseline(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "no baseline yet:", err)
		return
	}
	regressions, newPacks := check.Compare(baseline, current)
	for _, r := range regressions {
		fmt.Fprintln(os.Stderr, "REGRESSION", r)
	}
	for _, p := range newPacks {
		fmt.Fprintln(os.Stderr, "NEW PACK", p)
	}
	if len(regressions) > 0 || len(newPacks) > 0 {
		os.Exit(1)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "ratchet:", err)
	os.Exit(1)
}
