// Package racegate keeps a test that starts no goroutine of its own out of the
// race detector's run on a pull request, and in every other run.
//
// The race detector instruments every memory access, so a test that parses a
// large input on one goroutine costs ten to forty times as much under -race and
// can find nothing there: a race needs two goroutines. Pull requests run the
// race job with -race -short, and such a test calls SkipUnderRaceOnPullRequest.
// main and the nightly run race without -short, so it still runs under race
// there, and the unit job runs it on every pull request without race.
package racegate

import "testing"

// Enabled reports whether this binary was built with -race.
const Enabled = enabled

// SkipUnderRaceOnPullRequest skips t when the binary was built with -race and
// the run is -short, which together are the pull-request race job and nothing
// else. why says what the test does on one goroutine; it goes in the skip
// message, so the log says what was left out and why.
func SkipUnderRaceOnPullRequest(t testing.TB, why string) {
	t.Helper()
	if skip(Enabled, testing.Short()) {
		t.Skipf("race on a pull request (-race -short): %s, on one goroutine; it runs under -race on main and nightly, and without it in unit", why)
	}
}

func skip(race, short bool) bool { return race && short }
