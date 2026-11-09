package racegate

import "testing"

func TestOnlyRaceAndShortTogetherSkip(t *testing.T) {
	for _, c := range []struct{ race, short, want bool }{
		{false, false, false}, {false, true, false}, {true, false, false}, {true, true, true},
	} {
		if got := skip(c.race, c.short); got != c.want {
			t.Errorf("race=%v short=%v: skip=%v", c.race, c.short, got)
		}
	}
}

func TestSkipsOnlyInThePullRequestRaceRun(t *testing.T) {
	ran := false
	t.Run("probe", func(t *testing.T) {
		SkipUnderRaceOnPullRequest(t, "a probe")
		ran = true
	})
	if want := !(Enabled && testing.Short()); ran != want {
		t.Errorf("ran=%v under race=%v short=%v", ran, Enabled, testing.Short())
	}
}
