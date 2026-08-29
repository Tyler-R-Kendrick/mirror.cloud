package clock_test

import (
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
)

func TestControllableNowAdvanceSince(t *testing.T) {
	c := clock.NewControllable()
	epoch := time.Unix(0, 0).UTC()
	if !c.Now().Equal(epoch) {
		t.Fatalf("start: %v", c.Now())
	}
	if err := c.Advance(3 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := c.Now(); !got.Equal(epoch.Add(3 * time.Second)) {
		t.Fatalf("after advance: %v", got)
	}
	if d := c.Since(epoch); d != 3*time.Second {
		t.Fatalf("since: %v", d)
	}
}

func TestAfterFiresWhenAdvancePassesDeadline(t *testing.T) {
	c := clock.NewControllable()
	ch := c.After(time.Hour)
	select {
	case <-ch:
		t.Fatal("After fired before Advance")
	default:
	}
	if err := c.Advance(time.Hour - time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("After fired before deadline")
	default:
	}
	if err := c.Advance(time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-ch:
		if got.Before(c.Now()) && !got.Equal(c.Now()) {
			t.Fatalf("fired at %v, now %v", got, c.Now())
		}
	default:
		t.Fatal("After did not fire when Advance reached the deadline")
	}
}

func TestAfterZeroFiresWithoutAdvance(t *testing.T) {
	c := clock.NewControllable()
	ch := c.After(0)
	select {
	case <-ch:
	default:
		t.Fatal("After(0) should fire immediately")
	}
}

func TestRealAdvanceRefused(t *testing.T) {
	if err := (clock.Real{}).Advance(time.Second); err == nil {
		t.Fatal("real clock must refuse Advance")
	}
}

// TestAfterTimeSurvivesAnAdvanceDuringRegistration is the regression test for a
// lost wakeup that took three PRs' CI red before it was understood.
//
// Every background loop in this emulator has the same shape: work out when the
// next thing is due, then park until then. Expressed with After, that is three
// steps -- read the clock, subtract, register -- and a clock advance landing
// between the read and the register is silently absorbed: the delay was
// measured against the old time, so the timer is set that far past the *new*
// time. A controllable clock is only advanced by a test, and the test has
// already advanced, so nothing ever fires and the loop sleeps forever.
//
// The sequence below is exactly what such a loop does, with the advance placed
// in the window on purpose. No goroutines are needed to show it: the bug is in
// the arithmetic, not in the scheduling.
func TestAfterTimeSurvivesAnAdvanceDuringRegistration(t *testing.T) {
	deadline := func(c *clock.Controllable) time.Time { return c.Now().Add(90 * time.Second) }

	t.Run("After loses the wakeup", func(t *testing.T) {
		c := clock.NewControllable()
		due := deadline(c)
		delay := due.Sub(c.Now())
		// The window: a test advances past the deadline before the loop parks.
		if err := c.Advance(2 * time.Minute); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.After(delay):
			t.Fatal("After fired; this test documents that it does not, so if it " +
				"now does, the fix moved and this test should be retired")
		default:
		}
	})

	t.Run("AfterTime keeps it", func(t *testing.T) {
		c := clock.NewControllable()
		due := deadline(c)
		if err := c.Advance(2 * time.Minute); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.AfterTime(due):
		default:
			t.Fatal("AfterTime did not fire for a deadline the clock is already past")
		}
	})

	t.Run("AfterTime still waits for a deadline ahead", func(t *testing.T) {
		c := clock.NewControllable()
		due := deadline(c)
		ch := c.AfterTime(due)
		select {
		case <-ch:
			t.Fatal("AfterTime fired before its instant")
		default:
		}
		if err := c.Advance(2 * time.Minute); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ch:
		default:
			t.Fatal("AfterTime did not fire once the clock reached its instant")
		}
	})
}
