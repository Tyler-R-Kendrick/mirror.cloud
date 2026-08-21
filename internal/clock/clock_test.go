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
