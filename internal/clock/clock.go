// Package clock is the only source of time in the process.
package clock

import (
	"errors"
	"sync"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// Real is a wall-clock Clock. Advance returns an error.
type Real struct{}

func (Real) Now() time.Time                  { return time.Now() }
func (Real) Since(t time.Time) time.Duration { return time.Since(t) }
func (Real) Advance(time.Duration) error {
	return errors.New("real clock cannot be advanced")
}
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// AfterTime waits until at. A deadline already past fires immediately, which
// is what time.After does with a non-positive duration anyway.
func (Real) AfterTime(at time.Time) <-chan time.Time { return time.After(time.Until(at)) }

// Controllable is a deterministic clock starting at epoch.
type Controllable struct {
	mu   sync.Mutex
	now  time.Time
	wait []waiter
}

type waiter struct {
	at time.Time
	ch chan time.Time
}

// NewControllable returns a clock at Unix epoch UTC.
func NewControllable() *Controllable {
	return &Controllable{now: time.Unix(0, 0).UTC()}
}

func (c *Controllable) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Controllable) Since(t time.Time) time.Duration {
	return c.Now().Sub(t)
}

func (c *Controllable) Advance(d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	remain := c.wait[:0]
	for _, w := range c.wait {
		if !c.now.Before(w.at) {
			select {
			case w.ch <- c.now:
			default:
			}
		} else {
			remain = append(remain, w)
		}
	}
	c.wait = remain
	return nil
}

func (c *Controllable) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	at := c.now.Add(d)
	c.mu.Unlock()
	return c.AfterTime(at)
}

// AfterTime registers a waiter for an absolute instant.
//
// Taking the instant rather than a delay is what closes the lost-wakeup window
// for a caller waiting on a deadline. Such a caller reads Now, works out how
// far away the deadline is, and only then registers. If the clock advances in
// between, a delay is measured from the new time and the waiter is parked
// beyond a deadline that has already passed -- and a controllable clock has no
// second jump to rescue it. With the instant, the advance either lands before
// this call, in which case at is already past and it fires immediately, or
// after it, in which case the waiter is in the list and Advance delivers.
func (c *Controllable) AfterTime(at time.Time) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	if !c.now.Before(at) {
		now := c.now
		c.mu.Unlock()
		ch <- now
		return ch
	}
	c.wait = append(c.wait, waiter{at: at, ch: ch})
	c.mu.Unlock()
	return ch
}

var _ spi.Clock = (*Controllable)(nil)
var _ spi.Clock = Real{}
