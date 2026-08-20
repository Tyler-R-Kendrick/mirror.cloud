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
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	at := c.now.Add(d)
	if !c.now.Before(at) {
		c.mu.Unlock()
		ch <- c.now
		return ch
	}
	c.wait = append(c.wait, waiter{at: at, ch: ch})
	c.mu.Unlock()
	return ch
}

var _ spi.Clock = (*Controllable)(nil)
var _ spi.Clock = Real{}
