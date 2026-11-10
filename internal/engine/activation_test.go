package engine

import (
	"testing"
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/bir"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/clock"
	"github.com/tyler-r-kendrick/mirror.cloud/internal/spi"
)

// tickingClock moves on by step every time it is read.
type tickingClock struct {
	*clock.Controllable
	step time.Duration
}

func (c tickingClock) Now() time.Time {
	now := c.Controllable.Now()
	_ = c.Controllable.Advance(c.step)
	return now
}

// `now` and `now_ms` are one reading of the clock. Read separately, an
// expression that mixes them -- a whole-second time reported beside a
// millisecond deadline -- could see two instants either side of a second.
func TestActivationReadsTheClockOnce(t *testing.T) {
	clk := tickingClock{Controllable: clock.NewControllable(), step: 700 * time.Millisecond}
	e := &Engine{deps: spi.Deps{Clock: clk}}
	for i := 0; i < 4; i++ {
		act := e.newEval(&spi.Request{}, bir.Operation{}).activation()
		now, ms := act["now"].(time.Time), act["now_ms"].(int64)
		if now.UnixMilli() != ms {
			t.Fatalf("evaluation %d: now is %d ms and now_ms is %d", i, now.UnixMilli(), ms)
		}
	}
}
