package scheduler

import (
	"time"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduleexpr"
)

type scheduleExpression struct{ scheduleexpr.Expression }

func parseScheduleExpression(raw, timezone string) (scheduleExpression, error) {
	expression, err := scheduleexpr.Parse(raw, timezone)
	return scheduleExpression{expression}, err
}

func (e scheduleExpression) first(now, start time.Time) time.Time { return e.First(now, start) }
func (e scheduleExpression) after(t time.Time) time.Time          { return e.After(t) }
