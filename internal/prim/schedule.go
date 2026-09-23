package prim

import (
	"fmt"

	"github.com/tyler-r-kendrick/mirror.cloud/internal/services/aws/scheduleexpr"
)

// scheduler.expression_error answers why a schedule expression does not
// parse in its time zone, or "" when it does: the at/rate/cron grammar and
// the IANA zone lookup, which CEL has neither of.
func init() {
	Register(Func{
		Name:    "scheduler.expression_error",
		Version: 1,
		Call: func(args []any) (any, error) {
			if len(args) != 2 {
				return nil, fmt.Errorf("scheduler.expression_error takes expression and time zone, got %d arguments", len(args))
			}
			expression, _ := args[0].(string)
			zone, _ := args[1].(string)
			if _, err := scheduleexpr.Parse(expression, zone); err != nil {
				return err.Error(), nil
			}
			return "", nil
		},
	})
}
