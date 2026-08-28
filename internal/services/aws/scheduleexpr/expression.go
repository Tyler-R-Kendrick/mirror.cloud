// Package scheduleexpr parses AWS EventBridge rate, cron, and at schedule expressions.
package scheduleexpr

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Expression is a parsed AWS schedule expression.
type Expression struct {
	at    time.Time
	every time.Duration
	cron  *cronExpression
}

// Parse validates and parses an AWS schedule expression in timezone.
func Parse(raw, timezone string) (Expression, error) {
	loc := time.UTC
	if timezone != "" {
		var err error
		loc, err = time.LoadLocation(timezone)
		if err != nil {
			return Expression{}, fmt.Errorf("invalid timezone %q", timezone)
		}
	}
	body := func(prefix string) (string, bool) {
		return strings.TrimSuffix(strings.TrimPrefix(raw, prefix+"("), ")"), strings.HasPrefix(raw, prefix+"(") && strings.HasSuffix(raw, ")")
	}
	if value, ok := body("at"); ok {
		at, err := time.ParseInLocation("2006-01-02T15:04:05", value, loc)
		if err != nil {
			return Expression{}, fmt.Errorf("invalid at expression")
		}
		return Expression{at: at}, nil
	}
	if value, ok := body("rate"); ok {
		parts := strings.Fields(value)
		if len(parts) != 2 {
			return Expression{}, fmt.Errorf("invalid rate expression")
		}
		n, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil || n < 1 {
			return Expression{}, fmt.Errorf("invalid rate value")
		}
		units := map[string]time.Duration{
			"minute": time.Minute, "minutes": time.Minute,
			"hour": time.Hour, "hours": time.Hour,
			"day": 24 * time.Hour, "days": 24 * time.Hour,
		}
		unit, ok := units[parts[1]]
		if !ok || (n == 1) != !strings.HasSuffix(parts[1], "s") {
			return Expression{}, fmt.Errorf("invalid rate unit")
		}
		if n > int64((1<<63-1)/unit) {
			return Expression{}, fmt.Errorf("rate value is too large")
		}
		return Expression{every: time.Duration(n) * unit}, nil
	}
	if value, ok := body("cron"); ok {
		cron, err := parseCron(value, loc)
		return Expression{cron: cron}, err
	}
	return Expression{}, fmt.Errorf("invalid schedule expression")
}

// First returns the first invocation on or after now and start.
func (e Expression) First(now, start time.Time) time.Time {
	if !e.at.IsZero() {
		if e.at.Before(now) {
			return time.Time{}
		}
		return e.at
	}
	if start.After(now) {
		now = start
	}
	switch {
	case e.every > 0:
		return now
	case e.cron != nil:
		next := e.cron.next(now.Add(-time.Minute))
		if !next.IsZero() && next.Before(now) {
			next = e.cron.next(next)
		}
		return next
	}
	return time.Time{}
}

// After returns the next invocation strictly after t.
func (e Expression) After(t time.Time) time.Time {
	switch {
	case e.every > 0:
		return t.Add(e.every)
	case e.cron != nil:
		return e.cron.next(t)
	}
	return time.Time{}
}

// OneTime reports whether this is an at expression.
func (e Expression) OneTime() bool { return !e.at.IsZero() }

type cronExpression struct {
	location                *time.Location
	minute, hour, month, yr intField
	dayOfMonth, dayOfWeek   dayField
}

type intField map[int]bool

type dayField struct {
	any, last        bool
	values           intField
	nearestWeekday   int
	lastWeekday      int
	weekday, ordinal int
}

var months = map[string]int{"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
var weekdays = map[string]int{"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7}

func parseCron(raw string, loc *time.Location) (*cronExpression, error) {
	parts := strings.Fields(strings.ToUpper(raw))
	if len(parts) != 6 || (parts[2] == "?") == (parts[4] == "?") {
		return nil, fmt.Errorf("cron requires six fields and ? in exactly one day field")
	}
	minute, err := parseIntField(parts[0], 0, 59, nil)
	if err != nil {
		return nil, err
	}
	hour, err := parseIntField(parts[1], 0, 23, nil)
	if err != nil {
		return nil, err
	}
	dom, err := parseDayField(parts[2], false)
	if err != nil {
		return nil, err
	}
	month, err := parseIntField(parts[3], 1, 12, months)
	if err != nil {
		return nil, err
	}
	dow, err := parseDayField(parts[4], true)
	if err != nil {
		return nil, err
	}
	year, err := parseIntField(parts[5], 1970, 2199, nil)
	if err != nil {
		return nil, err
	}
	return &cronExpression{location: loc, minute: minute, hour: hour, dayOfMonth: dom, month: month, dayOfWeek: dow, yr: year}, nil
}

func parseIntField(raw string, min, max int, names map[string]int) (intField, error) {
	out := intField{}
	for _, item := range strings.Split(raw, ",") {
		base, stepRaw, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			var err error
			step, err = strconv.Atoi(stepRaw)
			if err != nil || step < 1 {
				return nil, fmt.Errorf("invalid cron step %q", item)
			}
		}
		lo, hi := min, max
		if base != "*" {
			left, right, ranged := strings.Cut(base, "-")
			var err error
			lo, err = fieldNumber(left, names)
			if err != nil {
				return nil, err
			}
			hi = lo
			if ranged {
				hi, err = fieldNumber(right, names)
				if err != nil {
					return nil, err
				}
			} else if hasStep {
				hi = max
			}
		}
		if lo < min || hi > max || (lo > hi && (min != 0 || max != 23)) {
			return nil, fmt.Errorf("cron value %q out of range", item)
		}
		if lo > hi {
			for n := lo; n <= max; n += step {
				out[n] = true
			}
			lo = min
		}
		for n := lo; n <= hi; n += step {
			out[n] = true
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty cron field")
	}
	return out, nil
}

func fieldNumber(raw string, names map[string]int) (int, error) {
	if n, ok := names[raw]; ok {
		return n, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid cron value %q", raw)
	}
	return n, nil
}

func parseDayField(raw string, weekday bool) (dayField, error) {
	if raw == "?" {
		return dayField{any: true}, nil
	}
	if !weekday && raw == "L" {
		return dayField{last: true}, nil
	}
	if !weekday && strings.HasSuffix(raw, "W") {
		n, err := fieldNumber(strings.TrimSuffix(raw, "W"), nil)
		if err != nil || n < 1 || n > 31 {
			return dayField{}, fmt.Errorf("invalid W day %q", raw)
		}
		return dayField{nearestWeekday: n}, nil
	}
	if weekday {
		if value, ordinal, ok := strings.Cut(raw, "#"); ok {
			w, err := fieldNumber(value, weekdays)
			n, nerr := strconv.Atoi(ordinal)
			if err != nil || nerr != nil || w < 1 || w > 7 || n < 1 || n > 5 {
				return dayField{}, fmt.Errorf("invalid # day %q", raw)
			}
			return dayField{weekday: w, ordinal: n}, nil
		}
		if strings.HasSuffix(raw, "L") {
			w, err := fieldNumber(strings.TrimSuffix(raw, "L"), weekdays)
			if err != nil || w < 1 || w > 7 {
				return dayField{}, fmt.Errorf("invalid L day %q", raw)
			}
			return dayField{lastWeekday: w}, nil
		}
	}
	max, names := 31, map[string]int(nil)
	if weekday {
		max, names = 7, weekdays
	}
	values, err := parseIntField(raw, 1, max, names)
	return dayField{values: values}, err
}

func (c *cronExpression) next(after time.Time) time.Time {
	// ponytail: calendar-field scan; jump tables only if sparse schedules become a measured bottleneck.
	t := after.In(c.location).Truncate(time.Minute).Add(time.Minute)
	for t.Year() <= 2199 {
		switch {
		case !c.yr[t.Year()]:
			t = time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, c.location)
		case !c.month[int(t.Month())]:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, c.location)
		case !c.matchesDay(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, c.location)
		case !c.hour[t.Hour()]:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, c.location)
		case !c.minute[t.Minute()]:
			t = t.Add(time.Minute)
		default:
			return t
		}
	}
	return time.Time{}
}

func (c *cronExpression) matchesDay(t time.Time) bool {
	return c.dayOfMonth.matches(t, false) && c.dayOfWeek.matches(t, true)
}

func (d dayField) matches(t time.Time, weekday bool) bool {
	if d.any {
		return true
	}
	lastDay := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location()).Day()
	if d.last {
		return t.Day() == lastDay
	}
	awsWeekday := int(t.Weekday()) + 1
	if d.lastWeekday != 0 {
		return awsWeekday == d.lastWeekday && t.Day()+7 > lastDay
	}
	if d.weekday != 0 {
		return awsWeekday == d.weekday && (t.Day()-1)/7+1 == d.ordinal
	}
	if d.nearestWeekday != 0 {
		day := d.nearestWeekday
		if day > lastDay {
			return false
		}
		candidate := time.Date(t.Year(), t.Month(), day, 0, 0, 0, 0, t.Location())
		switch candidate.Weekday() {
		case time.Saturday:
			if day == 1 {
				day += 2
			} else {
				day--
			}
		case time.Sunday:
			if day == lastDay {
				day -= 2
			} else {
				day++
			}
		}
		return t.Day() == day
	}
	value := t.Day()
	if weekday {
		value = awsWeekday
	}
	return d.values[value]
}
